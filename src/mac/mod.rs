//! Two-way Mac ↔ VM access (ADR 0113). VM agents reach the paired Mac through its helper; the Mac runs commands on the VM.
use crate::{preview::Peer, session::Manager};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path as RoutePath, Query, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs, io,
    os::unix::{fs::FileTypeExt, fs::PermissionsExt, net::UnixStream as StdUnixStream},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;

pub(crate) const SOCKET: &str = "/run/cloudroom/mac.sock";
/// Raw bytes per stdin, stdout, or stderr. Hex doubles this on the wire.
const LIMIT: usize = 16 * 1024 * 1024;
const BODY: usize = 5 * LIMIT;
/// A connected helper must confirm each job quickly; a sleeping Mac must not hang agents.
const ACK: Duration = Duration::from_secs(10);
const KEEP: Duration = Duration::from_secs(3600);
/// The helper needs a few seconds to find a new thread's sandbox or reconnect after a wake, so early calls wait for it.
const FRESH: Duration = Duration::from_secs(60);
const ATTACH: Duration = Duration::from_secs(30);
const UNAVAILABLE: &str = "Mac unavailable: the paired Mac is offline, asleep, or has Mac access turned off. Continue cloud work and try again later.";
pub const SKILL: &str = include_str!("cloud-mac/SKILL.md");
pub const ROOM_CLI_SKILL: &str = include_str!("room-cli/SKILL.md");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    command: String,
    #[serde(default)]
    stdin: String,
    cwd: Option<String>,
    /// The command changes the Mac. Below Full access the helper refuses it or asks the user first (ADR 0186).
    #[serde(default)]
    write: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    device: String,
    state: String,
    code: Option<i32>,
    #[serde(default)]
    stdout: String,
    #[serde(default)]
    stderr: String,
    #[serde(default)]
    truncated: bool,
    /// A `part` report's position in its stream's hex text. Some sandbox proxies drop requests over about 8 MB,
    /// so the helper sends large output in parts before a final `done` report (`upload_parts`).
    offset: Option<usize>,
}
#[derive(Deserialize)]
pub struct Device {
    device: String,
}

struct Job {
    device: String,
    acked: bool,
    result: Option<Value>,
    at: Instant,
    /// Hex output received in parts so far: stdout, stderr.
    parts: [Vec<u8>; 2],
}
struct Helper {
    generation: u64,
    device: String,
    events: mpsc::Sender<std::result::Result<Event, Infallible>>,
    /// A job went unanswered: the Mac likely slept while a proxy kept its stream open.
    stale: bool,
}

pub struct Mac {
    jobs: Mutex<BTreeMap<String, Job>>,
    helper: Mutex<Option<Helper>>,
    changed: watch::Sender<u64>,
    next: AtomicU64,
    since: Mutex<Instant>,
}
impl Default for Mac {
    fn default() -> Self {
        Self {
            jobs: Mutex::default(),
            helper: Mutex::default(),
            changed: watch::channel(0).0,
            next: AtomicU64::new(0),
            since: Mutex::new(Instant::now()),
        }
    }
}
impl Mac {
    /// A new thread or a dropped helper reopens the short wait for the Mac.
    pub(crate) fn expect(&self) {
        *self.since.lock().unwrap() = Instant::now();
    }
    fn touch(&self) {
        self.changed.send_modify(|n| *n += 1);
    }
    fn connected(&self, generation: u64) -> bool {
        self.helper
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.generation == generation)
    }
}

#[derive(Debug)]
pub struct Failure(StatusCode, String);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
type Result<T> = std::result::Result<T, Failure>;
fn conflict(message: &'static str) -> Failure {
    Failure(StatusCode::CONFLICT, message.into())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[usize::from(byte >> 4)] as char);
        text.push(DIGITS[usize::from(byte & 15)] as char);
    }
    text
}
fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}
fn validate(run: &Run) -> Result<Vec<u8>> {
    if run.command.trim().is_empty() || run.command.len() > 65536 || run.command.contains('\0') {
        return Err(conflict("command must contain 1-65536 bytes"));
    }
    if run
        .cwd
        .as_ref()
        .is_some_and(|d| d.is_empty() || d.len() > 4096 || d.contains('\0'))
    {
        return Err(conflict("invalid folder"));
    }
    if run.stdin.len() > 2 * LIMIT {
        return Err(conflict("input exceeds the 16 MiB limit"));
    }
    unhex(&run.stdin).ok_or(conflict("stdin must be hex"))
}

/// Cancels the Mac command when its waiting agent disappears, for example after Stop.
struct Cancel {
    id: String,
    events: mpsc::Sender<std::result::Result<Event, Infallible>>,
    armed: bool,
}
impl Drop for Cancel {
    fn drop(&mut self) {
        if self.armed {
            let data = json!({"id":self.id}).to_string();
            let _ = self
                .events
                .try_send(Ok(Event::default().event("cancel").data(data)));
        }
    }
}

pub fn routes() -> Router<Arc<Manager>> {
    Router::new()
        .route("/v1/mac/jobs", get(stream))
        .route(
            "/v1/mac/results/{id}",
            post(report).layer(DefaultBodyLimit::max(BODY)),
        )
        .route(
            "/v1/vm/run",
            post(vm_run).layer(DefaultBodyLimit::max(BODY)),
        )
        .route(
            "/v1/vm/run/raw",
            post(vm_run_raw).layer(DefaultBodyLimit::max(LIMIT)),
        )
}

/// The paired Mac's helper receives jobs here. The newest connection replaces older ones.
async fn stream(
    State(m): State<Arc<Manager>>,
    Query(input): Query<Device>,
) -> Result<impl IntoResponse> {
    if !m.previews.paired(&input.device) {
        return Err(Failure(
            StatusCode::FORBIDDEN,
            "This Mac is not paired with the VM".into(),
        ));
    }
    let (events, receiver) = mpsc::channel(64);
    let generation = m.mac.next.fetch_add(1, Ordering::Relaxed);
    *m.mac.helper.lock().unwrap() = Some(Helper {
        generation,
        device: input.device,
        events: events.clone(),
        stale: false,
    });
    m.mac.touch();
    let mac = m.clone();
    tokio::spawn(async move {
        events.closed().await;
        let mut helper = mac.mac.helper.lock().unwrap();
        if helper.as_ref().is_some_and(|h| h.generation == generation) {
            *helper = None;
            mac.mac.expect();
        }
        drop(helper);
        mac.mac.touch();
    });
    Ok(Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::default()))
}

async fn report(
    State(m): State<Arc<Manager>>,
    RoutePath(id): RoutePath<String>,
    Json(input): Json<Report>,
) -> Result<Json<Value>> {
    let mut jobs = m.mac.jobs.lock().unwrap();
    let job = jobs
        .get_mut(&id)
        .filter(|j| j.device == input.device)
        .ok_or(Failure(
            StatusCode::NOT_FOUND,
            "Unknown or expired Mac job".into(),
        ))?;
    let output = [input.stdout, input.stderr];
    if output.iter().any(|hex| unhex(hex).is_none()) {
        return Err(conflict("Invalid Mac job report"));
    }
    match (input.state.as_str(), input.offset) {
        ("running", None) => job.acked = true,
        ("part", Some(offset)) => {
            for (parts, hex) in job.parts.iter_mut().zip(&output) {
                if !hex.is_empty() {
                    crate::api::append_part(parts, offset, hex.as_bytes(), 2 * LIMIT)
                        .map_err(conflict)?;
                }
            }
            job.acked = true;
            job.at = Instant::now();
        }
        ("done", None) => {
            let [stdout, stderr] = std::mem::take(&mut job.parts);
            let [stdout, stderr] =
                [(stdout, &output[0]), (stderr, &output[1])].map(|(mut parts, rest)| {
                    parts.extend_from_slice(rest.as_bytes());
                    String::from_utf8(parts).unwrap_or_default()
                });
            if stdout.len() > 2 * LIMIT || stderr.len() > 2 * LIMIT {
                return Err(conflict("Invalid Mac job report"));
            }
            job.acked = true;
            job.at = Instant::now();
            job.result = Some(json!({"state":"done","code":input.code,"stdout":stdout,
                "stderr":stderr,"truncated":input.truncated}));
        }
        _ => return Err(conflict("Invalid Mac job report")),
    }
    drop(jobs);
    if let Some(h) = m
        .mac
        .helper
        .lock()
        .unwrap()
        .as_mut()
        .filter(|h| h.device == input.device)
    {
        h.stale = false;
    }
    m.mac.touch();
    Ok(Json(json!({"accepted":true})))
}

fn local_auth(m: &Manager, peer: Peer) -> Result<()> {
    match m.previews.agent() {
        Some((_, uid)) if peer.0 == Some(uid) => Ok(()),
        _ => Err(Failure(
            StatusCode::FORBIDDEN,
            "Only the VM agent account may use Mac access".into(),
        )),
    }
}

async fn local_run(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(run): Json<Run>,
) -> Result<Json<Value>> {
    local_auth(&m, peer)?;
    validate(&run)?;
    let mac = &m.mac;
    let mut changed = mac.changed.subscribe();
    let fresh = mac.since.lock().unwrap().elapsed() < FRESH;
    let wait = Instant::now() + if fresh { ATTACH } else { Duration::ZERO };
    let (generation, device, events) = loop {
        if let Some(h) = mac.helper.lock().unwrap().as_ref() {
            break (h.generation, h.device.clone(), h.events.clone());
        }
        if Instant::now() >= wait {
            return Err(conflict(UNAVAILABLE));
        }
        tokio::select! {
            _ = changed.changed() => {},
            _ = tokio::time::sleep_until(wait.into()) => {},
        }
    };
    let id = format!(
        "{}-{}",
        std::process::id(),
        mac.next.fetch_add(1, Ordering::Relaxed)
    );
    {
        let mut jobs = mac.jobs.lock().unwrap();
        jobs.retain(|_, j| j.at.elapsed() < if j.result.is_some() { KEEP } else { 24 * KEEP });
        jobs.insert(
            id.clone(),
            Job {
                device,
                acked: false,
                result: None,
                at: Instant::now(),
                parts: Default::default(),
            },
        );
    }
    let data =
        json!({"id":id,"command":run.command,"stdin":run.stdin,"cwd":run.cwd,"write":run.write})
            .to_string();
    if events
        .send(Ok(Event::default().event("job").data(data)))
        .await
        .is_err()
    {
        mac.jobs.lock().unwrap().remove(&id);
        return Err(conflict(UNAVAILABLE));
    }
    let mut cancel = Cancel {
        id: id.clone(),
        events,
        armed: true,
    };
    let started = Instant::now();
    loop {
        let connected = mac.connected(generation);
        {
            let mut jobs = mac.jobs.lock().unwrap();
            let job = jobs.get(&id).ok_or(conflict(UNAVAILABLE))?;
            if let Some(result) = &job.result {
                cancel.armed = false;
                let mut result = result.clone();
                result["job"] = json!(id);
                return Ok(Json(result));
            }
            if !job.acked && (!connected || started.elapsed() >= ACK) {
                jobs.remove(&id);
                if let Some(h) = mac
                    .helper
                    .lock()
                    .unwrap()
                    .as_mut()
                    .filter(|h| h.generation == generation)
                {
                    h.stale = true;
                }
                return Err(conflict(UNAVAILABLE));
            }
            // In-flight work keeps running on the Mac; its helper reports the result after reconnecting.
            if !connected {
                cancel.armed = false;
                return Ok(Json(json!({"job":id,"state":"unknown",
                    "message":format!("The Mac disconnected while this command was running. It may still finish; check later with: cloudroom mac result {id}")})));
            }
        }
        tokio::select! {
            _ = changed.changed() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
    }
}

async fn local_result(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    RoutePath(id): RoutePath<String>,
) -> Result<Json<Value>> {
    local_auth(&m, peer)?;
    let jobs = m.mac.jobs.lock().unwrap();
    let job = jobs.get(&id).ok_or(Failure(
        StatusCode::NOT_FOUND,
        "Unknown or expired Mac job".into(),
    ))?;
    let mut result = job.result.clone().unwrap_or(json!({"state":"running"}));
    result["job"] = json!(id);
    Ok(Json(result))
}

async fn local_status(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
) -> Result<Json<Value>> {
    local_auth(&m, peer)?;
    Ok(Json(
        json!({"connected":m.mac.helper.lock().unwrap().as_ref().is_some_and(|h| !h.stale)}),
    ))
}

pub async fn listen(manager: &Arc<Manager>) -> io::Result<()> {
    let Some((path, _)) = manager.previews.agent() else {
        return Ok(());
    };
    if let Ok(info) = fs::symlink_metadata(&path) {
        if !info.file_type().is_socket() || StdUnixStream::connect(&path).is_ok() {
            return Err(io::Error::other("Mac access socket is already occupied"));
        }
        fs::remove_file(&path)?;
    }
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666))?;
    let app = Router::new()
        .route("/jobs", post(local_run))
        .route("/jobs/{id}", get(local_result))
        .route("/status", get(local_status))
        .merge(crate::secrets::agent_routes())
        .merge(crate::session::thread::agent_routes())
        .merge(crate::session::report::agent_routes())
        .merge(crate::computer_use::agent_routes())
        .layer(DefaultBodyLimit::max(BODY))
        .with_state(manager.clone());
    tokio::spawn(async move {
        if axum::serve(listener, app.into_make_service_with_connect_info::<Peer>())
            .await
            .is_err()
        {
            eprintln!("Cloudroom Mac access socket stopped");
        }
    });
    Ok(())
}

/// Keeps output even when a background process holds the pipe open after the command exits.
async fn capped(mut reader: impl AsyncRead + Unpin, kept: Arc<Mutex<(Vec<u8>, bool)>>) {
    let mut buffer = vec![0; 65536];
    while let Ok(n) = reader.read(&mut buffer).await {
        if n == 0 {
            break;
        }
        let mut kept = kept.lock().unwrap();
        let room = LIMIT - kept.0.len();
        kept.1 |= n > room;
        kept.0.extend_from_slice(&buffer[..n.min(room)]);
    }
}

/// The paired Mac runs a shell command as the VM agent account.
async fn vm_run(State(m): State<Arc<Manager>>, Json(run): Json<Run>) -> Result<Json<Value>> {
    let stdin = validate(&run)?;
    execute(&m, run, stdin, false).await
}

/// Like `vm_run`, with the command in the query and stdin as the raw body: half the bytes of hex.
/// Bulk project copies use it, so it stays blocked while disk space is critically low.
async fn vm_run_raw(
    State(m): State<Arc<Manager>>,
    Query(run): Query<Run>,
    stdin: Bytes,
) -> Result<Json<Value>> {
    validate(&run)?;
    execute(&m, run, stdin.into(), true).await
}

async fn execute(m: &Manager, run: Run, stdin: Vec<u8>, gated: bool) -> Result<Json<Value>> {
    let home = &m.config.account_home;
    let folder = run.cwd.as_ref().map_or(home.clone(), |d| home.join(d));
    if !folder.is_dir() {
        return Err(conflict("VM folder not found"));
    }
    let mut command = tokio::process::Command::new("/bin/bash");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", home)
        .env("LANG", "C.UTF-8")
        .args(["-lc", &run.command])
        .current_dir(folder)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Plain commands are the emergency way in: they still run while storage is blocked.
    let spawned = if gated {
        m.storage.spawn_writer(&mut command)
    } else {
        m.storage.spawn_operator(&mut command)
    };
    let (mut child, _workload) = spawned.map_err(|error| {
        Failure(
            StatusCode::CONFLICT,
            format!("VM command could not start: {error}"),
        )
    })?;
    let mut input = child.stdin.take().unwrap();
    tokio::spawn(async move {
        let _ = input.write_all(&stdin).await;
    });
    let (out, err) = (Arc::new(Mutex::default()), Arc::new(Mutex::default()));
    let readers = [
        tokio::spawn(capped(child.stdout.take().unwrap(), out.clone())),
        tokio::spawn(capped(child.stderr.take().unwrap(), err.clone())),
    ];
    let status = child.wait().await.map_err(|error| {
        Failure(
            StatusCode::CONFLICT,
            format!("VM command status unavailable: {error}"),
        )
    })?;
    for reader in readers {
        let _ = tokio::time::timeout(Duration::from_secs(2), reader).await;
    }
    let (out, err) = (out.lock().unwrap(), err.lock().unwrap());
    Ok(Json(
        json!({"code":status.code(),"stdout":hex(&out.0),"stderr":hex(&err.0),
        "truncated":out.1 || err.1}),
    ))
}

/// Mac paths may start with ~; everything else stays literal.
fn quote(path: &str) -> String {
    let (prefix, rest) = match path {
        "~" => ("\"$HOME\"", ""),
        _ => match path.strip_prefix("~/") {
            Some(rest) => ("\"$HOME\"/", rest),
            None => ("", path),
        },
    };
    if rest.is_empty() {
        return prefix.to_owned();
    }
    format!("{prefix}'{}'", rest.replace('\'', "'\\''"))
}

pub(crate) async fn request(
    socket: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> io::Result<Value> {
    let mut stream = UnixStream::connect(socket).await.map_err(|error| {
        io::Error::other(format!(
            "Cloudroom agent access is not installed on this VM ({socket}: {error})"
        ))
    })?;
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| io::Error::other("invalid Mac access response"))?;
    let value: Value = serde_json::from_slice(&response[split + 4..])?;
    if let Some(message) = value.get("error").and_then(Value::as_str) {
        return Err(io::Error::other(message.to_owned()));
    }
    Ok(value)
}

/// Returns the finished command's stdout, or an error with its output.
fn finished(value: Value) -> io::Result<(Option<i32>, Vec<u8>, Vec<u8>)> {
    if value["state"] != "done" {
        return Err(io::Error::other(value.to_string()));
    }
    let stream = |key: &str| {
        value[key]
            .as_str()
            .and_then(unhex)
            .ok_or_else(|| io::Error::other("invalid Mac output"))
    };
    if value["truncated"] == true {
        eprintln!("Output exceeded 16 MiB and was truncated.");
    }
    Ok((
        value["code"].as_i64().map(|c| c as i32),
        stream("stdout")?,
        stream("stderr")?,
    ))
}

async fn copy(socket: &str, command: String, stdin: Vec<u8>, write: bool) -> io::Result<Vec<u8>> {
    let body = json!({"command":command,"stdin":hex(&stdin),"write":write});
    let (code, stdout, stderr) = finished(request(socket, "POST", "/jobs", Some(body)).await?)?;
    if code != Some(0) {
        return Err(io::Error::other(
            String::from_utf8_lossy(&stderr).into_owned(),
        ));
    }
    Ok(stdout)
}

/// Returns the process exit code.
pub async fn cli(args: &[String]) -> io::Result<i32> {
    let mut args = args.to_vec();
    let mut socket = SOCKET.to_owned();
    if args.first().map(String::as_str) == Some("--socket") && args.len() > 1 {
        socket = args.remove(1);
        args.remove(0);
    }
    let usage = "cloudroom mac run [--cwd DIR] [--stdin] [--write] COMMAND
cloudroom mac pull MAC_PATH [VM_FOLDER]
cloudroom mac push VM_PATH [MAC_FOLDER]
cloudroom mac result JOB
cloudroom mac status
Runs on the user's paired Mac as the user, in their home folder by default.
Add --write when a command changes the Mac. Unless the user gives Full access,
other commands run read-only, and --write needs their approval.";
    let name = args.first().cloned().unwrap_or_default();
    match (name.as_str(), &args[1.min(args.len())..]) {
        ("run", rest) => {
            let (mut cwd, mut stdin, mut write, mut words) = (None, false, false, Vec::new());
            let mut rest = rest.iter();
            while let Some(word) = rest.next() {
                match word.as_str() {
                    "--cwd" if words.is_empty() => cwd = rest.next().cloned(),
                    "--stdin" if words.is_empty() => stdin = true,
                    "--write" if words.is_empty() => write = true,
                    _ => words.push(word.clone()),
                }
            }
            if words.is_empty() {
                return Err(io::Error::other(usage));
            }
            let mut input = Vec::new();
            if stdin {
                std::io::Read::read_to_end(&mut std::io::stdin(), &mut input)?;
            }
            let body =
                json!({"command":words.join(" "),"stdin":hex(&input),"cwd":cwd,"write":write});
            let value = request(&socket, "POST", "/jobs", Some(body)).await?;
            print_result(value).await
        }
        ("result", [job]) if job.bytes().all(|b| b.is_ascii_digit() || b == b'-') => {
            print_result(request(&socket, "GET", &format!("/jobs/{job}"), None).await?).await
        }
        ("status", []) => {
            println!("{}", request(&socket, "GET", "/status", None).await?);
            Ok(0)
        }
        ("pull", [source, rest @ ..]) if rest.len() <= 1 => {
            let target = PathBuf::from(rest.first().map_or(".", String::as_str));
            let archive = copy(&socket, format!(
                "p={}; cd -- \"$(dirname -- \"$p\")\" && COPYFILE_DISABLE=1 tar --no-xattrs -czf - -- \"$(basename -- \"$p\")\"",
                quote(source)), Vec::new(), false).await?;
            fs::create_dir_all(&target)?;
            let mut tar = tokio::process::Command::new("tar")
                .arg("-xzf")
                .arg("-")
                .arg("-C")
                .arg(&target)
                .stdin(Stdio::piped())
                .spawn()?;
            tar.stdin.take().unwrap().write_all(&archive).await?;
            if !tar.wait().await?.success() {
                return Err(io::Error::other("Could not unpack the copied files"));
            }
            println!("{}", json!({"copied":source,"to":target}));
            Ok(0)
        }
        ("push", [source, rest @ ..]) if rest.len() <= 1 => {
            let path = PathBuf::from(source);
            let (Some(parent), Some(file)) = (path.parent(), path.file_name()) else {
                return Err(io::Error::other("push needs a file or folder path"));
            };
            let parent = if parent.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                parent.to_owned()
            };
            let archive = tokio::process::Command::new("tar")
                .arg("-czf")
                .arg("-")
                .arg("-C")
                .arg(parent)
                .arg("--")
                .arg(file)
                .output()
                .await?;
            if !archive.status.success() || archive.stdout.len() > LIMIT {
                return Err(io::Error::other(
                    "Could not pack the files; the compressed limit is 16 MiB",
                ));
            }
            let target = rest.first().map_or("~", String::as_str);
            copy(
                &socket,
                format!(
                    "d={}; mkdir -p -- \"$d\" && tar -xzf - -C \"$d\"",
                    quote(target)
                ),
                archive.stdout,
                true,
            )
            .await?;
            println!("{}", json!({"copied":source,"to":target}));
            Ok(0)
        }
        _ => {
            println!("{usage}");
            Ok(if name.is_empty() || name == "--help" {
                0
            } else {
                2
            })
        }
    }
}

async fn print_result(value: Value) -> io::Result<i32> {
    let (code, stdout, stderr) = finished(value)?;
    use std::io::Write;
    std::io::stdout().write_all(&stdout)?;
    std::io::stdout().flush()?;
    std::io::stderr().write_all(&stderr)?;
    Ok(code.unwrap_or(1))
}
