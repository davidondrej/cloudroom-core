//! Cloud agents rename or archive their own thread, change its effort, and start, stop and archive child threads in their own sandbox. Each request
//! is a session record, so the app applies it whenever it next reads the thread, including after it was offline.
use super::Manager;
use crate::preview::Peer;
use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::{self, Read},
    sync::Arc,
};

const USAGE: &str = "cloudroom thread update --self --title TITLE
cloudroom thread update --self --reasoning-level LEVEL
cloudroom thread archive --self
cloudroom thread stop --self
cloudroom thread spawn --provider codex|claude-code|pi|opencode [--model MODEL] [--reasoning-level LEVEL] [--title TITLE] --prompt TEXT|--prompt-file PATH
cloudroom thread list [--include-archived]
cloudroom thread output CHILD_ID
cloudroom thread tell CHILD_ID TEXT
cloudroom thread stop CHILD_ID
cloudroom thread archive CHILD_ID";
const SPAWN_NOTE: &str = "Started in this sandbox and folder. Keep working: Cloudroom sends you a message each time it finishes a turn.";
pub(crate) type Failure = (StatusCode, Json<Value>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rename {
    title: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnBody {
    request_id: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    reasoning: Option<String>,
    title: Option<String>,
    prompt: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Effort {
    reasoning: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tell {
    text: String,
}

fn valid(title: &str) -> bool {
    !title.is_empty() && title.chars().count() <= 200 && !title.chars().any(char::is_control)
}
/// Served on the agent-only socket that `cloudroom mac` also uses.
pub(crate) fn agent_routes() -> Router<Arc<Manager>> {
    Router::new()
        .route("/title", post(rename))
        .route("/reasoning", post(set_reasoning))
        .route("/archive", post(archive))
        .route("/children", get(list).post(spawn))
        .route("/children/{id}", get(output))
        .route("/children/{id}/messages", post(tell))
        .route("/children/{id}/stop", post(stop_child))
        .route("/children/{id}/archive", post(archive_child))
}

/// The session whose harness process sent this `cloudroom <command>` request.
pub(crate) fn caller(m: &Manager, peer: Peer, command: &str) -> Result<String, Failure> {
    if peer.0.is_none() || m.previews.agent().map(|(_, uid)| uid) != peer.0 {
        return Err(fail(
            StatusCode::FORBIDDEN,
            &format!("Only the VM agent account may run cloudroom {command}"),
        ));
    }
    peer.1
        .and_then(|pid| m.harness_session(&crate::secrets::ancestors(pid)))
        .ok_or(fail(
            StatusCode::CONFLICT,
            &format!("Run cloudroom {command} from a Cloudroom cloud thread"),
        ))
}

pub(crate) fn fail(status: StatusCode, error: &str) -> Failure {
    (status, Json(json!({"error":error})))
}

fn record(m: &Manager, session: &str, kind: &str, data: Value) -> Result<Json<Value>, Failure> {
    m.note(session, kind, data.clone()).map_err(|error| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("Could not record the {kind}: {error}"),
        )
    })?;
    Ok(Json(data))
}

async fn rename(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(input): Json<Rename>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer, "thread")?;
    let title = input.title.trim();
    if !valid(title) {
        return Err(fail(
            StatusCode::CONFLICT,
            "Use a title of 1-200 characters",
        ));
    }
    record(&m, &session, "title", json!({"title":title}))
}

/// The new effort applies from this session's next turn; the app shows it as the thread's effort.
async fn set_reasoning(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(input): Json<Effort>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer, "thread")?;
    m.check_prompt_reasoning(&session, Some(&input.reasoning))
        .await
        .map_err(session_failure)?;
    record(
        &m,
        &session,
        "reasoning",
        json!({"reasoning":input.reasoning}),
    )
}

/// The app archives the thread and its children, which also stops this session.
async fn archive(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer, "thread")?;
    record(&m, &session, "archive", json!({"archived":true}))
}

fn session_failure(error: super::Error) -> Failure {
    let status = match error {
        super::Error::NotFound => StatusCode::NOT_FOUND,
        super::Error::Conflict(_) => StatusCode::CONFLICT,
        super::Error::Storage(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    fail(status, &error.message())
}

fn random_key() -> io::Result<String> {
    let mut bytes = [0u8; 12];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// The calling thread starts a child in its own sandbox; Core tells the parent when each child turn ends.
async fn spawn(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(input): Json<SpawnBody>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread spawn")?;
    let bad = |message: &str| fail(StatusCode::CONFLICT, message);
    if input.prompt.trim().is_empty() || input.prompt.len() > 32768 {
        return Err(bad("Use a prompt of 1-32768 bytes"));
    }
    if input.title.as_deref().is_some_and(|t| !valid(t.trim())) {
        return Err(bad("Use a title of 1-200 characters"));
    }
    if input
        .model
        .as_ref()
        .is_some_and(|m| m.is_empty() || m.len() > 256 || m.chars().any(char::is_control))
    {
        return Err(bad("invalid model"));
    }
    if input
        .reasoning
        .as_ref()
        .is_some_and(|r| r.is_empty() || r.len() > 64 || !r.bytes().all(|b| b.is_ascii_lowercase()))
    {
        return Err(bad("invalid reasoning level"));
    }
    let harness = input
        .provider
        .map(|p| {
            serde_json::from_value(json!(p))
                .map_err(|_| bad("Use --provider codex, claude-code, pi or opencode"))
        })
        .transpose()?;
    let key = match input.request_id {
        Some(key) => key,
        None => random_key().map_err(|e| fail(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?,
    };
    if key.is_empty()
        || key.len() > 64
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(bad(
            "request_id must be 1-64 ASCII letters, digits, underscores or hyphens",
        ));
    }
    let spawn = super::Spawn {
        harness,
        model: input.model,
        reasoning: input.reasoning,
        title: input.title.map(|t| t.trim().to_owned()),
        prompt: input.prompt,
    };
    let (id, receipt) = m
        .spawn_child(&parent, &key, spawn)
        .await
        .map_err(session_failure)?;
    Ok(Json(
        json!({"id":id,"harness":receipt.input["harness"],"model":receipt.model,"note":SPAWN_NOTE}),
    ))
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    include_archived: bool,
}

async fn list(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread list")?;
    Ok(Json(
        m.children(&parent, None, query.include_archived).await,
    ))
}

async fn output(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread output")?;
    m.own_child(&parent, &id).map_err(session_failure)?;
    Ok(Json(
        m.children(&parent, Some(&id), true).await["children"][0].clone(),
    ))
}

async fn tell(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Path(id): Path<String>,
    Json(input): Json<Tell>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread tell")?;
    m.own_child(&parent, &id).map_err(session_failure)?;
    if input.text.trim().is_empty() || input.text.len() > 32768 {
        return Err(fail(StatusCode::CONFLICT, "Use a message of 1-32768 bytes"));
    }
    let request =
        random_key().map_err(|e| fail(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    let receipt = m
        .command(&id, request, "prompt", json!({"text":input.text}))
        .map_err(session_failure)?;
    Ok(Json(
        json!({"id":id,"state":receipt.state,"note":"Queued. Cloudroom tells you when the child finishes it."}),
    ))
}

/// Stops the child's running turn and pauses its queue, like the app's stop button.
async fn stop_child(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread stop")?;
    m.own_child(&parent, &id).map_err(session_failure)?;
    let request =
        random_key().map_err(|e| fail(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    let receipt = m
        .command(&id, request, "stop", json!({}))
        .map_err(session_failure)?;
    Ok(Json(json!({"id":id,"state":receipt.state})))
}

/// Stops the child now; the app archives it, and its own children, whenever it next reads the thread.
async fn archive_child(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    let parent = caller(&m, peer, "thread archive")?;
    m.own_child(&parent, &id).map_err(session_failure)?;
    let request =
        random_key().map_err(|e| fail(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    // A child that already ended has nothing to stop.
    let _ = m.command(&id, request, "stop", json!({}));
    record(&m, &id, "archive", json!({"id":id,"archived":true}))
}

/// Flag value after `name`, for `cloudroom thread spawn`.
fn flag<'a>(words: &[&'a str], name: &str) -> Option<&'a str> {
    words
        .iter()
        .position(|w| *w == name)
        .and_then(|i| words.get(i + 1).copied())
}

async fn spawn_cli(words: &[&str]) -> io::Result<i32> {
    // Local habits are harmless here: a child always runs in this cloud thread's sandbox.
    if flag(words, "--machine")
        .or(flag(words, "--host"))
        .is_some_and(|m| m != "cloud")
    {
        eprintln!("Cloud threads start cloud children only, in this sandbox. Omit --machine.");
        return Ok(2);
    }
    let prompt = match (flag(words, "--prompt"), flag(words, "--prompt-file")) {
        (Some(text), None) => text.to_owned(),
        (None, Some(path)) => std::fs::read_to_string(path)?,
        _ => {
            println!("{USAGE}");
            return Ok(2);
        }
    };
    let mut body = json!({"prompt":prompt});
    for (name, key) in [
        ("--provider", "provider"),
        ("--model", "model"),
        ("--reasoning-level", "reasoning"),
        ("--title", "title"),
        ("--request-id", "request_id"),
    ] {
        if let Some(value) = flag(words, name) {
            body[key] = json!(value);
        }
    }
    let answer = crate::mac::request(crate::mac::SOCKET, "POST", "/children", Some(body)).await?;
    println!("{answer}");
    Ok(0)
}

/// The same `cloudroom thread ... --self` forms local threads use. `--json` is accepted and
/// ignored because the answer is always JSON.
pub async fn cli(args: &[String]) -> io::Result<i32> {
    let words: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|word| *word != "--json")
        .collect();
    let help = words.iter().any(|word| matches!(*word, "--help" | "-h"));
    let (path, body) = match words.as_slice() {
        _ if help => {
            println!("{USAGE}");
            return Ok(0);
        }
        ["spawn", rest @ ..] => return spawn_cli(rest).await,
        ["list", rest @ ..] => {
            let path = if rest.contains(&"--include-archived") {
                "/children?include_archived=true"
            } else {
                "/children"
            };
            let answer = crate::mac::request(crate::mac::SOCKET, "GET", path, None).await?;
            println!("{answer}");
            return Ok(0);
        }
        ["output", id] => {
            let answer =
                crate::mac::request(crate::mac::SOCKET, "GET", &format!("/children/{id}"), None)
                    .await?;
            println!("{answer}");
            return Ok(0);
        }
        ["tell", id, text] | ["tell", id, "--prompt", text] => {
            let answer = crate::mac::request(
                crate::mac::SOCKET,
                "POST",
                &format!("/children/{id}/messages"),
                Some(json!({"text":text})),
            )
            .await?;
            println!("{answer}");
            return Ok(0);
        }
        ["update", "--self", "--title", title] | ["update", "--title", title, "--self"] => {
            ("/title", Some(json!({"title":title})))
        }
        ["update", "--self", "--reasoning-level", level]
        | ["update", "--reasoning-level", level, "--self"] => {
            ("/reasoning", Some(json!({"reasoning":level})))
        }
        ["archive", "--self"] => ("/archive", None),
        ["stop", "--self"] => {
            println!(
                "{}",
                json!({"stopped":true,"note":"Archiving a cloud thread stops it; nothing else to do."})
            );
            return Ok(0);
        }
        [action @ ("stop" | "archive"), id] if !id.starts_with('-') => {
            let answer = crate::mac::request(
                crate::mac::SOCKET,
                "POST",
                &format!("/children/{id}/{action}"),
                None,
            )
            .await?;
            println!("{answer}");
            return Ok(0);
        }
        _ => {
            println!(
                "{USAGE}\nRenames, archives or changes the effort of this cloud thread, or starts, checks, stops and archives child threads in this sandbox."
            );
            return Ok(if words.is_empty() { 0 } else { 2 });
        }
    };
    let answer = crate::mac::request(crate::mac::SOCKET, "POST", path, body).await?;
    println!("{answer}");
    Ok(0)
}
