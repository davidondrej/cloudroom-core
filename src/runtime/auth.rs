use super::{Adapter, Event, Kind, Progress, codex, process::Process};
use crate::{config::Config, workspace::storage::Guard};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io,
    process::Stdio,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{Mutex as AsyncMutex, watch},
};

const LOGIN_LIFETIME: Duration = Duration::from_secs(10 * 60);

fn credentials(value: Value) -> Option<Value> {
    if value.get("auth_mode").is_some_and(|mode| mode != "chatgpt")
        || value
            .get("OPENAI_API_KEY")
            .is_some_and(|key| !key.is_null())
    {
        return None;
    }
    let mut tokens = serde_json::Map::new();
    for key in ["id_token", "access_token", "refresh_token", "account_id"] {
        let token = value["tokens"][key].as_str().filter(|s| !s.is_empty())?;
        tokens.insert(key.into(), json!(token));
    }
    let mut result = json!({"auth_mode":"chatgpt", "OPENAI_API_KEY":null, "tokens":tokens});
    if let Some(refreshed) = value["last_refresh"].as_str() {
        result["last_refresh"] = json!(refreshed);
    }
    Some(result)
}

async fn import_file(config: &Config, storage: &Guard, credentials: Value) -> io::Result<()> {
    let home = &config
        .harnesses
        .get(&Kind::Codex)
        .ok_or_else(|| io::Error::other("Codex is not configured"))?
        .home;
    let mut command = Command::new("python3");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .args(["-c", include_str!("../sync/files.py")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let (mut child, _workload) = storage.spawn_writer(&mut command)?;
    let mut stdin = child.stdin.take().unwrap();
    // Credentials only travel over private stdin, never process arguments or diagnostic records.
    stdin.write_all(&serde_json::to_vec(&json!({"op":"import_auth", "tree":{"root":home,"kind":"auth","filename":"auth.json","create":true}, "credentials":credentials}))?).await?;
    stdin.write_all(b"\n").await?;
    drop(stdin);
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await??;
    if !output.status.success() || serde_json::from_slice::<Value>(&output.stdout)?["ok"] != true {
        return Err(io::Error::other("Codex login import failed"));
    }
    Ok(())
}

#[derive(Clone, Serialize)]
pub struct Status {
    pub state: &'static str,
    pub email: Option<String>,
    pub plan: Option<String>,
    pub message: Option<&'static str>,
    pub login_id: Option<String>,
    pub verification_url: Option<String>,
    pub user_code: Option<String>,
}
impl Status {
    pub(super) fn new(state: &'static str, message: Option<&'static str>) -> Self {
        Self {
            state,
            message,
            email: None,
            plan: None,
            login_id: None,
            verification_url: None,
            user_code: None,
        }
    }
}

// Login RPC traffic never enters session history or diagnostics.
struct Protocol;
impl Adapter for Protocol {
    fn encode(&mut self, id: u64, method: &str, params: Value, _: Option<&str>) -> Value {
        json!({"id":id,"method":method,"params":params})
    }
    fn receive(&mut self, value: &Value, _: String, _: &mut Progress) -> io::Result<Vec<Event>> {
        if value["method"] == "account/login/completed" {
            Ok(vec![Event::Record {
                kind: "login_completed",
                data: json!({"id":value["params"]["loginId"],"success":value["params"]["success"]}),
                native: None,
            }])
        } else {
            Ok(Vec::new())
        }
    }
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)> {
        if value.get("method").is_some() {
            return None;
        }
        let id = value["id"].as_u64()?;
        let result = if let Some(error) = value.get("error") {
            let message = error["message"].as_str().unwrap_or("").to_ascii_lowercase();
            let auth = [
                "401",
                "unauthorized",
                "not logged in",
                "not authenticated",
                "refresh token",
                "authentication token",
            ]
            .iter()
            .any(|s| message.contains(s));
            Err(io::Error::new(
                if auth {
                    io::ErrorKind::PermissionDenied
                } else {
                    io::ErrorKind::Other
                },
                "Codex account check failed",
            ))
        } else {
            Ok(value["result"].clone())
        };
        Some((id, result))
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        Ok(Vec::new())
    }
}

struct Rpc {
    process: Process,
    completed: watch::Receiver<Option<Value>>,
    /// Ends once the app-server has exited.
    exited: tokio::task::JoinHandle<()>,
}
impl Rpc {
    async fn open(config: &Config) -> io::Result<Self> {
        let profile = config
            .harnesses
            .get(&Kind::Codex)
            .ok_or_else(|| io::Error::other("Codex is not configured"))?;
        let (process, mut events) =
            Process::spawn(config, codex::command(config, profile), Box::new(Protocol))?;
        let (changed, completed) = watch::channel(None);
        let exited = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::Record {
                        kind: "login_completed",
                        data,
                        ..
                    } => {
                        changed.send_replace(Some(data));
                    }
                    Event::Exited { .. } => break,
                    _ => {}
                }
            }
        });
        let rpc = Self {
            process,
            completed,
            exited,
        };
        let ready = async {
            rpc.call(
                "initialize",
                json!({"clientInfo":{"name":"cloudroom","version":env!("CARGO_PKG_VERSION")}}),
            )
            .await?;
            rpc.process.notify("initialized").await
        }
        .await;
        match ready {
            Ok(()) => Ok(rpc),
            Err(error) => {
                rpc.close().await;
                Err(error)
            }
        }
    }
    /// Stops the app-server and waits for it to exit, so a quick service stop cannot leave it running.
    async fn close(mut self) {
        self.process.request_shutdown();
        let _ = (&mut self.exited).await;
    }
    async fn call(&self, method: &str, params: Value) -> io::Result<Value> {
        self.process
            .call_timeout(method, params, None, Duration::from_secs(10))
            .await
    }
    async fn probe(&self) -> io::Result<Status> {
        let result = self
            .call("account/read", json!({"refreshToken":false}))
            .await?;
        let account = &result["account"];
        if account.is_null() {
            return if result["requiresOpenaiAuth"] == false {
                Ok(Status::new(
                    "connected",
                    Some("Using the configured inference provider."),
                ))
            } else if result["requiresOpenaiAuth"] == true {
                Ok(Status::new(
                    "missing",
                    Some("Connect your ChatGPT subscription to use Codex in Cloud."),
                ))
            } else {
                Err(io::Error::other("Invalid account response"))
            };
        }
        if account["type"] != "chatgpt" {
            return Ok(Status::new(
                "connected",
                Some("Using the configured inference provider."),
            ));
        }
        let limits = self.call("account/rateLimits/read", json!({})).await?;
        let limit = limits
            .get("rateLimits")
            .ok_or_else(|| io::Error::other("Missing live account limits"))?;
        // Purchased credits keep Codex working past the plan limit.
        let credits = &limit["credits"];
        let exhausted = credits["hasCredits"] != true
            && credits["unlimited"] != true
            && ["primary", "secondary"].iter().any(|key| {
                limit[*key]["usedPercent"]
                    .as_f64()
                    .is_some_and(|n| n >= 100.0)
            });
        let mut status = Status::new(
            if exhausted { "limited" } else { "connected" },
            if exhausted {
                Some(
                    "Your Codex usage limit is reached. Sign in with another ChatGPT account to keep working, or wait for it to reset.",
                )
            } else {
                None
            },
        );
        status.email = account["email"].as_str().map(str::to_owned);
        status.plan = account["planType"].as_str().map(str::to_owned);
        Ok(status)
    }
}
impl Drop for Rpc {
    fn drop(&mut self) {
        self.process.request_shutdown();
    }
}

struct Login {
    request: String,
    native_id: String,
    started: Instant,
    rpc: Rpc,
}
struct State {
    status: Status,
    checked: Option<Instant>,
    login: Option<Login>,
    // A completed sign-in the session manager has not yet acted on.
    switched: bool,
}
pub struct CodexAuth {
    state: AsyncMutex<State>,
    process: Mutex<Option<Process>>,
}
impl Default for CodexAuth {
    fn default() -> Self {
        Self {
            state: AsyncMutex::new(State {
                status: Status::new("missing", None),
                checked: None,
                login: None,
                switched: false,
            }),
            process: Mutex::new(None),
        }
    }
}
fn failure(error: io::Error) -> Status {
    if error.kind() == io::ErrorKind::PermissionDenied {
        Status::new(
            "missing",
            Some("Your Codex login is missing or expired. Sign in again."),
        )
    } else {
        Status::new(
            "unavailable",
            Some("Could not verify Codex. Check the connection and try again."),
        )
    }
}
impl CodexAuth {
    async fn inspect(state: &mut State, config: &Config, refresh: bool) -> Status {
        if let Some(login) = &state.login {
            let completed = login.rpc.completed.borrow().clone();
            let expired = login.started.elapsed() >= LOGIN_LIFETIME;
            let disconnected = login.rpc.completed.has_changed().is_err();
            if let Some(result) = completed.filter(|v| v["id"].as_str() == Some(&login.native_id)) {
                state.status = if result["success"] == true {
                    let status = login.rpc.probe().await.unwrap_or_else(failure);
                    state.switched = status.state == "connected";
                    status
                } else {
                    Status::new("error", Some("Sign-in was not completed. Try again."))
                };
            } else if expired || disconnected {
                let _ = login
                    .rpc
                    .call("account/login/cancel", json!({"loginId":login.native_id}))
                    .await;
                state.status = Status::new("expired", Some("Sign-in expired. Try again."));
            } else {
                return state.status.clone();
            }
            if let Some(login) = state.login.take() {
                login.rpc.close().await;
            }
            state.checked = Some(Instant::now());
        } else if refresh
            || state
                .checked
                .is_none_or(|at| at.elapsed() > Duration::from_secs(15))
        {
            state.status = match Rpc::open(config).await {
                Ok(rpc) => {
                    let status = rpc.probe().await.unwrap_or_else(failure);
                    rpc.close().await;
                    status
                }
                Err(error) => failure(error),
            };
            state.checked = Some(Instant::now());
        }
        state.status.clone()
    }
    pub async fn status(&self, config: &Config) -> Status {
        Self::inspect(&mut *self.state.lock().await, config, false).await
    }
    pub async fn verify(&self, config: &Config) -> Status {
        Self::inspect(&mut *self.state.lock().await, config, true).await
    }
    pub async fn take_switched(&self) -> bool {
        std::mem::take(&mut self.state.lock().await.switched)
    }
    pub(crate) async fn import(&self, config: &Config, storage: &Guard, value: Value) -> Status {
        let mut state = self.state.lock().await;
        let current = Self::inspect(&mut state, config, true).await;
        if state.login.is_some() || !matches!(current.state, "missing" | "error" | "expired") {
            return current;
        }
        let Some(credentials) = credentials(value) else {
            return Status::new(
                "error",
                Some("Saved Codex login is invalid. Sign in with ChatGPT."),
            );
        };
        match import_file(config, storage, credentials).await {
            Ok(_) => {
                let status = Self::inspect(&mut state, config, true).await;
                state.switched = status.state == "connected";
                status
            }
            Err(_) => Status::new(
                "unavailable",
                Some("Could not copy the saved Codex login. Check cloud storage and try again."),
            ),
        }
    }
    pub async fn login(&self, config: &Config, request: String) -> Status {
        let mut state = self.state.lock().await;
        let current = Self::inspect(&mut state, config, true).await;
        if state.login.is_some() || matches!(current.state, "connected" | "unavailable") {
            return current;
        }
        let attempt = async {
            let rpc = Rpc::open(config).await?;
            let result = rpc
                .call("account/login/start", json!({"type":"chatgptDeviceCode"}))
                .await?;
            let id = result["loginId"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| io::Error::other("Missing login ID"))?
                .to_owned();
            let url = result["verificationUrl"]
                .as_str()
                .filter(|v| *v == "https://auth.openai.com/codex/device")
                .ok_or_else(|| io::Error::other("Unexpected login URL"))?
                .to_owned();
            let code = result["userCode"]
                .as_str()
                .filter(|v| {
                    !v.is_empty()
                        && v.len() <= 32
                        && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
                .ok_or_else(|| io::Error::other("Invalid login code"))?
                .to_owned();
            Ok::<_, io::Error>((rpc, id, url, code))
        }
        .await;
        match attempt {
            Ok((rpc, native_id, url, code)) => {
                state.status = Status {
                    login_id: Some(request.clone()),
                    verification_url: Some(url),
                    user_code: Some(code),
                    ..Status::new(
                        "waiting",
                        Some("Finish signing in with OpenAI, then return here."),
                    )
                };
                *self.process.lock().unwrap() = Some(rpc.process.clone());
                let expiry = rpc.process.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(LOGIN_LIFETIME).await;
                    expiry.request_shutdown();
                });
                state.login = Some(Login {
                    request,
                    native_id,
                    started: Instant::now(),
                    rpc,
                });
            }
            Err(_) => {
                state.status = Status::new(
                    "error",
                    Some(
                        "Could not start sign-in. Enable device-code login in ChatGPT security settings and try again.",
                    ),
                );
            }
        }
        state.status.clone()
    }
    pub async fn cancel(&self, request: &str) -> Status {
        let mut state = self.state.lock().await;
        if let Some(login) = &state.login {
            if login.request != request {
                return state.status.clone();
            }
            let _ = login
                .rpc
                .call("account/login/cancel", json!({"loginId":login.native_id}))
                .await;
            if let Some(login) = state.login.take() {
                login.rpc.close().await;
            }
            state.checked = None;
            state.status = Status::new(
                "missing",
                Some("Sign-in cancelled. Your task has not been sent."),
            );
        }
        state.status.clone()
    }
    /// Also waits for a pending sign-in's app-server to exit, so a quick service stop cannot leave it running.
    pub async fn shutdown(&self) {
        if let Some(process) = self.process.lock().unwrap().take() {
            process.request_shutdown();
        }
        let _ = tokio::time::timeout(super::SHUTDOWN_GRACE, async {
            let login = self.state.lock().await.login.take();
            if let Some(login) = login {
                login.rpc.close().await;
            }
        })
        .await;
    }
}
