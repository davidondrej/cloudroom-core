use super::{Adapter, Event, Handle, Kind, Progress, Resume, command as child_command};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, TryRecvError, sync_channel},
    time::Duration,
};
use tokio::process::Command;

const HISTORY: &str = include_str!("cursor-history.py");
const PLAIN_CHAT: &str = "Ask the user questions in plain chat. Do not use structured question or plan-approval tools. Never infer the user's answer or approval.";

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// What differs between ACP harnesses that share this protocol.
#[derive(Clone, Copy)]
pub(super) struct Flavor {
    pub harness: &'static str,
    pub name: &'static str,
    /// Where the agent keeps a session, from its home and session ID, when Core does not capture it.
    pub native_path: fn(&Path, &str) -> PathBuf,
    pub valid_id: fn(&str) -> bool,
    pub capture: bool,
    /// Diagnostics the agent sends as message text; never part of the reply.
    pub notices: &'static [&'static str],
    /// Records the history an agent replays on `session/load`. The agent already keeps it.
    pub replay: bool,
    /// Says why the agent ended a turn early, as Local threads do.
    pub explain_stops: bool,
}
pub(super) const CURSOR: Flavor = Flavor {
    harness: "cursor",
    name: "Cursor",
    native_path: |home, id| home.join("chats").join(id).join("meta.json"),
    valid_id,
    capture: true,
    notices: &[],
    replay: true,
    explain_stops: false,
};

pub(crate) fn capabilities(kind: Kind) -> Value {
    json!({"resume":true,"interrupt":true,"system_notice":false,"interactive_dialogs":false,
        "steer":true,"compact":false,"rewind":false,"attachments":true,
        "service_tier":kind.fast(),"subagents":false,"usage":false,"command_guard":false})
}

pub(super) fn command(config: &Config, profile: &HarnessConfig) -> io::Result<Command> {
    if profile.home.canonicalize()? != config.account_home.join(".cursor").canonicalize()? {
        return Err(invalid(
            "Cursor home must belong to the configured agent account",
        ));
    }
    let mut command = child_command(&std::env::current_exe()?, config);
    super::cursor_auth::apply_key(&mut command, config)?;
    command.arg("--cursor-driver").arg(&profile.binary);
    Ok(command)
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    let (id, session) = open(handle, "Cursor").await?;
    select_model(handle, &id, &session).await?;
    Ok(id)
}

pub(super) async fn open(handle: &Handle, name: &str) -> io::Result<(String, Value)> {
    initialize(handle, name).await?;
    let mut params = json!({"cwd":handle.repository,"mcpServers":[]});
    let method = if let Some(saved) = &handle.resume {
        params["sessionId"] = json!(saved.id);
        "session/load"
    } else {
        "session/new"
    };
    attach(handle, name, method, params).await
}

/// Returns the agent's capabilities and keeps them on the handle.
pub(super) async fn initialize(handle: &Handle, name: &str) -> io::Result<Value> {
    let init = handle
        .call(
            "initialize",
            json!({"protocolVersion":1,
        "clientInfo":{"name":"cloudroom","version":env!("CARGO_PKG_VERSION")},
        "clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false}}),
        )
        .await?;
    if init["protocolVersion"] != 1 || init["agentCapabilities"]["loadSession"] != true {
        return Err(io::Error::other(format!(
            "{name} ACP session loading is unavailable"
        )));
    }
    let capabilities = init["agentCapabilities"].clone();
    *handle.acp.lock().unwrap() = capabilities.clone();
    Ok(capabilities)
}

/// Creates, loads, or resumes the session and checks it is the saved one.
pub(super) async fn attach(
    handle: &Handle,
    name: &str,
    method: &str,
    params: Value,
) -> io::Result<(String, Value)> {
    let session = handle.call(method, params).await?;
    let id = handle.native()?;
    if handle.resume.as_ref().is_some_and(|saved| saved.id != id) {
        return Err(io::Error::other(format!(
            "{name} resumed a different session"
        )));
    }
    Ok((id, session))
}

async fn select_model(handle: &Handle, id: &str, session: &Value) -> io::Result<()> {
    let requested = match handle.profile.model.as_str() {
        "auto" => "default",
        value => value,
    };
    let catalog = session["models"]["availableModels"]
        .as_array()
        .ok_or_else(|| io::Error::other("Cursor model catalog unavailable"))?;
    let ids = catalog.iter().filter_map(|model| model["modelId"].as_str());
    let reasoning = handle.reasoning.as_deref();
    let selected = super::cursor_models::resolve(ids, requested, reasoning).ok_or_else(|| {
        invalid(&format!(
            "Cursor offers no {} variant of {requested} for this account",
            reasoning.unwrap_or("default")
        ))
    })?;
    let result = handle
        .call(
            "session/set_config_option",
            json!({"sessionId":id,"configId":"model","value":selected}),
        )
        .await?;
    if option_value(&result, "model") != Some(selected.as_str()) {
        return Err(io::Error::other(
            "Cursor did not confirm the requested model and reasoning",
        ));
    }
    Ok(())
}

pub(super) fn option_value<'a>(state: &'a Value, name: &str) -> Option<&'a str> {
    state["configOptions"]
        .as_array()?
        .iter()
        .find(|option| option["id"] == name)?["currentValue"]
        .as_str()
}

pub(super) async fn send(handle: &Handle, request: &str, input: &Value) -> io::Result<()> {
    if handle.kind == Kind::Cursor {
        // Fast is per turn: the driver runs the selected model's `-fast` variant when Cursor offers one.
        let fast = input["service_tier"] == "fast";
        handle
            .call(
                "session/set_config_option",
                json!({"sessionId":handle.native()?,"configId":"fast","value":fast.to_string()}),
            )
            .await?;
    }
    prompt(handle, request, input, false).await
}

/// Cloudroom's rules for the agent: plain-chat questions and the Cloudroom system prompt.
pub(super) fn instructions(handle: &Handle) -> String {
    match &handle.system_prompt {
        Some(system_prompt) => {
            format!("{PLAIN_CHAT}\n\n[Cloudroom system prompt]\n{system_prompt}")
        }
        None => PLAIN_CHAT.to_owned(),
    }
}

/// Sends one turn. Agents that do not keep the instructions for the session get them with every prompt.
pub(super) async fn prompt(
    handle: &Handle,
    request: &str,
    input: &Value,
    instructed: bool,
) -> io::Result<()> {
    let mut text = input["text"].as_str().unwrap_or_default().to_owned();
    if let Some(attachments) = input["attachments"].as_array() {
        for attachment in attachments {
            if let Some(path) = attachment["path"].as_str() {
                text.push_str(&format!("\n[Attached file: {path}]"));
            }
        }
    }
    if !instructed {
        text = format!("{}\n\n{text}", instructions(handle));
    }
    handle
        .process
        .dispatch(
            "session/prompt",
            json!({"sessionId":handle.native()?,"prompt":[{"type":"text","text":text}]}),
            Some(request),
            None,
        )
        .await
}

pub(super) async fn steer(handle: &Handle, request: &str, text: &str) -> io::Result<()> {
    let _control = handle.controls.lock().await;
    handle
        .process
        .control(
            "cloudroom/steer",
            json!({"sessionId":handle.native()?,
        "prompt":[{"type":"text","text":text}]}),
            request,
        )
        .await?;
    Ok(())
}

pub(super) async fn interrupt(handle: &Handle, state: Progress) -> io::Result<()> {
    let _control = handle.controls.lock().await;
    handle
        .process
        .control(
            "session/cancel",
            json!({"sessionId":handle.native()?}),
            state
                .request
                .as_deref()
                .ok_or_else(|| invalid("No active ACP turn"))?,
        )
        .await?;
    Ok(())
}

pub(super) fn checkpoint(
    previous: &Value,
    data: &Value,
    native: Option<&str>,
) -> io::Result<Value> {
    let snapshot = data["snapshot"]
        .as_str()
        .ok_or_else(|| io::Error::other("Missing Cursor snapshot identity"))?;
    let index = data["index"]
        .as_u64()
        .ok_or_else(|| io::Error::other("Missing Cursor snapshot offset"))?;
    if native.is_none()
        || index != 0
            && (previous["snapshot"] != snapshot || previous["index"].as_u64() != Some(index))
    {
        return Err(io::Error::other(
            "Cursor snapshot is incomplete or out of order",
        ));
    }
    Ok(json!({"snapshot":snapshot,"index":index + 1,"complete":data["complete"] == true}))
}

pub(super) fn validate(
    profile: &HarnessConfig,
    saved: &Resume,
    identity: super::files::Identity,
) -> io::Result<()> {
    chat_root(&profile.home, &saved.id, &saved.path)?;
    let file = super::files::open(&profile.home.join("chats"), &saved.path, identity)?;
    let value: Value = serde_json::from_reader(file.take(super::process::MAX_LINE as u64))?;
    if value["schemaVersion"] != 1 || !value["cwd"].is_string() {
        return Err(io::Error::other("Unsupported Cursor native metadata"));
    }
    Ok(())
}

/// Print-mode chats live at chats/<md5 of the working folder>/<chat ID>/meta.json.
fn chat_root(home: &Path, id: &str, path: &Path) -> io::Result<PathBuf> {
    let root = path.parent().and_then(Path::parent).filter(|root| {
        root.parent() == Some(home.join("chats").as_path())
            && root
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.len() == 32
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                })
    });
    match root {
        Some(root) if valid_id(id) && root.join(id).join("meta.json") == path => {
            Ok(root.to_owned())
        }
        _ => Err(io::Error::other(
            "Cursor native path does not match its session",
        )),
    }
}

pub(super) fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, byte)| {
            if [8, 13, 18, 23].contains(&i) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
            }
        })
}

pub(super) struct Protocol {
    flavor: Flavor,
    home: PathBuf,
    expected: Option<String>,
    opening: Option<u64>,
    prompt: Option<u64>,
    cancel: Option<u64>,
    restart: Option<(u64, Value)>,
    /// Steering prompts the agent joins to the running turn; each is answered when that turn ends.
    steering: Vec<u64>,
    acknowledgement: Option<(Value, u64)>,
    responses: Vec<Value>,
    closing: bool,
    tools: BTreeMap<String, Value>,
    capture: Capture,
    terminal: Option<(String, String, Option<String>)>,
    message: u64,
    text: String,
    thinking: String,
}
impl Protocol {
    pub fn new(
        config: &Config,
        profile: &HarnessConfig,
        resume: Option<&Resume>,
        flavor: Flavor,
    ) -> Self {
        Self {
            flavor,
            home: profile.home.clone(),
            expected: resume.map(|saved| saved.id.clone()),
            opening: None,
            prompt: None,
            cancel: None,
            restart: None,
            steering: Vec::new(),
            acknowledgement: None,
            responses: Vec::new(),
            closing: false,
            tools: BTreeMap::new(),
            capture: Capture::new(profile, config.storage.clone()),
            terminal: None,
            message: 0,
            text: String::new(),
            thinking: String::new(),
        }
    }
    fn record(&self, kind: &'static str, data: Value) -> Event {
        Event::Record {
            kind,
            data,
            native: None,
        }
    }
    fn finish_message(&mut self, state: &Progress, events: &mut Vec<Event>) {
        for (kind, text) in [
            ("agentMessage", std::mem::take(&mut self.text)),
            ("reasoning", std::mem::take(&mut self.thinking)),
        ] {
            if !text.is_empty() {
                events.push(self.record("item_completed", json!({"harness":self.flavor.harness,"request_id":state.request,
                    "item_id":format!("{}:{}:{}", self.flavor.harness, self.message, kind),"item_type":kind,"text":text})));
            }
        }
        self.message += 1;
    }
}
impl Adapter for Protocol {
    fn validate(&self, method: &str, _: &Value) -> io::Result<()> {
        if matches!(
            method,
            "cloudroom/steer" | "cloudroom/join" | "session/cancel"
        ) && (self.prompt.is_none() || self.restart.is_some() || self.cancel.is_some())
        {
            return Err(invalid(&format!(
                "{} turn has ended or an interrupt is already pending",
                self.flavor.name
            )));
        }
        Ok(())
    }
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value {
        if matches!(method, "session/new" | "session/load" | "session/resume") {
            self.opening = Some(id);
        }
        if request.is_some() {
            self.prompt = Some(id);
        }
        if method == "cloudroom/join" {
            self.steering.push(id);
            return json!({"jsonrpc":"2.0","id":id,"method":"session/prompt","params":params});
        }
        if method == "cloudroom/steer" {
            self.restart = Some((id, params.clone()));
            return json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":params["sessionId"]}});
        }
        if method == "session/cancel" {
            self.cancel = Some(id);
            return json!({"jsonrpc":"2.0","method":method,"params":params});
        }
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
    }
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        state: &mut Progress,
    ) -> io::Result<Vec<Event>> {
        self.responses.clear();
        self.acknowledgement = None;
        if !self.flavor.replay && self.opening.is_some() && value["method"] == "session/update" {
            return Ok(Vec::new());
        }
        if value.get("method").is_none()
            && let Some(at) = value["id"]
                .as_u64()
                .and_then(|id| self.steering.iter().position(|joined| *joined == id))
        {
            self.steering.remove(at);
        }
        let mut events = Vec::new();
        if value.get("method").is_none()
            && value["id"].as_u64() == self.opening
            && self.opening.is_some()
            && value.get("error").is_none()
        {
            self.opening = None;
            let id = value["result"]["sessionId"]
                .as_str()
                .or(self.expected.as_deref())
                .ok_or_else(|| {
                    io::Error::other(format!("Missing {} session identity", self.flavor.name))
                })?
                .to_owned();
            if !(self.flavor.valid_id)(&id)
                || self
                    .expected
                    .as_ref()
                    .is_some_and(|expected| expected != &id)
            {
                return Err(io::Error::other(format!(
                    "{} native identity changed",
                    self.flavor.name
                )));
            }
            let path = if self.flavor.capture {
                // The Cursor print driver reports where Cursor keeps this chat.
                let path = PathBuf::from(
                    value["result"]["path"]
                        .as_str()
                        .ok_or_else(|| io::Error::other("Missing Cursor chat location"))?,
                );
                self.capture.root = chat_root(&self.home, &id, &path)?;
                self.capture.id = Some(id.clone());
                path
            } else {
                (self.flavor.native_path)(&self.home, &id)
            };
            self.expected = Some(id.clone());
            state.native = Some(id.clone());
            events.push(self.record("native_identity", json!({"id":id,"path":path})));
        }
        if value["method"] == "session/update" && self.prompt.is_some() {
            if value["params"]["sessionId"].as_str() != state.native.as_deref() {
                return Err(io::Error::other(format!(
                    "{} event belongs to a different session",
                    self.flavor.name
                )));
            }
            let update = &value["params"]["update"];
            let kind = update["sessionUpdate"].as_str().unwrap_or("");
            if matches!(
                kind,
                "agent_message_chunk" | "agent_thought_chunk" | "tool_call" | "tool_call_update"
            ) {
                events.extend(state.started());
            }
            let text = update["content"]["text"].as_str().unwrap_or_default();
            let notice = kind == "agent_message_chunk"
                && self
                    .flavor
                    .notices
                    .iter()
                    .any(|prefix| text.starts_with(prefix));
            match kind {
                "agent_message_chunk" | "agent_thought_chunk" if !notice => {
                    let thinking = kind == "agent_thought_chunk";
                    let item_type = if thinking {
                        "reasoning"
                    } else {
                        "agentMessage"
                    };
                    let item_id = format!("{}:{}:{}", self.flavor.harness, self.message, item_type);
                    let empty = if thinking {
                        self.thinking.is_empty()
                    } else {
                        self.text.is_empty()
                    };
                    if empty {
                        events.push(self.record("item_started", json!({"harness":self.flavor.harness,"request_id":state.request,"item_id":item_id,"item_type":item_type})));
                    }
                    if thinking {
                        self.thinking.push_str(text);
                    } else {
                        self.text.push_str(text);
                    }
                    events.push(self.record(if thinking { "thinking_delta" } else { "text_delta" }, json!({"harness":self.flavor.harness,"request_id":state.request,"item_id":item_id,"delta":text})));
                }
                "tool_call" | "tool_call_update" => {
                    if kind == "tool_call" {
                        self.finish_message(state, &mut events);
                    }
                    let id = update["toolCallId"].as_str().ok_or_else(|| {
                        io::Error::other(format!("Missing {} tool identity", self.flavor.name))
                    })?;
                    let stored = self.tools.entry(id.into()).or_insert_with(|| json!({}));
                    if let Some(fields) = update.as_object() {
                        for (key, value) in fields {
                            stored[key] = value.clone();
                        }
                    }
                    let data = json!({"harness":self.flavor.harness,"request_id":state.request,"item_id":id,"tool":stored});
                    let complete = matches!(
                        update["status"].as_str(),
                        Some("completed" | "failed" | "cancelled")
                    );
                    events.push(self.record(
                        if complete {
                            "item_completed"
                        } else if kind == "tool_call" {
                            "item_started"
                        } else {
                            "tool_snapshot"
                        },
                        data,
                    ));
                    if complete {
                        self.capture.dirty = true;
                    }
                }
                _ => {}
            }
        }
        if value["method"] == "session/request_permission" {
            let option = value["params"]["options"]
                .as_array()
                .and_then(|options| options.iter().find(|option| option["kind"] == "allow_once"))
                .and_then(|option| option["optionId"].as_str());
            let outcome = if self.closing
                || self.cancel.is_some()
                || self.restart.is_some()
                || value["params"]["sessionId"].as_str() != state.native.as_deref()
            {
                json!({"outcome":"cancelled"})
            } else {
                option
                    .map(|id| json!({"outcome":"selected","optionId":id}))
                    .unwrap_or_else(|| json!({"outcome":"cancelled"}))
            };
            self.responses
                .push(json!({"jsonrpc":"2.0","id":value["id"],"result":{"outcome":outcome}}));
        } else if let Some(method) = value["method"]
            .as_str()
            .filter(|_| value.get("id").is_some())
        {
            let result = match method {
                "cursor/ask_question" => {
                    Some(json!({"outcome":{"outcome":"skipped","reason":PLAIN_CHAT}}))
                }
                "cursor/create_plan" => {
                    Some(json!({"outcome":{"outcome":"rejected","reason":PLAIN_CHAT}}))
                }
                "cursor/task" | "cursor/update_todos" | "cursor/generate_image" => Some(json!({})),
                _ => None,
            };
            self.responses.push(match result {
                Some(result) => json!({"jsonrpc":"2.0","id":value["id"],"result":result}),
                None => json!({"jsonrpc":"2.0","id":value["id"],"error":{"code":-32601,"message":"Client operation is unsupported"}}),
            });
        }
        if value.get("method").is_none()
            && self.prompt.is_some()
            && value["id"].as_u64() == self.prompt
        {
            self.finish_message(state, &mut events);
            self.capture.dirty = true;
            let stop = value["result"]["stopReason"].as_str();
            if value.get("error").is_none()
                && matches!(stop, Some("cancelled" | "end_turn"))
                && self.restart.is_some()
            {
                let (id, params) = self.restart.take().unwrap();
                self.responses.push(
                    json!({"jsonrpc":"2.0","id":id,"method":"session/prompt","params":params}),
                );
                self.acknowledgement = Some((value["id"].clone(), id));
                self.prompt = Some(id);
            } else if value.get("error").is_none()
                && stop == Some("end_turn")
                && !self.steering.is_empty()
            {
                // A steer that came in as the turn ended runs as the next turn; this turn waits for it.
                self.prompt = Some(self.steering.remove(0));
            } else {
                self.prompt = None;
                self.steering.clear();
                if let Some(id) = self
                    .cancel
                    .take()
                    .or_else(|| self.restart.take().map(|(id, _)| id))
                {
                    self.acknowledgement = Some((value["id"].clone(), id));
                }
                let status = match stop {
                    Some("end_turn") => "completed",
                    Some("cancelled") => "interrupted",
                    _ => "failed",
                };
                let error = (self.flavor.explain_stops && status == "failed").then(|| {
                    match (stop, value["error"]["message"].as_str()) {
                        (Some(stop), _) => format!("{} stopped the turn: {stop}", self.flavor.name),
                        (None, Some(message)) => {
                            format!("{} failed the turn: {message}", self.flavor.name)
                        }
                        (None, None) => {
                            format!("{} ended the turn without a reason", self.flavor.name)
                        }
                    }
                });
                if let Some(request) = state.request.clone() {
                    events.extend(state.started());
                    state.finished = true;
                    self.terminal = Some((request, status.into(), error));
                }
            }
        }
        if let Some(Event::Record { native, .. }) = events
            .iter_mut()
            .find(|event| matches!(event, Event::Record { .. }))
        {
            *native = Some(raw);
        } else {
            events.insert(
                0,
                Event::Record {
                    kind: "native_event",
                    data: json!({"harness":self.flavor.harness,"request_id":state.request}),
                    native: Some(raw),
                },
            );
        }
        Ok(events)
    }
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)> {
        if value.get("method").is_some() {
            return None;
        }
        let id = self
            .acknowledgement
            .as_ref()
            .filter(|(source, _)| source == &value["id"])
            .map(|(_, id)| *id)
            .or_else(|| value["id"].as_u64())?;
        Some((
            id,
            if let Some(error) = value.get("error") {
                Err(invalid(&format!(
                    "{} rejected the operation (code {}): {}",
                    self.flavor.name,
                    error["code"],
                    error["message"]
                        .as_str()
                        .unwrap_or("no error message returned")
                )))
            } else {
                Ok(value["result"].clone())
            },
        ))
    }
    fn respond(&self, _: &Value) -> Vec<Value> {
        self.responses.clone()
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        let mut events = self.capture.poll()?;
        if !self.capture.pending()
            && let Some((request, status, error)) = self.terminal.take()
        {
            events.push(Event::Finished {
                request,
                status,
                error,
            });
        }
        Ok(events)
    }
    fn capture_pending(&self) -> bool {
        self.capture.pending() || self.terminal.is_some()
    }
    fn close(&mut self) -> Vec<Value> {
        self.closing = true;
        self.restart = None;
        if self.prompt.is_some() {
            vec![
                json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":self.expected}}),
            ]
        } else {
            vec![]
        }
    }
    fn closed(&mut self, _: &Value) -> bool {
        self.closing && self.prompt.is_none()
    }
}

struct Capture {
    policy: Option<crate::workspace::storage::Policy>,
    root: PathBuf,
    id: Option<String>,
    dirty: bool,
    worker: Option<Receiver<io::Result<Event>>>,
}
impl Capture {
    fn new(profile: &HarnessConfig, policy: Option<crate::workspace::storage::Policy>) -> Self {
        Self {
            policy,
            root: profile.home.join("chats"),
            id: None,
            dirty: false,
            worker: None,
        }
    }
    fn pending(&self) -> bool {
        self.worker.is_some() || self.dirty && self.id.is_some()
    }
    fn poll(&mut self) -> io::Result<Vec<Event>> {
        let mut events = Vec::new();
        if let Some(worker) = &self.worker {
            loop {
                match worker.try_recv() {
                    Ok(event) => events.push(event?),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.worker = None;
                        break;
                    }
                }
            }
        }
        if self.worker.is_none()
            && self.dirty
            && let Some(id) = self.id.clone()
        {
            self.dirty = false;
            let mut command = Command::new("python3");
            command
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .current_dir("/")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            command
                .args(["-I", "-c", HISTORY, "capture"])
                .arg(&self.root)
                .arg(id);
            let group = self
                .policy
                .as_ref()
                .map(|policy| {
                    let group = super::linux::Workload::create(policy)?;
                    group.attach(&mut command, policy)?;
                    Ok::<_, io::Error>(group)
                })
                .transpose()?;
            let (sender, receiver) = sync_channel(8);
            self.worker = Some(receiver);
            std::thread::spawn(move || {
                let _group = group;
                let result = (|| {
                    let mut child = command.as_std_mut().spawn()?;
                    drop(child.stdin.take());
                    let stdout = child
                        .stdout
                        .take()
                        .ok_or_else(|| io::Error::other("Missing Cursor capture output"))?;
                    let result = (|| {
                        for line in BufReader::new(stdout).lines() {
                            let line = line?;
                            let value: Value = serde_json::from_str(&line)?;
                            let native = value["record"]
                                .as_str()
                                .ok_or_else(|| io::Error::other("Invalid Cursor snapshot"))?
                                .to_owned();
                            let event = Event::Record {
                                kind: "native_record",
                                data: json!({"harness":"cursor","snapshot":value["snapshot"],"index":value["index"],"complete":value["complete"]}),
                                native: Some(native + "\n"),
                            };
                            sender.send(Ok(event)).map_err(|_| {
                                io::Error::other("Cursor capture owner disconnected")
                            })?;
                        }
                        Ok::<_, io::Error>(())
                    })();
                    if result.is_err() {
                        let _ = child.kill();
                    }
                    let status = child.wait()?;
                    result?;
                    if !status.success() {
                        let mut error = String::new();
                        if let Some(stderr) = child.stderr.take() {
                            let _ = stderr.take(4096).read_to_string(&mut error);
                        }
                        return Err(io::Error::other(format!(
                            "Cursor native snapshot failed: {error}"
                        )));
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    let _ = sender.send(Err(error));
                }
            });
        }
        Ok(events)
    }
}

pub(super) fn recover(
    profile: &HarnessConfig,
    saved: &Resume,
    policy: Option<&crate::workspace::storage::Policy>,
    mut emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    validate(
        profile,
        saved,
        policy.map(|policy| (policy.agent_uid, policy.agent_gid)),
    )?;
    let mut capture = Capture::new(profile, policy.cloned());
    capture.root = chat_root(&profile.home, &saved.id, &saved.path)?;
    capture.id = Some(saved.id.clone());
    capture.dirty = true;
    while capture.pending() {
        for event in capture.poll()? {
            emit(event)?;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}
