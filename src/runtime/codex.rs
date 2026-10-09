use super::{
    Adapter, Event, Handle, Progress, Resume, command as child_command, process::MAX_LINE,
};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

pub(super) fn command(config: &Config, profile: &HarnessConfig) -> Command {
    let mut command = child_command(&profile.binary, config);
    command
        .env("CODEX_HOME", &profile.home)
        .args(["app-server", "--listen", "stdio://"]);
    command
}

async fn initialize(handle: &Handle) -> io::Result<()> {
    handle.call("initialize",json!({"clientInfo":{"name":"cloudroom","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
    handle.process.notify("initialized").await
}

pub(super) async fn models(handle: &Handle) -> io::Result<Vec<super::Model>> {
    initialize(handle).await?;
    let mut models = Vec::new();
    let mut cursor = Value::Null;
    loop {
        let page = handle
            .call("model/list", json!({"includeHidden":true,"cursor":cursor}))
            .await?;
        let entries = page["data"]
            .as_array()
            .ok_or_else(|| io::Error::other("missing model catalog"))?;
        for entry in entries {
            let model = entry["model"]
                .as_str()
                .ok_or_else(|| io::Error::other("missing model name"))?;
            let levels = entry["supportedReasoningEfforts"]
                .as_array()
                .ok_or_else(|| io::Error::other("missing reasoning levels"))?;
            let reasoning_levels = levels
                .iter()
                .map(|level| {
                    level["reasoningEffort"]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| io::Error::other("invalid reasoning level"))
                })
                .collect::<io::Result<Vec<_>>>()?;
            models.push(super::Model {
                model: model.into(),
                reasoning_levels,
            });
        }
        let next = page["nextCursor"].clone();
        if next.is_null() {
            return Ok(models);
        }
        if !next.is_string() || next == cursor {
            return Err(io::Error::other("invalid model cursor"));
        }
        cursor = next;
    }
}

/// Starts, resumes, or (for a side chat) forks the session's thread. A fork only reads its source.
pub(super) async fn start(handle: &Handle, fork: Option<&super::Fork>) -> io::Result<String> {
    initialize(handle).await?;
    let mut params = json!({"cwd":handle.repository,"model":handle.profile.model,"approvalPolicy":"never","sandbox":"danger-full-access","ephemeral":false});
    if let Some(reasoning) = &handle.reasoning {
        params["config"] = json!({"model_reasoning_effort":reasoning});
    }
    if let Some(system_prompt) = &handle.system_prompt {
        params["developerInstructions"] = json!(system_prompt);
    }
    if let Some(turn) = fork.and_then(|f| f.last_turn_id.as_ref()) {
        params["lastTurnId"] = json!(turn);
    }
    let method = if let Some(saved) = fork.map(|f| &f.source).or(handle.resume.as_ref()) {
        params["threadId"] = json!(saved.id);
        params["path"] = json!(saved.path);
        params["excludeTurns"] = json!(true);
        if fork.is_some() {
            "thread/fork"
        } else {
            "thread/resume"
        }
    } else {
        "thread/start"
    };
    let result = handle.call(method, params).await?;
    if result["model"].as_str() != Some(&handle.profile.model) {
        return Err(io::Error::other("harness selected a different model"));
    }
    let id = result
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("missing native identity"))?;
    if handle.resume.as_ref().is_some_and(|s| s.id != id) {
        return Err(io::Error::other("harness resumed a different session"));
    }
    if fork.is_some_and(|f| f.source.id == id) {
        return Err(io::Error::other("harness did not fork the session"));
    }
    Ok(id.into())
}
fn prompt_items(input: &Value) -> Vec<Value> {
    let mut items = Vec::new();
    if let Some(content) = input["content"].as_array() {
        for part in content {
            match part["type"].as_str() {
                Some("text") => items.push(json!({
                    "type":"text",
                    "text":part["text"].as_str().unwrap_or(""),
                    "text_elements":[]
                })),
                Some("image") | Some("localImage") => {
                    if let Some(path) = part["path"].as_str().or_else(|| part["url"].as_str()) {
                        items.push(json!({"type":"localImage","path":path}));
                    }
                }
                Some("localFile") | Some("file") => {
                    if let Some(path) = part["path"].as_str() {
                        items.push(json!({
                            "type":"text",
                            "text":format!("[Attached file: {path}]"),
                            "text_elements":[]
                        }));
                    }
                }
                _ => {}
            }
        }
    }
    if items.is_empty() {
        items.push(json!({
            "type":"text",
            "text":input["text"].as_str().unwrap_or(""),
            "text_elements":[]
        }));
    }
    if let Some(attachments) = input["attachments"].as_array() {
        for attachment in attachments {
            let Some(path) = attachment["path"].as_str() else {
                continue;
            };
            if attachment["kind"] == "image" {
                items.push(json!({"type":"localImage","path":path}));
            } else {
                items.push(json!({
                    "type":"text",
                    "text":format!("[Attached file: {path}]"),
                    "text_elements":[]
                }));
            }
        }
    }
    items
}

fn service_tier_for_turn(input: &Value) -> &'static str {
    match input["service_tier"].as_str() {
        Some("fast") => "fast",
        _ => "default",
    }
}

pub(super) async fn send(handle: &Handle, request: &str, input: &Value) -> io::Result<()> {
    let mut params = json!({
        "threadId":handle.native()?,
        "clientUserMessageId":request,
        "input":prompt_items(input),
        "serviceTierForTurn":service_tier_for_turn(input)
    });
    if let Some(reasoning) = handle
        .reasoning
        .as_deref()
        .or_else(|| input["reasoning"].as_str())
    {
        params["effort"] = json!(reasoning);
    }
    let result = handle
        .process
        .call("turn/start", params, Some(request))
        .await?;
    result
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .map(|_| ())
        .ok_or_else(|| io::Error::other("missing native turn identity; outcome uncertain"))
}

pub(super) async fn steer(handle: &Handle, state: Progress, text: &str) -> io::Result<()> {
    let turn = state
        .native_turn
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "turn has not started"))?;
    let native = handle.native()?;
    handle
        .process
        .control(
            "turn/steer",
            json!({
                "threadId":native,
                "expectedTurnId":turn,
                "input":[{"type":"text","text":text,"text_elements":[]}]
            }),
            state.request.as_deref().unwrap(),
        )
        .await?;
    Ok(())
}

pub(super) async fn compact(handle: &Handle) -> io::Result<()> {
    handle
        .call("thread/compact/start", json!({"threadId":handle.native()?}))
        .await?;
    Ok(())
}

pub(super) async fn rewind(handle: &Handle, input: &Value) -> io::Result<Value> {
    // Without these, the fork falls back to Codex's own sandbox, which cannot run in Cloud sandboxes.
    let mut params = json!({"threadId":handle.native()?,"approvalPolicy":"never","sandbox":"danger-full-access"});
    if let Some(system_prompt) = &handle.system_prompt {
        params["developerInstructions"] = json!(system_prompt);
    }
    if let Some(id) = input["last_turn_id"]
        .as_str()
        .or_else(|| input["lastTurnId"].as_str())
    {
        params["lastTurnId"] = json!(id);
    }
    if let Some(id) = input["before"]
        .as_str()
        .or_else(|| input["before_turn_id"].as_str())
        .or_else(|| input["beforeTurnId"].as_str())
    {
        params["beforeTurnId"] = json!(id);
    }
    let result = handle.call("thread/fork", params).await?;
    let id = result
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("missing forked thread identity"))?;
    Ok(json!({
        "id":id,
        "path":result.pointer("/thread/path"),
        "cursor":{"offset":0}
    }))
}
/// User goal changes carry `origin: "user"`, so Codex treats them as the user's own instruction.
pub(super) async fn goal(handle: &Handle, input: &Value) -> io::Result<()> {
    let mut params = json!({"threadId":handle.native()?,"origin":"user"});
    if input["clear"] == true {
        handle.call("thread/goal/clear", params).await?;
        return Ok(());
    }
    for key in ["status", "objective"] {
        if let Some(value) = input[key].as_str() {
            params[key] = json!(value);
        }
    }
    handle.call("thread/goal/set", params).await?;
    Ok(())
}

/// Codex's own UI pauses an active goal before Stop; otherwise the goal starts its next turn at once.
async fn pause_goal(handle: &Handle, thread: &str) {
    let Ok(result) = handle
        .call("thread/goal/get", json!({"threadId":thread}))
        .await
    else {
        return;
    };
    if result.pointer("/goal/status") == Some(&json!("active")) {
        let params = json!({"threadId":thread,"status":"paused","origin":"user"});
        let _ = handle.call("thread/goal/set", params).await;
    }
}

pub(super) async fn notice(handle: &Handle, text: &str) -> io::Result<()> {
    handle.call("thread/inject_items", json!({"threadId":handle.native()?,"items":[{"type":"message","role":"developer","content":[{"type":"input_text","text":text}]}]})).await?;
    Ok(())
}

pub(super) async fn interrupt(handle: &Handle, state: Progress) -> io::Result<()> {
    let turn = state
        .native_turn
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "turn has not started"))?;
    let native = handle.native()?;
    pause_goal(handle, &native).await;
    handle
        .process
        .control(
            "turn/interrupt",
            json!({"threadId":native,"turnId":turn}),
            state.request.as_deref().unwrap(),
        )
        .await?;
    let mut progress = handle.process.progress.clone();
    let processes = tokio::time::timeout(
        Duration::from_secs(30),
        progress.wait_for(|s| s.request == state.request && s.finished),
    )
    .await
    .map_err(|_| io::Error::other("turn cancellation timed out; outcome uncertain"))?
    .map_err(|_| io::Error::other("harness ended during cancellation"))?
    .processes
    .clone();
    for process in processes {
        let result = handle
            .call(
                "thread/backgroundTerminals/terminate",
                json!({"threadId":native,"processId":process}),
            )
            .await?;
        if result["terminated"].as_bool().is_none() {
            return Err(io::Error::other("invalid tool termination response"));
        }
    }
    Ok(())
}

pub(super) struct Protocol {
    home: PathBuf,
    file_identity: super::files::Identity,
    tail: Tail,
    root: Option<String>,
    prompt_id: Option<u64>,
    forking: Option<u64>,
    compaction: Option<(u64, Option<String>)>,
}
impl Protocol {
    pub fn new(
        profile: &HarnessConfig,
        resume: Option<&Resume>,
        file_identity: super::files::Identity,
    ) -> Self {
        Self {
            home: profile.home.clone(),
            file_identity,
            tail: resume
                .map(|s| Tail::new(s.path.clone(), s.cursor["offset"].as_u64().unwrap_or(0)))
                .unwrap_or_default(),
            root: resume.map(|s| s.id.clone()),
            prompt_id: None,
            forking: None,
            compaction: None,
        }
    }
}
impl Adapter for Protocol {
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value {
        if request.is_some() {
            self.prompt_id = Some(id);
        }
        // A side chat's first fork is its start; only a rewind replaces a known thread.
        if method == "thread/fork" && self.root.is_some() {
            self.forking = Some(id);
        }
        if method == "thread/compact/start" {
            self.compaction = Some((id, None));
        }
        if method == "initialized" {
            json!({"method":method})
        } else {
            json!({"id":id,"method":method,"params":params})
        }
    }
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        state: &mut Progress,
    ) -> io::Result<Vec<Event>> {
        if let Some(path) = value
            .pointer("/params/thread/path")
            .or_else(|| value.pointer("/result/thread/path"))
            .and_then(Value::as_str)
            && self.tail.path.is_none()
        {
            self.tail.path = Some(path.into());
        }
        let identity = value
            .pointer("/params/thread")
            .or_else(|| value.pointer("/result/thread"));
        if self.root.is_none() {
            self.root = identity.and_then(|v| v["id"].as_str()).map(str::to_owned);
        }
        let method = value["method"].as_str().unwrap_or("response");
        let params = &value["params"];
        let thread = params["threadId"]
            .as_str()
            .or_else(|| params.pointer("/thread/id").and_then(Value::as_str));
        let root = thread.is_none() || thread == self.root.as_deref();
        let kind = match method {
            "item/agentMessage/delta" => "text_delta",
            "item/commandExecution/outputDelta" => "tool_delta",
            "item/started" => "item_started",
            "item/completed" => "item_completed",
            _ => "native_event",
        };
        let usage = params
            .pointer("/turn/usage")
            .or_else(|| params.pointer("/turn/tokenUsage"))
            .or_else(|| params.get("usage"))
            .cloned()
            .filter(|value| !value.is_null());
        if root
            && method == "item/completed"
            && params["item"]["type"] == "agentMessage"
            && let Some(text) = params["item"]["text"].as_str()
        {
            state.last_text = text.to_owned();
        }
        let mut events = vec![Event::Record {
            kind: if root { kind } else { "native_event" },
            data: json!({"method":method,"item_id":params["itemId"].as_str().or_else(||params.pointer("/item/id").and_then(Value::as_str)),"request_id":params["item"]["clientId"],"delta":params["delta"],"text":params["item"]["text"],"tool_name":params["item"]["command"],"output":params["item"]["aggregatedOutput"],"status":params["item"]["status"],"usage":usage}),
            native: Some(raw),
        }];
        if let Some((id, turn)) = &mut self.compaction {
            if value["id"].as_u64() == Some(*id) && value.get("error").is_some() {
                self.compaction = None;
            } else if thread == self.root.as_deref() && thread.is_some() {
                let native_turn = params.pointer("/turn/id").and_then(Value::as_str);
                if method == "turn/started" && turn.is_none() {
                    *turn = native_turn.map(str::to_owned);
                }
                let status = if method == "turn/completed"
                    && turn.is_some()
                    && native_turn == turn.as_deref()
                {
                    Some(params["turn"]["status"].as_str().unwrap_or("unknown"))
                } else if method == "thread/compacted" && turn.is_none() {
                    Some("completed")
                } else {
                    None
                };
                if let Some(status) = status {
                    events.push(Event::Compacted {
                        status: status.to_owned(),
                    });
                    self.compaction = None;
                }
            }
        }
        if self.forking.is_some() && value["id"].as_u64() == self.forking {
            self.forking = None;
            if value.get("error").is_none() {
                let identity = identity.ok_or_else(|| io::Error::other("missing fork identity"))?;
                let id = identity["id"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing fork ID"))?;
                let path = identity["path"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing fork path"))?;
                loop {
                    events.extend(self.tail.capture(&self.home, self.file_identity)?);
                    if !self.tail.more {
                        break;
                    }
                }
                self.root = Some(id.to_owned());
                self.tail = Tail::new(path.into(), 0);
                *state = Progress {
                    native: Some(id.to_owned()),
                    finished: true,
                    ..Progress::default()
                };
                events.push(Event::Record {
                    kind: "rewind_ready",
                    data: json!({"id":id,"path":path,"cursor":{"offset":0}}),
                    native: None,
                });
            }
        } else if let Some(identity) = identity
            && identity["id"].as_str() == self.root.as_deref()
            && let Some(id) = &self.root
        {
            state.native = Some(id.clone());
            events.push(Event::Record {
                kind: "native_identity",
                data: json!({"id":id,"path":identity["path"].as_str()}),
                native: None,
            });
        }
        if value["id"].as_u64() == self.prompt_id && value.get("error").is_some() {
            state.finished = true;
        }
        // Codex steers a turn/start into a turn that is already running (a goal continuation),
        // so the response, not a turn/started notification, names the prompt's turn.
        if value["id"].as_u64() == self.prompt_id
            && state.native_turn.is_none()
            && let Some(turn) = value.pointer("/result/turn/id").and_then(Value::as_str)
        {
            state.native_turn = Some(turn.into());
            events.extend(state.started());
        }
        let limited = Some(&json!("usageLimitExceeded"));
        if root
            && (params.pointer("/turn/error/codexErrorInfo") == limited
                || params.pointer("/error/codexErrorInfo") == limited)
        {
            events.push(Event::Record {
                kind: "usage_limited",
                data: json!({"harness":"codex"}),
                native: None,
            });
        }
        match if root { method } else { "" } {
            "turn/started" => {
                if let Some(id) = params.pointer("/turn/id").and_then(Value::as_str) {
                    if (state.request.is_none() || state.finished) && self.compaction.is_none() {
                        // Codex started this turn itself (a goal continuation): track it like a requested one,
                        // before its records, so they belong to it.
                        *state = Progress {
                            native: state.native.clone(),
                            model: state.model.clone(),
                            request: Some(format!("auto_{id}")),
                            native_turn: Some(id.into()),
                            auto: true,
                            ..Progress::default()
                        };
                        events.splice(0..0, state.started());
                    } else {
                        state.native_turn = Some(id.into());
                        events.extend(state.started());
                    }
                }
            }
            "thread/tokenUsage/updated" => {
                let usage = &params["tokenUsage"];
                let (total, last) = (&usage["total"], &usage["last"]);
                state.last_usage = json!({"contextUsage":{"tokens":last["totalTokens"],"contextWindow":usage["modelContextWindow"]},"tokens":{"input":total["inputTokens"],"output":total["outputTokens"],"cacheRead":total["cachedInputTokens"],"cacheWrite":total["cacheWriteInputTokens"],"total":total["totalTokens"]},"lastUsage":{"input":last["inputTokens"],"output":last["outputTokens"],"cacheRead":last["cachedInputTokens"],"cacheWrite":last["cacheWriteInputTokens"],"totalTokens":last["totalTokens"]}});
            }
            "item/started" => {
                if params["item"]["type"] == "commandExecution"
                    && params["turnId"].as_str() == state.native_turn.as_deref()
                    && let Some(pid) = params["item"]["processId"].as_str()
                {
                    state.processes.insert(pid.into());
                }
            }
            "turn/completed"
                if params.pointer("/turn/id").and_then(Value::as_str)
                    == state.native_turn.as_deref()
                    && state.native_turn.is_some() =>
            {
                let error = params
                    .pointer("/turn/error/message")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                events.extend(state.finished(
                    params["turn"]["status"].as_str().unwrap_or("unknown"),
                    error,
                ));
            }
            _ => {}
        }
        Ok(events)
    }
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)> {
        if value.get("method").is_some() {
            return None;
        }
        let id = value["id"].as_u64()?;
        Some((
            id,
            if let Some(error) = value.get("error") {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "harness rejected command (code {}): {}",
                        error["code"],
                        error["message"]
                            .as_str()
                            .unwrap_or("no error message returned")
                    ),
                ))
            } else {
                Ok(value.get("result").cloned().unwrap_or(Value::Null))
            },
        ))
    }
    fn respond(&self, value: &Value) -> Vec<Value> {
        if value.get("method").is_some() && value.get("id").is_some() {
            vec![
                json!({"id":value["id"],"error":{"code":-32601,"message":"interaction unsupported by this core slice"}}),
            ]
        } else {
            vec![]
        }
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        self.tail.capture(&self.home, self.file_identity)
    }
    fn capture_pending(&self) -> bool {
        self.tail.more
    }
}

pub(super) fn for_client(data: &mut Value, native: Option<&str>) -> io::Result<()> {
    let frame: Value = serde_json::from_str(native.unwrap_or("null"))?;
    data["value"] = frame.get("params").cloned().unwrap_or(Value::Null);
    Ok(())
}

#[derive(Default)]
struct Tail {
    path: Option<PathBuf>,
    file: Option<File>,
    read_offset: u64,
    committed_offset: u64,
    partial: Vec<u8>,
    more: bool,
}
impl Tail {
    fn new(path: PathBuf, offset: u64) -> Self {
        Self {
            path: Some(path),
            file: None,
            read_offset: offset,
            committed_offset: offset,
            partial: Vec::new(),
            more: false,
        }
    }
    fn capture(&mut self, home: &Path, identity: super::files::Identity) -> io::Result<Vec<Event>> {
        self.more = false;
        let Some(path) = &self.path else {
            return Ok(vec![]);
        };
        if self.file.is_none() {
            self.file = Some(
                match super::files::open(&home.join("sessions"), path, identity) {
                    Ok(file) => file,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
                    Err(error) => return Err(error),
                },
            );
        }
        let file = self.file.as_mut().unwrap();
        if file.metadata()?.len() < self.read_offset {
            return Err(io::Error::other("native history was truncated"));
        }
        file.seek(SeekFrom::Start(self.read_offset))?;
        let mut events = Vec::new();
        // A bounded batch lets shutdown and RPC replies make progress during large rollouts.
        let mut bytes = vec![0; 65536];
        let n = file.read(&mut bytes)?;
        let previous_length = self.partial.len();
        self.partial.extend_from_slice(&bytes[..n]);
        let parsed = (|| {
            let mut consumed = 0;
            let mut offset = self.committed_offset;
            while let Some(end) = self.partial[consumed..].iter().position(|b| *b == b'\n') {
                if end + 1 > MAX_LINE {
                    return Err(io::Error::other("native history record too large"));
                }
                let line = &self.partial[consumed..consumed + end + 1];
                let raw = std::str::from_utf8(line)
                    .map_err(io::Error::other)?
                    .to_owned();
                events.push(Event::Record {
                    kind: "native_record",
                    data: json!({"offset":offset}),
                    native: Some(raw),
                });
                consumed += end + 1;
                offset += line.len() as u64;
            }
            if self.partial.len() - consumed > MAX_LINE {
                return Err(io::Error::other("native history record too large"));
            }
            Ok((consumed, offset))
        })();
        match parsed {
            Ok((consumed, offset)) => {
                self.partial.drain(..consumed);
                self.read_offset += n as u64;
                self.committed_offset = offset;
                self.more = n > 0;
                Ok(events)
            }
            Err(error) => {
                // Exit capture retries this same Tail. Never publish a cursor for a lost batch.
                self.partial.truncate(previous_length);
                Err(error)
            }
        }
    }
}
pub(super) fn recover(
    profile: &HarnessConfig,
    saved: &Resume,
    identity: super::files::Identity,
    mut emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    let mut tail = Tail::new(
        saved.path.clone(),
        saved.cursor["offset"].as_u64().unwrap_or(0),
    );
    loop {
        let before = tail.read_offset;
        for event in tail.capture(&profile.home, identity)? {
            emit(event)?;
        }
        if before == tail.read_offset {
            return Ok(());
        }
    }
}
