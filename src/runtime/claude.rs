//! Claude owns inference; this adapter owns its stdio protocol and native transcript.
use super::{Adapter, Event, Handle, Progress, Resume, files, process::MAX_LINE};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use tokio::process::Command;

const INSTRUCTIONS: &str = "Cloudroom owns this cloud session. Ask clarifying questions in ordinary chat, not interactive tools. Start child threads with `cloudroom thread spawn` (any harness and model; see `cloudroom thread --help`); each runs in this sandbox, shows under this thread, and Cloudroom messages you when it finishes. Do not use native subagents, workflows, or agent teams. Fast mode is unavailable. Rewinding changes conversation history only, never files.";

fn uuid() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

pub(super) fn command(
    config: &Config,
    profile: &HarnessConfig,
    saved: Option<&Resume>,
    fork: Option<&str>,
    guarded: bool,
    strip_ai_co_authors: bool,
    system_prompt: Option<&str>,
) -> io::Result<(Command, String)> {
    let id = match saved.filter(|_| fork.is_none()) {
        Some(saved) => saved.id.clone(),
        None => uuid()?,
    };
    let mut command = if guarded {
        super::command_guard::claude_command(config, profile)
    } else {
        let mut command = super::command(&profile.binary, config);
        command.args([
            "--settings",
            r#"{"fastMode":false,"enableWorkflows":false}"#,
        ]);
        command
    };
    profile_env(&mut command, config, profile);
    command
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--replay-user-messages",
            "--dangerously-skip-permissions",
            "--permission-prompts",
            "none",
            "--no-chrome",
        ])
        .args([
            "--model",
            &profile.model,
            "--append-system-prompt",
            &system_prompt.map_or(INSTRUCTIONS.to_owned(), |p| {
                format!("{INSTRUCTIONS}\n\n{p}")
            }),
        ])
        .args([
            "--disallowedTools",
            "AskUserQuestion,EnterPlanMode,ExitPlanMode,Agent,Task,Workflow,SendMessage",
        ])
        .args([
            "--mcp-config",
            r#"{"mcpServers":{"cloudroom":{"type":"sdk","name":"cloudroom"}}}"#,
        ]);
    if strip_ai_co_authors {
        command.args(["--settings", r#"{"attribution":{"commit":"","pr":""}}"#]);
    }
    if let Some(saved) = saved {
        command.arg("--resume").arg(&saved.id);
        if let Some(before) = fork {
            command.args(["--fork-session", "--session-id", &id]);
            // An empty checkpoint forks the whole conversation (a Cloud fork from its last message).
            if !before.is_empty() {
                command.args(["--resume-session-at", before]);
            }
        }
    } else {
        command.args(["--session-id", &id]);
    }
    Ok((command, id))
}

/// The one-year token from `claude setup-token` (ADR 0121), then the user's plan on line two.
/// Only core can read this file.
pub(super) fn token_path(config: &Config) -> PathBuf {
    config.state_dir.join("claude-oauth-token")
}

/// The user's Anthropic API key, the alternative to a subscription. Only core can read this file.
pub(super) fn key_path(config: &Config) -> PathBuf {
    config.state_dir.join("claude-api-key")
}

/// Fingerprint of the login files `profile_env` reads, so a running agent can tell its login changed.
pub(super) fn login(config: &Config) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    fs::read(key_path(config)).ok().hash(&mut hasher);
    fs::read(token_path(config)).ok().hash(&mut hasher);
    hasher.finish()
}

pub(super) fn profile_env(command: &mut Command, config: &Config, profile: &HarnessConfig) {
    // Setting this even to ~/.claude relocates ~/.claude.json and hides native user settings.
    if profile.home != config.account_home.join(".claude") {
        command.env("CLAUDE_CONFIG_DIR", &profile.home);
    }
    // Cloudroom matches the Mac's Claude version (ADR 0133); Claude's own updater must not move it.
    command.env("DISABLE_AUTOUPDATER", "1");
    // Claude prefers an API key over a subscription token, so pass only one.
    let key = fs::read_to_string(key_path(config)).unwrap_or_default();
    if !key.trim().is_empty() {
        command.env("ANTHROPIC_API_KEY", key.trim());
    } else if let Ok(saved) = fs::read_to_string(token_path(config)) {
        let mut lines = saved.lines().map(str::trim);
        if let Some(token) = lines.next().filter(|s| !s.is_empty()) {
            command.env("CLAUDE_CODE_OAUTH_TOKEN", token);
        }
        // Without the plan, Claude hides plan-only models such as Opus 1M.
        if let Some(plan) = lines.next().filter(|s| !s.is_empty()) {
            command.env("CLAUDE_CODE_SUBSCRIPTION_TYPE", plan);
        }
    }
}

pub(super) async fn auth_ready(config: &Config) -> io::Result<bool> {
    let (code, value) = run_json(config, &["auth", "status", "--json"], 10).await?;
    match (code, value["loggedIn"].as_bool()) {
        (Some(0), Some(true)) => Ok(true),
        (Some(1), Some(false)) => Ok(false),
        _ => Err(io::Error::other("invalid Claude account response")),
    }
}

/// One tiny real request with the exact model and effort. `auth status` can report
/// "logged in" for a token Anthropic rejects, so Teleport verifies with inference.
pub(super) async fn probe(config: &Config, model: &str, reasoning: Option<&str>) -> io::Result<()> {
    // The prompt goes first: `--tools` takes a list and would swallow a trailing prompt.
    let mut args = vec![
        "-p",
        "Reply with exactly: OK",
        "--model",
        model,
        "--output-format",
        "json",
        "--no-session-persistence",
        "--tools",
        "",
    ];
    if let Some(effort) = reasoning.filter(|effort| *effort != "none") {
        args.extend(["--effort", effort]);
    }
    let (_, value) = run_json(config, &args, 40).await?;
    if value["is_error"] == false {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "Claude rejected a test request with model {model}: {}",
        result_error(&value)
    )))
}

/// Run the configured Claude CLI as the agent and parse its single JSON reply.
async fn run_json(
    config: &Config,
    args: &[&str],
    seconds: u64,
) -> io::Result<(Option<i32>, Value)> {
    let (code, bytes) = run_output(config, args, seconds).await?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| io::Error::other("Claude returned an unreadable response"))?;
    Ok((code, value))
}

/// Run the configured Claude CLI as the agent and return its exit code and stdout.
pub(super) async fn run_output(
    config: &Config,
    args: &[&str],
    seconds: u64,
) -> io::Result<(Option<i32>, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let profile = config
        .harnesses
        .get(&super::Kind::Claude)
        .ok_or_else(|| io::Error::other("Claude is not configured"))?;
    let mut command = super::command(&profile.binary, config);
    profile_env(&mut command, config, profile);
    command.args(args).stderr(std::process::Stdio::null());
    let group = config
        .storage
        .as_ref()
        .map(|policy| {
            let group = super::linux::Workload::create(policy)?;
            group.attach(&mut command, policy)?;
            Ok::<_, io::Error>(group)
        })
        .transpose()?;
    let mut child = command.spawn()?;
    let mut stdout = child.stdout.take().unwrap().take(65537);
    let result = tokio::time::timeout(std::time::Duration::from_secs(seconds), async {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).await?;
        if bytes.len() > 65536 {
            return Err(io::Error::other("Claude response too large"));
        }
        Ok((child.wait().await?.code(), bytes))
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Claude did not answer in time"))
    .and_then(|r| r);
    let _ = child.kill().await;
    let _ = child.wait().await;
    if let Some(group) = group {
        group.stop().await?;
    }
    result
}

pub(super) async fn initialize(handle: &Handle) -> io::Result<Value> {
    let info = handle.call("initialize", json!({"hooks":{"SubagentStart":[{"hookCallbackIds":["cloudroom-native-child"]}]},"sdkMcpServers":["cloudroom"]})).await?;
    if info["hooks_applied"] != true {
        return Err(io::Error::other(
            "Claude did not register the managed-delegation hook",
        ));
    }
    Ok(info)
}

pub(super) fn model_catalog(value: &Value) -> io::Result<Vec<super::Model>> {
    let entries = value["models"]
        .as_array()
        .ok_or_else(|| io::Error::other("missing Claude model catalog"))?;
    let mut models = Vec::<super::Model>::new();
    for entry in entries {
        let mut levels: Vec<String> = entry["supportedEffortLevels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        if entry["supportsEffort"] != true && levels.is_empty() {
            levels.push("none".into());
        }
        for name in [entry["value"].as_str(), entry["resolvedModel"].as_str()]
            .into_iter()
            .flatten()
        {
            if !models.iter().any(|model| model.model == name) {
                models.push(super::Model {
                    model: name.into(),
                    reasoning_levels: levels.clone(),
                });
            }
        }
    }
    Ok(models)
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    let info = initialize(handle).await?;
    if let Some(reasoning) = &handle.reasoning {
        let models = model_catalog(&info)?;
        super::supports(
            super::Kind::Claude,
            &models,
            &handle.profile.model,
            reasoning,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if reasoning != "none" {
            handle
                .call(
                    "apply_flag_settings",
                    json!({"settings":{"effortLevel":reasoning,"fastMode":false}}),
                )
                .await?;
        }
    }
    handle
        .call(
            "bootstrap",
            json!({"text":"Automatic Cloudroom note: this session has started or resumed. Messages after this note come from the user."}),
        )
        .await?;
    let id = handle.native()?;
    let path = transcript(&handle.profile, &id)?;
    let file = validate(&handle.profile, &path, &id, handle.file_identity)?;
    file.sync_all()?;
    files::open_directory(
        &handle.profile.home.join("projects"),
        path.parent().unwrap(),
        handle.file_identity,
    )?
    .sync_all()?;
    Ok(id)
}

/// Returns a warning for each selected skill that could not load.
pub(super) async fn send(handle: &Handle, request: &str, input: &Value) -> io::Result<Vec<Value>> {
    let text = input["text"].as_str().unwrap_or_default();
    if text.split_whitespace().next() == Some("/fast") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Claude Fast mode is disabled",
        ));
    }
    let reader = handle.clone();
    let payload = input.clone();
    let (text, warnings) =
        tokio::task::spawn_blocking(move || super::claude_skills::expand(&reader, &payload))
            .await
            .map_err(io::Error::other)??;
    let mut settings = json!({"fastMode":false,"effortLevel":null});
    if let Some(reasoning) = input["reasoning"].as_str().or(handle.reasoning.as_deref()) {
        settings["effortLevel"] = if reasoning == "none" {
            Value::Null
        } else {
            json!(reasoning)
        };
    }
    handle
        .call("apply_flag_settings", json!({"settings":settings}))
        .await?;
    let mut content = Vec::new();
    if !text.is_empty() {
        content.push(json!({"type":"text","text":text}));
    }
    // Attachments are staged by Workspace, using the same safe reader as Pi.
    let reader = handle.clone();
    let payload = input.clone();
    let images = tokio::task::spawn_blocking(move || files::images(&reader, &payload, MAX_LINE))
        .await
        .map_err(io::Error::other)?
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot read Claude image attachment: {e}"),
            )
        })?;
    for image in images {
        if !matches!(
            image["mimeType"].as_str(),
            Some("image/png" | "image/jpeg" | "image/gif" | "image/webp")
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Claude requires PNG, JPEG, GIF or WebP images",
            ));
        }
        content.push(json!({"type":"image","source":{"type":"base64","media_type":image["mimeType"],"data":image["data"]}}));
    }
    if let Some(attachments) = input["attachments"].as_array() {
        for attachment in attachments.iter().filter(|a| a["kind"] != "image") {
            if let Some(path) = attachment["path"].as_str() {
                content.push(json!({"type":"text","text":format!("[Attached file: {path}] Use the Read tool to inspect it.")}));
            }
        }
    }
    if content.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Claude prompt is empty",
        ));
    }
    handle
        .process
        .call("prompt", json!({"content":content}), Some(request))
        .await?;
    Ok(warnings)
}

pub(super) async fn compact(handle: &Handle) -> io::Result<()> {
    handle.call("compact", json!({"text":"/compact"})).await?;
    Ok(())
}

pub(super) async fn notice(handle: &Handle, text: &str) -> io::Result<()> {
    handle.call("notice", json!({"text":text})).await?;
    Ok(())
}

/// A message sent mid-turn, which Claude reads at its next step, as Local's Claude steering does.
pub(super) async fn steer(handle: &Handle, request: &str, text: &str) -> io::Result<()> {
    handle
        .process
        .control(
            "steer",
            json!({"content":[{"type":"text","text":text}]}),
            request,
        )
        .await?;
    Ok(())
}

pub(super) async fn interrupt(handle: &Handle, request: &str) -> io::Result<()> {
    let response = handle
        .process
        .control("interrupt", json!({"cancel_queued":true}), request)
        .await?;
    if !response["still_queued"]
        .as_array()
        .is_some_and(Vec::is_empty)
    {
        return Err(io::Error::other(
            "Claude did not confirm cancellation of its native queue",
        ));
    }
    Ok(())
}

pub(super) async fn child_result(handle: &Handle, request: &str, result: Value) -> io::Result<()> {
    handle
        .process
        .control("child_result", result, request)
        .await?;
    Ok(())
}

pub(super) fn transcript(profile: &HarnessConfig, id: &str) -> io::Result<PathBuf> {
    if id.len() != 36 || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(io::Error::other("invalid Claude session identity"));
    }
    let root = profile.home.join("projects");
    let mut found = None;
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path().join(format!("{id}.jsonl"));
        if path.try_exists()? {
            if found.is_some() {
                return Err(io::Error::other("ambiguous Claude session identity"));
            }
            found = Some(path);
        }
    }
    found.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Claude transcript is missing"))
}

pub(super) fn validate(
    profile: &HarnessConfig,
    path: &Path,
    id: &str,
    identity: files::Identity,
) -> io::Result<File> {
    let mut file = files::open(&profile.home.join("projects"), path, identity)?;
    let mut reader = BufReader::new(&mut file);
    loop {
        let mut raw = String::new();
        if (&mut reader)
            .take((MAX_LINE + 1) as u64)
            .read_line(&mut raw)?
            == 0
        {
            break;
        }
        if raw.len() > MAX_LINE || !raw.ends_with('\n') {
            return Err(io::Error::other("invalid Claude transcript"));
        }
        let value: Value = serde_json::from_str(&raw)?;
        if value["sessionId"] == id && value["type"] == "user" {
            return Ok(file);
        }
    }
    Err(io::Error::other("Claude transcript identity is missing"))
}

/// Fork the whole conversation, through the native fork flag.
pub(super) fn fork_tip(handle: &Handle) -> io::Result<(Resume, String)> {
    let id = handle.native()?;
    let path = transcript(&handle.profile, &id)?;
    validate(&handle.profile, &path, &id, handle.file_identity)?;
    Ok((resume(handle, id, path), String::new()))
}

fn resume(handle: &Handle, id: String, path: PathBuf) -> Resume {
    Resume {
        id,
        path,
        cursor: Value::Null,
        model: Some(handle.profile.model.clone()),
        provider: handle.profile.provider.clone(),
        reasoning: handle.reasoning.clone(),
    }
}

/// Fork just before a user checkpoint, through the native resume/fork flags.
pub(super) fn fork_source(handle: &Handle, before: &str) -> io::Result<(Resume, String)> {
    let id = handle.native()?;
    let path = transcript(&handle.profile, &id)?;
    let saved = resume(handle, id, path);
    let parent = checkpoint_parent(&handle.profile, &saved, before, handle.file_identity)?;
    Ok((saved, parent))
}

/// The message a fork of `saved` resumes at: the saved parent of user message `before`.
pub(super) fn checkpoint_parent(
    profile: &HarnessConfig,
    saved: &Resume,
    before: &str,
    identity: files::Identity,
) -> io::Result<String> {
    let file = validate(profile, &saved.path, &saved.id, identity)?;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(0))?;
    loop {
        let mut raw = String::new();
        let n = (&mut reader)
            .take((MAX_LINE + 1) as u64)
            .read_line(&mut raw)?;
        if n == 0 {
            break;
        }
        if n > MAX_LINE || !raw.ends_with('\n') {
            return Err(io::Error::other("invalid Claude transcript"));
        }
        let value: Value = serde_json::from_str(&raw)?;
        if value["uuid"] == before && value["type"] == "user" && value["isMeta"] != true {
            let parent = value["parentUuid"].as_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Claude checkpoint has no saved parent",
                )
            })?;
            return Ok(parent.into());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Claude rewind checkpoint not found",
    ))
}

struct UserCall {
    id: u64,
    kind: String,
}
struct ChildCall {
    wire: String,
    rpc: Value,
}

pub(super) struct Protocol {
    profile: HarnessConfig,
    native: String,
    input_namespace: String,
    identity: files::Identity,
    capture: Capture,
    active: Option<(String, String)>,
    users: HashMap<String, UserCall>,
    children: HashMap<String, ChildCall>,
    reply: Option<(u64, Value, Option<String>)>,
    pending_frames: Vec<Value>,
    interrupting: bool,
    compacted: bool,
    bootstrapped: bool,
    close_confirmed: bool,
    last_result_failed: bool,
    message_id: String,
    blocks: HashMap<u64, Value>,
    streamed: HashSet<String>,
    tools: HashMap<String, String>,
    // The last model call's prompt size; `result.usage` sums every call in the turn.
    context: Option<u64>,
}
impl Protocol {
    pub(super) fn new(
        profile: &HarnessConfig,
        id: String,
        saved: Option<&Resume>,
        identity: files::Identity,
    ) -> io::Result<Self> {
        Ok(Self {
            profile: profile.clone(),
            native: id,
            input_namespace: uuid()?,
            identity,
            capture: Capture::new(profile, saved, identity),
            active: None,
            users: HashMap::new(),
            children: HashMap::new(),
            reply: None,
            pending_frames: Vec::new(),
            interrupting: false,
            compacted: false,
            bootstrapped: false,
            close_confirmed: false,
            last_result_failed: false,
            message_id: String::new(),
            blocks: HashMap::new(),
            streamed: HashSet::new(),
            tools: HashMap::new(),
            context: None,
        })
    }
    fn reply_control(id: &str, response: Value) -> Value {
        json!({"type":"control_response","response":{"subtype":"success","request_id":id,"response":response}})
    }
    fn mcp(&mut self, value: &Value, state: &Progress) -> io::Result<Vec<Event>> {
        let request = &value["request"];
        let wire = value["request_id"]
            .as_str()
            .ok_or_else(|| io::Error::other("missing Claude control identity"))?;
        let message = &request["message"];
        if wire.len() > 48
            || wire.is_empty()
            || !wire.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(io::Error::other("invalid Claude control identity"));
        }
        let result = match message["method"].as_str() {
            Some("initialize") => {
                json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"cloudroom","version":env!("CARGO_PKG_VERSION")}})
            }
            Some("notifications/initialized") => json!({}),
            Some("tools/list") => {
                json!({"tools":[{"name":"delegate","description":"Run a separately managed Cloudroom child agent in this workspace. Returns its result and session ID. The child has independent history and controls.","inputSchema":{"type":"object","properties":{"prompt":{"type":"string","minLength":1,"maxLength":32768}},"required":["prompt"],"additionalProperties":false}}]})
            }
            Some("tools/call") if message["params"]["name"] == "delegate" => {
                let prompt = message["params"]["arguments"]["prompt"]
                    .as_str()
                    .filter(|p| !p.trim().is_empty() && p.len() <= 32768);
                if let (Some(prompt), Some((_, active))) = (
                    prompt,
                    self.active.as_ref().filter(|(_, active)| {
                        state.request.as_ref() == Some(active) && !state.finished
                    }),
                ) {
                    if self.children.contains_key(wire) {
                        return Ok(vec![]);
                    }
                    self.children.insert(
                        wire.into(),
                        ChildCall {
                            wire: wire.into(),
                            rpc: message["id"].clone(),
                        },
                    );
                    return Ok(vec![Event::ChildRequest(super::ChildRequest {
                        request: active.clone(),
                        id: wire.into(),
                        tool_call_id: wire.into(),
                        prompt: prompt.into(),
                    })]);
                }
                json!({"isError":true,"content":[{"type":"text","text":"Delegation requires an active parent and a non-empty prompt of at most 32768 bytes."}]})
            }
            _ => {
                json!({"isError":true,"content":[{"type":"text","text":"Unsupported Cloudroom tool operation"}]})
            }
        };
        let response = if message.get("id").is_some() {
            json!({"jsonrpc":"2.0","id":message["id"],"result":result})
        } else {
            json!({})
        };
        self.pending_frames
            .push(Self::reply_control(wire, json!({"mcp_response":response})));
        Ok(vec![])
    }
}

impl Adapter for Protocol {
    fn validate(&self, method: &str, params: &Value) -> io::Result<()> {
        if matches!(method, "prompt" | "steer") {
            let frame = json!({"type":"user","uuid":self.input_namespace,"session_id":self.native,"parent_tool_use_id":null,"message":{"role":"user","content":params["content"]},"isSynthetic":false,"shouldQuery":true});
            if serde_json::to_vec(&frame)?.len() + 1 > MAX_LINE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Claude prompt exceeds the native frame limit",
                ));
            }
        }
        Ok(())
    }
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value {
        if matches!(
            method,
            "prompt" | "steer" | "bootstrap" | "notice" | "compact"
        ) {
            let wire = format!("{}{:012x}", &self.input_namespace[..24], id);
            self.users.insert(
                wire.clone(),
                UserCall {
                    id,
                    kind: method.into(),
                },
            );
            if let Some(request) = request {
                self.active = Some((wire.clone(), request.into()));
                self.interrupting = false;
                self.blocks.clear();
                self.streamed.clear();
                self.tools.clear();
            }
            if method == "compact" {
                self.compacted = false;
            }
            return json!({"type":"user","uuid":wire,"session_id":self.native,"parent_tool_use_id":null,"message":{"role":"user","content":params.get("content").cloned().unwrap_or_else(||json!(params["text"]))},"isSynthetic":matches!(method,"notice"|"bootstrap"),"shouldQuery":!matches!(method,"notice"|"bootstrap")});
        }
        if method == "interrupt" {
            self.interrupting = true;
        }
        if method == "child_result"
            && let Some(child) = params["id"]
                .as_str()
                .and_then(|id| self.children.remove(id))
        {
            let result = json!({"isError":params.get("error").is_some(),"content":[{"type":"text","text":params.to_string()}]});
            // The CLI echoes a control response, allowing the driver to acknowledge delivery.
            self.users.insert(
                child.wire.clone(),
                UserCall {
                    id,
                    kind: "child_result".into(),
                },
            );
            return Self::reply_control(
                &child.wire,
                json!({"mcp_response":{"jsonrpc":"2.0","id":child.rpc,"result":result}}),
            );
        }
        let mut request = params;
        request["subtype"] = json!(method);
        json!({"type":"control_request","request_id":id.to_string(),"request":request})
    }
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        state: &mut Progress,
    ) -> io::Result<Vec<Event>> {
        self.reply = None;
        self.pending_frames.clear();
        let mut events = vec![];
        let kind = value["type"].as_str().unwrap_or("");
        if kind == "control_request" {
            if value["request"]["subtype"] == "mcp_message"
                && value["request"]["server_name"] == "cloudroom"
            {
                return self.mcp(value, state);
            }
            let id = value["request_id"]
                .as_str()
                .ok_or_else(|| io::Error::other("missing Claude control identity"))?;
            let response = if value["request"]["subtype"] == "hook_callback" {
                json!({"continue":false,"stopReason":"Native Claude subagents are disabled. Start a managed child thread with `cloudroom thread spawn`."})
            } else {
                json!({"behavior":"deny","message":"Interactive requests are unsupported. Ask the user in ordinary chat."})
            };
            self.pending_frames.push(Self::reply_control(id, response));
            return Ok(vec![Event::Record {
                kind: "interaction_cancelled",
                data: json!({"harness":"claude-code","reason":"native interaction unsupported"}),
                native: None,
            }]);
        }
        if kind == "control_response" {
            let response = &value["response"];
            if let Some(wire) = response["request_id"].as_str() {
                let id = wire
                    .parse::<u64>()
                    .ok()
                    .or_else(|| self.users.remove(wire).map(|call| call.id));
                if let Some(id) = id {
                    self.reply = Some((
                        id,
                        response["response"].clone(),
                        (response["subtype"] != "success").then(|| {
                            response["error"]
                                .as_str()
                                .map_or_else(|| response.to_string(), str::to_owned)
                        }),
                    ));
                }
            }
            return Ok(events);
        }
        // Long tools emit a 30s heartbeat whose parent_tool_use_id is the tool itself, not a subagent.
        if kind == "tool_progress" {
            return Ok(events);
        }
        if value["parent_tool_use_id"].as_str().is_some() {
            return Err(io::Error::other("unmanaged Claude subagent emitted output"));
        }
        if let Some(id) = value["session_id"].as_str()
            && id != self.native
        {
            return Err(io::Error::other("Claude session identity changed"));
        }
        if kind == "system" && value["subtype"] == "init" {
            let caps = value["capabilities"]
                .as_array()
                .ok_or_else(|| io::Error::other("Claude CLI lacks required capabilities"))?;
            if !["msg_lifecycle_v1", "interrupt_cancel_queued_v1"]
                .iter()
                .all(|c| caps.iter().any(|v| v == c))
            {
                return Err(io::Error::other(
                    "Claude CLI lacks request lifecycle or queue cancellation support",
                ));
            }
            if value["permissionMode"] != "bypassPermissions" || value["fast_mode_state"] == "on" {
                return Err(io::Error::other(
                    "Claude launch permissions or Fast mode differ from the requested configuration",
                ));
            }
            state.native = Some(self.native.clone());
            state.model = value["model"].as_str().map(str::to_owned);
        }
        if kind == "command_lifecycle"
            && let Some(wire) = value["command_uuid"].as_str()
            && let Some(call) = self.users.get(wire)
        {
            if call.kind != "bootstrap"
                && matches!(
                    value["state"].as_str(),
                    Some("queued" | "rejected" | "cancelled")
                )
            {
                self.reply = Some((
                    call.id,
                    Value::Null,
                    (value["state"] != "queued").then(|| format!("command was {}", value["state"])),
                ));
            }
            if value["state"] == "started" && self.active.as_ref().is_some_and(|(id, _)| id == wire)
            {
                events.extend(state.started());
                events.push(Event::Record {
                    kind: "checkpoint",
                    data: json!({"kind":"message","id":wire,"request_id":state.request}),
                    native: None,
                });
            }
        }
        if kind == "system" && value["subtype"] == "compact_boundary" {
            self.compacted = true;
            self.context = value["compact_metadata"]["post_tokens"].as_u64();
        }
        // Claude starts its own turn when a background task or wakeup fires. Track it like a prompt.
        if self.active.is_none()
            && kind == "stream_event"
            && value["event"]["type"] == "message_start"
            && !self.users.values().any(|call| call.kind == "compact")
        {
            let request = format!("auto_{}", uuid()?);
            self.active = Some((String::new(), request.clone()));
            self.interrupting = false;
            self.streamed.clear();
            self.tools.clear();
            *state = Progress {
                native: state.native.clone(),
                model: state.model.clone(),
                request: Some(request),
                auto: true,
                ..Progress::default()
            };
            events.extend(state.started());
        }
        let request = self.active.as_ref().map(|(_, id)| id.clone());
        let mut data = json!({"harness":"claude-code","type":kind,"request_id":request});
        let mut keep = true;
        let event_kind = if kind == "stream_event" {
            let event = &value["event"];
            let index = event["index"].as_u64().unwrap_or(0);
            match event["type"].as_str() {
                Some("message_start") => {
                    self.blocks.clear();
                    self.message_id = event["message"]["id"].as_str().unwrap_or("").into();
                    self.streamed.insert(self.message_id.clone());
                    "native_event"
                }
                Some("content_block_start") => {
                    let block = &event["content_block"];
                    let id = block["id"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("{}:{index}", self.message_id));
                    data["item_id"] = json!(id);
                    data["item_type"] = block["type"].clone();
                    data["tool_name"] = block["name"].clone();
                    data["input"] = block["input"].clone();
                    data["text"] = json!("");
                    if let Some(name) = block["name"].as_str() {
                        self.tools.insert(id, name.into());
                    }
                    self.blocks.insert(index, data.clone());
                    if block["type"] == "tool_use" {
                        "native_event"
                    } else {
                        "item_started"
                    }
                }
                Some("content_block_delta") => {
                    if let Some(block) = self.blocks.get_mut(&index) {
                        data["item_id"] = block["item_id"].clone();
                        data["item_type"] = block["item_type"].clone();
                        let delta = &event["delta"];
                        let text = delta
                            .get("text")
                            .or_else(|| delta.get("thinking"))
                            .or_else(|| delta.get("partial_json"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let mut accumulated = block["text"].as_str().unwrap_or("").to_owned();
                        if accumulated.len() + text.len() > MAX_LINE {
                            return Err(io::Error::other(
                                "Claude content block exceeds the native frame limit",
                            ));
                        }
                        accumulated.push_str(text);
                        block["text"] = json!(accumulated);
                        data["delta"] = json!(text);
                    }
                    match event["delta"]["type"].as_str() {
                        Some("text_delta") => "text_delta",
                        Some("thinking_delta") => "thinking_delta",
                        // Tool input arrives a few characters per frame, and `item_started` saves it whole.
                        // Saving each piece made most of a busy sandbox's history.
                        _ => {
                            keep = false;
                            "native_event"
                        }
                    }
                }
                Some("content_block_stop") => {
                    if let Some(block) = self.blocks.remove(&index) {
                        data = block;
                        if data["item_type"] == "tool_use" {
                            if let Ok(input) =
                                serde_json::from_str::<Value>(data["text"].as_str().unwrap_or(""))
                            {
                                data["input"] = input;
                            }
                            "item_started"
                        } else {
                            "item_completed"
                        }
                    } else {
                        "native_event"
                    }
                }
                _ => "native_event",
            }
        } else {
            "native_event"
        };
        if kind == "assistant"
            && value["error"] == "authentication_failed"
            && value["isReplay"] != true
        {
            events.push(Event::Authentication { accepted: false });
        }
        if kind == "assistant"
            && let Some(tokens) = prompt_tokens(&value["message"]["usage"])
            && tokens > 0
        {
            self.context = Some(tokens);
        }
        if kind == "assistant"
            && let Some(content) = value["message"]["content"].as_array()
        {
            let text = content
                .iter()
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            if !text.is_empty() {
                state.last_text = text;
            }
            if !self
                .streamed
                .contains(value["message"]["id"].as_str().unwrap_or(""))
            {
                for (index, block) in content.iter().enumerate() {
                    let id = block["id"].as_str().map(str::to_owned).unwrap_or_else(|| {
                        format!(
                            "{}:{index}",
                            value["message"]["id"].as_str().unwrap_or("message")
                        )
                    });
                    if let Some(name) = block["name"].as_str() {
                        self.tools.insert(id.clone(), name.into());
                    }
                    let part = json!({"harness":"claude-code","request_id":request,"item_id":id,"item_type":block["type"],"text":block.get("text").or_else(||block.get("thinking")),"tool_name":block["name"],"input":block["input"]});
                    events.push(Event::Record {
                        kind: "item_started",
                        data: part.clone(),
                        native: None,
                    });
                    if block["type"] != "tool_use" {
                        events.push(Event::Record {
                            kind: "item_completed",
                            data: part,
                            native: None,
                        });
                    }
                }
            }
        }
        if kind == "user" {
            for block in value["message"]["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|b| b["type"] == "tool_result")
            {
                let id = block["tool_use_id"].as_str().unwrap_or("");
                events.push(Event::Record{kind:"item_completed",data:json!({"harness":"claude-code","request_id":request,"item_id":id,"item_type":"tool_use","tool_name":self.tools.get(id),"result":block["content"],"is_error":block["is_error"]}),native:None});
            }
        }
        if kind == "result" {
            self.last_result_failed = value["is_error"] == true;
            let ids: HashSet<&str> = value["user_message_uuids"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .chain(value["user_message_uuid"].as_str())
                .collect();
            // A steer that missed its turn runs as Claude's own turn, so it does not claim it.
            let auto = self.active.as_ref().is_some_and(|(id, _)| id.is_empty())
                && !ids
                    .iter()
                    .any(|id| self.users.get(*id).is_some_and(|call| call.kind != "steer"));
            for id in &ids {
                if let Some(call) = self.users.remove(*id) {
                    if call.kind == "bootstrap" {
                        self.bootstrapped = value["is_error"] != true;
                        self.reply = Some((
                            call.id,
                            Value::Null,
                            (!self.bootstrapped)
                                .then(|| format!("bootstrap failed: {}", value["result"])),
                        ));
                    }
                    if call.kind == "compact" {
                        events.push(Event::Compacted {
                            status: if self.compacted && value["is_error"] != true {
                                "completed"
                            } else {
                                "failed"
                            }
                            .into(),
                        });
                    }
                }
            }
            if auto
                || self
                    .active
                    .as_ref()
                    .is_some_and(|(id, _)| ids.contains(id.as_str()))
            {
                state.last_usage = usage(value, self.context);
                data["usage"] = state.last_usage.clone();
                let status = if self.interrupting {
                    "interrupted"
                } else if value["is_error"] == false && value["subtype"] == "success" {
                    "completed"
                } else {
                    "failed"
                };
                // Local commands and initialization can succeed without authenticating to inference.
                if status == "completed"
                    && value["usage"]["output_tokens"]
                        .as_u64()
                        .is_some_and(|n| n > 0)
                {
                    events.push(Event::Authentication { accepted: true });
                }
                let error = (status == "failed").then(|| result_error(value));
                events.extend(state.finished(status, error));
                self.active = None;
                self.children.clear();
            }
        }
        if keep {
            events.insert(
                0,
                Event::Record {
                    kind: event_kind,
                    data,
                    native: Some(raw),
                },
            );
        }
        Ok(events)
    }
    fn response(&self, _: &Value) -> Option<(u64, io::Result<Value>)> {
        self.reply.as_ref().map(|(id, value, error)| {
            (
                *id,
                if let Some(error) = error {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Claude rejected the command: {error}"),
                    ))
                } else {
                    Ok(value.clone())
                },
            )
        })
    }
    fn respond(&self, _: &Value) -> Vec<Value> {
        self.pending_frames.clone()
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        if !self.bootstrapped && self.capture.path.is_none() {
            return Ok(vec![]);
        }
        if self.capture.path.is_none() {
            match transcript(&self.profile, &self.native) {
                Ok(path) => {
                    validate(&self.profile, &path, &self.native, self.identity)?;
                    self.capture.path = Some(path.clone());
                    let mut events = vec![Event::Record {
                        kind: "native_identity",
                        data: json!({"id":self.native,"path":path}),
                        native: None,
                    }];
                    events.extend(self.capture.read()?);
                    return Ok(events);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
                Err(e) => return Err(e),
            }
        }
        self.capture.read()
    }
    fn capture_pending(&self) -> bool {
        self.capture.more
    }
    fn close(&mut self) -> Vec<Value> {
        vec![
            json!({"type":"control_request","request_id":"close","request":{"subtype":"interrupt","cancel_queued":true}}),
        ]
    }
    fn closed(&mut self, v: &Value) -> bool {
        self.close_confirmed = v["type"] == "control_response"
            && v["response"]["request_id"] == "close"
            && v["response"]["subtype"] == "success";
        self.close_confirmed
    }
    fn clean_exit(&self, code: Option<i32>) -> bool {
        // Claude preserves the last failed turn's exit code after an acknowledged shutdown.
        code == Some(0) || (self.close_confirmed && self.last_result_failed && code == Some(1))
    }
}

/// Claude's own words for a failed turn, so receipts show the real cause (ADR 0123).
fn result_error(value: &Value) -> String {
    let errors: Vec<&str> = value["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    match value["result"].as_str().filter(|text| !text.is_empty()) {
        Some(text) => text.to_owned(),
        None if !errors.is_empty() => errors.join("; "),
        None => format!(
            "Claude turn failed ({})",
            value["subtype"].as_str().unwrap_or("no error details")
        ),
    }
}

fn prompt_tokens(usage: &Value) -> Option<u64> {
    usage.is_object().then(|| {
        [
            "input_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ]
        .iter()
        .map(|key| usage[key].as_u64().unwrap_or(0))
        .sum()
    })
}

fn usage(value: &Value, context: Option<u64>) -> Value {
    let mut input = 0u64;
    let mut output = 0u64;
    let mut read = 0u64;
    let mut write = 0u64;
    let mut window = None;
    for model in value["modelUsage"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.values())
    {
        input += model["inputTokens"].as_u64().unwrap_or(0);
        output += model["outputTokens"].as_u64().unwrap_or(0);
        read += model["cacheReadInputTokens"].as_u64().unwrap_or(0);
        write += model["cacheCreationInputTokens"].as_u64().unwrap_or(0);
        // Helper models (such as Haiku) share this map; the conversation model has the largest window.
        window = window.max(model["contextWindow"].as_u64());
    }
    let last = &value["usage"];
    let used = prompt_tokens(last).unwrap_or(0);
    json!({"contextUsage":{"tokens":context.unwrap_or(used),"contextWindow":window},"tokens":{"input":input,"output":output,"cacheRead":read,"cacheWrite":write,"total":input+output+read+write},"lastUsage":{"input":last["input_tokens"],"output":last["output_tokens"],"cacheRead":last["cache_read_input_tokens"],"cacheWrite":last["cache_creation_input_tokens"],"totalTokens":used+last["output_tokens"].as_u64().unwrap_or(0)},"estimated_cost_usd":value["total_cost_usd"]})
}

struct Capture {
    root: PathBuf,
    path: Option<PathBuf>,
    file: Option<File>,
    offset: u64,
    identity: files::Identity,
    more: bool,
}
impl Capture {
    fn new(profile: &HarnessConfig, saved: Option<&Resume>, identity: files::Identity) -> Self {
        Self {
            root: profile.home.join("projects"),
            path: saved.map(|s| s.path.clone()),
            file: None,
            offset: saved.and_then(|s| s.cursor["offset"].as_u64()).unwrap_or(0),
            identity,
            more: false,
        }
    }
    fn read(&mut self) -> io::Result<Vec<Event>> {
        self.more = false;
        let Some(path) = &self.path else {
            return Ok(vec![]);
        };
        if self.file.is_none() {
            self.file = Some(files::open(&self.root, path, self.identity)?);
        }
        let file = self.file.as_mut().unwrap();
        if file.metadata()?.len() < self.offset {
            return Err(io::Error::other("Claude history was truncated"));
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut reader = BufReader::new(file);
        let mut events = vec![];
        let start = self.offset;
        let mut offset = start;
        while offset - start < 65536 {
            let mut raw = String::new();
            let n = (&mut reader)
                .take((MAX_LINE + 1) as u64)
                .read_line(&mut raw)?;
            if n == 0 {
                break;
            }
            if n > MAX_LINE {
                return Err(io::Error::other("Claude native record too large"));
            }
            if !raw.ends_with('\n') {
                break;
            }
            let _: Value = serde_json::from_str(&raw)?;
            events.push(Event::Record {
                kind: "native_record",
                data: json!({"offset":offset}),
                native: Some(raw),
            });
            offset += n as u64;
        }
        self.offset = offset;
        self.more = offset - start >= 65536;
        Ok(events)
    }
}
pub(super) fn recover(
    profile: &HarnessConfig,
    saved: &Resume,
    identity: files::Identity,
    mut emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    validate(profile, &saved.path, &saved.id, identity)?;
    let mut capture = Capture::new(profile, Some(saved), identity);
    loop {
        for event in capture.read()? {
            emit(event)?;
        }
        if !capture.more {
            return Ok(());
        }
    }
}
