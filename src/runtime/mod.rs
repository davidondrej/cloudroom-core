use crate::config::{Config, HarnessConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{process::Command, sync::mpsc};
pub(crate) mod auth;
mod claude;
pub(crate) mod claude_auth;
pub(crate) mod claude_login;
mod claude_skills;
pub(crate) mod claude_version;
mod codex;
mod command_guard;
pub(crate) mod cursor;
pub(crate) mod cursor_auth;
pub(crate) mod cursor_models;
pub mod cursor_print;
mod files;
mod fx;
pub(crate) mod linux;
mod opencode;
mod pi;
pub(crate) mod pi_auth;
mod process;
pub mod terminal;

pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_secs(4);
// Cold Pi handshakes exceeded the ordinary 30s RPC deadline during VM recovery.
const STARTUP_RPC_MULTIPLIER: u32 = 4;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Codex,
    Pi,
    Cursor,
    #[serde(rename = "claude-code")]
    Claude,
    Fx,
    #[serde(rename = "opencode")]
    OpenCode,
}
impl Kind {
    /// Harnesses that accept a per-turn `fast` service tier. Claude Fast stays off (ADR 0133).
    pub fn fast(self) -> bool {
        matches!(self, Kind::Codex | Kind::Cursor)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Model {
    pub model: String,
    pub reasoning_levels: Vec<String>,
}

/// Effort levels the catalog allows for `model`, or `None` when the model is not offered.
/// Claude also runs exact models its catalog omits, such as `claude-opus-5-5[1m]`, so Cloud
/// accepts what Local accepts (ADR 0133) and Claude itself reports an unknown model.
pub fn reasoning_levels<'a>(kind: Kind, models: &'a [Model], model: &str) -> Option<Vec<&'a str>> {
    if let Some(entry) = models.iter().find(|entry| entry.model == model) {
        return Some(entry.reasoning_levels.iter().map(String::as_str).collect());
    }
    (kind == Kind::Claude).then(|| {
        let mut levels = Vec::new();
        for level in models.iter().flat_map(|entry| &entry.reasoning_levels) {
            if !levels.contains(&level.as_str()) {
                levels.push(level.as_str());
            }
        }
        levels
    })
}

/// One detailed answer for "can this catalog run this model at this effort?" (ADR 0123).
pub fn supports(kind: Kind, models: &[Model], model: &str, reasoning: &str) -> Result<(), String> {
    let Some(levels) = reasoning_levels(kind, models, model) else {
        let offered: Vec<&str> = models.iter().map(|entry| entry.model.as_str()).collect();
        return Err(format!(
            "model {model} is not offered on this VM; offered models: {}",
            offered.join(", ")
        ));
    };
    if levels.contains(&reasoning) {
        return Ok(());
    }
    Err(format!(
        "model {model} does not support {reasoning} effort on this VM; supported: {}",
        levels.join(", ")
    ))
}

pub const PI_REASONING_LEVELS: &[&str] = &["none", "minimal", "low", "medium", "high", "xhigh"];
pub use opencode::REASONING_LEVELS as OPENCODE_REASONING_LEVELS;
pub use opencode::import as import_opencode;

pub async fn claude_auth_ready(config: &Config) -> io::Result<bool> {
    claude::auth_ready(config).await
}

pub async fn codex_models(config: &Config) -> (io::Result<Vec<Model>>, Option<Event>) {
    models(config, Kind::Codex).await
}

/// Prove the harness can answer with this exact model before Teleport moves anything.
pub async fn probe(
    config: &Config,
    kind: Kind,
    model: &str,
    reasoning: Option<&str>,
) -> io::Result<()> {
    match kind {
        Kind::Claude => claude::probe(config, model, reasoning).await,
        _ => Ok(()),
    }
}

pub async fn models(config: &Config, kind: Kind) -> (io::Result<Vec<Model>>, Option<Event>) {
    let (handle, mut events) = match Handle::spawn(config, kind, None) {
        Ok(spawned) => spawned,
        Err(error) => return (Err(error), None),
    };
    let drain = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if matches!(event, Event::Exited { .. }) {
                return Some(event);
            }
        }
        None
    });
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        match kind {
            Kind::Codex => codex::models(&handle).await,
            Kind::Claude => claude::model_catalog(&claude::initialize(&handle).await?),
            _ => Err(io::Error::other("harness uses its own model catalog")),
        }
    })
    .await
    .map_err(|_| io::Error::other("model discovery timed out"))
    .and_then(|result| result);
    handle.request_shutdown();
    (result, drain.await.ok().flatten())
}

#[derive(Clone, Debug)]
pub struct Resume {
    pub id: String,
    pub path: PathBuf,
    pub cursor: Value,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<String>,
}

/// A side chat's history: a copy of another session's conversation, cut before user message
/// `before` (Claude Code) or after turn `last_turn_id` (Codex). The source is only read.
pub struct Fork {
    pub source: Resume,
    pub before: Option<String>,
    pub last_turn_id: Option<String>,
}

pub struct ChildRequest {
    pub request: String,
    pub id: String,
    pub tool_call_id: String,
    pub prompt: String,
}

pub enum Event {
    Authentication {
        accepted: bool,
    },
    ChildRequest(ChildRequest),
    Record {
        kind: &'static str,
        data: Value,
        native: Option<String>,
    },
    Started {
        request: String,
        native_turn: Option<String>,
        /// The harness began this turn by itself, without a Cloudroom prompt.
        auto: bool,
    },
    Finished {
        request: String,
        status: String,
        error: Option<String>,
    },
    Compacted {
        status: String,
    },
    Exited {
        reason: &'static str,
        expected: bool,
        cleaned_up: bool,
        details: ExitDetails,
    },
}

/// Stderr is untrusted, local-only diagnostic data, never a native/session record.
/// Intentionally neither Serialize nor Debug: only Observability's private writer formats it.
#[derive(Default)]
pub struct ExitDetails {
    /// Core-side error behind the exit reason (not harness output), shown to users (ADR 0123).
    pub cause: Option<String>,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub stderr_bytes: u64,
    pub stderr_complete: bool,
    pub(crate) stderr: Vec<u8>,
}
impl ExitDetails {
    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }
}

#[derive(Clone, Default)]
struct Progress {
    native: Option<String>,
    request: Option<String>,
    native_turn: Option<String>,
    started: bool,
    finished: bool,
    status: String,
    last_usage: Value,
    last_text: String,
    model: Option<String>,
    processes: HashSet<String>,
    auto: bool,
}
impl Progress {
    fn started(&mut self) -> Option<Event> {
        let request = self.request.clone()?;
        if self.started || self.finished {
            return None;
        }
        self.started = true;
        Some(Event::Started {
            request,
            native_turn: self.native_turn.clone(),
            auto: self.auto,
        })
    }
    fn finished(&mut self, status: &str, error: Option<String>) -> Option<Event> {
        let request = self.request.clone()?;
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(Event::Finished {
            request,
            status: status.into(),
            error,
        })
    }
}

// Protocol differences stay here, not in Session Management or the process driver.
trait Adapter: Send {
    fn validate(&self, _method: &str, _params: &Value) -> io::Result<()> {
        Ok(())
    }
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value;
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        progress: &mut Progress,
    ) -> io::Result<Vec<Event>>;
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)>;
    fn respond(&self, _value: &Value) -> Vec<Value> {
        Vec::new()
    }
    fn capture(&mut self) -> io::Result<Vec<Event>>;
    fn capture_pending(&self) -> bool {
        false
    }
    fn close(&mut self) -> Vec<Value> {
        Vec::new()
    }
    fn closed(&mut self, _value: &Value) -> bool {
        false
    }
    fn clean_exit(&self, code: Option<i32>) -> bool {
        code == Some(0)
    }
}

/// A Git 2.54+ config hook that drops AI agent Co-authored-by lines from every commit message.
fn strip_ai_co_authors_env(command: &mut Command) {
    const HOOK: &str = r#"cloudroom_strip_ai_co_authors() { grep -viE '^co-authored-by:.*(claude|anthropic|cursor|codex|openai|chatgpt|copilot|gemini|aider\.chat|devin-ai|windsurf|codeium)' "$1" > "$1.cloudroom"; [ $? -le 1 ] && mv "$1.cloudroom" "$1" || rm -f "$1.cloudroom"; }; cloudroom_strip_ai_co_authors"#;
    command.envs([
        ("GIT_CONFIG_COUNT", "3"),
        (
            "GIT_CONFIG_KEY_0",
            "hook.cloudroom-strip-ai-co-authors.command",
        ),
        ("GIT_CONFIG_VALUE_0", HOOK),
        (
            "GIT_CONFIG_KEY_1",
            "hook.cloudroom-strip-ai-co-authors.event",
        ),
        ("GIT_CONFIG_VALUE_1", "prepare-commit-msg"),
        (
            "GIT_CONFIG_KEY_2",
            "hook.cloudroom-strip-ai-co-authors.event",
        ),
        ("GIT_CONFIG_VALUE_2", "commit-msg"),
    ]);
}

#[derive(Clone)]
pub struct Handle {
    process: process::Process,
    kind: Kind,
    profile: HarnessConfig,
    repository: PathBuf,
    file_identity: files::Identity,
    resume: Option<Resume>,
    session_file: Option<PathBuf>,
    context_file: Option<PathBuf>,
    reasoning: Option<String>,
    baseline: Arc<Mutex<Option<String>>>,
    /// ACP agent capabilities from `initialize`; empty for other harnesses.
    acp: Arc<Mutex<Value>>,
    rpc_timeout: Duration,
    command_guard_enabled: bool,
    strip_ai_co_authors: bool,
    system_prompt: Option<String>,
    controls: Arc<tokio::sync::Mutex<()>>,
    /// Claude's login when it started; it reads the login only then.
    login: Option<u64>,
}
impl Handle {
    pub fn spawn(
        config: &Config,
        kind: Kind,
        resume: Option<Resume>,
    ) -> io::Result<(Self, mpsc::Receiver<Event>)> {
        Self::spawn_guarded(config, kind, resume, true, true, None)
    }

    pub fn spawn_guarded(
        config: &Config,
        kind: Kind,
        resume: Option<Resume>,
        command_guard_enabled: bool,
        strip_ai_co_authors: bool,
        system_prompt: Option<String>,
    ) -> io::Result<(Self, mpsc::Receiver<Event>)> {
        Self::spawn_inner(
            config,
            kind,
            resume,
            (command_guard_enabled, strip_ai_co_authors),
            system_prompt,
            None,
        )
    }

    /// Claude Code forks through its resume flags; Codex starts fresh and forks in `start_fork`.
    pub fn spawn_fork(
        config: &Config,
        kind: Kind,
        fork: &Fork,
        command_guard_enabled: bool,
        strip_ai_co_authors: bool,
        system_prompt: Option<String>,
    ) -> io::Result<(Self, mpsc::Receiver<Event>)> {
        if kind != Kind::Claude {
            return Self::spawn_guarded(
                config,
                kind,
                None,
                command_guard_enabled,
                strip_ai_co_authors,
                system_prompt,
            );
        }
        let profile = config
            .harnesses
            .get(&kind)
            .ok_or_else(|| io::Error::other("harness not configured"))?;
        let identity = config.storage.as_ref().map(|p| (p.agent_uid, p.agent_gid));
        // An empty checkpoint copies the whole conversation.
        let at = match &fork.before {
            Some(before) => claude::checkpoint_parent(profile, &fork.source, before, identity)?,
            None => String::new(),
        };
        let resume = Some(fork.source.clone());
        Self::spawn_inner(
            config,
            kind,
            resume,
            (command_guard_enabled, strip_ai_co_authors),
            system_prompt,
            Some(&at),
        )
    }

    fn spawn_inner(
        config: &Config,
        kind: Kind,
        resume: Option<Resume>,
        (command_guard_enabled, strip_ai_co_authors): (bool, bool),
        system_prompt: Option<String>,
        fork: Option<&str>,
    ) -> io::Result<(Self, mpsc::Receiver<Event>)> {
        let mut profile = config
            .harnesses
            .get(&kind)
            .cloned()
            .ok_or_else(|| io::Error::other("harness not configured"))?;
        if let Some(saved) = &resume {
            check_resume_history(kind, &profile, saved, config.storage.as_ref())?;
            if let Some(model) = &saved.model {
                profile.model = model.clone();
            }
            if saved.provider.is_some() {
                profile.provider = saved.provider.clone();
            }
        }
        let file_identity = config.storage.as_ref().map(|p| (p.agent_uid, p.agent_gid));
        // Read before the command does, so a login saved in between causes a restart, never a miss.
        let login = matches!(kind, Kind::Claude).then(|| claude::login(config));
        let (command, adapter, session_file, context_file): (_, Box<dyn Adapter>, _, _) = match kind
        {
            Kind::Claude => {
                let (command, id) = claude::command(
                    config,
                    &profile,
                    resume.as_ref(),
                    fork,
                    command_guard_enabled,
                    strip_ai_co_authors,
                    system_prompt.as_deref(),
                )?;
                let saved = resume.as_ref().filter(|_| fork.is_none());
                (
                    command,
                    Box::new(claude::Protocol::new(&profile, id, saved, file_identity)?),
                    None,
                    None,
                )
            }
            Kind::Codex => (
                if command_guard_enabled {
                    command_guard::codex_command(config, &profile)
                } else {
                    codex::command(config, &profile)
                },
                Box::new(codex::Protocol::new(
                    &profile,
                    resume.as_ref(),
                    file_identity,
                )),
                None,
                None,
            ),
            Kind::Cursor => (
                cursor::command(config, &profile)?,
                Box::new(cursor::Protocol::new(
                    config,
                    &profile,
                    resume.as_ref(),
                    cursor::CURSOR,
                )),
                None,
                None,
            ),
            Kind::Fx => (
                fx::command(config, &profile)?,
                Box::new(cursor::Protocol::new(
                    config,
                    &profile,
                    resume.as_ref(),
                    fx::FX,
                )),
                None,
                None,
            ),
            Kind::OpenCode => (
                opencode::command(config, &profile)?,
                Box::new(cursor::Protocol::new(
                    config,
                    &profile,
                    resume.as_ref(),
                    opencode::OPENCODE,
                )),
                None,
                None,
            ),
            Kind::Pi => {
                let (command, path, helper) = pi::command(
                    config,
                    &profile,
                    resume.as_ref(),
                    command_guard_enabled,
                    system_prompt.as_deref(),
                )?;
                (
                    command,
                    Box::new(pi::Protocol::new(
                        &profile,
                        &path,
                        resume.as_ref(),
                        file_identity,
                    )?),
                    Some(path),
                    Some(helper),
                )
            }
        };
        let mut command = command;
        if strip_ai_co_authors {
            strip_ai_co_authors_env(&mut command);
        }
        let (process, events) = process::Process::spawn(config, command, adapter)?;
        Ok((
            Self {
                process,
                kind,
                profile,
                repository: config.repository.clone(),
                file_identity,
                reasoning: resume.as_ref().and_then(|saved| saved.reasoning.clone()),
                baseline: Arc::new(Mutex::new(None)),
                acp: Arc::new(Mutex::new(Value::Null)),
                rpc_timeout: config.rpc_timeout,
                command_guard_enabled,
                strip_ai_co_authors,
                system_prompt,
                controls: Arc::new(tokio::sync::Mutex::new(())),
                login,
                resume,
                session_file,
                context_file,
            },
            events,
        ))
    }
    pub fn with_reasoning(mut self, reasoning: Option<String>) -> Self {
        self.reasoning = reasoning;
        self
    }
    pub(super) fn remember_baseline(&self, level: &str) {
        *self.baseline.lock().unwrap() = Some(level.to_owned());
    }
    pub fn launch_baseline(&self) -> Option<String> {
        self.baseline.lock().unwrap().clone()
    }

    pub fn pid(&self) -> u32 {
        self.process.pid()
    }
    /// A new Claude sign-in or API key was saved after this agent started.
    pub fn login_changed(&self, config: &Config) -> bool {
        self.login
            .is_some_and(|login| login != claude::login(config))
    }
    pub fn process_count(&self) -> Option<usize> {
        self.process.process_count()
    }
    pub fn same_process(&self, other: &Self) -> bool {
        self.process.progress.same_channel(&other.process.progress)
    }
    pub fn model(&self) -> &str {
        &self.profile.model
    }
    pub fn provider(&self) -> Option<&str> {
        self.profile.provider.as_deref()
    }
    pub fn native_identity(&self, id: &str) -> io::Result<Value> {
        let model = self
            .process
            .progress
            .borrow()
            .model
            .clone()
            .unwrap_or_else(|| self.profile.model.clone());
        let mut data = json!({"id":id,"model":model,"provider":self.provider(),"capabilities":self.capabilities()});
        if self.kind == Kind::Claude {
            data["path"] = json!(self.native_location()?);
        }
        Ok(data)
    }
    pub fn capabilities(&self) -> Value {
        if matches!(self.kind, Kind::Cursor | Kind::Fx | Kind::OpenCode) {
            return cursor::capabilities(self.kind);
        }
        json!({"resume":true,"interrupt":true,"system_notice":true,"interactive_dialogs":false,
            "steer":true,"compact":true,"rewind":true,"attachments":true,
            "service_tier":self.kind.fast(),"subagents":true,"usage":true,"goal":self.kind==Kind::Codex})
    }
    pub async fn start_session(&self) -> io::Result<String> {
        let mut startup = self.clone();
        startup.rpc_timeout = self.rpc_timeout * STARTUP_RPC_MULTIPLIER;
        match self.kind {
            Kind::Codex => codex::start(&startup, None).await,
            Kind::Pi => pi::start(&startup).await,
            Kind::Claude => claude::start(&startup).await,
            Kind::Cursor => cursor::start(&startup).await,
            Kind::Fx => fx::start(&startup).await,
            Kind::OpenCode => opencode::start(&startup).await,
        }
    }
    /// Starts a `spawn_fork` harness and returns the side chat's new native ID.
    pub async fn start_fork(&self, fork: &Fork) -> io::Result<String> {
        if self.kind != Kind::Codex {
            return self.start_session().await;
        }
        let mut startup = self.clone();
        startup.rpc_timeout = self.rpc_timeout * STARTUP_RPC_MULTIPLIER;
        codex::start(&startup, Some(fork)).await
    }
    /// Pi confirms the turn's thinking level before the prompt. Codex sends effort with turn/start.
    pub async fn prepare_turn(&self) -> io::Result<()> {
        if self.kind == Kind::Pi
            && let Some(reasoning) = &self.reasoning
        {
            return pi::apply_thinking(self, reasoning).await;
        }
        Ok(())
    }
    pub async fn send(&self, request: &str, text: &str) -> io::Result<()> {
        self.send_prompt(request, &json!({"text": text}))
            .await
            .map(drop)
    }
    /// Delivers a prompt. Returns warnings the user should see, such as a skill that could not load.
    pub async fn send_prompt(&self, request: &str, input: &Value) -> io::Result<Vec<Value>> {
        match self.kind {
            Kind::Codex => codex::send(self, request, input).await.map(|()| Vec::new()),
            Kind::Pi => pi::send(self, request, input).await.map(|()| Vec::new()),
            Kind::Claude => claude::send(self, request, input).await,
            Kind::Cursor | Kind::OpenCode => cursor::send(self, request, input)
                .await
                .map(|()| Vec::new()),
            Kind::Fx => fx::send(self, request, input).await.map(|()| Vec::new()),
        }
    }
    pub async fn steer(&self, request: &str, text: &str) -> io::Result<()> {
        let state = self.process.progress.borrow().clone();
        if state.request.as_deref() != Some(request) || state.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "steer target is no longer active",
            ));
        }
        match self.kind {
            Kind::Codex => codex::steer(self, state, text).await,
            Kind::Pi => pi::steer(self, request, text).await,
            Kind::Claude => claude::steer(self, request, text).await,
            Kind::Cursor | Kind::OpenCode => cursor::steer(self, request, text).await,
            Kind::Fx => fx::steer(self, request, text).await,
        }
    }
    pub async fn compact(&self) -> io::Result<()> {
        match self.kind {
            Kind::Codex => codex::compact(self).await,
            Kind::Pi => pi::compact(self).await,
            Kind::Claude => claude::compact(self).await,
            Kind::Cursor | Kind::Fx | Kind::OpenCode => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ACP compaction is not supported",
            )),
        }
    }
    /// Sets, pauses, resumes, or clears the harness's durable goal as the user.
    pub async fn goal(&self, input: &Value) -> io::Result<()> {
        match self.kind {
            Kind::Codex => codex::goal(self, input).await,
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "goal controls are only supported for Codex",
            )),
        }
    }
    pub async fn rewind(&self, input: &Value) -> io::Result<Value> {
        match self.kind {
            Kind::Codex => codex::rewind(self, input).await,
            Kind::Pi => pi::rewind(self, input).await,
            Kind::Claude => Err(io::Error::other(
                "Claude rewind requires native process replacement",
            )),
            Kind::Cursor | Kind::Fx | Kind::OpenCode => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ACP rewind is not supported",
            )),
        }
    }
    pub fn rewind_replacement(
        &self,
        config: &Config,
        input: &Value,
    ) -> io::Result<Option<(Self, mpsc::Receiver<Event>)>> {
        if self.kind != Kind::Claude {
            return Ok(None);
        }
        let (saved, parent) = match input["before"].as_str() {
            Some(before) => claude::fork_source(self, before)?,
            None if input["fork"] == true => claude::fork_tip(self)?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Claude requires a user-message checkpoint",
                ));
            }
        };
        Self::spawn_inner(
            config,
            self.kind,
            Some(saved),
            (self.command_guard_enabled, self.strip_ai_co_authors),
            self.system_prompt.clone(),
            Some(&parent),
        )
        .map(|(handle, events)| Some((handle.with_reasoning(self.reasoning.clone()), events)))
    }
    pub fn native_location(&self) -> io::Result<PathBuf> {
        match self.kind {
            Kind::Claude => claude::transcript(&self.profile, &self.native()?),
            _ => Err(io::Error::other(
                "native location is delivered by the adapter",
            )),
        }
    }
    pub async fn child_result(&self, request: &str, result: Value) -> io::Result<()> {
        if self.kind == Kind::Claude {
            return claude::child_result(self, request, result).await;
        }
        self.process
            .control(
                "prompt",
                json!({"message":format!("/cloudroom_child_result {}", result)}),
                request,
            )
            .await?;
        Ok(())
    }
    /// The latest reply text this process has streamed, without asking the harness.
    pub fn seen_text(&self) -> String {
        self.process.progress.borrow().last_text.clone()
    }
    pub async fn last_text(&self) -> io::Result<String> {
        if self.kind != Kind::Pi {
            return Ok(self.seen_text());
        }
        let result = self.call("get_last_assistant_text", json!({})).await?;
        Ok(result["text"].as_str().unwrap_or_default().to_owned())
    }
    pub async fn usage(&self) -> io::Result<Option<Value>> {
        match self.kind {
            // A Codex turn interrupted before its first model reply reports no usage.
            Kind::Codex => Ok(Some(self.process.progress.borrow().last_usage.clone())
                .filter(|usage| !usage.is_null())),
            Kind::Pi => pi::usage(self).await,
            Kind::Claude => Ok(Some(self.process.progress.borrow().last_usage.clone())),
            Kind::Cursor | Kind::Fx | Kind::OpenCode => Ok(None),
        }
    }
    pub async fn interrupt(&self, request: &str) -> io::Result<()> {
        let state = self.process.progress.borrow().clone();
        if state.request.as_deref() != Some(request) || state.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interrupt target is no longer active",
            ));
        }
        match self.kind {
            Kind::Codex => codex::interrupt(self, state).await,
            Kind::Pi => pi::interrupt(self, request).await,
            Kind::Claude => claude::interrupt(self, request).await,
            Kind::Cursor | Kind::Fx | Kind::OpenCode => cursor::interrupt(self, state).await,
        }
    }
    pub async fn system_message(&self, text: &str) -> io::Result<()> {
        match self.kind {
            Kind::Codex => codex::notice(self, text).await,
            Kind::Pi => pi::notice(self, text).await,
            Kind::Claude => claude::notice(self, text).await,
            Kind::Cursor | Kind::Fx | Kind::OpenCode => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "ACP context-only notices are unavailable",
            )),
        }
    }
    pub async fn pause(&self, paused: bool) -> io::Result<()> {
        self.process.pause(paused).await
    }
    /// Stops the tool command filling the disk; see `linux::Workload::kill_top_writer`.
    pub fn kill_top_writer(&self) -> io::Result<Option<String>> {
        self.process.kill_top_writer()
    }
    pub fn request_shutdown(&self) {
        self.process.request_shutdown();
    }
    async fn call(&self, method: &str, params: Value) -> io::Result<Value> {
        self.process
            .call_timeout(method, params, None, self.rpc_timeout)
            .await
    }
    fn native(&self) -> io::Result<String> {
        self.process
            .progress
            .borrow()
            .native
            .clone()
            .ok_or_else(|| io::Error::other("session not initialized"))
    }
}

pub fn check_resume_history(
    kind: Kind,
    profile: &HarnessConfig,
    saved: &Resume,
    policy: Option<&crate::workspace::storage::Policy>,
) -> io::Result<()> {
    let identity = policy.map(|p| (p.agent_uid, p.agent_gid));
    match kind {
        Kind::Cursor => return cursor::validate(profile, saved, identity),
        Kind::Fx => return fx::validate(profile, saved, identity),
        Kind::OpenCode => return opencode::validate(profile, saved, identity),
        _ => {}
    }
    let file = files::open(
        &profile.home.join(if kind == Kind::Claude {
            "projects"
        } else {
            "sessions"
        }),
        &saved.path,
        identity,
    )?;
    if file.metadata()?.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "saved conversation is empty",
        ));
    }
    Ok(())
}

pub fn recover_records(
    profile: &HarnessConfig,
    kind: Kind,
    saved: &Resume,
    policy: Option<&crate::workspace::storage::Policy>,
    emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    match kind {
        Kind::Cursor => cursor::recover(profile, saved, policy, emit),
        // fx keeps its own session files; Cloudroom stores the ACP stream only.
        Kind::Fx => fx::validate(profile, saved, policy.map(|p| (p.agent_uid, p.agent_gid))),
        Kind::OpenCode => {
            opencode::validate(profile, saved, policy.map(|p| (p.agent_uid, p.agent_gid)))
        }
        Kind::Codex => codex::recover(
            profile,
            saved,
            policy.map(|p| (p.agent_uid, p.agent_gid)),
            emit,
        ),
        Kind::Pi => pi::recover(
            profile,
            saved,
            policy.map(|p| (p.agent_uid, p.agent_gid)),
            emit,
        ),
        Kind::Claude => claude::recover(
            profile,
            saved,
            policy.map(|p| (p.agent_uid, p.agent_gid)),
            emit,
        ),
    }
}

// Legacy records remain byte-for-byte unchanged on disk. Only their read projection changes.
pub fn for_client(data: &mut Value, native: Option<&str>) -> io::Result<()> {
    if data.get("method").is_some() && data.get("value").is_none() && data.get("harness").is_none()
    {
        codex::for_client(data, native)?;
    }
    Ok(())
}
pub fn checkpoint(
    kind: Kind,
    previous: &Value,
    data: &Value,
    native: Option<&str>,
) -> io::Result<Value> {
    match kind {
        Kind::Codex | Kind::Claude => files::checkpoint(previous, data, native),
        Kind::Pi => pi::checkpoint(data, native),
        Kind::Cursor => cursor::checkpoint(previous, data, native),
        Kind::Fx | Kind::OpenCode => Err(io::Error::other("ACP stream has no native records")),
    }
}

pub(super) fn command(binary: &Path, config: &Config) -> Command {
    // System tools come first; tools the agent installed itself follow (ADR 0119).
    let mut path = std::ffi::OsString::from("/usr/local/bin:/usr/bin:/bin");
    for folder in [".local/bin", ".cargo/bin", "go/bin"] {
        path.push(":");
        path.push(config.account_home.join(folder));
    }
    let mut command = Command::new(binary);
    command.env_clear();
    // The user's own variables from Cloud environment settings, one NAME=value per line. Read at every
    // start, so saved changes reach new agents at once. Core's own values below win over them.
    let saved = std::fs::read_to_string(config.state_dir.join("environment")).unwrap_or_default();
    for (name, value) in saved.lines().filter_map(|line| line.split_once('=')) {
        if !name.is_empty() && !name.contains('\0') && !value.contains('\0') {
            command.env(name, value);
        }
    }
    command
        .env("PATH", path)
        .env("HOME", &config.account_home)
        .env("LANG", "C.UTF-8")
        .current_dir(&config.repository)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

pub async fn reconcile(config: &Config) -> io::Result<()> {
    if let Some(policy) = &config.storage {
        linux::Workload::clear(policy).await?;
    }
    Ok(())
}

/// Unprotected tests never signal a saved PID: ownership cannot be proved after restart.
pub async fn wait_for_exit(pid: u32) -> io::Result<()> {
    tokio::time::timeout(SHUTDOWN_GRACE, async {
        loop {
            let output = Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "stat="])
                .kill_on_drop(true)
                .output()
                .await?;
            if output.status.code() == Some(1)
                && output.stderr.is_empty()
                && output.stdout.is_empty()
                || output.status.success()
                    && String::from_utf8_lossy(&output.stdout)
                        .trim_start()
                        .starts_with('Z')
            {
                return Ok(());
            }
            if !output.status.success() {
                return Err(io::Error::other("previous process ownership is unknown"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("previous process has not exited; refusing replacement"))?
}
