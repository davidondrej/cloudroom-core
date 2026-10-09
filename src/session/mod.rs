mod children;
pub use children::{ForkFrom, Spawn};
mod outbox;
pub mod teleport;
pub use outbox::Journal;
mod accounts;
mod catalog;
mod guard;
mod history;
mod prompt;
pub mod report;
mod sleep;
mod state;
pub mod thread;

use crate::{
    config::Config,
    observability::{Observability, Signal, elapsed_ms, metrics::AgentCounts, now_ms},
    runtime,
    workspace::storage::{Guard, Level, Snapshot},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc, watch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub sequence: u64,
    pub session_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native: Option<String>,
}

impl Record {
    // Preserve the existing wire format without storing params twice alongside the native frame.
    fn for_client(mut self) -> Result<Self> {
        runtime::for_client(&mut self.data, self.native.as_deref())?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub request_id: String,
    pub command: String,
    pub input: Value,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<crate::workspace::Workspace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    #[serde(default)]
    pub teleport_output: bool,
    pub harness: runtime::Kind,
    pub capabilities: Value,
    pub native_cursor: Value,
    pub provider: Option<String>,
    pub native_id: Option<String>,
    pub native_path: Option<std::path::PathBuf>,
    pub native_offset: u64,
    pub state: String,
    pub current_request: Option<String>,
    pub current_turn: Option<String>,
    pub startup_error: Option<String>,
    /// The detailed reason recorded with `startup_error`, shown to users verbatim.
    pub startup_reason: Option<String>,
    // Why the session failed or lost its process, with harness stderr, for `thread output`.
    #[serde(skip)]
    failure: Option<String>,
    #[serde(skip)]
    has_dispatched: bool,
    pub last_sequence: u64,
    pub receipts: BTreeMap<String, Receipt>,
    // Ordered prompts accepted while busy, delivered one turn at a time.
    pub queue: Vec<String>,
    #[serde(default)]
    pub prompts: BTreeMap<String, prompt::PromptState>,
    pub storage_paused: bool,
    pub queue_paused: bool,
    #[serde(default)]
    pub compacting: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Value>,
    pub rewind_request: Option<String>,
    pub parent_session: Option<String>,
    // Archived by `cloudroom thread archive`; `thread list` hides it unless asked.
    #[serde(default)]
    pub archived: bool,
    pub reasoning: Option<String>,
    pub workspace: Option<crate::workspace::Workspace>,
    #[serde(skip)]
    last_activity: Option<u64>,
    #[serde(skip)]
    last_prompt_state: Option<String>,
    #[serde(skip)]
    last_prompt_error: Option<String>,
    #[serde(skip)]
    model: Option<String>,
    #[serde(skip)]
    storage_warned: bool,
    #[serde(skip)]
    handle: Option<runtime::Handle>,
    #[serde(skip)]
    ready: bool,
    #[serde(skip)]
    recovery_attempted: bool,
    #[serde(skip)]
    close_request: Option<String>,
    // Last harness pid, so a later instance can reconcile ownership before resuming.
    #[serde(skip)]
    harness_pid: Option<u32>,
    // The latest turn stopped on a subscription usage limit.
    #[serde(skip)]
    usage_limited: bool,
    #[serde(skip)]
    awake: Option<sleep::Awake>,
    // The harness was asked to exit so this idle session can sleep.
    #[serde(skip)]
    releasing: bool,
    // A child's latest completed replies by request, so a queued turn cannot hide them from its parent.
    #[serde(skip)]
    replies: Vec<(String, String)>,
}

impl Session {
    fn dashboard_state(&self) -> &'static str {
        if self.storage_paused {
            return "waiting";
        }
        match self.state.as_str() {
            "starting" | "resuming" => "queued",
            "starting_turn" | "running" | "interrupting" | "closing" => "working",
            "idle" => match self.last_prompt_state.as_deref() {
                Some("failed") => "failed",
                Some("unknown" | "unknown_after_restart") => "unknown",
                _ => "waiting",
            },
            "closed" => "stopped",
            "sleeping" => "sleeping",
            "failed" | "process_lost" => "failed",
            _ => "unknown",
        }
    }

    fn command_guard_enabled(&self) -> bool {
        self.receipts
            .values()
            .find(|r| r.command == "start")
            .and_then(|r| r.input["command_guard_enabled"].as_bool())
            .unwrap_or(true)
    }

    fn strip_ai_co_authors(&self) -> bool {
        self.receipts
            .values()
            .find(|r| r.command == "start")
            .and_then(|r| r.input["strip_ai_co_authors"].as_bool())
            .unwrap_or(true)
    }

    fn system_prompt(&self) -> Option<String> {
        self.receipts
            .values()
            .find(|r| r.command == "start")
            .and_then(|r| r.input["system_prompt"].as_str())
            .map(str::to_owned)
    }

    fn resume(&self) -> Option<runtime::Resume> {
        Some(runtime::Resume {
            id: self.native_id.clone()?,
            path: self.native_path.clone()?,
            cursor: self.native_cursor.clone(),
            model: self.model.clone(),
            provider: self.provider.clone(),
            reasoning: self.reasoning.clone(),
        })
    }

    fn can_resume(&self) -> bool {
        self.native_id.is_some()
            && self.native_path.is_some()
            && self.close_request.is_none()
            && !matches!(
                self.state.as_str(),
                "closed" | "process_lost" | "failed" | "resuming" | "rewinding"
            )
    }

    fn waiting_start(&self) -> Option<String> {
        if !matches!(self.state.as_str(), "pending" | "waiting_for_files")
            || self.close_request.is_some()
            || self.native_id.is_some()
            || self.harness_pid.is_some()
        {
            return None;
        }
        self.receipts
            .values()
            .find(|r| r.command == "start" && r.state == "accepted")
            .map(|r| r.request_id.clone())
    }

    fn interrupt_pending(&self) -> bool {
        self.receipts.values().any(|receipt| {
            matches!(receipt.command.as_str(), "interrupt" | "stop")
                && matches!(receipt.state.as_str(), "accepted" | "delivered")
        })
    }
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    Conflict(&'static str),
    /// Carries the underlying cause so API clients can see why storage failed.
    Storage(String),
}
impl Error {
    /// Human-readable cause for API bodies that report failures as data.
    pub fn message(&self) -> String {
        match self {
            Self::NotFound => "session not found".into(),
            Self::Conflict(message) => (*message).into(),
            Self::Storage(detail) => format!("storage unavailable ({detail})"),
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Storage(error.to_string())
    }
}
const JOURNAL_UNWRITABLE: &str = "session journal is not writable";
type Result<T> = std::result::Result<T, Error>;

struct Local {
    journal: Journal,
    sessions: BTreeMap<String, Session>,
    // Status snapshots must not clone the potentially large replay index.
    sequences: BTreeMap<String, Vec<u64>>,
    changed: watch::Sender<u64>,
    database_available: bool,
    // Set by a drain before the sandbox stops; refuses new work until released.
    draining: bool,
    // The journal position of the latest replay snapshot (or failed attempt).
    snapshot_at: u64,
}

pub struct Manager {
    pub(crate) config: Config,
    local: Arc<Mutex<Local>>,
    history: history::History,
    pub(crate) observability: Observability,
    pub(crate) storage: Guard,
    pub(crate) workspaces: crate::workspace::Workspaces,
    pub(crate) sync: crate::sync::Sync,
    pub(crate) previews: Arc<crate::preview::Previews>,
    pub(crate) terminals: runtime::terminal::Terminals,
    pub(crate) mac: crate::mac::Mac,
    pub(crate) secrets: crate::secrets::Secrets,
    codex_models: tokio::sync::Mutex<Option<(Instant, Vec<runtime::Model>)>>,
    claude_models: tokio::sync::Mutex<Option<(Instant, Vec<runtime::Model>)>>,
    cursor_models: tokio::sync::Mutex<Option<(Instant, Vec<runtime::Model>)>>,
    codex_auth: runtime::auth::CodexAuth,
    claude_auth: Arc<runtime::claude_auth::ClaudeAuth>,
    claude_login: runtime::claude_login::ClaudeLogin,
    cursor_auth: runtime::cursor_auth::CursorAuth,
    stopping: AtomicBool,
    startups: Semaphore,
    teleports: teleport::Transfers,
}

impl Manager {
    pub fn open(config: Config) -> io::Result<Arc<Self>> {
        let mut local = Local::open(&config.state_dir)?;
        // Resume conversations, not uncertain actions. An interrupted resume attempt
        // is left failed instead of creating a service-restart recovery loop.
        for session in local.sessions.values().cloned().collect::<Vec<_>>() {
            if session.workspace.is_none() && !session.receipts.is_empty() {
                local.append(
                    &session.session_id,
                    "workspace",
                    json!({"id":"legacy","path":config.repository}),
                    None,
                )?;
            }
            if session.waiting_start().is_some() {
                continue;
            }
            let eligible = session.can_resume()
                || session.startup_error.as_deref() == Some("startup_timeout")
                || (session.state == "resuming"
                    && !session.has_dispatched
                    && session.native_path.is_some());
            for mut receipt in session.receipts.values().cloned() {
                let terminal = matches!(
                    receipt.state.as_str(),
                    "completed" | "interrupted" | "failed" | "unknown" | "unknown_after_restart"
                );
                if terminal {
                    continue;
                }
                let queued =
                    receipt.command == "prompt" && session.queue.contains(&receipt.request_id);
                if !eligible || !queued {
                    receipt.state = if queued {
                        "failed"
                    } else {
                        "unknown_after_restart"
                    }
                    .into();
                    local.append(
                        &session.session_id,
                        "receipt",
                        serde_json::to_value(receipt)?,
                        None,
                    )?;
                }
            }
            if !matches!(
                session.state.as_str(),
                "closed" | "process_lost" | "failed" | "sleeping"
            ) {
                local.append(
                    &session.session_id,
                    "state",
                    json!({"state":if eligible { "suspended" } else { "process_lost" }}),
                    None,
                )?;
            }
        }
        // Native recovery runs helpers (Cursor capture) inside the agent cgroup, so prepare it first.
        let storage = Guard::new(&config)?;
        for session in local.sessions.values().cloned().collect::<Vec<_>>() {
            if let Some(saved) = session.resume()
                && let Some(profile) = config.harnesses.get(&session.harness)
                && let Err(error) = runtime::recover_records(
                    profile,
                    session.harness,
                    &saved,
                    config.storage.as_ref(),
                    |event| {
                        if let runtime::Event::Record { kind, data, native } = event {
                            local.append(&session.session_id, kind, data, native)?;
                        }
                        Ok(())
                    },
                )
            {
                local.append(
                    &session.session_id,
                    "native_history_unavailable",
                    json!({"reason":format!("native history missing, changed or unreadable: {error}")}),
                    None,
                )?;
                if session.can_resume() {
                    local.append(
                        &session.session_id,
                        "state",
                        json!({"state":"process_lost"}),
                        None,
                    )?;
                    local
                        .fail_pending(&session.session_id)
                        .map_err(|_| io::Error::other("cannot record native recovery failure"))?;
                }
            }
        }
        local.save_snapshot();
        let workspaces = crate::workspace::Workspaces::open(&config)?;
        let sync = crate::sync::Sync::open(&config)?;
        let previews = crate::preview::Previews::open(&config)?;
        let history = history::History::new(&config)?;
        let local = Arc::new(Mutex::new(local));
        let registry = local.clone();
        let observability = Observability::start(&config, history.pool.clone(), move || {
            registry.lock().unwrap().agent_counts()
        });
        Ok(Arc::new(Self {
            config,
            local,
            history,
            observability,
            storage,
            workspaces,
            sync,
            teleports: teleport::Transfers::default(),
            previews,
            terminals: Default::default(),
            mac: crate::mac::Mac::default(),
            secrets: crate::secrets::Secrets::default(),
            codex_models: tokio::sync::Mutex::new(None),
            claude_models: tokio::sync::Mutex::new(None),
            cursor_models: tokio::sync::Mutex::new(None),
            codex_auth: runtime::auth::CodexAuth::default(),
            claude_auth: Arc::new(runtime::claude_auth::ClaudeAuth::default()),
            claude_login: runtime::claude_login::ClaudeLogin::default(),
            cursor_auth: runtime::cursor_auth::CursorAuth::default(),
            stopping: AtomicBool::new(false),
            // Bound cold-start CPU contention, not the number of running sessions.
            startups: Semaphore::new(
                std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1)),
            ),
        }))
    }

    /// The session whose live harness process is one of `pids`.
    pub(crate) fn harness_session(&self, pids: &[u32]) -> Option<String> {
        let local = self.local.lock().unwrap();
        pids.iter().find_map(|pid| {
            local
                .sessions
                .values()
                .find(|s| s.handle.as_ref().is_some_and(|h| h.pid() == *pid))
                .map(|s| s.session_id.clone())
        })
    }

    /// Appends a record that no harness produced, such as a secret request.
    pub(crate) fn note(&self, session: &str, kind: &str, data: Value) -> io::Result<()> {
        self.local
            .lock()
            .unwrap()
            .append(session, kind, data, None)
            .map(drop)
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.local.lock().unwrap().changed.subscribe()
    }

    pub(crate) fn recording_available(&self) -> bool {
        self.local.lock().unwrap().journal.writable()
    }

    pub async fn ready(&self) -> bool {
        !self.is_stopping()
            && !self.storage.blocks()
            && self.recording_available()
            && self.history.ready().await
    }

    pub fn saving(&self) -> Value {
        let local = self.local.lock().unwrap();
        json!({"locally_recorded_through":local.journal.last(),"externally_saved_through":local.journal.saved(),
            "pending_records":local.journal.last()-local.journal.saved(),"last_upload_succeeded":local.database_available})
    }

    pub async fn records(&self, session: &str, after: u64) -> Result<Vec<Record>> {
        {
            let local = self.local.lock().unwrap();
            if let Some(sequences) = local.sequences.get(session) {
                return sequences[sequences.partition_point(|id| *id <= after)..]
                    .iter()
                    .take(256)
                    .map(|sequence| {
                        let record: Record =
                            serde_json::from_slice(&local.journal.read(*sequence)?)
                                .map_err(io::Error::other)?;
                        record.for_client()
                    })
                    .collect();
            }
        }
        self.history
            .read(session, after)
            .await
            .map_err(|e| Error::Storage(format!("history database: {e}")))?
            .into_iter()
            .map(Record::for_client)
            .collect()
    }

    pub async fn session(&self, id: &str) -> Result<Session> {
        if let Some(session) = self.local.lock().unwrap().sessions.get(id).cloned() {
            return Ok(session);
        }
        self.history
            .summary(id, None)
            .await
            .map_err(|e| Error::Storage(format!("history database: {e}")))?
            .0
            .ok_or(Error::NotFound)
    }

    pub fn recovery_check(&self, session: &Session) -> Value {
        let (status, message) = if session.close_request.is_some()
            || session.state == "closed"
            || session.state == "saved_history_only"
        {
            (
                "unavailable",
                "This session is closed or has saved history only",
            )
        } else if let Some(saved) = session.resume()
            && let Some(profile) = self.config.harnesses.get(&session.harness)
        {
            match runtime::check_resume_history(
                session.harness,
                profile,
                &saved,
                self.config.storage.as_ref(),
            ) {
                Ok(()) => (
                    "ready",
                    "Saved conversation is available; the native resume handshake is still required",
                ),
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && !session.has_dispatched =>
                {
                    (
                        "empty",
                        "No turn was dispatched and no saved conversation exists; there is nothing to resume",
                    )
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (
                    "missing",
                    "Saved conversation is missing; do not create a replacement or replay work",
                ),
                Err(_) => ("unavailable", "Saved conversation could not be read safely"),
            }
        } else {
            (
                "unavailable",
                "No saved native conversation identity is available",
            )
        };
        json!({"status":status,"message":message})
    }

    fn retry(
        session: &Session,
        request: &str,
        command: &str,
        input: &Value,
    ) -> Result<Option<Receipt>> {
        match session.receipts.get(request) {
            Some(receipt) if receipt.command != command || receipt.input != *input => {
                Err(Error::Conflict("request_id already has different content"))
            }
            other => Ok(other.cloned()),
        }
    }

    /// Match cloud Claude Code to the Mac's version (ADR 0133); waits while Claude runs.
    pub async fn match_claude_version(&self, version: String) -> Result<Value> {
        if !runtime::claude_version::valid(&version) {
            return Err(Error::Conflict("invalid version"));
        }
        if !self.config.harnesses.contains_key(&runtime::Kind::Claude) {
            return Err(Error::Conflict("harness is not configured"));
        }
        let busy = self.local.lock().unwrap().sessions.values().any(|s| {
            s.harness == runtime::Kind::Claude
                && (s.handle.is_some() || matches!(s.state.as_str(), "starting" | "resuming"))
        });
        Ok(runtime::claude_version::ensure(self.config.clone(), version, busy).await)
    }

    /// Account checks shared by new sessions and Teleport's preflight.
    async fn harness_ready(self: &Arc<Self>, kind: runtime::Kind) -> Result<()> {
        if kind == runtime::Kind::Claude {
            match self
                .claude_auth
                .ready(&self.config, &self.observability)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return Err(Error::Conflict(
                        "connect Claude Code before starting cloud work",
                    ));
                }
                Err(_) => return Err(Error::Conflict("Claude account could not be verified")),
            }
        }
        if kind == runtime::Kind::Cursor {
            match self.cursor_auth_status().await.state {
                "connected" => {}
                "unavailable" => {
                    return Err(Error::Conflict("Cursor account could not be verified"));
                }
                _ => return Err(Error::Conflict("connect Cursor before starting cloud work")),
            }
        }
        if kind == runtime::Kind::Codex {
            match self.codex_auth_status().await.state {
                "connected" => {}
                "missing" | "waiting" | "expired" | "error" => {
                    return Err(Error::Conflict("connect Codex before starting cloud work"));
                }
                "limited" => return Err(Error::Conflict("Codex usage limit reached")),
                _ => return Err(Error::Conflict("Codex account could not be verified")),
            }
        }
        Ok(())
    }

    /// Model and effort checks shared by new sessions and Teleport's preflight.
    async fn execution_supported(
        &self,
        kind: runtime::Kind,
        model: Option<&String>,
        reasoning: &str,
    ) -> Result<()> {
        let supported = match kind {
            runtime::Kind::Codex | runtime::Kind::Claude | runtime::Kind::Cursor => {
                let selected = model.unwrap_or(&self.config.harnesses[&kind].model);
                self.reasoning_supported(kind, selected, reasoning).await?
            }
            runtime::Kind::Pi | runtime::Kind::Fx => {
                runtime::PI_REASONING_LEVELS.contains(&reasoning)
            }
            runtime::Kind::OpenCode => runtime::OPENCODE_REASONING_LEVELS.contains(&reasoning),
        };
        if !supported {
            return Err(Error::Conflict("invalid reasoning effort"));
        }
        Ok(())
    }

    pub async fn start(
        self: &Arc<Self>,
        request: String,
        harness: Option<runtime::Kind>,
        model: Option<String>,
        reasoning: Option<String>,
        (workspace, workspace_name): (Option<String>, Option<String>),
        (provider, command_guard_enabled, strip_ai_co_authors, system_prompt): (
            Option<String>,
            Option<bool>,
            Option<bool>,
            Option<String>,
        ),
    ) -> Result<(String, Receipt)> {
        let id = format!("cr_{request}");
        let kind = harness
            .or_else(|| {
                self.local
                    .lock()
                    .unwrap()
                    .sessions
                    .get(&id)
                    .map(|s| s.harness)
            })
            .unwrap_or(self.config.default_harness);
        let mut input = if kind == runtime::Kind::Codex {
            json!({})
        } else {
            json!({"harness":kind})
        };
        if command_guard_enabled == Some(false) {
            input["command_guard_enabled"] = json!(false);
        }
        if strip_ai_co_authors == Some(false) {
            input["strip_ai_co_authors"] = json!(false);
        }
        if let Some(system_prompt) = &system_prompt {
            input["system_prompt"] = json!(system_prompt);
        }
        if let Some(model) = &model {
            input["model"] = json!(model);
        }
        if let Some(provider) = &provider {
            if kind != runtime::Kind::Pi {
                return Err(Error::Conflict("provider selection requires Pi"));
            }
            input["provider"] = json!(provider);
        }
        if let Some(reasoning) = &reasoning {
            input["reasoning"] = json!(reasoning);
        }
        if let Some(workspace) = &workspace {
            crate::workspace::valid_id(workspace)
                .map_err(|_| Error::Conflict("invalid workspace"))?;
            input["workspace"] = json!(workspace);
        }
        if let Some(name) = &workspace_name {
            if workspace.is_none() || crate::workspace::valid_name(name).is_err() {
                return Err(Error::Conflict("invalid workspace"));
            }
            input["workspace_name"] = json!(name);
        }
        if let Some(session) = self.local.lock().unwrap().sessions.get(&id) {
            return Self::retry(session, &request, "start", &input)?
                .map(|r| (id.clone(), r))
                .ok_or(Error::Conflict("session already exists"));
        }
        if !self.recording_available() {
            return Err(Error::Storage(JOURNAL_UNWRITABLE.into()));
        }
        if self.storage.blocks() {
            return Err(Error::Conflict("storage unsafe; new execution is blocked"));
        }
        if self.config.harnesses.is_empty() {
            return Err(Error::Conflict("agent setup is incomplete"));
        }
        // The history, login, and model checks run together; their results apply in this order.
        let configured = self.config.harnesses.contains_key(&kind);
        let (history, ready, supported) = tokio::join!(
            self.history.summary(&id, Some(&request)),
            async {
                if configured {
                    self.harness_ready(kind).await
                } else {
                    Ok(())
                }
            },
            async {
                match &reasoning {
                    Some(reasoning) if configured => {
                        self.execution_supported(kind, model.as_ref(), reasoning)
                            .await
                    }
                    _ => Ok(()),
                }
            },
        );
        // Fresh-state retries must not recreate an externally saved session.
        let (external, receipt) =
            history.map_err(|e| Error::Storage(format!("history database: {e}")))?;
        if let Some(external) = external {
            if harness.is_some_and(|kind| kind != external.harness) {
                return Err(Error::Conflict("request_id already has different content"));
            }
            let receipt =
                receipt.ok_or(Error::Conflict("saved session has no matching receipt"))?;
            if receipt.input != input {
                return Err(Error::Conflict("request_id already has different content"));
            }
            return Ok((id, receipt));
        }
        if !configured {
            return Err(Error::Conflict("harness is not configured"));
        }
        ready?;
        supported?;
        let workspace = match workspace {
            // Unregistered like legacy sessions, so no project folder is created.
            Some(id) if id == crate::workspace::ROOT => crate::workspace::Workspace {
                id: "legacy".into(),
                path: self.workspaces.root().to_path_buf(),
                parent: None,
            },
            Some(id) => self
                .workspaces
                .resolve(&id, workspace_name.as_deref())
                .map_err(|_| Error::Conflict("workspace mapping unavailable"))?,
            None => crate::workspace::Workspace {
                id: "legacy".into(),
                path: self.config.repository.clone(),
                parent: None,
            },
        };
        let receipt = Receipt {
            request_id: request.clone(),
            command: "start".into(),
            input,
            state: "accepted".into(),
            model: Some(model.unwrap_or_else(|| self.config.harnesses[&kind].model.clone())),
            provider: provider.or_else(|| self.config.harnesses[&kind].provider.clone()),
            workspace: Some(workspace),
            error: None,
        };
        {
            let mut local = self.local.lock().unwrap();
            if let Some(session) = local.sessions.get(&id) {
                return Self::retry(session, &request, "start", &receipt.input)?
                    .map(|r| (id.clone(), r))
                    .ok_or(Error::Conflict("session already exists"));
            }
            if self.is_stopping() {
                return Err(Error::Conflict("service is stopping"));
            }
            if local.draining {
                return Err(Error::Conflict("service is draining"));
            }
            if self.storage.blocks() {
                return Err(Error::Conflict("storage unsafe; new execution is blocked"));
            }
            local.append(
                &id,
                "receipt",
                serde_json::to_value(&receipt).map_err(io::Error::other)?,
                None,
            )?;
        }
        let manager = self.clone();
        let session = id.clone();
        tokio::spawn(async move {
            manager.launch(session, request).await;
        });
        Ok((id, receipt))
    }

    async fn launch(self: Arc<Self>, id: String, request: String) {
        let started = Instant::now();
        let Ok(_startup) = self.startups.acquire().await else {
            return;
        };
        let workspace = loop {
            let workspace = {
                let local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                if self.is_stopping() || session.close_request.is_some() {
                    return;
                }
                session.workspace.clone()
            };
            if !self.storage.blocks() {
                break workspace;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if self.is_stopping() {
            return;
        }
        let mut failure_reason = "Cloud folder could not be opened or created; check its permissions, symlinks, and available disk space";
        let result = async {
            let workspace = workspace.ok_or_else(|| io::Error::other("session workspace missing"))?;
            self.workspaces.ensure_directory(&workspace).await?;
            failure_reason = "Harness initialization failed; inspect native history and protected local harness diagnostics for this session";
            let fork;
            let (handle, events) = {
                let mut local = self.local.lock().unwrap();
                if self.is_stopping() {
                    return Err(io::Error::other("service is stopping"));
                }
                if local.sessions[&id].close_request.is_some() { return Ok(()); }
                let session = &local.sessions[&id];
                let mut config = self.config.clone();
                failure_reason = "The selected harness is no longer configured";
                let profile = config.harnesses.get_mut(&session.harness)
                    .ok_or_else(|| io::Error::other(failure_reason))?;
                if let Some(model) = &session.model { profile.model = model.clone(); }
                if let Some(provider) = &session.provider { profile.provider = Some(provider.clone()); }
                failure_reason = "Harness initialization failed; inspect native history and protected local harness diagnostics for this session";
                config.repository = session.workspace.as_ref().ok_or_else(|| io::Error::other("session workspace missing"))?.path.canonicalize()?;
                let (kind, reasoning) = (session.harness, session.reasoning.clone());
                let command_guard_enabled = session.command_guard_enabled();
                let strip_ai_co_authors = session.strip_ai_co_authors();
                let system_prompt = session.system_prompt();
                failure_reason = "The side chat's source conversation could not be copied";
                fork = children::fork_history(&local, &id, &request)?;
                failure_reason = "Harness initialization failed; inspect native history and protected local harness diagnostics for this session";
                // Claim before spawning: a crash after this point must not repeat uncertain execution.
                local.append(&id, "state", json!({"state":"starting"}), None)?;
                let (handle, events) = match &fork {
                    Some(fork) => runtime::Handle::spawn_fork(&config, kind, fork, command_guard_enabled, strip_ai_co_authors, system_prompt)?,
                    None => runtime::Handle::spawn_guarded(&config, kind, None, command_guard_enabled, strip_ai_co_authors, system_prompt)?,
                };
                let handle = handle.with_reasoning(reasoning);
                let pid = handle.pid();
                local.sessions.get_mut(&id).unwrap().handle = Some(handle.clone());
                local.append(&id, "harness", json!({"pid":pid}), None)?;
                (handle, events)
            };
            self.watch_events(id.clone(), handle.clone(), events);
            let native = match &fork {
                Some(fork) => handle.start_fork(fork).await?,
                None => handle.start_session().await?,
            };
            let mut local = self.local.lock().unwrap();
            self.remember_launch_reasoning(&mut local, &id, &handle)?;
            local.append(&id, "native_identity", handle.native_identity(&native)?, None)?;
            if local.sessions[&id].handle.is_none() {
                return Err(io::Error::other("harness exited during initialization"));
            }
            local.sessions.get_mut(&id).unwrap().ready = true;
            local.sessions.get_mut(&id).unwrap().awaken();
            local.append(&id, "state", json!({"state":"idle"}), None)?;
            local
                .finish_receipt(&id, &request, "completed")
                .map_err(|_| io::Error::other("cannot record startup"))?;
            // A stop accepted before the first harness existed only pauses the queue.
            let stops: Vec<_> = local.sessions[&id].receipts.values()
                .filter(|r| r.command == "stop" && r.state == "accepted")
                .map(|r| r.request_id.clone()).collect();
            for stop in stops {
                local.finish_receipt(&id, &stop, "completed")
                    .map_err(|_| io::Error::other("cannot record startup stop"))?;
            }
            self.advance(&mut local, &id, true);
            Ok::<_, io::Error>(())
        }
        .await;
        self.observability.record(Signal::AgentStart {
            session_id: id.clone(),
            success: result.is_ok(),
            duration_ms: elapsed_ms(started),
        });
        if let Err(error) = &result {
            let cause = error.to_string();
            let reason = if cause == failure_reason {
                cause
            } else {
                format!("{failure_reason}. Cause: {cause}")
            };
            let mut local = self.local.lock().unwrap();
            let _ = local.finish_receipt(&id, &request, "failed");
            let _ = local.append(
                &id,
                "state",
                json!({"state":"failed","reason":reason}),
                None,
            );
            let _ = local.fail_pending(&id);
            if let Some(handle) = local.sessions[&id].handle.as_ref() {
                handle.request_shutdown();
            }
        }
    }

    pub fn known_request(&self, id: &str, request: &str) -> bool {
        self.local
            .lock()
            .unwrap()
            .sessions
            .get(id)
            .is_some_and(|session| session.receipts.contains_key(request))
    }

    pub fn receipt(&self, id: &str, request: &str) -> Option<Receipt> {
        self.local
            .lock()
            .unwrap()
            .sessions
            .get(id)
            .and_then(|session| session.receipts.get(request))
            .cloned()
    }

    pub fn command(
        self: &Arc<Self>,
        id: &str,
        request: String,
        command: &str,
        input: Value,
    ) -> Result<Receipt> {
        enum Next {
            None,
            Deliver(runtime::Handle, String),
            Interrupt(runtime::Handle, String),
            Steer(runtime::Handle, String, String),
            Compact(runtime::Handle, String),
            Goal(runtime::Handle, Value, String),
            Rewind(runtime::Handle, Value, String),
            Close(Option<runtime::Handle>),
            Resume,
        }
        let (receipt, next) = {
            let mut local = self.local.lock().unwrap();
            let session = local.sessions.get(id).ok_or(Error::NotFound)?;
            if let Some(receipt) = Self::retry(session, &request, command, &input)? {
                return Ok(receipt);
            }
            if !local.journal.writable() {
                return Err(Error::Storage(JOURNAL_UNWRITABLE.into()));
            }
            if self.is_stopping() {
                return Err(Error::Conflict("service is stopping"));
            }
            if command == "sleep" {
                return self.request_sleep(&mut local, id, request);
            }
            if local.draining && !matches!(command, "interrupt" | "close" | "stop" | "cancel") {
                return Err(Error::Conflict("service is draining"));
            }
            if matches!(command, "compact" | "rewind" | "goal") && session.state == "sleeping" {
                self.schedule_resume(&mut local, id, false)?;
                return Err(Error::Conflict(
                    "this thread was asleep and is waking up; try again in a few seconds",
                ));
            }
            let session = &local.sessions[id];
            if session.rewind_request.is_some() {
                return Err(Error::Conflict("wait for the pending rewind"));
            }
            if session.close_request.is_some()
                || (command != "close" && session.interrupt_pending())
            {
                return Err(Error::Conflict("wait for the pending close or interrupt"));
            }
            if command == "close" && session.state == "starting" {
                return Err(Error::Conflict("session is still starting"));
            }
            if !matches!(
                command,
                "prompt"
                    | "interrupt"
                    | "close"
                    | "stop"
                    | "resume"
                    | "edit"
                    | "cancel"
                    | "reorder"
                    | "steer"
                    | "compact"
                    | "goal"
                    | "rewind"
                    | "attach"
            ) {
                return Err(Error::Conflict("unsupported command"));
            }
            let handle = session.handle.clone();
            let retry_startup = command == "resume"
                && session.state == "process_lost"
                && session.startup_error.is_some()
                && session.current_request.is_none()
                && handle.is_none()
                && session.harness_pid.is_none();
            if retry_startup && self.recovery_check(session)["status"] != "ready" {
                return Err(Error::Conflict(
                    "saved conversation is unavailable; inspect session recovery before retrying",
                ));
            }
            if matches!(session.state.as_str(), "closed" | "process_lost" | "failed")
                && !retry_startup
            {
                return Err(Error::Conflict(
                    "process unavailable; history does not restore execution",
                ));
            }
            // A prompt starts its native turn immediately only when the harness is
            // idle with nothing queued; otherwise it is accepted and queued in order.
            if command == "prompt" && self.storage.blocks() {
                return Err(Error::Conflict("storage unsafe; new execution is blocked"));
            }
            if command == "interrupt" && session.storage_paused {
                return Err(Error::Conflict(
                    "workload is paused for storage; close it or wait for recovery",
                ));
            }
            if matches!(command, "edit" | "cancel") {
                let target = input["target_request_id"].as_str().unwrap_or_default();
                let cancelled = session
                    .prompts
                    .get(target)
                    .is_some_and(|prompt| prompt.cancelled);
                if target.is_empty() {
                    return Err(Error::Conflict("queued message is no longer pending"));
                }
                if command == "edit" || !cancelled {
                    if !session.queue.contains(&target.to_owned()) {
                        return Err(Error::Conflict("queued message is no longer pending"));
                    }
                    if session.current_request.as_deref() == Some(target) {
                        return Err(Error::Conflict("queued message is already being sent"));
                    }
                }
                if command == "edit" && cancelled {
                    return Err(Error::Conflict("queued message is no longer pending"));
                }
                if command == "edit" {
                    let expected = input["expected_revision"].as_u64().unwrap_or(0);
                    let current = session
                        .prompts
                        .get(target)
                        .map(|prompt| prompt.revision)
                        .unwrap_or(1);
                    if expected != current {
                        return Err(Error::Conflict(
                            "queued message changed since editing began",
                        ));
                    }
                }
            }
            if command == "steer"
                && (!session.ready
                    || handle.is_none()
                    || session.current_request.as_deref() != input["target_request_id"].as_str())
            {
                return Err(Error::Conflict(
                    "steer target is no longer the active request",
                ));
            }
            if matches!(command, "compact" | "rewind" | "steer" | "goal")
                && session.capabilities[command] == false
            {
                return Err(Error::Conflict("operation is unsupported by this harness"));
            }
            if command == "compact" {
                if session.compacting {
                    return Err(Error::Conflict("compaction is already running"));
                }
                if session.current_request.is_some()
                    || !matches!(session.state.as_str(), "idle" | "failed")
                {
                    return Err(Error::Conflict(
                        "context can only be compacted while the thread is idle",
                    ));
                }
                if !session.ready || handle.is_none() {
                    return Err(Error::Conflict("harness is not ready for compaction"));
                }
            }
            if command == "goal" && (!session.ready || handle.is_none()) {
                return Err(Error::Conflict(
                    "the agent is starting; try again in a few seconds",
                ));
            }
            if command == "rewind" {
                if local.sessions.values().any(|child| {
                    child.parent_session.as_deref() == Some(id)
                        && (child.current_request.is_some()
                            || !child.queue.is_empty()
                            || matches!(
                                child.state.as_str(),
                                "pending" | "starting" | "resuming" | "waiting_for_files"
                            ))
                }) {
                    return Err(Error::Conflict(
                        "wait for child agents before editing a message",
                    ));
                }
                // A fork (a new Cloud thread from another's conversation) may copy the whole conversation.
                if input["fork"] != true
                    && input["before"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .is_none()
                    && input["last_turn_id"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .is_none()
                {
                    return Err(Error::Conflict("a native rewind checkpoint is required"));
                }
                if let Some(replacement) = input.get("replacement") {
                    let key = replacement["request_id"]
                        .as_str()
                        .ok_or(Error::Conflict("replacement request ID required"))?;
                    if key == request || session.receipts.contains_key(key) {
                        return Err(Error::Conflict("replacement request ID already exists"));
                    }
                }
                if session.compacting {
                    return Err(Error::Conflict("wait for compaction"));
                }
                if !session.queue.is_empty() {
                    return Err(Error::Conflict(
                        "send or remove queued messages before editing a message",
                    ));
                }
                if session.current_request.is_some()
                    || !matches!(session.state.as_str(), "idle" | "failed")
                {
                    return Err(Error::Conflict(
                        "wait for the active turn to finish before editing a sent message",
                    ));
                }
                if !session.ready || handle.is_none() {
                    return Err(Error::Conflict("harness is not ready to rewind"));
                }
            }
            let deliver_now = command == "prompt"
                && session.ready
                && session.state == "idle"
                && !session.queue_paused
                && !session.compacting
                && session.queue.is_empty();
            if command == "interrupt"
                && (!session.ready
                    || handle.is_none()
                    || session.current_request.as_deref() != input["target_request_id"].as_str())
            {
                return Err(Error::Conflict(
                    "interrupt target is no longer the active request",
                ));
            }
            let target = session.current_request.clone();
            let deliver_now = deliver_now && !self.sleep_for_login(&mut local, id);
            let receipt = Receipt {
                request_id: request.clone(),
                command: command.into(),
                input: input.clone(),
                state: "accepted".into(),
                model: None,
                provider: None,
                workspace: None,
                error: None,
            };
            local.append(
                id,
                "receipt",
                serde_json::to_value(&receipt).map_err(io::Error::other)?,
                None,
            )?;
            let next = match command {
                "prompt" if deliver_now => {
                    self.begin_turn(&mut local, id, &request)?;
                    Next::Deliver(handle.expect("ready harness"), request.clone())
                }
                "prompt" => {
                    self.wake(&mut local, id)?;
                    Next::None
                }
                "edit" | "cancel" | "reorder" | "attach" => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::None
                }
                "resume" if retry_startup => {
                    self.schedule_resume(&mut local, id, false)?;
                    Next::None
                }
                "resume" => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::Resume
                }
                "stop" if target.is_none() || handle.is_none() => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::None
                }
                "steer" => Next::Steer(
                    handle.expect("validated live steer target"),
                    input["target_request_id"]
                        .as_str()
                        .expect("validated steer target")
                        .to_owned(),
                    request.clone(),
                ),
                "compact" => {
                    local.append(id, "state", json!({"state":"running"}), None)?;
                    Next::Compact(handle.expect("validated compact harness"), request.clone())
                }
                "goal" => Next::Goal(
                    handle.expect("validated goal harness"),
                    input,
                    request.clone(),
                ),
                "rewind" => Next::Rewind(
                    handle.expect("validated rewind harness"),
                    input,
                    request.clone(),
                ),
                "close" => {
                    let session = &local.sessions[id];
                    let data = json!({"state":"closing","request_id":session.current_request,"turn_id":session.current_turn});
                    local.append(id, "state", data, None)?;
                    local.fail_pending(id)?;
                    if handle.is_none() {
                        local.append(id, "state", json!({"state":"closed"}), None)?;
                        local.finish_receipt(id, &request, "completed")?;
                    }
                    Next::Close(handle)
                }
                _ => Next::Interrupt(
                    handle.expect("validated live interrupt target"),
                    target.expect("validated target"),
                ),
            };
            (receipt, next)
        };
        match next {
            Next::None => {}
            Next::Resume => self.advance(&mut self.local.lock().unwrap(), id, false),
            Next::Close(handle) => {
                if let Some(handle) = handle {
                    handle.request_shutdown();
                }
            }
            Next::Deliver(handle, request) => self.clone().deliver(id.to_owned(), request, handle),
            Next::Steer(handle, target, request) => {
                self.clone().steer(id.to_owned(), request, target, handle)
            }
            Next::Compact(handle, request) => self.clone().compact(id.to_owned(), request, handle),
            Next::Goal(handle, input, request) => {
                let manager = self.clone();
                let id = id.to_owned();
                tokio::spawn(async move {
                    let result = handle.goal(&input).await;
                    let mut local = manager.local.lock().unwrap();
                    if let Err(error) = &result
                        && let Some(receipt) = local
                            .sessions
                            .get_mut(&id)
                            .and_then(|session| session.receipts.get_mut(&request))
                    {
                        receipt.error = Some(error.to_string());
                    }
                    let state = if result.is_ok() {
                        "completed"
                    } else {
                        "failed"
                    };
                    let _ = local.finish_receipt(&id, &request, state);
                });
            }
            Next::Rewind(handle, input, request) => {
                self.clone().rewind(id.to_owned(), request, input, handle)
            }
            Next::Interrupt(handle, target) => {
                let manager = self.clone();
                let id = id.to_owned();
                tokio::spawn(async move {
                    let result = handle.interrupt(&target).await;
                    let state = match &result {
                        Ok(_) => "completed",
                        Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                        Err(_) => "unknown",
                    };
                    let mut local = manager.local.lock().unwrap();
                    if !local.sessions[&id]
                        .handle
                        .as_ref()
                        .is_some_and(|current| current.same_process(&handle))
                    {
                        return;
                    }
                    let _ = local.finish_receipt(&id, &request, state);
                    // A settled interrupt returns the session to idle or the next turn.
                    if matches!(state, "completed" | "failed") {
                        manager.advance(&mut local, &id, false);
                    }
                });
            }
        }
        Ok(receipt)
    }

    /// Pi's process keeps the last thinking level. Remember the level it started with
    /// so a later message that omits reasoning can put that default back.
    fn remember_launch_reasoning(
        &self,
        local: &mut Local,
        id: &str,
        handle: &runtime::Handle,
    ) -> io::Result<()> {
        if local.sessions[id].reasoning.is_some() {
            return Ok(());
        }
        let Some(level) = handle.launch_baseline() else {
            return Ok(());
        };
        local.append(id, "launch_reasoning", json!({"reasoning": level}), None)?;
        Ok(())
    }

    /// Record the start of a queued or immediate turn; dequeues the request.
    fn begin_turn(&self, local: &mut Local, id: &str, request: &str) -> io::Result<()> {
        local.append(
            id,
            "state",
            json!({"state":"starting_turn","request_id":request}),
            None,
        )?;
        Ok(())
    }

    /// Deliver one prompt to the harness and record the outcome. On rejection the
    /// turn never began, so advance to the next queued prompt.
    fn deliver(self: Arc<Self>, id: String, request: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let (input, reasoning) = {
                let local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                let input = prompt::latest(&session.prompts, &session.receipts, &request)
                    .cloned()
                    .unwrap_or_else(|| json!({"text":""}));
                let reasoning = prompt::reasoning(&input).or_else(|| session.reasoning.clone());
                (input, reasoning)
            };
            // The stored handle keeps the launch level. This copy is only for the turn being started.
            let handle = handle.with_reasoning(reasoning);
            let result = async {
                handle.prepare_turn().await?;
                handle.send_prompt(&request, &input).await
            }
            .await;
            let state = match &result {
                Ok(_) => "delivered",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            for warning in result.as_ref().map(Vec::as_slice).unwrap_or_default() {
                let mut data = warning.clone();
                data["request_id"] = json!(request);
                let _ = local.append(&id, "prompt_warning", data, None);
            }
            if let Err(error) = &result
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
            if result.is_err_and(|e| e.kind() == io::ErrorKind::InvalidInput)
                && local.sessions.get(&id).is_some_and(|s| {
                    s.current_request.as_deref() == Some(&request) && s.close_request.is_none()
                })
            {
                self.advance(&mut local, &id, true);
            }
        });
    }

    fn steer(
        self: Arc<Self>,
        id: String,
        request: String,
        target: String,
        handle: runtime::Handle,
    ) {
        tokio::spawn(async move {
            let text = {
                let local = self.local.lock().unwrap();
                local.sessions[&id]
                    .receipts
                    .get(&request)
                    .map(|receipt| {
                        receipt.input["text"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned()
                    })
                    .unwrap_or_default()
            };
            let result = handle.steer(&target, &text).await;
            let state = match &result {
                Ok(_) => "completed",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            if let Err(error) = &result
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
        });
    }

    fn compact(self: Arc<Self>, id: String, request: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let result = handle.compact().await;
            let state = match &result {
                Ok(_) => "delivered",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            if let Err(error) = &result
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
                && !matches!(
                    receipt.state.as_str(),
                    "completed" | "failed" | "interrupted"
                )
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
            if state == "failed" {
                self.advance(&mut local, &id, false);
            }
        });
    }

    fn rewind(self: Arc<Self>, id: String, request: String, input: Value, handle: runtime::Handle) {
        tokio::spawn(async move {
            let result = self.rewind_native(&id, &request, &input, &handle).await;
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            match result {
                Ok(_) => {} // The adapter's ordered rewind_ready event commits the identity before capture or dispatch.
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                    if let Some(receipt) = local
                        .sessions
                        .get_mut(&id)
                        .and_then(|session| session.receipts.get_mut(&request))
                    {
                        receipt.error = Some(error.to_string());
                    }
                    let _ = local.finish_receipt(&id, &request, "failed");
                    let _ = local.append(&id, "rewind_failed", json!({"request_id":request}), None);
                }
                Err(error) => {
                    if let Some(receipt) = local
                        .sessions
                        .get_mut(&id)
                        .and_then(|session| session.receipts.get_mut(&request))
                    {
                        receipt.error = Some(error.to_string());
                    }
                    let _ = local.finish_receipt(&id, &request, "unknown");
                }
            }
        });
    }

    async fn rewind_native(
        self: &Arc<Self>,
        id: &str,
        request: &str,
        input: &Value,
        handle: &runtime::Handle,
    ) -> io::Result<Value> {
        let mut config = self.config.clone();
        {
            let local = self.local.lock().unwrap();
            config.repository = local.sessions[id]
                .workspace
                .as_ref()
                .ok_or_else(|| io::Error::other("session workspace missing"))?
                .path
                .canonicalize()?;
        }
        let Some((replacement, events)) = handle.rewind_replacement(&config, input)? else {
            return handle.rewind(input).await;
        };
        let result = async {
            let native = replacement.start_session().await?;
            let path = replacement.native_location()?;
            handle.request_shutdown();
            // Let the original recorder drain before changing its native-history cursor.
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if self.local.lock().unwrap().sessions[id].handle.is_none() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .map_err(|_| {
                io::Error::other("original harness did not finish recording before rewind")
            })?;
            {
                let mut local = self.local.lock().unwrap();
                if self.is_stopping()
                    || local.sessions[id].rewind_request.as_deref() != Some(request)
                {
                    return Err(io::Error::other("rewind ownership changed"));
                }
                local.sessions.get_mut(id).unwrap().handle = Some(replacement.clone());
                local.sessions.get_mut(id).unwrap().ready = true;
                local.sessions.get_mut(id).unwrap().awaken();
                local.append(id, "harness", json!({"pid":replacement.pid()}), None)?;
            }
            self.record_native(
                id,
                &replacement,
                runtime::Event::Record {
                    kind: "rewind_ready",
                    data: json!({"id":native,"path":path,"cursor":{"offset":0}}),
                    native: None,
                },
            )
            .map_err(|_| io::Error::other("could not persist replacement identity"))?;
            self.watch_events(id.to_owned(), replacement.clone(), events);
            Ok(json!({"id":native,"path":path}))
        }
        .await;
        if result.is_err() {
            replacement.request_shutdown();
        }
        result
    }

    /// Move a ready session to its next turn: dispatch the next queued prompt, wait
    /// out a pending interrupt, or return to idle. `turn_ended` bypasses the active
    /// turn guard when the just-finished turn is the reason for advancing.
    fn advance(self: &Arc<Self>, local: &mut Local, id: &str, turn_ended: bool) {
        if local.sessions[id].state == "sleeping" {
            let _ = self.wake(local, id);
            return;
        }
        let session = &local.sessions[id];
        if session.close_request.is_some()
            || session.rewind_request.is_some()
            || !session.ready
            || self.is_stopping()
        {
            return;
        }
        if !turn_ended && session.current_request.is_some() {
            return; // a turn is still active; wait for its completion
        }
        if session.interrupt_pending() {
            let _ = local.append(id, "state", json!({"state":"interrupting"}), None);
            return;
        }
        if self.storage.blocks()
            || session.storage_paused
            || session.queue_paused
            || session.compacting
        {
            let state = if session.compacting {
                "running"
            } else {
                "idle"
            };
            let _ = local.append(id, "state", json!({"state":state}), None);
            return;
        }
        if let Some(next) = session.queue.first().cloned()
            && let Some(handle) = session.handle.clone()
            && self.begin_turn(local, id, &next).is_ok()
        {
            self.clone().deliver(id.to_owned(), next, handle);
            return;
        }
        let _ = local.append(id, "state", json!({"state":"idle"}), None);
    }

    fn snapshot_usage(self: Arc<Self>, id: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let Ok(Some(usage)) = handle.usage().await else {
                return;
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions.get(&id).is_some_and(|session| {
                session
                    .handle
                    .as_ref()
                    .is_some_and(|current| current.same_process(&handle))
            }) {
                return;
            }
            if usage["sessionId"]
                .as_str()
                .is_some_and(|native| local.sessions[&id].native_id.as_deref() != Some(native))
            {
                return;
            }
            let _ = local.append(&id, "usage", usage, None);
        });
    }

    /// Forward native events into the record path, shutting the harness if recording fails.
    fn watch_events(
        self: &Arc<Self>,
        id: String,
        handle: runtime::Handle,
        mut events: mpsc::Receiver<runtime::Event>,
    ) {
        let manager = self.clone();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if manager.record_native(&id, &handle, event).is_err() {
                    if let Some(handle) = manager
                        .local
                        .lock()
                        .unwrap()
                        .sessions
                        .get(&id)
                        .and_then(|s| s.handle.clone())
                    {
                        handle.request_shutdown();
                    }
                    break;
                }
            }
        });
    }

    /// Reconcile the previous service's protected workloads before admitting starts.
    /// Native handshakes then run in the background, keeping reconnects responsive.
    pub async fn restore_all(self: &Arc<Self>) -> io::Result<()> {
        runtime::reconcile(&self.config).await?;
        let mut local = self.local.lock().unwrap();
        let now = now_ms();
        let ids: Vec<_> = local
            .sessions
            .values()
            .filter(|s| s.can_resume() && s.handle.is_none())
            .map(|s| s.session_id.clone())
            .collect();
        for id in ids {
            if local.sessions[&id].restores_awake(now) {
                self.schedule_resume(&mut local, &id, false)
                    .map_err(|_| io::Error::other("cannot save recovery state"))?;
            } else if local.sessions[&id].state != "sleeping" {
                local.append(&id, "state", json!({"state":"sleeping"}), None)?;
            }
        }
        for session in local.sessions.values() {
            if let Some(request) = session.waiting_start() {
                let (manager, id) = (self.clone(), session.session_id.clone());
                tokio::spawn(async move {
                    manager.launch(id, request).await;
                });
            }
        }
        self.watch_children(&local);
        Ok(())
    }

    fn schedule_resume(
        self: &Arc<Self>,
        local: &mut Local,
        id: &str,
        recovery: bool,
    ) -> Result<()> {
        // Claim a crash recovery durably before spawning; a failed or interrupted attempt
        // cannot trigger an endless restart loop. Completed turns rearm recovery.
        // Planned resumes (wake, restart, teleport) leave the one crash recovery unused.
        local.append(
            id,
            "state",
            json!({"state":"resuming","recovery":recovery}),
            None,
        )?;
        let (manager, id) = (self.clone(), id.to_owned());
        tokio::spawn(async move {
            manager.relaunch(id).await;
        });
        Ok(())
    }

    async fn relaunch(self: Arc<Self>, id: String) {
        let Ok(_startup) = self.startups.acquire().await else {
            return;
        };
        let result = async {
            loop {
                {
                    let mut local = self.local.lock().unwrap();
                    if local.sessions[&id].close_request.is_some() {
                        return Ok(());
                    }
                    if self.is_stopping() {
                        local.append(&id, "state", json!({"state":"suspended"}), None)?;
                        return Ok(());
                    }
                }
                if !self.storage.blocks() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let previous = self.local.lock().unwrap().sessions[&id].harness_pid;
            if self.config.storage.is_none()
                && let Some(pid) = previous
            {
                runtime::wait_for_exit(pid).await?;
            }
            let (handle, events, native) = {
                let mut local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                if session.close_request.is_some() || self.is_stopping() {
                    return Ok(());
                }
                let native = session
                    .native_id
                    .clone()
                    .ok_or_else(|| io::Error::other("missing native identity"))?;
                let kind = session.harness;
                let mut saved = session
                    .resume()
                    .ok_or_else(|| io::Error::other("missing native resume state"))?;
                let profile = self
                    .config
                    .harnesses
                    .get(&kind)
                    .ok_or_else(|| io::Error::other("harness not configured"))?;
                // Capture any final orphan writes after ownership reconciliation.
                runtime::recover_records(
                    profile,
                    kind,
                    &saved,
                    self.config.storage.as_ref(),
                    |event| {
                        if let runtime::Event::Record { kind, data, native } = event {
                            local.append(&id, kind, data, native)?;
                        }
                        Ok(())
                    },
                )?;
                saved.cursor = local.sessions[&id].native_cursor.clone();
                let mut config = self.config.clone();
                config.repository = local.sessions[&id]
                    .workspace
                    .as_ref()
                    .ok_or_else(|| io::Error::other("session workspace missing"))?
                    .path
                    .canonicalize()?;
                let (handle, events) = runtime::Handle::spawn_guarded(
                    &config,
                    kind,
                    Some(saved),
                    local.sessions[&id].command_guard_enabled(),
                    local.sessions[&id].strip_ai_co_authors(),
                    local.sessions[&id].system_prompt(),
                )?;
                local.sessions.get_mut(&id).unwrap().handle = Some(handle.clone());
                local.append(&id, "harness", json!({"pid":handle.pid()}), None)?;
                (handle, events, native)
            };
            self.watch_events(id.clone(), handle.clone(), events);
            handle.start_session().await?;
            let mut local = self.local.lock().unwrap();
            self.remember_launch_reasoning(&mut local, &id, &handle)?;
            if local.sessions[&id].close_request.is_some() || self.is_stopping() {
                return Ok(());
            }
            if local.sessions[&id].handle.is_none() {
                return Err(io::Error::other("resumed harness exited"));
            }
            local.sessions.get_mut(&id).unwrap().ready = true;
            local.sessions.get_mut(&id).unwrap().awaken();
            local.append(
                &id,
                "native_identity",
                handle.native_identity(&native)?,
                None,
            )?;
            local.append(&id, "state", json!({"state":"idle"}), None)?;
            let retries: Vec<_> = local.sessions[&id]
                .receipts
                .values()
                .filter(|r| r.command == "resume" && r.state == "accepted")
                .map(|r| r.request_id.clone())
                .collect();
            for request in retries {
                local
                    .finish_receipt(&id, &request, "completed")
                    .map_err(|_| io::Error::other("cannot record resume outcome"))?;
            }
            if local.sessions[&id].storage_paused {
                local.append(
                    &id,
                    "storage_recovered",
                    json!({"text":"Cloudroom: workload resumed after restart."}),
                    None,
                )?;
            }
            self.advance(&mut local, &id, true);
            Ok::<_, io::Error>(())
        }
        .await;
        if let Err(error) = result {
            if self.is_stopping() {
                return;
            }
            let mut local = self.local.lock().unwrap();
            if local.sessions[&id].close_request.is_none() {
                let check = self.recovery_check(&local.sessions[&id]);
                let (code, reason) = if error.kind() == io::ErrorKind::TimedOut {
                    (
                        "startup_timeout",
                        "Harness startup timed out; saved history and queued prompts are preserved. Retry resume when load settles.",
                    )
                } else if check["status"] == "empty" {
                    (
                        "empty_session",
                        "No saved native conversation exists and no turn was dispatched; nothing can be resumed.",
                    )
                } else if check["status"] == "missing" {
                    (
                        "missing_history",
                        "Saved native conversation is missing; refusing to replace it or replay work.",
                    )
                } else {
                    (
                        "resume_failed",
                        "Native resume failed; inspect native history and protected local harness diagnostics for this session, then retry explicitly after correcting the cause.",
                    )
                };
                let lost = format!("{reason} Cause: {error}");
                let _ = local.append(
                    &id,
                    "state",
                    json!({"state":"process_lost","reason":lost,"startup_error":code}),
                    None,
                );
                if code != "startup_timeout" {
                    let _ = local.fail_pending_because(&id, Some(&lost));
                }
                let retries: Vec<_> = local.sessions[&id]
                    .receipts
                    .values()
                    .filter(|r| r.command == "resume" && r.state == "accepted")
                    .map(|r| r.request_id.clone())
                    .collect();
                for request in retries {
                    let _ = local.finish_receipt(&id, &request, "failed");
                }
            }
            if let Some(handle) = local.sessions[&id].handle.as_ref() {
                handle.request_shutdown();
            }
        }
    }

    fn record_native(
        self: &Arc<Self>,
        id: &str,
        handle: &runtime::Handle,
        event: runtime::Event,
    ) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        if !local.sessions[id]
            .handle
            .as_ref()
            .is_some_and(|current| current.same_process(handle))
        {
            return Ok(());
        }
        match event {
            runtime::Event::Authentication { accepted } => {
                if local.sessions[id].harness == runtime::Kind::Claude {
                    self.claude_auth.request(accepted, &self.observability);
                }
            }
            runtime::Event::ChildRequest(child) => {
                self.start_child(&mut local, id, handle, child)?;
            }
            runtime::Event::Record {
                kind: "rewind_ready",
                mut data,
                ..
            } => {
                let request = local.sessions[id]
                    .rewind_request
                    .clone()
                    .ok_or(Error::Conflict("unexpected native replacement"))?;
                let input = local.sessions[id].receipts[&request].input.clone();
                data["request_id"] = json!(request);
                data["before"] = input["before"].clone();
                if let Some(replacement) = input.get("replacement") {
                    data["replacement"] = json!(Receipt {
                        request_id: replacement["request_id"]
                            .as_str()
                            .ok_or(Error::Conflict("invalid replacement"))?
                            .into(),
                        command: "prompt".into(),
                        input: replacement["input"].clone(),
                        state: "accepted".into(),
                        model: None,
                        provider: None,
                        workspace: None,
                        error: None,
                    });
                }
                local.append(id, "rewind", data, None)?;
                self.advance(&mut local, id, true);
            }
            runtime::Event::Record { kind, data, native } => {
                let usage = data.get("usage").cloned().filter(|value| !value.is_null());
                local.append(id, kind, data, native)?;
                if let Some(usage) = usage {
                    let _ = local.append(id, "usage", usage, None);
                }
            }
            runtime::Event::Compacted { status } => {
                if let Some(request) = local.sessions[id].compact_request.clone() {
                    let status = match status.as_str() {
                        "completed" => "completed",
                        "interrupted" => "interrupted",
                        _ => "failed",
                    };
                    if status == "failed" {
                        local
                            .sessions
                            .get_mut(id)
                            .unwrap()
                            .receipts
                            .get_mut(&request)
                            .unwrap()
                            .error = Some("Context compaction failed; see native history".into());
                    }
                    local.finish_receipt(id, &request, status)?;
                    self.clone().snapshot_usage(id.to_owned(), handle.clone());
                    self.advance(&mut local, id, false);
                }
            }
            runtime::Event::Started {
                request,
                native_turn,
                auto,
            } => {
                let session = &local.sessions[id];
                // A turn the harness began itself gets a receipt, so it can finish, stop and recover like a prompt.
                if auto
                    && session.ready
                    && session.current_request.is_none()
                    && session.close_request.is_none()
                    && session.rewind_request.is_none()
                    && !session.compacting
                {
                    let receipt = Receipt {
                        request_id: request.clone(),
                        command: "auto".into(),
                        input: json!({}),
                        state: "running".into(),
                        model: None,
                        provider: None,
                        workspace: None,
                        error: None,
                    };
                    let receipt = serde_json::to_value(receipt).map_err(io::Error::other)?;
                    local.append(id, "receipt", receipt, None)?;
                    self.begin_turn(&mut local, id, &request)?;
                }
                let session = &local.sessions[id];
                if !session.ready || session.current_request.as_deref() != Some(&request) {
                    return Ok(());
                }
                let state = if session.close_request.is_some() {
                    "closing"
                } else {
                    "running"
                };
                let data = json!({"state":state,"request_id":request,"turn_id":native_turn});
                local.append(id, "state", data, None)?;
            }
            runtime::Event::Finished {
                request,
                status,
                error,
            } => {
                if local.sessions[id].current_request.as_deref() == Some(&request) {
                    let session = &local.sessions[id];
                    let checkpoint = session
                        .current_turn
                        .clone()
                        .map(|turn| json!({"kind":"turn","id":turn,"request_id":request}));
                    if let Some(checkpoint) = checkpoint {
                        let _ = local.append(id, "checkpoint", checkpoint, None);
                    }
                    self.clone().snapshot_usage(id.to_owned(), handle.clone());
                    if let Some(request) = local.sessions[id].current_request.clone() {
                        local.finish_receipt_with(id, &request, &status, error)?;
                        let session = local.sessions.get_mut(id).ok_or(Error::NotFound)?;
                        if status == "completed" && session.parent_session.is_some() {
                            let text = handle.seen_text().chars().take(32768).collect();
                            session.replies.push((request, text));
                            session
                                .replies
                                .drain(..session.replies.len().saturating_sub(8));
                        }
                    }
                    self.advance(&mut local, id, true);
                }
            }
            runtime::Event::Exited {
                reason,
                expected,
                cleaned_up,
                details,
            } => {
                let exit = match (details.code, details.signal) {
                    (Some(code), _) => format!("exit code {code}"),
                    (None, Some(signal)) => format!("signal {signal}"),
                    _ => "no exit status".into(),
                };
                let stderr = stderr_tail(details.stderr());
                let cause = details.cause.clone();
                let diagnostic_id = self.observability.agent_exit(
                    Some(id),
                    local.sessions[id].harness,
                    reason,
                    expected,
                    details,
                );
                if expected && cleaned_up && local.sessions[id].rewind_request.is_some() {
                    let session = local.sessions.get_mut(id).ok_or(Error::NotFound)?;
                    session.handle = None;
                    session.ready = false;
                    local.append(id, "harness", json!({"pid":null}), None)?;
                    return Ok(());
                }
                let session = &local.sessions[id];
                if session.releasing && cleaned_up && session.close_request.is_none() {
                    return self.slept(&mut local, id);
                }
                let session = local.sessions.get_mut(id).ok_or(Error::NotFound)?;
                session.releasing = false;
                let restoring = session.state == "resuming"
                    && session.native_id.is_some()
                    && session.native_path.is_some();
                let resumable = cleaned_up
                    && ((session.ready && session.can_resume())
                        || (self.is_stopping() && restoring));
                let recover = resumable && !self.is_stopping() && !session.recovery_attempted;
                // Launch already recorded the real cause; stopping its harness must not hide it.
                let failed = session
                    .failure
                    .clone()
                    .filter(|_| session.state == "failed");
                session.handle = None;
                session.ready = false;
                let request = session.current_request.clone();
                let close = session.close_request.clone();
                let preserve_queue = session.startup_error.as_deref() == Some("startup_timeout");
                let interrupts: Vec<_> = session
                    .receipts
                    .values()
                    .filter(|r| {
                        matches!(r.command.as_str(), "interrupt" | "stop")
                            && matches!(r.state.as_str(), "accepted" | "delivered" | "unknown")
                    })
                    .map(|r| r.request_id.clone())
                    .collect();
                let lost = failed.clone().unwrap_or_else(|| match &cause {
                    Some(cause) => format!("{reason}: {cause} ({exit})"),
                    None => format!("{reason} ({exit})"),
                });
                if let Some(request) = request {
                    // Keep the cause on the lost turn: an automatic recovery leaves the thread idle and error-free.
                    local.finish_receipt_with(
                        id,
                        &request,
                        if self.is_stopping() || close.is_some() {
                            "unknown"
                        } else {
                            "unknown_after_restart"
                        },
                        (!expected).then(|| lost.clone()),
                    )?;
                }
                for request in interrupts {
                    local.finish_receipt(id, &request, "unknown_after_restart")?;
                }
                if cleaned_up {
                    local.append(id, "harness", json!({"pid":null}), None)?;
                }
                let state = if expected && close.is_some() {
                    "closed"
                } else if resumable && (self.is_stopping() || recover) {
                    "suspended"
                } else if failed.is_some() {
                    "failed"
                } else {
                    "process_lost"
                };
                let lost_or_failed = matches!(state, "process_lost" | "failed");
                local.append(
                    id,
                    "state",
                    json!({"state":state,"reason":lost,"diagnostic_id":diagnostic_id,"stderr":(lost_or_failed && !stderr.is_empty()).then_some(stderr)}),
                    None,
                )?;
                if let Some(request) = close {
                    local.finish_receipt(
                        id,
                        &request,
                        if expected { "completed" } else { "unknown" },
                    )?;
                }
                if state != "suspended" && !preserve_queue {
                    local.fail_pending_because(id, Some(&lost))?;
                }
                if recover {
                    self.schedule_resume(&mut local, id, true)?;
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.codex_auth.shutdown().await;
        self.claude_auth.shutdown().await;
        self.claude_login.shutdown().await;
        self.cursor_auth.shutdown().await;
        let mut changed = self.subscribe();
        {
            let mut local = self.local.lock().unwrap();
            self.stopping.store(true, Ordering::Relaxed);
            let waiting: Vec<_> = local
                .sessions
                .values()
                .filter(|s| s.state == "resuming" && s.handle.is_none())
                .map(|s| s.session_id.clone())
                .collect();
            for id in waiting {
                let _ = local.append(&id, "state", json!({"state":"suspended"}), None);
            }
            // Before the harness cleanup, which may outlast the deadline. Its few records replay next start.
            local.save_snapshot();
            local.changed.send_replace(0);
            // Signal all harnesses together; their recorded Exited events confirm cleanup.
            for handle in local.sessions.values().filter_map(|s| s.handle.as_ref()) {
                handle.request_shutdown();
            }
        }
        let cleanup = async {
            while self
                .local
                .lock()
                .unwrap()
                .sessions
                .values()
                .any(|s| s.handle.is_some())
            {
                if changed.changed().await.is_err() {
                    break;
                }
            }
            self.observability.shutdown().await;
        };
        if tokio::time::timeout(runtime::SHUTDOWN_GRACE + Duration::from_secs(1), cleanup)
            .await
            .is_err()
        {
            eprintln!("Cloudroom shutdown cleanup deadline reached");
        }
    }
}

/// Last lines of harness stderr, shown under unexpected exits so the cause is visible in chat (ADR 0123).
fn stderr_tail(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(2000)..]);
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}
