//! Replays and appends journal records: the session state machine.
use super::*;

/// Bump when `apply` builds state differently, so older snapshots are replayed from the journal instead.
const SNAPSHOT_VERSION: u32 = 1;
/// A busy sandbox wrote up to ~100k records an hour; this bounds the replay after an unplanned stop.
const SNAPSHOT_EVERY: u64 = 20_000;

/// The replayed state at journal position `sequence`, so startup replays only newer records.
/// Listing and reading every record took over a minute in a sandbox with ~500k of them.
#[derive(Serialize, Deserialize)]
struct Snapshot<S = Vec<Saved>, Q = BTreeMap<String, Vec<u64>>> {
    version: u32,
    sequence: u64,
    sessions: S,
    sequences: Q,
}

#[derive(Serialize, Deserialize)]
struct Saved<S = Session> {
    session: S,
    derived: Derived,
}

/// Fields `apply` builds that the session API leaves out (`serde(skip)`). Any new one belongs here too.
#[derive(Serialize, Deserialize)]
struct Derived {
    failure: Option<String>,
    has_dispatched: bool,
    last_activity: Option<u64>,
    last_prompt_state: Option<String>,
    last_prompt_error: Option<String>,
    model: Option<String>,
    storage_warned: bool,
    recovery_attempted: bool,
    close_request: Option<String>,
    harness_pid: Option<u32>,
    usage_limited: bool,
}

impl Derived {
    fn of(s: &Session) -> Self {
        Self {
            failure: s.failure.clone(),
            has_dispatched: s.has_dispatched,
            last_activity: s.last_activity,
            last_prompt_state: s.last_prompt_state.clone(),
            last_prompt_error: s.last_prompt_error.clone(),
            model: s.model.clone(),
            storage_warned: s.storage_warned,
            recovery_attempted: s.recovery_attempted,
            close_request: s.close_request.clone(),
            harness_pid: s.harness_pid,
            usage_limited: s.usage_limited,
        }
    }

    fn restore(self, s: &mut Session) {
        s.failure = self.failure;
        s.has_dispatched = self.has_dispatched;
        s.last_activity = self.last_activity;
        s.last_prompt_state = self.last_prompt_state;
        s.last_prompt_error = self.last_prompt_error;
        s.model = self.model;
        s.storage_warned = self.storage_warned;
        s.recovery_attempted = self.recovery_attempted;
        s.close_request = self.close_request;
        s.harness_pid = self.harness_pid;
        s.usage_limited = self.usage_limited;
    }
}

impl Local {
    /// Opens the journal and rebuilds the sessions: from a usable snapshot, then the records after it.
    /// A missing, unreadable, older or too-new snapshot means a full replay.
    pub(super) fn open(directory: &std::path::Path) -> io::Result<Self> {
        let snapshot = Journal::read_snapshot(directory)
            .and_then(|bytes| serde_json::from_slice::<Snapshot>(&bytes).ok())
            .filter(|s| s.version == SNAPSHOT_VERSION);
        let journal = Journal::open_from(directory, snapshot.as_ref().map_or(0, |s| s.sequence))?;
        let (changed, _) = watch::channel(journal.last());
        let mut local = Local {
            journal,
            sessions: BTreeMap::new(),
            sequences: BTreeMap::new(),
            changed,
            database_available: false,
            draining: false,
            snapshot_at: 0,
        };
        if let Some(snapshot) = snapshot.filter(|s| s.sequence <= local.journal.last()) {
            for Saved {
                mut session,
                derived,
            } in snapshot.sessions
            {
                derived.restore(&mut session);
                local.sessions.insert(session.session_id.clone(), session);
            }
            local.sequences = snapshot.sequences;
            local.snapshot_at = snapshot.sequence;
        }
        for sequence in local.snapshot_at + 1..=local.journal.last() {
            let record: Record = serde_json::from_slice(&local.journal.read(sequence)?)?;
            if record.sequence != sequence {
                return Err(io::Error::other("journal sequence mismatch"));
            }
            local.apply(&record)?;
            // A first replay of a large journal can outlast the wake's wait, which then restarts Core.
            // Saved progress lets that next start continue from here.
            if sequence >= local.snapshot_at + SNAPSHOT_EVERY {
                local.snapshot(sequence);
            }
        }
        Ok(local)
    }

    /// Saves the replayed state. A failure only costs a longer replay at the next start.
    pub(super) fn save_snapshot(&mut self) {
        self.snapshot(self.journal.last());
    }

    /// `sequence` is the last record the state includes.
    fn snapshot(&mut self, sequence: u64) {
        if sequence == self.snapshot_at {
            return;
        }
        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            sequence,
            sessions: (self.sessions.values())
                .map(|session| Saved {
                    session,
                    derived: Derived::of(session),
                })
                .collect::<Vec<_>>(),
            sequences: &self.sequences,
        };
        // A failure waits for the next scheduled save rather than retrying on every record.
        self.snapshot_at = sequence;
        match serde_json::to_vec(&snapshot).map(|bytes| self.journal.write_snapshot(&bytes)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("Cloudroom could not save the replay snapshot: {error}"),
            Err(error) => eprintln!("Cloudroom could not encode the replay snapshot: {error}"),
        }
    }

    pub(super) fn agent_counts(&self) -> AgentCounts {
        let mut counts = AgentCounts::default();
        for session in self.sessions.values() {
            match session.dashboard_state() {
                "working" => counts.working += 1,
                "queued" => counts.queued += 1,
                "waiting" => counts.waiting += 1,
                "sleeping" => counts.sleeping += 1,
                "failed" => counts.failed += 1,
                _ => {}
            }
        }
        counts
    }

    pub(super) fn finish_receipt(&mut self, id: &str, request: &str, state: &str) -> Result<()> {
        self.finish_receipt_with(id, request, state, None)
    }

    pub(super) fn finish_receipt_with(
        &mut self,
        id: &str,
        request: &str,
        state: &str,
        error: Option<String>,
    ) -> Result<()> {
        let mut receipt = self
            .sessions
            .get(id)
            .and_then(|s| s.receipts.get(request))
            .cloned()
            .ok_or(Error::NotFound)?;
        if matches!(
            receipt.state.as_str(),
            "completed" | "interrupted" | "failed" | "unknown_after_restart"
        ) || receipt.state == state
        {
            return Ok(());
        }
        receipt.state = state.into();
        if error.is_some() {
            receipt.error = error;
        }
        self.append(
            id,
            "receipt",
            serde_json::to_value(receipt).map_err(io::Error::other)?,
            None,
        )?;
        Ok(())
    }

    pub(super) fn fail_pending(&mut self, id: &str) -> Result<()> {
        self.fail_pending_because(id, None)
    }

    /// Fail queued prompts, keeping the cause on each receipt (ADR 0123).
    pub(super) fn fail_pending_because(&mut self, id: &str, cause: Option<&str>) -> Result<()> {
        for request in self.sessions[id].queue.clone() {
            self.finish_receipt_with(id, &request, "failed", cause.map(str::to_owned))?;
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, record: &Record) -> io::Result<()> {
        if record.kind == "teleport" {
            let before = self.sequences.get(&record.session_id).map_or(0, Vec::len);
            for data in std::iter::once(&record.data["receipt"]).chain(
                record.data["prompts"]
                    .as_array()
                    .ok_or_else(|| io::Error::other("invalid imported prompts"))?,
            ) {
                self.apply(&Record {
                    kind: "receipt".into(),
                    data: data.clone(),
                    native: None,
                    ..record.clone()
                })?;
            }
            let session = self.sessions.get_mut(&record.session_id).unwrap();
            session.native_id = Some(
                record.data["native_id"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing imported identity"))?
                    .into(),
            );
            session.native_path = Some(
                record.data["native_path"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing imported history"))?
                    .into(),
            );
            session.state = "suspended".into();
            let sequences = self.sequences.entry(record.session_id.clone()).or_default();
            sequences.truncate(before);
            sequences.push(record.sequence);
            return Ok(());
        }
        let session = self
            .sessions
            .entry(record.session_id.clone())
            .or_insert_with(|| Session {
                session_id: record.session_id.clone(),
                state: "starting".into(),
                ..Session::default()
            });
        session.last_sequence = record.sequence;
        if matches!(record.kind.as_str(), "text_delta" | "thinking_delta")
            && record.data["delta"].as_str().is_some_and(|s| !s.is_empty())
        {
            session.teleport_output = true;
        }
        // Lifecycle bookkeeping, such as restart recovery, is not user or agent activity. Neither are a harness's
        // environment notices, such as Codex's `skills/changed` after skills sync: they would keep idle work awake.
        let notice = record.kind == "native_event"
            && record.data["method"].as_str().is_some_and(|method| {
                ["skills/", "account/", "mcpServer/", "config/"]
                    .iter()
                    .any(|prefix| method.starts_with(prefix))
            });
        if !notice
            && !matches!(
                record.kind.as_str(),
                "state"
                    | "harness"
                    | "workspace"
                    | "launch_reasoning"
                    | "native_identity"
                    | "native_history_unavailable"
            )
        {
            session.last_activity = record.timestamp_ms.or(session.last_activity);
        }
        match record.kind.as_str() {
            "receipt" => {
                let receipt = serde_json::from_value::<Receipt>(record.data.clone())?;
                // Only its first record sets the parent, so a later copy cannot undo a detach.
                if receipt.command == "start" && !session.receipts.contains_key(&receipt.request_id)
                {
                    session.parent_session =
                        receipt.input["parent_session"].as_str().map(str::to_owned);
                }
                if receipt.command == "start" {
                    if session.parent_session.is_some()
                        && receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        let task = Receipt {
                            request_id: receipt.request_id.replacen("child_", "task_", 1),
                            command: "prompt".into(),
                            input: json!({"text":receipt.input["prompt"],"reasoning":receipt.input["reasoning"]}),
                            state: "accepted".into(),
                            model: None,
                            provider: None,
                            workspace: None,
                            error: None,
                        };
                        prompt::apply(&mut session.prompts, &task);
                        session.queue.push(task.request_id.clone());
                        session.receipts.insert(task.request_id.clone(), task);
                        session.state = "pending".into();
                    }
                    session.workspace = receipt.workspace.clone().or(session.workspace.clone());
                    if receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        session.state = "pending".into();
                    }
                    // Completing this receipt must not erase a default captured after it was accepted.
                    if let Some(level) = receipt.input["reasoning"].as_str() {
                        session.reasoning = Some(level.to_owned());
                    }
                    if receipt.model.is_some() {
                        session.model = receipt.model.clone();
                    }
                    if receipt.provider.is_some() {
                        session.provider = receipt.provider.clone();
                    }
                    session.harness = serde_json::from_value(
                        receipt
                            .input
                            .get("harness")
                            .cloned()
                            .unwrap_or(json!("codex")),
                    )?;
                }
                if receipt.state == "accepted"
                    && !session.receipts.contains_key(&receipt.request_id)
                {
                    if receipt.command == "stop" {
                        session.queue_paused = true;
                    }
                    if receipt.command == "resume" {
                        session.queue_paused = false;
                    }
                }
                if receipt.command == "close" {
                    session.close_request = Some(receipt.request_id.clone());
                }
                if receipt.command == "prompt" {
                    // Acceptance is also queue insertion: one fsynced record, including
                    // legacy receipts whose separate enqueue write never finished.
                    if receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        session.queue.push(receipt.request_id.clone());
                        session.recovery_attempted = false; // A new user request permits one recovery.
                        session.usage_limited = false;
                    }
                    if !matches!(receipt.state.as_str(), "accepted" | "running" | "delivered") {
                        session.queue.retain(|id| id != &receipt.request_id);
                        session.last_prompt_state = Some(receipt.state.clone());
                        session.last_prompt_error = receipt.error.clone();
                    }
                    if matches!(receipt.state.as_str(), "completed" | "interrupted") {
                        session.recovery_attempted = false; // Healthy progress permits a later recovery.
                    }
                }
                if receipt.command == "cancel"
                    && matches!(receipt.state.as_str(), "accepted" | "completed")
                    && let Some(target) = receipt.input["target_request_id"].as_str()
                {
                    session.queue.retain(|id| id != target);
                }
                if receipt.command == "reorder"
                    && receipt.state == "accepted"
                    && !session.receipts.contains_key(&receipt.request_id)
                    && let Some(order) = receipt.input["order"].as_array()
                {
                    // Listed requests go first in the new order. Anything queued since, or
                    // missing from a stale list, keeps its place after them.
                    let mut queue: Vec<String> = order
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|id| session.queue.iter().any(|queued| queued == id))
                        .map(str::to_owned)
                        .collect();
                    let rest: Vec<String> = session
                        .queue
                        .iter()
                        .filter(|id| !queue.contains(id))
                        .cloned()
                        .collect();
                    queue.extend(rest);
                    session.queue = queue;
                }
                if receipt.command == "compact" {
                    let terminal = matches!(
                        receipt.state.as_str(),
                        "completed"
                            | "failed"
                            | "interrupted"
                            | "unknown"
                            | "unknown_after_restart"
                    );
                    session.compacting = !terminal
                        && matches!(
                            receipt.state.as_str(),
                            "accepted" | "delivered" | "running" | "unknown"
                        );
                    session.compact_request = (!terminal).then(|| receipt.request_id.clone());
                }
                if receipt.command == "rewind" && receipt.state == "accepted" {
                    session.rewind_request = Some(receipt.request_id.clone());
                    session.state = "rewinding".into();
                }
                prompt::apply(&mut session.prompts, &receipt);
                session.receipts.insert(receipt.request_id.clone(), receipt);
            }
            "native_identity" => {
                let id = record.data["id"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing native identity"))?;
                if session.native_id.as_deref().is_some_and(|old| old != id)
                    && record.data["replaced"] != true
                {
                    return Err(io::Error::other("native identity changed"));
                }
                session.native_id = Some(id.into());
                if let Some(capabilities) = record.data.get("capabilities") {
                    session.capabilities = capabilities.clone();
                }
                if let Some(model) = record.data["model"].as_str() {
                    session.model = Some(model.into());
                }
                if let Some(provider) = record.data["provider"].as_str() {
                    session.provider = Some(provider.into());
                }
                if let Some(path) = record.data["path"].as_str() {
                    session.native_path = Some(path.into());
                }
            }
            "native_record" => {
                session.native_cursor = runtime::checkpoint(
                    session.harness,
                    &session.native_cursor,
                    &record.data,
                    record.native.as_deref(),
                )?;
                session.native_offset = session.native_cursor["offset"].as_u64().unwrap_or(0);
            }
            "state" => {
                session.state = record.data["state"].as_str().unwrap_or("unknown").into();
                session.failure =
                    matches!(session.state.as_str(), "failed" | "process_lost").then(|| {
                        let reason = record.data["reason"].as_str().unwrap_or("unknown cause");
                        match record.data["stderr"].as_str() {
                            Some(stderr) => format!("{reason}\n{stderr}"),
                            None => reason.into(),
                        }
                    });
                if session.state == "resuming" && record.data["recovery"] == true {
                    session.recovery_attempted = true;
                }
                if let Some(error) = record.data["startup_error"].as_str() {
                    session.startup_error = Some(error.into());
                    session.startup_reason = record.data["reason"].as_str().map(str::to_owned);
                } else if record.data["reason"] == "native resume or process cleanup failed" {
                    session.startup_error = Some("resume_failed".into());
                    session.startup_reason = Some("native resume or process cleanup failed".into());
                } else if session.state == "idle" {
                    session.startup_error = None;
                    session.startup_reason = None;
                }
                session.current_request = record.data["request_id"].as_str().map(str::to_owned);
                session.current_turn = record.data["turn_id"].as_str().map(str::to_owned);
                // Dispatching a queued prompt removes it from the pending queue.
                if session.state == "starting_turn"
                    && let Some(request) = record.data["request_id"].as_str()
                {
                    session.has_dispatched = true;
                    session.queue.retain(|id| id != request);
                }
            }
            "enqueue" => {} // Legacy queue membership is already reconstructed from its receipt.
            "harness" => {
                session.harness_pid = record.data["pid"].as_u64().map(|pid| pid as u32);
            }
            "reasoning" => {
                if let Some(level) = record.data["reasoning"].as_str() {
                    session.reasoning = Some(level.into());
                }
            }
            "launch_reasoning" => {
                if session.reasoning.is_none() {
                    session.reasoning = record.data["reasoning"].as_str().map(str::to_owned);
                }
            }
            "workspace" => session.workspace = Some(serde_json::from_value(record.data.clone())?),
            "checkpoint" => session.checkpoint = Some(record.data.clone()),
            "rewind" => {
                if let Some(id) = record.data["id"].as_str() {
                    session.native_id = Some(id.into());
                }
                if let Some(path) = record.data["path"].as_str() {
                    session.native_path = Some(path.into());
                }
                session.native_cursor = record.data["cursor"].clone();
                session.native_offset = session.native_cursor["offset"].as_u64().unwrap_or(0);
                session.checkpoint = None;
                session.rewind_request = None;
                session.state = "idle".into();
                if let Some(request) = record.data["request_id"].as_str()
                    && let Some(receipt) = session.receipts.get_mut(request)
                {
                    receipt.state = "completed".into();
                }
                if let Some(replacement) = record.data.get("replacement").filter(|v| !v.is_null()) {
                    let receipt: Receipt = serde_json::from_value(replacement.clone())?;
                    if !session.receipts.contains_key(&receipt.request_id) {
                        prompt::apply(&mut session.prompts, &receipt);
                        session.queue.push(receipt.request_id.clone());
                        session.receipts.insert(receipt.request_id.clone(), receipt);
                    }
                }
            }
            "rewind_failed" => {
                session.rewind_request = None;
                session.state = "idle".into();
            }
            "archive" => session.archived = true,
            "detach" => session.parent_session = None,
            "usage_limited" => session.usage_limited = true,
            "storage_warning" => session.storage_warned = true,
            "storage_pause" => session.storage_paused = record.data["paused"] == true,
            "storage_recovered" => {
                session.storage_warned = false;
                session.storage_paused = false;
            }
            _ => {}
        }
        self.sequences
            .entry(record.session_id.clone())
            .or_default()
            .push(record.sequence);
        Ok(())
    }

    pub(super) fn append(
        &mut self,
        session: &str,
        kind: &str,
        data: Value,
        native: Option<String>,
    ) -> io::Result<Record> {
        if kind == "native_record" {
            let current = self
                .sessions
                .get(session)
                .ok_or_else(|| io::Error::other("native record has no session"))?;
            runtime::checkpoint(
                current.harness,
                &current.native_cursor,
                &data,
                native.as_deref(),
            )?;
        }
        let record = Record {
            sequence: self.journal.last() + 1,
            session_id: session.into(),
            kind: kind.into(),
            timestamp_ms: Some(now_ms()),
            data,
            native,
        };
        self.journal.append(&serde_json::to_vec(&record)?)?;
        self.apply(&record)?;
        if record.sequence >= self.snapshot_at + SNAPSHOT_EVERY {
            self.save_snapshot();
        }
        self.changed.send_replace(record.sequence);
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn state(local: &Local) -> Value {
        let sessions: Vec<_> = (local.sessions.values())
            .map(|s| json!([s, Derived::of(s)]))
            .collect();
        json!([sessions, local.sequences])
    }

    #[test]
    fn snapshot_and_newer_records_rebuild_the_full_replay() {
        let dir = std::env::temp_dir().join(format!("cloudroom-snapshot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut local = Local::open(&dir).unwrap();
        let receipt = |id: &str, command: &str, state: &str, input: Value| json!({"request_id":id,"command":command,"state":state,"input":input,"model":"m1"});
        let records = [
            (
                "a",
                "receipt",
                receipt(
                    "s1",
                    "start",
                    "accepted",
                    json!({"harness":"claude-code","reasoning":"high"}),
                ),
            ),
            (
                "a",
                "receipt",
                receipt("p1", "prompt", "accepted", json!({"text":"hi"})),
            ),
            ("a", "harness", json!({"pid":42})),
            (
                "a",
                "state",
                json!({"state":"starting_turn","request_id":"p1"}),
            ),
            (
                "b",
                "receipt",
                receipt("c1", "close", "accepted", json!({})),
            ),
            ("a", "text_delta", json!({"delta":"hello"})),
            (
                "a",
                "receipt",
                receipt("p1", "prompt", "completed", json!({"text":"hi"})),
            ),
            ("a", "storage_warning", json!({"text":"low"})),
            (
                "b",
                "state",
                json!({"state":"failed","reason":"boom","stderr":"trace"}),
            ),
            ("b", "usage_limited", json!({})),
            ("b", "state", json!({"state":"resuming","recovery":true})),
        ];
        for (i, (id, kind, data)) in records.into_iter().enumerate() {
            local.append(id, kind, data, None).unwrap();
            if i == 5 {
                local.save_snapshot();
            }
        }
        let expected = state(&local);
        drop(local);
        let restored = Local::open(&dir).unwrap();
        assert_eq!(restored.snapshot_at, 6);
        assert_eq!(state(&restored), expected);
        drop(restored);
        fs::remove_file(dir.join("snapshot.json")).unwrap();
        assert_eq!(state(&Local::open(&dir).unwrap()), expected);
        fs::remove_dir_all(&dir).unwrap();
    }
}
