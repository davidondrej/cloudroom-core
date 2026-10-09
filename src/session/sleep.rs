//! Idle sessions release their harness process and resume on the next message (ADR 0031).
use super::*;

/// ADR 0031: stop the harness after 30 minutes without active work.
const IDLE_SLEEP_MS: u64 = 30 * 60 * 1000;
const CHECK_EVERY: Duration = Duration::from_secs(5);

/// The running harness and its workload size when it became ready.
#[derive(Clone, Copy)]
pub(super) struct Awake {
    pid: u32,
    since_ms: u64,
    processes: Option<usize>,
}

impl Session {
    pub(super) fn awaken(&mut self) {
        self.awake = self.handle.as_ref().map(|handle| Awake {
            pid: handle.pid(),
            since_ms: now_ms(),
            processes: handle.process_count(),
        });
    }

    pub(super) fn sleep_pending(&self) -> bool {
        self.receipts
            .values()
            .any(|r| r.command == "sleep" && r.state == "accepted")
    }

    pub(super) fn has_work(&self) -> bool {
        !self.queue.is_empty() && !self.queue_paused
    }

    pub(super) fn busy(&self) -> bool {
        self.current_request.is_some()
            || self.has_work()
            || matches!(
                self.state.as_str(),
                "pending"
                    | "waiting_for_files"
                    | "starting"
                    | "resuming"
                    | "starting_turn"
                    | "running"
            )
    }

    fn idle_ms(&self, now: u64) -> u64 {
        let since = self.last_activity.unwrap_or(0);
        now.saturating_sub(since.max(self.awake.map_or(0, |a| a.since_ms)))
    }

    /// A restart relaunches only sessions with work or recent activity.
    pub(super) fn restores_awake(&self, now: u64) -> bool {
        self.has_work()
            || (self.state != "sleeping"
                && self
                    .last_activity
                    .is_none_or(|at| now.saturating_sub(at) < IDLE_SLEEP_MS))
    }
}

impl Local {
    /// Only an idle harness with no queued work, children, or background jobs may stop.
    fn can_sleep(&self, id: &str) -> bool {
        let s = &self.sessions[id];
        let Some(handle) = &s.handle else {
            return false;
        };
        // New background jobs (dev servers, watchers) count as activity; uncertain means keep running.
        let background = s.awake.is_some_and(|a| {
            a.pid == handle.pid()
                && handle
                    .process_count()
                    .zip(a.processes)
                    .is_some_and(|(now, baseline)| now > baseline)
        });
        s.ready
            && !s.releasing
            && s.state == "idle"
            && !s.busy()
            && !s.compacting
            && s.rewind_request.is_none()
            && s.close_request.is_none()
            && !s.interrupt_pending()
            && !s.storage_paused
            && !background
            && !self
                .sessions
                .values()
                .any(|c| c.parent_session.as_deref() == Some(id) && c.busy())
    }
}

impl Manager {
    /// Before a sandbox stops, refuse new work, but only when nothing is running.
    /// `hold: false` releases the drain, for example when the stop is cancelled.
    pub fn drain(&self, hold: bool) -> Value {
        let mut local = self.local.lock().unwrap();
        if !hold {
            local.draining = false;
            return json!({"drained": false});
        }
        // Closed sessions keep their close request forever, but nothing of theirs is running.
        // A low-disk pause may never lift on its own, so it must not keep the machine awake.
        let busy = local
            .sessions
            .values()
            .filter(|s| s.state != "closed" && !s.storage_paused)
            .find(|s| {
                s.busy()
                    || s.compacting
                    || s.rewind_request.is_some()
                    || s.close_request.is_some()
                    || s.interrupt_pending()
            });
        if let Some(session) = busy {
            return json!({"drained":false,"reason":"session_busy","session_id":session.session_id});
        }
        if self.teleports.busy() {
            return json!({"drained":false,"reason":"transfer_running"});
        }
        local.draining = true;
        // The sandbox stops next, so its next start replays almost nothing.
        local.save_snapshot();
        json!({"drained":true,"pending_records":local.journal.last() - local.journal.saved()})
    }

    pub fn start_idle_sleeper(self: &Arc<Self>) {
        let manager = self.clone();
        tokio::spawn(async move {
            while !manager.is_stopping() {
                tokio::time::sleep(CHECK_EVERY).await;
                let mut local = manager.local.lock().unwrap();
                let now = now_ms();
                let ids: Vec<_> = local
                    .sessions
                    .values()
                    .filter(|s| s.handle.is_some())
                    .map(|s| s.session_id.clone())
                    .collect();
                for id in ids {
                    let session = local.sessions.get_mut(&id).unwrap();
                    let pid = session.handle.as_ref().map(|h| h.pid());
                    if session.ready && session.awake.map(|a| a.pid) != pid {
                        session.awaken();
                        continue;
                    }
                    let due = session.sleep_pending() || session.idle_ms(now) >= IDLE_SLEEP_MS;
                    if due && local.can_sleep(&id) {
                        manager.release(&mut local, &id);
                    }
                }
            }
        });
    }

    fn release(&self, local: &mut Local, id: &str) {
        let session = local.sessions.get_mut(id).unwrap();
        session.ready = false;
        session.releasing = true;
        if let Some(handle) = &session.handle {
            handle.request_shutdown();
        }
    }

    /// Claude reads its login only at start, so after a new sign-in an idle agent sleeps
    /// before its next prompt; the queued prompt wakes it with the new login.
    pub(super) fn sleep_for_login(&self, local: &mut Local, id: &str) -> bool {
        let session = &local.sessions[id];
        let changed = session.can_resume()
            && local.can_sleep(id)
            && session
                .handle
                .as_ref()
                .is_some_and(|h| h.login_changed(&self.config));
        if changed {
            self.release(local, id);
        }
        changed
    }

    /// Archive asks for sleep. It applies once any running turn or interrupt settles.
    pub(super) fn request_sleep(
        self: &Arc<Self>,
        local: &mut Local,
        id: &str,
        request: String,
    ) -> Result<Receipt> {
        let receipt = Receipt {
            request_id: request.clone(),
            command: "sleep".into(),
            input: json!({}),
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
        let session = &local.sessions[id];
        if session.handle.is_some() {
            if local.can_sleep(id) {
                self.release(local, id);
            }
        } else if !session.busy() {
            if session.can_resume() && session.state != "sleeping" {
                local.append(id, "state", json!({"state":"sleeping"}), None)?;
            }
            local.finish_receipt(id, &request, "completed")?;
        }
        Ok(receipt)
    }

    /// The released harness exited; history stays on disk for the next resume.
    pub(super) fn slept(self: &Arc<Self>, local: &mut Local, id: &str) -> Result<()> {
        let session = local.sessions.get_mut(id).ok_or(Error::NotFound)?;
        session.handle = None;
        session.ready = false;
        session.releasing = false;
        session.awake = None;
        local.append(id, "harness", json!({"pid":null}), None)?;
        local.append(id, "state", json!({"state":"sleeping"}), None)?;
        let pending: Vec<_> = local.sessions[id]
            .receipts
            .values()
            .filter(|r| r.command == "sleep" && r.state == "accepted")
            .map(|r| r.request_id.clone())
            .collect();
        for request in pending {
            local.finish_receipt(id, &request, "completed")?;
        }
        self.wake(local, id)
    }

    /// Work arrived for a sleeping session: resume its native conversation.
    pub(super) fn wake(self: &Arc<Self>, local: &mut Local, id: &str) -> Result<()> {
        let s = &local.sessions[id];
        if s.state == "sleeping" && s.handle.is_none() && s.has_work() && s.can_resume() {
            self.schedule_resume(local, id, false)?;
        }
        Ok(())
    }
}
