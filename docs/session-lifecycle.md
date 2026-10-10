# Session lifecycle and replay

## Inspect a session's checkout

Authenticated `GET /v1/sessions/{id}/workspace` reads the session's cloud directory and returns `{path, branch, head}`. Detached HEAD has a null branch; an unborn branch has a null head. Both are null when Git metadata is unavailable. Reads never change the checkout or start a harness. Missing session/workspace data or an unreadable directory returns an error.

## Close a session

`POST /v1/sessions/{id}/close` requires the same bearer authentication as other commands:

```json
{"request_id":"close-this-session"}
```

HTTP 202 means the close request is saved locally. Wait for its receipt to become `completed` and the session state to become `closed`. New prompts are rejected once close is accepted. Retrying the same request ID returns its receipt without repeating execution.

Close releases the harness slot and preserves history. Start with a new request ID to create another session; closing never resumes or replaces the old one. A session still initializing returns 409 instead of accepting close prematurely.

Runtime closes Codex's stdin, allowing native cleanup, and continues collecting records until exit. A four-second grace bounds an unresponsive harness; forced or unsuccessful exits produce `unknown`, not a successful close receipt. Protected Linux execution also stops the session's workload cgroup and waits until it has no live descendants. Cleanup failure blocks replacement; it is never treated as a successful close.

Service shutdown signals all harnesses before waiting. Session recording and best-effort diagnostics get a five-second budget. HTTP draining ends after six seconds even if a peer stops reading. Shutdown is distinct from client disconnect, which never stops execution.

## Sleep idle sessions

- After 30 minutes without activity, Core stops the session's harness process and marks it `sleeping`. History stays on disk. A running turn, queued work, busy child sessions, pending interrupts, or new background processes in its workload keep it awake. Unprotected local runs cannot see background processes.
- `POST /v1/sessions/{id}/sleep` (same body as close) asks for sleep now. It applies once a running turn or interrupt settles. The GUI sends Stop, then Sleep, on Archive.
- The next prompt, Teleport update, or `resume` of a paused queue resumes the same native conversation, then delivers the work. Compact or rewind on a sleeping session wakes it and returns 409; retry once it is idle.
- Restart relaunches only sessions with queued work or activity in the last 30 minutes. Other resumable sessions stay or become `sleeping`, so a reboot does not refill memory.

## Queued prompts and restart

- A prompt that arrives while a turn is running is saved locally, then acknowledged with an `accepted` receipt. Queued prompts run in acceptance order, one turn at a time. Retrying a request ID returns its receipt; different content for the same ID is rejected.
- Acceptance and queue membership come from one fsynced receipt. Old separate `enqueue` records remain readable without duplicate delivery, including a crash between the old two writes.
- Interrupt stops the current turn only; queued prompts continue afterwards. Close prevents further delivery and marks unrun queued receipts `failed`.
- Normal service shutdown leaves eligible sessions `suspended`. Both normal and abrupt restarts resume the same saved Codex conversation, now or when work arrives (see sleep above). Deliberately closed sessions stay closed; historical `process_lost`/`failed` sessions are not automatically reopened.
- If a pending start's harness is no longer configured, that start and its unrun queued prompts fail with a recorded reason. Other sessions and history remain available. Restore configuration and use a new start request ID; retrying the failed ID does not launch or replay work.
- A harness that fails to launch stays `failed` with its cause and stderr tail; it never ran the prompt. `cloudroom thread list|output` show a failed or lost child's cause as `error`.
- An unexpected harness exit gets one recovery attempt after confirmed cleanup. A new user request or completed/interrupted turn permits a later attempt. Failed native resumes become `process_lost`; there is no restart loop or fresh-conversation fallback. Timeouts during native resume preserve unrun queued receipts, including across service restarts; other failures settle them as `failed`.
- New and restored harnesses share a startup queue. At most half the available logical CPUs (minimum one) initialize concurrently; this does not cap running sessions. Startup RPCs allow 120 seconds instead of the ordinary 30-second deadline, and retain storage-pause-aware timing. A normal shutdown while waiting for or performing recovery leaves the saved conversation resumable.
- Unfinished dispatched work becomes `unknown` on shutdown or `unknown_after_restart` on a crash and is never resent. Queued work waits for the native resume handshake, sufficient storage, and a ready harness. This does not promise continuation of an interrupted task or exactly-once external effects.
- A parent hears about each finished child turn, except turns a restart cut off. `POST /v1/sessions/{id}/detach` (empty body) makes a child top-level: its parent hears no more about it. The GUI sends it before new work reaches an unarchived child.
- Before admitting execution, protected Linux startup clears only its validated workload subtree and confirms it is empty. Runtime also confirms each session's descendants have exited before recovery. Saved PIDs or similar command text never authorize killing a process. Unprotected local tests merely wait for an old PID to disappear; uncertainty blocks replacement and provides no descendant-containment guarantee.

## Recovery preflight and explicit retry

Authenticated `GET /v1/sessions/{id}/recovery` is read-only. It returns `status` and `message`: `ready` means a readable saved conversation exists, `empty` means none exists and no turn was dispatched, `missing` means dispatched work has lost its native history, and `unavailable` covers closed/history-only sessions or unsafe/unreadable files. File availability is not proof that the native handshake will succeed.

Check this before deployment for every session expected to resume. Do not silently replace or discard `empty`/`missing` sessions to pass verification.

A failed resume exposes `startup_error` (`startup_timeout`, `empty_session`, `missing_history`, or `resume_failed`). After correcting the cause, `POST /v1/sessions/{id}/resume` with a new `request_id` explicitly retries a failed startup only after old-process cleanup and a successful history preflight. Its receipt completes after the native handshake. It unpauses the queue, but never replays completed, failed, or uncertain prompts. Retrying the same request ID does not launch another process. Closed sessions stay closed.

## Storage and replay

- All locally accepted records are fsynced before subscribers are notified. Native turn events own progress; RPC replies acknowledge delivery or rejection.
- Native checkpoints are validated before journal persistence. Malformed native history fails that session without writing invalid checkpoints; source history is retained. Already-corrupt journals require explicit repair, never silent deletion.
- A local journal write failure makes readiness and dashboard runtime readiness false and blocks new mutations. Saved history remains readable. Correct the storage problem and restart for validated recovery; never reset the write guard blindly. A remote database outage alone still permits local buffering.
- Local replay uses a rebuilt, per-session sequence index. It does not scan other sessions.
- `snapshot.json` saves the replayed state at a journal position: after startup, on drain and shutdown, and every 20,000 records. Startup restores it and replays only newer records. A missing, unreadable or older snapshot means a full replay. After editing old records by hand, delete it.
- Claude Code's tool-input deltas are not recorded. `item_started` carries the complete input.
- Local and database event pages contain at most 256 records. Continue after the last returned sequence; sequences need not be contiguous within a session.
- Saved-only metadata and the requested latest receipt share one bounded-memory scan, separate from replay pagination. Parsing stays in Rust because native output may contain NUL characters that PostgreSQL JSON processing rejects.
- New native-event records omit the duplicate `data.value` payload on disk and in PostgreSQL. The existing API field is reconstructed from the original frame on reads. Old records are unchanged, including during upload retries. No schema migration is needed.
- Saved-only sessions still do not restore running processes or populate the full receipt map.

## Verify

```sh
cargo fmt --check
CARGO_BUILD_JOBS=1 cargo clippy --locked --all-targets -- -D warnings
CARGO_BUILD_JOBS=1 cargo test --locked --all-targets
CARGO_BUILD_JOBS=1 cargo build --locked
python3 tests/core_fixture.py
python3 tests/core_e2e.py --fixture
python3 tests/core_e2e.py
```

The standalone fixtures use no inference or database. `--fixture` adds isolated PostgreSQL and HTTP lifecycle checks on Linux. The full E2E command uses the configured Codex account and checks actual tool exit before cleanup. Missing resources must fail explicitly; fixtures are not real-inference evidence.
