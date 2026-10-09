# HTTP API reference

Every Cloudroom feature is available over this API. It is plain HTTP on `127.0.0.1:9840` by default; put an HTTPS proxy in front for remote clients ([setup](setup.md)). The TypeScript client in `integrations/bb/client.ts` wraps it.

## Conventions

- **Auth:** send `Authorization: Bearer CLOUDROOM_TOKEN` on every request, including health checks and streams. Anything else returns `401`.
- **Commands are asynchronous.** A `POST` returns `202` with `{session_id, receipt, saving}`. `202` means saved, not finished.
- **Request IDs:** every command carries a `request_id` of 1–64 letters, digits, `_` or `-`. Retrying an ID returns the same receipt and never repeats work. Reusing an ID for different content returns `request_conflict`.
- **Session IDs** are `cr_` plus the start request's ID. `POST /v1/sessions` with `request_id: "demo"` creates `cr_demo`.
- **Receipts** move from `accepted` to `delivered`, then end as `completed`, `interrupted`, `failed`, `unknown` or `unknown_after_restart`. Uncertain work is never resent.
- **Limits:** JSON bodies up to 64 KiB, prompt text up to 32,768 bytes. Check `GET /v1/capabilities` before using optional features; a missing flag means unsupported.

## Health and discovery

- `GET /v1/health`: service status and disk safety snapshot.
- `GET /v1/ready`: `200` only when storage and the database are usable, otherwise `503`.
- `GET /v1/capabilities`: feature flags, plus each configured harness with its models, reasoning levels and options.
- `GET /v1/dashboard` and `GET /v1/metrics`: safe machine and session summaries for dashboards ([dashboard API](dashboard.md)).

## Sessions

- `GET /v1/sessions`: the latest 1,000 sessions, newest activity first, as `{total, sessions}`. Each summary has `session_id`, `harness`, `model`, `provider`, `state`, `workspace`, `parent_session`, `current_request`, `queued`, `last_sequence` and `last_activity_ms`.
- `POST /v1/sessions`: start a session. Body: `request_id`, plus optional `harness` (`codex`, `claude-code`, `pi`, `cursor`, `opencode`), `model`, `reasoning`, `provider` (Pi only), `workspace`, `workspace_name`, `command_guard_enabled` and `system_prompt` (up to 32768 bytes; added to the harness's system prompt, or to each Cursor prompt, and inherited by children).
  - With `parent_session` and `prompt` (plus optional `harness`, `model`, `reasoning` and `title`), it starts a child thread instead: session `cr_child_{request_id}`, in the parent's folder, with the parent's guard and system prompt. The parent keeps working. Each time a child turn ends, Core queues the parent a `notice_…` prompt with the child's reply, even while no app is connected. The parent records a `child_thread` with the child's `id`, `harness` and `title`. Agents use the same path through `cloudroom thread spawn|list|output|tell`. Capability: `child_threads`.
  - With `fork: {session, before?, last_turn_id?}` (plus optional `harness`), it starts a side chat: session `cr_{request_id}` on a copy of that Claude Code or Codex session's conversation, in its folder, with its harness, model, guard and system prompt. `before` (Claude Code) cuts before that user-message checkpoint; `last_turn_id` (Codex) cuts after that turn. Without either it copies the whole conversation, so the source must be idle. The source is only read. A side chat may also use its source's child threads. Capability: `side_chat`.
- `GET /v1/sessions/{id}`: full state, including receipts and the queue.
- `GET /v1/sessions/{id}/workspace`: the session's folder, branch and commit.
- `GET /v1/sessions/{id}/recovery`: whether a stopped session can resume ([lifecycle](session-lifecycle.md)).

Commands, all `POST /v1/sessions/{id}/...` with a `request_id`:

- `prompts`: send a message. Body: `text`, plus optional `content`, `attachments`, `reasoning` and `service_tier`. Messages sent while busy queue in order.
- `edit`: replace a queued message. Adds `target_request_id` and `expected_revision`.
- `cancel`: remove a queued message (`target_request_id`).
- `reorder`: set the queue order (`order`: request IDs). Unlisted queued messages keep their place after the listed ones.
- `steer`: add guidance to the running turn (`target_request_id`, `text`), where the harness supports it.
- `interrupt`: stop the current turn (`target_request_id`). Queued messages continue.
- `stop` and `resume`: pause and restart the queue.
- `sleep`: release an idle harness process. The next message wakes it.
- `compact`: ask the harness to compact its context.
- `rewind`: go back to an earlier message (`before` or `last_turn_id`), with an optional `replacement` prompt.
- `attachments?request_id=ID&name=NAME&kind=image|file`: upload a raw file body (images up to 10 MiB, files up to 25 MiB).
  With `upload_parts`, add `&offset=N&total=SIZE` to send it in parts of at most 8 MiB (clients use 4 MiB, since some sandbox proxies drop requests over about 8 MB). Earlier parts return `{"received":N}`; the last returns the receipt. Repeated parts are ignored, gaps refused, and unfinished uploads expire after 10 minutes.
- `close`: end the session and keep its history.

`POST /v1/sessions/{id}/secrets/{request}` answers an agent's secret request with the requested values. It takes no `request_id`.

## History and live events

- `GET /v1/sessions/{id}/events?after=N`: up to 256 records after sequence `N`. Page with the last returned `sequence`.
- `GET /v1/sessions/{id}/stream?after=N`: server-sent events named `record`. Each event's `id` is its sequence, so reconnecting with `Last-Event-ID` resumes exactly.

Each record is `{sequence, session_id, kind, data, native?, timestamp_ms?}`. `native` holds the harness's original output line. The database copy leaves it out of `native_record` and Claude records (ADR 0203).

- **Lifecycle:** `receipt`, `state`, `harness`, `workspace`, `native_identity`, `launch_reasoning`, `checkpoint`, `usage`, `usage_limited`, `child`, `child_result`, `child_thread`, `rewind`, `rewind_ready`, `rewind_failed`, `teleport`, `secret_request`, `interaction_cancelled`, `native_history_unavailable`, `prompt_warning` (a selected skill could not load; the prompt still runs).
- **Disk safety:** `storage_warning`, `storage_warning_delivery`, `storage_pause`, `storage_recovered`.
- **Harness output:** `text_delta`, `thinking_delta`, `item_started`, `item_completed`, `tool_delta`, `tool_snapshot`, `native_event` and `native_record`.

Output records are not yet harness-neutral. Kinds are shared, but `data` differs by harness; Codex records also carry the native app-server `method`. Check `session.harness` before parsing `data`.

Session `state` values: `pending`, `starting`, `resuming`, `starting_turn`, `running`, `interrupting`, `closing`, `idle`, `sleeping`, `suspended`, `rewinding`, `closed`, `failed`, `process_lost`, `waiting_for_files`, and `saved_history_only` for history read from the database.

## Accounts

Harness logins run on the VM. Responses report status and sign-in links, never tokens ([harnesses](harnesses.md)).

- Codex: `GET /v1/accounts/codex`; `POST .../login`, `.../import`, `.../switched`, `.../cancel`.
- Claude Code: `GET /v1/accounts/claude`; `POST .../{login|cancel|complete|token}`.
- Cursor: `GET /v1/accounts/cursor`; `POST .../{login|cancel|key}`.
- Pi: `GET /v1/accounts/pi`; `POST .../import`, `.../key`, `.../setup`.

## Workspaces, sync, previews and transfers

- `GET /v1/workspaces/{id}`: a cloud folder mapping ([cloud folders](setup.md#cloud-folders)).
- `GET /v1/settings`, `POST /v1/sync`, `GET /v1/sync/{id}`, `GET|PUT /v1/sync/{id}/file`: skills and settings sync ([sync](setup.md#skills-and-login-sync)).
- `/v1/previews...`: open cloud web servers on your laptop's localhost ([previews](previews.md)).
- `GET /v1/terminals/{id}?cols=C&rows=R[&session=S][&since=N][&command=CMD]` with `Upgrade: websocket`: an interactive shell on a PTY, in session `S`'s workspace. After `101`, both sides send frames with no WebSocket framing: a kind byte, a big-endian u32 length, then the data. Core sends `0` output, `1` exit `{"code"}`, `2` hello `{"offset","cwd","shell"}` and `3` ping; the app sends `0` input, `1` resize (cols and rows as u16) and `3` ping. Reconnect with `since` set to the output bytes received; Core replays up to 1 MiB, and a new hello marks skipped output. An unknown ID with `since` above 0 answers `410`: the shell ended when Core restarted. The shell gets the agents' Claude Code, Codex and Cursor logins, read once at start. `DELETE /v1/terminals/{id}` ends the shell and everything it started.
- `POST /v1/teleports`, `POST /v1/teleports/check`, `GET /v1/teleports/{id}`, `POST .../activate`, `.../cancel`, `.../files/{index}`: move a local conversation and its files to the cloud.
- `GET /v1/mac/jobs`, `POST /v1/mac/results/{id}`, `POST /v1/vm/run`: two-way Mac and VM access for a paired Mac.
  Large Mac output arrives as `part` reports (`offset` in that stream's hex text) before the final `done`.

## Errors

Errors return `{"error": "...", "code": "..."}`. Show `error` to people; branch on `code`.

- `401`: missing or wrong token.
- `404`: unknown session or other resource.
- `409`: rejected. Some codes are temporary: `storage_blocked`, `service_stopping`, `model_catalog_unavailable`, and the `*_auth_unavailable` codes.
- `503` with `storage_unavailable`: saving failed. Retry with the same `request_id`.

Codes: `invalid_model`, `invalid_provider`, `invalid_reasoning_effort`, `invalid_service_tier`, `invalid_workspace`, `invalid_attachment`, `attachment_too_large`, `attachment_permission_denied`, `request_conflict`, `harness_not_configured`, `unsupported_command`, `storage_blocked`, `service_stopping`, `model_catalog_unavailable`, `codex_auth_required`, `codex_auth_unavailable`, `codex_auth_busy`, `codex_usage_limit`, `claude_auth_required`, `claude_auth_unavailable`, `cursor_auth_required`, `cursor_auth_unavailable`, `cursor_auth_busy`, `teleport_rejected`, `teleport_cancelled`, `teleport_running`, and `request_rejected` for everything else.
