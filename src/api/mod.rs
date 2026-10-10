use crate::{
    observability::{Observability, Signal, elapsed_ms},
    session::{self, Manager},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, Version, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};
use tokio_stream::wrappers::ReceiverStream;

pub fn router(manager: Arc<Manager>, token: String) -> Router {
    Router::new()
        .merge(crate::preview::routes())
        .merge(crate::runtime::terminal::routes())
        .merge(crate::mac::routes())
        .merge(crate::secrets::routes())
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/accounts/cursor", get(cursor_auth))
        .route("/v1/accounts/cursor/{action}", post(cursor_account))
        .route("/v1/accounts/claude", get(claude_auth))
        .route("/v1/accounts/claude/version", post(claude_version))
        .route("/v1/accounts/claude/{action}", post(claude_account))
        .route("/v1/accounts/pi", get(pi_auth))
        .route("/v1/accounts/pi/import", post(pi_import))
        .route("/v1/accounts/pi/setup", post(pi_setup))
        .route("/v1/accounts/pi/key", post(pi_key))
        .route("/v1/accounts/codex", get(codex_auth))
        .route("/v1/accounts/codex/login", post(codex_login))
        .route("/v1/accounts/codex/import", post(codex_import))
        .route("/v1/accounts/codex/switched", post(codex_switched))
        .route("/v1/accounts/codex/cancel", post(codex_cancel))
        .route("/v1/workspaces/{id}", get(workspace))
        .route("/v1/settings", get(crate::sync::settings))
        .route("/v1/sync", post(crate::sync::check_in))
        .route("/v1/sync/{id}", get(crate::sync::scan))
        .route(
            "/v1/sync/{id}/file",
            get(crate::sync::read)
                .put(crate::sync::apply)
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/dashboard", get(dashboard))
        .route("/v1/drain", post(drain))
        .route("/v1/metrics", get(metrics))
        .route(
            "/v1/teleports",
            post(teleport_prepare).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route("/v1/teleports/check", post(teleport_check))
        .route("/v1/teleports/{id}", get(teleport_status))
        .route("/v1/teleports/{id}/activate", post(teleport_activate))
        .route("/v1/teleports/{id}/cancel", post(teleport_cancel))
        .route(
            "/v1/teleports/{id}/files/{index}",
            post(teleport_upload).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route("/v1/sessions", post(start).get(list_sessions))
        .route("/v1/sessions/{id}", get(status))
        .route("/v1/sessions/{id}/workspace", get(session_workspace))
        .route("/v1/sessions/{id}/recovery", get(recovery))
        .route("/v1/sessions/{id}/prompts", post(prompt))
        .route("/v1/sessions/{id}/edit", post(edit))
        .route("/v1/sessions/{id}/cancel", post(cancel))
        .route("/v1/sessions/{id}/reorder", post(reorder))
        .route("/v1/sessions/{id}/steer", post(steer))
        .route("/v1/sessions/{id}/compact", post(compact))
        .route("/v1/sessions/{id}/goal", post(goal))
        .route("/v1/sessions/{id}/rewind", post(rewind))
        .route(
            "/v1/sessions/{id}/attachments",
            post(attach).layer(DefaultBodyLimit::max(26 * 1024 * 1024)),
        )
        .route("/v1/sessions/{id}/interrupt", post(interrupt))
        .route("/v1/sessions/{id}/stop", post(stop))
        .route("/v1/sessions/{id}/sleep", post(sleep))
        .route("/v1/sessions/{id}/detach", post(detach))
        .route("/v1/sessions/{id}/resume", post(resume))
        .route("/v1/sessions/{id}/close", post(close))
        .route("/v1/sessions/{id}/events", get(events))
        .route("/v1/sessions/{id}/stream", get(stream))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            Arc::new(token),
            authenticate,
        ))
        .layer(middleware::from_fn_with_state(
            manager.observability.clone(),
            observe,
        ))
        .with_state(manager)
}

async fn cursor_auth(State(manager): State<Arc<Manager>>) -> Json<crate::runtime::auth::Status> {
    Json(manager.cursor_auth_status().await)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorAccountRequest {
    request_id: String,
    api_key: Option<String>,
}
async fn cursor_account(
    State(manager): State<Arc<Manager>>,
    Path(action): Path<String>,
    Json(input): Json<CursorAccountRequest>,
) -> Result<(StatusCode, Json<crate::runtime::auth::Status>)> {
    crate::workspace::valid_id(&input.request_id)
        .map_err(|_| session::Error::Conflict("invalid request ID"))?;
    if !matches!(action.as_str(), "login" | "cancel" | "key")
        || (action == "key") != input.api_key.is_some()
    {
        return Err(session::Error::Conflict("invalid Cursor account operation"));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(
            manager
                .cursor_account(&action, input.request_id, input.api_key)
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaudeAccountRequest {
    request_id: String,
    code: Option<String>,
    state: Option<String>,
    token: Option<String>,
    plan: Option<String>,
    api_key: Option<String>,
}
async fn claude_auth(State(manager): State<Arc<Manager>>) -> Json<crate::runtime::auth::Status> {
    Json(manager.claude_auth_status().await)
}
async fn claude_account(
    State(manager): State<Arc<Manager>>,
    Path(action): Path<String>,
    Json(input): Json<ClaudeAccountRequest>,
) -> Result<(StatusCode, Json<crate::runtime::auth::Status>)> {
    crate::workspace::valid_id(&input.request_id)
        .map_err(|_| session::Error::Conflict("invalid request ID"))?;
    let valid = |value: &Option<String>, max: usize| {
        value.as_ref().is_some_and(|s| {
            !s.is_empty()
                && s.len() <= max
                && s.bytes().all(|c| c.is_ascii_graphic())
                && !s.starts_with("sk-ant-")
        })
    };
    let secret = |value: &Option<String>, prefix: &str| {
        value.as_ref().is_some_and(|s| {
            s.starts_with(prefix)
                && s.len() <= 1024
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        })
    };
    let token = secret(&input.token, "sk-ant-oat");
    let key = input
        .api_key
        .as_deref()
        .is_some_and(crate::runtime::claude_login::is_api_key);
    if !matches!(
        action.as_str(),
        "login" | "cancel" | "complete" | "token" | "key"
    ) || (action == "complete" && (!valid(&input.code, 2048) || !valid(&input.state, 512)))
        || (action != "complete" && (input.code.is_some() || input.state.is_some()))
        || (action == "token") != token
        || (action != "token" && input.token.is_some())
        || (action == "key") != key
        || (action != "key" && input.api_key.is_some())
        || (action != "token" && input.plan.is_some())
        || input.plan.as_ref().is_some_and(|s| {
            s.is_empty() || s.len() > 32 || !s.bytes().all(|c| c.is_ascii_lowercase() || c == b'_')
        })
    {
        return Err(session::Error::Conflict("invalid Claude sign-in request"));
    }
    let (code, state) = match action.as_str() {
        "token" => (input.token, input.plan),
        "key" => (input.api_key, None),
        _ => (input.code, input.state),
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(
            manager
                .claude_account(&action, input.request_id, code, state)
                .await?,
        ),
    ))
}

fn pi_error(error: std::io::Error) -> session::Error {
    if error.kind() == std::io::ErrorKind::InvalidInput {
        session::Error::Conflict("invalid Pi login")
    } else {
        error.into()
    }
}
async fn pi_auth(State(manager): State<Arc<Manager>>) -> Result<Json<Value>> {
    let result = crate::runtime::pi_auth::providers(&manager.config, &manager.storage).await;
    Ok(Json(result.map_err(pi_error)?))
}
async fn pi_import(
    State(manager): State<Arc<Manager>>,
    Json(credentials): Json<Value>,
) -> Result<Json<Value>> {
    if manager.is_stopping() {
        return Err(session::Error::Conflict("service is stopping"));
    }
    let result =
        crate::runtime::pi_auth::import(&manager.config, &manager.storage, credentials).await;
    Ok(Json(result.map_err(pi_error)?))
}
/// Copies the Mac's custom Pi providers and installs its packages in the background.
async fn pi_setup(
    State(manager): State<Arc<Manager>>,
    Json(setup): Json<Value>,
) -> Result<Json<Value>> {
    if manager.is_stopping() {
        return Err(session::Error::Conflict("service is stopping"));
    }
    let (result, missing) =
        crate::runtime::pi_auth::setup(&manager.config, &manager.storage, setup)
            .await
            .map_err(pi_error)?;
    if !missing.is_empty() {
        let manager = manager.clone();
        tokio::spawn(async move {
            crate::runtime::pi_auth::install(&manager.config, &manager.storage, missing).await
        });
    }
    Ok(Json(result))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PiKey {
    provider: String,
    key: String,
}
async fn pi_key(
    State(manager): State<Arc<Manager>>,
    Json(input): Json<PiKey>,
) -> Result<Json<Value>> {
    if manager.is_stopping() {
        return Err(session::Error::Conflict("service is stopping"));
    }
    let result = crate::runtime::pi_auth::set_key(
        &manager.config,
        &manager.storage,
        input.provider,
        input.key,
    )
    .await;
    Ok(Json(result.map_err(pi_error)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Drain {
    hold: bool,
}
/// Sandbox hosts call this before stopping the machine (see session::sleep).
async fn drain(State(manager): State<Arc<Manager>>, Json(input): Json<Drain>) -> Json<Value> {
    Json(manager.drain(input.hold))
}

async fn codex_auth(State(manager): State<Arc<Manager>>) -> Json<crate::runtime::auth::Status> {
    Json(manager.codex_auth_status().await)
}
async fn codex_login(
    State(manager): State<Arc<Manager>>,
    Json(input): Json<RequestId>,
) -> Result<(StatusCode, Json<crate::runtime::auth::Status>)> {
    crate::workspace::valid_id(&input.request_id)
        .map_err(|_| session::Error::Conflict("invalid request ID"))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(manager.codex_login(input.request_id).await?),
    ))
}
async fn codex_import(
    State(manager): State<Arc<Manager>>,
    Json(credentials): Json<Value>,
) -> Result<Json<crate::runtime::auth::Status>> {
    Ok(Json(manager.import_codex_login(credentials).await?))
}
async fn codex_switched(State(manager): State<Arc<Manager>>) -> Json<Value> {
    let (status, continued) = manager.codex_switched().await;
    Json(json!({"account":status,"continued":continued}))
}
async fn codex_cancel(
    State(manager): State<Arc<Manager>>,
    Json(input): Json<RequestId>,
) -> (StatusCode, Json<crate::runtime::auth::Status>) {
    (
        StatusCode::ACCEPTED,
        Json(manager.cancel_codex_login(&input.request_id).await),
    )
}

async fn teleport_prepare(
    State(manager): State<Arc<Manager>>,
    Json(manifest): Json<session::teleport::Manifest>,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(manager.teleport_prepare(manifest).await?),
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaudeVersion {
    version: String,
}
async fn claude_version(
    State(manager): State<Arc<Manager>>,
    Json(input): Json<ClaudeVersion>,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(manager.match_claude_version(input.version).await?),
    ))
}
async fn teleport_check(
    State(manager): State<Arc<Manager>>,
    Json(check): Json<session::teleport::Check>,
) -> (StatusCode, Json<Value>) {
    (
        StatusCode::ACCEPTED,
        Json(manager.teleport_check(check).await),
    )
}
async fn teleport_status(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(manager.teleport_status(&id).await?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TeleportActivation {
    retry_request_id: Option<String>,
}
async fn teleport_activate(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<TeleportActivation>,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            manager
                .teleport_activate(&id, body.retry_request_id)
                .await?,
        ),
    ))
}
async fn teleport_cancel(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(manager.teleport_cancel(&id).await?),
    ))
}
#[derive(Deserialize)]
struct TeleportOffset {
    offset: u64,
    sha256: String,
    size: Option<u64>,
}
async fn teleport_upload(
    State(manager): State<Arc<Manager>>,
    Path((id, index)): Path<(String, usize)>,
    Query(query): Query<TeleportOffset>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            manager
                .teleport_upload(&id, index, query.offset, query.sha256, query.size, body)
                .await?,
        ),
    ))
}

async fn observe(
    State(diagnostics): State<Observability>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let version = request.version();
    let method = match request.method().as_str() {
        "GET" => "GET",
        "POST" => "POST",
        _ => "other",
    };
    // Route templates only: arbitrary URLs, query strings and headers may contain secrets.
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_owned();
    let mut response = next.run(request).await;
    // The hosting gateway retains idle upstream sockets. Keep SSE streams and upgraded tunnels, not completed requests.
    if matches!(version, Version::HTTP_10 | Version::HTTP_11)
        && response.status() != StatusCode::SWITCHING_PROTOCOLS
        && !response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"text/event-stream"))
    {
        response
            .headers_mut()
            .insert(header::CONNECTION, "close".parse().unwrap());
    }
    let id = diagnostics.record(Signal::Api {
        method,
        route,
        status: response.status().as_u16(),
        duration_ms: elapsed_ms(started),
    });
    response.headers_mut().insert(
        "x-cloudroom-diagnostic-id",
        id.parse().expect("generated diagnostic ID"),
    );
    response
}

async fn authenticate(State(token): State<Arc<String>>, request: Request, next: Next) -> Response {
    // Some sandbox proxies replace Authorization with their own login, so the token may also arrive in X-Cloudroom-Token.
    let headers = request.headers();
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let custom = headers
        .get("x-cloudroom-token")
        .and_then(|h| h.to_str().ok());
    let authorized = [bearer, custom]
        .into_iter()
        .flatten()
        .any(|supplied| same_token(supplied, token.as_str()));
    let mut response = if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(json!({"error":"unauthorized"})),
        )
            .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

/// Compares every byte, so response timing does not reveal how much of the token matched.
fn same_token(supplied: &str, expected: &str) -> bool {
    let (supplied, expected) = (supplied.as_bytes(), expected.as_bytes());
    let difference = supplied
        .iter()
        .zip(expected)
        .fold(0, |difference, (a, b)| difference | (a ^ b));
    supplied.len() == expected.len() && std::hint::black_box(difference) == 0
}

impl IntoResponse for session::Error {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "session not found"),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Storage(detail) => {
                eprintln!("storage unavailable: {detail}");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "error": format!("storage unavailable ({detail}); retry with the same request_id"),
                        "code": "storage_unavailable",
                    })),
                )
                    .into_response();
            }
        };
        let code = match error {
            "invalid model" => "invalid_model",
            "invalid provider" | "provider selection requires Pi" => "invalid_provider",
            "invalid reasoning effort" => "invalid_reasoning_effort",
            "invalid service tier" => "invalid_service_tier",
            "invalid teleport manifest"
            | "invalid teleport file"
            | "invalid transfer ID"
            | "invalid upload offset"
            | "overlapping upload"
            | "upload content changed"
            | "upload checksum changed"
            | "teleport file validation failed"
            | "session text is still uploading"
            | "saved transfer requires recovery"
            | "native conversation already belongs to a cloud session" => "teleport_rejected",
            "transfer cancelled" => "teleport_cancelled",
            "cloud execution already owns this transfer; use Stop" => "teleport_running",
            "Cloud folder permission denied" => "attachment_permission_denied",
            "attachment upload failed" => "invalid_attachment",
            "image exceeds the 10 MiB limit" | "file exceeds the 25 MiB limit" => {
                "attachment_too_large"
            }
            "request_id already has different content"
            | "session already exists"
            | "saved session has no matching receipt" => "request_conflict",
            "agent setup is incomplete" | "harness is not configured" => "harness_not_configured",
            "invalid workspace" | "workspace mapping unavailable" => "invalid_workspace",
            "storage unsafe; new execution is blocked" => "storage_blocked",
            "service is stopping" => "service_stopping",
            "service is draining" => "service_draining",
            "model catalog unavailable" => "model_catalog_unavailable",
            "connect Codex before starting cloud work" => "codex_auth_required",
            "connect Claude Code before starting cloud work" => "claude_auth_required",
            "Claude account could not be verified" => "claude_auth_unavailable",
            "Codex usage limit reached" => "codex_usage_limit",
            "Codex account could not be verified" => "codex_auth_unavailable",
            "connect Cursor before starting cloud work" => "cursor_auth_required",
            "Cursor account could not be verified" => "cursor_auth_unavailable",
            "close Cursor sessions before changing the cloud login" => "cursor_auth_busy",
            "finish active Codex work before signing in" => "codex_auth_busy",
            _ => "request_rejected",
        };
        (status, Json(json!({"error":error,"code":code}))).into_response()
    }
}

type Result<T> = std::result::Result<T, session::Error>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestId {
    request_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    request_id: String,
    harness: Option<crate::runtime::Kind>,
    model: Option<String>,
    reasoning: Option<String>,
    workspace: Option<String>,
    provider: Option<String>,
    workspace_name: Option<String>,
    command_guard_enabled: Option<bool>,
    strip_ai_co_authors: Option<bool>,
    system_prompt: Option<String>,
    /// Starts a child thread in this parent's folder; it inherits the parent's settings.
    parent_session: Option<String>,
    prompt: Option<String>,
    title: Option<String>,
    /// Starts a side chat on a copy of this session's conversation; it inherits the source's settings.
    fork: Option<session::ForkFrom>,
}

async fn workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match manager.workspaces.get(&id) {
        Ok(Some(workspace)) => (StatusCode::OK, Json(json!(workspace))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"workspace not found"})),
        ),
        Err(error) => (
            StatusCode::CONFLICT,
            Json(json!({"error":format!("workspace unavailable: {error}")})),
        ),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prompt {
    request_id: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    attachments: Option<Value>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    request_id: String,
    target_request_id: String,
    expected_revision: u64,
    #[serde(default)]
    text: String,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    attachments: Option<Value>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    request_id: String,
    target_request_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reorder {
    request_id: String,
    order: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Steer {
    request_id: String,
    target_request_id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rewind {
    request_id: String,
    replacement: Option<Prompt>,
    #[serde(default)]
    before: Option<String>,
    #[serde(default)]
    last_turn_id: Option<String>,
    #[serde(default)]
    fork: bool,
}
#[derive(Deserialize)]
struct AttachQuery {
    request_id: String,
    name: String,
    kind: String,
    /// Large files arrive in parts (`upload_parts`): this part's byte offset and the whole file's size.
    offset: Option<usize>,
    total: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Interrupt {
    request_id: String,
    target_request_id: String,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    #[serde(default)]
    after: u64,
}

fn key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(session::Error::Conflict(
            "request_id must be 1-64 ASCII letters, digits, underscores or hyphens",
        ));
    }
    Ok(())
}
fn cursor(after: u64) -> Result<()> {
    if after > i64::MAX as u64 {
        return Err(session::Error::Conflict("invalid replay cursor"));
    }
    Ok(())
}

async fn dashboard(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(manager.dashboard())
}

async fn list_sessions(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(manager.list_sessions())
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricsQuery {
    #[serde(default)]
    range: crate::observability::metrics::Range,
}
async fn metrics(
    State(manager): State<Arc<Manager>>,
    Query(query): Query<MetricsQuery>,
) -> Response {
    match manager.observability.metrics.read(query.range).await {
        Ok(value) => Json(value).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error":"Metric history is unavailable. Check diagnostic storage; agents may still be working."
        }))).into_response(),
    }
}

async fn ready(State(manager): State<Arc<Manager>>) -> impl IntoResponse {
    let ready = manager.ready().await;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({"ready":ready})),
    )
}

async fn capabilities(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(manager.capabilities().await)
}

async fn stop(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "stop", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn resume(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "resume", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn sleep(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "sleep", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn detach(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>)> {
    manager.detach(&id)?;
    Ok((StatusCode::ACCEPTED, Json(json!({"detached":true}))))
}

async fn health(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(json!({"status":"ready","saving":manager.saving(),"storage":manager.storage.snapshot()}))
}
async fn start(
    State(manager): State<Arc<Manager>>,
    Json(body): Json<Start>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    manager.mac.expect();
    if body.provider.as_ref().is_some_and(|p| {
        p.is_empty()
            || p.chars()
                .any(|c| c.is_control() || c.is_whitespace() || c == '/')
    }) {
        return Err(session::Error::Conflict("invalid provider"));
    }
    if body
        .model
        .as_ref()
        .is_some_and(|m| m.is_empty() || m.len() > 256 || m.chars().any(char::is_control))
    {
        return Err(session::Error::Conflict("invalid model"));
    }
    if body
        .reasoning
        .as_deref()
        .is_some_and(|r| r.is_empty() || r.len() > 64 || !r.bytes().all(|b| b.is_ascii_lowercase()))
    {
        return Err(session::Error::Conflict("invalid reasoning effort"));
    }
    if body
        .system_prompt
        .as_ref()
        .is_some_and(|p| p.trim().is_empty() || p.len() > 32768 || p.contains('\0'))
    {
        return Err(session::Error::Conflict("invalid system prompt"));
    }
    if let Some(fork) = body.fork {
        if body.parent_session.is_some()
            || body.prompt.is_some()
            || body.title.is_some()
            || body.model.is_some()
            || body.reasoning.is_some()
            || body.workspace.is_some()
            || body.workspace_name.is_some()
            || body.provider.is_some()
            || body.command_guard_enabled.is_some()
            || body.strip_ai_co_authors.is_some()
            || body.system_prompt.is_some()
        {
            return Err(session::Error::Conflict(
                "a side chat takes only harness and fork",
            ));
        }
        let (id, receipt) = manager.fork(body.request_id, body.harness, fork).await?;
        return Ok(accepted(&manager, &id, receipt));
    }
    if let Some(parent) = body.parent_session {
        let prompt = body
            .prompt
            .filter(|p| !p.trim().is_empty() && p.len() <= 32768);
        let title = body.title.map(|t| t.trim().to_owned());
        if body.workspace.is_some()
            || body.workspace_name.is_some()
            || body.provider.is_some()
            || body.command_guard_enabled.is_some()
            || body.strip_ai_co_authors.is_some()
            || body.system_prompt.is_some()
            || title.as_ref().is_some_and(|t| {
                t.is_empty() || t.chars().count() > 200 || t.chars().any(char::is_control)
            })
        {
            return Err(session::Error::Conflict(
                "a child takes only harness, model, reasoning, title and prompt",
            ));
        }
        let prompt = prompt.ok_or(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ))?;
        let spawn = session::Spawn {
            harness: body.harness,
            model: body.model,
            reasoning: body.reasoning,
            title,
            prompt,
        };
        let (id, receipt) = manager
            .spawn_child(&parent, &body.request_id, spawn)
            .await?;
        return Ok(accepted(&manager, &id, receipt));
    }
    if body.prompt.is_some() || body.title.is_some() {
        return Err(session::Error::Conflict(
            "prompt and title require parent_session",
        ));
    }
    let (id, receipt) = manager
        .start(
            body.request_id,
            body.harness,
            body.model,
            body.reasoning,
            (body.workspace, body.workspace_name),
            (
                body.provider,
                body.command_guard_enabled,
                body.strip_ai_co_authors,
                body.system_prompt,
            ),
        )
        .await?;
    Ok(accepted(&manager, &id, receipt))
}

fn accepted(manager: &Manager, id: &str, receipt: session::Receipt) -> (StatusCode, Json<Value>) {
    (
        StatusCode::ACCEPTED,
        Json(json!({"session_id":id,"receipt":receipt,"saving":manager.saving()})),
    )
}

async fn close(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "close", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn status(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(
        json!({"session":manager.session(&id).await?,"saving":manager.saving()}),
    ))
}
async fn recovery(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    let session = manager.session(&id).await?;
    Ok(Json(manager.recovery_check(&session)))
}
async fn session_workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    let session = manager.session(&id).await?;
    let workspace = session.workspace.ok_or(session::Error::NotFound)?;
    Ok(Json(manager.workspaces.checkout(&workspace).await?))
}

fn prompt_input(
    text: String,
    content: Option<Value>,
    attachments: Option<Value>,
    reasoning: Option<String>,
    service_tier: Option<String>,
) -> Result<Value> {
    let has_attachments = attachments
        .as_ref()
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty());
    if (text.trim().is_empty() && !has_attachments) || text.len() > 32768 {
        return Err(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ));
    }
    let mut input = json!({"text": text});
    if let Some(content) = content {
        input["content"] = content;
    }
    if let Some(attachments) = attachments {
        input["attachments"] = attachments;
    }
    if let Some(reasoning) = reasoning {
        input["reasoning"] = json!(reasoning);
    }
    if let Some(service_tier) = service_tier {
        input["service_tier"] = json!(service_tier);
    }
    Ok(input)
}

async fn prompt(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Prompt>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let input = prompt_input(
        body.text,
        body.content,
        body.attachments,
        body.reasoning.clone(),
        body.service_tier.clone(),
    )?;
    // An accepted retry must not depend on the model catalog still being available.
    if !manager.known_request(&id, &body.request_id) {
        if body.reasoning.as_deref().is_some_and(|level| {
            level.is_empty() || level.len() > 64 || !level.bytes().all(|b| b.is_ascii_lowercase())
        }) {
            return Err(session::Error::Conflict("invalid reasoning effort"));
        }
        manager
            .check_prompt_reasoning(&id, body.reasoning.as_deref())
            .await?;
        manager.check_prompt_service_tier(&id, body.service_tier.as_deref())?;
    }
    let receipt = manager.command(&id, body.request_id, "prompt", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn edit(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Edit>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let mut input = prompt_input(
        body.text,
        body.content,
        body.attachments,
        body.reasoning.clone(),
        body.service_tier.clone(),
    )?;
    input["target_request_id"] = json!(body.target_request_id);
    input["expected_revision"] = json!(body.expected_revision);
    if !manager.known_request(&id, &body.request_id) {
        manager
            .check_prompt_reasoning(&id, body.reasoning.as_deref())
            .await?;
        manager.check_prompt_service_tier(&id, body.service_tier.as_deref())?;
    }
    let receipt = manager.command(&id, body.request_id, "edit", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn cancel(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Target>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let receipt = manager.command(
        &id,
        body.request_id,
        "cancel",
        json!({"target_request_id":body.target_request_id}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}

async fn reorder(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Reorder>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    for target in &body.order {
        key(target)?;
    }
    let receipt = manager.command(&id, body.request_id, "reorder", json!({"order":body.order}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn steer(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Steer>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    if body.text.trim().is_empty() || body.text.len() > 32768 {
        return Err(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ));
    }
    let receipt = manager.command(
        &id,
        body.request_id,
        "steer",
        json!({"target_request_id":body.target_request_id,"text":body.text}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Goal {
    request_id: String,
    status: Option<String>,
    objective: Option<String>,
    #[serde(default)]
    clear: bool,
}

/// The user's goal control: pause, resume, replace the objective, or clear.
async fn goal(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Goal>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    if body
        .status
        .as_deref()
        .is_some_and(|status| !matches!(status, "active" | "paused"))
        || body
            .objective
            .as_ref()
            .is_some_and(|text| text.trim().is_empty() || text.len() > 4000)
        || (body.clear == (body.status.is_some() || body.objective.is_some()))
    {
        return Err(session::Error::Conflict(
            "goal needs clear, or an active/paused status and/or an objective of 1-4000 bytes",
        ));
    }
    let input = json!({"status":body.status,"objective":body.objective,"clear":body.clear});
    let receipt = manager.command(&id, body.request_id, "goal", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn compact(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "compact", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn rewind(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Rewind>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let mut input = json!({});
    if let Some(before) = body.before {
        input["before"] = json!(before);
    }
    if let Some(last_turn_id) = body.last_turn_id {
        input["last_turn_id"] = json!(last_turn_id);
    }
    if body.fork {
        input["fork"] = json!(true);
    }
    if let Some(prompt) = body.replacement {
        key(&prompt.request_id)?;
        let payload = prompt_input(
            prompt.text,
            prompt.content,
            prompt.attachments,
            prompt.reasoning.clone(),
            prompt.service_tier.clone(),
        )?;
        if !manager.known_request(&id, &body.request_id) {
            manager
                .check_prompt_reasoning(&id, prompt.reasoning.as_deref())
                .await?;
            manager.check_prompt_service_tier(&id, prompt.service_tier.as_deref())?;
        }
        input["replacement"] = json!({"request_id":prompt.request_id,"input":payload});
    }
    let receipt = manager.command(&id, body.request_id, "rewind", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn attach(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<AttachQuery>,
    body: Body,
) -> Result<impl IntoResponse> {
    key(&query.request_id)?;
    // A part is read before any reply, so a retried last part gets its receipt instead of a reset connection.
    let (body, part) = match query.total {
        Some(_) => (
            Body::empty(),
            Some(
                axum::body::to_bytes(body, PART_BYTES)
                    .await
                    .map_err(|_| session::Error::Conflict("upload part exceeds 8 MiB"))?,
            ),
        ),
        None => (body, None),
    };
    if let Some(receipt) = manager.receipt(&id, &query.request_id) {
        if receipt.command != "attach" {
            return Err(session::Error::Conflict(
                "request_id already has different content",
            ));
        }
        return Ok(accepted(&manager, &id, receipt));
    }
    let session = manager.session(&id).await?;
    let workspace = session
        .workspace
        .ok_or(session::Error::Conflict("invalid workspace"))?;
    if workspace.id != "legacy" && manager.workspaces.get(&workspace.id)?.is_none() {
        return Err(session::Error::Conflict("workspace mapping unavailable"));
    }
    if !manager.recording_available() {
        return Err(session::Error::Storage(
            "session journal is not writable".into(),
        ));
    }
    if manager.is_stopping() || manager.storage.blocks() {
        return Err(session::Error::Conflict(
            "storage unsafe; uploads are blocked",
        ));
    }
    let key = format!("{id}/{}", query.request_id);
    let body = match (query.offset, query.total, part) {
        (None, None, _) => body,
        (Some(offset), Some(total), Some(part)) => {
            let limit = if query.kind == "image" {
                10 << 20
            } else {
                25 << 20
            };
            if total > limit {
                return Err(session::Error::Conflict(if query.kind == "image" {
                    "image exceeds the 10 MiB limit"
                } else {
                    "file exceeds the 25 MiB limit"
                }));
            }
            match attach_part(&key, offset, total, &part).map_err(session::Error::Conflict)? {
                (_, Some(whole)) => Body::from(whole),
                (received, None) => {
                    return Ok((StatusCode::ACCEPTED, Json(json!({ "received": received }))));
                }
            }
        }
        _ => {
            return Err(session::Error::Conflict(
                "offset and total must be sent together",
            ));
        }
    };
    let written = manager
        .workspaces
        .attach(
            &workspace,
            &query.request_id,
            &query.name,
            &query.kind,
            body,
            &manager.storage,
        )
        .await
        .map_err(|error| {
            eprintln!(
                "attachment upload failed: session={id} request={} kind={:?} errno={:?}: {error}",
                query.request_id,
                error.kind(),
                error.raw_os_error()
            );
            session::Error::Conflict(match error.kind() {
                std::io::ErrorKind::WouldBlock => "storage unsafe; uploads are blocked",
                std::io::ErrorKind::PermissionDenied => "Cloud folder permission denied",
                std::io::ErrorKind::FileTooLarge if query.kind == "image" => {
                    "image exceeds the 10 MiB limit"
                }
                std::io::ErrorKind::FileTooLarge => "file exceeds the 25 MiB limit",
                _ => "attachment upload failed",
            })
        })?;
    let receipt = manager.command(&id, query.request_id, "attach", written)?;
    PARTS.lock().unwrap().remove(&key);
    Ok(accepted(&manager, &id, receipt))
}

/// Attachments arriving in parts, by session and request: (total size, bytes so far, last part). Some sandbox
/// proxies drop requests over about 8 MB, so clients send large files in 4 MiB parts. Unfinished uploads expire.
type Partial = (usize, Vec<u8>, Instant);
static PARTS: LazyLock<Mutex<HashMap<String, Partial>>> = LazyLock::new(Default::default);
const PART_BYTES: usize = 8 << 20;

/// Adds one part; returns the bytes received and, once complete, the whole file. The partial stays until its
/// receipt is recorded, so a retried last part can save it again.
fn attach_part(
    key: &str,
    offset: usize,
    total: usize,
    part: &[u8],
) -> std::result::Result<(usize, Option<Vec<u8>>), &'static str> {
    let mut parts = PARTS.lock().unwrap();
    parts.retain(|_, (_, _, at)| at.elapsed() < Duration::from_secs(600));
    if offset == 0 && !parts.contains_key(key) {
        if parts.len() >= 8 {
            return Err("too many uploads in progress; try again");
        }
        parts.insert(key.to_owned(), (total, Vec::new(), Instant::now()));
    }
    let (size, bytes, at) = parts.get_mut(key).ok_or("upload part out of order")?;
    if *size != total {
        return Err("upload size changed");
    }
    append_part(bytes, offset, part, total)?;
    *at = Instant::now();
    Ok((bytes.len(), (bytes.len() == total).then(|| bytes.clone())))
}

/// Appends one part of an upload that arrives in pieces. A repeated part is ignored, so retrying after a lost
/// response is safe; a gap or overflow is refused.
pub(crate) fn append_part(
    buffer: &mut Vec<u8>,
    offset: usize,
    part: &[u8],
    total: usize,
) -> std::result::Result<(), &'static str> {
    let end = offset
        .checked_add(part.len())
        .filter(|end| *end <= total)
        .ok_or("upload part exceeds the upload size")?;
    if offset == buffer.len() {
        buffer.extend_from_slice(part);
    } else if end > buffer.len() {
        return Err("upload part out of order");
    }
    Ok(())
}
async fn interrupt(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Interrupt>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let receipt = manager.command(
        &id,
        body.request_id,
        "interrupt",
        json!({"target_request_id":body.target_request_id}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}
async fn events(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<Cursor>,
) -> Result<Json<Value>> {
    cursor(query.after)?;
    manager.session(&id).await?;
    Ok(Json(
        json!({"events":manager.records(&id,query.after).await?,"saving":manager.saving()}),
    ))
}
async fn stream(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<Cursor>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let mut after = match headers.get("last-event-id") {
        Some(header) => header
            .to_str()
            .ok()
            .and_then(|h| h.parse().ok())
            .ok_or(session::Error::Conflict("invalid Last-Event-ID"))?,
        None => query.after,
    };
    cursor(after)?;
    let mut changed = manager.subscribe();
    let session = manager.session(&id).await?;
    if after > session.last_sequence {
        return Err(session::Error::Conflict("cursor exceeds session history"));
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        loop {
            if manager.is_stopping() {
                return;
            }
            match manager.records(&id, after).await {
                Ok(records) if !records.is_empty() => {
                    for record in records {
                        let sequence = record.sequence;
                        let event = Event::default()
                            .id(sequence.to_string())
                            .event("record")
                            .data(serde_json::to_string(&record).expect("record serialization"));
                        tokio::select! {
                            result = sender.send(Ok::<_, Infallible>(event)) => if result.is_err() { return; },
                            _ = changed.wait_for(|_| manager.is_stopping()) => return,
                        }
                        after = sequence;
                    }
                }
                Ok(_) => tokio::select! {
                    _=sender.closed()=>return,
                    result=changed.changed()=>if result.is_err(){return;},
                },
                Err(_) => return,
            }
        }
    });
    Ok(Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::default()))
}
