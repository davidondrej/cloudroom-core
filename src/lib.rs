pub mod api;
pub mod computer_use;
pub mod config;
pub mod mac;
mod observability;
pub mod preview;
pub mod runtime;
pub mod secrets;
pub mod session;
pub mod sync;
pub mod workspace;

/// Bumped by `tools/release-core.py` on every release (ADR 0120).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The git commit this binary was built from, set by `build.rs`.
pub const COMMIT: &str = env!("CLOUDROOM_COMMIT");
/// Features `/v1/capabilities` always reports. Release pins record this list, so CI can
/// check that new VMs support everything the app uses.
pub const FEATURES: &[&str] = &[
    "stop",
    "resume",
    "launch_settings",
    "prompt_reasoning",
    "workspaces",
    "sync",
    "mcp_sync",
    "direct_workspaces",
    "root_workspace",
    "teleport",
    "command_guard",
    "strip_ai_co_authors",
    "system_prompt",
    "computer_use",
    "codex_auth",
    "codex_auth_import",
    "cursor_auth",
    "claude_auth",
    "pi_auth_import",
    "pi_setup",
    "structured_prompt",
    "queue_edit",
    "queue_cancel",
    "queue_reorder",
    "steer",
    "rewind",
    "fork",
    "side_chat",
    "attachments",
    "upload_parts",
    "compact",
    "goal",
    "usage",
    "subagents",
    "child_threads",
    "session_list",
    "drain",
    "terminals",
    "files_wait",
];

pub async fn serve(mut config: config::Config) -> Result<(), Box<dyn std::error::Error>> {
    use std::{future::IntoFuture, time::Duration};
    config.account_home = config.account_home.canonicalize()?;
    config.repository = config.repository.canonicalize()?;
    for profile in config.harnesses.values_mut() {
        profile.home = profile.home.canonicalize()?;
        profile.binary = profile.binary.canonicalize()?;
    }
    // A bind failure must not mutate saved sessions or claim their recovery.
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let manager = session::Manager::open(config.clone())?;
    manager.check_storage().await;
    // Reconcile previous workloads before the API or storage guard can start work.
    manager.restore_all().await?;
    manager.previews.listen().await?;
    if let Err(error) = mac::listen(&manager).await {
        eprintln!("Mac access unavailable: {error}");
    }
    manager.start_storage_guard();
    manager.start_idle_sleeper();
    manager.start_uploader();
    manager.start_auth_monitor();
    eprintln!("Cloudroom listening on {}", listener.local_addr()?);
    let shutdown = manager.clone();
    let mut changed = manager.subscribe();
    let server = axum::serve(listener, api::router(manager.clone(), config.token))
        .with_graceful_shutdown(async move {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal handler");
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
            shutdown.shutdown().await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = async {
            let _ = changed.wait_for(|_| manager.is_stopping()).await;
            // A peer may stop reading bytes already buffered by HTTP, outside the SSE queue.
            tokio::time::sleep(runtime::SHUTDOWN_GRACE + Duration::from_secs(2)).await;
        } => eprintln!("Cloudroom stopped waiting for unresponsive HTTP clients"),
    }
    Ok(())
}
