//! quota-server binary: memory backend, env-driven config.
//!
//! `QUOTA_BIND` (default `127.0.0.1:8080`) — listen address.
//! `QUOTA_RECLAIM_INTERVAL_SECS` (default `5`) — expiry-sweep cadence.
//!
//! The PoC binary always runs the in-memory backend; the Postgres backend
//! arrives as a second construction path here, not a different server.

use std::sync::Arc;

use quota_server::{ServerState, serve};
use quota_store::{GrantPolicy, MemoryStore, SystemClock};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("QUOTA_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let reclaim_secs: u64 = std::env::var("QUOTA_RECLAIM_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    let state = ServerState {
        store: MemoryStore::new(GrantPolicy::default()),
        clock: Arc::new(SystemClock),
    };
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("quota-server listening on {bind}");
    serve(
        listener,
        state,
        std::time::Duration::from_secs(reclaim_secs),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
}
