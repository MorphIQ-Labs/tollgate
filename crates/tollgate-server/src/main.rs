//! tollgate-server binary: env-driven config with backend selection.
//!
//! `TOLLGATE_BIND` (default `127.0.0.1:8080`) — listen address. The admin
//! surface is unauthenticated: keep this loopback (or otherwise
//! network-restricted) until the control-plane credential seam lands
//! (docs/DESIGN.md); a non-loopback bind logs a warning.
//! `TOLLGATE_STORE` — `memory` (default; ephemeral, dev/demo only) or
//! `postgres` (durable; requires `TOLLGATE_PG_URL`, runs migrations on
//! startup).
//! `TOLLGATE_RECLAIM_INTERVAL_SECS` (default `5`) — expiry-sweep cadence.

use std::sync::Arc;

use tollgate_server::{ServerState, serve};
use tollgate_store::{GrantPolicy, MemoryStore, SystemClock};
use tollgate_store_postgres::PostgresStore;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("TOLLGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let reclaim_secs: u64 = std::env::var("TOLLGATE_RECLAIM_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let backend = std::env::var("TOLLGATE_STORE").unwrap_or_else(|_| "memory".to_string());

    if !bind.starts_with("127.") && !bind.starts_with("localhost") && !bind.starts_with("[::1]") {
        eprintln!(
            "WARNING: binding {bind} exposes an unauthenticated admin surface; \
             keep tollgate-server loopback-only until control-plane auth exists"
        );
    }

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let reclaim = std::time::Duration::from_secs(reclaim_secs);
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    match backend.as_str() {
        "memory" => {
            eprintln!(
                "tollgate-server listening on {bind} (memory backend — ephemeral, dev/demo only)"
            );
            serve(
                listener,
                ServerState {
                    store: MemoryStore::new(GrantPolicy::default()),
                    clock: Arc::new(SystemClock),
                },
                reclaim,
                shutdown,
            )
            .await
        }
        "postgres" => {
            let url = std::env::var("TOLLGATE_PG_URL").unwrap_or_else(|_| {
                eprintln!("TOLLGATE_STORE=postgres requires TOLLGATE_PG_URL");
                std::process::exit(2);
            });
            let store = PostgresStore::connect(&url, GrantPolicy::default())
                .await
                .unwrap_or_else(|e| {
                    eprintln!("cannot connect to postgres: {e}");
                    std::process::exit(2);
                });
            eprintln!("tollgate-server listening on {bind} (postgres backend)");
            serve(
                listener,
                ServerState {
                    store,
                    clock: Arc::new(SystemClock),
                },
                reclaim,
                shutdown,
            )
            .await
        }
        other => {
            eprintln!("unknown TOLLGATE_STORE {other:?} (expected memory|postgres)");
            std::process::exit(2);
        }
    }
}
