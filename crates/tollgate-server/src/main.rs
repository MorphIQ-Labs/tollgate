//! tollgate-server binary: env-driven config with backend selection.
//!
//! `TOLLGATE_SECURITY_CONFIG` — required identity/TLS JSON manifest, reloaded
//! every five seconds. Its optional `issuer` entry enables customer-credential
//! issuance, fixed for the process lifetime. See
//! `docs/CONTROL_PLANE_SECURITY.md`.
//! `TOLLGATE_BIND` (default `127.0.0.1:8080`) — listen address. Non-loopback
//! listeners require TLS; every protected route requires an identity.
//! `TOLLGATE_STORE` — `memory` (default; ephemeral, dev/demo only) or, when
//! built with the default `postgres` feature, `postgres` (durable; requires
//! `TOLLGATE_PG_URL`, runs migrations on startup). The memory backend never
//! forgets a usage event — it is the idempotency index — so its footprint
//! grows with lifetime request count and it is not a soak- or load-test
//! target. Each reclaim sweep logs what it is holding at `debug`.
//! `TOLLGATE_RECLAIM_INTERVAL_SECS` (default `5`) — expiry-sweep cadence.

use std::sync::Arc;

use tollgate_server::{ServerState, serve};
use tollgate_store::{GrantPolicy, MemoryStore, SystemClock};
#[cfg(feature = "postgres")]
use tollgate_store_postgres::PostgresStore;

#[cfg(feature = "postgres")]
const COMPILED_BACKENDS: &str = "memory|postgres";
#[cfg(not(feature = "postgres"))]
const COMPILED_BACKENDS: &str = "memory";

/// Install the process-wide subscriber. Libraries emit; only a binary decides
/// where events go, and `RUST_LOG` is how an operator turns the control plane
/// up without a rebuild.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
                .add_directive(
                    "tollgate::audit=info"
                        .parse()
                        .expect("static audit directive"),
                ),
        )
        .init();
}

/// The sweep cadence, or `None` if the configured value cannot run a sweep.
/// A zero interval would spin the reclaim loop without ever waiting, so it is
/// refused at startup rather than accepted into a busy loop (INVARIANTS GL-16).
fn reclaim_interval(secs: u64) -> Option<std::time::Duration> {
    (secs > 0).then(|| std::time::Duration::from_secs(secs))
}

fn main() -> std::io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    for arg in args.iter().take_while(|arg| *arg != "--") {
        match arg.to_str() {
            Some("--help" | "-h") => {
                println!(
                    "tollgate-server {}\nUsage: tollgate-server [--help | --version] [--]\nConfiguration: TOLLGATE_SECURITY_CONFIG (required JSON manifest), TOLLGATE_BIND (default 127.0.0.1:8080), TOLLGATE_STORE ({COMPILED_BACKENDS}), TOLLGATE_PG_URL, TOLLGATE_RECLAIM_INTERVAL_SECS (default 5).\nThe manifest's optional issuer.secret_file enables credential issuance; it applies at start only.\nSee docs/CONTROL_PLANE_SECURITY.md. Credentials reload every five seconds.",
                    env!("CARGO_PKG_VERSION")
                );
                return Ok(());
            }
            Some("--version" | "-V") => {
                println!("tollgate-server {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            _ => {}
        }
    }
    if !(args.is_empty() || args == ["--"]) {
        eprintln!("tollgate-server: unexpected argument; use --help");
        std::process::exit(2);
    }
    run_server()
}

#[tokio::main]
async fn run_server() -> std::io::Result<()> {
    init_tracing();
    let bind = std::env::var("TOLLGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let reclaim_secs: u64 = std::env::var("TOLLGATE_RECLAIM_INTERVAL_SECS")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .unwrap_or_else(|_| {
            tracing::error!("TOLLGATE_RECLAIM_INTERVAL_SECS must be an integer");
            std::process::exit(2)
        });
    let Some(reclaim) = reclaim_interval(reclaim_secs) else {
        tracing::error!("TOLLGATE_RECLAIM_INTERVAL_SECS must be positive");
        std::process::exit(2);
    };
    let backend = std::env::var("TOLLGATE_STORE").unwrap_or_else(|_| "memory".to_string());

    if !COMPILED_BACKENDS
        .split('|')
        .any(|compiled| compiled == backend)
    {
        tracing::error!(
            store = backend,
            "unknown TOLLGATE_STORE (expected {COMPILED_BACKENDS})"
        );
        std::process::exit(2);
    }
    if backend == "postgres" && std::env::var("TOLLGATE_PG_URL").is_err() {
        tracing::error!("TOLLGATE_STORE=postgres requires TOLLGATE_PG_URL");
        std::process::exit(2);
    }
    let path = std::env::var("TOLLGATE_SECURITY_CONFIG").unwrap_or_else(|_| {
        tracing::error!("TOLLGATE_SECURITY_CONFIG is required; see --help");
        std::process::exit(2)
    });
    let clock: Arc<dyn tollgate_store::Clock> = Arc::new(SystemClock);
    let mut loader = tollgate_server::config::SecurityLoader::new(path);
    let loaded = loader
        .load(clock.now())
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::other("initial security configuration missing"))?;
    let security = loader.start(loaded).map_err(std::io::Error::other)?;
    // Taken before the loader moves into the reloader, which never replaces it.
    let issuer = loader.issuer();
    let issuance = if issuer.is_some() {
        "enabled"
    } else {
        "disabled"
    };
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    if !security.encrypted()
        && !tollgate_server::transport::is_loopback(listener.local_addr()?.ip())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "non-loopback listeners require TLS",
        ));
    }
    let bind = listener.local_addr()?.to_string();
    tracing::info!(target: "tollgate::audit", "administrative audit events remain enabled independently of RUST_LOG");
    let _reloader = tollgate_server::config::SecurityReloader::spawn(
        loader,
        Arc::clone(&security),
        Arc::clone(&clock),
    );
    #[cfg(unix)]
    let mut termination =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result { tracing::error!(%error, "cannot listen for shutdown signal"); }
            },
            _ = termination.recv() => {},
        }
        #[cfg(not(unix))]
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for shutdown signal");
        }
    };

    match backend.as_str() {
        "memory" => {
            tracing::info!(
                %bind,
                backend = "memory",
                issuance,
                "tollgate-server listening (ephemeral, dev/demo only; memory grows with \
                 lifetime request count — not a soak or load-test target)"
            );
            serve(
                listener,
                ServerState {
                    store: MemoryStore::new(GrantPolicy::default())
                        .expect("default grant policy is valid"),
                    clock: Arc::clone(&clock),
                    security: Arc::clone(&security),
                    // The manifest's durable `issuer`, never the per-start
                    // control-plane bearer registry; absent answers 501.
                    issuer,
                },
                reclaim,
                shutdown,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let url = std::env::var("TOLLGATE_PG_URL").unwrap_or_else(|_| {
                tracing::error!("TOLLGATE_STORE=postgres requires TOLLGATE_PG_URL");
                std::process::exit(2);
            });
            let store = PostgresStore::connect(&url, GrantPolicy::default())
                .await
                .unwrap_or_else(|_| {
                    // Both the URL and arbitrary driver text may carry secrets
                    // outside userinfo, including query or keyword parameters.
                    tracing::error!(
                        backend = "postgres",
                        operation = "connect",
                        code = "storage",
                        "cannot initialize postgres; verify connectivity, TOLLGATE_PG_URL credentials and schema migrations"
                    );
                    std::process::exit(2);
                });
            tracing::info!(%bind, backend = "postgres", issuance, "tollgate-server listening");
            serve(
                listener,
                ServerState {
                    store,
                    clock: Arc::clone(&clock),
                    security: Arc::clone(&security),
                    // The manifest's durable `issuer`, never the per-start
                    // control-plane bearer registry; absent answers 501.
                    issuer,
                },
                reclaim,
                shutdown,
            )
            .await
        }
        other => {
            tracing::error!(
                store = other,
                "unknown TOLLGATE_STORE (expected {COMPILED_BACKENDS})"
            );
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Zero is the one value that cannot work: a sweep loop with no wait
    /// spins. Everything else is a cadence.
    #[test]
    fn only_a_zero_reclaim_interval_is_refused() {
        assert_eq!(reclaim_interval(0), None);
        assert_eq!(reclaim_interval(1), Some(std::time::Duration::from_secs(1)));
        assert_eq!(
            reclaim_interval(3_600),
            Some(std::time::Duration::from_secs(3_600))
        );
    }
}
