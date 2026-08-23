//! tollgate-server binary: env-driven config with backend selection.
//!
//! `TOLLGATE_BIND` (default `127.0.0.1:8080`) — listen address. The admin
//! surface is unauthenticated: keep this loopback (or otherwise
//! network-restricted) until the control-plane credential seam lands
//! (docs/DESIGN.md); a non-loopback bind logs a warning.
//! `TOLLGATE_STORE` — `memory` (default; ephemeral, dev/demo only) or, when
//! built with the default `postgres` feature, `postgres` (durable; requires
//! `TOLLGATE_PG_URL`, runs migrations on startup).
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
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

/// A connection string with its userinfo removed: enough to identify which
/// database was unreachable, never enough to authenticate to it.
#[cfg(feature = "postgres")]
fn redact_dsn(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, host)) => format!("{scheme}://<redacted>@{host}"),
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}

/// The sweep cadence, or `None` if the configured value cannot run a sweep.
/// A zero interval would spin the reclaim loop without ever waiting, so it is
/// refused at startup rather than accepted into a busy loop (INVARIANTS #16).
fn reclaim_interval(secs: u64) -> Option<std::time::Duration> {
    (secs > 0).then(|| std::time::Duration::from_secs(secs))
}

/// Whether this bind address keeps the unauthenticated admin surface off the
/// network. Deliberately conservative: anything not recognisably loopback is
/// treated as exposed, because the failure mode of guessing wrong is an open
/// admin API.
fn is_loopback(bind: &str) -> bool {
    bind.starts_with("127.") || bind.starts_with("localhost") || bind.starts_with("[::1]")
}

/// Warn when this bind puts the unauthenticated admin surface on a network.
/// Kept out of `main` so the decision is testable: warning on the wrong side
/// of it is the difference between a caught misconfiguration and a silent
/// open admin API.
fn warn_if_exposed(bind: &str) {
    if !is_loopback(bind) {
        tracing::warn!(
            %bind,
            "binding exposes an unauthenticated admin surface; keep tollgate-server \
             loopback-only until control-plane auth exists"
        );
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();
    let bind = std::env::var("TOLLGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let reclaim_secs: u64 = std::env::var("TOLLGATE_RECLAIM_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let Some(reclaim) = reclaim_interval(reclaim_secs) else {
        tracing::error!("TOLLGATE_RECLAIM_INTERVAL_SECS must be positive");
        std::process::exit(2);
    };
    let backend = std::env::var("TOLLGATE_STORE").unwrap_or_else(|_| "memory".to_string());

    warn_if_exposed(&bind);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let shutdown = async {
        // A failed handler install means this process cannot be asked to stop
        // gracefully — worth saying, since the symptom otherwise appears much
        // later as a SIGKILL with unflushed usage.
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for shutdown signal");
        }
    };

    match backend.as_str() {
        "memory" => {
            tracing::info!(%bind, backend = "memory", "tollgate-server listening (ephemeral, dev/demo only)");
            serve(
                listener,
                ServerState {
                    store: MemoryStore::new(GrantPolicy::default())
                        .expect("default grant policy is valid"),
                    clock: Arc::new(SystemClock),
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
                .unwrap_or_else(|e| {
                    // A DSN carries a password, and a driver error may quote
                    // the DSN back at us — so the target is reported with its
                    // userinfo stripped, and the error text has any verbatim
                    // copy of the DSN removed (AGENTS.md: never log
                    // credential-bearing database URLs).
                    let target = redact_dsn(&url);
                    tracing::error!(
                        %target,
                        error = e.to_string().replace(&url, &target),
                        "cannot connect to postgres"
                    );
                    std::process::exit(2);
                });
            tracing::info!(%bind, backend = "postgres", "tollgate-server listening");
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

    /// Credential redaction is security behavior, not formatting: a DSN in a
    /// log line is a leaked password (AGENTS.md).
    #[cfg(feature = "postgres")]
    #[test]
    fn redact_dsn_strips_userinfo() {
        assert_eq!(
            redact_dsn("postgres://user:hunter2@db.internal:5432/tollgate"),
            "postgres://<redacted>@db.internal:5432/tollgate"
        );
        assert!(!redact_dsn("postgres://user:hunter2@db/tollgate").contains("hunter2"));
        // Nothing to strip: pass through rather than mangle.
        assert_eq!(
            redact_dsn("postgres://db.internal/tollgate"),
            "postgres://db.internal/tollgate"
        );
        assert_eq!(redact_dsn("not-a-url"), "not-a-url");
    }

    /// The warning must fire on exactly the exposed binds and no others:
    /// inverted, it would be silent precisely when it matters.
    #[test]
    fn exposure_warning_fires_only_for_exposed_binds() {
        use std::sync::{Arc, Mutex};
        use tracing::subscriber::with_default;
        use tracing_subscriber::layer::SubscriberExt as _;

        #[derive(Clone, Default)]
        struct Count(Arc<Mutex<usize>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Count {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() == tracing::Level::WARN {
                    *self.0.lock().unwrap() += 1;
                }
            }
        }

        let warned = |bind: &str| {
            let count = Count::default();
            with_default(tracing_subscriber::registry().with(count.clone()), || {
                warn_if_exposed(bind);
            });
            *count.0.lock().unwrap()
        };

        assert_eq!(warned("0.0.0.0:8080"), 1, "an exposed bind must warn");
        assert_eq!(warned("127.0.0.1:8080"), 0, "loopback must stay quiet");
    }

    /// Guessing wrong here leaves an unauthenticated admin API on the
    /// network, so the predicate is pinned in both directions.
    #[test]
    fn only_recognisable_loopback_binds_are_treated_as_safe() {
        for safe in ["127.0.0.1:8080", "localhost:8080", "[::1]:8080"] {
            assert!(is_loopback(safe), "{safe} is loopback");
        }
        for exposed in [
            "0.0.0.0:8080",
            "10.0.0.5:8080",
            "192.168.1.10:8080",
            "example.com:8080",
            "[::]:8080",
        ] {
            assert!(
                !is_loopback(exposed),
                "{exposed} must be treated as exposed"
            );
        }
    }
}
