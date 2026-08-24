//! pricing-api binary.
//!
//! `PRICING_BIND` (default `127.0.0.1:8081`), `PRICING_DEPOSIT` (default
//! `1_000_000` units), and `TOLLGATE_LOCAL_SHARDS` (default `1`). One demo
//! account, API key `demo-key-1`:
//!
//! ```sh
//! curl -s -H 'Authorization: Bearer demo-key-1' \
//!      -H 'Content-Type: application/json' \
//!      -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
//!      http://127.0.0.1:8081/v1/price
//! ```

use std::num::NonZeroUsize;

use pricing_api::build_app_with_sharding;
use tollgate_core::LocalSharding;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("PRICING_BIND").unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    let deposit: u64 = std::env::var("PRICING_DEPOSIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let sharding = match std::env::var("TOLLGATE_LOCAL_SHARDS") {
        Ok(value) => LocalSharding::new(value.parse::<NonZeroUsize>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("TOLLGATE_LOCAL_SHARDS must be a positive integer: {error}"),
            )
        })?),
        Err(std::env::VarError::NotPresent) => LocalSharding::SINGLE,
        Err(error) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cannot read TOLLGATE_LOCAL_SHARDS: {error}"),
            ));
        }
    };

    // The embedding application installs the subscriber; the tollgate
    // libraries only emit. `RUST_LOG=tollgate_client=debug` turns up the
    // control plane without touching this service.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let (router, runtime) = build_app_with_sharding(deposit, true, sharding);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, deposit, local_shards = sharding.get(), "pricing-api listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    // Flush billing, then release leases (order matters — INVARIANTS.md).
    runtime.shutdown().await;
    Ok(())
}
