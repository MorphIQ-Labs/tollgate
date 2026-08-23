//! pricing-api binary.
//!
//! `PRICING_BIND` (default `127.0.0.1:8081`), `PRICING_DEPOSIT` (default
//! `1_000_000` units). One demo account, API key `demo-key-1`:
//!
//! ```sh
//! curl -s -H 'Authorization: Bearer demo-key-1' \
//!      -H 'Content-Type: application/json' \
//!      -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
//!      http://127.0.0.1:8081/v1/price
//! ```

use pricing_api::build_app;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("PRICING_BIND").unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    let deposit: u64 = std::env::var("PRICING_DEPOSIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);

    // The embedding application installs the subscriber; the tollgate
    // libraries only emit. `RUST_LOG=tollgate_client=debug` turns up the
    // control plane without touching this service.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let (router, runtime) = build_app(deposit, true);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, deposit, "pricing-api listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    // Flush billing, then release leases (order matters — INVARIANTS.md).
    runtime.shutdown().await;
    Ok(())
}
