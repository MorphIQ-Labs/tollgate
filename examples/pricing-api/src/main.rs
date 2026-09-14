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

use std::{ffi::OsString, num::NonZeroUsize, process::ExitCode};

use pricing_api::{PricingConnection, build_app_with_sharding};
use tollgate_core::LocalSharding;

enum Startup {
    Help,
    Version,
    Serve,
}

fn parse_startup(args: &[OsString]) -> Result<Startup, &'static str> {
    for arg in args.iter().take_while(|arg| *arg != "--") {
        match arg.to_str() {
            Some("--help" | "-h") => return Ok(Startup::Help),
            Some("--version" | "-V") => return Ok(Startup::Version),
            _ => {}
        }
    }
    if args.is_empty() || (args.len() == 1 && args[0] == "--") {
        Ok(Startup::Serve)
    } else {
        Err("unexpected argument; use --help")
    }
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    match parse_startup(&args) {
        Ok(Startup::Help) => {
            println!(
                "pricing-api {}\nUsage: pricing-api [--help | --version] [--]\n\nConfiguration (environment variables):\n  PRICING_BIND          Listener address (default 127.0.0.1:8081)\n  PRICING_DEPOSIT       Demo account deposit in units (default 1000000)\n  TOLLGATE_LOCAL_SHARDS Positive local shard count (default 1)\n  RUST_LOG              Log filter (default info)\n\nThis example uses the demo credential documented in README.md.",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::SUCCESS
        }
        Ok(Startup::Version) => {
            println!("pricing-api {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Startup::Serve) => match serve() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("pricing-api: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("pricing-api: {error}");
            ExitCode::from(2)
        }
    }
}

#[tokio::main]
async fn serve() -> std::io::Result<()> {
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

    let (router, runtime) = build_app_with_sharding(deposit, true, sharding).await;
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, deposit, local_shards = sharding.get(), "pricing-api listening");
    // `into_make_service_with_connect_info` is what gives each accepted
    // connection its own `PricingConnection`, and therefore its own credential
    // cache. Serving the router directly would silently fall back to
    // per-request HMAC (#2).
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<PricingConnection>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
    });
    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            if let Err(error) = signal {
                runtime.shutdown_server(server, stop).await.map_err(std::io::Error::other)?;
                return Err(error);
            }
        }
        result = &mut server => {
            runtime.shutdown().await;
            return result.map_err(std::io::Error::other)?;
        }
    }
    runtime
        .shutdown_server(server, stop)
        .await
        .map_err(std::io::Error::other)?;
    Ok(())
}
