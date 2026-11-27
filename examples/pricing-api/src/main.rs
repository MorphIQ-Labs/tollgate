//! pricing-api binary.
//!
//! `PRICING_BIND` (default `127.0.0.1:8081`) and `TOLLGATE_LOCAL_SHARDS`
//! (default `1`). `--help` lists the rest.
//!
//! With `PRICING_CONTROL_URL` set, accounts, credentials and funding come from
//! that `tollgate-server`. Without it the control plane is embedded, funded by
//! `PRICING_DEPOSIT` (default `1_000_000` units), with one demo account, API
//! key `demo-key-1`:
//!
//! ```sh
//! curl -s -H 'Authorization: Bearer demo-key-1' \
//!      -H 'Content-Type: application/json' \
//!      -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
//!      http://127.0.0.1:8081/v1/price
//! ```

use std::{ffi::OsString, num::NonZeroUsize, process::ExitCode};

use jiff::SignedDuration;
use pricing_api::{
    AppRuntime, ControlPlaneTiming, PricingConnection, RemoteConfig, build_app_with_sharding,
    build_remote_app,
};
use tollgate_core::{CostUnits, LocalSharding};

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
                "pricing-api {}\nUsage: pricing-api [--help | --version] [--]\n\nConfiguration (environment variables):\n  PRICING_BIND          Listener address (default 127.0.0.1:8081)\n  TOLLGATE_LOCAL_SHARDS Positive local shard count (default 1)\n  RUST_LOG              Log filter (default info)\n\nEmbedded control plane (PRICING_CONTROL_URL unset):\n  PRICING_DEPOSIT       Demo account deposit in units (default 1000000)\n\nRemote control plane:\n  PRICING_CONTROL_URL            tollgate-server base URL\n  PRICING_CONTROL_TOKEN_FILE     Instance bearer credential (required)\n  PRICING_CREDENTIAL_SECRET_FILE The server's issuer secret (required)\n  PRICING_REFRESH_MS             Snapshot and credential refresh (default 5000)\n  PRICING_LEASE_TTL_SECS         Lease lifetime (default 60)\n  PRICING_LEASE_UNITS            Units acquired per lease (default 10000)\n  PRICING_USAGE_FLUSH_MS         Longest wait for a usage batch (default 25)\n\nThe embedded mode uses the demo credential documented in README.md.",
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
    let deposit: u64 = match std::env::var("PRICING_DEPOSIT") {
        Err(std::env::VarError::NotPresent) => 1_000_000,
        Err(error) => return Err(invalid(format!("cannot read PRICING_DEPOSIT: {error}"))),
        Ok(value) => value
            .parse()
            .map_err(|_| invalid("PRICING_DEPOSIT must be an integer".into()))?,
    };
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

    match remote_config(sharding)? {
        Some(config) => {
            let (router, runtime) = build_remote_app(config)
                .await
                .map_err(std::io::Error::other)?;
            run(&bind, router, runtime).await
        }
        None => {
            tracing::info!(deposit, "embedded demo control plane");
            let (router, runtime) = build_app_with_sharding(deposit, true, sharding).await;
            run(&bind, router, runtime).await
        }
    }
}

/// A positive integer setting, or its default when unset.
fn positive(name: &str, default: u64) -> std::io::Result<u64> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
        Ok(value) => match value.parse() {
            Ok(parsed) if parsed > 0 => Ok(parsed),
            _ => Err(invalid(format!("{name} must be a positive integer"))),
        },
    }
}

fn invalid(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

/// The contents of the file `name` points at, without its trailing newline.
/// The error names the setting, never the contents.
fn secret_file(name: &str) -> std::io::Result<String> {
    let path = std::env::var_os(name)
        .ok_or_else(|| invalid(format!("{name} is required with PRICING_CONTROL_URL")))?;
    let text = std::fs::read_to_string(path)
        .map_err(|error| invalid(format!("cannot read {name}: {error}")))?;
    Ok(text.trim_end().to_owned())
}

fn remote_config(sharding: LocalSharding) -> std::io::Result<Option<RemoteConfig>> {
    let control_url = match std::env::var("PRICING_CONTROL_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => {
            return Err(invalid(format!("cannot read PRICING_CONTROL_URL: {error}")));
        }
    };
    let control_token =
        tollgate_client::BearerToken::new(secret_file("PRICING_CONTROL_TOKEN_FILE")?)
            .map_err(|error| invalid(format!("PRICING_CONTROL_TOKEN_FILE: {error}")))?;
    let defaults = ControlPlaneTiming::REMOTE;
    let millis = |name, default: std::time::Duration| {
        let default = u64::try_from(default.as_millis()).expect("default fits");
        positive(name, default).map(std::time::Duration::from_millis)
    };
    let ttl = positive(
        "PRICING_LEASE_TTL_SECS",
        defaults.lease_ttl.as_secs().unsigned_abs(),
    )?;
    let units = positive("PRICING_LEASE_UNITS", 10_000)?;
    Ok(Some(RemoteConfig {
        control_url,
        control_token,
        credential_secret: secret_file("PRICING_CREDENTIAL_SECRET_FILE")?.into_bytes(),
        timing: ControlPlaneTiming {
            refresh_interval: millis("PRICING_REFRESH_MS", defaults.refresh_interval)?,
            lease_ttl: SignedDuration::from_secs(
                i64::try_from(ttl)
                    .map_err(|_| invalid("PRICING_LEASE_TTL_SECS is too large".into()))?,
            ),
            usage_flush_interval: millis("PRICING_USAGE_FLUSH_MS", defaults.usage_flush_interval)?,
        },
        target_grant: CostUnits(units),
        low_water: low_water(units),
        sharding,
    }))
}

/// Refill once a quarter of a grant remains, so the next one is in hand before
/// this one is spent.
fn low_water(units: u64) -> CostUnits {
    CostUnits(units / 4)
}

/// Serve until SIGINT or SIGTERM, then drain: quiesce HTTP, settle usage and
/// return leases. An orchestrator stops a service with SIGTERM, so ignoring it
/// would lose queued usage on every deployment.
async fn run<S>(bind: &str, router: axum::Router, runtime: AppRuntime<S>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let bind = listener.local_addr()?;
    tracing::info!(%bind, "pricing-api listening");
    #[cfg(unix)]
    let mut termination =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stop_requested = async move {
        #[cfg(unix)]
        return tokio::select! {
            signal = tokio::signal::ctrl_c() => signal,
            _ = termination.recv() => Ok(()),
        };
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await
    };
    // `into_make_service_with_connect_info` is what gives each accepted
    // connection its own `PricingConnection`, and therefore its own credential
    // cache. Serving the router directly would silently fall back to
    // per-request HMAC (GL-2).
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
        signal = stop_requested => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instance_refills_when_a_quarter_of_a_grant_remains() {
        assert_eq!(low_water(10_000), CostUnits(2_500));
        assert_eq!(low_water(1_001), CostUnits(250));
        // A grant too small to divide refills only once it is spent.
        assert_eq!(low_water(3), CostUnits::ZERO);
    }
}
