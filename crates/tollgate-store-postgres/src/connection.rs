//! How the store reaches PostgreSQL: what the connection settings guarantee
//! about the path to the database, and what may be said about them in a log.
//!
//! The settings keep the meaning libpq gives them, so this module refuses
//! nothing the driver accepts. It reports, once per connect and before any
//! connection is opened, a non-loopback database whose settings do not both
//! require TLS and authenticate the server (INVARIANTS.md 42).
//!
//! Nothing here logs the connection URL, a user name, a password or the value
//! of any parameter it does not name.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use sqlx::postgres::{PgConnectOptions, PgSslMode};
use tollgate_store::StoreError;
use tracing::field::{Field, Visit};
use tracing::span;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

/// libpq parameter names the driver does not read. Only a name on this list is
/// repeated in a log: an unrecognized "name" can be a fragment of a password
/// that contains an unescaped `&`.
const UNREAD_LIBPQ_PARAMETERS: &[&str] = &[
    "channel_binding",
    "connect_timeout",
    "gssencmode",
    "keepalives",
    "passfile",
    "require_auth",
    "requiressl",
    "ssl_max_protocol_version",
    "ssl_min_protocol_version",
    "sslcertmode",
    "sslcrl",
    "sslcrldir",
    "sslnegotiation",
    "sslpassword",
    "sslsni",
    "target_session_attrs",
    "tcp_user_timeout",
];

/// What the settings leave open on connections to a non-loopback database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exposure {
    /// `disable`, and `allow`, which this driver treats the same.
    Plaintext,
    /// `prefer`: TLS when the server offers it, to any certificate.
    PlaintextPermitted,
    /// `require`: encrypted, to any certificate.
    ServerUnauthenticated,
    /// `verify-ca`: a certificate from a trusted authority, for any name.
    ServerNameUnverified,
}

/// `None` when the server is authenticated by name.
fn exposure(mode: PgSslMode) -> Option<Exposure> {
    match mode {
        PgSslMode::Disable | PgSslMode::Allow => Some(Exposure::Plaintext),
        PgSslMode::Prefer => Some(Exposure::PlaintextPermitted),
        PgSslMode::Require => Some(Exposure::ServerUnauthenticated),
        PgSslMode::VerifyCa => Some(Exposure::ServerNameUnverified),
        PgSslMode::VerifyFull => None,
    }
}

/// The libpq spelling, for logs.
fn spelling(mode: PgSslMode) -> &'static str {
    match mode {
        PgSslMode::Disable => "disable",
        PgSslMode::Allow => "allow",
        PgSslMode::Prefer => "prefer",
        PgSslMode::Require => "require",
        PgSslMode::VerifyCa => "verify-ca",
        PgSslMode::VerifyFull => "verify-full",
    }
}

/// Whether the connection stays on this machine: a Unix-domain socket, a
/// loopback address, or the name `localhost`. `hostaddr`, when set, is the
/// address actually dialled. Names are not resolved: any other name counts as
/// remote.
fn stays_local(socket: bool, host: &str, hostaddr: Option<IpAddr>) -> bool {
    if socket || host.starts_with('/') {
        return true;
    }
    let address = hostaddr.or_else(|| {
        host.trim_start_matches('[')
            .trim_end_matches(']')
            .parse()
            .ok()
    });
    match address {
        Some(IpAddr::V4(address)) => address.is_loopback(),
        Some(IpAddr::V6(address)) => {
            address.is_loopback()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped.is_loopback())
        }
        None => host.eq_ignore_ascii_case("localhost"),
    }
}

/// The environment the driver also reads, for the settings it does not expose.
#[derive(Debug, Default)]
struct Environment {
    ssl_mode: Option<String>,
    root_cert: bool,
    hostaddr: Option<String>,
}

impl Environment {
    fn read() -> Self {
        Environment {
            ssl_mode: std::env::var("PGSSLMODE").ok(),
            root_cert: std::env::var("PGSSLROOTCERT").is_ok(),
            hostaddr: std::env::var("PGHOSTADDR").ok(),
        }
    }
}

/// What the report is made from.
#[derive(Debug, PartialEq, Eq)]
struct Assessment {
    /// `None` for a local connection or an authenticated server.
    exposure: Option<Exposure>,
    /// A root certificate is configured for an exposed connection whose mode
    /// never consults it.
    root_cert_ignored: bool,
    /// `PGSSLMODE` is in effect and is not a mode, so the driver used its
    /// default.
    ssl_mode_variable_ignored: bool,
}

fn assess(url: &url::Url, options: &PgConnectOptions, environment: &Environment) -> Assessment {
    let mut hostaddr = None;
    let mut url_names_mode = false;
    let mut url_names_root_cert = false;
    for (name, value) in url.query_pairs() {
        match &*name {
            "hostaddr" => hostaddr = value.parse().ok(),
            "sslmode" | "ssl-mode" => url_names_mode = true,
            "sslrootcert" | "ssl-root-cert" | "ssl-ca" => url_names_root_cert = true,
            _ => {}
        }
    }
    let hostaddr = hostaddr.or_else(|| {
        environment
            .hostaddr
            .as_deref()
            .and_then(|value| value.parse().ok())
    });
    let mode = options.get_ssl_mode();
    let local = stays_local(options.get_socket().is_some(), options.get_host(), hostaddr);
    let exposure = if local { None } else { exposure(mode) };
    Assessment {
        exposure,
        root_cert_ignored: exposure.is_some()
            && !matches!(mode, PgSslMode::VerifyCa)
            && (url_names_root_cert || environment.root_cert),
        ssl_mode_variable_ignored: !url_names_mode
            && environment
                .ssl_mode
                .as_deref()
                .is_some_and(|value| PgSslMode::from_str(value).is_err()),
    }
}

fn report(assessment: &Assessment, options: &PgConnectOptions) {
    if assessment.ssl_mode_variable_ignored {
        tracing::warn!(
            "PGSSLMODE is not a recognized sslmode and was ignored; the default, prefer, applies"
        );
    }
    let Some(exposure) = assessment.exposure else {
        return;
    };
    let (security, risk) = match exposure {
        Exposure::Plaintext => ("plaintext", "connections are not encrypted"),
        Exposure::PlaintextPermitted => (
            "plaintext_permitted",
            "connections fall back to plaintext when TLS is not offered, and accept any certificate",
        ),
        Exposure::ServerUnauthenticated => (
            "server_unauthenticated",
            "connections are encrypted to any certificate, so the server is not authenticated",
        ),
        Exposure::ServerNameUnverified => (
            "server_name_unverified",
            "any certificate from a trusted authority is accepted for any host name, including \
             certificates from the bundled public authorities",
        ),
    };
    tracing::warn!(
        host = options.get_host(),
        sslmode = spelling(options.get_ssl_mode()),
        security,
        sslrootcert_ignored = assessment.root_cert_ignored,
        "the PostgreSQL database is not on loopback and {risk}; set sslmode=verify-full in the \
         connection URL to require TLS and authenticate the server"
    );
}

/// One warning the driver raised while reading the settings, without the
/// values it attached.
#[derive(Debug, Default)]
struct DriverWarning {
    message: String,
    parameter: Option<&'static str>,
    path: Option<String>,
}

impl Visit for DriverWarning {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // An allow-list: a field this does not name is never copied.
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "path" => self.path = Some(format!("{value:?}")),
            "key" => {
                let key = format!("{value:?}");
                self.parameter = UNREAD_LIBPQ_PARAMETERS
                    .iter()
                    .copied()
                    .find(|known| *known == key);
            }
            _ => {}
        }
    }
}

/// Collects the driver's warnings instead of letting them reach the process
/// subscriber with their values.
#[derive(Clone, Default)]
struct DriverWarnings(Arc<Mutex<Vec<DriverWarning>>>);

impl Subscriber for DriverWarnings {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        // Never cached: this subscriber is installed for one call.
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && *metadata.level() <= Level::WARN
    }

    fn event(&self, event: &Event<'_>) {
        let mut warning = DriverWarning::default();
        event.record(&mut warning);
        if let Ok(mut warnings) = self.0.lock() {
            warnings.push(warning);
        }
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

/// Parse the settings as the driver does, replacing its warnings with ones that
/// carry no values. The driver logs the value of a parameter it does not read,
/// such as `sslpassword`, and a whole malformed password-file line.
///
/// With no `tracing` dispatcher installed the driver's events are left alone:
/// installing even a scoped one would permanently stop `tracing` forwarding
/// every event to a `log` logger.
fn parse(url: &str) -> Result<PgConnectOptions, sqlx::Error> {
    if !tracing::dispatcher::has_been_set() {
        return PgConnectOptions::from_str(url);
    }
    let collected = DriverWarnings::default();
    let options =
        tracing::subscriber::with_default(collected.clone(), || PgConnectOptions::from_str(url));
    let warnings = collected
        .0
        .lock()
        .map(|mut warnings| std::mem::take(&mut *warnings))
        .unwrap_or_default();
    for warning in warnings {
        tracing::warn!(
            driver_message = warning.message,
            parameter = warning.parameter,
            path = warning.path,
            "the PostgreSQL driver warned while reading the connection settings"
        );
    }
    options
}

/// Read the connection settings and report what they leave exposed.
pub(crate) fn options(url: &str) -> Result<PgConnectOptions, StoreError> {
    let options = parse(url).map_err(crate::storage)?;
    // The driver has already parsed this text as a URL.
    let url = url::Url::parse(url)
        .map_err(|error| StoreError(format!("postgres: invalid connection URL: {error}")))?;
    report(&assess(&url, &options, &Environment::read()), &options);
    Ok(options)
}

/// Why TLS could not be established, if that is what failed. The driver reports
/// a server without TLS and an unusable certificate file as its TLS error, and
/// a failed handshake as invalid data from the socket.
fn tls_cause(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Tls(cause) => Some(without_pem(cause.to_string())),
        sqlx::Error::Io(cause) if cause.kind() == std::io::ErrorKind::InvalidData => {
            Some(without_pem(cause.to_string()))
        }
        _ => None,
    }
}

/// The driver quotes an inline certificate it cannot read. Whatever was pasted
/// there stays out of logs.
fn without_pem(text: String) -> String {
    match text.split_once("-----BEGIN") {
        Some((before, _)) => format!("{before}[inline PEM withheld]"),
        None => text,
    }
}

/// The error for a first connection that failed, logged when TLS is the cause
/// so an operator can tell it from an unreachable database or a refused
/// password.
pub(crate) fn connect_error(error: sqlx::Error, options: &PgConnectOptions) -> StoreError {
    let Some(cause) = tls_cause(&error) else {
        return crate::storage(error);
    };
    tracing::error!(
        failure = "tls",
        sslmode = spelling(options.get_ssl_mode()),
        cause,
        "cannot establish TLS to PostgreSQL; check sslmode, sslrootcert and the server certificate"
    );
    StoreError(format!("postgres: TLS: {cause}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assessed(url: &str, environment: &Environment) -> Assessment {
        let options = PgConnectOptions::from_str(url).unwrap();
        assess(&url::Url::parse(url).unwrap(), &options, environment)
    }

    #[test]
    fn only_an_authenticated_server_name_leaves_nothing_exposed() {
        for (mode, expected) in [
            ("disable", Some(Exposure::Plaintext)),
            ("allow", Some(Exposure::Plaintext)),
            ("prefer", Some(Exposure::PlaintextPermitted)),
            ("require", Some(Exposure::ServerUnauthenticated)),
            ("verify-ca", Some(Exposure::ServerNameUnverified)),
            ("verify-full", None),
        ] {
            let mode_value = PgSslMode::from_str(mode).unwrap();
            assert_eq!(exposure(mode_value), expected, "{mode}");
            assert_eq!(spelling(mode_value), mode);
        }
    }

    #[test]
    fn sockets_loopback_addresses_and_localhost_are_the_only_local_paths() {
        let address = |text: &str| Some(text.parse().unwrap());
        for (socket, host, hostaddr) in [
            (true, "db.example", address("192.0.2.1")),
            (false, "/var/run/postgresql", None),
            (false, "localhost", None),
            (false, "LocalHost", None),
            (false, "127.0.0.1", None),
            (false, "127.8.9.10", None),
            (false, "[::1]", None),
            (false, "::1", None),
            (false, "[::ffff:127.0.0.1]", None),
            (false, "db.example", address("127.0.0.1")),
            (false, "db.example", address("::1")),
        ] {
            assert!(stays_local(socket, host, hostaddr), "{host} {hostaddr:?}");
        }
        for (host, hostaddr) in [
            ("db.example", None),
            ("localhost.example", None),
            ("db.localhost", None),
            ("10.0.0.5", None),
            ("0.0.0.0", None),
            ("[2001:db8::1]", None),
            ("[::ffff:192.0.2.1]", None),
            ("localhost", address("192.0.2.1")),
            ("127.0.0.1", address("2001:db8::1")),
        ] {
            assert!(!stays_local(false, host, hostaddr), "{host} {hostaddr:?}");
        }
    }

    #[test]
    fn a_hostaddr_from_the_url_overrides_the_environment() {
        let environment = Environment {
            hostaddr: Some("127.0.0.1".into()),
            ..Environment::default()
        };
        let url = "postgres://db.example/db?sslmode=disable";
        assert_eq!(assessed(url, &environment).exposure, None);
        assert_eq!(
            assessed(url, &Environment::default()).exposure,
            Some(Exposure::Plaintext)
        );
        let url = format!("{url}&hostaddr=192.0.2.1");
        assert_eq!(
            assessed(&url, &environment).exposure,
            Some(Exposure::Plaintext)
        );
    }

    #[test]
    fn a_root_certificate_the_mode_ignores_is_reported() {
        let environment = Environment {
            root_cert: true,
            ..Environment::default()
        };
        for mode in ["disable", "allow", "prefer", "require"] {
            let plain = format!("postgres://db.example/db?sslmode={mode}");
            assert!(!assessed(&plain, &Environment::default()).root_cert_ignored);
            assert!(assessed(&plain, &environment).root_cert_ignored, "{mode}");
            for name in ["sslrootcert", "ssl-root-cert", "ssl-ca"] {
                let url = format!("{plain}&{name}=/ca.pem");
                assert!(
                    assessed(&url, &Environment::default()).root_cert_ignored,
                    "{url}"
                );
            }
        }
        for url in [
            "postgres://db.example/db?sslmode=verify-ca",
            "postgres://db.example/db?sslmode=verify-full",
            "postgres://127.0.0.1/db?sslmode=require",
        ] {
            assert!(!assessed(url, &environment).root_cert_ignored, "{url}");
        }
    }

    #[test]
    fn an_unrecognized_mode_variable_matters_only_while_it_is_in_effect() {
        let environment = |value: &str| Environment {
            ssl_mode: Some(value.into()),
            ..Environment::default()
        };
        let bare = "postgres://db.example/db";
        assert!(assessed(bare, &environment("verify_full")).ssl_mode_variable_ignored);
        assert!(!assessed(bare, &environment("VERIFY-FULL")).ssl_mode_variable_ignored);
        assert!(!assessed(bare, &Environment::default()).ssl_mode_variable_ignored);
        for name in ["sslmode", "ssl-mode"] {
            let url = format!("{bare}?{name}=require");
            assert!(!assessed(&url, &environment("verify_full")).ssl_mode_variable_ignored);
        }
    }

    #[test]
    fn only_a_tls_failure_has_a_tls_cause() {
        let io = |kind| sqlx::Error::Io(std::io::Error::new(kind, "fixture cause"));
        assert_eq!(
            tls_cause(&sqlx::Error::Tls("fixture cause".into())).as_deref(),
            Some("fixture cause")
        );
        assert_eq!(
            tls_cause(&io(std::io::ErrorKind::InvalidData)).as_deref(),
            Some("fixture cause")
        );
        assert_eq!(tls_cause(&io(std::io::ErrorKind::ConnectionRefused)), None);
        assert_eq!(tls_cause(&sqlx::Error::PoolTimedOut), None);
    }

    #[test]
    fn an_inline_certificate_is_never_quoted() {
        let text = "Invalid certificate -----BEGIN PRIVATE KEY-----\nfixture".to_owned();
        assert_eq!(
            without_pem(text),
            "Invalid certificate [inline PEM withheld]"
        );
        assert_eq!(without_pem("UnknownIssuer".into()), "UnknownIssuer");
    }
}
