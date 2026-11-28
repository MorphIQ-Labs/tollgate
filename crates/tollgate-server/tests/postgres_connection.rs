//! What the stock binary says about its path to PostgreSQL (INVARIANTS.md 42):
//! a warning for an exposed one, a cause for a failed TLS negotiation, and no
//! secret in either. No database is needed; every connection here fails.

#![cfg(feature = "postgres")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

/// A public fixture that must never reach a log.
const SECRET: &str = "fixture-connection-sensitive-100";
const EXPOSED: &str = "the PostgreSQL database is not on loopback";
const DRIVER: &str = "the PostgreSQL driver warned";

/// Start the binary on `url`, wait for it to give up, and return what it
/// logged. The environment holds only what is given, so the caller's `PG*`
/// variables and password file cannot reach it.
fn startup(url: &str, environment: &[(&str, &str)]) -> String {
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_tollgate-server"));
    command
        .env_clear()
        .env("NO_COLOR", "1")
        .env("HOME", directory.path())
        .env("TOLLGATE_STORE", "postgres")
        .env("TOLLGATE_PG_URL", url)
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_SECURITY_CONFIG", &manifest)
        .env("RUST_LOG", "info");
    command.envs(environment.iter().copied());
    let output = command.output().expect("tollgate-server binary must start");
    assert_eq!(output.status.code(), Some(2), "no connection can succeed");
    let mut bytes = output.stdout;
    bytes.extend_from_slice(&output.stderr);
    let log = String::from_utf8(bytes).expect("server diagnostics must be UTF-8");
    assert!(log.contains("cannot initialize postgres"), "{log}");
    assert!(!log.contains(SECRET), "a secret reached the log");
    log
}

/// A name that never resolves, so the connection fails without leaving the
/// machine.
fn remote(parameters: &str) -> String {
    format!("postgres://tollgate:{SECRET}@database.invalid:5432/tollgate?{parameters}")
}

#[test]
fn an_exposed_database_is_reported_with_what_its_mode_leaves_open() {
    for (parameters, mode, security) in [
        ("application_name=tollgate", "prefer", "plaintext_permitted"),
        ("sslmode=disable", "disable", "plaintext"),
        ("sslmode=allow", "allow", "plaintext"),
        ("sslmode=require", "require", "server_unauthenticated"),
        ("sslmode=verify-ca", "verify-ca", "server_name_unverified"),
    ] {
        let log = startup(&remote(parameters), &[]);
        let warnings: Vec<_> = log.lines().filter(|line| line.contains(EXPOSED)).collect();
        let [warning] = warnings[..] else {
            panic!("{parameters}: one warning per connect, found {warnings:?}");
        };
        for field in [
            " WARN ".to_owned(),
            "host=\"database.invalid\"".to_owned(),
            format!("sslmode=\"{mode}\""),
            format!("security=\"{security}\""),
            "sslrootcert_ignored=false".to_owned(),
            "set sslmode=verify-full".to_owned(),
        ] {
            assert!(warning.contains(&field), "{field}: {warning}");
        }
        assert!(!log.contains(DRIVER), "{log}");
    }
}

#[test]
fn requiring_an_authenticated_server_or_staying_on_loopback_is_not_reported() {
    let urls = [
        remote("sslmode=verify-full"),
        // Nothing listens on port 1.
        format!("postgres://tollgate:{SECRET}@127.0.0.1:1/tollgate"),
        format!("postgres://tollgate:{SECRET}@localhost:1/tollgate?sslmode=disable"),
        remote("hostaddr=127.0.0.1&port=1"),
        // With no password the driver looks for a password file, and says so
        // only below the warning level.
        "postgres://tollgate@127.0.0.1:1/tollgate".to_owned(),
    ];
    // Together: a refused connection is retried until the pool gives up.
    std::thread::scope(|scope| {
        for url in &urls {
            scope.spawn(move || {
                let log = startup(url, &[]);
                assert!(!log.contains(EXPOSED), "{log}");
                assert!(!log.contains(DRIVER), "{log}");
            });
        }
    });
    let log = startup(&remote(""), &[("PGSSLMODE", "verify-full")]);
    assert!(!log.contains(EXPOSED), "{log}");
}

#[test]
fn the_environment_the_driver_reads_is_reported_too() {
    let log = startup(&remote(""), &[("PGSSLMODE", "verify_full")]);
    assert!(
        log.contains("PGSSLMODE is not a recognized sslmode"),
        "{log}"
    );
    assert!(log.contains("security=\"plaintext_permitted\""), "{log}");

    let log = startup(&remote(""), &[("PGSSLROOTCERT", "/nonexistent/ca.pem")]);
    assert!(log.contains("sslrootcert_ignored=true"), "{log}");
    assert!(!log.contains("PGSSLMODE"), "{log}");

    let log = startup(&remote("port=1"), &[("PGHOSTADDR", "127.0.0.1")]);
    assert!(!log.contains(EXPOSED), "{log}");
}

/// The driver would log the value of a parameter it does not read.
#[test]
fn an_unread_parameter_is_reported_without_its_value() {
    let log = startup(
        &remote(&format!("sslmode=verify-full&sslpassword={SECRET}")),
        &[],
    );
    let warning = log
        .lines()
        .find(|line| line.contains(DRIVER))
        .unwrap_or_else(|| panic!("the unread parameter is reported: {log}"));
    assert!(
        warning.contains("ignoring unrecognized connect parameter"),
        "{warning}"
    );
    assert!(warning.contains("parameter=\"sslpassword\""), "{warning}");

    // A name libpq does not have may itself be part of a password.
    let log = startup(
        &remote(&format!("sslmode=verify-full&password=a&{SECRET}=1")),
        &[],
    );
    assert!(log.contains(DRIVER), "{log}");
    assert!(!log.contains("parameter="), "{log}");
}

/// The driver would log a malformed password-file line, password and all.
#[test]
fn a_malformed_password_file_line_is_reported_without_the_line() {
    let directory = tempfile::tempdir().unwrap();
    let passfile = directory.path().join("pgpass");
    std::fs::write(&passfile, format!("database.invalid:{SECRET}\n")).unwrap();
    // The driver ignores a password file others can read.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&passfile, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let log = startup(
        "postgres://tollgate@database.invalid:5432/tollgate?sslmode=verify-full",
        &[("PGPASSFILE", passfile.to_str().unwrap())],
    );
    let warning = log
        .lines()
        .find(|line| line.contains(DRIVER))
        .unwrap_or_else(|| panic!("the malformed line is reported: {log}"));
    assert!(
        warning.contains("Malformed line in pgpass file"),
        "{warning}"
    );
}

#[test]
fn an_unopenable_password_file_is_reported_with_its_path() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    std::fs::write(&file, "").unwrap();
    // A path through a file cannot be opened, whoever asks.
    let passfile = file.join("pgpass");
    let passfile = passfile.to_str().unwrap();
    let log = startup(
        "postgres://tollgate@database.invalid:5432/tollgate?sslmode=verify-full",
        &[("PGPASSFILE", passfile)],
    );
    let warning = log
        .lines()
        .find(|line| line.contains(DRIVER))
        .unwrap_or_else(|| panic!("the unopenable file is reported: {log}"));
    assert!(
        warning.contains("Failed to open `.pgpass` file"),
        "{warning}"
    );
    assert!(
        warning.contains(&format!("path=\"{passfile}\"")),
        "{warning}"
    );
}

/// Answer the driver's request for TLS with `reply`, once per connection.
fn fake_server(reply: &'static [u8]) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // Ends with the test process.
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut request = [0; 8];
            if stream.read_exact(&mut request).is_err() || stream.write_all(reply).is_err() {
                continue;
            }
            // Hold the connection until the client has judged the reply; a
            // client that hangs up first is the expected end.
            let mut rest = Vec::new();
            drop(stream.read_to_end(&mut rest));
        }
    });
    address
}

#[test]
fn a_failed_tls_negotiation_is_logged_with_its_cause() {
    for (reply, cause) in [
        (&b"N"[..], "server does not support TLS"),
        // Accepts, then sends something that is not a TLS record.
        (&b"SHTTP/1.1 400 Bad Request\r\n\r\n"[..], "corrupt message"),
    ] {
        let address = fake_server(reply);
        let log = startup(
            &format!("postgres://tollgate:{SECRET}@{address}/tollgate?sslmode=require"),
            &[],
        );
        let failure = log
            .lines()
            .find(|line| line.contains("cannot establish TLS to PostgreSQL"))
            .unwrap_or_else(|| panic!("{cause}: {log}"));
        for field in ["failure=\"tls\"", "sslmode=\"require\"", cause] {
            assert!(failure.contains(field), "{field}: {failure}");
        }
    }
}

#[test]
fn an_unreachable_database_is_not_called_a_tls_failure() {
    let log = startup(
        &format!("postgres://tollgate:{SECRET}@127.0.0.1:1/tollgate?sslmode=verify-full"),
        &[],
    );
    assert!(!log.contains("cannot establish TLS"), "{log}");
}
