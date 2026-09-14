//! The server binary advertises exactly the backends compiled into it.
//!
//! Running a fresh process is also the non-racy witness that `init_tracing`
//! installs the binary's global subscriber: without that install, each error
//! event asserted below would be discarded. A unit test cannot make the same
//! claim from `tracing::dispatcher::has_been_set()`, because any concurrent
//! thread-scoped `with_default` call permanently flips that process-global
//! history bit.

use std::process::{Command, Output};

// Keep diagnostic assertions independent of the invoking terminal's color policy.
fn server_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tollgate-server"));
    command.env("NO_COLOR", "1");
    command
}

fn run_with_backend(backend: &str) -> Output {
    server_command()
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_STORE", backend)
        .env("RUST_LOG", "error")
        .env_remove("TOLLGATE_PG_URL")
        .env_remove("TOLLGATE_SECURITY_CONFIG")
        .output()
        .expect("tollgate-server binary must start")
}

fn diagnostics(output: &Output) -> String {
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    String::from_utf8(bytes).expect("server diagnostics must be UTF-8")
}

#[test]
fn unknown_backend_lists_only_compiled_backends() {
    let output = run_with_backend("unsupported");
    assert_eq!(output.status.code(), Some(2));

    let expected = if cfg!(feature = "postgres") {
        "memory|postgres"
    } else {
        "memory"
    };
    assert!(
        diagnostics(&output).contains(&format!("unknown TOLLGATE_STORE (expected {expected})")),
        "diagnostic must list only the backends this binary can run"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_backend_is_available_when_compiled() {
    let output = run_with_backend("postgres");
    assert_eq!(output.status.code(), Some(2));

    let diagnostic = diagnostics(&output);
    assert!(
        diagnostic.contains("TOLLGATE_STORE=postgres requires TOLLGATE_PG_URL"),
        "the compiled Postgres arm must validate its required configuration"
    );
    assert!(!diagnostic.contains("unknown TOLLGATE_STORE"));
}

#[cfg(not(feature = "postgres"))]
#[test]
fn postgres_backend_fails_closed_when_not_compiled() {
    let output = run_with_backend("postgres");
    assert_eq!(output.status.code(), Some(2));

    let diagnostic = diagnostics(&output);
    assert!(diagnostic.contains("unknown TOLLGATE_STORE (expected memory)"));
    assert!(
        !diagnostic.contains("TOLLGATE_PG_URL"),
        "an uncompiled backend must not enter Postgres configuration"
    );
}

#[test]
fn help_and_version_precede_configuration_and_respect_the_end_marker() {
    for argument in ["--help", "-h", "--version", "-V"] {
        let output = server_command()
            .args(["unknown", argument])
            .env("TOKIO_WORKER_THREADS", "0")
            .env("TOLLGATE_STORE", "invalid")
            .env_remove("TOLLGATE_SECURITY_CONFIG")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(diagnostics(&output).contains(env!("CARGO_PKG_VERSION")));
    }
    let output = server_command()
        .args(["--", "--help"])
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("unexpected argument"));

    let output = server_command()
        .args(["--version", "--help"])
        .output()
        .unwrap();
    assert_eq!(
        diagnostics(&output),
        format!("tollgate-server {}\n", env!("CARGO_PKG_VERSION"))
    );
    let output = server_command()
        .args(["--"])
        .env("TOLLGATE_STORE", "invalid")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("unknown TOLLGATE_STORE"));
}

#[cfg(unix)]
#[test]
fn server_native_arguments_cannot_panic_or_hide_help_and_version() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    for flag in ["--help", "--version"] {
        let output = server_command()
            .arg(OsString::from_vec(vec![0xff]))
            .arg(flag)
            .env("TOLLGATE_STORE", "invalid")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
    }
    for leading in [vec![], vec!["--"]] {
        let output = server_command()
            .args(leading)
            .arg(OsString::from_vec(vec![0xff]))
            .env("TOLLGATE_STORE", "invalid")
            .env("RUST_LOG", "off")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(diagnostics(&output).contains("unexpected argument"));
        assert!(!diagnostics(&output).contains("panicked"));
    }
}

#[test]
fn credentials_are_required_even_for_loopback_and_invalid_intervals_are_rejected() {
    let output = run_with_backend("memory");
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("TOLLGATE_SECURITY_CONFIG is required"));
    for interval in ["invalid", "0"] {
        let output = server_command()
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", interval)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(diagnostics(&output).contains("TOLLGATE_RECLAIM_INTERVAL_SECS must be"));
    }
}

#[test]
fn normal_log_verbosity_cannot_silence_the_binarys_audit_target() {
    use std::io::BufRead;
    use std::process::Stdio;
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    let mut child = server_command()
        .env("TOLLGATE_STORE", "memory")
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_SECURITY_CONFIG", manifest)
        .env("RUST_LOG", "off,tollgate::audit=off")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        let result = std::io::BufReader::new(stdout).read_line(&mut line);
        sender
            .send((result, line))
            .expect("the receiver outlives the reader");
    });
    let first = received.recv_timeout(std::time::Duration::from_secs(5));
    child
        .kill()
        .expect("the configured server is still running");
    child.wait().unwrap();
    reader.join().unwrap();
    let (result, line) = first.expect("audit must remain enabled with normal logging disabled");
    result.unwrap();
    assert!(
        line.contains("administrative audit events remain enabled"),
        "{line}"
    );
}

#[test]
fn the_binary_refuses_an_exposed_plaintext_listener_before_opening_the_backend() {
    use std::process::Stdio;
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    for backend in ["memory", "postgres"]
        .into_iter()
        .filter(|backend| *backend == "memory" || cfg!(feature = "postgres"))
    {
        let mut child = server_command()
            .env("TOLLGATE_STORE", backend)
            .env("TOLLGATE_PG_URL", "invalid-fixture-url")
            .env("TOLLGATE_BIND", "0.0.0.0:0")
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
            .env("TOLLGATE_SECURITY_CONFIG", &manifest)
            .env("RUST_LOG", "error")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("exposed plaintext listener must refuse startup");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());
        assert!(diagnostics(&output).contains("non-loopback listeners require TLS"));
    }
}

#[cfg(feature = "postgres")]
#[test]
fn backend_startup_failure_never_discloses_connection_strings_or_driver_text() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    // Public fixtures. An invalid port or URL fails before any connection.
    for url in [
        "postgres://user:fixture-startup-sensitive-70@localhost:invalid/db?password=fixture-startup-sensitive-70",
        "host=localhost port=invalid password=fixture-startup-sensitive-70",
        "malformed-fixture-startup-sensitive-70",
    ] {
        let output = server_command()
            .env("TOLLGATE_STORE", "postgres")
            .env("TOLLGATE_PG_URL", url)
            .env("TOLLGATE_BIND", "127.0.0.1:0")
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
            .env("TOLLGATE_SECURITY_CONFIG", &manifest)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let diagnostic = diagnostics(&output);
        assert!(diagnostic.contains("cannot initialize postgres"));
        assert!(diagnostic.contains("operation=\"connect\""));
        assert!(!diagnostic.contains("fixture-startup-sensitive-70"));
    }
}
