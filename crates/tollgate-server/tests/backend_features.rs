//! The server binary advertises exactly the backends compiled into it.
//!
//! Running a fresh process is also the non-racy witness that `init_tracing`
//! installs the binary's global subscriber: without that install, each error
//! event asserted below would be discarded. A unit test cannot make the same
//! claim from `tracing::dispatcher::has_been_set()`, because any concurrent
//! thread-scoped `with_default` call permanently flips that process-global
//! history bit.

use std::process::{Command, Output};

fn run_with_backend(backend: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tollgate-server"))
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_STORE", backend)
        .env("RUST_LOG", "error")
        .env_remove("TOLLGATE_PG_URL")
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
