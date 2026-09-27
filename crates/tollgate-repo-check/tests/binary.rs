//! The exit-code contract `check_backend_parity.sh` depends on.
//!
//! CI reads this binary's status, not its output, so "reports a divergence"
//! and "fails the build" are two claims and only one of them is about text.
//! Nothing invoked the binary before, which the mutation gate caught: the
//! whole of `run` could be replaced with `Ok(true)` — a gate that never fails
//! — and every test still passed (GL-85).
//!
//! `CARGO_BIN_EXE_*` is set by Cargo for integration tests, so this runs the
//! binary that was actually built rather than guessing at a path.
use std::io::Write as _;
use std::process::Command;

/// A throwaway repository holding just the two suites the check reads.
fn repo_with(memory: &str, postgres: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("scratch repository");
    for (path, body) in [
        ("crates/tollgate-store/tests/store_suite.rs", memory),
        (
            "crates/tollgate-store-postgres/tests/postgres_suite.rs",
            postgres,
        ),
    ] {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("has a parent")).expect("create dirs");
        let mut file = std::fs::File::create(full).expect("create suite");
        file.write_all(body.as_bytes()).expect("write suite");
    }
    dir
}

#[test]
fn matching_contracts_exit_zero() {
    let repo = repo_with(
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
    );
    let out = Command::new(env!("CARGO_BIN_EXE_check_backend_parity"))
        .arg(repo.path())
        .output()
        .expect("the binary runs");
    assert!(
        out.status.success(),
        "parity must exit zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("backend parity: OK"));
}

/// The half that matters: a divergence must *fail the build*, not merely be
/// mentioned. A gate that reports and exits zero is not a gate.
#[test]
fn a_divergence_exits_nonzero_and_names_the_test() {
    let repo = repo_with(
        "async fn mirrored() { store.publish_snapshot(p, s).unwrap(); }",
        "async fn mirrored() { AdminStore::publish_snapshot(&*store, p, s).await.unwrap(); }",
    );
    let out = Command::new(env!("CARGO_BIN_EXE_check_backend_parity"))
        .arg(repo.path())
        .output()
        .expect("the binary runs");
    assert!(!out.status.success(), "a divergence must fail the build");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("mirrored"), "it names the test: {stderr}");
    assert!(stderr.contains("publish_snapshot"), "and the operation");
}

/// An unreadable suite is an error, never a silent pass — the failure mode a
/// check like this must not have.
#[test]
fn an_unparsable_suite_exits_nonzero() {
    let repo = repo_with("async fn broken( {", "async fn mirrored() {}");
    let out = Command::new(env!("CARGO_BIN_EXE_check_backend_parity"))
        .arg(repo.path())
        .output()
        .expect("the binary runs");
    assert!(
        !out.status.success(),
        "a parse failure must not read as parity"
    );
}

#[test]
fn help_and_version_exit_zero_without_checking_anything() {
    for flag in ["--help", "--version"] {
        let out = Command::new(env!("CARGO_BIN_EXE_check_backend_parity"))
            .arg(flag)
            .output()
            .expect("the binary runs");
        assert!(out.status.success(), "{flag} exits zero");
        assert!(!String::from_utf8_lossy(&out.stdout).is_empty());
    }
}
