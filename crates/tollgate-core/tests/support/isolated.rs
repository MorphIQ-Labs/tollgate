//! Process isolation for tests that assert against process-global state.

/// Re-run only the calling test in a fresh process. Returns true in the parent,
/// which must return immediately; the child returns false and runs the body.
pub fn rerun_in_child() -> bool {
    const CHILD: &str = "TOLLGATE_ISOLATED_TEST";
    let thread = std::thread::current();
    let name = thread.name().expect("libtest names its test threads");
    if std::env::var(CHILD).as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, name)
        .output()
        .expect("start isolated test process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "isolated test {name} failed ({}):\n{stdout}\n{stderr}",
        output.status
    );
    true
}
