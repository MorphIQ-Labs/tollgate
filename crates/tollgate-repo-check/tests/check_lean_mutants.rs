//! The Lean mutation gate, end to end through its binary.
//!
//! The binary drives the pinned toolchain: it asks `lake` for the package's
//! `LEAN_PATH`, checks every original module, then checks each mutant with
//! `lean --json`. These tests put scripted `lake` and `lean` first on `PATH`,
//! so every outcome the gate classifies (killed, unviable, survived,
//! allowlisted, stale) and every failure it must refuse to hide is exercised
//! without a Lean installation.
use std::{os::unix::fs::PermissionsExt as _, path::Path, process::Command};

const LEAN_PATH: &str = "/fake/lean/path";

/// Four mutants, one of each fate: `+ -> -` is caught by the theorem (killed),
/// `≤ -> <` fails to elaborate (unviable), and `&& -> ||` and `true -> false`
/// change nothing the scripted checker looks at (survived).
const MODEL: &str = "namespace M

def f (a : Nat) : Nat :=
  a + 1

def g (a : Nat) : Bool :=
  a ≤ 3 && true

theorem t : f 1 = 2 := rfl

end M
";

const SURVIVOR_OR: &str = "M :: a ≤ 3 && true :: && -> || #0";
const SURVIVOR_FALSE: &str = "M :: a ≤ 3 && true :: true -> false #0";

/// A checker standing in for `lean --json FILE`: it refuses a wrong
/// `LEAN_PATH` without a located error, reports the theorem (line 9) failing
/// for the killed mutant, and the definition (line 7) failing for the
/// unviable one.
const FAKE_LEAN: &str = r#"#!/bin/sh
[ "$LEAN_PATH" = "/fake/lean/path" ] || { echo "unexpected LEAN_PATH" >&2; exit 1; }
file="$2"
if grep -q 'BASELINE_BROKEN' "$file"; then
  echo '{"severity":"error","pos":{"line":1,"column":0},"data":"broken"}'; exit 1
fi
if grep -q 'a - 1' "$file"; then
  echo '{"severity":"error","pos":{"line":9,"column":0},"data":"proof failed"}'; exit 1
fi
if grep -q 'a < 3' "$file"; then
  echo '{"severity":"error","pos":{"line":7,"column":0},"data":"type mismatch"}'; exit 1
fi
exit 0
"#;

struct Fixture {
    repo: tempfile::TempDir,
    bin: tempfile::TempDir,
}

fn fixture(model: &str, allowlist: Option<&str>, lake_ok: bool) -> Fixture {
    let repo = tempfile::tempdir().expect("scratch repository");
    let models = repo.path().join("formal/lean/Tollgate");
    std::fs::create_dir_all(&models).expect("models dir");
    std::fs::write(models.join("M.lean"), model).expect("model");
    if let Some(text) = allowlist {
        std::fs::write(repo.path().join("formal/lean/mutants-allowed.txt"), text)
            .expect("allowlist");
    }
    let bin = tempfile::tempdir().expect("fake toolchain");
    let lake = if lake_ok {
        format!("#!/bin/sh\necho {LEAN_PATH}\n")
    } else {
        "#!/bin/sh\necho 'no lakefile' >&2\nexit 1\n".to_owned()
    };
    for (name, script) in [("lake", lake.as_str()), ("lean", FAKE_LEAN)] {
        let path = bin.path().join(name);
        std::fs::write(&path, script).expect("script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    Fixture { repo, bin }
}

struct Outcome {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run(fixture: &Fixture) -> Outcome {
    let path = format!(
        "{}:{}",
        fixture.bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new(env!("CARGO_BIN_EXE_check_lean_mutants"))
        .arg(fixture.repo.path())
        .env("PATH", path)
        .output()
        .expect("run the gate");
    Outcome {
        success: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn allow(keys: &[&str]) -> String {
    keys.iter()
        .map(|key| format!("{key} :: reason: equivalent in this fixture\n"))
        .collect()
}

#[test]
fn every_outcome_is_counted_and_an_unexcused_survivor_fails() {
    let out = run(&fixture(MODEL, None, true));
    assert!(!out.success, "{}", out.stderr);
    assert!(
        out.stdout
            .contains("4 mutants: 1 killed, 1 unviable, 2 survived (0 allowlisted as equivalent)"),
        "{}",
        out.stdout
    );
    assert!(
        out.stderr.contains(&format!("SURVIVED M:7  {SURVIVOR_OR}")),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains(&format!("SURVIVED M:7  {SURVIVOR_FALSE}")),
        "{}",
        out.stderr
    );
}

#[test]
fn survivors_the_allowlist_names_pass() {
    let out = run(&fixture(
        MODEL,
        Some(&allow(&[SURVIVOR_OR, SURVIVOR_FALSE])),
        true,
    ));
    assert!(out.success, "{}", out.stderr);
    assert!(
        out.stdout
            .contains("4 mutants: 1 killed, 1 unviable, 2 survived (2 allowlisted as equivalent)"),
        "{}",
        out.stdout
    );
    assert!(!out.stderr.contains("SURVIVED"), "{}", out.stderr);
}

#[test]
fn an_entry_excuses_only_the_survivor_it_names() {
    let out = run(&fixture(MODEL, Some(&allow(&[SURVIVOR_OR])), true));
    assert!(!out.success);
    assert!(
        out.stdout
            .contains("4 mutants: 1 killed, 1 unviable, 2 survived (1 allowlisted as equivalent)"),
        "{}",
        out.stdout
    );
    assert!(
        out.stderr
            .contains(&format!("SURVIVED M:7  {SURVIVOR_FALSE}")),
        "{}",
        out.stderr
    );
    assert!(
        !out.stderr.contains(&format!("SURVIVED M:7  {SURVIVOR_OR}")),
        "{}",
        out.stderr
    );
}

#[test]
fn an_allowlist_entry_matching_no_survivor_fails_as_stale() {
    let stale = "M :: a + 1 :: + -> - #0";
    let out = run(&fixture(
        MODEL,
        Some(&allow(&[SURVIVOR_OR, SURVIVOR_FALSE, stale])),
        true,
    ));
    assert!(!out.success);
    assert!(
        out.stderr.contains(&format!(
            "stale allowlist entry (no such survivor): {stale}"
        )),
        "{}",
        out.stderr
    );
    assert!(!out.stderr.contains("SURVIVED"), "{}", out.stderr);
}

#[test]
fn a_model_that_fails_unmutated_is_refused_before_any_mutant() {
    let broken = format!("-- BASELINE_BROKEN\n{MODEL}");
    let out = run(&fixture(&broken, None, true));
    assert!(!out.success);
    assert!(
        out.stderr.contains("M.lean does not check unmutated"),
        "{}",
        out.stderr
    );
    assert!(!out.stdout.contains("mutants:"), "{}", out.stdout);
}

#[test]
fn a_toolchain_that_cannot_report_its_path_is_an_error() {
    let out = run(&fixture(MODEL, None, false));
    assert!(!out.success);
    assert!(out.stderr.contains("lake env failed"), "{}", out.stderr);
}

#[test]
fn a_checker_failure_without_a_located_error_is_an_error() {
    let fixture = fixture(MODEL, None, true);
    std::fs::write(
        fixture.bin.path().join("lean"),
        "#!/bin/sh\necho 'crashed' >&2\nexit 3\n",
    )
    .expect("script");
    let out = run(&fixture);
    assert!(!out.success);
    assert!(
        out.stderr.contains("lean failed without a located error"),
        "{}",
        out.stderr
    );
}

#[test]
fn help_is_printed_without_touching_the_toolchain() {
    let out = Command::new(env!("CARGO_BIN_EXE_check_lean_mutants"))
        .arg("--help")
        .env("PATH", Path::new("/nonexistent"))
        .output()
        .expect("run the gate");
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("Usage: check_lean_mutants"));
}
