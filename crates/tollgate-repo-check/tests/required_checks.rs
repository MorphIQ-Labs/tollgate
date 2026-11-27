//! Every CI job is a required check, and the check that says so bites (GH-88).
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use tollgate_repo_check::required_checks::{REQUIRED, WORKFLOW, check, disagreements, drift};

const GOOD: &str = "\
name: ci
on:
  pull_request:
jobs:
  fmt:
    name: check / fmt
    if: github.event_name == 'pull_request'
    steps:
      - run: |
          cargo fmt --check
  # A comment at any depth is not a job.
  test:
    name: test / test
    needs: [fmt]
    steps:
      - run: cargo test
  shard:
    name: assurance / shard ${{ matrix.n }}
    needs: [test]
    strategy:
      fail-fast: false
      matrix:
        n: [0, 1]
  summary:
    name: assurance / summary
    needs: [shard]
    if: always() && github.event_name == 'pull_request'
";
const LIST: &str = "# comment\n\ncheck / fmt\ntest / test\nassurance / summary\n";

fn problems(workflow: &str, list: &str) -> Vec<String> {
    disagreements(workflow, list).expect("a readable workflow")
}

#[test]
fn the_repository_declares_every_job_required() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(check(&root), Ok(Vec::new()));
}

#[test]
fn a_matrix_is_required_through_its_aggregate() {
    assert_eq!(problems(GOOD, LIST), Vec::<String>::new());
}

/// The defect this exists for: a job others depend on, left unrequired.
#[test]
fn an_unlisted_job_is_reported() {
    let found = problems(GOOD, &LIST.replace("check / fmt\n", ""));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("`check / fmt`"), "{found:?}");
}

#[test]
fn stale_and_repeated_entries_are_reported() {
    let found = problems(GOOD, &format!("{LIST}check / gone\ntest / test\n"));
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found.iter().any(|f| f.contains("`check / gone`")));
    assert!(found.iter().any(|f| f.contains("listed twice")));
}

#[test]
fn listing_a_matrix_name_is_not_a_way_to_require_it() {
    let found = problems(
        GOOD,
        &format!("{LIST}assurance / shard ${{{{ matrix.n }}}}\n"),
    );
    assert_eq!(found.len(), 1, "{found:?}");
}

#[test]
fn a_matrix_without_an_aggregate_is_reported() {
    for broken in [
        GOOD.replace("if: always() && ", "if: "),
        GOOD.replace("needs: [shard]", "needs: [shard, test]"),
        GOOD.replace("needs: [shard]", "needs: [test]"),
    ] {
        let found = problems(&broken, LIST);
        assert!(
            found.iter().any(|f| f.contains("matrix job `shard`")),
            "{found:?}"
        );
    }
}

#[test]
fn unnamed_duplicated_and_unknown_jobs_are_reported() {
    let unnamed = GOOD.replace("    name: test / test\n", "");
    assert!(
        problems(&unnamed, LIST)
            .iter()
            .any(|f| f.contains("no name"))
    );
    let twice = GOOD.replace("name: test / test", "name: check / fmt");
    assert!(
        problems(&twice, LIST)
            .iter()
            .any(|f| f.contains("two jobs are named"))
    );
    let unknown = GOOD.replace("needs: [fmt]", "needs: [fnt]");
    assert!(
        problems(&unknown, LIST)
            .iter()
            .any(|f| f.contains("`fnt`, which is not a job"))
    );
}

/// A shape the reader does not understand must not read as fewer jobs.
#[test]
fn an_unfamiliar_workflow_shape_is_an_error() {
    for (from, to, expected) in [
        ("name: test / test", "name: \"test / test\"", "unquoted"),
        ("name: test / test", "name:", "unquoted"),
        ("needs: [fmt]", "needs: fmt", "inline list"),
        ("  test:\n", "  test: {}\n", "job id"),
        ("  test:\n", "  fmt:\n", "declared twice"),
        ("    needs: [fmt]", "   needs: [fmt]", "four spaces"),
        ("    needs: [fmt]", "    - needs", "key: value"),
        ("  fmt:\n", "", "two spaces"),
    ] {
        let error = disagreements(&GOOD.replace(from, to), LIST).unwrap_err();
        assert!(error.contains(expected), "{to:?}: {error}");
    }
    assert!(
        disagreements("name: ci\n", LIST)
            .unwrap_err()
            .contains("no jobs")
    );
}

#[test]
fn a_job_id_is_a_single_plain_word() {
    for id in ["te st", "te.st", "te/st", ""] {
        let workflow = GOOD.replace("  test:\n", &format!("  {id}:\n"));
        let error = disagreements(&workflow, LIST).unwrap_err();
        assert!(error.contains("job id"), "{id:?}: {error}");
    }
    let renamed = GOOD
        .replace("  test:\n", "  te-st_1:\n")
        .replace("needs: [test]", "needs: [te-st_1]");
    assert_eq!(problems(&renamed, LIST), Vec::<String>::new());
}

/// Only a `strategy` key's own `matrix` fans a job out. Read any wider, an
/// ordinary job would count as a matrix and stop being required by its name.
#[test]
fn a_matrix_elsewhere_does_not_make_a_job_a_matrix() {
    for addition in [
        "    with:\n      matrix: an input\n",
        "    steps:\n      - run: |\n          matrix: text in a script\n",
        "    strategy:\n      fail-fast: false\n",
        "    strategy:\n      fail-fast: false\n    env:\n      matrix: not-strategy\n",
        "    strategy:\n      options:\n        matrix: nested\n",
    ] {
        let workflow = GOOD.replace("  test:\n", &format!("  test:\n{addition}"));
        assert_eq!(
            problems(&workflow, LIST),
            Vec::<String>::new(),
            "{addition:?}"
        );
    }
}

#[test]
fn keys_outside_jobs_are_not_jobs() {
    let trailing = format!("{GOOD}env:\n  NOT_A_JOB: 1\n");
    assert_eq!(problems(&trailing, LIST), Vec::<String>::new());
}

fn protection(strict: bool, names: &[&str]) -> String {
    serde_json::json!({
        "strict": strict,
        "checks": names.iter().map(|n| serde_json::json!({"context": n})).collect::<Vec<_>>(),
    })
    .to_string()
}

const NAMES: [&str; 3] = ["check / fmt", "test / test", "assurance / summary"];

#[test]
fn the_audit_compares_both_directions_and_strictness() {
    assert_eq!(drift(&protection(true, &NAMES), LIST), Ok(Vec::new()));
    let found = drift(
        &protection(false, &["check / fmt", "test / test", "old"]),
        LIST,
    )
    .unwrap();
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(found[0].contains("`strict` is false"));
    assert!(found[1].contains("`assurance / summary` is listed but not required"));
    assert!(found[2].contains("`old` is required"));
}

#[test]
fn an_unreadable_response_never_passes_the_audit() {
    for response in [
        "",
        "not json",
        r#"{"message":"Not Found"}"#,
        r#"{"strict":true}"#,
        r#"{"strict":true,"checks":[{}]}"#,
    ] {
        let error = drift(response, LIST).unwrap_err();
        assert!(error.contains("was not read"), "{response:?}: {error}");
    }
}

fn repository(list: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("scratch repository");
    for (path, body) in [(WORKFLOW, GOOD), (REQUIRED, list)] {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("has a parent")).expect("dirs");
        std::fs::write(full, body).expect("write");
    }
    dir
}

fn run(binary: &str, args: &[&std::ffi::OsStr], input: &str) -> Output {
    let mut child = Command::new(binary)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the binary runs");
    // A binary that answers without reading closes the pipe early.
    drop(
        child
            .stdin
            .take()
            .expect("stdin is piped")
            .write_all(input.as_bytes()),
    );
    child.wait_with_output().expect("the binary exits")
}

/// CI reads the status, so a disagreement has to fail the build.
#[test]
fn both_binaries_fail_on_a_disagreement_and_pass_without_one() {
    let check = env!("CARGO_BIN_EXE_check_required_checks");
    let audit = env!("CARGO_BIN_EXE_audit_required_checks");
    let good = repository(LIST);
    let bad = repository("check / fmt\n");
    let passed = run(check, &[good.path().as_os_str()], "");
    assert!(passed.status.success(), "{passed:?}");
    assert!(String::from_utf8_lossy(&passed.stdout).contains("required checks: OK"));
    let failed = run(check, &[bad.path().as_os_str()], "");
    assert_eq!(failed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("test / test"));

    let response = protection(true, &NAMES);
    let passed = run(audit, &[good.path().as_os_str()], &response);
    assert!(passed.status.success(), "{passed:?}");
    assert!(String::from_utf8_lossy(&passed.stdout).contains("audit: OK"));
    let failed = run(audit, &[bad.path().as_os_str()], &response);
    assert_eq!(failed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("not listed"));
    assert_eq!(
        run(audit, &[good.path().as_os_str()], "").status.code(),
        Some(1)
    );
}

#[test]
fn both_binaries_answer_help_and_refuse_what_they_cannot_read() {
    for binary in [
        env!("CARGO_BIN_EXE_check_required_checks"),
        env!("CARGO_BIN_EXE_audit_required_checks"),
    ] {
        let help = run(binary, &["--help".as_ref()], "");
        assert!(help.status.success());
        assert!(String::from_utf8_lossy(&help.stdout).contains("Usage:"));
        assert_eq!(
            run(binary, &["--bogus".as_ref()], "").status.code(),
            Some(1)
        );
        let empty = tempfile::tempdir().expect("scratch directory");
        let missing = run(binary, &[empty.path().as_os_str()], "");
        assert_eq!(missing.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&missing.stderr).contains("cannot read"));
    }
}
