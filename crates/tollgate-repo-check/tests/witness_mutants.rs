//! The witness-mutant gate, end to end.
//!
//! The runner drives cargo in a copy of the repository. These tests give it a
//! scripted cargo that reads the copied source, so every outcome the gate must
//! tell apart (a witness that bites, one that survives, one that never ran,
//! crashed or hung, a mutant that does not compile, a failing baseline, a
//! pattern that drifted) is exercised without compiling anything.

use std::{
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{Duration, Instant},
};

use tollgate_repo_check::witness_mutants::{
    Mutant, Options, Target, Verdict, Witness, drift, run, validate,
};

const SOURCE: &str = "alpha = on\nbeta = on\n";

/// Stands in for cargo, run in the copied tree. The "tests" are decided by the
/// copied `src.txt`: a witness named `bites_<line>` fails once `<line>` is off,
/// and the other names produce the outcome they spell.
const FAKE_CARGO: &str = r#"#!/bin/sh
printf '%s | %s %s %s\n' "$*" "$CARGO_TARGET_DIR" "$TOLLGATE_REQUIRE_PG" "$(tr '\n' ' ' < src.txt)" >> "$RECORD"
summary() { echo "test result: $1. $2 passed; $3 failed; 0 ignored; 0 measured; 9 filtered out"; }
for argument in "$@"; do
  if [ "$argument" = "--no-run" ]; then
    if grep -q 'UNVIABLE' src.txt; then echo 'error: could not compile'; exit 101; fi
    exit 0
  fi
  name=$argument
done
case $name in
  bites_*)
    if grep -q "${name#bites_} = on" src.txt; then summary ok 1 0; else summary FAILED 0 1; exit 101; fi ;;
  survives) summary ok 1 0 ;;
  broken_unmodified) summary FAILED 0 1; exit 101 ;;
  absent) summary ok 0 0 ;;
  crashes)
    if grep -q ' = off' src.txt; then echo 'process abort'; exit 134; fi
    summary ok 1 0 ;;
  hangs)
    if grep -q ' = off' src.txt; then sleep 600; fi
    summary ok 1 0 ;;
esac
"#;

const fn witness(test: &'static str) -> Witness {
    Witness {
        package: "demo",
        target: Target::Test("suite"),
        test,
    }
}

const fn mutant(
    id: &'static str,
    find: &'static str,
    replace: &'static str,
    witnesses: &'static [Witness],
) -> Mutant {
    Mutant {
        id,
        disables: "a fixture mechanism",
        file: "src.txt",
        find,
        replace,
        witnesses,
    }
}

const ALPHA: Mutant = mutant(
    "alpha",
    "alpha = on",
    "alpha = off",
    &[witness("bites_alpha")],
);
const BETA: Mutant = mutant(
    "beta",
    "beta = on",
    "beta = off",
    &[Witness {
        package: "demo",
        target: Target::Lib,
        test: "bites_beta",
    }],
);

struct Fixture {
    directory: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("scratch directory");
        let repository = directory.path().join("repository");
        std::fs::create_dir_all(repository.join("nested")).expect("repository");
        std::fs::write(repository.join("src.txt"), SOURCE).expect("source");
        std::fs::write(repository.join("nested/untracked.txt"), "new").expect("untracked");
        std::fs::write(repository.join(".gitignore"), "/ignored\n").expect("gitignore");
        std::fs::write(repository.join("ignored"), "secret").expect("ignored");
        std::fs::write(repository.join("deleted.txt"), "gone").expect("deleted");
        for arguments in [&["init", "--quiet"][..], &["add", "src.txt", "deleted.txt"]] {
            let status = Command::new("git")
                .args(arguments)
                .current_dir(&repository)
                .status()
                .expect("git");
            assert!(status.success(), "git {arguments:?}");
        }
        std::fs::remove_file(repository.join("deleted.txt")).expect("delete");
        let cargo = directory.path().join("cargo");
        let script = FAKE_CARGO.replace(
            "$RECORD",
            &directory.path().join("record").display().to_string(),
        );
        std::fs::write(&cargo, script).expect("script");
        std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        Fixture { directory }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn options(&self) -> Options {
        Options {
            root: self.path("repository"),
            work: self.path("work"),
            logs: self.path("logs"),
            cargo: self.path("cargo").into(),
            witness_timeout: Duration::from_secs(30),
        }
    }

    fn run(&self, catalog: &[Mutant]) -> (Vec<String>, Vec<String>) {
        let mut reported = Vec::new();
        let failures = run(&self.options(), catalog, &mut |line| {
            reported.push(line.to_owned())
        })
        .expect("the run is carried out");
        (failures, reported)
    }

    /// One line per cargo invocation: arguments, then what it saw.
    fn record(&self) -> Vec<String> {
        std::fs::read_to_string(self.path("record"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

#[test]
fn witnesses_that_fail_under_their_mutant_pass_the_gate() {
    let fixture = Fixture::new();
    let (failures, reported) = fixture.run(&[ALPHA, BETA]);
    assert_eq!(failures, Vec::<String>::new());
    assert_eq!(
        reported,
        [
            "baseline: 2 witnesses, unmodified",
            "alpha: witnessed by 1",
            "beta: witnessed by 1",
        ]
    );
}

#[test]
fn each_mutant_is_applied_alone_in_a_copy_and_the_repository_is_untouched() {
    let fixture = Fixture::new();
    // Left from an earlier run: both are replaced.
    std::fs::create_dir_all(fixture.path("work/tree")).unwrap();
    std::fs::write(fixture.path("work/tree/stale.txt"), "stale").unwrap();
    std::fs::create_dir_all(fixture.path("logs")).unwrap();
    std::fs::write(fixture.path("logs/stale.log"), "stale").unwrap();
    let (failures, _) = fixture.run(&[ALPHA, BETA]);
    assert_eq!(failures, Vec::<String>::new());

    let build = fixture.path("work/build").display().to_string();
    let seen = |arguments: &str, source: &str| format!("{arguments} | {build} 1 {source} ");
    let suite = "test --locked --all-features -p demo --test suite";
    let lib = "test --locked --all-features -p demo --lib";
    let whole = "alpha = on beta = on";
    let no_alpha = "alpha = off beta = on";
    let no_beta = "alpha = on beta = off";
    assert_eq!(
        fixture.record(),
        [
            seen(&format!("{lib} --no-run"), whole),
            seen(&format!("{suite} --no-run"), whole),
            seen(&format!("{lib} -- --exact bites_beta"), whole),
            seen(&format!("{suite} -- --exact bites_alpha"), whole),
            seen(&format!("{suite} --no-run"), no_alpha),
            seen(&format!("{suite} -- --exact bites_alpha"), no_alpha),
            seen(&format!("{lib} --no-run"), no_beta),
            seen(&format!("{lib} -- --exact bites_beta"), no_beta),
        ]
    );
    let read = |name: &str| std::fs::read_to_string(fixture.path(name));
    assert_eq!(read("repository/src.txt").unwrap(), SOURCE);
    assert_eq!(read("work/tree/src.txt").unwrap(), SOURCE, "restored");
    assert_eq!(read("work/tree/nested/untracked.txt").unwrap(), "new");
    for absent in ["ignored", "deleted.txt", "stale.txt"] {
        assert!(!fixture.path("work/tree").join(absent).exists(), "{absent}");
    }
    assert!(!fixture.path("logs/stale.log").exists());
    assert_eq!(std::fs::read_dir(fixture.path("logs")).unwrap().count(), 8);
}

/// One mutant, one witness, expected to be refused with `outcome`.
fn refused(witnesses: &'static [Witness], timeout: Duration, outcome: &str) {
    let fixture = Fixture::new();
    let options = Options {
        witness_timeout: timeout,
        ..fixture.options()
    };
    let gate = mutant("gate", "alpha = on", "alpha = off", witnesses);
    let failures = run(&options, &[gate, BETA], &mut |_| {}).expect("carried out");
    assert_eq!(failures.len(), 1, "{failures:?}");
    let expected = format!(
        "gate: `demo --test suite {}` {outcome}, and must have failed; see ",
        witnesses.last().unwrap().test
    );
    assert!(failures[0].starts_with(&expected), "{failures:?}");
    let log = Path::new(failures[0].strip_prefix(&expected).unwrap());
    assert!(log.is_file(), "{failures:?}");
    // The failure does not stop the mutants after it.
    assert!(
        fixture.record().last().unwrap().contains("bites_beta"),
        "{:?}",
        fixture.record()
    );
}

#[test]
fn every_listed_witness_must_fail() {
    refused(
        const { &[witness("bites_alpha"), witness("survives")] },
        Duration::from_secs(30),
        "passed",
    );
}

#[test]
fn a_witness_that_crashes_is_no_evidence() {
    refused(
        const { &[witness("crashes")] },
        Duration::from_secs(30),
        "gave no verdict",
    );
}

#[test]
fn a_witness_that_hangs_is_killed_and_refused() {
    let started = Instant::now();
    refused(
        const { &[witness("hangs")] },
        Duration::from_secs(2),
        "timed out",
    );
    // The fixture sleeps for ten minutes; its process group was killed.
    assert!(started.elapsed() < Duration::from_secs(120));
}

#[test]
fn a_mutant_that_does_not_compile_is_refused_and_restored() {
    let fixture = Fixture::new();
    let unviable = mutant(
        "unviable",
        "alpha = on",
        "UNVIABLE",
        const { &[witness("bites_alpha")] },
    );
    let (failures, reported) = fixture.run(&[unviable, BETA]);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0].starts_with("unviable: the mutant is unviable: demo does not build; see "),
        "{failures:?}"
    );
    assert!(reported.contains(&failures[0]), "{reported:?}");
    assert!(reported.contains(&"beta: witnessed by 1".to_owned()));
}

#[test]
fn a_witness_must_run_and_pass_unmodified_before_any_mutant() {
    for (witnesses, outcome) in [
        (const { &[witness("broken_unmodified")] }, "failed"),
        (const { &[witness("absent")] }, "did not run"),
    ] {
        let fixture = Fixture::new();
        let gate = mutant("gate", "alpha = on", "alpha = off", witnesses);
        let (failures, _) = fixture.run(&[gate]);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].contains(&format!("{outcome}, and must have passed")),
            "{failures:?}"
        );
        assert!(
            !fixture.record().iter().any(|line| line.contains("= off")),
            "{:?}",
            fixture.record()
        );
    }
}

#[test]
fn a_baseline_that_does_not_build_stops_the_run() {
    let fixture = Fixture::new();
    std::fs::write(fixture.path("repository/src.txt"), "alpha = on UNVIABLE\n").unwrap();
    let (failures, _) = fixture.run(&[ALPHA]);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0].starts_with("baseline: demo does not build"),
        "{failures:?}"
    );
    assert_eq!(fixture.record().len(), 1);
}

#[test]
fn drift_stops_the_run_before_anything_is_built() {
    let fixture = Fixture::new();
    let moved = mutant(
        "moved",
        "gamma = on",
        "gamma = off",
        const { &[witness("survives")] },
    );
    let (failures, _) = fixture.run(&[ALPHA, moved]);
    assert_eq!(
        failures,
        ["moved: the pattern must occur exactly once in src.txt, found 0 occurrences"]
    );
    assert!(fixture.record().is_empty());
}

#[test]
fn a_pattern_must_occur_exactly_once_counting_overlaps() {
    let apply = |find, source: &str| mutant("m", find, "x", &[]).apply(source);
    assert_eq!(apply("bb", "abba").as_deref(), Ok("axa"));
    for (find, source, count) in [
        ("a", "bb", 0),
        ("a", "aba", 2),
        ("aa", "aaa", 2),
        ("", "", 0),
    ] {
        let error = apply(find, source).expect_err(find);
        assert!(
            error.ends_with(&format!("found {count} occurrences")),
            "{error}"
        );
    }
    assert!(apply("", "a").is_err(), "an empty pattern names no site");
}

#[test]
fn a_missing_file_is_drift() {
    let fixture = Fixture::new();
    let gone = Mutant {
        file: "gone.txt",
        ..ALPHA
    };
    let found = drift(&fixture.path("repository"), &[gone]);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].starts_with("alpha: cannot read gone.txt: "));
}

#[test]
fn a_malformed_catalogue_is_reported() {
    assert_eq!(validate(&[ALPHA, BETA]), Vec::<String>::new());
    assert_eq!(
        validate(&[]),
        ["the catalogue is empty, so it witnesses nothing"]
    );
    let hollow = Mutant {
        id: "alpha",
        disables: "",
        replace: "alpha = on",
        witnesses: &[],
        ..ALPHA
    };
    assert_eq!(
        validate(&[ALPHA, hollow]),
        [
            "`alpha` is declared twice",
            "alpha: say what the edit disables",
            "alpha: the replacement changes nothing",
            "alpha: no witness is named",
        ]
    );
    for id in ["", "Upper", "with space", "under_score"] {
        let found = validate(&[Mutant { id, ..ALPHA }]);
        assert_eq!(found.len(), 1, "{id}: {found:?}");
    }
    assert_eq!(
        validate(&[Mutant { id: "a-1", ..ALPHA }]),
        Vec::<String>::new()
    );
    let twice = Mutant {
        witnesses: const { &[witness("survives"), witness("survives")] },
        ..ALPHA
    };
    assert_eq!(validate(&[twice]), ["alpha: a witness is named twice"]);
}

#[test]
fn a_verdict_needs_one_test_and_an_agreeing_exit_status() {
    let summary = |passed, failed| {
        format!("noise\ntest result: ok. {passed} passed; {failed} failed; 0 ignored\n\n")
    };
    for (output, success, verdict) in [
        (summary(1, 0), true, Verdict::Passed),
        (summary(0, 1), false, Verdict::Failed),
        (summary(0, 0), true, Verdict::NotRun),
        (summary(1, 0), false, Verdict::Broken),
        (summary(0, 1), true, Verdict::Broken),
        (summary(0, 0), false, Verdict::Broken),
        (summary(2, 0), true, Verdict::Broken),
        (summary(0, 2), false, Verdict::Broken),
        ("no summary".to_owned(), false, Verdict::Broken),
        // The harness prints its summary last; a test's own output may not
        // stand in for it.
        (summary(1, 0) + &summary(0, 1), false, Verdict::Failed),
    ] {
        assert_eq!(Verdict::read(&output, success), verdict, "{output:?}");
    }
}

#[test]
fn only_the_postgres_package_needs_a_database() {
    assert!(!witness("x").needs_postgres());
    let backend = Witness {
        package: "tollgate-store-postgres",
        ..witness("x")
    };
    assert!(backend.needs_postgres());
}

#[test]
fn a_directory_that_is_not_a_repository_is_an_error() {
    let fixture = Fixture::new();
    std::fs::remove_dir_all(fixture.path("repository/.git")).unwrap();
    let options = fixture.options();
    let error = run(&options, &[ALPHA], &mut |_| {}).expect_err("no file list");
    assert!(error.starts_with("git ls-files failed in "), "{error}");
}

// ---- the binary ------------------------------------------------------------

fn binary(arguments: &[&str]) -> Output {
    let empty = tempfile::tempdir().expect("empty directory");
    Command::new(env!("CARGO_BIN_EXE_check_witness_mutants"))
        .args(arguments)
        .current_dir(empty.path())
        .env_remove("TOLLGATE_PG_URL")
        .output()
        .expect("run the gate")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn help_and_version_win_over_every_other_argument() {
    for flag in ["--help", "-h"] {
        let out = binary(&["--nonsense", flag]);
        assert!(out.status.success());
        assert!(text(&out.stdout).starts_with("Usage: check_witness_mutants"));
    }
    for flag in ["--version", "-V"] {
        let out = binary(&["--only", flag]);
        assert!(out.status.success());
        assert!(text(&out.stdout).starts_with("check_witness_mutants "));
    }
}

#[test]
fn every_misuse_is_refused_with_its_reason() {
    for (arguments, reason) in [
        (&["--nonsense"][..], "unknown option: --nonsense"),
        (&["one", "two"], "expected at most one repository path"),
        // After `--` everything is a repository path, option-shaped or not.
        (&["--", "one", "--"], "expected at most one repository path"),
        (
            &["--", "--only", "x"],
            "expected at most one repository path",
        ),
        (
            &["--", "--shard", "x"],
            "expected at most one repository path",
        ),
        (&["--only"], "--only needs a mutant id"),
        (&["--only", "no-such"], "no mutant is named `no-such`"),
        (&["--shard"], "--shard needs K/N"),
        (&["--shard", "2/2"], "--shard must be K/N with 0 <= K < N"),
        (&["--shard", "0/0"], "--shard must be K/N with 0 <= K < N"),
        (&["--shard", "one"], "--shard must be K/N with 0 <= K < N"),
        (&["--shard", "9998/9999"], "the selection holds no mutant"),
        (&["--verify", "--list"], "give one of --verify and --list"),
        (
            &["--verify", "--only", "snapshot-sweep"],
            "--only and --shard select what runs",
        ),
        (
            &["--list", "--shard", "0/2"],
            "--only and --shard select what runs",
        ),
        (
            &["--only", "postgres-push-status"],
            "set TOLLGATE_PG_URL: PostgreSQL witnesses skip without it",
        ),
    ] {
        let out = binary(arguments);
        assert!(!out.status.success(), "{arguments:?}");
        let stderr = text(&out.stderr);
        assert!(stderr.contains(reason), "{arguments:?}: {stderr}");
    }
}

#[test]
fn a_tree_the_catalogue_does_not_apply_to_fails_verification_and_a_run() {
    // After `--`, a path that looks like an option is the repository.
    for arguments in [
        &["--verify", "--", "--list"][..],
        &["--only", "snapshot-sweep"],
    ] {
        let out = binary(arguments);
        assert!(!out.status.success(), "{arguments:?}");
        let stderr = text(&out.stderr);
        assert!(
            stderr.contains("snapshot-sweep") || stderr.contains("cannot resolve"),
            "{arguments:?}: {stderr}"
        );
        assert!(!text(&out.stdout).contains("OK"));
    }
}

#[test]
fn the_catalogue_is_listed_without_reading_a_repository() {
    let out = binary(&["--list"]);
    assert!(out.status.success());
    let listed = text(&out.stdout);
    assert!(listed.contains("snapshot-sweep: "), "{listed}");
    assert!(
        listed.contains("  tollgate-client --test snapshot_manager "),
        "{listed}"
    );
}

/// A repository holding exactly the sites the catalogue edits, and a cargo
/// whose witnesses pass while every site is intact.
fn catalogue_fixture() -> tempfile::TempDir {
    use std::collections::BTreeMap;
    use tollgate_repo_check::witness_mutants::catalog::CATALOG;

    let directory = tempfile::tempdir().expect("scratch directory");
    let repository = directory.path().join("repository");
    let mut sites: BTreeMap<&str, String> = BTreeMap::new();
    for mutant in CATALOG {
        let source = sites.entry(mutant.file).or_default();
        if !source.contains(mutant.find) {
            source.push_str(mutant.find);
            source.push_str("\n;\n");
        }
    }
    for (file, source) in &sites {
        let path = repository.join(file);
        std::fs::create_dir_all(path.parent().expect("a file in a crate")).expect("parents");
        std::fs::write(path, source).expect("sites");
    }
    std::fs::write(repository.join(".gitignore"), "/target\n/reports\n").expect("gitignore");
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&repository)
        .status()
        .expect("git");
    assert!(status.success());
    // The first line of the record names the package of each test run. A
    // witness fails once the copied tree differs from the repository.
    let script = format!(
        r#"#!/bin/sh
for argument in "$@"; do
  [ "$argument" = "--no-run" ] && exit 0
done
echo "$*" >> "{record}"
if diff -r -q -x .git -x target -x reports "{repository}" . > /dev/null; then
  echo "test result: ok. 1 passed; 0 failed; 0 ignored"
else
  echo "test result: FAILED. 0 passed; 1 failed; 0 ignored"; exit 101
fi
"#,
        record = directory.path().join("record").display(),
        repository = repository.display(),
    );
    let cargo = directory.path().join("cargo");
    std::fs::write(&cargo, script).expect("script");
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    directory
}

fn gate(fixture: &tempfile::TempDir, arguments: &[&str], database: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_check_witness_mutants"));
    command
        .args(arguments)
        .arg(fixture.path().join("repository"))
        .env("CARGO", fixture.path().join("cargo"))
        .env_remove("TOLLGATE_PG_URL");
    if database {
        command.env("TOLLGATE_PG_URL", "postgres://fixture");
    }
    command.output().expect("run the gate")
}

#[test]
fn verification_passes_a_tree_every_edit_applies_to() {
    use tollgate_repo_check::witness_mutants::catalog::CATALOG;

    let fixture = catalogue_fixture();
    let out = gate(&fixture, &["--verify"], false);
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains(&format!(
            "OK ({} edits each apply exactly once; none was run)",
            CATALOG.len()
        )),
        "{}",
        text(&out.stdout)
    );
    assert!(!fixture.path().join("record").exists(), "nothing ran");
}

#[test]
fn a_selection_runs_its_mutants_under_the_repository() {
    let fixture = catalogue_fixture();
    // Catalogue order, not the order asked for, and every second one from the
    // second: the shard of a selection.
    let out = gate(
        &fixture,
        &[
            "--only",
            "usage-retry",
            "--only",
            "snapshot-sweep",
            "--only",
            "snapshot-push",
            "--shard",
            "1/2",
        ],
        false,
    );
    let stdout = text(&out.stdout);
    assert!(out.status.success(), "{stdout}{}", text(&out.stderr));
    assert!(stdout.contains("snapshot-push: witnessed by "), "{stdout}");
    assert!(!stdout.contains("snapshot-sweep"), "{stdout}");
    assert!(!stdout.contains("usage-retry"), "{stdout}");
    assert!(
        stdout.contains("OK (1 mutants, every witness fails without its mechanism)"),
        "{stdout}"
    );
    let repository = fixture.path().join("repository");
    assert!(repository.join("target/witness-mutants/tree").is_dir());
    assert!(
        std::fs::read_dir(repository.join("reports/witness-mutants"))
            .expect("logs")
            .count()
            > 0
    );
}

#[test]
fn a_database_admits_postgres_witnesses() {
    let fixture = catalogue_fixture();
    let out = gate(&fixture, &["--only", "postgres-push-status"], true);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let ran = std::fs::read_to_string(fixture.path().join("record")).expect("witnesses ran");
    assert!(ran.contains("-p tollgate-store-postgres"), "{ran}");
}

#[test]
fn a_surviving_witness_fails_the_gate_with_its_name() {
    let fixture = catalogue_fixture();
    std::fs::write(
        fixture.path().join("cargo"),
        "#!/bin/sh\necho 'test result: ok. 1 passed; 0 failed; 0 ignored'\n",
    )
    .expect("script");
    let out = gate(&fixture, &["--only", "snapshot-lag-recovery"], false);
    assert!(!out.status.success());
    let stderr = text(&out.stderr);
    assert!(stderr.contains("FAILED (1 problems)"), "{stderr}");
    assert!(
        stderr.contains("a_lagged_push_stream_refetches_every_tracked_principal` passed"),
        "{stderr}"
    );
}

#[test]
fn the_catalogue_is_well_formed() {
    use tollgate_repo_check::witness_mutants::catalog::CATALOG;
    assert_eq!(validate(CATALOG), Vec::<String>::new());
}
