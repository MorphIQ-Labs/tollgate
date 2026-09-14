//! Exercise the CLI with synthetic Criterion output; these tests run no benchmarks.
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use serde_json::{Value, json};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "tollgate-recording-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let fixture = Self { root };
        fixture.write(
            "manifest.json",
            &json!({
                "benchmarks": [
                    {"id": "a", "target_ns": 500.0, "threshold_ns": 1000.0},
                    {"id": "b", "target_ns": 500.0, "threshold_ns": 1000.0}
                ],
                "ratios": [{"numerator": "b", "denominator": "a", "max_ratio": 2.5}]
            }),
        );
        fixture.write(
            "baseline.json",
            &json!({
                "host": {
                    "id": "controlled", "architecture": "test-target", "cpu": "test-cpu",
                    "os": "test-os", "rustc": "test-rustc"
                },
                "recorded_at": "2026-09-11T00:00:00Z", "git_revision": REVISION,
                "profile": "criterion release", "samples": 3,
                "benchmarks": [
                    {"id": "a", "mean_ns": 100.0, "max_regression": 0.15},
                    {"id": "b", "mean_ns": 200.0}
                ]
            }),
        );
        fixture.measure(1, 100.0);
        fixture
    }

    fn write(&self, path: &str, value: &Value) {
        let path = self.root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    fn read(&self, path: &str) -> Value {
        serde_json::from_slice(&std::fs::read(self.root.join(path)).unwrap()).unwrap()
    }

    fn measure(&self, run: u64, mean: f64) {
        // Stable, distinct start markers without sleeps. Estimate mtimes remain
        // newer than every marker, just as a completed benchmark run requires.
        let marker = std::fs::File::create(self.root.join("marker")).unwrap();
        marker
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_750_000_000 + run))
            .unwrap();
        self.write(
            "criterion/a/new/estimates.json",
            &json!({"mean": {"point_estimate": mean}}),
        );
        self.write(
            "criterion/b/new/estimates.json",
            &json!({"mean": {"point_estimate": 200.0}}),
        );
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_check_benchmark_thresholds"));
        command.current_dir(&self.root).args(args).envs([
            ("TOLLGATE_PERF_HOST", "controlled"),
            ("TOLLGATE_GATE_REVISION", REVISION),
            ("TOLLGATE_GATE_TARGET", "test-target"),
            ("TOLLGATE_GATE_CPU", "test-cpu"),
            ("TOLLGATE_GATE_OS", "test-os"),
            ("TOLLGATE_GATE_RUSTC", "test-rustc"),
        ]);
        command
    }

    fn run(&self, args: &[&str], overrides: &[(&str, Option<&str>)]) -> Output {
        let mut command = self.command(args);
        command.args([
            "--samples",
            "samples",
            "manifest.json",
            "criterion",
            "report.json",
            "marker",
        ]);
        for (name, value) in overrides {
            if let Some(value) = value {
                command.env(name, value);
            } else {
                command.env_remove(name);
            }
        }
        command.output().unwrap()
    }

    fn collect_two(&self) {
        for run in 1..=2 {
            self.measure(run, 99.0 + run as f64);
            assert_exit(&self.run(&["--baseline", "baseline.json"], &[]), 0);
        }
        self.measure(3, 102.0);
    }

    fn sample_paths(&self) -> Vec<PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(self.root.join("samples"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        paths.sort();
        paths
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn assert_exit(output: &Output, expected: i32) {
    assert_eq!(
        output.status.code(),
        Some(expected),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_error(output: &Output, expected: &str) {
    assert_exit(output, 1);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn unknown_manifest_keys_fail_before_measurement_reads_or_output_changes() {
    let fixture = Fixture::new();
    let manifest = fixture.read("manifest.json");
    let baseline = std::fs::read(fixture.root.join("baseline.json")).unwrap();
    fixture.write("report.json", &json!({"previous": "report"}));
    std::fs::remove_dir_all(fixture.root.join("criterion")).unwrap();
    for (pointer, key, value) in [
        ("", "truts", json!({"moved_shift": 0.2})),
        ("", "ratios_typo", json!([])),
        ("/trust", "moved_shft", json!(0.2)),
        ("/benchmarks/0", "threshold_typo", json!(5)),
        ("/ratios/0", "max_ratio_typo", json!(1)),
    ] {
        let mut invalid = manifest.clone();
        invalid["trust"] = json!({});
        invalid.pointer_mut(pointer).unwrap()[key] = value;
        fixture.write("manifest.json", &invalid);
        let output = fixture.run(&["--record", "baseline.json"], &[]);
        assert_error(&output, &format!("unknown field `{key}`"));
        assert_eq!(fixture.read("report.json"), json!({"previous": "report"}));
        assert_eq!(
            std::fs::read(fixture.root.join("baseline.json")).unwrap(),
            baseline
        );
        assert!(!fixture.root.join("samples").exists());
    }
}

#[test]
fn unknown_baseline_keys_cannot_be_used_for_comparison_or_promoted_over() {
    let fixture = Fixture::new();
    fixture.collect_two();
    let mut baseline = fixture.read("baseline.json");
    let entry = baseline["benchmarks"][0].as_object_mut().unwrap();
    let bound = entry.remove("max_regression").unwrap();
    entry.insert("max_regresion".to_owned(), bound);
    fixture.write("baseline.json", &baseline);
    let previous = std::fs::read(fixture.root.join("baseline.json")).unwrap();
    let previous_report = std::fs::read(fixture.root.join("report.json")).unwrap();
    let comparison = fixture.run(&["--baseline", "baseline.json"], &[]);
    assert_error(&comparison, "unknown field `max_regresion`");
    assert_eq!(
        std::fs::read(fixture.root.join("report.json")).unwrap(),
        previous_report
    );
    let recording = fixture.run(&["--record", "baseline.json"], &[]);
    assert_error(&recording, "unknown field `max_regresion`");
    assert_eq!(
        std::fs::read(fixture.root.join("baseline.json")).unwrap(),
        previous
    );
}

#[test]
fn optional_samples_preserve_gate_verdicts_without_recordable_provenance() {
    let fixture = Fixture::new();
    assert_exit(
        &fixture.run(&["--ratios-only"], &[("TOLLGATE_PERF_HOST", None)]),
        0,
    );
    assert!(!fixture.root.join("samples").exists());
    let output = fixture.run(
        &["--baseline", "baseline.json"],
        &[("TOLLGATE_GATE_REVISION", Some("dirty"))],
    );
    assert_exit(&output, 0);
    assert!(String::from_utf8_lossy(&output.stderr).contains("not depositing a sample"));
    assert_eq!(fixture.read("report.json")["passed"], true);
    assert_exit(
        &fixture.run(
            &["--baseline", "baseline.json"],
            &[("TOLLGATE_PERF_HOST", None)],
        ),
        4,
    );
    fixture.measure(2, 50.0); // A real ratio failure must still fail.
    assert_exit(
        &fixture.run(
            &["--baseline", "baseline.json"],
            &[("TOLLGATE_GATE_REVISION", Some("dirty"))],
        ),
        1,
    );
}

#[test]
fn a_diagnostic_run_never_deposits_calibration_samples() {
    let fixture = Fixture::new();
    assert_exit(&fixture.run(&["--ratios-only"], &[]), 0);
    assert!(!fixture.root.join("samples").exists());
}

#[test]
fn cli_judges_instability_over_only_measured_normally_steady_rows() {
    for steady_is_unstable in [false, true] {
        for missing_row in [false, true] {
            let fixture = Fixture::new();
            let ids = ["a", "b", "c", "contended_a", "contended_b"];
            let mut manifest = fixture.read("manifest.json");
            manifest["benchmarks"] = json!(ids.map(|id| json!({
                "id": id, "target_ns": 500.0, "threshold_ns": 1000.0
            })));
            manifest["trust"] = json!({"unstable_fraction": 0.6});
            if missing_row {
                manifest["benchmarks"].as_array_mut().unwrap().push(json!({
                    "id": "missing", "target_ns": 500.0, "threshold_ns": 1000.0
                }));
            }
            fixture.write("manifest.json", &manifest);
            let mut baseline = fixture.read("baseline.json");
            baseline["benchmarks"] = json!(ids.map(|id| json!({
                "id": id, "mean_ns": 100.0,
                "max_regression": if id.starts_with("contended") { 0.15 } else { 0.05 }
            })));
            fixture.write("baseline.json", &baseline);
            for id in ids {
                let wide =
                    id.starts_with("contended") || (steady_is_unstable && matches!(id, "a" | "b"));
                fixture.write(
                    &format!("criterion/{id}/new/estimates.json"),
                    &json!({"mean": {
                        "point_estimate": 100.0,
                        "confidence_interval": {
                            "lower_bound": if wide { 80.0 } else { 99.0 },
                            "upper_bound": if wide { 120.0 } else { 101.0 }
                        }
                    }}),
                );
            }
            let output = fixture.run(&["--baseline", "baseline.json"], &[]);
            assert_exit(
                &output,
                if steady_is_unstable {
                    3
                } else if missing_row {
                    1
                } else {
                    0
                },
            );
            let report = fixture.read("report.json");
            assert_eq!(
                report["trust"],
                if steady_is_unstable {
                    json!({"verdict": "unreadable", "unstable": 2, "measured": 3})
                } else {
                    json!({"verdict": "no_history"})
                }
            );
        }
    }
}

#[test]
fn optional_sample_io_errors_preserve_the_verdict_but_recording_refuses() {
    let fixture = Fixture::new();
    fixture.write("samples", &json!("a file is not a sample directory"));
    assert_exit(&fixture.run(&["--baseline", "baseline.json"], &[]), 0);
    let before = fixture.read("baseline.json");
    assert_error(
        &fixture.run(&["--record", "baseline.json"], &[]),
        "cannot deposit sample",
    );
    assert_eq!(fixture.read("baseline.json"), before);
}

#[test]
fn recording_requires_samples_before_reading_input_and_help_still_works() {
    let fixture = Fixture::new();
    let args = [
        "--record",
        "baseline.json",
        "missing",
        "criterion",
        "report.json",
        "marker",
    ];
    assert_error(
        &fixture.command(&args).output().unwrap(),
        "--record requires --samples",
    );
    for option in ["--help", "--version"] {
        assert_exit(&fixture.command(&args).arg(option).output().unwrap(), 0);
    }
    assert_error(
        &fixture.run(
            &["--record", "baseline.json"],
            &[("TOLLGATE_GATE_REVISION", Some("dirty"))],
        ),
        "committed revision",
    );
}

#[test]
fn checker_retries_cannot_satisfy_the_minimum_run_count() {
    let fixture = Fixture::new();
    for _ in 0..3 {
        assert_exit(&fixture.run(&["--baseline", "baseline.json"], &[]), 0);
    }
    assert_eq!(fixture.sample_paths().len(), 1);
    let original = fixture.sample_paths().remove(0);
    std::fs::copy(&original, fixture.root.join("samples/copied.json")).unwrap();
    assert_error(
        &fixture.run(&["--record", "baseline.json"], &[]),
        "1 available",
    );
    assert_eq!(
        fixture.read("baseline.json")["benchmarks"][0]["mean_ns"],
        100.0
    );
}

#[test]
fn recording_validates_the_destination_and_preserves_its_bounds() {
    for comparison in [None, Some("comparison.json")] {
        let fixture = Fixture::new();
        let mut other = fixture.read("baseline.json");
        other["benchmarks"][0]["max_regression"] = json!(0.05);
        fixture.write("comparison.json", &other);
        fixture.collect_two();
        let mut args = vec!["--record", "baseline.json"];
        if let Some(comparison) = comparison {
            args.extend(["--baseline", comparison]);
        }
        // Without a comparison input, recording succeeds but full gating is UNENFORCED.
        assert_exit(
            &fixture.run(&args, &[]),
            if comparison.is_some() { 0 } else { 4 },
        );
        let baseline = fixture.read("baseline.json");
        assert_eq!(baseline["samples"], 3);
        assert_eq!(baseline["benchmarks"][0]["mean_ns"], 101.0);
        assert_eq!(baseline["benchmarks"][0]["max_regression"], 0.15);
        assert_eq!(baseline["benchmarks"][1]["mean_ns"], 200.0);
    }
}

#[test]
fn recording_creates_a_new_destination_but_refuses_an_unreadable_existing_one() {
    let fixture = Fixture::new();
    fixture.collect_two();
    assert_exit(&fixture.run(&["--record", "new.json"], &[]), 4);
    let baseline = fixture.read("new.json");
    assert_eq!(baseline["samples"], 3);
    assert_eq!(baseline["benchmarks"][0]["mean_ns"], 101.0);
    assert_eq!(baseline["benchmarks"][0]["max_regression"], Value::Null);
    std::fs::create_dir(fixture.root.join("directory.json")).unwrap();
    assert_error(
        &fixture.run(&["--record", "directory.json"], &[]),
        "cannot read destination",
    );
    assert!(fixture.root.join("directory.json").is_dir());
}

#[test]
fn a_foreign_or_invalid_destination_survives_recording_without_baseline_input() {
    for damage in ["host", "json", "bound"] {
        let fixture = Fixture::new();
        fixture.collect_two();
        let mut destination = fixture.read("baseline.json");
        match damage {
            "host" => destination["host"]["id"] = json!("another-host"),
            "bound" => destination["benchmarks"][0]["max_regression"] = json!(-0.1),
            _ => destination = json!("not a baseline"),
        }
        fixture.write("baseline.json", &destination);
        let before = std::fs::read(fixture.root.join("baseline.json")).unwrap();
        assert_exit(&fixture.run(&["--record", "baseline.json"], &[]), 1);
        assert_eq!(
            std::fs::read(fixture.root.join("baseline.json")).unwrap(),
            before
        );
        assert!(!fixture.root.join("baseline.json.staged").exists());
    }
}

#[test]
fn samples_from_different_revisions_or_environments_are_not_combined() {
    for (name, old_value) in [
        (
            "TOLLGATE_GATE_REVISION",
            "abcdef0123456789abcdef0123456789abcdef01",
        ),
        ("TOLLGATE_PERF_HOST", "old-host"),
        ("TOLLGATE_GATE_TARGET", "old-target"),
        ("TOLLGATE_GATE_CPU", "old-cpu"),
        ("TOLLGATE_GATE_OS", "old-os"),
        ("TOLLGATE_GATE_RUSTC", "old-rustc"),
    ] {
        let fixture = Fixture::new();
        for run in 1..=2 {
            fixture.measure(run, 100.0);
            assert_exit(&fixture.run(&[], &[(name, Some(old_value))]), 4);
        }
        fixture.measure(3, 100.0);
        assert_error(
            &fixture.run(&["--record", "baseline.json"], &[]),
            "1 available",
        );
        assert_eq!(fixture.sample_paths().len(), 3);
    }
}

#[test]
fn profile_changes_and_legacy_samples_cannot_supply_missing_runs() {
    let fixture = Fixture::new();
    fixture.collect_two();
    for path in fixture.sample_paths() {
        let mut sample: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        sample["context"]["profile"] = json!("production");
        std::fs::write(path, serde_json::to_vec(&sample).unwrap()).unwrap();
    }
    fixture.write(
        "samples/legacy.json",
        &json!({"revision": REVISION, "recorded_at": "old", "means": {"a": 100.0, "b": 200.0}}),
    );
    let output = fixture.run(&["--record", "baseline.json"], &[]);
    assert_error(&output, "1 available");
    assert!(String::from_utf8_lossy(&output.stderr).contains("skipping legacy sample"));
}

#[test]
fn replay_cannot_relabel_a_runs_environment_or_replace_its_measurements() {
    let fixture = Fixture::new();
    assert_exit(&fixture.run(&["--baseline", "baseline.json"], &[]), 0);
    let path = fixture.sample_paths().remove(0);
    let before = std::fs::read(&path).unwrap();
    assert_error(
        &fixture.run(
            &["--record", "baseline.json"],
            &[("TOLLGATE_GATE_RUSTC", Some("new-rustc"))],
        ),
        "conflicting sample",
    );
    fixture.measure(1, 101.0);
    assert_error(
        &fixture.run(&["--record", "baseline.json"], &[]),
        "conflicting sample",
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn a_partial_run_cannot_record_using_previous_complete_samples() {
    let fixture = Fixture::new();
    fixture.collect_two();
    assert_exit(&fixture.run(&["--baseline", "baseline.json"], &[]), 0);
    fixture.measure(4, 100.0);
    std::fs::remove_file(fixture.root.join("criterion/b/new/estimates.json")).unwrap();
    let before = fixture.read("baseline.json");
    assert_error(
        &fixture.run(&["--record", "baseline.json"], &[]),
        "every sample must cover the whole manifest",
    );
    assert_eq!(fixture.read("baseline.json"), before);
}

#[test]
fn an_existing_staging_owner_blocks_replacement_without_touching_either_file() {
    let fixture = Fixture::new();
    fixture.collect_two();
    let staged = "baseline.json.staged";
    fixture.write(staged, &json!("another recorder owns this file"));
    let before = fixture.read("baseline.json");
    assert_error(
        &fixture.run(&["--record", "baseline.json"], &[]),
        "cannot exclusively stage",
    );
    assert_eq!(fixture.read("baseline.json"), before);
    assert_eq!(fixture.read(staged), "another recorder owns this file");
}

#[test]
fn concurrent_retries_publish_one_complete_sample() {
    let fixture = Fixture::new();
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    assert_exit(&fixture.run(&["--baseline", "baseline.json"], &[]), 0);
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    assert_eq!(fixture.sample_paths().len(), 1);
    let sample: Value =
        serde_json::from_slice(&std::fs::read(&fixture.sample_paths()[0]).unwrap()).unwrap();
    assert_eq!(sample["means"], json!({"a": 100.0, "b": 200.0}));
    assert_eq!(
        std::fs::read_dir(fixture.root.join("samples"))
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn divergent_copies_and_invalid_sample_values_preserve_the_destination() {
    for damage in ["copy", "mean", "id"] {
        let fixture = Fixture::new();
        fixture.collect_two();
        let mut sample: Value =
            serde_json::from_slice(&std::fs::read(&fixture.sample_paths()[0]).unwrap()).unwrap();
        match damage {
            "copy" => sample["means"]["a"] = json!(101.0),
            "mean" => sample["means"]["a"] = json!(-1.0),
            _ => sample["run_id"] = json!("../outside"),
        }
        fixture.write("samples/copied.json", &sample);
        let before = fixture.read("baseline.json");
        assert_error(
            &fixture.run(&["--record", "baseline.json"], &[]),
            if damage == "copy" {
                "conflicting samples"
            } else {
                "invalid sample"
            },
        );
        assert_eq!(fixture.read("baseline.json"), before);
    }
}

#[test]
fn invalid_criterion_means_fail_gating_without_depositing_a_sample() {
    for mean in [0.0, -1.0] {
        let fixture = Fixture::new();
        fixture.measure(1, mean);
        assert_error(
            &fixture.run(&["--baseline", "baseline.json"], &[]),
            "invalid mean",
        );
        assert!(!fixture.root.join("samples").exists());
    }
}
