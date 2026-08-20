//! Compares Criterion benchmark means against a threshold manifest.
//!
//! Usage:
//!   check_benchmark_thresholds <manifest.json> <criterion-root> <report.json> <freshness-marker>
//!
//! Mirrors ferro-risk's gate semantics:
//! - `target_ns` is aspirational, `threshold_ns` is the failing bound;
//!   a manifest with `target_ns > threshold_ns` is itself invalid.
//! - Each benchmark id maps to `<criterion-root>/<id>/new/estimates.json`,
//!   whose `mean.point_estimate` (nanoseconds) is compared to `threshold_ns`.
//! - Estimates older than the freshness marker are rejected: the gate must
//!   never pass on stale output left by a previous run.
//! - A JSON report is always written; the exit code is nonzero when any
//!   benchmark is missing, stale, or over threshold.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Manifest {
    benchmarks: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    id: String,
    target_ns: f64,
    threshold_ns: f64,
}

#[derive(Deserialize)]
struct Estimates {
    mean: PointEstimate,
}

#[derive(Deserialize)]
struct PointEstimate {
    point_estimate: f64,
}

#[derive(Serialize)]
struct ReportRow {
    id: String,
    mean_ns: Option<f64>,
    target_ns: f64,
    threshold_ns: f64,
    status: &'static str,
}

#[derive(Serialize)]
struct Report {
    rows: Vec<ReportRow>,
    passed: bool,
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("perf-gate: {msg}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [manifest_path, criterion_root, report_path, marker_path] = match args.as_slice() {
        [a, b, c, d] => [a, b, c, d].map(PathBuf::from),
        _ => {
            return fail(
                "usage: check_benchmark_thresholds <manifest.json> <criterion-root> <report.json> <freshness-marker>",
            );
        }
    };

    let manifest: Manifest = match std::fs::read_to_string(&manifest_path)
        .map_err(|e| e.to_string())
        .and_then(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
    {
        Ok(m) => m,
        Err(e) => {
            return fail(&format!(
                "cannot read manifest {}: {e}",
                manifest_path.display()
            ));
        }
    };
    if manifest.benchmarks.is_empty() {
        return fail("manifest lists no benchmarks");
    }

    let marker_mtime = match std::fs::metadata(&marker_path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(e) => {
            return fail(&format!(
                "cannot stat freshness marker {}: {e}",
                marker_path.display()
            ));
        }
    };

    let mut rows = Vec::new();
    let mut passed = true;
    for entry in &manifest.benchmarks {
        if entry.target_ns > entry.threshold_ns {
            eprintln!(
                "perf-gate: INVALID {}: target_ns {} exceeds threshold_ns {}",
                entry.id, entry.target_ns, entry.threshold_ns
            );
            passed = false;
            rows.push(ReportRow {
                id: entry.id.clone(),
                mean_ns: None,
                target_ns: entry.target_ns,
                threshold_ns: entry.threshold_ns,
                status: "invalid-manifest",
            });
            continue;
        }
        let estimates_path: PathBuf = criterion_root
            .join(Path::new(&entry.id))
            .join("new/estimates.json");
        let (mean, status) = match read_estimates(&estimates_path, marker_mtime) {
            Ok(mean) if mean <= entry.threshold_ns => (Some(mean), "pass"),
            Ok(mean) => {
                passed = false;
                (Some(mean), "over-threshold")
            }
            Err(e) => {
                passed = false;
                eprintln!("perf-gate: {}: {e}", entry.id);
                (None, "missing-or-stale")
            }
        };
        if let Some(mean) = mean {
            let verdict = if mean <= entry.target_ns {
                "within target"
            } else if mean <= entry.threshold_ns {
                "over target, within threshold"
            } else {
                "OVER THRESHOLD"
            };
            println!(
                "perf-gate: {}: mean {:.1} ns (target {:.1}, threshold {:.1}) — {verdict}",
                entry.id, mean, entry.target_ns, entry.threshold_ns
            );
        }
        rows.push(ReportRow {
            id: entry.id.clone(),
            mean_ns: mean,
            target_ns: entry.target_ns,
            threshold_ns: entry.threshold_ns,
            status,
        });
    }

    let report = Report { rows, passed };
    if let Some(parent) = report_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(
        &report_path,
        serde_json::to_string_pretty(&report).expect("report serializes"),
    ) {
        return fail(&format!(
            "cannot write report {}: {e}",
            report_path.display()
        ));
    }

    if passed {
        println!("perf-gate: PASS ({} benchmarks)", report.rows.len());
        ExitCode::SUCCESS
    } else {
        fail("FAIL — see report")
    }
}

fn read_estimates(path: &Path, marker_mtime: SystemTime) -> Result<f64, String> {
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("missing estimates {}: {e}", path.display()))?;
    let mtime = meta
        .modified()
        .map_err(|e| format!("cannot stat {}: {e}", path.display()))?;
    if mtime < marker_mtime {
        return Err(format!(
            "stale estimates {} predate this gate run; refusing to gate on old output",
            path.display()
        ));
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let estimates: Estimates =
        serde_json::from_str(&text).map_err(|e| format!("cannot parse {}: {e}", path.display()))?;
    Ok(estimates.mean.point_estimate)
}
