//! Compares Criterion benchmark means against a threshold manifest.
//!
//! Usage:
//!   check_benchmark_thresholds [--ratios-only] [--baseline <baseline.json>]
//!     <manifest.json> <criterion-root> <report.json> <freshness-marker>
//!
//! Mirrors ferro-risk's gate semantics:
//! - `target_ns` is aspirational, `threshold_ns` is the failing bound;
//!   a manifest with `target_ns > threshold_ns` is itself invalid.
//! - Each benchmark id maps to `<criterion-root>/<id>/new/estimates.json`,
//!   whose `mean.point_estimate` (nanoseconds) is compared to `threshold_ns`.
//! - Optional ratio bounds compare two measurements from the same run, so a
//!   contention budget is portable across otherwise different hosts.
//! - An optional recorded baseline enforces per-row regressions only when
//!   `TOLLGATE_PERF_HOST` matches its host id and the run is trusted.
//! - `--ratios-only` still requires every fresh row but deliberately skips
//!   absolute thresholds and recorded-baseline decisions for shared CI.
//! - Estimates older than the freshness marker are rejected: the gate must
//!   never pass on stale output left by a previous run.
//! - A JSON report is always written; the exit code is nonzero when required
//!   data is missing/stale or any active bound is exceeded.
//!
//! It also decides whether the run is worth believing at all (#49). A gate
//! that only ever answers PASS or FAIL cannot distinguish a regression from a
//! measurement taken while the host was busy, and the two look identical in
//! the output — which is how a 2.6x inflation of every `tollgate-core`
//! benchmark was read as a real threshold breach, and how a baseline captured
//! on a loaded machine made a later change look 16% faster than it was.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// Exit code for a run the gate will not draw a conclusion from. Distinct
/// from FAILURE so a caller can tell "your change is slow" from "ask me
/// again on a quiet machine".
const UNTRUSTED_EXIT: u8 = 3;
const USAGE: &str = "usage: check_benchmark_thresholds [--ratios-only] [--baseline <baseline.json>] <manifest.json> <criterion-root> <report.json> <freshness-marker>";

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum GateMode {
    Full,
    RatiosOnly,
}

#[derive(Debug, PartialEq)]
enum Command {
    Run {
        manifest_path: PathBuf,
        criterion_root: PathBuf,
        report_path: PathBuf,
        marker_path: PathBuf,
        baseline_path: Option<PathBuf>,
        mode: GateMode,
    },
    Help,
    Version,
}

#[derive(Deserialize)]
struct Manifest {
    benchmarks: Vec<Entry>,
    #[serde(default)]
    ratios: Vec<RatioBound>,
    #[serde(default)]
    trust: TrustPolicy,
}

#[derive(Deserialize)]
struct RatioBound {
    numerator: String,
    denominator: String,
    max_ratio: f64,
}

#[derive(Deserialize)]
struct Entry {
    id: String,
    target_ns: f64,
    threshold_ns: f64,
}

#[derive(Clone, Deserialize, Serialize)]
struct BaselineHost {
    id: String,
    architecture: String,
    cpu: String,
    os: String,
    rustc: String,
}

#[derive(Deserialize)]
struct Baseline {
    host: BaselineHost,
    recorded_at: String,
    git_revision: String,
    profile: String,
    benchmarks: Vec<BaselineEntry>,
}

#[derive(Deserialize)]
struct BaselineEntry {
    id: String,
    mean_ns: f64,
    #[serde(default = "default_max_regression")]
    max_regression: f64,
}

fn default_max_regression() -> f64 {
    0.05
}

/// When to stop believing a run.
#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
struct TrustPolicy {
    /// Relative change against the previous run at which a benchmark counts
    /// as having moved.
    #[serde(default = "default_shift")]
    moved_shift: f64,
    /// Fraction of benchmarks that must have moved before the whole run is
    /// untrusted. A real change moves one or two; a busy host moves most.
    #[serde(default = "default_fraction")]
    moved_fraction: f64,
    /// Relative width of a benchmark's 95% confidence interval above which
    /// its own measurement is too unstable to read.
    #[serde(default = "default_ci_width")]
    max_ci_width: f64,
    /// Fraction of benchmarks that must be individually unreadable before the
    /// whole run is. The history-free counterpart to `moved_fraction`, and the
    /// only breadth signal shared CI can produce.
    #[serde(default = "default_unstable_fraction")]
    unstable_fraction: f64,
}

fn default_shift() -> f64 {
    0.40
}
fn default_fraction() -> f64 {
    0.34
}
fn default_ci_width() -> f64 {
    0.10
}
fn default_unstable_fraction() -> f64 {
    0.10
}

impl Default for TrustPolicy {
    fn default() -> Self {
        TrustPolicy {
            moved_shift: default_shift(),
            moved_fraction: default_fraction(),
            max_ci_width: default_ci_width(),
            unstable_fraction: default_unstable_fraction(),
        }
    }
}

#[derive(Deserialize)]
struct Estimates {
    mean: PointEstimate,
}

#[derive(Deserialize)]
struct PointEstimate {
    point_estimate: f64,
    /// Already written by criterion into the file the gate has always read;
    /// nothing new is measured to obtain it.
    confidence_interval: Option<ConfidenceInterval>,
}

#[derive(Deserialize, Clone, Copy)]
struct ConfidenceInterval {
    lower_bound: f64,
    upper_bound: f64,
}

/// One benchmark's reading: what it measured, and how steady the measurement
/// was while it did.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Measurement {
    mean_ns: f64,
    /// `(upper - lower) / point`, or `None` when criterion recorded no
    /// interval.
    ci_width: Option<f64>,
}

/// What the gate concluded about the run as a whole.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case", tag = "verdict")]
enum Trust {
    /// Nothing to compare against — a first run, or a fresh CI job.
    NoHistory,
    Trusted {
        moved: usize,
        compared: usize,
    },
    Untrusted {
        moved: usize,
        compared: usize,
    },
    /// Too much of the run was individually unreadable to believe any of it.
    ///
    /// The same breadth argument as `Untrusted`, on the one signal that needs
    /// no history — which is what makes it the verdict shared CI can actually
    /// reach (#112).
    Unreadable {
        unstable: usize,
        measured: usize,
    },
}

/// Decide whether this run can be believed, by asking how much of it moved.
///
/// The discriminator is breadth, not magnitude: a change makes one or two
/// benchmarks faster or slower, while a loaded host drags everything it
/// touches. Comparing against this host's own previous run keeps the check
/// free of any absolute expectation, which would be as host-dependent as the
/// thresholds the manifest already calls provisional.
fn assess_trust(
    current: &BTreeMap<String, f64>,
    previous: &BTreeMap<String, f64>,
    unstable: usize,
    measured: usize,
    policy: &TrustPolicy,
) -> Trust {
    // Checked first, and without history, because this is the only one of the
    // two verdicts shared CI can reach: every job starts from a clean
    // workspace, so `previous` is always empty there and the shift comparison
    // below returns `NoHistory` no matter how contaminated the run was.
    //
    // Same discriminator as the shift rule — breadth. Calibrated on three
    // observed runs of one unchanged tree: a clean pipeline had 0 of 39
    // benchmarks individually unreadable, while the two that reported false
    // ratio failures had 8 of 33 and 6 of 33. A quiet controlled host still
    // marks the odd contention benchmark, so the fraction sits below those
    // 18-24% and above that handful.
    #[allow(clippy::cast_precision_loss)]
    // `measured > 0` is load-bearing rather than defensive: the division below
    // is what decides the verdict, and a run that measured nothing would make
    // it NaN or infinite — either of which compares its way to "unreadable"
    // and condemns a run that produced no benchmarks for a reason the host
    // cannot explain.
    let widely_unreadable = unstable > 1
        && measured > 0
        && (unstable as f64) / (measured as f64) >= policy.unstable_fraction;
    if widely_unreadable {
        return Trust::Unreadable { unstable, measured };
    }
    let shifts: Vec<f64> = current
        .iter()
        .filter_map(|(id, now)| {
            let before = previous.get(id)?;
            (*before > 0.0).then(|| (now - before).abs() / before)
        })
        .collect();
    if shifts.is_empty() {
        return Trust::NoHistory;
    }
    let moved = shifts.iter().filter(|s| **s > policy.moved_shift).count();
    let compared = shifts.len();
    // Guard the degenerate case: with one or two benchmarks in common, any
    // single genuine change clears the fraction, so require a plurality to
    // be more than one benchmark.
    #[allow(clippy::cast_precision_loss)]
    let broad = moved > 1 && (moved as f64) / (compared as f64) >= policy.moved_fraction;
    if broad {
        Trust::Untrusted { moved, compared }
    } else {
        Trust::Trusted { moved, compared }
    }
}

/// Whether every benchmark cleared its threshold.
///
/// Separate from `main` for the same reason as `evaluate`: the aggregation
/// that decides the exit code should be checkable without running the binary.
fn all_passed(rows: &[ReportRow]) -> bool {
    rows.iter().all(|row| row.status == "pass")
}

/// Whether both the absolute-latency rows and the portable ratio rows passed.
///
/// Keep this aggregation outside `main` so its fail-closed conjunction is a
/// directly testable contract rather than process wiring.
fn all_results_passed(
    rows: &[ReportRow],
    ratios: &[RatioRow],
    regressions: &[RegressionRow],
) -> bool {
    all_passed(rows)
        // `inconclusive` is not a pass; it is the absence of a verdict, and it
        // is allowed through only because the alternative is a verdict from a
        // measurement the gate has already called unreadable (#112). The
        // whole-run guard below is what stops that becoming a gate that can
        // never fail.
        && ratios
            .iter()
            .all(|ratio| matches!(ratio.status, "pass" | "inconclusive"))
        && ratios_reached_a_verdict(ratios)
        && regressions
            .iter()
            .all(|regression| regression.status != "regressed")
}

/// A ratio is only as readable as the two measurements it divides.
///
/// `unstable` carries the ids whose own confidence interval was too wide to
/// read — the judgement `ReportRow::unstable` already makes and prints. Until
/// #112 that judgement stopped there: the gate declared a measurement
/// unreadable and then divided it anyway, and a shared runner that widened one
/// side's spread produced a verdict about the code from a number the gate had
/// already said nothing could be concluded from. It failed a merge request
/// whose diff could not touch either mechanism, twice measuring the *same*
/// code at ×3.40 and ×13.11.
///
/// So an unstable side yields `inconclusive`, not `pass` and not `over-ratio`.
/// This is the per-measurement form of the whole-run `Trust::Untrusted` rule,
/// and it exists because that rule needs a previous run on the same host to
/// fire — which shared CI, with a fresh workspace every time, never has. The
/// confidence interval is evidence that needs no history.
///
/// An inconclusive ratio is reported loudly and does not fail the gate; a run
/// in which *every* ratio is inconclusive does fail, because a gate that
/// measured nothing must not read as a gate that passed.
fn evaluate_ratio(
    bound: &RatioBound,
    means: &BTreeMap<String, f64>,
    unstable: &BTreeSet<String>,
) -> RatioRow {
    let numerator_ns = means.get(&bound.numerator).copied();
    let denominator_ns = means.get(&bound.denominator).copied();
    let numerator_unstable = unstable.contains(&bound.numerator);
    let denominator_unstable = unstable.contains(&bound.denominator);
    let ratio = numerator_ns
        .zip(denominator_ns)
        .and_then(|(numerator, denominator)| {
            (denominator > 0.0).then_some(numerator / denominator)
        });
    let status = match ratio {
        // Checked before the bound, deliberately: an unstable measurement that
        // happens to land under the ceiling is no more readable than one that
        // lands over it, and calling it `pass` would be the same mistake in
        // the direction nobody notices.
        Some(_) if numerator_unstable || denominator_unstable => "inconclusive",
        Some(ratio) if bound.max_ratio > 0.0 && ratio <= bound.max_ratio => "pass",
        Some(_) if bound.max_ratio > 0.0 => "over-ratio",
        Some(_) => "invalid-manifest",
        None => "missing-or-zero-denominator",
    };
    RatioRow {
        numerator: bound.numerator.clone(),
        denominator: bound.denominator.clone(),
        numerator_ns,
        denominator_ns,
        numerator_unstable,
        denominator_unstable,
        ratio,
        max_ratio: bound.max_ratio,
        status,
    }
}

/// What an unreadable run means, which depends on where it ran.
///
/// On a controlled host it is a failure with an action attached: the machine
/// is available, so re-run it idle. Shared CI has no idle host to re-run on,
/// so failing there reports nothing about the change and blocks a merge
/// request for the state of a runner — the whole defect #112 exists to
/// remove, and renaming FAIL to UNREADABLE would not have removed it. It is
/// the same trade the loopback load lane already makes, for the same reason,
/// and the message prints on every affected pipeline so a runner that is
/// unreadable forever stays visible rather than silently unguarded.
///
/// An unreadable run abstains from *measurement* verdicts only. `structural`
/// carries the failures that are never about the host — a benchmark the run
/// did not produce, or produced stale — and those still fail everywhere,
/// because a missing measurement is a configuration defect that a quiet
/// machine would not have fixed.
const fn unreadable_exit(mode: GateMode, structural: bool) -> u8 {
    match mode {
        GateMode::Full => UNTRUSTED_EXIT,
        GateMode::RatiosOnly if structural => 1,
        GateMode::RatiosOnly => 0,
    }
}

/// What an unreadable run should *say*, alongside what it returns.
///
/// The message is the only signal an operator gets from a run that declined to
/// judge, so which one prints is a decision and not formatting. Left inline in
/// `main` it was reachable by no test: mutation testing deleted the negation
/// guarding one and inverted the mode comparison guarding the other, and the
/// suite stayed green both times.
const fn unreadable_note(mode: GateMode, structural: bool) -> &'static str {
    match mode {
        _ if structural => {
            "perf-gate: FAIL — a benchmark is missing or stale, which no amount of quiet \
             would have fixed. That is not the host."
        }
        GateMode::RatiosOnly => {
            "perf-gate: no verdict (shared CI has no idle host to re-run on; every readable \
             ratio is in the report as evidence)"
        }
        GateMode::Full => "perf-gate: re-run on an idle host.",
    }
}

/// What to say when no ratio reached a verdict, or `None` when some did.
const fn no_verdict_note(reached_a_verdict: bool) -> Option<&'static str> {
    if reached_a_verdict {
        None
    } else {
        Some(
            "perf-gate: every ratio was inconclusive — this run measured nothing readable, \
             so it cannot stand as evidence. Re-run on an idle host.",
        )
    }
}

/// A failure the host cannot explain: the run did not produce a benchmark the
/// manifest requires, or produced one older than this run's freshness marker.
fn has_structural_failure(rows: &[ReportRow]) -> bool {
    rows.iter().any(|row| row.status == "missing-or-stale")
}

/// Whether the ratio rows produced any verdict at all.
///
/// Every ratio inconclusive means the run measured nothing readable, and a
/// gate that measured nothing must be red rather than quietly green — the same
/// reason `check_ci_rules.sh` exists. An empty manifest is not that case:
/// there was nothing to conclude.
fn ratios_reached_a_verdict(ratios: &[RatioRow]) -> bool {
    ratios.is_empty() || ratios.iter().any(|ratio| ratio.status != "inconclusive")
}

/// How far a measurement has moved from its previous value, as a fraction.
///
/// `None` when there is nothing to compare against, or when the previous
/// value was zero — a division there would yield an infinity that condemns
/// the run for no reason.
fn shift_from(mean_ns: f64, previous_ns: Option<f64>) -> Option<f64> {
    let before = previous_ns?;
    (before > 0.0).then(|| (mean_ns - before).abs() / before)
}

/// Criterion's confidence interval expressed relative to the mean, so a
/// 1 ns benchmark and a 3 µs one are comparable.
fn relative_ci_width(mean_ns: f64, interval: Option<ConfidenceInterval>) -> Option<f64> {
    let interval = interval?;
    (mean_ns > 0.0).then(|| (interval.upper_bound - interval.lower_bound) / mean_ns)
}

/// The three-way reading a human wants: comfortably inside, past the
/// aspiration but acceptable, or over the bound that fails.
fn verdict_label(mean_ns: f64, target_ns: f64, threshold_ns: f64) -> &'static str {
    if mean_ns <= target_ns {
        "within target"
    } else if mean_ns <= threshold_ns {
        "over target, within threshold"
    } else {
        "OVER THRESHOLD"
    }
}

/// Turn one benchmark's reading into its report row.
///
/// Pure, and separate from `main`, because this is where the gate decides
/// whether a benchmark passed — logic reachable only through file I/O is
/// logic no test can hold to account.
fn evaluate(
    entry: &Entry,
    measured: Option<Measurement>,
    previous_ns: Option<f64>,
    policy: &TrustPolicy,
    mode: GateMode,
) -> ReportRow {
    let status = match measured {
        None => "missing-or-stale",
        Some(_) if mode == GateMode::RatiosOnly => "pass",
        Some(m) if m.mean_ns <= entry.threshold_ns => "pass",
        Some(_) => "over-threshold",
    };
    ReportRow {
        id: entry.id.clone(),
        mean_ns: measured.map(|m| m.mean_ns),
        target_ns: entry.target_ns,
        threshold_ns: entry.threshold_ns,
        status,
        previous_ns,
        shift: measured.and_then(|m| shift_from(m.mean_ns, previous_ns)),
        ci_width: measured.and_then(|m| m.ci_width),
        unstable: measured
            .and_then(|m| m.ci_width)
            .is_some_and(|w| w > policy.max_ci_width),
    }
}

fn parse_args(args: &[String]) -> Result<Command, String> {
    let separator = args.iter().position(|arg| arg == "--");
    let option_end = separator.unwrap_or(args.len());
    for arg in &args[..option_end] {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            _ => {}
        }
    }

    let mut mode = GateMode::Full;
    let mut baseline_path = None;
    let mut positional = Vec::new();
    let mut index = 0;
    while index < option_end {
        match args[index].as_str() {
            "--ratios-only" if mode == GateMode::Full => mode = GateMode::RatiosOnly,
            "--ratios-only" => return Err("--ratios-only may be specified only once".to_owned()),
            "--baseline" if baseline_path.is_none() => {
                index += 1;
                if index >= option_end || args[index].starts_with('-') {
                    return Err("--baseline requires a path".to_owned());
                }
                baseline_path = Some(PathBuf::from(&args[index]));
            }
            "--baseline" => return Err("--baseline may be specified only once".to_owned()),
            value if value.starts_with('-') => return Err(format!("unknown option: {value}")),
            _ => positional.push(args[index].clone()),
        }
        index += 1;
    }
    if let Some(separator) = separator {
        positional.extend_from_slice(&args[separator + 1..]);
    }

    match positional.as_slice() {
        [manifest, criterion, report, marker] => Ok(Command::Run {
            manifest_path: PathBuf::from(manifest),
            criterion_root: PathBuf::from(criterion),
            report_path: PathBuf::from(report),
            marker_path: PathBuf::from(marker),
            baseline_path,
            mode,
        }),
        _ => Err(USAGE.to_owned()),
    }
}

fn baseline_entries(baseline: &Baseline) -> Result<BTreeMap<String, &BaselineEntry>, String> {
    if baseline.host.id.is_empty() {
        return Err("baseline host id must not be empty".to_owned());
    }
    if baseline.recorded_at.is_empty()
        || baseline.git_revision.is_empty()
        || baseline.profile.is_empty()
    {
        return Err("baseline metadata must not be empty".to_owned());
    }
    if baseline.benchmarks.is_empty() {
        return Err("baseline lists no benchmarks".to_owned());
    }
    let mut entries = BTreeMap::new();
    for entry in &baseline.benchmarks {
        if entry.id.is_empty()
            || !entry.mean_ns.is_finite()
            || entry.mean_ns <= 0.0
            || !entry.max_regression.is_finite()
            || entry.max_regression < 0.0
        {
            return Err(format!("invalid baseline entry for {:?}", entry.id));
        }
        if entries.insert(entry.id.clone(), entry).is_some() {
            return Err(format!("duplicate baseline entry for {}", entry.id));
        }
    }
    Ok(entries)
}

fn evaluate_regression(
    id: &str,
    measured_ns: Option<f64>,
    baseline: Option<&BaselineEntry>,
    enforced: bool,
) -> RegressionRow {
    let (baseline_ns, max_regression) = baseline
        .map(|entry| (Some(entry.mean_ns), Some(entry.max_regression)))
        .unwrap_or((None, None));
    // `baseline_entries` validated a positive denominator before this point.
    let ratio = measured_ns
        .zip(baseline_ns)
        .map(|(measured, baseline)| measured / baseline);
    let status = match (enforced, measured_ns, baseline) {
        (false, _, _) => "baseline-skipped",
        (true, _, None) => "no-baseline",
        (true, None, Some(_)) => "missing-measurement",
        (true, Some(measured), Some(entry))
            if measured <= entry.mean_ns * (1.0 + entry.max_regression) =>
        {
            "pass"
        }
        (true, Some(_), Some(_)) => "regressed",
    };
    RegressionRow {
        id: id.to_owned(),
        measured_ns,
        baseline_ns,
        ratio,
        max_regression,
        status,
    }
}

fn should_enforce_baseline(
    mode: GateMode,
    configured_host: Option<&str>,
    active_host: Option<&str>,
    untrusted: bool,
) -> bool {
    baseline_skip_reason(mode, configured_host, active_host, untrusted).is_none()
}

fn baseline_skip_reason(
    mode: GateMode,
    configured_host: Option<&str>,
    active_host: Option<&str>,
    untrusted: bool,
) -> Option<&'static str> {
    match (configured_host, mode, active_host, untrusted) {
        (None, _, _, _) => Some("no-baseline-file"),
        (Some(_), GateMode::RatiosOnly, _, _) => Some("ratios-only"),
        (Some(_), GateMode::Full, None, _) => Some("host-unset"),
        (Some(configured), GateMode::Full, Some(active), _) if configured != active => {
            Some("host-mismatch")
        }
        (Some(_), GateMode::Full, Some(_), true) => Some("untrusted-run"),
        (Some(_), GateMode::Full, Some(_), false) => None,
    }
}

#[derive(Serialize)]
struct ReportRow {
    id: String,
    mean_ns: Option<f64>,
    target_ns: f64,
    threshold_ns: f64,
    status: &'static str,
    /// This benchmark's value on the previous run, when there was one.
    previous_ns: Option<f64>,
    /// Relative change against `previous_ns`.
    shift: Option<f64>,
    ci_width: Option<f64>,
    /// The measurement's own spread was too wide to read, whatever the mean
    /// says.
    unstable: bool,
}

#[derive(Serialize)]
struct RatioRow {
    numerator: String,
    denominator: String,
    numerator_ns: Option<f64>,
    denominator_ns: Option<f64>,
    /// Which side, if either, the gate could not read — so the artifact
    /// explains an `inconclusive` without needing the console log beside it.
    numerator_unstable: bool,
    denominator_unstable: bool,
    ratio: Option<f64>,
    max_ratio: f64,
    status: &'static str,
}

#[derive(Serialize)]
struct RegressionRow {
    id: String,
    measured_ns: Option<f64>,
    baseline_ns: Option<f64>,
    ratio: Option<f64>,
    max_regression: Option<f64>,
    status: &'static str,
}

fn benchmark_line(
    mode: GateMode,
    entry: &Entry,
    measurement: Measurement,
    row: &ReportRow,
) -> String {
    let note = if row.unstable {
        " — UNSTABLE spread"
    } else {
        ""
    };
    match mode {
        GateMode::RatiosOnly => format!(
            "perf-gate: {}: mean {:.1} ns — absolute threshold skipped{note}",
            entry.id, measurement.mean_ns
        ),
        GateMode::Full => format!(
            "perf-gate: {}: mean {:.1} ns (target {:.1}, threshold {:.1}) — {}{note}",
            entry.id,
            measurement.mean_ns,
            entry.target_ns,
            entry.threshold_ns,
            verdict_label(measurement.mean_ns, entry.target_ns, entry.threshold_ns)
        ),
    }
}

fn regression_line(row: &RegressionRow) -> Option<String> {
    (row.status == "regressed").then(|| {
        format!(
            "perf-gate: {}: {:.1} ns / {:.1} ns baseline = x{:.3}, max x{:.3} — REGRESSED",
            row.id,
            row.measured_ns.unwrap_or_default(),
            row.baseline_ns.unwrap_or_default(),
            row.ratio.unwrap_or_default(),
            1.0 + row.max_regression.unwrap_or_default(),
        )
    })
}

#[derive(Serialize)]
struct BaselineReport {
    configured_host: Option<BaselineHost>,
    active_host: Option<String>,
    enforced: bool,
    skip_reason: Option<&'static str>,
}

/// What the machine looked like while measuring, so a suspect result can be
/// diagnosed after the fact instead of re-litigated.
#[derive(Serialize)]
struct RunContext {
    recorded_at_unix: u64,
    available_parallelism: Option<usize>,
    /// Captured by the wrapper script, which can read it portably; absent
    /// when the gate is invoked directly.
    load_average: Option<String>,
}

impl RunContext {
    fn capture() -> Self {
        RunContext {
            recorded_at_unix: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            available_parallelism: std::thread::available_parallelism().ok().map(Into::into),
            load_average: std::env::var("TOLLGATE_GATE_LOAD").ok(),
        }
    }
}

#[derive(Serialize)]
struct Report {
    rows: Vec<ReportRow>,
    ratios: Vec<RatioRow>,
    regressions: Vec<RegressionRow>,
    passed: bool,
    mode: GateMode,
    baseline: BaselineReport,
    trust: Trust,
    policy: TrustPolicy,
    run: RunContext,
}

/// Previous means, keyed by benchmark id.
#[derive(Serialize, Deserialize, Default)]
struct History {
    means: BTreeMap<String, f64>,
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("perf-gate: {msg}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (manifest_path, criterion_root, report_path, marker_path, baseline_path, mode) =
        match parse_args(&args) {
            Ok(Command::Run {
                manifest_path,
                criterion_root,
                report_path,
                marker_path,
                baseline_path,
                mode,
            }) => (
                manifest_path,
                criterion_root,
                report_path,
                marker_path,
                baseline_path,
                mode,
            ),
            Ok(Command::Help) => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            Ok(Command::Version) => {
                println!("check_benchmark_thresholds {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            Err(error) => return fail(&error),
        };
    // Beside the report, not beside the manifest: it is generated output that
    // describes this host, never a checked-in expectation.
    let history_path = report_path.with_file_name("perf_gate_history.json");

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

    let baseline: Option<Baseline> = match baseline_path.as_ref() {
        None => None,
        Some(path) => match std::fs::read_to_string(path)
            .map_err(|e| e.to_string())
            .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
        {
            Ok(baseline) => Some(baseline),
            Err(error) => {
                return fail(&format!("cannot read baseline {}: {error}", path.display()));
            }
        },
    };
    let baseline_by_id = match baseline.as_ref().map(baseline_entries).transpose() {
        Ok(entries) => entries,
        Err(error) => return fail(&format!("invalid baseline: {error}")),
    };

    let marker_mtime = match std::fs::metadata(&marker_path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(e) => {
            return fail(&format!(
                "cannot stat freshness marker {}: {e}",
                marker_path.display()
            ));
        }
    };

    let previous: BTreeMap<String, f64> = std::fs::read_to_string(&history_path)
        .ok()
        .and_then(|s| serde_json::from_str::<History>(&s).ok())
        .map(|h| h.means)
        .unwrap_or_default();

    let mut rows = Vec::new();
    let mut current = BTreeMap::new();
    for entry in &manifest.benchmarks {
        if entry.target_ns > entry.threshold_ns {
            eprintln!(
                "perf-gate: INVALID {}: target_ns {} exceeds threshold_ns {}",
                entry.id, entry.target_ns, entry.threshold_ns
            );
            rows.push(ReportRow {
                id: entry.id.clone(),
                mean_ns: None,
                target_ns: entry.target_ns,
                threshold_ns: entry.threshold_ns,
                status: "invalid-manifest",
                previous_ns: None,
                shift: None,
                ci_width: None,
                unstable: false,
            });
            continue;
        }
        let estimates_path: PathBuf = criterion_root
            .join(Path::new(&entry.id))
            .join("new/estimates.json");
        let measured = match read_estimates(&estimates_path, marker_mtime) {
            Ok(m) => Some(m),
            Err(e) => {
                eprintln!("perf-gate: {}: {e}", entry.id);
                None
            }
        };
        let previous_ns = previous.get(&entry.id).copied();
        let row = evaluate(entry, measured, previous_ns, &manifest.trust, mode);

        if let Some(m) = measured {
            current.insert(entry.id.clone(), m.mean_ns);
            println!("{}", benchmark_line(mode, entry, m, &row));
        }
        rows.push(row);
    }

    // The same instability the rows above already reported, now reaching the
    // verdicts computed from them (#112).
    let unstable: BTreeSet<String> = rows
        .iter()
        .filter(|row| row.unstable)
        .map(|row| row.id.clone())
        .collect();
    let ratios: Vec<_> = manifest
        .ratios
        .iter()
        .map(|bound| evaluate_ratio(bound, &current, &unstable))
        .collect();
    for ratio in &ratios {
        let because = match (ratio.numerator_unstable, ratio.denominator_unstable) {
            (true, true) => " — both sides too unstable to read",
            (true, false) => " — numerator too unstable to read",
            (false, true) => " — denominator too unstable to read",
            (false, false) => "",
        };
        match ratio.ratio {
            Some(measured) => println!(
                "perf-gate: {}/{}: ratio {:.2} (max {:.2}) — {}{}",
                ratio.numerator,
                ratio.denominator,
                measured,
                ratio.max_ratio,
                ratio.status,
                because
            ),
            None => eprintln!(
                "perf-gate: {}/{}: ratio unavailable — {}",
                ratio.numerator, ratio.denominator, ratio.status
            ),
        }
    }
    if let Some(note) = no_verdict_note(ratios_reached_a_verdict(&ratios)) {
        eprintln!("{note}");
    }
    let measured = rows.iter().filter(|row| row.mean_ns.is_some()).count();
    let trust = assess_trust(
        &current,
        &previous,
        unstable.len(),
        measured,
        &manifest.trust,
    );

    let active_host = std::env::var("TOLLGATE_PERF_HOST").ok();
    let untrusted = matches!(trust, Trust::Untrusted { .. });
    let baseline_enforced = should_enforce_baseline(
        mode,
        baseline.as_ref().map(|baseline| baseline.host.id.as_str()),
        active_host.as_deref(),
        untrusted,
    );
    let regressions: Vec<_> = manifest
        .benchmarks
        .iter()
        .map(|entry| {
            evaluate_regression(
                &entry.id,
                current.get(&entry.id).copied(),
                baseline_by_id
                    .as_ref()
                    .and_then(|entries| entries.get(&entry.id).copied()),
                baseline_enforced,
            )
        })
        .collect();
    for regression in &regressions {
        if let Some(line) = regression_line(regression) {
            eprintln!("{line}");
        }
    }
    let passed = all_results_passed(&rows, &ratios, &regressions);
    let skip_reason = baseline_skip_reason(
        mode,
        baseline.as_ref().map(|baseline| baseline.host.id.as_str()),
        active_host.as_deref(),
        untrusted,
    );

    let report = Report {
        rows,
        ratios,
        regressions,
        passed,
        mode,
        baseline: BaselineReport {
            configured_host: baseline.as_ref().map(|baseline| baseline.host.clone()),
            active_host,
            enforced: baseline_enforced,
            skip_reason,
        },
        trust: trust.clone(),
        policy: manifest.trust,
        run: RunContext::capture(),
    };
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

    if let Trust::Unreadable { unstable, measured } = trust {
        // History is not written from a run that could not read itself, for
        // the same reason UNTRUSTED does not write it.
        eprintln!(
            "perf-gate: UNREADABLE — {unstable} of {measured} benchmarks had a confidence \
             interval wider than {:.0}% of their own mean. That is the host, not the code: \
             a change makes one or two benchmarks noisy, a disturbed machine makes many. \
             No ratio from this run is evidence.",
            manifest.trust.max_ci_width * 100.0,
        );
        let structural = has_structural_failure(&report.rows);
        eprintln!("{}", unreadable_note(mode, structural));
        return ExitCode::from(unreadable_exit(mode, structural));
    }

    if let Trust::Untrusted { moved, compared } = trust {
        // Deliberately not recording history here: a contaminated run must
        // not become the yardstick the next one is measured against.
        eprintln!(
            "perf-gate: UNTRUSTED — {moved} of {compared} benchmarks moved more than \
             {:.0}% against the previous run on this host. A change moves one or two; \
             this looks like the machine, not the code. Re-run on an idle host. If the \
             host itself changed, delete {} to re-baseline.",
            manifest.trust.moved_shift * 100.0,
            history_path.display()
        );
        return ExitCode::from(UNTRUSTED_EXIT);
    }

    // Only a believable run is worth remembering.
    if let Err(e) = std::fs::write(
        &history_path,
        serde_json::to_string_pretty(&History { means: current }).expect("history serializes"),
    ) {
        eprintln!(
            "perf-gate: cannot write history {}: {e} (the next run has nothing to \
             compare against)",
            history_path.display()
        );
    }

    if passed {
        println!("perf-gate: PASS ({} benchmarks)", report.rows.len());
        ExitCode::SUCCESS
    } else {
        fail("FAIL — see report")
    }
}

fn read_estimates(path: &Path, marker_mtime: SystemTime) -> Result<Measurement, String> {
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
    let mean_ns = estimates.mean.point_estimate;
    Ok(Measurement {
        mean_ns,
        ci_width: relative_ci_width(mean_ns, estimates.mean.confidence_interval),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn means(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
        pairs
            .iter()
            .map(|(id, ns)| ((*id).to_string(), *ns))
            .collect()
    }

    /// The nine benchmarks the manifest actually gates, at their quiet-host
    /// values — the yardstick the cases below are measured against.
    fn quiet() -> BTreeMap<String, f64> {
        means(&[
            ("cost_table/quote", 1.7),
            ("snapshot/admit", 1.1),
            ("lease/reserve_commit", 37.7),
            ("lease/reserve_cancel", 37.0),
            ("admission/snapshot_lookup_arc_swap", 23.8),
            ("admission/snapshot_lookup_moka", 74.3),
            ("admission/full_check", 115.6),
            ("admission/full_check_contended_8", 2684.0),
            ("admission/full_check_denied", 20.3),
        ])
    }

    #[test]
    fn cli_handles_modes_baselines_help_version_and_separator() {
        assert_eq!(parse_args(&strings(&["bad", "--help"])), Ok(Command::Help));
        assert_eq!(parse_args(&strings(&["-V", "extra"])), Ok(Command::Version));
        assert_eq!(
            parse_args(&strings(&[
                "--ratios-only",
                "--baseline",
                "baseline.json",
                "manifest.json",
                "criterion",
                "report.json",
                "marker"
            ])),
            Ok(Command::Run {
                manifest_path: PathBuf::from("manifest.json"),
                criterion_root: PathBuf::from("criterion"),
                report_path: PathBuf::from("report.json"),
                marker_path: PathBuf::from("marker"),
                baseline_path: Some(PathBuf::from("baseline.json")),
                mode: GateMode::RatiosOnly,
            })
        );
        assert_eq!(
            parse_args(&strings(&[
                "--",
                "--manifest",
                "criterion",
                "report.json",
                "marker"
            ])),
            Ok(Command::Run {
                manifest_path: PathBuf::from("--manifest"),
                criterion_root: PathBuf::from("criterion"),
                report_path: PathBuf::from("report.json"),
                marker_path: PathBuf::from("marker"),
                baseline_path: None,
                mode: GateMode::Full,
            })
        );
    }

    #[test]
    fn cli_rejects_duplicate_or_incomplete_options() {
        assert_eq!(
            parse_args(&strings(&["--ratios-only", "--ratios-only"])),
            Err("--ratios-only may be specified only once".to_owned())
        );
        assert_eq!(
            parse_args(&strings(&["--baseline"])),
            Err("--baseline requires a path".to_owned())
        );
        assert_eq!(
            parse_args(&strings(&[
                "--baseline",
                "first.json",
                "--baseline",
                "second.json",
                "manifest.json",
                "criterion",
                "report.json",
                "marker"
            ])),
            Err("--baseline may be specified only once".to_owned())
        );
        assert_eq!(
            parse_args(&strings(&["--wat"])),
            Err("unknown option: --wat".to_owned())
        );
        assert_eq!(parse_args(&[]), Err(USAGE.to_owned()));
    }

    #[test]
    fn controlled_baseline_boundary_is_inclusive_and_defaults_to_five_percent() {
        let baseline: BaselineEntry =
            serde_json::from_str(r#"{"id":"admission/full_check","mean_ns":100.0}"#).unwrap();
        assert_eq!(baseline.max_regression, 0.05);
        let boundary = evaluate_regression(&baseline.id, Some(105.0), Some(&baseline), true);
        assert_eq!(boundary.baseline_ns, Some(100.0));
        assert_eq!(boundary.ratio, Some(1.05));
        assert_eq!(boundary.max_regression, Some(0.05));
        assert_eq!(boundary.status, "pass");
        assert_eq!(
            evaluate_regression(&baseline.id, Some(105.000_1), Some(&baseline), true).status,
            "regressed"
        );
    }

    #[test]
    fn checked_in_baseline_is_a_valid_subset_with_owned_rows_recorded() {
        let manifest: Manifest =
            serde_json::from_str(include_str!("../../../testing/perf_thresholds.json")).unwrap();
        let baseline: Baseline =
            serde_json::from_str(include_str!("../../../testing/perf_baseline.json")).unwrap();
        let entries = baseline_entries(&baseline).unwrap();
        let manifest_ids: std::collections::BTreeSet<_> =
            manifest.benchmarks.iter().map(|entry| &entry.id).collect();

        assert_eq!(baseline.host.id, "mistral-apple-m1-pro");
        assert!(
            entries.keys().all(|id| manifest_ids.contains(id)),
            "a baseline row must name a benchmark the gate still runs"
        );
        for owned in [
            "admission/request_rate_token",
            "admission/concurrency_acquire",
        ] {
            assert!(
                entries.contains_key(owned),
                "the owner that activates {owned} must record its controlled-host baseline"
            );
        }
        assert!(
            entries.len() < manifest.benchmarks.len(),
            "not every historical benchmark has a controlled-host recording yet"
        );
    }

    #[test]
    fn invalid_or_duplicate_baseline_rows_are_rejected() {
        let parse = |benchmarks| Baseline {
            host: BaselineHost {
                id: "host".to_owned(),
                architecture: "arch".to_owned(),
                cpu: "cpu".to_owned(),
                os: "os".to_owned(),
                rustc: "rustc".to_owned(),
            },
            recorded_at: "time".to_owned(),
            git_revision: "revision".to_owned(),
            profile: "release".to_owned(),
            benchmarks,
        };
        let entry = || BaselineEntry {
            id: "a".to_owned(),
            mean_ns: 100.0,
            max_regression: 0.05,
        };
        assert!(baseline_entries(&parse(vec![entry()])).is_ok());
        assert!(baseline_entries(&parse(vec![entry(), entry()])).is_err());

        let mut missing_host = parse(vec![entry()]);
        missing_host.host.id.clear();
        assert!(baseline_entries(&missing_host).is_err());
        let mut missing_recorded_at = parse(vec![entry()]);
        missing_recorded_at.recorded_at.clear();
        assert!(baseline_entries(&missing_recorded_at).is_err());
        let mut missing_revision = parse(vec![entry()]);
        missing_revision.git_revision.clear();
        assert!(baseline_entries(&missing_revision).is_err());
        let mut missing_profile = parse(vec![entry()]);
        missing_profile.profile.clear();
        assert!(baseline_entries(&missing_profile).is_err());
        assert!(baseline_entries(&parse(Vec::new())).is_err());

        let mut invalid = entry();
        invalid.id.clear();
        assert!(baseline_entries(&parse(vec![invalid])).is_err());
        let mut invalid = entry();
        invalid.mean_ns = 0.0;
        assert!(baseline_entries(&parse(vec![invalid])).is_err());
        let mut invalid = entry();
        invalid.mean_ns = f64::NAN;
        assert!(baseline_entries(&parse(vec![invalid])).is_err());
        let mut invalid = entry();
        invalid.max_regression = -0.01;
        assert!(baseline_entries(&parse(vec![invalid])).is_err());
        let mut invalid = entry();
        invalid.max_regression = f64::INFINITY;
        assert!(baseline_entries(&parse(vec![invalid])).is_err());

        let mut zero_regression = entry();
        zero_regression.max_regression = 0.0;
        assert!(baseline_entries(&parse(vec![zero_regression])).is_ok());
    }

    #[test]
    fn host_mismatch_untrusted_and_ratio_only_runs_skip_the_baseline() {
        assert!(should_enforce_baseline(
            GateMode::Full,
            Some("controlled"),
            Some("controlled"),
            false
        ));
        assert!(!should_enforce_baseline(
            GateMode::Full,
            Some("controlled"),
            Some("another-host"),
            false
        ));
        assert!(!should_enforce_baseline(
            GateMode::Full,
            Some("controlled"),
            Some("controlled"),
            true
        ));
        assert!(!should_enforce_baseline(
            GateMode::RatiosOnly,
            Some("controlled"),
            Some("controlled"),
            false
        ));

        let entry = BaselineEntry {
            id: "a".to_owned(),
            mean_ns: 100.0,
            max_regression: 0.05,
        };
        assert_eq!(
            evaluate_regression("a", Some(1_000.0), Some(&entry), false).status,
            "baseline-skipped"
        );
        assert_eq!(
            baseline_skip_reason(GateMode::Full, None, Some("controlled"), false),
            Some("no-baseline-file")
        );
        assert_eq!(
            baseline_skip_reason(
                GateMode::RatiosOnly,
                Some("controlled"),
                Some("controlled"),
                false
            ),
            Some("ratios-only")
        );
        assert_eq!(
            baseline_skip_reason(GateMode::Full, Some("controlled"), None, false),
            Some("host-unset")
        );
        assert_eq!(
            baseline_skip_reason(
                GateMode::Full,
                Some("controlled"),
                Some("another-host"),
                false
            ),
            Some("host-mismatch")
        );
        assert_eq!(
            baseline_skip_reason(GateMode::Full, Some("controlled"), Some("controlled"), true),
            Some("untrusted-run")
        );
        assert_eq!(
            baseline_skip_reason(
                GateMode::Full,
                Some("controlled"),
                Some("controlled"),
                false
            ),
            None
        );
    }

    #[test]
    fn human_lines_distinguish_gate_modes_instability_and_regressions() {
        let policy = TrustPolicy::default();
        let measurement = Measurement {
            mean_ns: 120.0,
            ci_width: Some(0.01),
        };
        let stable = evaluate(&entry(), Some(measurement), None, &policy, GateMode::Full);
        let full = benchmark_line(GateMode::Full, &entry(), measurement, &stable);
        let ratios = benchmark_line(GateMode::RatiosOnly, &entry(), measurement, &stable);
        assert!(full.contains("target 110.0, threshold 250.0"));
        assert!(!full.contains("absolute threshold skipped"));
        assert!(ratios.contains("absolute threshold skipped"));

        let unstable_measurement = Measurement {
            mean_ns: 120.0,
            ci_width: Some(0.50),
        };
        let unstable = evaluate(
            &entry(),
            Some(unstable_measurement),
            None,
            &policy,
            GateMode::Full,
        );
        assert!(
            benchmark_line(GateMode::Full, &entry(), unstable_measurement, &unstable)
                .contains("UNSTABLE spread")
        );

        let baseline = BaselineEntry {
            id: entry().id,
            mean_ns: 100.0,
            max_regression: 0.05,
        };
        let passing = evaluate_regression(&baseline.id, Some(104.0), Some(&baseline), true);
        let regressed = evaluate_regression(&baseline.id, Some(106.0), Some(&baseline), true);
        assert!(regression_line(&passing).is_none());
        assert!(regression_line(&regressed).unwrap().contains("REGRESSED"));
    }

    #[test]
    fn ratio_only_mode_still_requires_fresh_rows_but_ignores_absolute_bounds() {
        let policy = TrustPolicy::default();
        assert_eq!(
            evaluate(
                &entry(),
                measured(9_999.0, None),
                None,
                &policy,
                GateMode::RatiosOnly
            )
            .status,
            "pass"
        );
        assert_eq!(
            evaluate(&entry(), None, None, &policy, GateMode::RatiosOnly).status,
            "missing-or-stale"
        );
    }

    /// !40, reproduced: every `tollgate-core` benchmark inflated ~2.6x while
    /// the admission ones held steady, on a branch that touched neither. The
    /// gate reported FAIL on two lease benchmarks; a human had to notice the
    /// affected benchmarks were unrelated to the change.
    #[test]
    fn a_run_where_one_crate_inflated_together_is_untrusted() {
        let mut contaminated = quiet();
        for id in [
            "cost_table/quote",
            "snapshot/admit",
            "lease/reserve_commit",
            "lease/reserve_cancel",
        ] {
            *contaminated.get_mut(id).unwrap() *= 2.6;
        }

        assert_eq!(
            assess_trust(
                &contaminated,
                &quiet(),
                0,
                contaminated.len(),
                &TrustPolicy::default()
            ),
            Trust::Untrusted {
                moved: 4,
                compared: 9
            }
        );
    }

    /// !48, reproduced: the *baseline* was the outlier, at 138.9 ns against a
    /// 113-117 ns quiet band, which made a later comparison read as a 16%
    /// improvement. Contamination is symmetric — a run that is uniformly
    /// faster than history is just as suspect as one uniformly slower.
    #[test]
    fn a_run_that_is_uniformly_faster_is_equally_untrusted() {
        let mut fast = quiet();
        for value in fast.values_mut() {
            *value *= 0.5;
        }

        assert!(matches!(
            assess_trust(&fast, &quiet(), 0, fast.len(), &TrustPolicy::default()),
            Trust::Untrusted { .. }
        ));
    }

    /// The case that matters more than the others: a trust check that
    /// swallows real regressions is worse than no trust check. One benchmark
    /// genuinely slower must leave the run trusted, so the ordinary FAIL path
    /// still reports it.
    #[test]
    fn a_single_genuine_regression_stays_trusted() {
        let mut regressed = quiet();
        *regressed.get_mut("admission/full_check").unwrap() = 400.0;

        assert_eq!(
            assess_trust(
                &regressed,
                &quiet(),
                0,
                regressed.len(),
                &TrustPolicy::default()
            ),
            Trust::Trusted {
                moved: 1,
                compared: 9
            }
        );
    }

    /// Two related benchmarks moving together is still a change, not a
    /// contaminated host — a lease-counter change moves both lease
    /// benchmarks and nothing else.
    #[test]
    fn two_related_benchmarks_moving_stays_trusted() {
        let mut regressed = quiet();
        *regressed.get_mut("lease/reserve_commit").unwrap() = 90.0;
        *regressed.get_mut("lease/reserve_cancel").unwrap() = 88.0;

        assert!(matches!(
            assess_trust(
                &regressed,
                &quiet(),
                0,
                regressed.len(),
                &TrustPolicy::default()
            ),
            Trust::Trusted { .. }
        ));
    }

    /// The changes actually shipped this week nudged several admission
    /// benchmarks by a percent or two. None of that may read as
    /// contamination, or the check would cry wolf on every real MR.
    #[test]
    fn small_broad_movement_is_normal_and_stays_trusted() {
        let mut nudged = quiet();
        for value in nudged.values_mut() {
            *value *= 1.02;
        }

        assert!(matches!(
            assess_trust(&nudged, &quiet(), 0, nudged.len(), &TrustPolicy::default()),
            Trust::Trusted { .. }
        ));
    }

    /// A first run, or any CI job, has nothing to compare against and must
    /// behave exactly as the gate did before this existed.
    #[test]
    fn without_history_there_is_no_trust_verdict() {
        assert_eq!(
            assess_trust(
                &quiet(),
                &BTreeMap::new(),
                0,
                quiet().len(),
                &TrustPolicy::default()
            ),
            Trust::NoHistory
        );
        // A history for entirely different benchmark ids is no history at all.
        assert_eq!(
            assess_trust(
                &quiet(),
                &means(&[("renamed/bench", 5.0)]),
                0,
                quiet().len(),
                &TrustPolicy::default()
            ),
            Trust::NoHistory
        );
    }

    /// With one or two benchmarks in common, a single genuine change is a
    /// large enough fraction to trip the plurality rule. It must not.
    #[test]
    fn one_moved_benchmark_never_condemns_a_small_run() {
        let before = means(&[("a", 10.0), ("b", 20.0)]);
        let after = means(&[("a", 100.0), ("b", 20.0)]);

        assert_eq!(
            assess_trust(&after, &before, 0, after.len(), &TrustPolicy::default()),
            Trust::Trusted {
                moved: 1,
                compared: 2
            }
        );
    }

    /// A previous value of zero cannot yield a relative shift; it is skipped
    /// rather than producing an infinity that would condemn the run.
    #[test]
    fn a_zero_previous_value_is_skipped_not_divided_by() {
        let before = means(&[("a", 0.0), ("b", 20.0)]);
        let after = means(&[("a", 10.0), ("b", 20.0)]);

        assert_eq!(
            assess_trust(&after, &before, 0, after.len(), &TrustPolicy::default()),
            Trust::Trusted {
                moved: 0,
                compared: 1
            }
        );
    }

    /// The manifest may omit the trust block entirely; the existing manifest
    /// does, and must keep working.
    #[test]
    fn a_manifest_without_a_trust_block_uses_the_defaults() {
        let manifest: Manifest = serde_json::from_str(
            r#"{"benchmarks":[{"id":"a","target_ns":1.0,"threshold_ns":2.0}]}"#,
        )
        .expect("a manifest without a trust block parses");

        assert_eq!(manifest.trust.moved_shift, default_shift());
        assert_eq!(manifest.trust.moved_fraction, default_fraction());
        assert_eq!(manifest.trust.max_ci_width, default_ci_width());
    }

    /// Both tolerances are exclusive: landing exactly on the line is inside
    /// it. Worth pinning, because "40%" reads the same either way in prose
    /// and only the code says which.
    #[test]
    fn a_value_exactly_on_a_tolerance_is_within_it() {
        let policy = TrustPolicy::default();

        // Exactly the shift tolerance, on every benchmark: not "moved".
        let before = means(&[("a", 100.0), ("b", 100.0), ("c", 100.0)]);
        let exactly = means(&[("a", 140.0), ("b", 140.0), ("c", 140.0)]);
        assert_eq!(
            assess_trust(&exactly, &before, 0, exactly.len(), &policy),
            Trust::Trusted {
                moved: 0,
                compared: 3
            }
        );
        // A hair past it, and the same run is condemned.
        let past = means(&[("a", 141.0), ("b", 141.0), ("c", 141.0)]);
        assert!(matches!(
            assess_trust(&past, &before, 0, past.len(), &policy),
            Trust::Untrusted { .. }
        ));

        // Exactly the interval-width tolerance: not "unstable".
        assert!(
            !evaluate(
                &entry(),
                measured(120.0, Some(0.10)),
                None,
                &policy,
                GateMode::Full
            )
            .unstable
        );
        assert!(
            evaluate(
                &entry(),
                measured(120.0, Some(0.11)),
                None,
                &policy,
                GateMode::Full
            )
            .unstable
        );
    }

    /// One bad row is enough to fail the run, and only "pass" counts as good
    /// — a missing benchmark must not be mistaken for an acceptable one.
    #[test]
    fn the_run_passes_only_when_every_row_did() {
        let policy = TrustPolicy::default();
        let good = evaluate(
            &entry(),
            measured(120.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        let slow = evaluate(
            &entry(),
            measured(9_999.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        let absent = evaluate(&entry(), None, None, &policy, GateMode::Full);

        assert!(all_passed(std::slice::from_ref(&good)));
        assert!(!all_passed(&[good, slow]));
        assert!(!all_passed(&[absent]));
        assert!(all_passed(&[]), "nothing to fail");
    }

    /// No measurement in the run was too unstable to read — the ordinary
    /// case, named so the ratio tests below say which world they are in.
    fn steady() -> BTreeSet<String> {
        BTreeSet::new()
    }

    /// Shared CI can never reach the shift-based verdict, so instability
    /// breadth is the one it can.
    ///
    /// #112: every CI job starts from a clean workspace, so `previous` is
    /// always empty there and `assess_trust` answered `NoHistory` however
    /// contaminated the run was. Calibrated on three observed runs of one
    /// unchanged tree — a clean pipeline had 0 of 39 benchmarks individually
    /// unreadable, the two that reported false ratio failures had 8 of 33 and
    /// 6 of 33.
    #[test]
    fn a_widely_unreadable_run_is_untrusted_even_with_no_history() {
        let policy = TrustPolicy::default();
        let nothing = BTreeMap::new();

        assert!(matches!(
            assess_trust(&nothing, &nothing, 8, 33, &policy),
            Trust::Unreadable {
                unstable: 8,
                measured: 33
            }
        ));
        assert!(matches!(
            assess_trust(&nothing, &nothing, 6, 33, &policy),
            Trust::Unreadable { .. }
        ));
        // The clean run stays believable, and so does the quiet host that
        // marks the odd contention benchmark.
        assert!(matches!(
            assess_trust(&nothing, &nothing, 0, 39, &policy),
            Trust::NoHistory
        ));
        assert!(matches!(
            assess_trust(&nothing, &nothing, 2, 39, &policy),
            Trust::NoHistory
        ));
        // One unreadable benchmark is never a verdict about the machine, at
        // any population size — the same degenerate-case guard the shift rule
        // carries.
        assert!(matches!(
            assess_trust(&nothing, &nothing, 1, 2, &policy),
            Trust::NoHistory
        ));
        // And it outranks the shift rule: a run that cannot read itself is
        // not made readable by having history to compare against.
        let quiet_history = means(&[("a", 100.0), ("b", 100.0)]);
        assert!(matches!(
            assess_trust(&quiet_history, &quiet_history, 8, 33, &policy),
            Trust::Unreadable { .. }
        ));
    }

    /// A run that measured nothing is not an unreadable run.
    ///
    /// The `measured > 0` guard decides that, and mutation testing showed it
    /// was doing nothing a test could see: dividing by zero yields NaN or an
    /// infinity, and both compare their way past the fraction, condemning a
    /// run whose real problem is that it produced no benchmarks at all — which
    /// the missing/stale rows already report, for a reason no quiet host
    /// fixes.
    #[test]
    fn a_run_with_no_measurements_is_not_blamed_on_the_host() {
        let policy = TrustPolicy::default();
        let nothing = BTreeMap::new();
        assert!(matches!(
            assess_trust(&nothing, &nothing, 2, 0, &policy),
            Trust::NoHistory
        ));
        // And one benchmark measured, with more claimed unstable than exist,
        // still is not a fraction anybody can read.
        assert!(matches!(
            assess_trust(&nothing, &nothing, 2, 1, &policy),
            Trust::Unreadable { .. }
        ));
    }

    /// The message an unreadable run prints is a decision, not formatting.
    ///
    /// It is the only signal an operator gets from a run that declined to
    /// judge, and inline in `main` no test could reach it — mutation testing
    /// deleted one guard and inverted the other with the suite still green.
    #[test]
    fn an_unreadable_run_says_which_kind_of_unreadable_it_was() {
        // A structural failure is named as not-the-host, in either mode,
        // because that is the one an operator must not wait out.
        for mode in [GateMode::Full, GateMode::RatiosOnly] {
            let note = unreadable_note(mode, true);
            assert!(note.contains("missing or stale"), "{mode:?}: {note}");
            assert!(note.contains("not the host"), "{mode:?}: {note}");
        }

        // Shared CI says it drew no verdict and points at the report.
        let abstained = unreadable_note(GateMode::RatiosOnly, false);
        assert!(abstained.contains("no verdict"));
        assert!(abstained.contains("evidence"));
        assert!(!abstained.contains("missing or stale"));

        // A controlled host says what to do instead, because it can.
        let controlled = unreadable_note(GateMode::Full, false);
        assert!(controlled.contains("idle host"));
        assert_ne!(controlled, abstained);
    }

    /// The no-verdict note appears exactly when there was no verdict.
    #[test]
    fn a_run_that_concluded_something_says_nothing_about_concluding_nothing() {
        assert_eq!(no_verdict_note(true), None);
        let note = no_verdict_note(false).expect("a run with no verdict must say so");
        assert!(note.contains("every ratio was inconclusive"));
    }

    /// An unreadable run abstains where abstaining is the only honest answer,
    /// and never where the failure is not the host's fault.
    ///
    /// Renaming FAIL to UNREADABLE would have left #112 exactly where it was:
    /// shared CI has no idle host to re-run on, so a non-zero exit there still
    /// blocks a merge request for the state of a runner. A controlled host
    /// does have one, and there the action is to use it.
    #[test]
    fn an_unreadable_run_abstains_on_shared_ci_and_fails_on_a_controlled_host() {
        assert_eq!(unreadable_exit(GateMode::RatiosOnly, false), 0);
        assert_eq!(unreadable_exit(GateMode::Full, false), UNTRUSTED_EXIT);

        // A missing or stale benchmark is a configuration defect. No amount of
        // quiet would have fixed it, so it fails in both modes — the freshness
        // marker exists precisely so this cannot pass unnoticed.
        assert_ne!(unreadable_exit(GateMode::RatiosOnly, true), 0);
        assert_eq!(unreadable_exit(GateMode::Full, true), UNTRUSTED_EXIT);

        let row = |status| ReportRow {
            id: "admission/full_check".to_owned(),
            mean_ns: Some(100.0),
            target_ns: 100.0,
            threshold_ns: 200.0,
            status,
            previous_ns: None,
            shift: None,
            ci_width: None,
            unstable: false,
        };
        assert!(!has_structural_failure(&[row("pass")]));
        assert!(has_structural_failure(&[
            row("pass"),
            row("missing-or-stale")
        ]));
        // An over-threshold row is a measurement verdict, not a structural
        // one: it is exactly what an unreadable run must not be trusted to
        // pronounce.
        assert!(!has_structural_failure(&[row("over-threshold")]));
    }

    #[test]
    fn a_same_run_ratio_enforces_the_portable_contention_budget() {
        let bound = RatioBound {
            numerator: "contended".to_string(),
            denominator: "uncontended".to_string(),
            max_ratio: 3.0,
        };
        let within = means(&[("contended", 300.0), ("uncontended", 100.0)]);
        let over = means(&[("contended", 301.0), ("uncontended", 100.0)]);

        assert_eq!(evaluate_ratio(&bound, &within, &steady()).status, "pass");
        assert_eq!(
            evaluate_ratio(&bound, &over, &steady()).status,
            "over-ratio"
        );
        assert_eq!(
            evaluate_ratio(&bound, &BTreeMap::new(), &steady()).status,
            "missing-or-zero-denominator"
        );
        for denominator in [0.0, -1.0] {
            let unusable = means(&[("contended", 300.0), ("uncontended", denominator)]);
            let row = evaluate_ratio(&bound, &unusable, &steady());
            assert_eq!(row.status, "missing-or-zero-denominator");
            assert_eq!(row.ratio, None);
        }

        // A ratio ceiling is a contract, not a switch. Zero and negative
        // values are invalid even when the measured ratio is also zero; they
        // must never become an accidental pass or an ordinary threshold miss.
        let zero_measurement = means(&[("contended", 0.0), ("uncontended", 100.0)]);
        for max_ratio in [0.0, -1.0] {
            let invalid = RatioBound {
                numerator: bound.numerator.clone(),
                denominator: bound.denominator.clone(),
                max_ratio,
            };
            assert_eq!(
                evaluate_ratio(&invalid, &zero_measurement, &steady()).status,
                "invalid-manifest"
            );
            assert_eq!(
                evaluate_ratio(&invalid, &over, &steady()).status,
                "invalid-manifest"
            );
        }
    }

    /// A ratio computed from a measurement the gate itself called unreadable
    /// is not a verdict, in either direction.
    ///
    /// This is #112. Two pipelines on the same shared runner measured the same
    /// unchanged code at ×3.40 and ×13.11, and the second failed a merge
    /// request whose diff could not touch the mechanism — because the gate
    /// printed "UNSTABLE spread" for one side and then divided it anyway.
    #[test]
    fn an_unstable_side_makes_a_ratio_inconclusive_rather_than_a_verdict() {
        let bound = RatioBound {
            numerator: "contended".to_string(),
            denominator: "uncontended".to_string(),
            max_ratio: 3.0,
        };
        let over = means(&[("contended", 1300.0), ("uncontended", 100.0)]);
        let within = means(&[("contended", 100.0), ("uncontended", 100.0)]);
        let unstable = |id: &str| BTreeSet::from([id.to_string()]);

        // Either side, and both, withdraw the verdict.
        for shaky in [
            unstable("contended"),
            unstable("uncontended"),
            BTreeSet::from(["contended".to_string(), "uncontended".to_string()]),
        ] {
            assert_eq!(
                evaluate_ratio(&bound, &over, &shaky).status,
                "inconclusive",
                "an unreadable measurement must not condemn the code"
            );
            // And the direction nobody notices: an unstable measurement that
            // lands under the ceiling is no more readable than one over it.
            assert_eq!(
                evaluate_ratio(&bound, &within, &shaky).status,
                "inconclusive",
                "an unreadable measurement must not clear the code either"
            );
        }

        // The row carries which side, so the artifact explains itself.
        let row = evaluate_ratio(&bound, &over, &unstable("uncontended"));
        assert!(!row.numerator_unstable && row.denominator_unstable);
        assert_eq!(row.ratio, Some(13.0), "the measurement is still reported");

        // An unrelated benchmark being unstable changes nothing.
        assert_eq!(
            evaluate_ratio(&bound, &over, &unstable("something/else")).status,
            "over-ratio"
        );
    }

    /// Inconclusive is survivable; a run where *every* ratio is inconclusive
    /// is not. A gate that measured nothing must be red rather than quietly
    /// green.
    #[test]
    fn a_run_that_concluded_nothing_is_not_a_pass() {
        let row = |status| RatioRow {
            numerator: "contended".to_string(),
            denominator: "uncontended".to_string(),
            numerator_ns: Some(100.0),
            denominator_ns: Some(100.0),
            numerator_unstable: status == "inconclusive",
            denominator_unstable: false,
            ratio: Some(1.0),
            max_ratio: 3.0,
            status,
        };

        assert!(ratios_reached_a_verdict(&[]), "nothing to conclude");
        assert!(ratios_reached_a_verdict(&[row("pass")]));
        assert!(ratios_reached_a_verdict(&[
            row("inconclusive"),
            row("pass")
        ]));
        assert!(!ratios_reached_a_verdict(&[
            row("inconclusive"),
            row("inconclusive")
        ]));

        // And the aggregate agrees on both halves.
        assert!(all_results_passed(
            &[],
            &[row("inconclusive"), row("pass")],
            &[]
        ));
        assert!(!all_results_passed(&[], &[row("inconclusive")], &[]));
        assert!(!all_results_passed(&[], &[row("over-ratio")], &[]));
    }

    #[test]
    fn absolute_and_ratio_verdicts_must_both_pass() {
        let policy = TrustPolicy::default();
        let passing_row = evaluate(
            &entry(),
            measured(120.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        let failing_row = evaluate(
            &entry(),
            measured(9_999.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        let ratio = |status| RatioRow {
            numerator: "contended".to_string(),
            denominator: "uncontended".to_string(),
            numerator_ns: Some(100.0),
            denominator_ns: Some(100.0),
            numerator_unstable: false,
            denominator_unstable: false,
            ratio: Some(1.0),
            max_ratio: 3.0,
            status,
        };
        let regression = |status| RegressionRow {
            id: "admission/full_check".to_owned(),
            measured_ns: Some(120.0),
            baseline_ns: Some(100.0),
            ratio: Some(1.2),
            max_regression: Some(0.05),
            status,
        };

        assert!(all_results_passed(&[passing_row], &[ratio("pass")], &[]));
        let passing_row = evaluate(
            &entry(),
            measured(120.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        assert!(!all_results_passed(&[failing_row], &[ratio("pass")], &[]));
        assert!(!all_results_passed(
            &[passing_row],
            &[ratio("over-ratio")],
            &[]
        ));
        assert!(!all_results_passed(
            &[evaluate(
                &entry(),
                measured(120.0, None),
                None,
                &policy,
                GateMode::Full,
            )],
            &[ratio("pass")],
            &[regression("regressed")]
        ));
    }

    fn entry() -> Entry {
        Entry {
            id: "admission/full_check".to_string(),
            target_ns: 110.0,
            threshold_ns: 250.0,
        }
    }

    fn measured(mean_ns: f64, ci_width: Option<f64>) -> Option<Measurement> {
        Some(Measurement { mean_ns, ci_width })
    }

    /// The bound that fails is `threshold_ns`, and it is inclusive — a mean
    /// exactly on it passes, as it always has.
    #[test]
    fn the_threshold_is_the_failing_bound_and_includes_its_own_value() {
        let policy = TrustPolicy::default();
        assert_eq!(
            evaluate(
                &entry(),
                measured(250.0, None),
                None,
                &policy,
                GateMode::Full
            )
            .status,
            "pass"
        );
        assert_eq!(
            evaluate(
                &entry(),
                measured(250.1, None),
                None,
                &policy,
                GateMode::Full
            )
            .status,
            "over-threshold"
        );
        assert_eq!(
            evaluate(&entry(), None, None, &policy, GateMode::Full).status,
            "missing-or-stale"
        );
    }

    /// `target_ns` is aspirational and must never decide pass or fail — only
    /// what the line printed to a human says.
    #[test]
    fn the_target_colours_the_reading_but_never_the_verdict() {
        let policy = TrustPolicy::default();
        let over_target = evaluate(
            &entry(),
            measured(200.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        assert_eq!(over_target.status, "pass", "aspiration is not a bound");

        assert_eq!(verdict_label(100.0, 110.0, 250.0), "within target");
        assert_eq!(
            verdict_label(200.0, 110.0, 250.0),
            "over target, within threshold"
        );
        assert_eq!(verdict_label(300.0, 110.0, 250.0), "OVER THRESHOLD");
        // Both boundaries are inclusive on the kinder side.
        assert_eq!(verdict_label(110.0, 110.0, 250.0), "within target");
        assert_eq!(
            verdict_label(250.0, 110.0, 250.0),
            "over target, within threshold"
        );
    }

    /// A row records how far it moved, in either direction, as a magnitude.
    #[test]
    fn a_rows_shift_is_a_magnitude_against_its_previous_value() {
        assert_eq!(shift_from(110.0, Some(100.0)), Some(0.1));
        assert_eq!(shift_from(90.0, Some(100.0)), Some(0.1));
        assert_eq!(shift_from(100.0, Some(100.0)), Some(0.0));
        assert_eq!(shift_from(100.0, None), None, "nothing to compare against");
        assert_eq!(
            shift_from(100.0, Some(0.0)),
            None,
            "a zero previous value would divide to infinity"
        );
    }

    /// An unstable measurement is flagged but does not by itself fail the
    /// run: the mean may still be perfectly acceptable, and the point is to
    /// tell the reader the number wobbled.
    #[test]
    fn a_wide_interval_marks_a_row_unstable_without_failing_it() {
        let policy = TrustPolicy::default();
        let wobbly = evaluate(
            &entry(),
            measured(120.0, Some(0.5)),
            None,
            &policy,
            GateMode::Full,
        );
        assert!(wobbly.unstable);
        assert_eq!(wobbly.status, "pass");

        let steady = evaluate(
            &entry(),
            measured(120.0, Some(0.01)),
            None,
            &policy,
            GateMode::Full,
        );
        assert!(!steady.unstable);

        let unknown = evaluate(
            &entry(),
            measured(120.0, None),
            None,
            &policy,
            GateMode::Full,
        );
        assert!(
            !unknown.unstable,
            "a missing interval is unknown, not unstable"
        );
    }

    /// The confidence interval criterion already writes is read straight
    /// through; a wide one marks the row unstable without any new
    /// measurement.
    #[test]
    fn the_confidence_interval_becomes_a_relative_width() {
        let estimates: Estimates = serde_json::from_str(
            r#"{"mean":{"point_estimate":100.0,
                "confidence_interval":{"lower_bound":95.0,"upper_bound":105.0}}}"#,
        )
        .expect("criterion estimates parse");
        let width = relative_ci_width(
            estimates.mean.point_estimate,
            estimates.mean.confidence_interval,
        );

        assert_eq!(width, Some(0.10));
        // Relative, so a 3 µs benchmark and a 1 ns one are comparable.
        assert_eq!(
            relative_ci_width(
                2_000.0,
                Some(ConfidenceInterval {
                    lower_bound: 1_900.0,
                    upper_bound: 2_100.0
                })
            ),
            Some(0.10)
        );
        assert_eq!(
            relative_ci_width(
                0.0,
                Some(ConfidenceInterval {
                    lower_bound: 0.0,
                    upper_bound: 1.0
                })
            ),
            None,
            "a zero mean has no meaningful relative width"
        );
    }

    /// Older criterion output, or a hand-written fixture, may carry no
    /// interval; that is missing information, not instability.
    #[test]
    fn a_missing_confidence_interval_is_not_instability() {
        let estimates: Estimates =
            serde_json::from_str(r#"{"mean":{"point_estimate":100.0}}"#).expect("parses");
        assert!(estimates.mean.confidence_interval.is_none());
    }
}
