//! Compares Criterion benchmark means against a threshold manifest.
//!
//! Usage:
//!
//! ```text
//! check_benchmark_thresholds [--ratios-only] [--baseline <baseline.json>]
//!   [--samples <dir>] [--record <baseline.json>] [--history <history.json>]
//!   <manifest.json> <criterion-root> <report.json> <freshness-marker>
//! ```
//!
//! Gate semantics:
//! - `target_ns` is aspirational, `threshold_ns` is the failing bound;
//!   a manifest with `target_ns > threshold_ns` is itself invalid.
//! - Each benchmark id maps to `<criterion-root>/<id>/new/estimates.json`,
//!   whose `mean.point_estimate` (nanoseconds) is compared to `threshold_ns`.
//! - Optional ratio bounds compare two measurements from the same run, so a
//!   contention budget is portable across otherwise different hosts.
//! - An optional recorded baseline enforces per-row regressions only when
//!   `TOLLGATE_PERF_HOST` matches its host id and the run is trusted. A full
//!   run that was *given* a baseline and could not enforce it does not pass:
//!   see `UNENFORCED_EXIT`.
//! - `--record` requires `--samples` and rewrites the baseline from at least
//!   three distinct, compatible full runs, whole. It is the
//!   only supported way to calibrate, because the alternative — hand-editing
//!   the rows a change happens to care about — is what GL-114 diagnosed.
//! - `--ratios-only` still requires every fresh row but deliberately skips
//!   absolute thresholds and recorded-baseline decisions on other hosts.
//! - Estimates older than the freshness marker are rejected: the gate must
//!   never pass on stale output left by a previous run.
//! - A JSON report is always written; the exit code is nonzero when required
//!   data is missing/stale or any active bound is exceeded.
//!
//! It also decides whether the run is worth believing at all (GL-49). A gate
//! that only ever answers PASS or FAIL cannot distinguish a regression from a
//! measurement taken while the host was busy, and the two look identical in
//! the output — which is how a 2.6x inflation of every `tollgate-core`
//! benchmark was read as a real threshold breach, and how a baseline captured
//! on a loaded machine made a later change look 16% faster than it was.

#![allow(
    clippy::disallowed_methods,
    reason = "a reporting tool: these reads stamp when a report was produced, which is \
              metadata about the run rather than an input to any decision the workspace makes"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// Exit code for a run the gate will not draw a conclusion from. Distinct
/// from FAILURE so a caller can tell "your change is slow" from "ask me
/// again on a quiet machine".
const UNTRUSTED_EXIT: u8 = 3;
/// Exit code for a full run that was handed a recorded baseline and did not
/// enforce it. Distinct from FAILURE because nothing regressed, and distinct
/// from SUCCESS because nothing host-specific was checked either.
///
/// Timed measurement is a local review responsibility (GL-113), so a full run
/// *is* the acceptance evidence. Until GL-114 such a run printed `PASS` and
/// exited 0 with every regression row marked `baseline-skipped`: the recorded
/// baseline had in fact never been enforced once, locally or in CI, and twelve
/// days of drift accumulated behind a green verdict.
const UNENFORCED_EXIT: u8 = 4;
const USAGE: &str = "usage: check_benchmark_thresholds [--ratios-only] [--baseline <baseline.json>] [--samples <dir>] [--run-history <dir>] [--record <baseline.json>] [--history <history.json>] <manifest.json> <criterion-root> <report.json> <freshness-marker>";

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum GateMode {
    Full,
    RatiosOnly,
}

#[derive(Debug, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "parsed once per process from the command line; boxing buys nothing"
)]
enum Command {
    Run {
        manifest_path: PathBuf,
        criterion_root: PathBuf,
        report_path: PathBuf,
        marker_path: PathBuf,
        baseline_path: Option<PathBuf>,
        /// Where to rewrite the recorded baseline, whole.
        record_path: Option<PathBuf>,
        /// Where readable runs deposit their measurements, and where
        /// `--record` reads the runs it takes a median over.
        samples_path: Option<PathBuf>,
        /// Overrides the default beside the report. An isolated checkout —
        /// which `docs/PERFORMANCE.md` requires for acceptance runs — starts
        /// with no history and therefore no trust verdict at all, so the
        /// reviewer needs a way to carry one between runs.
        history_path: Option<PathBuf>,
        /// A bounded window of past readable runs, kept across series, from
        /// which `--record` derives each row's allowance (GL-141).
        run_history_path: Option<PathBuf>,
        mode: GateMode,
    },
    Help,
    Version,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    // Documentation is explicitly inert. It must not make misspelled policy
    // keys disappear into serde's general unknown-field fallback.
    #[serde(default)]
    _comment: serde::de::IgnoredAny,
    #[serde(default)]
    _reserved_ids: serde::de::IgnoredAny,
    benchmarks: Vec<Entry>,
    #[serde(default)]
    ratios: Vec<RatioBound>,
    #[serde(default)]
    trust: TrustPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RatioBound {
    #[serde(default)]
    _comment: serde::de::IgnoredAny,
    numerator: String,
    denominator: String,
    max_ratio: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    #[serde(default)]
    _comment: serde::de::IgnoredAny,
    id: String,
    target_ns: f64,
    threshold_ns: f64,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BaselineHost {
    id: String,
    architecture: String,
    cpu: String,
    os: String,
    rustc: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Baseline {
    host: BaselineHost,
    recorded_at: String,
    git_revision: String,
    profile: String,
    /// How many readable runs the medians were taken over. A baseline that
    /// cannot say is one nobody can judge.
    #[serde(default = "one_sample")]
    samples: usize,
    #[serde(rename = "_comment", default)]
    comment: String,
    benchmarks: Vec<BaselineEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BaselineEntry {
    id: String,
    mean_ns: f64,
    /// Written back only where it overrides the default, so a recorded file
    /// keeps saying which rows carry measured dispersion evidence and which
    /// simply take the standard bound.
    #[serde(
        default = "default_max_regression",
        skip_serializing_if = "is_default_max_regression"
    )]
    max_regression: f64,
}

fn default_max_regression() -> f64 {
    0.05
}

/// Pre-#114 baselines carry no sample count; they were taken from one run.
fn one_sample() -> usize {
    1
}

// serde's `skip_serializing_if` hands the predicate a reference whatever the
// field's type, so the signature is the attribute's, not a choice to pass a
// `f64` by reference.
#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's skip_serializing_if hands the predicate a reference whatever the field's type"
)]
fn is_default_max_regression(value: &f64) -> bool {
    *value == default_max_regression()
}

/// What every recorded baseline says about itself, so the convention that
/// produced GL-114 cannot be restated in the file it damaged.
const BASELINE_COMMENT: &str = "Generated by `check_benchmark_thresholds --record` from repeated full \
     `check_perf_thresholds.sh` runs on the host named above. Every row is the median of the same \
     distinct runs in the same measurement environment: rows are never edited one at a time, \
     because a filtered or partial \
     run measures the same code materially differently (GL-114). To recalibrate, re-record the whole \
     file and validate it with a fresh run.";

/// The profile `scripts/check_perf_thresholds.sh` benches under. Criterion
/// runs the release profile with `codegen-units = 1` pinned in the workspace
/// manifest, because the default sixteen let rustc repartition a crate as its
/// code grew and moved rows whose own source had not changed (GL-114); the
/// `production` profile (fat LTO, `panic=abort`) is what the load gate
/// measures.
///
/// It is part of a baseline's provenance, so changing it invalidates recorded
/// samples rather than silently comparing across profiles.
const BASELINE_PROFILE: &str = "criterion release, codegen-units=1";

/// When to stop believing a run.
#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
#[serde(deny_unknown_fields)]
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
    /// reach (GL-112).
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
/// Rows whose own baseline records measured dispersion.
///
/// A benchmark that carries a widened `max_regression` was widened because
/// somebody measured it moving between quiet runs, so a wide confidence
/// interval is what it does rather than evidence about the host. Counting it
/// toward the breadth signal below inverts that signal's meaning: across ten
/// full GL-114 runs, 39 of 61 unstable flags landed on these thirteen rows, and
/// `admission/full_check_contended_8_sharded` was flagged in all ten. With a
/// tenth of the manifest permanently noisy, "many benchmarks are unreadable"
/// stopped distinguishing a disturbed machine from an ordinary Tuesday, and
/// half of one session's runs abstained for no reason.
fn dispersion_prone(entries: Option<&BTreeMap<String, &BaselineEntry>>) -> BTreeSet<String> {
    entries
        .map(|entries| {
            entries
                .iter()
                .filter(|(_, entry)| entry.max_regression > default_max_regression())
                .map(|(id, _)| id.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// `unstable` and `measured` count only the rows that are *normally* steady —
/// see [`dispersion_prone`]. Rows known to disperse are still flagged
/// individually and still make their ratios inconclusive; they simply do not
/// vote on whether the host was disturbed.
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
    // Both operands are benchmark counts — tens, not billions — so the `f64`
    // conversion is exact far below the 2^53 where it stops being.
    #[allow(
        clippy::cast_precision_loss,
        reason = "both operands are benchmark counts, exact in f64 far below 2^53"
    )]
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
    //
    // Both operands are benchmark counts, exact in `f64` far below 2^53.
    #[allow(
        clippy::cast_precision_loss,
        reason = "both operands are benchmark counts, exact in f64 far below 2^53"
    )]
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
        // measurement the gate has already called unreadable (GL-112). The
        // whole-run guard below is what stops that becoming a gate that can
        // never fail.
        && ratios
            .iter()
            .all(|ratio| matches!(ratio.status, "pass" | "inconclusive"))
        && ratios_reached_a_verdict(ratios)
        // `inconclusive` is not a pass here either; it is the absence of a
        // verdict on a measurement the gate declined to read, allowed through
        // for the same reason and stopped from becoming a gate that can never
        // fail by the same kind of whole-run guard.
        && regressions
            .iter()
            .all(|regression| regression.status != "regressed")
        && regressions_reached_a_verdict(regressions)
}

/// A ratio is only as readable as the two measurements it divides.
///
/// `unstable` carries the ids whose own confidence interval was too wide to
/// read — the judgement `ReportRow::unstable` already makes and prints. Until
/// GL-112 that judgement stopped there: the gate declared a measurement
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
/// request for the state of a runner — the whole defect GL-112 exists to
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

/// The recorded-baseline counterpart: an enforced run in which every row it
/// could compare came back `inconclusive` has measured nothing about this
/// host, and must not read as a run that passed.
fn regressions_reached_a_verdict(regressions: &[RegressionRow]) -> bool {
    let comparable = regressions
        .iter()
        .filter(|row| matches!(row.status, "pass" | "regressed" | "inconclusive"))
        .count();
    comparable == 0
        || regressions
            .iter()
            .any(|row| matches!(row.status, "pass" | "regressed"))
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

fn parse_args<S: AsRef<std::ffi::OsStr>>(args: &[S]) -> Result<Command, String> {
    let separator = args.iter().position(|arg| arg.as_ref() == "--");
    let option_end = separator.unwrap_or(args.len());
    for arg in &args[..option_end] {
        match arg.as_ref().to_str() {
            Some("-h" | "--help") => return Ok(Command::Help),
            Some("-V" | "--version") => return Ok(Command::Version),
            _ => {}
        }
    }

    let args: Vec<String> = args
        .iter()
        .map(|arg| {
            arg.as_ref()
                .to_str()
                .map(str::to_owned)
                .ok_or("arguments must be valid UTF-8")
        })
        .collect::<Result<_, _>>()?;
    let mut mode = GateMode::Full;
    let mut baseline_path = None;
    let mut record_path = None;
    let mut samples_path = None;
    let mut history_path = None;
    let mut run_history_path = None;
    let mut positional = Vec::new();
    let mut index = 0;
    while index < option_end {
        // Each path option is `--name <path>`; the slot it fills is what
        // differs, so the arity and the "once only" rule live in one place.
        let slot = match args[index].as_str() {
            "--ratios-only" if mode == GateMode::Full => {
                mode = GateMode::RatiosOnly;
                index += 1;
                continue;
            }
            "--ratios-only" => {
                return Err("--ratios-only may be specified only once".to_owned());
            }
            "--baseline" => &mut baseline_path,
            "--record" => &mut record_path,
            "--samples" => &mut samples_path,
            "--history" => &mut history_path,
            "--run-history" => &mut run_history_path,
            value if value.starts_with('-') => return Err(format!("unknown option: {value}")),
            _ => {
                positional.push(args[index].clone());
                index += 1;
                continue;
            }
        };
        let name = args[index].clone();
        if slot.is_some() {
            return Err(format!("{name} may be specified only once"));
        }
        index += 1;
        if index >= option_end || args[index].starts_with('-') {
            return Err(format!("{name} requires a path"));
        }
        *slot = Some(PathBuf::from(&args[index]));
        index += 1;
    }
    if let Some(separator) = separator {
        positional.extend_from_slice(&args[separator + 1..]);
    }

    // Recording is an absolute, host-specific measurement of every row. A
    // ratios-only run deliberately reaches no absolute verdict, so it has
    // nothing to record from.
    if record_path.is_some() && mode == GateMode::RatiosOnly {
        return Err(
            "--record is a full-run mode; it cannot be combined with --ratios-only".to_owned(),
        );
    }
    if record_path.is_some() && samples_path.is_none() {
        return Err("--record requires --samples <dir>".to_owned());
    }

    match positional.as_slice() {
        [manifest, criterion, report, marker] => Ok(Command::Run {
            manifest_path: PathBuf::from(manifest),
            criterion_root: PathBuf::from(criterion),
            report_path: PathBuf::from(report),
            marker_path: PathBuf::from(marker),
            baseline_path,
            record_path,
            samples_path,
            history_path,
            run_history_path,
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

/// The provenance a recorded baseline must carry, taken from the run that is
/// about to become it.
///
/// Every field is required. Before GL-114 the file's single header claimed one
/// `recorded_at` and one `git_revision` for rows measured across four runs on
/// four revisions, and the revision it named was a tip of an unmerged feature
/// branch that squash-merge never put on `main`. Provenance that the tool
/// cannot supply is provenance a person invents, so the gate refuses to record
/// rather than write a header it made up.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct RecordContext {
    host: BaselineHost,
    revision: String,
    profile: String,
}

/// Whether a revision string identifies a specific, committed tree.
///
/// The wrapper marks a dirty worktree rather than hiding it, and a baseline
/// recorded from uncommitted work names a state nobody can check out again.
fn is_recordable_revision(revision: &str) -> bool {
    revision.len() == 40 && revision.chars().all(|c| c.is_ascii_hexdigit())
}

fn record_context(var: impl Fn(&str) -> Option<String>) -> Result<RecordContext, String> {
    let required = |name: &str| -> Result<String, String> {
        match var(name) {
            Some(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
            _ => Err(format!(
                "--record needs {name}; refusing to invent provenance"
            )),
        }
    };
    let revision = required("TOLLGATE_GATE_REVISION")?;
    if !is_recordable_revision(&revision) {
        return Err(format!(
            "--record needs a committed revision; TOLLGATE_GATE_REVISION was {revision:?}"
        ));
    }
    Ok(RecordContext {
        host: BaselineHost {
            id: required("TOLLGATE_PERF_HOST")?,
            architecture: required("TOLLGATE_GATE_TARGET")?,
            cpu: required("TOLLGATE_GATE_CPU")?,
            os: required("TOLLGATE_GATE_OS")?,
            rustc: required("TOLLGATE_GATE_RUSTC")?,
        },
        revision,
        profile: BASELINE_PROFILE.to_owned(),
    })
}

/// The fewest readable runs a recording may be taken from.
///
/// One run is not enough, and GL-114 proved it twice. The recorded file it
/// replaced was assembled a row at a time; the first attempt to replace it
/// recorded every row at once but from a single run, which happened to be the
/// fastest of the session — so `managed_credential/verify` went in at 794.5 ns
/// against 827-843 ns in every other run, and the next run failed it. A
/// baseline has to describe where a benchmark usually lands, and one sample
/// cannot say. `docs/DESIGN.md` has required a median of repeated runs since
/// GL-91; this is that requirement made mechanical rather than remembered.
const MIN_RECORD_SAMPLES: usize = 3;

/// One readable run's measurements, deposited for a later recording.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct Sample {
    /// Nanoseconds since the epoch of the benchmark's freshness marker, not
    /// the time the checker happened to process its output.
    run_id: String,
    context: RecordContext,
    recorded_at: String,
    means: BTreeMap<String, f64>,
}

impl Sample {
    fn validate(&self) -> Result<(), String> {
        if self.run_id.is_empty() || !self.run_id.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid benchmark run id".to_owned());
        }
        if self.means.is_empty() || self.means.values().any(|v| !v.is_finite() || *v <= 0.0) {
            return Err("sample means must be finite and positive".to_owned());
        }
        Ok(())
    }

    fn same_measurement(&self, other: &Self) -> bool {
        self.run_id == other.run_id && self.context == other.context && self.means == other.means
    }
}

// Old deposits cannot establish independent runs or comparable environments.
// Recognize them to give an actionable migration warning, never a fake default.
#[derive(Deserialize)]
#[serde(untagged)]
enum StoredSample {
    Current(Sample),
    Legacy {
        revision: String,
        recorded_at: String,
        means: BTreeMap<String, f64>,
    },
}

/// Per-row median across the samples, which is what a baseline row means.
///
/// The median rather than the mean: a contaminated run skews a mean and only
/// displaces a median, and the runs being combined are exactly the ones whose
/// contamination the gate cannot always detect.
fn median_of(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty()
        || values
            .iter()
            .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("finite means are ordered"));
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        values[mid - 1] + (values[mid] - values[mid - 1]) / 2.0
    } else {
        values[mid]
    })
}

/// Count each benchmark run once, retaining only exactly matching recording
/// contexts. A copied file cannot increase the population; divergent copies
/// of one run are an error rather than an arbitrary winner.
fn load_samples(dir: &Path, context: &RecordContext) -> Result<Vec<Sample>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read samples {}: {e}", dir.display()))?;
    let mut samples: BTreeMap<String, Sample> = BTreeMap::new();
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let stored: StoredSample = serde_json::from_str(&text)
            .map_err(|e| format!("cannot parse {}: {e}", path.display()))?;
        let sample = match stored {
            StoredSample::Current(sample) => sample,
            StoredSample::Legacy {
                revision,
                recorded_at,
                means,
            } => {
                eprintln!(
                    "perf-gate: skipping legacy sample {} ({} rows at {revision}, {recorded_at}): \
                     missing run identity and environment; collect a new series with --fresh-samples",
                    path.display(),
                    means.len()
                );
                continue;
            }
        };
        sample
            .validate()
            .map_err(|e| format!("invalid sample {}: {e}", path.display()))?;
        if let Some(previous) = samples.get(&sample.run_id) {
            if !previous.same_measurement(&sample) {
                return Err(format!(
                    "conflicting samples for benchmark run {}",
                    sample.run_id
                ));
            }
        } else {
            samples.insert(sample.run_id.clone(), sample);
        }
    }
    Ok(samples
        .into_values()
        .filter(|sample| {
            if &sample.context == context {
                true
            } else {
                eprintln!(
                    "perf-gate: skipping sample {}: revision or measurement environment differs",
                    sample.run_id
                );
                false
            }
        })
        .collect())
}

/// Deposit this run's measurements so a later `--record` can take a median
/// over it. Only ever called for a run the gate was willing to read.
fn write_sample(dir: &Path, sample: &Sample) -> Result<(), String> {
    sample.validate()?;
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    let path = dir.join(format!("run-{}.json", sample.run_id));
    // A separate staging file per writer, published exclusively by hard link:
    // readers see complete JSON, and a retry cannot replace original evidence.
    static NEXT_STAGE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT_STAGE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut staged = StagedFile::create(
        path.with_extension(format!("{}-{sequence}.staged", std::process::id())),
    )?;
    let text = serde_json::to_string_pretty(sample).map_err(|e| e.to_string())?;
    staged.write(&text)?;
    if let Err(error) = std::fs::hard_link(&staged.path, &path) {
        // Publication is already satisfied only by identical existing evidence,
        // regardless of which filesystem error prevented this writer's link.
        let previous: Sample = std::fs::read_to_string(&path)
            .map_err(|e| e.to_string())
            .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
            .map_err(|e| {
                format!(
                    "cannot publish {}: {error}; cannot read existing sample: {e}",
                    path.display()
                )
            })?;
        if !previous.same_measurement(sample) {
            return Err(format!(
                "conflicting sample for benchmark run {}; original preserved",
                sample.run_id
            ));
        }
    }
    Ok(())
}

/// Exclusive staging also serializes baseline recorders from destination
/// validation through promotion. An interrupted writer leaves a visible
/// .staged file; an operator must confirm it has stopped before removing it.
struct StagedFile {
    path: PathBuf,
    file: std::fs::File,
    cleanup: bool,
}

impl StagedFile {
    fn create(path: PathBuf) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| format!("cannot exclusively stage {}: {e}", path.display()))?;
        Ok(Self {
            path,
            file,
            cleanup: true,
        })
    }

    fn write(&mut self, text: &str) -> Result<(), String> {
        writeln!(self.file, "{text}")
            .map_err(|e| format!("cannot write {}: {e}", self.path.display()))
    }

    fn promote(mut self, path: &Path) -> Result<(), String> {
        std::fs::rename(&self.path, path)
            .map_err(|e| format!("cannot promote {}: {e}", path.display()))?;
        self.cleanup = false;
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Build the replacement baseline: every manifest row, from the median of the
/// samples taken at this revision.
///
/// A missing measurement is an error rather than an omitted row. Omitting it
/// is precisely how the checked-in file came to hold rows from four different
/// runs, and how thirteen benchmarks the gate runs ended up with no recorded
/// mean at all.
/// How many past runs the run history keeps. Enough for several recording
/// series on comparable code, bounded so the directory cannot grow without
/// limit on a host that runs the gate every day.
const HISTORY_WINDOW: usize = 40;
/// The fewest pooled runs, across at least [`MIN_SPREAD_REVISIONS`]
/// revisions, from which a row's spread is believed.
const MIN_SPREAD_RUNS: usize = 6;
const MIN_SPREAD_REVISIONS: usize = 2;
/// A run more than this far above its revision's median is an excursion:
/// listed, never absorbed into an allowance. Chosen against the observed
/// envelopes: every legitimate spread on the controlled host through GL-139 sat
/// under x1.45, and the one-off readings above it (x2.4, x2.6) were single runs.
const EXCURSION_RATIO: f64 = 1.5;
/// Headroom above the worst believed run.
const SPREAD_MARGIN: f64 = 0.03;
/// An allowance above this is a weak per-row gate; the report says so.
const WIDE_ALLOWANCE: f64 = 0.30;

/// A row's allowance as its own recorded spread implies (GL-141).
#[derive(Clone, Debug, PartialEq)]
struct DerivedAllowance {
    allowance: f64,
    runs: usize,
    revisions: usize,
    excursions: Vec<f64>,
}

/// Round an allowance up to the next 0.05, ignoring floating-point residue
/// that would otherwise push an exact multiple a whole step higher.
fn round_allowance_up(raw: f64) -> f64 {
    ((raw * 20.0) - 1e-9).ceil() / 20.0
}

/// Each row's allowance from the run history, or nothing where the history
/// cannot yet support one.
///
/// Runs are grouped by revision and each is divided by its own revision's
/// median, so a code change between revisions — which moves a row's level,
/// not its noise — drops out, and what is pooled is run-to-run spread on one
/// build. Only runs on this host, environment and profile count: the
/// revision is the one thing allowed to differ. Excursions above
/// [`EXCURSION_RATIO`] are reported rather than absorbed, and a row needs
/// [`MIN_SPREAD_RUNS`] believed runs across [`MIN_SPREAD_REVISIONS`]
/// revisions before its spread is trusted at all.
fn derive_allowances<'a>(
    history: &[Sample],
    context: &RecordContext,
    ids: impl Iterator<Item = &'a str>,
) -> BTreeMap<String, DerivedAllowance> {
    let comparable: Vec<&Sample> = history
        .iter()
        .filter(|sample| {
            sample.context.host == context.host && sample.context.profile == context.profile
        })
        .collect();
    let mut derived = BTreeMap::new();
    for id in ids {
        let mut by_revision: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
        for sample in &comparable {
            if let Some(&value) = sample.means.get(id) {
                by_revision
                    .entry(sample.context.revision.as_str())
                    .or_default()
                    .push(value);
            }
        }
        let mut ratios = Vec::new();
        let mut revisions = 0;
        for values in by_revision.into_values().filter(|values| values.len() >= 2) {
            let Some(median) = median_of(values.clone()) else {
                continue;
            };
            revisions += 1;
            ratios.extend(values.iter().map(|value| value / median));
        }
        let (excursions, believed): (Vec<f64>, Vec<f64>) = ratios
            .into_iter()
            .partition(|ratio| *ratio > EXCURSION_RATIO);
        if believed.len() < MIN_SPREAD_RUNS || revisions < MIN_SPREAD_REVISIONS {
            continue;
        }
        let worst = believed.iter().copied().fold(1.0_f64, f64::max);
        derived.insert(
            id.to_owned(),
            DerivedAllowance {
                allowance: round_allowance_up(worst - 1.0 + SPREAD_MARGIN)
                    .max(default_max_regression()),
                runs: believed.len(),
                revisions,
                excursions,
            },
        );
    }
    derived
}

/// Every retained run in `dir`, whatever its revision, each counted once.
/// Unreadable or legacy files are skipped with a notice: history informs an
/// allowance, and a damaged file must not block a recording.
fn load_history(dir: &Path) -> Vec<Sample> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut runs: BTreeMap<String, Sample> = BTreeMap::new();
    for path in entries.filter_map(|entry| entry.ok().map(|entry| entry.path())) {
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let parsed = std::fs::read_to_string(&path)
            .map_err(|e| e.to_string())
            .and_then(|text| {
                serde_json::from_str::<StoredSample>(&text).map_err(|e| e.to_string())
            });
        match parsed {
            Ok(StoredSample::Current(sample)) if sample.validate().is_ok() => {
                runs.entry(sample.run_id.clone()).or_insert(sample);
            }
            Ok(_) => eprintln!(
                "perf-gate: skipping run history {}: legacy or invalid",
                path.display()
            ),
            Err(error) => eprintln!(
                "perf-gate: skipping run history {}: {error}",
                path.display()
            ),
        }
    }
    runs.into_values().collect()
}

/// Keep the newest `window` runs in `dir`, by run id, and remove the rest.
fn prune_history(dir: &Path, window: usize) -> Result<usize, String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    let mut runs: Vec<(u128, PathBuf)> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter_map(|path| {
            let id = path
                .file_name()?
                .to_str()?
                .strip_prefix("run-")?
                .strip_suffix(".json")?
                .parse()
                .ok()?;
            Some((id, path))
        })
        .collect();
    runs.sort_unstable_by_key(|(id, _)| *id);
    let excess = runs.len().saturating_sub(window);
    for (_, path) in runs.drain(..excess) {
        std::fs::remove_file(&path).map_err(|e| format!("cannot prune {}: {e}", path.display()))?;
    }
    Ok(excess)
}

fn recorded_baseline(
    context: &RecordContext,
    previous: Option<&Baseline>,
    manifest: &Manifest,
    samples: &[BTreeMap<String, f64>],
    derived: &BTreeMap<String, DerivedAllowance>,
    recorded_at: String,
) -> Result<Baseline, String> {
    if samples.len() < MIN_RECORD_SAMPLES {
        return Err(format!(
            "a baseline is recorded from the median of at least {MIN_RECORD_SAMPLES} readable \
             runs at this revision; {} available. Run the gate again with the same --samples \
             directory.",
            samples.len()
        ));
    }
    let mut measured: BTreeMap<String, f64> = BTreeMap::new();
    for entry in &manifest.benchmarks {
        let values: Vec<f64> = samples
            .iter()
            .filter_map(|sample| sample.get(&entry.id).copied())
            .collect();
        // Every sample must have measured every row, or the "median" is taken
        // over a different population per row.
        if values.len() == samples.len()
            && let Some(median) = median_of(values)
        {
            measured.insert(entry.id.clone(), median);
        }
    }
    recorded_baseline_from(
        context,
        previous,
        manifest,
        &measured,
        derived,
        recorded_at,
        samples.len(),
    )
}

fn recorded_baseline_from(
    context: &RecordContext,
    previous: Option<&Baseline>,
    manifest: &Manifest,
    measured: &BTreeMap<String, f64>,
    derived: &BTreeMap<String, DerivedAllowance>,
    recorded_at: String,
    samples: usize,
) -> Result<Baseline, String> {
    if let Some(previous) = previous
        && previous.host.id != context.host.id
    {
        return Err(format!(
            "refusing to overwrite the baseline for host {:?} with a run on {:?}",
            previous.host.id, context.host.id
        ));
    }
    let carried: BTreeMap<&str, f64> = previous
        .map(|previous| {
            previous
                .benchmarks
                .iter()
                .map(|entry| (entry.id.as_str(), entry.max_regression))
                .collect()
        })
        .unwrap_or_default();

    let mut benchmarks = Vec::with_capacity(manifest.benchmarks.len());
    let mut missing = Vec::new();
    for entry in &manifest.benchmarks {
        match measured.get(&entry.id) {
            Some(&mean_ns) => benchmarks.push(BaselineEntry {
                id: entry.id.clone(),
                mean_ns,
                // The carried allowance is a floor, never lowered by what the
                // history implies: narrowing stays a deliberate edit (GL-141).
                max_regression: carried
                    .get(entry.id.as_str())
                    .copied()
                    .unwrap_or_else(default_max_regression)
                    .max(derived.get(&entry.id).map_or(0.0, |d| d.allowance)),
            }),
            None => missing.push(entry.id.as_str()),
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "the samples cover {} of {} benchmarks; a baseline is recorded whole or not at all \
             (missing: {})",
            benchmarks.len(),
            manifest.benchmarks.len(),
            missing.join(", ")
        ));
    }
    Ok(Baseline {
        host: context.host.clone(),
        recorded_at,
        git_revision: context.revision.clone(),
        profile: context.profile.clone(),
        samples,
        comment: BASELINE_COMMENT.to_owned(),
        benchmarks,
    })
}

/// Say which allowances the history widened, and which excursions it set
/// aside, so a recording's changes are read rather than discovered.
fn report_derived(
    previous: Option<&Baseline>,
    recorded: &Baseline,
    derived: &BTreeMap<String, DerivedAllowance>,
) {
    let before: BTreeMap<&str, f64> = previous
        .map(|previous| {
            previous
                .benchmarks
                .iter()
                .map(|entry| (entry.id.as_str(), entry.max_regression))
                .collect()
        })
        .unwrap_or_default();
    for entry in &recorded.benchmarks {
        let Some(evidence) = derived.get(&entry.id) else {
            continue;
        };
        let was = before
            .get(entry.id.as_str())
            .copied()
            .unwrap_or_else(default_max_regression);
        if entry.max_regression > was {
            println!(
                "perf-gate: {} allowance {was:.2} -> {:.2} from {} runs over {} revisions",
                entry.id, entry.max_regression, evidence.runs, evidence.revisions
            );
        }
        if !evidence.excursions.is_empty() {
            println!(
                "perf-gate: {} set aside {} excursion(s) above x{EXCURSION_RATIO}: {}",
                entry.id,
                evidence.excursions.len(),
                evidence
                    .excursions
                    .iter()
                    .map(|ratio| format!("x{ratio:.2}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

/// Stage, validate, then promote — so a failed record leaves the previous
/// last-known-good baseline exactly where it was.
fn write_baseline(staged: StagedFile, path: &Path, baseline: &Baseline) -> Result<(), String> {
    let text = serde_json::to_string_pretty(baseline).map_err(|e| e.to_string())?;
    let mut staged = staged;
    staged.write(&text)?;
    std::fs::read_to_string(&staged.path)
        .map_err(|e| e.to_string())
        .and_then(|text| serde_json::from_str::<Baseline>(&text).map_err(|e| e.to_string()))
        .and_then(|parsed| baseline_entries(&parsed).map(|_| ()))
        .map_err(|e| format!("staged baseline did not validate: {e}"))?;
    staged.promote(path)
}

/// Validate the actual destination while exclusively owning its staging slot.
/// `--baseline` is only the comparison input and cannot authorize replacement.
fn record_baseline(
    path: &Path,
    context: &RecordContext,
    manifest: &Manifest,
    samples_dir: &Path,
    run_history_dir: Option<&Path>,
) -> Result<usize, String> {
    let staged = StagedFile::create(path.with_extension("json.staged"))?;
    let previous: Option<Baseline> = match std::fs::read_to_string(path) {
        Ok(text) => Some(
            serde_json::from_str(&text)
                .map_err(|e| format!("cannot parse destination {}: {e}", path.display()))?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "cannot read destination {}: {error}",
                path.display()
            ));
        }
    };
    if let Some(previous) = &previous {
        baseline_entries(previous)?;
    }
    let samples = load_samples(samples_dir, context)?;
    let means: Vec<_> = samples.into_iter().map(|sample| sample.means).collect();
    let derived = run_history_dir.map_or_else(BTreeMap::new, |dir| {
        let history = load_history(dir);
        derive_allowances(
            &history,
            context,
            manifest.benchmarks.iter().map(|entry| entry.id.as_str()),
        )
    });
    let baseline = recorded_baseline(
        context,
        previous.as_ref(),
        manifest,
        &means,
        &derived,
        jiff::Timestamp::now().to_string(),
    )?;
    report_derived(previous.as_ref(), &baseline, &derived);
    write_baseline(staged, path, &baseline)?;
    Ok(baseline.samples)
}

/// Compare one row against its recorded baseline.
///
/// `unstable` is the judgement `ReportRow::unstable` already made and printed.
/// A row whose own confidence interval was too wide to read yields
/// `inconclusive` rather than `regressed`, for the same reason GL-112 gave the
/// ratio path: declaring a measurement unreadable and then drawing a verdict
/// from it says something about the code on the strength of a number the gate
/// has said nothing can be concluded from. GL-114 found the ratio path had been
/// fixed and this one had not — a validating run failed
/// `admission/request_rate_token` at 173.1 ns against 126-132 ns in every
/// neighbouring run, on a row it had flagged unstable in the same report.
fn evaluate_regression(
    id: &str,
    measured_ns: Option<f64>,
    baseline: Option<&BaselineEntry>,
    enforced: bool,
    unstable: bool,
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
        // Checked after the bound, so a row that passes on an unstable
        // measurement still passes: instability is only ever a reason to
        // withhold a *failure*, never to manufacture one.
        (true, Some(_), Some(_)) if unstable => "inconclusive",
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

/// How the whole run sat against the recorded baseline, independent of any
/// single row's verdict.
///
/// GL-114's failing run put nine rows over their 5% bound while the median of
/// all forty-five baselined rows sat at ×1.030 — the machine was slow, and the
/// rows that crossed were simply the ones with the least headroom. Nothing in
/// the report said so, and reconstructing it took reading every row. A gate
/// that can distinguish a regression from a busy host should say which it
/// thinks it saw.
#[derive(Serialize, Clone, Copy, Debug, PartialEq)]
struct RunDrift {
    compared: usize,
    median_ratio: f64,
    p25_ratio: f64,
    p75_ratio: f64,
}

/// Nearest-rank quartiles over the per-row ratios the regression rows already
/// carry — including on a run that did not enforce the baseline, because the
/// ratio is a description of the measurement and not a verdict about it.
fn run_drift(regressions: &[RegressionRow]) -> Option<RunDrift> {
    let mut ratios: Vec<f64> = regressions
        .iter()
        .filter_map(|regression| regression.ratio)
        .filter(|ratio| ratio.is_finite())
        .collect();
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(|a, b| a.partial_cmp(b).expect("finite ratios are ordered"));
    // The population is nonempty and the only fractions below are 1/4,
    // 1/2 and 3/4, so each truncated index is strictly below its length.
    let at = |fraction: f64| -> f64 { ratios[(ratios.len() as f64 * fraction) as usize] };
    Some(RunDrift {
        compared: ratios.len(),
        median_ratio: at(0.5),
        p25_ratio: at(0.25),
        p75_ratio: at(0.75),
    })
}

/// Whether a full run reached no recorded-baseline verdict at all.
///
/// `untrusted-run` is excluded because that run already fails with
/// `UNTRUSTED_EXIT` and its own explanation; this is about the run that looked
/// like a pass. `no-baseline-file`, `host-unset` and `host-mismatch` all mean
/// the same thing to a reviewer holding the report as acceptance evidence:
/// the host-specific bound was not checked.
fn baseline_verdict_missing(mode: GateMode, skip_reason: Option<&str>) -> bool {
    match (mode, skip_reason) {
        (GateMode::RatiosOnly, _) | (GateMode::Full, None) => false,
        (GateMode::Full, Some("untrusted-run")) => false,
        (GateMode::Full, Some(_)) => true,
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

#[derive(Clone, Serialize)]
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
    /// Swap in use and free memory, same provenance.
    ///
    /// GL-114 spent an evening attributing wide confidence intervals to load
    /// average and CPU idle, both of which looked fine, while the host sat at
    /// 6.5 GB of 8 GB swap with sixty megabytes of free pages. A run taken
    /// while the machine is paging is not a measurement of the code, and the
    /// report said nothing that would have shown it.
    memory: Option<String>,
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
            memory: std::env::var("TOLLGATE_GATE_MEMORY").ok(),
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
    /// Absent only when no row had a baseline to compare against.
    drift: Option<RunDrift>,
    trust: Trust,
    policy: TrustPolicy,
    run: RunContext,
    /// Rows whose recorded allowance exceeds [`WIDE_ALLOWANCE`]: their
    /// per-row comparison is weak, and a same-run ratio or absolute threshold
    /// is what actually guards them (GL-141).
    wide_allowances: Vec<String>,
}

/// The baselined rows whose allowance is too wide to discriminate much.
fn wide_allowances(baseline: Option<&Baseline>) -> Vec<String> {
    baseline.map_or_else(Vec::new, |baseline| {
        baseline
            .benchmarks
            .iter()
            .filter(|entry| entry.max_regression > WIDE_ALLOWANCE)
            .map(|entry| format!("{} ({:.2})", entry.id, entry.max_regression))
            .collect()
    })
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
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let (
        manifest_path,
        criterion_root,
        report_path,
        marker_path,
        baseline_path,
        record_path,
        samples_dir,
        history_override,
        run_history_dir,
        mode,
    ) = match parse_args(&args) {
        Ok(Command::Run {
            manifest_path,
            criterion_root,
            report_path,
            marker_path,
            baseline_path,
            record_path,
            samples_path,
            history_path,
            run_history_path,
            mode,
        }) => (
            manifest_path,
            criterion_root,
            report_path,
            marker_path,
            baseline_path,
            record_path,
            samples_path,
            history_path,
            run_history_path,
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
    // Beside the report by default, not beside the manifest: it is generated
    // output that describes this host, never a checked-in expectation. The
    // override exists because acceptance runs happen in an isolated checkout,
    // where the default path is empty on every run and the trust verdict is
    // therefore always `no_history`.
    let history_path =
        history_override.unwrap_or_else(|| report_path.with_file_name("perf_gate_history.json"));

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
    // verdicts computed from them (GL-112).
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
    // Breadth is judged over the rows that are normally steady, so a manifest
    // that grew a family of inherently noisy contention benchmarks does not
    // keep declaring quiet runs unreadable (GL-114).
    let prone = dispersion_prone(baseline_by_id.as_ref());
    let steady_unstable = unstable.iter().filter(|id| !prone.contains(*id)).count();
    let steady_measured = rows
        .iter()
        .filter(|row| row.mean_ns.is_some() && !prone.contains(&row.id))
        .count();
    let trust = assess_trust(
        &current,
        &previous,
        steady_unstable,
        steady_measured,
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
                unstable.contains(&entry.id),
            )
        })
        .collect();
    for regression in &regressions {
        if let Some(line) = regression_line(regression) {
            eprintln!("{line}");
        }
    }
    let skip_reason = baseline_skip_reason(
        mode,
        baseline.as_ref().map(|baseline| baseline.host.id.as_str()),
        active_host.as_deref(),
        untrusted,
    );
    let drift = run_drift(&regressions);
    if let Some(drift) = drift {
        println!(
            "perf-gate: run drift: median x{:.3} (IQR x{:.3}-x{:.3}) across {} baselined rows",
            drift.median_ratio, drift.p25_ratio, drift.p75_ratio, drift.compared
        );
    }
    // A full run that reached no baseline verdict is not a pass, whatever the
    // rows say: the artifact a reviewer holds must not read as evidence for a
    // check that never ran.
    let passed = all_results_passed(&rows, &ratios, &regressions)
        && !baseline_verdict_missing(mode, skip_reason);

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
        drift,
        trust: trust.clone(),
        policy: manifest.trust,
        run: RunContext::capture(),
        wide_allowances: wide_allowances(baseline.as_ref().filter(|_| baseline_enforced)),
    };
    if !report.wide_allowances.is_empty() {
        println!(
            "perf-gate: {} rows carry allowances above {WIDE_ALLOWANCE:.2}, weak per-row gates: {}",
            report.wide_allowances.len(),
            report.wide_allowances.join(", ")
        );
    }
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

    // Deposit and record only past the trust guards above: a baseline is the
    // yardstick every later run is judged by, so it is never taken from a run
    // the gate would not draw a conclusion from.
    //
    // The verdict below still describes this run against the *previous*
    // baseline. That is deliberate — the recorded file is a proposal, and the
    // fresh run that validates it is the evidence.
    let record_context = record_context(|name| std::env::var(name).ok());
    // Only a run that measured the whole manifest is a sample. A partial run
    // deposited one during GL-114 — its benchmarks were interrupted after 33 of
    // 58 rows — and while the completeness check below refused to record from
    // it, the file sat in the directory poisoning every later attempt.
    let complete = manifest
        .benchmarks
        .iter()
        .all(|entry| current.contains_key(&entry.id));
    if let Some(dir) = samples_dir.as_ref().filter(|_| mode == GateMode::Full) {
        let deposited = (|| {
            if !complete {
                return Err(format!(
                    "this run measured {} of {} benchmarks; every sample must cover the whole manifest",
                    current.len(),
                    manifest.benchmarks.len()
                ));
            }
            let context = record_context.as_ref().map_err(Clone::clone)?;
            let run_id = marker_mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|e| format!("invalid benchmark run marker: {e}"))?
                .as_nanos()
                .to_string();
            let sample = Sample {
                run_id,
                context: context.clone(),
                recorded_at: jiff::Timestamp::try_from(marker_mtime)
                    .map_err(|e| e.to_string())?
                    .to_string(),
                means: current.clone(),
            };
            write_sample(dir, &sample)
        })();
        if let (Ok(()), Some(history)) = (&deposited, run_history_dir.as_ref()) {
            // The same run, kept across series for `--record` to read its
            // spread from; `--fresh-samples` clears the samples, never this.
            let retained = (|| {
                let context = record_context.as_ref().map_err(Clone::clone)?;
                let sample = Sample {
                    run_id: marker_mtime
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_err(|e| e.to_string())?
                        .as_nanos()
                        .to_string(),
                    context: context.clone(),
                    recorded_at: jiff::Timestamp::try_from(marker_mtime)
                        .map_err(|e| e.to_string())?
                        .to_string(),
                    means: current.clone(),
                };
                write_sample(history, &sample)?;
                prune_history(history, HISTORY_WINDOW)
            })();
            if let Err(error) = retained {
                eprintln!("perf-gate: not retaining run history: {error}; gate verdict unchanged");
            }
        }
        match deposited {
            Ok(()) => println!(
                "perf-gate: retained this benchmark run in {} (retries count once)",
                dir.display()
            ),
            Err(error) if record_path.is_some() => {
                return fail(&format!("cannot deposit sample: {error}"));
            }
            Err(error) => {
                eprintln!("perf-gate: not depositing a sample: {error}; gate verdict unchanged")
            }
        }
    }
    if let Some(record_path) = record_path.as_ref() {
        let recorded = record_context.and_then(|context| {
            let dir = samples_dir
                .as_deref()
                .ok_or("--record requires --samples <dir>")?;
            record_baseline(
                record_path,
                &context,
                &manifest,
                dir,
                run_history_dir.as_deref(),
            )
        });
        match recorded {
            Ok(samples) => println!(
                "perf-gate: recorded {} rows to {} from the median of {samples} runs",
                manifest.benchmarks.len(),
                record_path.display()
            ),
            Err(error) => return fail(&format!("cannot record baseline: {error}")),
        }
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

    if baseline_verdict_missing(mode, skip_reason) {
        eprintln!(
            "perf-gate: UNENFORCED — a full run skipped the recorded baseline ({}). Absolute \
             bounds passing is not a passing baseline comparison, and this report is not \
             performance evidence. Set TOLLGATE_PERF_HOST to the host the baseline names, or \
             run --ratios-only and say so.",
            skip_reason.unwrap_or("unknown")
        );
        return ExitCode::from(UNENFORCED_EXIT);
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
    if !mean_ns.is_finite() || mean_ns <= 0.0 {
        return Err(format!(
            "invalid mean in {}: expected finite positive nanoseconds",
            path.display()
        ));
    }
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
                record_path: None,
                samples_path: None,
                history_path: None,
                run_history_path: None,
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
                record_path: None,
                samples_path: None,
                history_path: None,
                run_history_path: None,
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
        assert_eq!(parse_args(&strings(&[])), Err(USAGE.to_owned()));
        for once in ["--record", "--samples", "--history"] {
            assert_eq!(
                parse_args(&strings(&[once])),
                Err(format!("{once} requires a path"))
            );
            assert_eq!(
                parse_args(&strings(&[
                    once,
                    "first.json",
                    once,
                    "second.json",
                    "manifest.json",
                    "criterion",
                    "report.json",
                    "marker"
                ])),
                Err(format!("{once} may be specified only once"))
            );
        }
    }

    #[test]
    fn cli_accepts_recording_and_an_explicit_history_but_not_recording_without_absolutes() {
        assert_eq!(
            parse_args(&strings(&[
                "--baseline",
                "baseline.json",
                "--record",
                "baseline.json",
                "--samples",
                "samples",
                "--history",
                "history.json",
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
                record_path: Some(PathBuf::from("baseline.json")),
                samples_path: Some(PathBuf::from("samples")),
                history_path: Some(PathBuf::from("history.json")),
                run_history_path: None,
                mode: GateMode::Full,
            })
        );
        // A ratios-only run reaches no absolute verdict, so it has nothing to
        // record from.
        assert_eq!(
            parse_args(&strings(&[
                "--ratios-only",
                "--record",
                "baseline.json",
                "--samples",
                "samples",
                "manifest.json",
                "criterion",
                "report.json",
                "marker"
            ])),
            Err("--record is a full-run mode; it cannot be combined with --ratios-only".to_owned())
        );
    }

    #[test]
    fn controlled_baseline_boundary_is_inclusive_and_defaults_to_five_percent() {
        let baseline: BaselineEntry =
            serde_json::from_str(r#"{"id":"admission/full_check","mean_ns":100.0}"#).unwrap();
        assert_eq!(baseline.max_regression, 0.05);
        let boundary = evaluate_regression(&baseline.id, Some(105.0), Some(&baseline), true, false);
        assert_eq!(boundary.baseline_ns, Some(100.0));
        assert_eq!(boundary.ratio, Some(1.05));
        assert_eq!(boundary.max_regression, Some(0.05));
        assert_eq!(boundary.status, "pass");
        assert_eq!(
            evaluate_regression(&baseline.id, Some(105.000_1), Some(&baseline), true, false).status,
            "regressed"
        );
    }

    #[test]
    fn a_legacy_baseline_reports_one_sample_without_changing_explicit_counts() {
        let recorded = recorded_baseline(
            &recording_context(),
            None,
            &recording_manifest(&["a"]),
            &vec![means(&[("a", 100.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        let mut encoded = serde_json::to_value(recorded).unwrap();
        let decoded: Baseline = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded.samples, 3);

        encoded.as_object_mut().unwrap().remove("samples");
        let legacy: Baseline = serde_json::from_value(encoded).unwrap();
        assert_eq!(legacy.samples, 1);
        assert_eq!(serde_json::to_value(legacy).unwrap()["samples"], 1);
    }

    /// The checked-in baseline covers the manifest exactly, in both
    /// directions.
    ///
    /// This assertion used to be `entries.len() < manifest.benchmarks.len()` —
    /// a guardrail that *required* the baseline to be incomplete and would
    /// have failed the moment anyone finished the job. It passed for twelve
    /// days while thirteen benchmarks the gate runs had no recorded mean at
    /// all, and while the rows that did have one had been measured across four
    /// separate runs (GL-114).
    #[test]
    fn checked_in_baseline_covers_every_benchmark_the_gate_runs() {
        let manifest: Manifest =
            serde_json::from_str(include_str!("../../../testing/perf_thresholds.json")).unwrap();
        let baseline: Baseline =
            serde_json::from_str(include_str!("../../../testing/perf_baseline.json")).unwrap();
        let entries = baseline_entries(&baseline).unwrap();
        let manifest_ids: BTreeSet<&String> =
            manifest.benchmarks.iter().map(|entry| &entry.id).collect();

        assert_eq!(baseline.host.id, "mistral-apple-m1-pro");
        let unknown: Vec<_> = entries
            .keys()
            .filter(|id| !manifest_ids.contains(id))
            .collect();
        assert!(
            unknown.is_empty(),
            "a baseline row must name a benchmark the gate still runs: {unknown:?}"
        );
        let unrecorded: Vec<_> = manifest_ids
            .iter()
            .filter(|id| !entries.contains_key(id.as_str()))
            .collect();
        assert!(
            unrecorded.is_empty(),
            "every benchmark the gate runs is recorded by the same full run: {unrecorded:?}"
        );
    }

    fn record_env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn complete_record_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("TOLLGATE_PERF_HOST", "mistral-apple-m1-pro"),
            (
                "TOLLGATE_GATE_REVISION",
                "6aac1c4000000000000000000000000000000000",
            ),
            ("TOLLGATE_GATE_TARGET", "aarch64-apple-darwin"),
            ("TOLLGATE_GATE_CPU", "Apple M1 Pro (10 logical CPUs)"),
            ("TOLLGATE_GATE_OS", "macOS 26.6.2 (Darwin 25.6.0)"),
            ("TOLLGATE_GATE_RUSTC", "rustc 1.97.1"),
        ]
    }

    #[test]
    fn recording_refuses_provenance_it_would_have_to_invent() {
        assert!(record_context(record_env(&complete_record_env())).is_ok());
        for dropped in [
            "TOLLGATE_PERF_HOST",
            "TOLLGATE_GATE_REVISION",
            "TOLLGATE_GATE_TARGET",
            "TOLLGATE_GATE_CPU",
            "TOLLGATE_GATE_OS",
            "TOLLGATE_GATE_RUSTC",
        ] {
            let env: Vec<_> = complete_record_env()
                .into_iter()
                .filter(|(name, _)| *name != dropped)
                .collect();
            assert!(
                record_context(record_env(&env)).is_err(),
                "recording must refuse without {dropped}"
            );
            let blank: Vec<_> = complete_record_env()
                .into_iter()
                .map(|(name, value)| {
                    if name == dropped {
                        (name, " ")
                    } else {
                        (name, value)
                    }
                })
                .collect();
            assert!(
                record_context(record_env(&blank)).is_err(),
                "recording must refuse a blank {dropped}"
            );
        }
    }

    #[test]
    fn recording_refuses_a_revision_nobody_can_check_out() {
        assert!(is_recordable_revision(
            "6aac1c4000000000000000000000000000000000"
        ));
        for rejected in [
            "6aac1c4000000000000000000000000000000000-dirty",
            "6aac1c4",
            "",
            "unknown",
            "6aac1c400000000000000000000000000000000g",
        ] {
            assert!(
                !is_recordable_revision(rejected),
                "{rejected:?} does not identify a committed tree"
            );
            let env: Vec<_> = complete_record_env()
                .into_iter()
                .map(|(name, value)| {
                    if name == "TOLLGATE_GATE_REVISION" {
                        (name, rejected)
                    } else {
                        (name, value)
                    }
                })
                .collect();
            assert!(record_context(record_env(&env)).is_err());
        }
    }

    fn recording_manifest(ids: &[&str]) -> Manifest {
        Manifest {
            _comment: serde::de::IgnoredAny,
            _reserved_ids: serde::de::IgnoredAny,
            benchmarks: ids
                .iter()
                .map(|id| Entry {
                    _comment: serde::de::IgnoredAny,
                    id: (*id).to_owned(),
                    target_ns: 10.0,
                    threshold_ns: 100.0,
                })
                .collect(),
            ratios: Vec::new(),
            trust: TrustPolicy::default(),
        }
    }

    fn recording_context() -> RecordContext {
        record_context(record_env(&complete_record_env())).unwrap()
    }

    #[test]
    fn a_baseline_is_recorded_whole_or_not_at_all() {
        let manifest = recording_manifest(&["a", "b", "c"]);
        let context = recording_context();
        let whole = |m: BTreeMap<String, f64>| vec![m.clone(), m.clone(), m];
        let partial = whole(means(&[("a", 1.0), ("c", 3.0)]));
        let error = recorded_baseline(
            &context,
            None,
            &manifest,
            &partial,
            &BTreeMap::new(),
            "2026-09-10T00:00:00Z".to_owned(),
        )
        .expect_err("a partial record is the defect, not a convenience");
        assert!(
            error.contains('b'),
            "the error names what is missing: {error}"
        );

        let complete = whole(means(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]));
        let recorded = recorded_baseline(
            &context,
            None,
            &manifest,
            &complete,
            &BTreeMap::new(),
            "2026-09-10T00:00:00Z".to_owned(),
        )
        .expect("every row measured in every sample");
        assert_eq!(recorded.benchmarks.len(), 3);
        assert_eq!(recorded.samples, 3);
        assert_eq!(recorded.git_revision, context.revision);
        assert_eq!(recorded.host, context.host);
        assert_eq!(recorded.profile, BASELINE_PROFILE);
        assert_eq!(recorded.comment, BASELINE_COMMENT);
        // Manifest order, so the file and the bounds it answers to read
        // side by side.
        let ids: Vec<_> = recorded.benchmarks.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[test]
    fn one_run_is_not_enough_to_record_a_baseline() {
        let manifest = recording_manifest(&["a"]);
        let sample = means(&[("a", 1.0)]);
        for count in 0..MIN_RECORD_SAMPLES {
            let samples = vec![sample.clone(); count];
            let error = recorded_baseline(
                &recording_context(),
                None,
                &manifest,
                &samples,
                &BTreeMap::new(),
                "now".to_owned(),
            )
            .expect_err("a baseline describes where a benchmark usually lands");
            assert!(error.contains("at least"), "{error}");
        }
        assert!(
            recorded_baseline(
                &recording_context(),
                None,
                &manifest,
                &vec![sample; MIN_RECORD_SAMPLES],
                &BTreeMap::new(),
                "now".to_owned(),
            )
            .is_ok()
        );
    }

    #[test]
    fn a_recorded_row_is_the_median_not_the_fastest_run() {
        // The GL-114 failure this rule exists for: recording from the quickest
        // run of a session put `managed_credential/verify` in at 794.5 ns
        // against 827-843 ns everywhere else, and the next run failed it.
        let manifest = recording_manifest(&["a"]);
        let samples = vec![
            means(&[("a", 794.5)]),
            means(&[("a", 842.9)]),
            means(&[("a", 835.2)]),
        ];
        let recorded = recorded_baseline(
            &recording_context(),
            None,
            &manifest,
            &samples,
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        assert_eq!(recorded.benchmarks[0].mean_ns, 835.2);
        assert_eq!(median_of(vec![3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median_of(vec![4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median_of(Vec::new()), None);
    }

    #[test]
    fn calibration_medians_reject_invalid_means_and_avoid_intermediate_overflow() {
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(median_of(vec![1.0, value, 2.0]), None);
        }
        assert_eq!(median_of(vec![f64::MAX, f64::MAX]), Some(f64::MAX));
        assert_eq!(
            median_of(vec![f64::MIN_POSITIVE; 4]),
            Some(f64::MIN_POSITIVE)
        );
    }

    #[test]
    fn a_sample_requires_a_safe_identity_and_nonempty_positive_finite_means() {
        let mut sample = Sample {
            run_id: "123".to_owned(),
            context: recording_context(),
            recorded_at: "2026-09-11T00:00:00Z".to_owned(),
            means: means(&[("a", 1.0)]),
        };
        assert!(sample.validate().is_ok());
        for id in ["", "../123", "abc"] {
            sample.run_id = id.to_owned();
            assert!(sample.validate().is_err());
        }
        sample.run_id = "123".to_owned();
        sample.means.clear();
        assert!(sample.validate().is_err());
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            sample.means = means(&[("a", value)]);
            assert!(sample.validate().is_err());
        }
    }

    #[test]
    fn a_row_missing_from_any_sample_is_not_recorded_from_the_rest() {
        // Otherwise each row's "median" is taken over a different population,
        // and the file says nothing about which.
        let manifest = recording_manifest(&["a", "b"]);
        let samples = vec![
            means(&[("a", 1.0), ("b", 2.0)]),
            means(&[("a", 1.0)]),
            means(&[("a", 1.0), ("b", 2.0)]),
        ];
        let error = recorded_baseline(
            &recording_context(),
            None,
            &manifest,
            &samples,
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .expect_err("b was not measured by every sample");
        assert!(error.contains('b'), "{error}");
    }

    #[test]
    fn deposited_samples_are_read_back_only_for_the_revision_being_recorded() {
        let dir = std::env::temp_dir().join(format!(
            "tollgate-perf-gate-samples-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        let mine = "e0fa2695772f000000000000000000000000abcd";
        let other = "6aac1c4000000000000000000000000000000000";
        for (revision, at, value) in [
            (mine, "2026-09-10T18:00:00Z", 1.0),
            (mine, "2026-09-10T18:10:00Z", 2.0),
            // A sample from another revision measured different code; folding
            // it into the median would describe neither.
            (other, "2026-09-10T18:20:00Z", 99.0),
        ] {
            write_sample(
                &dir,
                &Sample {
                    run_id: format!("{value:.0}"),
                    context: RecordContext {
                        revision: revision.to_owned(),
                        ..recording_context()
                    },
                    recorded_at: at.to_owned(),
                    means: means(&[("a", value)]),
                },
            )
            .unwrap();
        }
        // A stray non-JSON file in the directory is ignored, not fatal.
        std::fs::write(dir.join("notes.txt"), "scratch").unwrap();

        let mut values: Vec<f64> = load_samples(
            &dir,
            &RecordContext {
                revision: mine.to_owned(),
                ..recording_context()
            },
        )
        .unwrap()
        .iter()
        .map(|sample| sample.means["a"])
        .collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(values, vec![1.0, 2.0]);
        assert_eq!(
            load_samples(
                &dir,
                &RecordContext {
                    revision: other.to_owned(),
                    ..recording_context()
                }
            )
            .unwrap()
            .len(),
            1
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recording_carries_forward_measured_regression_bounds() {
        let manifest = recording_manifest(&["a", "b"]);
        let previous = Baseline {
            host: recording_context().host,
            recorded_at: "old".to_owned(),
            git_revision: "old".to_owned(),
            profile: "old".to_owned(),
            samples: 3,
            comment: String::new(),
            benchmarks: vec![BaselineEntry {
                id: "a".to_owned(),
                mean_ns: 1.0,
                max_regression: 0.15,
            }],
        };
        let recorded = recorded_baseline(
            &recording_context(),
            Some(&previous),
            &manifest,
            &vec![means(&[("a", 2.0), ("b", 3.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        // A widened bound is dispersion evidence someone measured; a
        // wholesale re-record must not quietly discard it.
        assert_eq!(recorded.benchmarks[0].max_regression, 0.15);
        assert_eq!(recorded.benchmarks[0].mean_ns, 2.0);
        assert_eq!(recorded.benchmarks[1].max_regression, 0.05);
    }

    #[test]
    fn recording_refuses_to_overwrite_another_hosts_baseline() {
        let manifest = recording_manifest(&["a"]);
        let mut previous = Baseline {
            host: recording_context().host,
            recorded_at: "old".to_owned(),
            git_revision: "old".to_owned(),
            profile: "old".to_owned(),
            samples: 3,
            comment: String::new(),
            benchmarks: vec![BaselineEntry {
                id: "a".to_owned(),
                mean_ns: 1.0,
                max_regression: 0.05,
            }],
        };
        previous.host.id = "perf-i9-10920x".to_owned();
        let error = recorded_baseline(
            &recording_context(),
            Some(&previous),
            &manifest,
            &vec![means(&[("a", 2.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .expect_err("a run on one host must not replace another host's contract");
        assert!(error.contains("perf-i9-10920x"), "{error}");
    }

    #[test]
    fn a_staged_baseline_is_promoted_only_after_it_validates() {
        let dir = std::env::temp_dir().join(format!(
            "tollgate-perf-gate-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("perf_baseline.json");
        std::fs::write(&path, "{\"previous\": \"good\"}\n").unwrap();

        let mut baseline = recorded_baseline(
            &recording_context(),
            None,
            &recording_manifest(&["a"]),
            &vec![means(&[("a", 2.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        // Invalid by the same rule the gate reads a baseline with, so the
        // staged copy must never become the file.
        baseline.benchmarks[0].mean_ns = 0.0;
        assert!(
            write_baseline(
                StagedFile::create(path.with_extension("json.staged")).unwrap(),
                &path,
                &baseline
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"previous\": \"good\"}\n",
            "a failed record leaves the last known good baseline in place"
        );
        assert!(
            !path.with_extension("json.staged").exists(),
            "the staged copy is cleaned up"
        );

        baseline.benchmarks[0].mean_ns = 2.0;
        write_baseline(
            StagedFile::create(path.with_extension("json.staged")).unwrap(),
            &path,
            &baseline,
        )
        .unwrap();
        let promoted: Baseline =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(promoted.benchmarks[0].mean_ns, 2.0);
        assert_eq!(promoted.comment, BASELINE_COMMENT);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_recorded_row_writes_its_bound_only_when_it_overrides_the_default() {
        let recorded = recorded_baseline(
            &recording_context(),
            None,
            &recording_manifest(&["a"]),
            &vec![means(&[("a", 2.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        let text = serde_json::to_string(&recorded).unwrap();
        assert!(
            !text.contains("max_regression"),
            "the default bound stays implicit: {text}"
        );
        let mut widened = recorded;
        widened.benchmarks[0].max_regression = 0.15;
        assert!(
            serde_json::to_string(&widened)
                .unwrap()
                .contains("\"max_regression\":0.15")
        );
    }

    #[test]
    fn a_partial_run_is_not_a_sample() {
        // The deposit rule and the median rule have to agree. GL-114 had a run
        // interrupted after 33 of 58 benchmarks deposit a sample that the
        // recording step then refused for exactly that reason, leaving a file
        // that would have poisoned every later attempt.
        let manifest = recording_manifest(&["a", "b"]);
        let partial = means(&[("a", 1.0)]);
        assert!(
            !manifest
                .benchmarks
                .iter()
                .all(|entry| partial.contains_key(&entry.id))
        );
        let full = means(&[("a", 1.0), ("b", 2.0)]);
        assert!(
            manifest
                .benchmarks
                .iter()
                .all(|entry| full.contains_key(&entry.id))
        );
        // And the recording step refuses the partial sample independently.
        assert!(
            recorded_baseline(
                &recording_context(),
                None,
                &manifest,
                &[full.clone(), partial, full],
                &BTreeMap::new(),
                "now".to_owned(),
            )
            .is_err()
        );
    }

    #[test]
    fn an_unreadable_row_withholds_a_failure_but_never_manufactures_one() {
        let entry = BaselineEntry {
            id: "admission/request_rate_token".to_owned(),
            mean_ns: 126.3,
            max_regression: 0.05,
        };
        // The GL-114 case: the gate flagged this row's own spread unreadable in
        // the same report it used the number to fail the run.
        assert_eq!(
            evaluate_regression(&entry.id, Some(173.1), Some(&entry), true, true).status,
            "inconclusive"
        );
        assert_eq!(
            evaluate_regression(&entry.id, Some(173.1), Some(&entry), true, false).status,
            "regressed"
        );
        // Instability is a reason to withhold a failure, not to withhold a
        // pass: a row inside its bound passes however wide its interval was.
        assert_eq!(
            evaluate_regression(&entry.id, Some(126.0), Some(&entry), true, true).status,
            "pass"
        );
        // One inconclusive row among rows that did reach a verdict is
        // tolerated; a run in which *every* comparable row was inconclusive
        // measured nothing and must not read as a pass.
        let inconclusive = evaluate_regression(&entry.id, Some(173.1), Some(&entry), true, true);
        let passing = evaluate_regression("steady", Some(1.0), Some(&entry), true, false);
        assert!(regressions_reached_a_verdict(&[
            inconclusive.clone(),
            passing
        ]));
        assert!(!regressions_reached_a_verdict(&[inconclusive]));
        // A run with no baseline rows at all is not condemned by this guard;
        // `baseline_verdict_missing` is what speaks to that.
        assert!(regressions_reached_a_verdict(&[]));
    }

    #[test]
    fn rows_known_to_disperse_do_not_vote_on_whether_the_host_was_disturbed() {
        let entry = |id: &str, bound: f64| BaselineEntry {
            id: id.to_owned(),
            mean_ns: 100.0,
            max_regression: bound,
        };
        let rows = [
            entry("contended_a", 0.15),
            entry("contended_b", 0.20),
            entry("steady", 0.05),
        ];
        let by_id: BTreeMap<String, &BaselineEntry> =
            rows.iter().map(|e| (e.id.clone(), e)).collect();
        let prone = dispersion_prone(Some(&by_id));
        assert_eq!(
            prone,
            BTreeSet::from(["contended_a".to_owned(), "contended_b".to_owned()])
        );
        // No baseline to consult means no row is known to disperse, so the
        // breadth signal falls back to counting everything.
        assert!(dispersion_prone(None).is_empty());

        let policy = TrustPolicy::default();
        let none = BTreeMap::new();
        // The GL-114 shape: six of fifty-eight rows unstable trips the tenth,
        // but four of them are the contention family doing what it always
        // does. Judged over the steady rows alone, the run is readable.
        assert_eq!(
            assess_trust(&none, &none, 6, 58, &policy),
            Trust::Unreadable {
                unstable: 6,
                measured: 58
            }
        );
        assert_eq!(assess_trust(&none, &none, 2, 54, &policy), Trust::NoHistory);
        // A genuinely disturbed host still fails: the steady rows go too.
        assert!(matches!(
            assess_trust(&none, &none, 12, 54, &policy),
            Trust::Unreadable { .. }
        ));
    }

    #[test]
    fn a_full_run_that_reached_no_baseline_verdict_is_not_a_pass() {
        for skipped in ["no-baseline-file", "host-unset", "host-mismatch"] {
            assert!(
                baseline_verdict_missing(GateMode::Full, Some(skipped)),
                "{skipped} leaves the host-specific bound unchecked"
            );
            // A diagnostic run on another host says so in its mode; it is not
            // being offered as acceptance evidence.
            assert!(!baseline_verdict_missing(
                GateMode::RatiosOnly,
                Some(skipped)
            ));
        }
        // Already reported, with its own exit code and explanation.
        assert!(!baseline_verdict_missing(
            GateMode::Full,
            Some("untrusted-run")
        ));
        assert!(!baseline_verdict_missing(GateMode::Full, None));
    }

    #[test]
    fn run_drift_summarises_how_the_whole_run_sat_against_the_baseline() {
        let row = |ratio: Option<f64>| RegressionRow {
            id: "id".to_owned(),
            measured_ns: Some(1.0),
            baseline_ns: ratio.map(|_| 1.0),
            ratio,
            max_regression: Some(0.05),
            status: "pass",
        };
        assert_eq!(run_drift(&[]), None);
        assert_eq!(run_drift(&[row(None)]), None);

        for (ratios, p25, median, p75) in [
            (vec![1.5], 1.5, 1.5, 1.5),
            (vec![2.0, 1.0], 1.0, 2.0, 2.0),
            (vec![3.0, 1.0, 2.0], 1.0, 2.0, 3.0),
            (vec![4.0, 1.0, 3.0, 2.0], 2.0, 3.0, 4.0),
        ] {
            let rows: Vec<_> = ratios.iter().map(|ratio| row(Some(*ratio))).collect();
            assert_eq!(
                run_drift(&rows),
                Some(RunDrift {
                    compared: ratios.len(),
                    median_ratio: median,
                    p25_ratio: p25,
                    p75_ratio: p75,
                })
            );
        }

        // The GL-114 shape: most of the run shifted a little, a few rows moved
        // a lot. The median is what says which of those the run was.
        let ratios = [1.00, 1.02, 1.03, 1.04, 1.36];
        let drift = run_drift(&ratios.map(|r| row(Some(r)))).expect("five comparable rows");
        assert_eq!(drift.compared, 5);
        assert!((drift.median_ratio - 1.03).abs() < 1e-9);
        assert!((drift.p25_ratio - 1.02).abs() < 1e-9);
        assert!((drift.p75_ratio - 1.04).abs() < 1e-9);
        // Unreadable rows never reach the summary.
        assert_eq!(
            run_drift(&[row(Some(f64::NAN)), row(Some(1.5))])
                .unwrap()
                .compared,
            1
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
            samples: 3,
            comment: String::new(),
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
            evaluate_regression("a", Some(1_000.0), Some(&entry), false, false).status,
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
        let passing = evaluate_regression(&baseline.id, Some(104.0), Some(&baseline), true, false);
        let regressed =
            evaluate_regression(&baseline.id, Some(106.0), Some(&baseline), true, false);
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
        assert_eq!(
            manifest.trust.unstable_fraction,
            default_unstable_fraction()
        );
    }

    #[test]
    fn misspelled_gate_settings_never_become_defaults() {
        let minimal = serde_json::json!({
            "benchmarks": [{"id": "a", "target_ns": 1.0, "threshold_ns": 2.0}]
        });
        for key in ["trust", "ratios"] {
            let mut input = minimal.clone();
            input[format!("{key}_typo")] = serde_json::json!({});
            let error = serde_json::from_value::<Manifest>(input)
                .err()
                .expect("unknown key must fail");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}_typo`"))
            );
        }
        for key in [
            "moved_shift",
            "moved_fraction",
            "max_ci_width",
            "unstable_fraction",
        ] {
            let mut input = minimal.clone();
            input["trust"] = serde_json::json!({format!("{key}_typo"): 0.2});
            let error = serde_json::from_value::<Manifest>(input)
                .err()
                .expect("unknown trust key must fail");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}_typo`"))
            );
        }
    }

    #[test]
    fn partial_trust_settings_preserve_explicit_values_and_omitted_defaults() {
        for (field, value) in [
            ("moved_shift", 0.2),
            ("moved_fraction", 0.25),
            ("max_ci_width", 0.05),
            ("unstable_fraction", 0.15),
        ] {
            let input = serde_json::json!({"benchmarks": [], "trust": {field: value}});
            let parsed: Manifest = serde_json::from_value(input).unwrap();
            let mut expected = serde_json::json!({
                "moved_shift": 0.40, "moved_fraction": 0.34,
                "max_ci_width": 0.10, "unstable_fraction": 0.10,
            });
            expected[field] = value.into();
            assert_eq!(serde_json::to_value(parsed.trust).unwrap(), expected);
        }
        let empty: TrustPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(
            serde_json::to_value(empty).unwrap(),
            serde_json::json!({
                "moved_shift": 0.40, "moved_fraction": 0.34,
                "max_ci_width": 0.10, "unstable_fraction": 0.10,
            })
        );
    }

    #[test]
    fn gate_metadata_is_explicit_and_does_not_hide_unknown_settings() {
        let fixture = serde_json::json!({
            "_comment": "operator documentation",
            "_reserved_ids": {"planned": ["b"]},
            "benchmarks": [{"_comment": "row", "id": "a", "target_ns": 1.0, "threshold_ns": 2.0}],
            "ratios": [{"_comment": "ratio", "numerator": "a", "denominator": "b", "max_ratio": 1.2}],
            "trust": {},
        });
        let manifest: Manifest = serde_json::from_value(fixture.clone()).unwrap();
        assert_eq!(manifest.benchmarks[0].id, "a");
        assert_eq!(manifest.ratios[0].max_ratio, 1.2);
        for pointer in ["", "/benchmarks/0", "/ratios/0", "/trust"] {
            let mut input = fixture.clone();
            input.pointer_mut(pointer).unwrap()["_unrecognized_setting"] = 0.1.into();
            let error = serde_json::from_value::<Manifest>(input)
                .err()
                .expect("unknown key must fail");
            assert!(
                error
                    .to_string()
                    .contains("unknown field `_unrecognized_setting`")
            );
        }
        let mut input = fixture;
        input["trust"]["_comment"] = "not a supported trust setting".into();
        assert!(serde_json::from_value::<Manifest>(input).is_err());
    }

    #[test]
    fn baseline_setting_typos_are_rejected_without_changing_legacy_defaults() {
        let recorded = recorded_baseline(
            &recording_context(),
            None,
            &recording_manifest(&["a"]),
            &vec![means(&[("a", 100.0)]); 3],
            &BTreeMap::new(),
            "now".to_owned(),
        )
        .unwrap();
        let mut input = serde_json::to_value(recorded).unwrap();
        input["benchmarks"][0]["max_regression"] = 0.15.into();
        let parsed: Baseline = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(parsed.benchmarks[0].max_regression, 0.15);
        for (pointer, key) in [("", "samples"), ("/benchmarks/0", "max_regression")] {
            let mut invalid = input.clone();
            let object = invalid
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap();
            let value = object.remove(key).unwrap();
            object.insert(format!("{key}_typo"), value);
            let error = serde_json::from_value::<Baseline>(invalid)
                .expect_err("unknown baseline key must fail");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}_typo`"))
            );
        }
        let mut invalid = input.clone();
        invalid["host"]["os_typo"] = "wrong setting".into();
        assert!(serde_json::from_value::<Baseline>(invalid).is_err());
        input.as_object_mut().unwrap().remove("samples");
        input["benchmarks"][0]
            .as_object_mut()
            .unwrap()
            .remove("max_regression");
        let legacy: Baseline = serde_json::from_value(input).unwrap();
        assert_eq!(legacy.samples, 1);
        assert_eq!(legacy.benchmarks[0].max_regression, 0.05);
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
    /// GL-112: every CI job starts from a clean workspace, so `previous` is
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
    /// Renaming FAIL to UNREADABLE would have left GL-112 exactly where it was:
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
            _comment: serde::de::IgnoredAny,
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
                _comment: serde::de::IgnoredAny,
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
    /// This is GL-112. Two pipelines on the same shared runner measured the same
    /// unchanged code at ×3.40 and ×13.11, and the second failed a merge
    /// request whose diff could not touch the mechanism — because the gate
    /// printed "UNSTABLE spread" for one side and then divided it anyway.
    #[test]
    fn an_unstable_side_makes_a_ratio_inconclusive_rather_than_a_verdict() {
        let bound = RatioBound {
            _comment: serde::de::IgnoredAny,
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
            _comment: serde::de::IgnoredAny,
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

    // ---- derived allowances (GL-141) ----------------------------------------

    fn revision(n: u8) -> String {
        format!("{n:040x}")
    }

    fn history_run(run: u32, revision: &str, value: f64) -> Sample {
        let mut context = recording_context();
        context.revision = revision.to_owned();
        Sample {
            run_id: run.to_string(),
            context,
            recorded_at: "2026-09-25T00:00:00Z".to_owned(),
            means: means(&[("row", value)]),
        }
    }

    fn history(runs: &[(u8, f64)]) -> Vec<Sample> {
        (0u32..)
            .zip(runs)
            .map(|(run, &(rev, value))| history_run(run, &revision(rev), value))
            .collect()
    }

    fn derive(runs: &[(u8, f64)]) -> Option<DerivedAllowance> {
        derive_allowances(&history(runs), &recording_context(), ["row"].into_iter()).remove("row")
    }

    /// A code change between revisions moves a row's level, not its noise:
    /// two quiet revisions a factor of two apart derive the default.
    #[test]
    fn a_level_change_between_revisions_is_not_spread() {
        let derived = derive(&[
            (1, 100.0),
            (1, 101.0),
            (1, 99.0),
            (2, 200.0),
            (2, 202.0),
            (2, 198.0),
        ])
        .unwrap();
        assert_eq!(derived.allowance, 0.05);
        assert_eq!((derived.runs, derived.revisions), (6, 2));
    }

    /// Spread is the worst run over its own revision's median, plus the
    /// margin, rounded up to the next 0.05 — and an exact multiple is not
    /// pushed a step higher by floating-point residue.
    #[test]
    fn spread_is_the_worst_run_over_its_revision_median() {
        let derived = derive(&[
            (1, 100.0),
            (1, 100.0),
            (1, 110.0),
            (2, 50.0),
            (2, 51.0),
            (2, 58.0),
        ])
        .unwrap();
        // 58 / 51 = x1.137 -> 0.137 + 0.03 -> 0.20
        assert_eq!(derived.allowance, 0.20);
        assert_eq!(round_allowance_up(0.10), 0.10);
        assert_eq!(round_allowance_up(0.07 + 0.03), 0.10);
        assert_eq!(round_allowance_up(0.1000001), 0.15);
    }

    /// One run far above its revision's median is set aside and listed,
    /// never absorbed into the allowance.
    #[test]
    fn an_excursion_is_listed_not_absorbed() {
        let derived = derive(&[
            (1, 100.0),
            (1, 100.0),
            (1, 102.0),
            (1, 250.0),
            (2, 100.0),
            (2, 101.0),
            (2, 99.0),
        ])
        .unwrap();
        assert_eq!(derived.excursions.len(), 1);
        assert!(derived.excursions[0] > 2.0);
        // The median of 100, 100, 102, 250 is 101: the worst believed run is
        // x1.01 over it, plus the margin, 0.05 — the excursion adds nothing.
        assert_eq!(derived.allowance, 0.05);
    }

    /// Too little history derives nothing, so the carried allowance stands.
    #[test]
    fn thin_history_derives_nothing() {
        assert_eq!(
            derive(&[
                (1, 100.0),
                (1, 130.0),
                (1, 99.0),
                (1, 101.0),
                (1, 98.0),
                (1, 97.0)
            ]),
            None,
            "one revision"
        );
        assert_eq!(
            derive(&[(1, 100.0), (1, 130.0), (2, 99.0), (2, 101.0), (3, 98.0)]),
            None,
            "five runs"
        );
        assert_eq!(
            derive(&[
                (1, 100.0),
                (2, 130.0),
                (3, 99.0),
                (4, 101.0),
                (5, 98.0),
                (6, 97.0)
            ]),
            None,
            "no revision has two runs"
        );
    }

    /// Only this host, environment and profile count; the revision is the one
    /// thing that may differ.
    #[test]
    fn history_from_another_host_is_ignored() {
        let mut runs = history(&[
            (1, 100.0),
            (1, 101.0),
            (1, 99.0),
            (2, 100.0),
            (2, 101.0),
            (2, 99.0),
        ]);
        for run in &mut runs[..3] {
            run.context.host.id = "another-host".to_owned();
        }
        assert!(
            derive_allowances(&runs, &recording_context(), ["row"].into_iter()).is_empty(),
            "three runs on one revision remain"
        );
    }

    /// The carried allowance is a floor: history can widen a row, never
    /// narrow it.
    #[test]
    fn derived_allowances_widen_but_never_narrow() {
        let manifest = recording_manifest(&["wide", "narrow", "none"]);
        let context = recording_context();
        let mut previous = recorded_baseline(
            &context,
            None,
            &manifest,
            &vec![means(&[("wide", 1.0), ("narrow", 1.0), ("none", 1.0)]); 3],
            &BTreeMap::new(),
            "then".to_owned(),
        )
        .unwrap();
        for entry in &mut previous.benchmarks {
            if entry.id == "narrow" {
                entry.max_regression = 0.30;
            }
        }
        let derived: BTreeMap<String, DerivedAllowance> = ["wide", "narrow"]
            .into_iter()
            .map(|id| {
                (
                    id.to_owned(),
                    DerivedAllowance {
                        allowance: 0.20,
                        runs: 6,
                        revisions: 2,
                        excursions: Vec::new(),
                    },
                )
            })
            .collect();
        let recorded = recorded_baseline(
            &context,
            Some(&previous),
            &manifest,
            &vec![means(&[("wide", 1.0), ("narrow", 1.0), ("none", 1.0)]); 3],
            &derived,
            "now".to_owned(),
        )
        .unwrap();
        let allowance = |id: &str| {
            recorded
                .benchmarks
                .iter()
                .find(|entry| entry.id == id)
                .unwrap()
                .max_regression
        };
        assert_eq!(allowance("wide"), 0.20, "widened by the history");
        assert_eq!(allowance("narrow"), 0.30, "never narrowed");
        assert_eq!(allowance("none"), 0.05, "no evidence, the default");
        assert_eq!(wide_allowances(Some(&recorded)), Vec::<String>::new());
        let mut wider = recorded;
        wider.benchmarks[0].max_regression = 0.55;
        assert_eq!(
            wide_allowances(Some(&wider)),
            vec![format!("{} (0.55)", wider.benchmarks[0].id)]
        );
    }

    /// The history keeps its newest runs by run id and removes the oldest,
    /// leaving anything that is not a run file alone.
    #[test]
    fn run_history_keeps_its_newest_window() {
        let dir = std::env::temp_dir().join(format!("perf-gate-history-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for id in [5u32, 1, 3, 2, 4] {
            write_sample(&dir, &history_run(id, &revision(1), 100.0)).unwrap();
        }
        std::fs::write(dir.join("notes.txt"), "kept").unwrap();
        assert_eq!(prune_history(&dir, 3).unwrap(), 2);
        let mut kept: Vec<String> = load_history(&dir).into_iter().map(|s| s.run_id).collect();
        kept.sort();
        assert_eq!(kept, ["3", "4", "5"]);
        assert!(dir.join("notes.txt").exists());
        assert_eq!(prune_history(&dir, 3).unwrap(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn run_history_is_an_option_taken_once() {
        let base = ["m.json", "c", "r.json", "f"];
        let with = |extra: &[&str]| {
            parse_args(&extra.iter().chain(base.iter()).copied().collect::<Vec<_>>())
        };
        match with(&["--run-history", "h"]).unwrap() {
            Command::Run {
                run_history_path, ..
            } => assert_eq!(run_history_path, Some(PathBuf::from("h"))),
            other => panic!("{other:?}"),
        }
        assert!(with(&["--run-history", "h", "--run-history", "i"]).is_err());
        assert!(with(&["--run-history"]).is_err());
    }
}
