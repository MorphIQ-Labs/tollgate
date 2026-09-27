//! Loopback load gate: measures the pricing-api over persistent HTTP/1.1
//! connections with admission on and off, and gates the delta against
//! `testing/load_thresholds.json`. It reports the original sequential
//! scenario and concurrent connections contending on one account.
//!
//! Usage: `load_gate [--evidence] <thresholds.json> <report.json>`
//!
//! Each client is a raw blocking `TcpStream` speaking minimal HTTP/1.1, so the
//! measurement is a persistent-loopback gate and adds no
//! client-library noise. Percentiles and aggregate throughput are computed
//! over the measured requests only (warmup excluded). Absolute latency and
//! throughput gate on the controlled host; each admitted-vs-baseline ratio is
//! meaningful only when the configured connection count is held constant.

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;

use serde::{Deserialize, Deserializer, Serialize};

use pricing_api::{
    DemoTenant, PricingConnection, build_app_with_capacity, demo_tenant, demo_tenants,
};
use tollgate_admission::ExecutionCapacityMode;
use tollgate_core::{CapacityClass, LocalSharding};

#[path = "load_gate/client.rs"]
mod client;
#[path = "load_gate/report.rs"]
mod report;
#[cfg(test)]
use client::throughput_for_window;
use client::{ClientWork, MeasuredSamples, drive_clients};
use report::{RunContext, report_failure, write_report};

#[cfg(test)]
#[path = "../../tests/fixtures/load_failures.rs"]
mod load_failures;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Thresholds {
    #[serde(default)]
    _comment: serde::de::IgnoredAny,
    warmup_requests: usize,
    measured_requests: usize,
    /// Persistent clients driven concurrently against the same account.
    concurrent_connections: usize,
    /// Admitted p50 may exceed baseline p50 by at most this factor.
    max_p50_overhead_ratio: f64,
    /// Concurrent same-account admitted p50 may exceed its like-for-like
    /// baseline p50 by at most this factor.
    max_concurrent_p50_overhead_ratio: f64,
    /// Controlled-host absolute ceilings for the admitted run (nanoseconds).
    /// Both are `null` in the portable CI manifest; one without the other is
    /// invalid because it would silently create a partial absolute gate.
    #[serde(deserialize_with = "deserialize_required_option")]
    max_p50_ns: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    max_p99_ns: Option<f64>,
    /// Controlled-host admitted throughput floors (requests/second). These
    /// are explicit `null` in shared CI for the same reason as the absolute
    /// latency ceilings: scheduler and CPU ownership are not controlled.
    #[serde(deserialize_with = "deserialize_required_option")]
    min_throughput_requests_per_second: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    min_concurrent_throughput_requests_per_second: Option<f64>,
    /// Distinct-account admitted p50 against its like-for-like baseline, at
    /// the same connection count (GL-99). `null` where it has not been
    /// calibrated, which reports as *disabled* rather than as passing.
    #[serde(deserialize_with = "deserialize_required_option")]
    max_distinct_account_p50_overhead_ratio: Option<f64>,
    /// Assured p50 under a reserved gate against the same assured workload
    /// with the gate disabled. `null` until calibrated, as above.
    #[serde(deserialize_with = "deserialize_required_option")]
    max_mixed_assured_p50_overhead_ratio: Option<f64>,
    /// How much less often assured work must be shed than best-effort work,
    /// as a ratio of the two shed fractions.
    ///
    /// The comparative form is the one that matches the invariant. GL-30 says
    /// best-effort work cannot consume the assured *reserve* — not that
    /// assured work is never shed. Five assured connections contending for two
    /// reachable units shed each other sometimes, and that is correct
    /// behaviour; a first draft of this gate asserted zero assured sheds and
    /// failed on 1.08% that no guarantee forbids. What the reserve promises is
    /// that best-effort saturation does not come out of assured work's
    /// capacity, and a shed *advantage* is that claim measured.
    min_assured_shed_advantage: f64,
    /// The same advantage under `Uniform`, which must be small.
    ///
    /// The control, and the reason the advantage above means anything: uniform
    /// bounds the instance exactly as reserved does but treats the classes
    /// alike, so any advantage there comes from the workload rather than the
    /// class. Without it, a scenario where assured connections simply asked
    /// for less would look identical to a working reserve.
    max_uniform_shed_advantage: f64,
    /// The fraction of best-effort requests that must be refused for the
    /// scenario to have proved anything.
    ///
    /// A reserve nobody contends is indistinguishable from no reserve, so a
    /// run that failed to saturate is a run that measured nothing — the same
    /// reasoning that makes an all-inconclusive perf run red rather than
    /// green.
    min_best_effort_shed_fraction: f64,
}

fn deserialize_required_option<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<f64>::deserialize(deserializer)
}

impl Thresholds {
    fn validate(&self) -> Result<NonZeroUsize, String> {
        let connections = NonZeroUsize::new(self.concurrent_connections)
            .ok_or_else(|| "concurrent_connections must be positive".to_owned())?;
        if connections.get() < 2 {
            return Err("concurrent_connections must be at least 2".to_owned());
        }
        if self.measured_requests < connections.get() {
            return Err(
                "measured_requests must give every concurrent connection at least one sample"
                    .to_owned(),
            );
        }
        if self.warmup_requests < connections.get() {
            return Err(
                "warmup_requests must warm every concurrent connection at least once".to_owned(),
            );
        }
        for (name, value) in [
            ("max_p50_overhead_ratio", self.max_p50_overhead_ratio),
            (
                "max_concurrent_p50_overhead_ratio",
                self.max_concurrent_p50_overhead_ratio,
            ),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(format!("{name} must be finite and positive"));
            }
        }
        match (self.max_p50_ns, self.max_p99_ns) {
            (Some(max_p50_ns), Some(max_p99_ns)) => {
                for (name, value) in [("max_p50_ns", max_p50_ns), ("max_p99_ns", max_p99_ns)] {
                    if !value.is_finite() || value <= 0.0 {
                        return Err(format!("{name} must be finite and positive"));
                    }
                }
            }
            (None, None) => {}
            _ => {
                return Err("max_p50_ns and max_p99_ns must both be set or both be null".to_owned());
            }
        }
        match (
            self.min_throughput_requests_per_second,
            self.min_concurrent_throughput_requests_per_second,
        ) {
            (Some(sequential), Some(concurrent)) => {
                for (name, value) in [
                    ("min_throughput_requests_per_second", sequential),
                    ("min_concurrent_throughput_requests_per_second", concurrent),
                ] {
                    if !value.is_finite() || value <= 0.0 {
                        return Err(format!("{name} must be finite and positive"));
                    }
                }
            }
            (None, None) => {}
            _ => {
                return Err("both throughput floors must be set or both must be null".to_owned());
            }
        }
        if !self.min_best_effort_shed_fraction.is_finite()
            || !(0.0..=1.0).contains(&self.min_best_effort_shed_fraction)
        {
            return Err(
                "min_best_effort_shed_fraction must be a finite fraction in [0, 1]".to_owned(),
            );
        }
        for (name, value) in [
            (
                "min_assured_shed_advantage",
                self.min_assured_shed_advantage,
            ),
            (
                "max_uniform_shed_advantage",
                self.max_uniform_shed_advantage,
            ),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(format!("{name} must be finite and positive"));
            }
        }
        if self.min_assured_shed_advantage <= self.max_uniform_shed_advantage {
            return Err(
                "min_assured_shed_advantage must exceed max_uniform_shed_advantage, or the \
                 reserved run is not being held to anything the uniform control does not \
                 already satisfy"
                    .to_owned(),
            );
        }
        if self.min_best_effort_shed_fraction <= 0.0 {
            return Err(
                "min_best_effort_shed_fraction must be positive; a mixed run that sheds \
                 nothing has not saturated and proves nothing"
                    .to_owned(),
            );
        }
        // Two connections is the minimum for the mixed workload to have one of
        // each class, and `Workload::mixed` splits with the odd one assured.
        Ok(connections)
    }
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
struct Percentiles {
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
}

#[derive(Debug, Clone, Copy)]
struct ScenarioMeasurement {
    latency: Percentiles,
    throughput_requests_per_second: f64,
}

#[derive(Serialize)]
struct ThroughputReport {
    baseline_requests_per_second: f64,
    admitted_requests_per_second: f64,
    min_admitted_requests_per_second: Option<f64>,
    /// `None` means the selected manifest deliberately disabled the
    /// controlled-host throughput floor.
    passed: Option<bool>,
}

#[derive(Serialize)]
struct ConcurrentReport {
    connections: usize,
    baseline: Percentiles,
    admitted: Percentiles,
    p50_overhead_ratio: f64,
    max_p50_overhead_ratio: f64,
    throughput: ThroughputReport,
    passed: bool,
}

#[derive(Serialize)]
struct AbsoluteLatencyReport {
    max_p50_ns: Option<f64>,
    max_p99_ns: Option<f64>,
    /// `None` means the selected manifest deliberately disabled absolute
    /// latency enforcement; ratio verdicts remain active.
    passed: Option<bool>,
}

#[derive(Serialize)]
struct Report {
    // Keep the original sequential fields stable for report consumers.
    baseline: Percentiles,
    admitted: Percentiles,
    p50_overhead_ratio: f64,
    sequential_passed: bool,
    absolute_latency: AbsoluteLatencyReport,
    throughput: ThroughputReport,
    concurrent_same_account: ConcurrentReport,
    /// GL-99's two witnesses.
    concurrent_distinct_accounts: ConcurrentReport,
    mixed_saturation: MixedReport,
    passed: bool,
    run: RunContext,
}

/// The mixed workload measured twice — once ungated, once reserved — and what
/// the pair concluded.
#[derive(Serialize)]
struct MixedReport {
    connections: usize,
    capacity_total: u32,
    assured_reserve: u32,
    /// The same accounts and classes with the gate disabled: the denominator
    /// for what the reserve costs, and the control that says the workload
    /// itself sheds nothing.
    ungated_assured: ClassOutcome,
    ungated_best_effort: ClassOutcome,
    /// The same bound without classes: the control.
    uniform_assured: ClassOutcome,
    uniform_best_effort: ClassOutcome,
    uniform_shed_advantage: f64,
    max_uniform_shed_advantage: f64,
    assured: ClassOutcome,
    best_effort: ClassOutcome,
    assured_p50_overhead_ratio: f64,
    max_assured_p50_overhead_ratio: Option<f64>,
    assured_shed_fraction: f64,
    assured_shed_advantage: f64,
    min_assured_shed_advantage: f64,
    best_effort_shed_fraction: f64,
    min_best_effort_shed_fraction: f64,
    throughput_requests_per_second: f64,
    verdict: MixedVerdict,
}

/// The single-contract body every pre-#99 scenario sent, kept as the fixture
/// `body(1)` is pinned against.
///
/// Test-only now: the scenarios build their bodies with `body`, and this is
/// the literal that says `body(1)` still produces what they were calibrated
/// with.
#[cfg(test)]
const BODY: &str =
    r#"{"contracts":[{"spot":100.0,"strike":105.0,"rate":0.05,"vol":0.2,"tte_years":0.25}]}"#;
/// Contracts per request in the mixed-saturation scenario.
///
/// Chosen so the kernel — which is what the permit covers — is a large enough
/// share of the round trip for a bounded pool to be contended by ten
/// connections. It stays inside the example snapshot's 1,024-item limit and
/// its rate burst, so the scenario measures capacity rather than tripping a
/// different guard.
const MIXED_CONTRACTS: usize = 512;

const CONTRACT: &str = r#"{"spot":100.0,"strike":105.0,"rate":0.05,"vol":0.2,"tte_years":0.25}"#;

/// One connection's HTTP/1.1 request, built once and replayed.
///
/// `None` for the key is the no-admission baseline, which sends no
/// `Authorization` header at all rather than an ignored one.
fn http_request(address: std::net::SocketAddr, api_key: Option<&str>, body: &str) -> String {
    let auth = api_key.map_or_else(String::new, |key| {
        format!("Authorization: Bearer {key}\r\n")
    });
    format!(
        "POST /v1/price HTTP/1.1\r\nHost: {address}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len(),
    )
}

/// A request body of `contracts` identical contracts.
///
/// One contract reproduces `BODY` exactly, so the scenarios calibrated against
/// it are byte-identical rather than merely equivalent.
fn body(contracts: usize) -> String {
    let mut body = String::from(r#"{"contracts":["#);
    for index in 0..contracts {
        if index > 0 {
            body.push(',');
        }
        body.push_str(CONTRACT);
    }
    body.push_str("]}");
    body
}
const USAGE: &str = "usage: load_gate [--evidence] <thresholds.json> <report.json>";

#[derive(Debug, PartialEq)]
enum Command {
    Run {
        thresholds_path: PathBuf,
        report_path: PathBuf,
        verdict_mode: VerdictMode,
    },
    Help,
    Version,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum VerdictMode {
    Gate,
    Evidence,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MeasurementVerdict {
    Passed,
    EvidenceMiss,
    GateFailure,
}

impl MeasurementVerdict {
    fn exit_code(self) -> u8 {
        match self {
            Self::Passed | Self::EvidenceMiss => 0,
            Self::GateFailure => 1,
        }
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
    let mut verdict_mode = VerdictMode::Gate;
    let mut positional = Vec::with_capacity(args.len());
    for arg in &args[..option_end] {
        match arg.as_str() {
            "--evidence" if verdict_mode == VerdictMode::Gate => {
                verdict_mode = VerdictMode::Evidence;
            }
            "--evidence" => return Err("--evidence may be specified only once".to_owned()),
            value if value.starts_with('-') => return Err(format!("unknown option: {value}")),
            _ => positional.push(arg.clone()),
        }
    }
    if let Some(index) = separator {
        positional.extend_from_slice(&args[index + 1..]);
    }
    match positional.as_slice() {
        [thresholds_path, report_path] => Ok(Command::Run {
            thresholds_path: PathBuf::from(thresholds_path),
            report_path: PathBuf::from(report_path),
            verdict_mode,
        }),
        _ => Err(USAGE.to_owned()),
    }
}

fn measurement_verdict(passed: bool, verdict_mode: VerdictMode) -> MeasurementVerdict {
    match (passed, verdict_mode) {
        (true, _) => MeasurementVerdict::Passed,
        (false, VerdictMode::Evidence) => MeasurementVerdict::EvidenceMiss,
        (false, VerdictMode::Gate) => MeasurementVerdict::GateFailure,
    }
}

fn distribute_requests(total: usize, connections: NonZeroUsize) -> Vec<usize> {
    let connections = connections.get();
    let per_connection = total / connections;
    let remainder = total % connections;
    (0..connections)
        .map(|index| per_connection + usize::from(index < remainder))
        .collect()
}

fn percentile(sorted: &[u64], q: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index] as f64
}

/// Which accounts a scenario serves, and which one each connection speaks as.
///
/// The gate served exactly one account until GL-99, so every connection was the
/// same principal and cross-account contention was explicitly out of scope.
/// Two of the witnesses this change adds are about precisely that, and a third
/// needs two *classes* — which, because the class is account-owned, is also
/// two accounts.
#[derive(Clone)]
struct Workload {
    tenants: Vec<DemoTenant>,
    capacity: ExecutionCapacityMode,
    /// Contracts per request, which is what decides whether execution capacity
    /// can bind at all.
    ///
    /// The permit covers the computational kernel only. At one contract the
    /// kernel is ~100 ns of an ~86 µs round trip, so ten connections produce
    /// on the order of 0.01 concurrent permit holders and a pool is never
    /// contended — measured, before this: 1 refusal in 5,000 requests. A
    /// scenario about execution capacity has to make execution the expensive
    /// part, which is also the shape of a service that would configure a gate.
    contracts: usize,
}

impl Workload {
    /// The original single-account workload. Every existing scenario keeps it,
    /// so their recorded ratios still measure what they were calibrated on.
    fn primary() -> Self {
        Workload {
            tenants: vec![demo_tenant()],
            capacity: ExecutionCapacityMode::Disabled,
            contracts: 1,
        }
    }

    /// One account per connection, so nothing is shared but the instance.
    fn distinct_accounts(connections: NonZeroUsize) -> Self {
        Workload {
            tenants: demo_tenants(connections.get(), 0),
            capacity: ExecutionCapacityMode::Disabled,
            contracts: 1,
        }
    }

    /// Half the connections assured, half best-effort, against a gate too
    /// small for all of them, on a request big enough for the kernel to be
    /// worth gating.
    fn mixed(connections: NonZeroUsize, capacity: ExecutionCapacityMode) -> Self {
        let assured = connections.get().div_ceil(2);
        Workload {
            tenants: demo_tenants(assured, connections.get() - assured),
            capacity,
            contracts: MIXED_CONTRACTS,
        }
    }

    /// Connection `index` speaks as this tenant. Round-robin, so a workload
    /// with one tenant is the original behaviour by construction rather than
    /// by a branch.
    fn tenant(&self, index: usize) -> &DemoTenant {
        &self.tenants[index % self.tenants.len()]
    }
}

/// Run one scenario in-process; returns latency percentiles and aggregate
/// measured-window throughput.
async fn run_scenario(
    admission: bool,
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
    workload: &Workload,
) -> Result<ScenarioMeasurement, String> {
    let measured_samples = measure(admission, connections, warmup, measured, workload).await?;
    let shed: usize = measured_samples.outcomes.iter().map(|o| o.shed).sum();
    if shed != 0 {
        // Only the mixed scenario configures a gate small enough to refuse,
        // and it reads the outcomes itself. A shed anywhere else means the
        // service ran out of something this scenario did not intend to test,
        // which must be reported rather than averaged into a percentile.
        return Err(format!(
            "load-gate saw {shed} capacity refusals in a scenario that configured no gate"
        ));
    }
    let mut sorted: Vec<u64> = measured_samples
        .outcomes
        .iter()
        .flat_map(|o| o.samples.iter().copied())
        .collect();
    if sorted.len() != measured {
        return Err(format!(
            "load-gate collected {} samples, expected {measured}",
            sorted.len()
        ));
    }
    sorted.sort_unstable();
    Ok(ScenarioMeasurement {
        latency: percentiles(&sorted),
        throughput_requests_per_second: measured_samples.throughput_requests_per_second,
    })
}

/// One shared unit and one reserve unit: the smallest configuration in which
/// the class changes an outcome, and the only one this workload contends.
///
/// Sizing the pool from the connection count was tried first and does not
/// work, which is worth recording because it looks obviously right. The permit
/// covers the computational kernel only, so ten connections do not produce ten
/// concurrent permit holders — they produce roughly (kernel ÷ round trip) × 10,
/// which even at 512 contracts is well under one. A pool of five refused 11
/// requests in 5,000; a pool of two is what actually contends. A deployment
/// sizes its pool to the hardware its kernel runs on; a *scenario* sizes it to
/// be contended, or it measures nothing.
fn reserved_mode(connections: NonZeroUsize) -> Result<MixedPool, String> {
    if connections.get() < 2 {
        return Err("a mixed workload needs a connection of each class".to_owned());
    }
    Ok(MixedPool {
        total: NonZeroU32::new(2).expect("two is nonzero"),
        assured_reserve: NonZeroU32::new(1).expect("one is nonzero"),
    })
}

/// The mixed scenario's pool, as sizes rather than as a mode.
///
/// It is asked for three things — a reserved mode, a class-blind control of
/// the same size, and two numbers for the report — and returning the enum made
/// each of those a `match` with an `unreachable!` arm that no test could
/// reach. The sizes are the fact; the modes are views of it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MixedPool {
    total: NonZeroU32,
    assured_reserve: NonZeroU32,
}

impl MixedPool {
    /// Class-aware: the measurement.
    const fn reserved(self) -> ExecutionCapacityMode {
        ExecutionCapacityMode::Reserved {
            total: self.total,
            assured_reserve: self.assured_reserve,
        }
    }

    /// The same bound, class-blind: the control.
    const fn uniform(self) -> ExecutionCapacityMode {
        ExecutionCapacityMode::Uniform { total: self.total }
    }
}

/// Assured p50 under the gate against the same assured workload without one.
///
/// Infinite when the ungated run measured nothing, so a missing denominator
/// reports as "immeasurably worse" rather than as a tidy zero that would pass
/// every ceiling.
fn assured_p50_ratio(gated: MixedMeasurement, ungated: MixedMeasurement) -> f64 {
    if ungated.assured.latency.p50_ns > 0.0 {
        gated.assured.latency.p50_ns / ungated.assured.latency.p50_ns
    } else {
        f64::INFINITY
    }
}

/// What one class saw in a mixed workload.
#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
struct ClassOutcome {
    latency: Percentiles,
    /// Requests the instance started and billed.
    served: usize,
    /// Requests refused for want of execution capacity.
    shed: usize,
}

impl ClassOutcome {
    /// Refusals as a fraction of everything this class asked for.
    ///
    /// Both operands are request counts from one bounded load run, exact in
    /// `f64` far below the 2^53 where the conversion stops being.
    #[allow(
        clippy::cast_precision_loss,
        reason = "request counts from one bounded run, exact in f64 far below 2^53"
    )]
    fn shed_fraction(&self) -> f64 {
        let asked = self.served + self.shed;
        if asked == 0 {
            return 0.0;
        }
        self.shed as f64 / asked as f64
    }
}

impl MixedMeasurement {
    /// How much less often assured work was shed than best-effort work.
    ///
    /// Infinite when assured work was never shed, which is the strongest
    /// possible result rather than an error — and finite the moment it is
    /// shed even once, so the number never flatters the reserve.
    fn assured_shed_advantage(&self) -> f64 {
        let assured = self.assured.shed_fraction();
        if assured <= 0.0 {
            return f64::INFINITY;
        }
        self.best_effort.shed_fraction() / assured
    }
}

#[derive(Debug, Clone, Copy)]
struct MixedMeasurement {
    assured: ClassOutcome,
    best_effort: ClassOutcome,
    throughput_requests_per_second: f64,
}

/// Drive assured and best-effort connections at one instance together, and
/// report what each class got.
///
/// The two classes must both be *present* and the instance must actually run
/// out, or the scenario proves nothing: a reserve that is never contended is
/// indistinguishable from no reserve at all. Both conditions are checked
/// rather than assumed, because a workload that quietly stopped saturating
/// would keep passing while measuring nothing.
async fn run_mixed_scenario(
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
    capacity: ExecutionCapacityMode,
) -> Result<MixedMeasurement, String> {
    let workload = Workload::mixed(connections, capacity);
    let samples = measure(true, connections, warmup, measured, &workload).await?;

    let mut by_class = [(Vec::new(), 0usize), (Vec::new(), 0usize)];
    for (index, outcome) in samples.outcomes.iter().enumerate() {
        let slot = match workload.tenant(index).capacity_class {
            CapacityClass::Assured => 0,
            CapacityClass::BestEffort => 1,
        };
        by_class[slot].0.extend(outcome.samples.iter().copied());
        by_class[slot].1 += outcome.shed;
    }

    let mut outcomes = Vec::with_capacity(2);
    for (class, (mut served, shed)) in [CapacityClass::Assured, CapacityClass::BestEffort]
        .into_iter()
        .zip(by_class)
    {
        if served.is_empty() {
            // A class that never got through has no percentiles, and reporting
            // zeroes for it would read as "instantaneous" rather than
            // "starved".
            return Err(format!(
                "load-gate mixed workload served no {} request at all",
                class.as_str()
            ));
        }
        served.sort_unstable();
        outcomes.push(ClassOutcome {
            latency: percentiles(&served),
            served: served.len(),
            shed,
        });
    }

    Ok(MixedMeasurement {
        assured: outcomes[0],
        best_effort: outcomes[1],
        throughput_requests_per_second: samples.throughput_requests_per_second,
    })
}

/// Percentiles of an already-sorted sample set.
fn percentiles(sorted: &[u64]) -> Percentiles {
    Percentiles {
        p50_ns: percentile(sorted, 0.50),
        p95_ns: percentile(sorted, 0.95),
        p99_ns: percentile(sorted, 0.99),
    }
}

/// Stand the service up, drive the clients, and return what each connection
/// saw. Shared by every scenario so they cannot drift apart in how they
/// measure — only in what they configure and what they conclude.
async fn measure(
    admission: bool,
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
    workload: &Workload,
) -> Result<MeasuredSamples, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| format!("bind load-gate server: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("read load-gate server address: {error}"))?;
    let sharding = LocalSharding::new(connections);
    // One construction for every scenario. A branch here that fell back to
    // `build_app_with_sharding` for a single disabled tenant was added to keep
    // the calibrated scenarios "byte-for-byte" identical, and mutation testing
    // showed it could be inverted with nothing noticing — because it was
    // decoration: `build_app_with_sharding` *is* this call with
    // `[demo_tenant()]` and `Disabled`. What actually preserves those
    // scenarios is `Workload::primary` carrying exactly that, which
    // `a_single_tenant_workload_sends_every_connection_to_one_account` pins.
    let (router, runtime) = build_app_with_capacity(
        u64::MAX / 4,
        admission,
        sharding,
        &workload.tenants,
        workload.capacity,
    )
    .await;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<PricingConnection>(),
        )
        .with_graceful_shutdown(async {
            let _ = stop_rx.await;
        })
        .await
    });

    let samples = async {
        if admission {
            // Wait for readiness: the lease slot must be stocked (GL-10).
            client::wait_ready(address, std::time::Duration::from_secs(5)).await?;
        }
        run_clients(address, admission, connections, warmup, measured, workload).await
    }
    .await;

    match (samples, runtime.shutdown_server(server, stop_tx).await) {
        (Ok(samples), Ok(())) => Ok(samples),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(shutdown)) => Err(format!("{error}; shutdown: {shutdown}")),
    }
}

async fn run_clients(
    address: std::net::SocketAddr,
    admission: bool,
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
    workload: &Workload,
) -> Result<MeasuredSamples, String> {
    let request_body = body(workload.contracts);
    let work = distribute_requests(warmup, connections)
        .into_iter()
        .zip(distribute_requests(measured, connections))
        .enumerate()
        .map(|(index, (warmup, measured))| ClientWork {
            request: http_request(
                address,
                admission.then(|| workload.tenant(index).api_key.as_str()),
                &request_body,
            ),
            warmup,
            measured,
        })
        .collect();
    drive_clients(address, work).await
}

fn p50_overhead_ratio(baseline: Percentiles, admitted: Percentiles) -> f64 {
    admitted.p50_ns / baseline.p50_ns.max(1.0)
}

fn absolute_latency_passed(thresholds: &Thresholds, admitted: Percentiles) -> Option<bool> {
    thresholds
        .max_p50_ns
        .zip(thresholds.max_p99_ns)
        .map(|(max_p50_ns, max_p99_ns)| {
            admitted.p50_ns <= max_p50_ns && admitted.p99_ns <= max_p99_ns
        })
}

fn throughput_passed(minimum: Option<f64>, measured: f64) -> Option<bool> {
    minimum.map(|minimum| measured >= minimum)
}

fn sequential_passed(
    thresholds: &Thresholds,
    baseline: ScenarioMeasurement,
    admitted: ScenarioMeasurement,
) -> bool {
    p50_overhead_ratio(baseline.latency, admitted.latency) <= thresholds.max_p50_overhead_ratio
        && absolute_latency_passed(thresholds, admitted.latency).unwrap_or(true)
        && throughput_passed(
            thresholds.min_throughput_requests_per_second,
            admitted.throughput_requests_per_second,
        )
        .unwrap_or(true)
}

fn concurrent_passed(
    thresholds: &Thresholds,
    baseline: ScenarioMeasurement,
    admitted: ScenarioMeasurement,
) -> bool {
    p50_overhead_ratio(baseline.latency, admitted.latency)
        <= thresholds.max_concurrent_p50_overhead_ratio
        && throughput_passed(
            thresholds.min_concurrent_throughput_requests_per_second,
            admitted.throughput_requests_per_second,
        )
        .unwrap_or(true)
}

/// The distinct-account scenario's own ratio, at its own connection count.
fn distinct_accounts_passed(
    thresholds: &Thresholds,
    baseline: ScenarioMeasurement,
    admitted: ScenarioMeasurement,
) -> Option<bool> {
    let ceiling = thresholds.max_distinct_account_p50_overhead_ratio?;
    Some(p50_overhead_ratio(baseline.latency, admitted.latency) <= ceiling)
}

/// What the mixed-saturation scenario concluded.
///
/// Three separate answers rather than one boolean, because they fail for
/// different reasons and an operator reading the report needs to tell them
/// apart: the guarantee broke, the run never saturated, or the assured path
/// got slower.
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
struct MixedVerdict {
    /// Best-effort saturation did not come out of assured work's capacity.
    /// The guarantee itself, measured comparatively.
    assured_protected: bool,
    /// Best-effort work *was* shed, so the reserve was actually contended.
    saturated: bool,
    /// The uniform control shed both classes alike, so the advantage above
    /// belongs to the class rather than to the workload.
    control_is_class_blind: bool,
    /// Assured p50 against the ungated assured workload. `None` where the
    /// ceiling is disabled.
    latency_passed: Option<bool>,
}

impl MixedVerdict {
    fn passed(self) -> bool {
        self.assured_protected
            && self.saturated
            && self.control_is_class_blind
            && self.latency_passed.unwrap_or(true)
    }
}

fn mixed_passed(
    thresholds: &Thresholds,
    mixed: MixedMeasurement,
    uniform: MixedMeasurement,
    ungated: MixedMeasurement,
) -> MixedVerdict {
    MixedVerdict {
        assured_protected: mixed.assured_shed_advantage() >= thresholds.min_assured_shed_advantage,
        saturated: mixed.best_effort.shed_fraction() >= thresholds.min_best_effort_shed_fraction,
        // A uniform pool bounds the instance identically and ignores the
        // class, so an advantage here would mean the assured connections were
        // simply asking for less — and would make the reserved number above
        // meaningless.
        control_is_class_blind: uniform.assured_shed_advantage()
            <= thresholds.max_uniform_shed_advantage,
        // Through the same function the report prints, not a second copy of
        // the same division. The two disagreeing about a zero denominator is
        // exactly the drift a duplicated rule produces, and mutation testing
        // found this copy's guard unwitnessed while the other's was pinned.
        latency_passed: thresholds
            .max_mixed_assured_p50_overhead_ratio
            .map(|ceiling| assured_p50_ratio(mixed, ungated) <= ceiling),
    }
}

fn all_scenarios_passed(
    sequential: bool,
    concurrent: bool,
    distinct_accounts: Option<bool>,
    mixed: MixedVerdict,
) -> bool {
    // Every scenario, conjoined here rather than at the call site, so a new
    // one cannot be measured, reported, and then silently left out of the
    // verdict. A disabled ceiling contributes `true` — it was deliberately not
    // asked — while a *failed* one contributes `false`.
    sequential && concurrent && distinct_accounts.unwrap_or(true) && mixed.passed()
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let (thresholds_path, report_path, verdict_mode) = match parse_args(&args) {
        Ok(Command::Run {
            thresholds_path,
            report_path,
            verdict_mode,
        }) => (thresholds_path, report_path, verdict_mode),
        Ok(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("load_gate {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Tokio's implicit environment parsing and runtime construction panic on
    // invalid configuration/build failure. This executable owns that boundary
    // so those failures can use the same report path as client failures.
    if let Some(workers) = std::env::var_os("TOKIO_WORKER_THREADS") {
        let Some(workers) = workers
            .to_str()
            .and_then(|value| value.parse::<NonZeroUsize>().ok())
        else {
            return report_failure(
                &report_path,
                "runtime configuration",
                "TOKIO_WORKER_THREADS must be a positive integer",
            );
        };
        builder.worker_threads(workers.get());
    }
    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(error) => return report_failure(&report_path, "runtime startup", &error.to_string()),
    };
    runtime.block_on(run(thresholds_path, report_path, verdict_mode))
}

async fn run(
    thresholds_path: PathBuf,
    report_path: PathBuf,
    verdict_mode: VerdictMode,
) -> ExitCode {
    let thresholds_json = match std::fs::read_to_string(&thresholds_path) {
        Ok(contents) => contents,
        Err(error) => {
            return report_failure(&report_path, "read thresholds", &error.to_string());
        }
    };
    let thresholds: Thresholds = match serde_json::from_str(&thresholds_json) {
        Ok(thresholds) => thresholds,
        Err(error) => {
            return report_failure(&report_path, "parse thresholds", &error.to_string());
        }
    };
    let concurrent_connections = match thresholds.validate() {
        Ok(connections) => connections,
        Err(error) => {
            return report_failure(&report_path, "validate thresholds", &error);
        }
    };
    let sequential_connection = NonZeroUsize::new(1).expect("one is nonzero");
    let primary = Workload::primary();

    let baseline = run_scenario(
        false,
        sequential_connection,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &primary,
    )
    .await;
    let baseline = match baseline {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "sequential baseline", &error);
        }
    };
    let admitted = run_scenario(
        true,
        sequential_connection,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &primary,
    )
    .await;
    let admitted = match admitted {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "sequential admitted scenario", &error);
        }
    };
    let concurrent_baseline = run_scenario(
        false,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &primary,
    )
    .await;
    let concurrent_baseline = match concurrent_baseline {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "concurrent baseline", &error);
        }
    };
    let concurrent_admitted = run_scenario(
        true,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &primary,
    )
    .await;
    let concurrent_admitted = match concurrent_admitted {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "concurrent admitted scenario", &error);
        }
    };

    // GL-99's two witnesses. Distinct accounts first: it is the same
    // concurrency the scenario above runs, with the one thing changed that the
    // pricing example could not vary until now.
    let distinct = Workload::distinct_accounts(concurrent_connections);
    let distinct_baseline = match run_scenario(
        false,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &distinct,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "distinct-account baseline", &error);
        }
    };
    let distinct_admitted = match run_scenario(
        true,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        &distinct,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "distinct-account admitted scenario", &error);
        }
    };

    // The mixed workload is run twice: once with the gate disabled, which is
    // the denominator for what the reserve costs assured latency, and once
    // reserved, which is the measurement. Same accounts, same classes, same
    // connections — only the gate differs, so the comparison is like for like.
    let mixed_ungated = match run_mixed_scenario(
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        ExecutionCapacityMode::Disabled,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "ungated mixed scenario", &error);
        }
    };
    let pool = match reserved_mode(concurrent_connections) {
        Ok(pool) => pool,
        Err(error) => {
            return report_failure(&report_path, "mixed scenario configuration", &error);
        }
    };
    let mixed_uniform = match run_mixed_scenario(
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        // The same bound, class-blind: the control that says the reserved run's
        // advantage comes from the class and not from the workload.
        pool.uniform(),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "uniform mixed control", &error);
        }
    };

    let mixed = match run_mixed_scenario(
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
        pool.reserved(),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return report_failure(&report_path, "mixed saturation scenario", &error);
        }
    };

    let ratio = p50_overhead_ratio(baseline.latency, admitted.latency);
    let concurrent_ratio =
        p50_overhead_ratio(concurrent_baseline.latency, concurrent_admitted.latency);
    let absolute_latency_passed = absolute_latency_passed(&thresholds, admitted.latency);
    let sequential_throughput_passed = throughput_passed(
        thresholds.min_throughput_requests_per_second,
        admitted.throughput_requests_per_second,
    );
    let concurrent_throughput_passed = throughput_passed(
        thresholds.min_concurrent_throughput_requests_per_second,
        concurrent_admitted.throughput_requests_per_second,
    );
    let sequential_passed = sequential_passed(&thresholds, baseline, admitted);
    let concurrent_passed =
        concurrent_passed(&thresholds, concurrent_baseline, concurrent_admitted);
    let distinct_accounts_passed =
        distinct_accounts_passed(&thresholds, distinct_baseline, distinct_admitted);
    let mixed_verdict = mixed_passed(&thresholds, mixed, mixed_uniform, mixed_ungated);
    let passed = all_scenarios_passed(
        sequential_passed,
        concurrent_passed,
        distinct_accounts_passed,
        mixed_verdict,
    );

    let report = Report {
        baseline: baseline.latency,
        admitted: admitted.latency,
        p50_overhead_ratio: ratio,
        sequential_passed,
        absolute_latency: AbsoluteLatencyReport {
            max_p50_ns: thresholds.max_p50_ns,
            max_p99_ns: thresholds.max_p99_ns,
            passed: absolute_latency_passed,
        },
        throughput: ThroughputReport {
            baseline_requests_per_second: baseline.throughput_requests_per_second,
            admitted_requests_per_second: admitted.throughput_requests_per_second,
            min_admitted_requests_per_second: thresholds.min_throughput_requests_per_second,
            passed: sequential_throughput_passed,
        },
        concurrent_same_account: ConcurrentReport {
            connections: concurrent_connections.get(),
            baseline: concurrent_baseline.latency,
            admitted: concurrent_admitted.latency,
            p50_overhead_ratio: concurrent_ratio,
            max_p50_overhead_ratio: thresholds.max_concurrent_p50_overhead_ratio,
            throughput: ThroughputReport {
                baseline_requests_per_second: concurrent_baseline.throughput_requests_per_second,
                admitted_requests_per_second: concurrent_admitted.throughput_requests_per_second,
                min_admitted_requests_per_second: thresholds
                    .min_concurrent_throughput_requests_per_second,
                passed: concurrent_throughput_passed,
            },
            passed: concurrent_passed,
        },
        concurrent_distinct_accounts: ConcurrentReport {
            connections: concurrent_connections.get(),
            baseline: distinct_baseline.latency,
            admitted: distinct_admitted.latency,
            p50_overhead_ratio: p50_overhead_ratio(
                distinct_baseline.latency,
                distinct_admitted.latency,
            ),
            max_p50_overhead_ratio: thresholds
                .max_distinct_account_p50_overhead_ratio
                .unwrap_or(f64::INFINITY),
            throughput: ThroughputReport {
                baseline_requests_per_second: distinct_baseline.throughput_requests_per_second,
                admitted_requests_per_second: distinct_admitted.throughput_requests_per_second,
                // No separate floor: the concurrent floor already covers this
                // connection count, and a second one calibrated on the same
                // workload would be the same number twice.
                min_admitted_requests_per_second: None,
                passed: None,
            },
            passed: distinct_accounts_passed.unwrap_or(true),
        },
        mixed_saturation: MixedReport {
            connections: concurrent_connections.get(),
            capacity_total: pool.total.get(),
            assured_reserve: pool.assured_reserve.get(),
            ungated_assured: mixed_ungated.assured,
            ungated_best_effort: mixed_ungated.best_effort,
            uniform_assured: mixed_uniform.assured,
            uniform_best_effort: mixed_uniform.best_effort,
            uniform_shed_advantage: mixed_uniform.assured_shed_advantage(),
            max_uniform_shed_advantage: thresholds.max_uniform_shed_advantage,
            assured: mixed.assured,
            best_effort: mixed.best_effort,
            assured_p50_overhead_ratio: assured_p50_ratio(mixed, mixed_ungated),
            max_assured_p50_overhead_ratio: thresholds.max_mixed_assured_p50_overhead_ratio,
            assured_shed_fraction: mixed.assured.shed_fraction(),
            assured_shed_advantage: mixed.assured_shed_advantage(),
            min_assured_shed_advantage: thresholds.min_assured_shed_advantage,
            best_effort_shed_fraction: mixed.best_effort.shed_fraction(),
            min_best_effort_shed_fraction: thresholds.min_best_effort_shed_fraction,
            throughput_requests_per_second: mixed.throughput_requests_per_second,
            verdict: mixed_verdict,
        },
        passed,
        run: RunContext::capture(),
    };
    println!(
        "load-gate sequential: baseline p50 {:.1}us p99 {:.1}us | admitted p50 {:.1}us p99 {:.1}us | overhead x{:.3} (max x{:.3})",
        baseline.latency.p50_ns / 1_000.0,
        baseline.latency.p99_ns / 1_000.0,
        admitted.latency.p50_ns / 1_000.0,
        admitted.latency.p99_ns / 1_000.0,
        ratio,
        thresholds.max_p50_overhead_ratio,
    );
    println!(
        "load-gate sequential throughput: baseline {:.0} req/s | admitted {:.0} req/s | floor {}",
        baseline.throughput_requests_per_second,
        admitted.throughput_requests_per_second,
        thresholds.min_throughput_requests_per_second.map_or_else(
            || "disabled".to_owned(),
            |floor| format!("{floor:.0} req/s")
        ),
    );
    println!(
        "load-gate concurrent same-account ({} connections): baseline p50 {:.1}us p99 {:.1}us | admitted p50 {:.1}us p99 {:.1}us | overhead x{:.3} (max x{:.3})",
        concurrent_connections,
        concurrent_baseline.latency.p50_ns / 1_000.0,
        concurrent_baseline.latency.p99_ns / 1_000.0,
        concurrent_admitted.latency.p50_ns / 1_000.0,
        concurrent_admitted.latency.p99_ns / 1_000.0,
        concurrent_ratio,
        thresholds.max_concurrent_p50_overhead_ratio,
    );
    println!(
        "load-gate concurrent throughput: baseline {:.0} req/s | admitted {:.0} req/s | floor {}",
        concurrent_baseline.throughput_requests_per_second,
        concurrent_admitted.throughput_requests_per_second,
        thresholds
            .min_concurrent_throughput_requests_per_second
            .map_or_else(
                || "disabled".to_owned(),
                |floor| format!("{floor:.0} req/s")
            ),
    );
    println!(
        "load-gate distinct accounts ({} connections, one account each): baseline p50 {:.1}us | admitted p50 {:.1}us | overhead x{:.3} (max {})",
        concurrent_connections,
        distinct_baseline.latency.p50_ns / 1_000.0,
        distinct_admitted.latency.p50_ns / 1_000.0,
        p50_overhead_ratio(distinct_baseline.latency, distinct_admitted.latency),
        thresholds
            .max_distinct_account_p50_overhead_ratio
            .map_or_else(|| "disabled".to_owned(), |max| format!("x{max:.3}")),
    );
    println!(
        "load-gate mixed saturation (pool {}, reserve {}, {} contracts): \
         assured {:.2}% shed | best-effort {:.2}% shed | advantage x{:.1} (floor x{:.1})",
        pool.total,
        pool.assured_reserve,
        MIXED_CONTRACTS,
        mixed.assured.shed_fraction() * 100.0,
        mixed.best_effort.shed_fraction() * 100.0,
        mixed.assured_shed_advantage(),
        thresholds.min_assured_shed_advantage,
    );
    println!(
        "load-gate mixed control: uniform advantage x{:.2} (ceiling x{:.1}) | ungated sheds {} assured, {} best-effort | assured p50 x{:.3} of ungated (max {})",
        mixed_uniform.assured_shed_advantage(),
        thresholds.max_uniform_shed_advantage,
        mixed_ungated.assured.shed,
        mixed_ungated.best_effort.shed,
        assured_p50_ratio(mixed, mixed_ungated),
        thresholds
            .max_mixed_assured_p50_overhead_ratio
            .map_or_else(|| "disabled".to_owned(), |max| format!("x{max:.3}")),
    );
    if let Err(error) = write_report(&report_path, &report) {
        eprintln!("load-gate: FAIL — report unavailable: {error}");
        return ExitCode::FAILURE;
    }

    let verdict = measurement_verdict(passed, verdict_mode);
    match verdict {
        MeasurementVerdict::Passed => println!("load-gate: PASS"),
        MeasurementVerdict::EvidenceMiss => eprintln!(
            "load-gate: THRESHOLD MISS (non-gating evidence) — see {}",
            report_path.display()
        ),
        MeasurementVerdict::GateFailure => {
            eprintln!("load-gate: FAIL — see {}", report_path.display());
        }
    }
    ExitCode::from(verdict.exit_code())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_clients_publish_diagnostics_without_measurements() {
        let directory = tempfile::tempdir().unwrap();
        load_failures::exercise(directory.path()).await;
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn valid_thresholds() -> Thresholds {
        Thresholds {
            _comment: serde::de::IgnoredAny,
            warmup_requests: 8,
            measured_requests: 16,
            concurrent_connections: 4,
            max_p50_overhead_ratio: 1.2,
            max_concurrent_p50_overhead_ratio: 1.5,
            max_p50_ns: Some(200.0),
            max_p99_ns: Some(300.0),
            min_throughput_requests_per_second: Some(1_000.0),
            min_concurrent_throughput_requests_per_second: Some(2_000.0),
            max_distinct_account_p50_overhead_ratio: Some(1.5),
            max_mixed_assured_p50_overhead_ratio: Some(1.5),
            min_assured_shed_advantage: 4.0,
            max_uniform_shed_advantage: 2.0,
            min_best_effort_shed_fraction: 0.02,
        }
    }

    /// A mixed measurement whose classes got what the feature promises:
    /// assured work served, best-effort work partly refused.
    fn mixed(assured_shed: usize, best_effort_shed: usize) -> MixedMeasurement {
        let class = |shed| ClassOutcome {
            latency: timings(100.0, 200.0),
            served: 100,
            shed,
        };
        MixedMeasurement {
            assured: class(assured_shed),
            best_effort: class(best_effort_shed),
            throughput_requests_per_second: 1_000.0,
        }
    }

    /// An ungated control whose assured p50 is `p50_ns`.
    fn ungated_at(p50_ns: f64) -> MixedMeasurement {
        let mut ungated = mixed(0, 0);
        ungated.assured.latency = timings(p50_ns, p50_ns);
        ungated
    }

    fn verdict(assured_protected: bool, saturated: bool) -> MixedVerdict {
        MixedVerdict {
            assured_protected,
            saturated,
            control_is_class_blind: true,
            latency_passed: Some(true),
        }
    }

    fn timings(p50_ns: f64, p99_ns: f64) -> Percentiles {
        Percentiles {
            p50_ns,
            p95_ns: p50_ns,
            p99_ns,
        }
    }

    fn measurement(
        p50_ns: f64,
        p99_ns: f64,
        throughput_requests_per_second: f64,
    ) -> ScenarioMeasurement {
        ScenarioMeasurement {
            latency: timings(p50_ns, p99_ns),
            throughput_requests_per_second,
        }
    }

    fn throughput_report() -> ThroughputReport {
        ThroughputReport {
            baseline_requests_per_second: 1_000.0,
            admitted_requests_per_second: 1_100.0,
            min_admitted_requests_per_second: None,
            passed: None,
        }
    }

    #[test]
    fn cli_handles_help_version_and_separator_before_arity_validation() {
        assert_eq!(
            parse_args(&strings(&["thresholds.json", "--help", "extra"])),
            Ok(Command::Help)
        );
        assert_eq!(
            parse_args(&strings(&["bad", "arity", "-V"])),
            Ok(Command::Version)
        );
        assert_eq!(
            parse_args(&strings(&["--", "--help", "report.json"])),
            Ok(Command::Run {
                thresholds_path: PathBuf::from("--help"),
                report_path: PathBuf::from("report.json"),
                verdict_mode: VerdictMode::Gate,
            })
        );
    }

    #[test]
    fn cli_rejects_unknown_options_and_wrong_positional_count() {
        assert_eq!(
            parse_args(&strings(&["--wat"])),
            Err("unknown option: --wat".to_owned())
        );
        assert_eq!(parse_args(&strings(&[])), Err(USAGE.to_owned()));
        assert_eq!(
            parse_args(&strings(&["thresholds.json", "report.json"])),
            Ok(Command::Run {
                thresholds_path: PathBuf::from("thresholds.json"),
                report_path: PathBuf::from("report.json"),
                verdict_mode: VerdictMode::Gate,
            })
        );
        assert_eq!(
            parse_args(&strings(&["thresholds.json", "--evidence", "report.json"])),
            Ok(Command::Run {
                thresholds_path: PathBuf::from("thresholds.json"),
                report_path: PathBuf::from("report.json"),
                verdict_mode: VerdictMode::Evidence,
            })
        );
        assert_eq!(
            parse_args(&strings(&[
                "--evidence",
                "--evidence",
                "thresholds.json",
                "report.json"
            ])),
            Err("--evidence may be specified only once".to_owned())
        );
    }

    #[test]
    fn evidence_mode_keeps_only_measurement_misses_non_gating() {
        let passed_gate = measurement_verdict(true, VerdictMode::Gate);
        let passed_evidence = measurement_verdict(true, VerdictMode::Evidence);
        let gate_failure = measurement_verdict(false, VerdictMode::Gate);
        let evidence_miss = measurement_verdict(false, VerdictMode::Evidence);

        assert_eq!(passed_gate, MeasurementVerdict::Passed);
        assert_eq!(passed_evidence, MeasurementVerdict::Passed);
        assert_eq!(gate_failure, MeasurementVerdict::GateFailure);
        assert_eq!(evidence_miss, MeasurementVerdict::EvidenceMiss);
        assert_eq!(passed_gate.exit_code(), 0);
        assert_eq!(passed_evidence.exit_code(), 0);
        assert_eq!(gate_failure.exit_code(), 1);
        assert_eq!(evidence_miss.exit_code(), 0);
    }

    #[test]
    fn request_distribution_preserves_exact_total_and_balances_remainder() {
        let work = distribute_requests(11, NonZeroUsize::new(3).unwrap());
        assert_eq!(work, vec![4, 4, 3]);
        assert_eq!(work.iter().sum::<usize>(), 11);

        let sparse = distribute_requests(2, NonZeroUsize::new(4).unwrap());
        assert_eq!(sparse, vec![1, 1, 0, 0]);
        assert_eq!(sparse.iter().sum::<usize>(), 2);
    }

    #[test]
    fn threshold_validation_requires_a_real_concurrent_workload() {
        assert_eq!(valid_thresholds().validate().unwrap().get(), 4);

        let mut thresholds = valid_thresholds();
        thresholds.concurrent_connections = 2;
        assert_eq!(thresholds.validate().unwrap().get(), 2);

        let mut thresholds = valid_thresholds();
        thresholds.concurrent_connections = 0;
        assert!(thresholds.validate().is_err());
        thresholds.concurrent_connections = 1;
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.measured_requests = 3;
        assert!(thresholds.validate().is_err());
        thresholds.measured_requests = 4;
        assert_eq!(thresholds.validate().unwrap().get(), 4);

        let mut thresholds = valid_thresholds();
        thresholds.warmup_requests = 3;
        assert!(thresholds.validate().is_err());
        thresholds.warmup_requests = 4;
        assert_eq!(thresholds.validate().unwrap().get(), 4);
    }

    #[test]
    fn threshold_validation_rejects_nonpositive_or_nonfinite_ceilings() {
        let mut thresholds = valid_thresholds();
        thresholds.max_p50_overhead_ratio = 0.0;
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.max_concurrent_p50_overhead_ratio = -1.0;
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.max_p50_ns = Some(f64::NAN);
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.max_p99_ns = Some(f64::INFINITY);
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.min_throughput_requests_per_second = Some(0.0);
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.min_concurrent_throughput_requests_per_second = Some(f64::NAN);
        assert!(thresholds.validate().is_err());
    }

    #[test]
    fn threshold_validation_requires_both_absolute_ceilings_or_neither() {
        let mut thresholds = valid_thresholds();
        thresholds.max_p50_ns = None;
        assert!(thresholds.validate().is_err());

        let mut thresholds = valid_thresholds();
        thresholds.max_p99_ns = None;
        assert!(thresholds.validate().is_err());

        thresholds.max_p50_ns = None;
        assert_eq!(thresholds.validate().unwrap().get(), 4);

        let mut thresholds = valid_thresholds();
        thresholds.min_throughput_requests_per_second = None;
        assert!(thresholds.validate().is_err());
        thresholds.min_concurrent_throughput_requests_per_second = None;
        assert_eq!(thresholds.validate().unwrap().get(), 4);
    }

    #[test]
    fn checked_in_manifests_share_workload_and_ratio_contracts() {
        let local_json = include_str!("../../../../testing/load_thresholds.json");
        let ci_json = include_str!("../../../../testing/load_thresholds_ci.json");
        let local: Thresholds = serde_json::from_str(local_json).unwrap();
        let ci: Thresholds = serde_json::from_str(ci_json).unwrap();

        assert!(local.max_p50_ns.is_some());
        assert!(local.max_p99_ns.is_some());
        assert!(local.min_throughput_requests_per_second.is_some());
        assert!(
            local
                .min_concurrent_throughput_requests_per_second
                .is_some()
        );
        assert!(ci.max_p50_ns.is_none());
        assert!(ci.max_p99_ns.is_none());
        assert!(ci.min_throughput_requests_per_second.is_none());
        assert!(ci.min_concurrent_throughput_requests_per_second.is_none());
        assert_eq!(local.warmup_requests, ci.warmup_requests);
        assert_eq!(local.measured_requests, ci.measured_requests);
        assert_eq!(local.concurrent_connections, ci.concurrent_connections);
        assert_eq!(local.max_p50_overhead_ratio, ci.max_p50_overhead_ratio);
        assert_eq!(
            local.max_concurrent_p50_overhead_ratio,
            ci.max_concurrent_p50_overhead_ratio
        );
        assert!(local.validate().is_ok());
        assert!(ci.validate().is_ok());

        assert!(ci.max_distinct_account_p50_overhead_ratio.is_none());
        assert!(ci.max_mixed_assured_p50_overhead_ratio.is_none());

        // The two shed fractions are not host-dependent, so both manifests
        // carry the same values: assured work has a measured shed advantage,
        // and a run that sheds no best-effort work has not saturated.
        assert_eq!(
            local.min_assured_shed_advantage,
            ci.min_assured_shed_advantage
        );
        assert_eq!(
            local.max_uniform_shed_advantage,
            ci.max_uniform_shed_advantage
        );
        assert_eq!(
            local.min_best_effort_shed_fraction,
            ci.min_best_effort_shed_fraction
        );
        assert!(ci.min_best_effort_shed_fraction > 0.0);
        assert!(ci.min_assured_shed_advantage > ci.max_uniform_shed_advantage);
    }

    #[test]
    fn every_load_setting_is_required_independent_of_json_layout() {
        for source in [
            include_str!("../../../../testing/load_thresholds.json"),
            include_str!("../../../../testing/load_thresholds_ci.json"),
        ] {
            let value: std::collections::BTreeMap<String, serde_json::Value> =
                serde_json::from_str(source).unwrap();
            // Compact and pretty serialization also reorder the original
            // object by key. Neither layout changes the configuration contract.
            for json in [
                serde_json::to_string(&value).unwrap(),
                serde_json::to_string_pretty(&value).unwrap(),
            ] {
                let input: serde_json::Value = serde_json::from_str(&json).unwrap();
                let complete: Thresholds = serde_json::from_str(&json).unwrap();
                assert!(complete.validate().is_ok());
                for key in input
                    .as_object()
                    .unwrap()
                    .keys()
                    .filter(|key| key.as_str() != "_comment")
                {
                    let mut missing = input.clone();
                    assert!(missing.as_object_mut().unwrap().remove(key).is_some());
                    let error = serde_json::from_value::<Thresholds>(missing)
                        .err()
                        .expect("absent settings cannot silently disable a bound");
                    assert!(
                        error
                            .to_string()
                            .contains(&format!("missing field `{key}`")),
                        "{key}: {error}"
                    );
                }
            }
        }
    }

    #[test]
    fn load_settings_accept_documented_comments_and_reject_unknown_keys() {
        let input: serde_json::Value =
            serde_json::from_str(include_str!("../../../../testing/load_thresholds_ci.json"))
                .unwrap();
        assert!(serde_json::from_value::<Thresholds>(input.clone()).is_ok());
        let mut without_comment = input.clone();
        without_comment.as_object_mut().unwrap().remove("_comment");
        assert!(serde_json::from_value::<Thresholds>(without_comment).is_ok());
        for key in ["max_p50_ns_typo", "_unrecognized_setting"] {
            let mut invalid = input.clone();
            invalid[key] = 10.into();
            let error = serde_json::from_value::<Thresholds>(invalid)
                .err()
                .expect("unknown load key must fail");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}`"))
            );
        }
    }

    #[test]
    fn percentile_uses_nearest_rank_in_sorted_samples() {
        let samples = [10, 20, 30, 40, 50];
        assert_eq!(percentile(&samples, 0.0), 10.0);
        assert_eq!(percentile(&samples, 0.5), 30.0);
        assert_eq!(percentile(&samples, 0.99), 50.0);
    }

    #[test]
    fn throughput_divides_completed_requests_by_the_measured_window() {
        assert_eq!(throughput_for_window(100, 2.0), 50.0);
        assert_eq!(throughput_for_window(1, 0.5), 2.0);
    }

    #[test]
    fn sequential_verdict_checks_ratio_and_both_absolute_ceilings() {
        let thresholds = valid_thresholds();
        assert!(sequential_passed(
            &thresholds,
            measurement(100.0, 100.0, 1_500.0),
            measurement(120.0, 300.0, 1_000.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            measurement(100.0, 100.0, 1_500.0),
            measurement(121.0, 100.0, 1_000.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            measurement(200.0, 100.0, 1_500.0),
            measurement(201.0, 100.0, 1_000.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            measurement(100.0, 100.0, 1_500.0),
            measurement(100.0, 301.0, 1_000.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            measurement(100.0, 100.0, 1_500.0),
            measurement(100.0, 100.0, 999.0)
        ));
    }

    #[test]
    fn shared_evidence_disables_absolute_latency_and_throughput_only() {
        let mut thresholds = valid_thresholds();
        thresholds.max_p50_ns = None;
        thresholds.max_p99_ns = None;
        thresholds.min_throughput_requests_per_second = None;
        thresholds.min_concurrent_throughput_requests_per_second = None;
        let baseline = measurement(1_000.0, 1_000.0, 1.0);

        assert_eq!(
            absolute_latency_passed(&thresholds, timings(1_000_000.0, 2_000_000.0)),
            None
        );
        assert!(sequential_passed(
            &thresholds,
            baseline,
            measurement(1_200.0, 2_000_000.0, 1.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            baseline,
            measurement(1_201.0, 1_201.0, 1.0)
        ));
    }

    #[test]
    fn concurrent_verdict_uses_its_own_ratio_ceiling() {
        let thresholds = valid_thresholds();
        assert_eq!(
            p50_overhead_ratio(timings(0.0, 0.0), timings(1.0, 1.0)),
            1.0
        );
        assert!(concurrent_passed(
            &thresholds,
            measurement(100.0, 100.0, 3_000.0),
            measurement(150.0, 100.0, 2_000.0)
        ));
        assert!(!concurrent_passed(
            &thresholds,
            measurement(100.0, 100.0, 3_000.0),
            measurement(151.0, 100.0, 2_000.0)
        ));
        assert!(!concurrent_passed(
            &thresholds,
            measurement(100.0, 100.0, 3_000.0),
            measurement(100.0, 100.0, 1_999.0)
        ));
    }

    #[test]
    fn overall_verdict_requires_every_scenario() {
        let good = verdict(true, true);
        assert!(all_scenarios_passed(true, true, Some(true), good));
        assert!(!all_scenarios_passed(true, false, Some(true), good));
        assert!(!all_scenarios_passed(false, true, Some(true), good));
        assert!(!all_scenarios_passed(false, false, Some(true), good));

        // GL-99's two witnesses are conjoined here, not merely reported. A
        // scenario that is measured, written into the report, and then left
        // out of the verdict is a gate that cannot fail.
        assert!(!all_scenarios_passed(true, true, Some(false), good));
        assert!(
            !all_scenarios_passed(true, true, Some(true), verdict(false, true)),
            "assured work being shed must fail the run"
        );
        assert!(
            !all_scenarios_passed(true, true, Some(true), verdict(true, false)),
            "a run that never saturated proved nothing and must not pass"
        );
        // A ceiling that was deliberately disabled is not a failure.
        assert!(all_scenarios_passed(true, true, None, good));
    }

    /// The mixed pool is one fact with two views, and both describe the same
    /// bound — which is what makes the control a control.
    #[test]
    fn the_control_bounds_the_instance_exactly_as_the_measurement_does() {
        let pool = reserved_mode(NonZeroUsize::new(10).unwrap()).expect("valid");
        let ExecutionCapacityMode::Reserved {
            total,
            assured_reserve,
        } = pool.reserved()
        else {
            panic!("reserved() must be reserved");
        };
        let ExecutionCapacityMode::Uniform {
            total: uniform_total,
        } = pool.uniform()
        else {
            panic!("uniform() must be uniform");
        };
        assert_eq!(
            total, uniform_total,
            "a control of a different size proves nothing"
        );
        assert!(assured_reserve < total);
    }

    /// The assured latency ratio is against the ungated run, and a missing
    /// denominator is immeasurably worse rather than free.
    #[test]
    fn a_missing_ungated_denominator_never_reads_as_a_pass() {
        let gated = mixed(0, 20);
        let mut ungated = mixed(0, 0);
        assert!((assured_p50_ratio(gated, ungated) - 1.0).abs() < f64::EPSILON);

        ungated.assured.latency = timings(50.0, 50.0);
        assert!((assured_p50_ratio(gated, ungated) - 2.0).abs() < f64::EPSILON);

        // Zero, not "nearly zero": a denominator that measured nothing cannot
        // divide into a verdict.
        ungated.assured.latency = timings(0.0, 0.0);
        assert_eq!(assured_p50_ratio(gated, ungated), f64::INFINITY);

        // And when *both* sides measured nothing. This is the case that makes
        // the guard a guard: `0.0 / 0.0` is NaN, which compares false against
        // every ceiling and so reports a run that measured nothing as one that
        // passed. Infinity fails them all instead.
        let mut nothing = gated;
        nothing.assured.latency = timings(0.0, 0.0);
        let ratio = assured_p50_ratio(nothing, ungated);
        assert!(
            ratio.is_infinite() && ratio.is_sign_positive(),
            "both sides zero gave {ratio}, which is not a refusal"
        );
    }

    /// The distinct-account ceiling gates, and a disabled one reports as
    /// disabled rather than as a pass.
    #[test]
    fn the_distinct_account_ceiling_gates_when_it_is_set() {
        let mut thresholds = valid_thresholds();
        thresholds.max_distinct_account_p50_overhead_ratio = Some(1.2);
        let baseline = measurement(100.0, 200.0, 1_000.0);

        assert_eq!(
            distinct_accounts_passed(&thresholds, baseline, measurement(120.0, 220.0, 900.0)),
            Some(true),
            "x1.2 is exactly the ceiling and must pass"
        );
        assert_eq!(
            distinct_accounts_passed(&thresholds, baseline, measurement(121.0, 220.0, 900.0)),
            Some(false)
        );

        thresholds.max_distinct_account_p50_overhead_ratio = None;
        assert_eq!(
            distinct_accounts_passed(&thresholds, baseline, measurement(500.0, 900.0, 10.0)),
            None,
            "a disabled ceiling is never a verdict, however bad the measurement"
        );
    }

    /// Every field `validate` refuses, refused for its own reason.
    #[test]
    fn threshold_validation_rejects_each_bad_shed_setting() {
        for (name, mutate) in [
            (
                "a shed fraction above one",
                (|t: &mut Thresholds| t.min_best_effort_shed_fraction = 1.5) as fn(&mut Thresholds),
            ),
            ("a negative shed fraction", |t| {
                t.min_best_effort_shed_fraction = -0.1;
            }),
            ("an infinite shed fraction", |t| {
                t.min_best_effort_shed_fraction = f64::INFINITY;
            }),
            ("a shed fraction of zero", |t| {
                t.min_best_effort_shed_fraction = 0.0;
            }),
            ("a non-positive advantage floor", |t| {
                t.min_assured_shed_advantage = 0.0;
            }),
            ("an infinite advantage floor", |t| {
                t.min_assured_shed_advantage = f64::INFINITY;
            }),
            ("a non-positive control ceiling", |t| {
                t.max_uniform_shed_advantage = -1.0;
            }),
            ("an infinite control ceiling", |t| {
                t.max_uniform_shed_advantage = f64::INFINITY;
            }),
        ] {
            let mut thresholds = valid_thresholds();
            mutate(&mut thresholds);
            assert!(
                thresholds.validate().is_err(),
                "{name} must be refused before any scenario runs"
            );
        }
        assert!(valid_thresholds().validate().is_ok());
    }

    /// A saturating mixed run counts its refusals, attributes them to the
    /// class that was refused, and loses none of them.
    ///
    /// The only test that drives the shed accounting end to end. Mutation
    /// testing found every step of it unwitnessed: `outcome.shed += 1` could
    /// become `-=`, the per-class fold could drop its sheds, and the
    /// served-plus-shed check could invert, all with the suite still green,
    /// because nothing but a real saturating scenario reaches any of it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_saturating_mixed_run_accounts_for_every_refusal() {
        let connections = NonZeroUsize::new(4).unwrap();
        let measured = 64;
        let pool = reserved_mode(connections).expect("valid");

        let mixed = run_mixed_scenario(connections, 8, measured, pool.reserved())
            .await
            .expect("the mixed scenario must run");

        // Nothing is lost: every request either produced a latency sample or a
        // refusal, and the two sum to what was asked for.
        let asked = mixed.assured.served
            + mixed.assured.shed
            + mixed.best_effort.served
            + mixed.best_effort.shed;
        assert_eq!(
            asked, measured,
            "requests went missing between the two classes"
        );
        assert!(mixed.assured.served > 0 && mixed.best_effort.served > 0);

        // An ungated run of the same workload sheds nothing at all, which is
        // what says the refusals above came from the pool and not from the
        // service failing under load.
        let ungated = run_mixed_scenario(connections, 8, measured, ExecutionCapacityMode::Disabled)
            .await
            .expect("the ungated control must run");
        assert_eq!(
            (ungated.assured.shed, ungated.best_effort.shed),
            (0, 0),
            "a disabled gate refuses nothing, so any refusal here is not capacity"
        );
        assert_eq!(
            ungated.assured.served + ungated.best_effort.served,
            measured
        );
    }

    /// A scenario that configured no gate treats a refusal as a failure rather
    /// than averaging it into a percentile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ungated_scenario_refuses_to_absorb_a_capacity_shed() {
        // `run_scenario` is the ungated path, and its sample-count check is
        // what stops a shed being silently dropped from the denominator.
        let result = run_scenario(
            true,
            NonZeroUsize::new(2).unwrap(),
            4,
            8,
            &Workload::primary(),
        )
        .await
        .expect("an unsaturated primary workload runs cleanly");
        assert!(result.latency.p50_ns > 0.0);
        assert!(result.throughput_requests_per_second > 0.0);
    }

    /// The mixed workload really is mixed, and the split is the one the pool
    /// is sized against.
    ///
    /// Mutation testing asked for this: `connections - assured` could become
    /// `+` or `/` and every other test stayed green, because nothing counted
    /// the classes a workload actually produces.
    #[test]
    fn the_mixed_workload_splits_its_connections_between_the_two_classes() {
        for connections in [2usize, 3, 4, 10, 11] {
            let workload = Workload::mixed(
                NonZeroUsize::new(connections).unwrap(),
                ExecutionCapacityMode::Disabled,
            );
            assert_eq!(
                workload.tenants.len(),
                connections,
                "one tenant per connection, or two connections share an account \
                 and stop being independent"
            );
            let classes: Vec<CapacityClass> = (0..connections)
                .map(|index| workload.tenant(index).capacity_class)
                .collect();
            let assured = classes
                .iter()
                .filter(|c| **c == CapacityClass::Assured)
                .count();
            let best_effort = connections - assured;
            assert_eq!(assured, connections.div_ceil(2));
            assert!(
                assured > 0 && best_effort > 0,
                "{connections} connections produced {assured} assured and \
                 {best_effort} best-effort; a single-class run measures nothing"
            );
            // Distinct accounts, or the classes could not differ at all.
            let accounts: std::collections::BTreeSet<_> =
                workload.tenants.iter().map(|t| t.account).collect();
            assert_eq!(accounts.len(), connections);
        }
    }

    /// The single-tenant workloads round-robin onto one account, which is what
    /// makes the original scenarios unchanged rather than merely similar.
    #[test]
    fn a_single_tenant_workload_sends_every_connection_to_one_account() {
        let primary = Workload::primary();
        assert_eq!(primary.contracts, 1);
        for index in 0..8 {
            assert_eq!(primary.tenant(index).api_key, demo_tenant().api_key);
        }

        let distinct = Workload::distinct_accounts(NonZeroUsize::new(4).unwrap());
        assert_eq!(distinct.contracts, 1);
        let keys: std::collections::BTreeSet<_> = (0..4)
            .map(|index| distinct.tenant(index).api_key.clone())
            .collect();
        assert_eq!(keys.len(), 4, "each connection must be its own principal");
        assert!(
            distinct
                .tenants
                .iter()
                .all(|t| t.capacity_class == CapacityClass::Assured),
            "the distinct-account scenario is about accounts, not classes"
        );
    }

    /// A multi-contract body is a well-formed list, not a run-on.
    #[test]
    fn a_body_separates_its_contracts() {
        for contracts in [1usize, 2, 7] {
            let body = body(contracts);
            let parsed: serde_json::Value =
                serde_json::from_str(&body).expect("the body must be valid JSON");
            assert_eq!(
                parsed["contracts"].as_array().unwrap().len(),
                contracts,
                "body({contracts}) did not carry {contracts} contracts"
            );
            assert_eq!(body.matches(',').count(), (contracts - 1) + contracts * 4);
        }
    }

    /// The scenarios calibrated on a one-contract body still send exactly that
    /// body, byte for byte.
    ///
    /// `body(1)` replaced a literal, and "equivalent JSON" would not be
    /// enough: a different length or spacing changes the bytes on the wire and
    /// with them the recorded ratios those scenarios are held to.
    #[test]
    fn a_one_contract_body_is_the_literal_the_baselines_were_taken_with() {
        assert_eq!(body(1), BODY);
        // And the mixed workload's body really is bigger, which is the whole
        // reason it can contend a pool.
        assert!(body(MIXED_CONTRACTS).len() > 100 * BODY.len());
    }

    /// The baseline scenario sends no `Authorization` header at all.
    #[test]
    fn the_unadmitted_baseline_sends_no_credential() {
        let address: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let baseline = http_request(address, None, BODY);
        assert!(!baseline.contains("Authorization"));
        let admitted = http_request(address, Some("demo-key-2"), BODY);
        assert!(admitted.contains("Authorization: Bearer demo-key-2\r\n"));
        // Content-Length must describe the body actually sent, or the server
        // blocks waiting for bytes that never arrive.
        assert!(admitted.contains(&format!("Content-Length: {}", BODY.len())));
    }

    /// The mixed verdict answers the guarantee, the saturation, the control,
    /// and the latency separately, because they fail for different reasons and
    /// an operator has to tell them apart.
    #[test]
    fn the_mixed_verdict_separates_the_guarantee_from_the_workload() {
        let thresholds = valid_thresholds();
        // A class-blind control: both classes shed alike, so it grants no
        // advantage of its own.
        let control = mixed(20, 20);

        // Best-effort shed, assured never: what the reserve is for, and an
        // infinite advantage.
        let good = mixed_passed(&thresholds, mixed(0, 20), control, ungated_at(100.0));
        assert_eq!(good, verdict(true, true));
        assert!(good.passed());

        // Assured shed a *little* is still the guarantee holding. Five assured
        // connections contending for two reachable units shed each other, and
        // GL-30 forbids best-effort consuming the reserve, not assured work ever
        // being refused. A first draft of this gate asserted zero assured
        // sheds and failed a real run on 1.08% that no invariant forbids.
        let contended = mixed_passed(&thresholds, mixed(4, 20), control, ungated_at(100.0));
        assert!(contended.assured_protected, "x5 clears the x4 floor");
        assert!(contended.passed());

        // Both classes shed alike is the reserve doing nothing.
        let flat = mixed_passed(&thresholds, mixed(20, 20), control, ungated_at(100.0));
        assert!(!flat.assured_protected, "x1 is no advantage at all");
        assert!(!flat.passed());

        // Nothing shed at all is not a pass: a reserve nobody contended is
        // indistinguishable from no reserve, so the run measured nothing.
        let idle = mixed_passed(&thresholds, mixed(0, 0), control, ungated_at(100.0));
        assert!(!idle.saturated);
        assert!(!idle.passed());

        // A control that itself favours assured work invalidates the reserved
        // number, because the advantage would be the workload's, not the
        // class's.
        let skewed = mixed_passed(&thresholds, mixed(0, 20), mixed(1, 20), ungated_at(100.0));
        assert!(
            !skewed.control_is_class_blind,
            "a class-blind pool that sheds assured work x20 less is not the control it claims"
        );
        assert!(!skewed.passed());

        // The assured latency ratio is against the *ungated* assured run.
        let slow = mixed_passed(&thresholds, mixed(0, 20), control, ungated_at(50.0));
        assert_eq!(
            slow.latency_passed,
            Some(false),
            "x2.0 exceeds the 1.5 ceiling"
        );
        assert!(!slow.passed());

        // A disabled ceiling reports as disabled and never as passing.
        let mut disabled = valid_thresholds();
        disabled.max_mixed_assured_p50_overhead_ratio = None;
        assert_eq!(
            mixed_passed(&disabled, mixed(0, 20), control, ungated_at(50.0)).latency_passed,
            None
        );
    }

    /// A reserved floor that does not exceed its control asks the reserved run
    /// for nothing the control already gives.
    #[test]
    fn a_reserved_floor_beneath_the_control_is_rejected() {
        let mut thresholds = valid_thresholds();
        thresholds.min_assured_shed_advantage = 2.0;
        thresholds.max_uniform_shed_advantage = 2.0;
        assert!(thresholds.validate().is_err());
    }

    /// The mixed pool leaves shared capacity, keeps a reserve, and stays small
    /// enough to be contended whatever the manifest's concurrency is.
    #[test]
    fn the_reserved_mode_keeps_a_reserve_and_stays_contendable() {
        for connections in [2usize, 4, 10, 64, 1_000] {
            let MixedPool {
                total,
                assured_reserve,
            } = reserved_mode(NonZeroUsize::new(connections).unwrap()).expect("valid");
            assert!(
                assured_reserve < total,
                "a reserve at or above total leaves no shared capacity, which the \
                 gate refuses at startup"
            );
            assert!(
                (total.get() as usize) <= connections,
                "{connections} connections against {total} units cannot contend"
            );
        }
        // One connection cannot carry two classes, so it is refused rather
        // than silently measured as a single-class run.
        assert!(reserved_mode(NonZeroUsize::new(1).unwrap()).is_err());
    }

    /// A refusal is not a latency sample, and it is not throughput either.
    #[test]
    fn a_shed_request_is_counted_but_never_timed() {
        let outcome = ClassOutcome {
            latency: timings(100.0, 200.0),
            served: 75,
            shed: 25,
        };
        assert!((outcome.shed_fraction() - 0.25).abs() < f64::EPSILON);
        // A class that asked for nothing has shed nothing, rather than a
        // division by zero.
        let idle = ClassOutcome {
            latency: timings(0.0, 0.0),
            served: 0,
            shed: 0,
        };
        assert!(idle.shed_fraction().abs() < f64::EPSILON);
    }

    #[test]
    fn report_keeps_sequential_fields_and_adds_concurrency_and_throughput() {
        let report = Report {
            baseline: timings(10.0, 20.0),
            admitted: timings(11.0, 21.0),
            p50_overhead_ratio: 1.1,
            sequential_passed: true,
            absolute_latency: AbsoluteLatencyReport {
                max_p50_ns: None,
                max_p99_ns: None,
                passed: None,
            },
            throughput: throughput_report(),
            concurrent_same_account: ConcurrentReport {
                connections: 4,
                baseline: timings(20.0, 30.0),
                admitted: timings(24.0, 34.0),
                p50_overhead_ratio: 1.2,
                max_p50_overhead_ratio: 1.5,
                throughput: throughput_report(),
                passed: true,
            },
            concurrent_distinct_accounts: ConcurrentReport {
                connections: 4,
                baseline: timings(20.0, 30.0),
                admitted: timings(22.0, 32.0),
                p50_overhead_ratio: 1.1,
                max_p50_overhead_ratio: 1.5,
                throughput: throughput_report(),
                passed: true,
            },
            mixed_saturation: MixedReport {
                connections: 4,
                capacity_total: 2,
                assured_reserve: 1,
                ungated_assured: mixed(0, 0).assured,
                ungated_best_effort: mixed(0, 0).best_effort,
                uniform_assured: mixed(20, 20).assured,
                uniform_best_effort: mixed(20, 20).best_effort,
                uniform_shed_advantage: 1.0,
                max_uniform_shed_advantage: 2.0,
                assured: mixed(0, 20).assured,
                best_effort: mixed(0, 20).best_effort,
                assured_p50_overhead_ratio: 1.0,
                max_assured_p50_overhead_ratio: Some(1.5),
                assured_shed_fraction: 0.0,
                assured_shed_advantage: f64::INFINITY,
                min_assured_shed_advantage: 4.0,
                best_effort_shed_fraction: 0.2,
                min_best_effort_shed_fraction: 0.02,
                throughput_requests_per_second: 1_000.0,
                verdict: verdict(true, true),
            },
            passed: true,
            run: RunContext {
                recorded_at_unix: 1,
                available_parallelism: Some(4),
                load_average: None,
            },
        };
        let json = serde_json::to_value(report).unwrap();
        assert_eq!(json["p50_overhead_ratio"], 1.1);
        assert_eq!(
            json["absolute_latency"]["max_p50_ns"],
            serde_json::Value::Null
        );
        assert_eq!(json["absolute_latency"]["passed"], serde_json::Value::Null);
        assert_eq!(json["throughput"]["admitted_requests_per_second"], 1_100.0);
        assert_eq!(json["concurrent_same_account"]["connections"], 4);
        assert_eq!(json["concurrent_same_account"]["p50_overhead_ratio"], 1.2);
        assert_eq!(json["passed"], true);
        // GL-99's witnesses reach the artifact, so a reader can see the reserve
        // doing its job without re-running anything.
        assert_eq!(json["concurrent_distinct_accounts"]["connections"], 4);
        assert_eq!(json["mixed_saturation"]["best_effort"]["shed"], 20);
        assert_eq!(json["mixed_saturation"]["assured"]["shed"], 0);
        assert_eq!(
            json["mixed_saturation"]["verdict"]["assured_protected"],
            true
        );
        assert_eq!(json["mixed_saturation"]["verdict"]["saturated"], true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_driver_runs_baseline_and_same_account_admission() {
        let connections = NonZeroUsize::new(2).unwrap();
        for admission in [false, true] {
            let result = run_scenario(admission, connections, 4, 8, &Workload::primary())
                .await
                .unwrap();
            assert!(result.latency.p50_ns > 0.0);
            assert!(result.latency.p50_ns <= result.latency.p95_ns);
            assert!(result.latency.p95_ns <= result.latency.p99_ns);
            assert!(result.throughput_requests_per_second > 0.0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_driver_surfaces_client_startup_failure_without_deadlock() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let unavailable_address = listener.local_addr().unwrap();
        drop(listener);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_clients(
                unavailable_address,
                false,
                NonZeroUsize::new(2).unwrap(),
                2,
                2,
                &Workload::primary(),
            ),
        )
        .await
        .expect("client failure must not deadlock");
        let error = result.unwrap_err();
        assert!(error.starts_with("client 0: connect:"), "{error}");
        assert!(error.contains("client 1: connect:"), "{error}");
        assert!(!error.contains("task did not complete"), "{error}");
    }
}
