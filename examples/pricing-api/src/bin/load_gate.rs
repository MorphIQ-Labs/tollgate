//! Loopback load gate: measures the pricing-api over persistent HTTP/1.1
//! connections with admission on and off, and gates the delta against
//! `testing/load_thresholds.json`. It reports the original sequential
//! scenario and concurrent connections contending on one account.
//!
//! Usage: `load_gate [--evidence] <thresholds.json> <report.json>`
//!
//! Each client is a raw blocking `TcpStream` speaking minimal HTTP/1.1, so the
//! measurement mirrors ferro-risk's persistent-loopback gate and adds no
//! client-library noise. Percentiles are computed over the measured requests
//! only (warmup excluded). Absolute numbers gate on the controlled host; each
//! admitted-vs-baseline ratio is the meaningful figure everywhere when the
//! configured connection count is held constant.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use serde::{Deserialize, Deserializer, Serialize};

use pricing_api::build_app;

#[derive(Clone, Deserialize)]
struct Thresholds {
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
        Ok(connections)
    }
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
struct Percentiles {
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
}

#[derive(Serialize)]
struct ConcurrentReport {
    connections: usize,
    baseline: Percentiles,
    admitted: Percentiles,
    p50_overhead_ratio: f64,
    max_p50_overhead_ratio: f64,
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
    concurrent_same_account: ConcurrentReport,
    passed: bool,
    run: RunContext,
}

/// What the machine looked like while measuring.
///
/// This gate takes one measurement and has no history to compare it against,
/// so unlike the perf gate it cannot judge its own trustworthiness (#49) — the
/// ratio's *denominator* is as exposed to a busy host as its numerator, which
/// is how a ×1.241 was once reported against three re-runs at ×1.096–×1.120.
/// Recording the context at least makes a suspect result diagnosable after the
/// fact instead of only reproducible.
#[derive(Serialize)]
struct RunContext {
    recorded_at_unix: u64,
    available_parallelism: Option<usize>,
    load_average: Option<String>,
}

impl RunContext {
    fn capture() -> Self {
        RunContext {
            recorded_at_unix: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            available_parallelism: std::thread::available_parallelism().ok().map(Into::into),
            load_average: std::env::var("TOLLGATE_GATE_LOAD").ok(),
        }
    }
}

const BODY: &str =
    r#"{"contracts":[{"spot":100.0,"strike":105.0,"rate":0.05,"vol":0.2,"tte_years":0.25}]}"#;
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

#[derive(Clone)]
struct MeasurementGate {
    state: Arc<(Mutex<Option<bool>>, Condvar)>,
}

impl MeasurementGate {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(None), Condvar::new())),
        }
    }

    fn wait(&self) -> bool {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("measurement gate poisoned");
        while state.is_none() {
            state = wake.wait(state).expect("measurement gate poisoned");
        }
        state.expect("measurement gate must have a decision")
    }

    fn release(&self, run: bool) {
        let (lock, wake) = &*self.state;
        *lock.lock().expect("measurement gate poisoned") = Some(run);
        wake.notify_all();
    }
}

fn percentile(sorted: &[u64], q: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index] as f64
}

/// Run one scenario in-process; returns latency percentiles.
async fn run_scenario(
    admission: bool,
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
) -> Result<Percentiles, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| format!("bind load-gate server: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("read load-gate server address: {error}"))?;
    let (router, runtime) = build_app(u64::MAX / 4, admission);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
    });

    let samples = async {
        if admission {
            // Wait for readiness: the lease slot must be stocked (#10).
            wait_ready(address).await?;
        }
        run_clients(address, admission, connections, warmup, measured).await
    }
    .await;

    let _ = stop_tx.send(());
    let server_result = match server.await {
        Ok(result) => result.map_err(|error| format!("load-gate server failed: {error}")),
        Err(error) => Err(format!("load-gate server task failed: {error}")),
    };
    runtime.shutdown().await;
    server_result?;

    let mut sorted = samples?;
    if sorted.len() != measured {
        return Err(format!(
            "load-gate collected {} samples, expected {measured}",
            sorted.len()
        ));
    }
    sorted.sort_unstable();
    Ok(Percentiles {
        p50_ns: percentile(&sorted, 0.50),
        p95_ns: percentile(&sorted, 0.95),
        p99_ns: percentile(&sorted, 0.99),
    })
}

async fn run_clients(
    address: std::net::SocketAddr,
    admission: bool,
    connections: NonZeroUsize,
    warmup: usize,
    measured: usize,
) -> Result<Vec<u64>, String> {
    let warmup_work = distribute_requests(warmup, connections);
    let measured_work = distribute_requests(measured, connections);
    let gate = MeasurementGate::new();
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut clients = Vec::with_capacity(connections.get());

    for (warmup, measured) in warmup_work.into_iter().zip(measured_work) {
        let gate = gate.clone();
        let ready_tx = ready_tx.clone();
        clients.push(tokio::task::spawn_blocking(move || {
            run_client(address, admission, warmup, measured, ready_tx, gate)
        }));
    }
    drop(ready_tx);

    let mut ready = 0;
    for _ in 0..connections.get() {
        if ready_rx.recv().await.is_none() {
            break;
        }
        ready += 1;
    }
    let all_ready = ready == connections.get();
    gate.release(all_ready);

    let mut samples = Vec::with_capacity(measured);
    let mut client_errors = Vec::new();
    for client in clients {
        match client.await {
            Ok(client_samples) => samples.extend(client_samples),
            Err(error) => client_errors.push(format!("load-gate client task failed: {error}")),
        }
    }

    let client_error = client_errors.into_iter().next();
    if !all_ready {
        return Err(client_error.unwrap_or_else(|| {
            format!(
                "only {ready} of {} load-gate clients completed warmup",
                connections.get()
            )
        }));
    }
    if let Some(error) = client_error {
        return Err(error);
    }
    Ok(samples)
}

fn run_client(
    address: std::net::SocketAddr,
    admission: bool,
    warmup: usize,
    measured: usize,
    ready_tx: tokio::sync::mpsc::UnboundedSender<()>,
    gate: MeasurementGate,
) -> Vec<u64> {
    let mut stream = TcpStream::connect(address).expect("connect");
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let auth = if admission {
        "Authorization: Bearer demo-key-1\r\n"
    } else {
        ""
    };
    let request = format!(
        "POST /v1/price HTTP/1.1\r\nHost: {address}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{BODY}",
        BODY.len(),
    );
    let mut buf = vec![0u8; 16 * 1024];
    for _ in 0..warmup {
        stream.write_all(request.as_bytes()).expect("write warmup");
        read_response(&mut stream, &mut buf);
    }
    ready_tx.send(()).expect("load-gate coordinator stopped");
    drop(ready_tx);
    if !gate.wait() {
        return Vec::new();
    }

    let mut samples = Vec::with_capacity(measured);
    for _ in 0..measured {
        let start = Instant::now();
        stream
            .write_all(request.as_bytes())
            .expect("write measured");
        read_response(&mut stream, &mut buf);
        samples.push(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    samples
}

/// Read one HTTP/1.1 response (headers + content-length body). The gate's
/// requests are always small and never chunked.
fn read_response(stream: &mut TcpStream, buf: &mut [u8]) {
    let mut filled = 0;
    loop {
        let n = stream.read(&mut buf[filled..]).expect("read");
        assert!(n > 0, "server closed connection");
        filled += n;
        let head = &buf[..filled];
        if let Some(header_end) = find_header_end(head) {
            let headers = std::str::from_utf8(&head[..header_end]).expect("ascii headers");
            assert!(
                headers.starts_with("HTTP/1.1 200"),
                "unexpected response: {}",
                headers.lines().next().unwrap_or("")
            );
            let content_length: usize = headers
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .map(String::from)
                })
                .and_then(|v| v.parse().ok())
                .expect("content-length");
            let total = header_end + 4 + content_length;
            while filled < total {
                let n = stream.read(&mut buf[filled..]).expect("read body");
                assert!(n > 0, "server closed mid-body");
                filled += n;
            }
            return;
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn wait_ready(address: std::net::SocketAddr) -> Result<(), String> {
    for _ in 0..500 {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let request =
                format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
            if stream.write_all(request.as_bytes()).is_ok() {
                let mut response = String::new();
                let _ = stream.read_to_string(&mut response);
                if response.starts_with("HTTP/1.1 200") {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err("service never became ready".to_owned())
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

fn sequential_passed(
    thresholds: &Thresholds,
    baseline: Percentiles,
    admitted: Percentiles,
) -> bool {
    p50_overhead_ratio(baseline, admitted) <= thresholds.max_p50_overhead_ratio
        && absolute_latency_passed(thresholds, admitted).unwrap_or(true)
}

fn concurrent_passed(
    thresholds: &Thresholds,
    baseline: Percentiles,
    admitted: Percentiles,
) -> bool {
    p50_overhead_ratio(baseline, admitted) <= thresholds.max_concurrent_p50_overhead_ratio
}

fn all_scenarios_passed(sequential: bool, concurrent: bool) -> bool {
    sequential && concurrent
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
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
    let thresholds_json = match std::fs::read_to_string(&thresholds_path) {
        Ok(contents) => contents,
        Err(error) => {
            eprintln!("read {}: {error}", thresholds_path.display());
            return ExitCode::FAILURE;
        }
    };
    let thresholds: Thresholds = match serde_json::from_str(&thresholds_json) {
        Ok(thresholds) => thresholds,
        Err(error) => {
            eprintln!("parse {}: {error}", thresholds_path.display());
            return ExitCode::FAILURE;
        }
    };
    let concurrent_connections = match thresholds.validate() {
        Ok(connections) => connections,
        Err(error) => {
            eprintln!("invalid {}: {error}", thresholds_path.display());
            return ExitCode::FAILURE;
        }
    };
    let sequential_connection = NonZeroUsize::new(1).expect("one is nonzero");

    let baseline = run_scenario(
        false,
        sequential_connection,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;
    let baseline = match baseline {
        Ok(result) => result,
        Err(error) => {
            eprintln!("sequential baseline failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let admitted = run_scenario(
        true,
        sequential_connection,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;
    let admitted = match admitted {
        Ok(result) => result,
        Err(error) => {
            eprintln!("sequential admitted scenario failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let concurrent_baseline = run_scenario(
        false,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;
    let concurrent_baseline = match concurrent_baseline {
        Ok(result) => result,
        Err(error) => {
            eprintln!("concurrent baseline failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let concurrent_admitted = run_scenario(
        true,
        concurrent_connections,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;
    let concurrent_admitted = match concurrent_admitted {
        Ok(result) => result,
        Err(error) => {
            eprintln!("concurrent admitted scenario failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    let ratio = p50_overhead_ratio(baseline, admitted);
    let concurrent_ratio = p50_overhead_ratio(concurrent_baseline, concurrent_admitted);
    let absolute_latency_passed = absolute_latency_passed(&thresholds, admitted);
    let sequential_passed = sequential_passed(&thresholds, baseline, admitted);
    let concurrent_passed =
        concurrent_passed(&thresholds, concurrent_baseline, concurrent_admitted);
    let passed = all_scenarios_passed(sequential_passed, concurrent_passed);

    let report = Report {
        baseline,
        admitted,
        p50_overhead_ratio: ratio,
        sequential_passed,
        absolute_latency: AbsoluteLatencyReport {
            max_p50_ns: thresholds.max_p50_ns,
            max_p99_ns: thresholds.max_p99_ns,
            passed: absolute_latency_passed,
        },
        concurrent_same_account: ConcurrentReport {
            connections: concurrent_connections.get(),
            baseline: concurrent_baseline,
            admitted: concurrent_admitted,
            p50_overhead_ratio: concurrent_ratio,
            max_p50_overhead_ratio: thresholds.max_concurrent_p50_overhead_ratio,
            passed: concurrent_passed,
        },
        passed,
        run: RunContext::capture(),
    };
    println!(
        "load-gate sequential: baseline p50 {:.1}us p99 {:.1}us | admitted p50 {:.1}us p99 {:.1}us | overhead x{:.3} (max x{:.3})",
        baseline.p50_ns / 1_000.0,
        baseline.p99_ns / 1_000.0,
        admitted.p50_ns / 1_000.0,
        admitted.p99_ns / 1_000.0,
        ratio,
        thresholds.max_p50_overhead_ratio,
    );
    println!(
        "load-gate concurrent same-account ({} connections): baseline p50 {:.1}us p99 {:.1}us | admitted p50 {:.1}us p99 {:.1}us | overhead x{:.3} (max x{:.3})",
        concurrent_connections,
        concurrent_baseline.p50_ns / 1_000.0,
        concurrent_baseline.p99_ns / 1_000.0,
        concurrent_admitted.p50_ns / 1_000.0,
        concurrent_admitted.p99_ns / 1_000.0,
        concurrent_ratio,
        thresholds.max_concurrent_p50_overhead_ratio,
    );
    if let Some(parent) = report_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let report_json = match serde_json::to_string_pretty(&report) {
        Ok(json) => json,
        Err(error) => {
            eprintln!("serialize load-gate report: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = std::fs::write(&report_path, report_json) {
        eprintln!("write {}: {error}", report_path.display());
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

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn valid_thresholds() -> Thresholds {
        Thresholds {
            warmup_requests: 8,
            measured_requests: 16,
            concurrent_connections: 4,
            max_p50_overhead_ratio: 1.2,
            max_concurrent_p50_overhead_ratio: 1.5,
            max_p50_ns: Some(200.0),
            max_p99_ns: Some(300.0),
        }
    }

    fn timings(p50_ns: f64, p99_ns: f64) -> Percentiles {
        Percentiles {
            p50_ns,
            p95_ns: p50_ns,
            p99_ns,
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
        assert_eq!(parse_args(&[]), Err(USAGE.to_owned()));
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
    }

    #[test]
    fn checked_in_manifests_share_workload_and_ratio_contracts() {
        let local_json = include_str!("../../../../testing/load_thresholds.json");
        let ci_json = include_str!("../../../../testing/load_thresholds_ci.json");
        let local: Thresholds = serde_json::from_str(local_json).unwrap();
        let ci: Thresholds = serde_json::from_str(ci_json).unwrap();

        assert!(local.max_p50_ns.is_some());
        assert!(local.max_p99_ns.is_some());
        assert!(ci.max_p50_ns.is_none());
        assert!(ci.max_p99_ns.is_none());
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

        let missing_p50 = ci_json.replace("  \"max_p50_ns\": null,\n", "");
        let missing_p99 = ci_json.replace("  \"max_p99_ns\": null\n", "");
        assert!(serde_json::from_str::<Thresholds>(&missing_p50).is_err());
        assert!(serde_json::from_str::<Thresholds>(&missing_p99).is_err());
    }

    #[test]
    fn measurement_gate_publishes_run_and_abort_decisions() {
        for decision in [true, false] {
            let gate = MeasurementGate::new();
            let waiter = gate.clone();
            let thread = std::thread::spawn(move || waiter.wait());
            gate.release(decision);
            assert_eq!(thread.join().unwrap(), decision);
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
    fn sequential_verdict_checks_ratio_and_both_absolute_ceilings() {
        let thresholds = valid_thresholds();
        assert!(sequential_passed(
            &thresholds,
            timings(100.0, 100.0),
            timings(120.0, 300.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            timings(100.0, 100.0),
            timings(121.0, 100.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            timings(200.0, 100.0),
            timings(201.0, 100.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            timings(100.0, 100.0),
            timings(100.0, 301.0)
        ));
    }

    #[test]
    fn ratio_only_verdict_disables_only_absolute_latency() {
        let mut thresholds = valid_thresholds();
        thresholds.max_p50_ns = None;
        thresholds.max_p99_ns = None;
        let baseline = timings(1_000.0, 1_000.0);

        assert_eq!(
            absolute_latency_passed(&thresholds, timings(1_000_000.0, 2_000_000.0)),
            None
        );
        assert!(sequential_passed(
            &thresholds,
            baseline,
            timings(1_200.0, 2_000_000.0)
        ));
        assert!(!sequential_passed(
            &thresholds,
            baseline,
            timings(1_201.0, 1_201.0)
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
            timings(100.0, 100.0),
            timings(150.0, 100.0)
        ));
        assert!(!concurrent_passed(
            &thresholds,
            timings(100.0, 100.0),
            timings(151.0, 100.0)
        ));
    }

    #[test]
    fn overall_verdict_requires_both_scenarios() {
        assert!(all_scenarios_passed(true, true));
        assert!(!all_scenarios_passed(true, false));
        assert!(!all_scenarios_passed(false, true));
        assert!(!all_scenarios_passed(false, false));
    }

    #[test]
    fn report_keeps_sequential_fields_and_adds_concurrent_ratio() {
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
            concurrent_same_account: ConcurrentReport {
                connections: 4,
                baseline: timings(20.0, 30.0),
                admitted: timings(24.0, 34.0),
                p50_overhead_ratio: 1.2,
                max_p50_overhead_ratio: 1.5,
                passed: true,
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
        assert_eq!(json["concurrent_same_account"]["connections"], 4);
        assert_eq!(json["concurrent_same_account"]["p50_overhead_ratio"], 1.2);
        assert_eq!(json["passed"], true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_driver_runs_baseline_and_same_account_admission() {
        let connections = NonZeroUsize::new(2).unwrap();
        for admission in [false, true] {
            let result = run_scenario(admission, connections, 4, 8).await.unwrap();
            assert!(result.p50_ns > 0.0);
            assert!(result.p50_ns <= result.p95_ns);
            assert!(result.p95_ns <= result.p99_ns);
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
            ),
        )
        .await
        .expect("client failure must not deadlock");
        assert!(result.is_err());
    }
}
