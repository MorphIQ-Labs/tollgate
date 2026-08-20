//! Loopback load gate: measures the pricing-api over a persistent HTTP/1.1
//! connection with admission on and off, and gates the delta against
//! `testing/load_thresholds.json`.
//!
//! Usage: `load_gate <thresholds.json> <report.json>`
//!
//! The client is a raw blocking `TcpStream` speaking minimal HTTP/1.1 —
//! keep-alive, sequential requests — so the measurement mirrors ferro-risk's
//! persistent-loopback gate and adds no client-library noise. Percentiles
//! are computed over the measured requests only (warmup excluded). Absolute
//! numbers gate on the controlled host; the ratio (admitted vs baseline)
//! is the meaningful figure everywhere.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use pricing_api::build_app;

#[derive(Deserialize)]
struct Thresholds {
    warmup_requests: usize,
    measured_requests: usize,
    /// Admitted p50 may exceed baseline p50 by at most this factor.
    max_p50_overhead_ratio: f64,
    /// Absolute ceilings for the admitted run (nanoseconds).
    max_p50_ns: f64,
    max_p99_ns: f64,
}

#[derive(Serialize, Clone, Copy)]
struct Percentiles {
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
}

#[derive(Serialize)]
struct Report {
    baseline: Percentiles,
    admitted: Percentiles,
    p50_overhead_ratio: f64,
    passed: bool,
}

const BODY: &str =
    r#"{"contracts":[{"spot":100.0,"strike":105.0,"rate":0.05,"vol":0.2,"tte_years":0.25}]}"#;

fn percentile(sorted: &[u64], q: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index] as f64
}

/// Run one scenario in-process; returns latency percentiles.
async fn run_scenario(admission: bool, warmup: usize, measured: usize) -> Percentiles {
    let (router, runtime) = build_app(u64::MAX / 4, admission);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
    });

    if admission {
        // Wait for readiness: the lease slot must be stocked (#10).
        wait_ready(address).await;
    }

    // Blocking client on its own thread: precise per-request timing over one
    // persistent connection.
    let samples = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(address).expect("connect");
        stream.set_nodelay(true).unwrap();
        let auth = if admission {
            "Authorization: Bearer demo-key-1\r\n"
        } else {
            ""
        };
        let request = format!(
            "POST /v1/price HTTP/1.1\r\nHost: {address}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{BODY}",
            BODY.len(),
        );
        let mut samples = Vec::with_capacity(measured);
        let mut buf = vec![0u8; 16 * 1024];
        for i in 0..(warmup + measured) {
            let start = Instant::now();
            stream.write_all(request.as_bytes()).expect("write");
            read_response(&mut stream, &mut buf);
            if i >= warmup {
                samples.push(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
        }
        samples
    })
    .await
    .unwrap();

    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
    runtime.shutdown().await;

    let mut sorted = samples;
    sorted.sort_unstable();
    Percentiles {
        p50_ns: percentile(&sorted, 0.50),
        p95_ns: percentile(&sorted, 0.95),
        p99_ns: percentile(&sorted, 0.99),
    }
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

async fn wait_ready(address: std::net::SocketAddr) {
    for _ in 0..500 {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let request =
                format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
            if stream.write_all(request.as_bytes()).is_ok() {
                let mut response = String::new();
                let _ = stream.read_to_string(&mut response);
                if response.starts_with("HTTP/1.1 200") {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("service never became ready");
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [thresholds_path, report_path] = match args.as_slice() {
        [a, b] => [a.clone(), b.clone()],
        _ => {
            eprintln!("usage: load_gate <thresholds.json> <report.json>");
            return ExitCode::FAILURE;
        }
    };
    let thresholds: Thresholds =
        serde_json::from_str(&std::fs::read_to_string(&thresholds_path).expect("read thresholds"))
            .expect("parse thresholds");

    let baseline = run_scenario(
        false,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;
    let admitted = run_scenario(
        true,
        thresholds.warmup_requests,
        thresholds.measured_requests,
    )
    .await;

    let ratio = admitted.p50_ns / baseline.p50_ns.max(1.0);
    let passed = ratio <= thresholds.max_p50_overhead_ratio
        && admitted.p50_ns <= thresholds.max_p50_ns
        && admitted.p99_ns <= thresholds.max_p99_ns;

    let report = Report {
        baseline,
        admitted,
        p50_overhead_ratio: ratio,
        passed,
    };
    println!(
        "load-gate: baseline p50 {:.1}us p99 {:.1}us | admitted p50 {:.1}us p99 {:.1}us | overhead x{:.3} (max x{:.3})",
        baseline.p50_ns / 1_000.0,
        baseline.p99_ns / 1_000.0,
        admitted.p50_ns / 1_000.0,
        admitted.p99_ns / 1_000.0,
        ratio,
        thresholds.max_p50_overhead_ratio,
    );
    if let Some(parent) = std::path::Path::new(&report_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&report_path, serde_json::to_string_pretty(&report).unwrap())
        .expect("write report");

    if passed {
        println!("load-gate: PASS");
        ExitCode::SUCCESS
    } else {
        eprintln!("load-gate: FAIL — see {report_path}");
        ExitCode::FAILURE
    }
}
