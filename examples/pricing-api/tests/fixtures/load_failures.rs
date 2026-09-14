//! Fixed protocol failures, shared by ordinary tests and the abort-profile probe.
//! No pricing server, timed workload, percentile or acceptance threshold runs.

use std::io::{Read, Write};
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use super::client::{ClientWork, drive_clients};
use super::report::report_failure;

pub async fn exercise(directory: &Path) {
    for (name, warmup, response, expected) in [
        (
            "warmup_status",
            1,
            "HTTP/1.1 500 fixture-secret\r\nContent-Length: 0\r\n\r\n",
            "warmup: unexpected HTTP status 500",
        ),
        (
            "measured_status",
            0,
            "HTTP/1.1 500 fixture-secret\r\nContent-Length: 0\r\n\r\n",
            "measured: unexpected HTTP status 500",
        ),
        (
            "truncated",
            0,
            "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nx",
            "incomplete response",
        ),
        (
            "bad_length",
            0,
            "HTTP/1.1 200 OK\r\nContent-Length: nope\r\n\r\n",
            "invalid content-length",
        ),
        (
            "quota_refusal",
            0,
            "HTTP/1.1 503 Unavailable\r\nContent-Length: 28\r\n\r\n{\"code\":\"quota-unavailable\"}",
            "not a capacity refusal",
        ),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        // The second client has no warmup. On a first-client warmup failure it
        // waits at the gate, then must close without issuing a measured request.
        let server = std::thread::spawn(move || {
            let mut handlers = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                handlers.push(std::thread::spawn(move || {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut request = Vec::new();
                    loop {
                        let mut byte = [0];
                        if stream.read(&mut byte).unwrap() == 0 {
                            return;
                        }
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    stream.write_all(response.as_bytes()).unwrap();
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
        let work = vec![
            ClientWork {
                request: "GET /fixture HTTP/1.1\r\n\r\n".to_owned(),
                warmup,
                measured: 1,
            },
            ClientWork {
                request: "GET /fixture HTTP/1.1\r\n\r\n".to_owned(),
                warmup: 0,
                measured: 1,
            },
        ];
        let result = tokio::time::timeout(Duration::from_secs(3), drive_clients(address, work))
            .await
            .expect("failed clients must release the rendezvous");
        let error = result.unwrap_err();
        server.join().unwrap();
        assert!(error.contains(expected), "{name}: {error}");
        assert!(
            !error.contains("task did not complete"),
            "operational errors must be returned: {error}"
        );
        assert!(!error.contains("fixture-secret"));
        if warmup != 0 {
            assert!(error.contains("only 1 of 2 clients completed warmup"));
        } else {
            assert!(error.contains("client 0:") && error.contains("client 1:"));
        }
        let path = directory.join(format!("{name}.json"));
        assert_eq!(report_failure(&path, name, &error), ExitCode::FAILURE);
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(report["passed"], false);
        assert_eq!(report["error"]["stage"], name);
        assert_eq!(report["error"]["message"], error);
        assert!(report.get("baseline").is_none());
    }
}
