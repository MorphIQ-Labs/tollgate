//! These process tests exercise CLI selection only; they run no load workload.
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn run(command: &mut Command) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("CLI must terminate without serving or running measurements: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn pricing() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pricing-api"));
    // Normal startup refuses this before opening a listener. Help/version and
    // argument failures must precede even this configuration validation.
    command
        .env("TOLLGATE_LOCAL_SHARDS", "0")
        .env("PRICING_BIND", "127.0.0.1:0")
        .env("PRICING_DEPOSIT", "1000000")
        .env("RUST_LOG", "off");
    command
}

fn stdout(output: &Output) -> &str {
    std::str::from_utf8(&output.stdout).unwrap()
}
fn stderr(output: &Output) -> &str {
    std::str::from_utf8(&output.stderr).unwrap()
}

#[test]
fn pricing_help_and_version_precede_configuration_and_argument_validation() {
    for flag in ["--help", "-h", "--version", "-V"] {
        let output =
            run(pricing()
                .env("TOKIO_WORKER_THREADS", "0")
                .args(["--invalid", flag, "extra"]));
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        if matches!(flag, "--help" | "-h") {
            assert!(stdout(&output).contains("Usage: pricing-api"));
            for setting in ["PRICING_BIND", "PRICING_DEPOSIT", "TOLLGATE_LOCAL_SHARDS"] {
                assert!(stdout(&output).contains(setting));
            }
        } else {
            assert_eq!(
                stdout(&output),
                format!("pricing-api {}\n", env!("CARGO_PKG_VERSION"))
            );
        }
    }
    let output = run(pricing().args(["--version", "--help"]));
    assert_eq!(
        stdout(&output),
        format!("pricing-api {}\n", env!("CARGO_PKG_VERSION"))
    );
    let output = run(pricing().args(["--help", "--version"]));
    assert!(stdout(&output).contains("Usage: pricing-api"));
}

#[test]
fn pricing_information_does_not_attempt_to_bind_a_configured_listener() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    for flag in ["--help", "--version"] {
        let output = run(pricing()
            .env("TOLLGATE_LOCAL_SHARDS", "1")
            .env("PRICING_BIND", occupied.local_addr().unwrap().to_string())
            .arg(flag));
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn pricing_rejects_arguments_and_treats_everything_after_the_marker_literally() {
    for args in [
        vec!["--invalid"],
        vec!["position"],
        vec!["--", "--help"],
        vec!["--", "--version"],
        vec!["--", "--"],
        vec!["position", "--"],
    ] {
        let output = run(pricing().args(args));
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert_eq!(
            stderr(&output),
            "pricing-api: unexpected argument; use --help\n"
        );
        assert!(output.stdout.is_empty());
    }
    for args in [vec![], vec!["--"]] {
        let output = run(pricing().args(args));
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(stderr(&output).contains("TOLLGATE_LOCAL_SHARDS must be a positive integer"));
    }
}

#[test]
fn load_cli_information_does_not_open_files_or_start_measurements() {
    let directory = tempfile::tempdir().unwrap();
    for flag in ["--help", "-h", "--version", "-V"] {
        let output = run(Command::new(env!("CARGO_BIN_EXE_load_gate"))
            .current_dir(directory.path())
            .env("TOKIO_WORKER_THREADS", "0")
            .args(["--invalid", flag]));
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        if matches!(flag, "--version" | "-V") {
            assert_eq!(
                stdout(&output),
                format!("load_gate {}\n", env!("CARGO_PKG_VERSION"))
            );
        } else {
            assert!(stdout(&output).contains("usage: load_gate"));
        }
    }
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    let output = run(Command::new(env!("CARGO_BIN_EXE_load_gate"))
        .current_dir(directory.path())
        .args(["--", "--help", "report.json"]));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        stderr(&output).contains("read thresholds failed:"),
        "{output:?}"
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("report.json")).unwrap())
            .unwrap();
    assert_eq!(report["passed"], false);
    assert_eq!(report["error"]["stage"], "read thresholds");
    let version = run(Command::new(env!("CARGO_BIN_EXE_load_gate")).args(["--version", "--help"]));
    assert_eq!(
        stdout(&version),
        format!("load_gate {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn load_configuration_failures_write_reports_even_in_evidence_mode() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("thresholds.json");
    let report = directory.path().join("nested/report.json");
    for evidence in [false, true] {
        for (contents, stage) in [
            ("{".to_owned(), "parse thresholds"),
            (
                {
                    let mut value: serde_json::Value =
                        serde_json::from_str(include_str!("../../../testing/load_thresholds.json"))
                            .unwrap();
                    value["concurrent_connections"] = 0.into();
                    value.to_string()
                },
                "validate thresholds",
            ),
        ] {
            std::fs::write(&input, contents).unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_load_gate"));
            if evidence {
                command.arg("--evidence");
            }
            let output = run(command.arg(&input).arg(&report));
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert!(stderr(&output).contains("load-gate: FAIL — see"));
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
            assert_eq!(value["passed"], false);
            assert_eq!(value["error"]["stage"], stage);
            assert!(value["error"]["message"].is_string());
            assert!(value.get("baseline").is_none());
        }
    }
    let previous = std::fs::read(&report).unwrap();
    let output = run(Command::new(env!("CARGO_BIN_EXE_load_gate"))
        .arg(&input)
        .arg(report.join("impossible")));
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("report unavailable"));
    assert_eq!(std::fs::read(&report).unwrap(), previous);
}

#[test]
fn load_runtime_configuration_errors_are_reported_before_starting_tasks() {
    let directory = tempfile::tempdir().unwrap();
    let report = directory.path().join("report.json");
    for value in ["0", "not-a-number"] {
        let output = run(Command::new(env!("CARGO_BIN_EXE_load_gate"))
            .env("TOKIO_WORKER_THREADS", value)
            .args(["--evidence", "unread-thresholds.json"])
            .arg(&report));
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(!stderr(&output).contains("panicked"));
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
        assert_eq!(value["passed"], false);
        assert_eq!(value["error"]["stage"], "runtime configuration");
        assert_eq!(
            value["error"]["message"],
            "TOKIO_WORKER_THREADS must be a positive integer"
        );
    }
}

#[test]
fn unknown_load_settings_produce_parse_errors_in_both_verdict_modes() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("thresholds.json");
    let report = directory.path().join("report.json");
    let mut thresholds: serde_json::Value =
        serde_json::from_str(include_str!("../../../testing/load_thresholds_ci.json")).unwrap();
    // Even the old permissive parser cannot run a workload with this fixture.
    // Unknown-key refusal must precede that independent validation error.
    thresholds["concurrent_connections"] = 0.into();
    thresholds["max_p50_ns_typo"] = 10.into();
    std::fs::write(&input, thresholds.to_string()).unwrap();
    for evidence in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_load_gate"));
        if evidence {
            command.arg("--evidence");
        }
        let output = run(command.arg(&input).arg(&report));
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
        assert_eq!(report["passed"], false);
        assert_eq!(report["error"]["stage"], "parse thresholds");
        assert!(
            report["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown field `max_p50_ns_typo`")
        );
        assert!(report.get("baseline").is_none());
    }
}

#[cfg(unix)]
#[test]
fn native_arguments_cannot_panic_or_hide_information_flags() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    for binary in [
        env!("CARGO_BIN_EXE_pricing-api"),
        env!("CARGO_BIN_EXE_load_gate"),
    ] {
        let expected_code = if binary == env!("CARGO_BIN_EXE_pricing-api") {
            2
        } else {
            1
        };
        let expected_error = if expected_code == 2 {
            "pricing-api: unexpected argument; use --help\n"
        } else {
            "arguments must be valid UTF-8\n"
        };
        for flag in ["--help", "--version"] {
            let output = run(Command::new(binary)
                .arg(OsString::from_vec(vec![0xff]))
                .arg(flag));
            assert!(output.status.success(), "{binary}: {output:?}");
            assert!(output.stderr.is_empty());
        }
        let output = run(Command::new(binary).arg(OsString::from_vec(vec![0xff])));
        assert_eq!(
            output.status.code(),
            Some(expected_code),
            "{binary}: {output:?}"
        );
        assert_eq!(stderr(&output), expected_error);
        let output = run(Command::new(binary)
            .arg("--")
            .arg(OsString::from_vec(vec![0xff]))
            .arg("--help"));
        assert_eq!(
            output.status.code(),
            Some(expected_code),
            "{binary}: {output:?}"
        );
        assert_eq!(stderr(&output), expected_error);
    }
}
