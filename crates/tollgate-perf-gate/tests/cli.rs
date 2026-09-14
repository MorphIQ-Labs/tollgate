//! CLI control flow only: no benchmark or Criterion output is produced.
use std::process::Command;

fn checker() -> Command {
    Command::new(env!("CARGO_BIN_EXE_check_benchmark_thresholds"))
}

#[test]
fn benchmark_information_precedes_validation_and_uses_the_first_flag() {
    for flag in ["--help", "-h", "--version", "-V"] {
        let output = checker()
            .args(["--record", flag, "--invalid"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        let text = std::str::from_utf8(&output.stdout).unwrap();
        if matches!(flag, "--version" | "-V") {
            assert_eq!(
                text,
                format!("check_benchmark_thresholds {}\n", env!("CARGO_PKG_VERSION"))
            );
        } else {
            assert!(text.contains("usage: check_benchmark_thresholds"));
        }
    }
    let version = checker().args(["--version", "--help"]).output().unwrap();
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        format!("check_benchmark_thresholds {}\n", env!("CARGO_PKG_VERSION"))
    );
    let after_marker = checker().args(["--", "--help"]).output().unwrap();
    assert_eq!(after_marker.status.code(), Some(1));
    assert!(after_marker.stdout.is_empty());
}

#[cfg(unix)]
#[test]
fn benchmark_native_arguments_report_errors_after_information_flags_are_checked() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    for flag in ["--help", "--version"] {
        let output = checker()
            .arg(OsString::from_vec(vec![0xff]))
            .arg(flag)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
    }
    for leading in [vec![], vec!["--"]] {
        let output = checker()
            .args(leading)
            .arg(OsString::from_vec(vec![0xff]))
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            "perf-gate: arguments must be valid UTF-8\n"
        );
    }
}
