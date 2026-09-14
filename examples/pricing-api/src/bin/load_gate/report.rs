//! Complete measurements and execution failures share one atomic publisher.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Serialize)]
pub(super) struct RunContext {
    pub(super) recorded_at_unix: u64,
    pub(super) available_parallelism: Option<usize>,
    pub(super) load_average: Option<String>,
}

impl RunContext {
    pub fn capture() -> Self {
        Self {
            recorded_at_unix: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            available_parallelism: std::thread::available_parallelism().ok().map(Into::into),
            load_average: std::env::var("TOLLGATE_GATE_LOAD").ok(),
        }
    }
}

#[derive(Serialize)]
struct FailureReport<'a> {
    passed: bool,
    error: Failure<'a>,
    run: RunContext,
}

#[derive(Serialize)]
struct Failure<'a> {
    stage: &'a str,
    message: &'a str,
}

pub(super) fn report_failure(path: &Path, stage: &str, message: &str) -> ExitCode {
    eprintln!("load-gate {stage} failed: {message}");
    let report = FailureReport {
        passed: false,
        error: Failure { stage, message },
        run: RunContext::capture(),
    };
    match write_report(path, &report) {
        Ok(()) => eprintln!("load-gate: FAIL — see {}", path.display()),
        Err(error) => eprintln!("load-gate: FAIL — report unavailable: {error}"),
    }
    // Execution failures cannot become successful evidence-mode exits.
    ExitCode::FAILURE
}

struct StagedReport(PathBuf);

impl Drop for StagedReport {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub(super) fn write_report(path: &Path, report: &impl Serialize) -> Result<(), String> {
    let json =
        serde_json::to_vec_pretty(report).map_err(|error| format!("serialize report: {error}"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).map_err(|error| format!("create report directory: {error}"))?;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let temporary = parent.join(format!(
        ".load-gate-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| format!("stage report: {error}"))?;
    // Install cleanup only after exclusive creation: a collision is not ours.
    let staged = StagedReport(temporary);
    file.write_all(&json)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("write staged report: {error}"))?;
    drop(file);
    let recorded =
        std::fs::read(&staged.0).map_err(|error| format!("read staged report: {error}"))?;
    let _: serde_json::Value = serde_json::from_slice(&recorded)
        .map_err(|error| format!("validate staged report: {error}"))?;
    std::fs::rename(&staged.0, path).map_err(|error| format!("promote report: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_replace_atomically_and_failures_keep_the_previous_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.json");
        write_report(&path, &serde_json::json!({"passed": true})).unwrap();
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("fixture serialization failure"))
            }
        }
        assert!(
            write_report(&path, &Invalid)
                .unwrap_err()
                .contains("serialize")
        );
        let previous: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(previous, serde_json::json!({"passed": true}));
        assert_eq!(
            report_failure(&path, "warmup", "fixture failure"),
            ExitCode::FAILURE
        );
        let failure: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(failure["passed"], false);
        assert_eq!(failure["error"]["stage"], "warmup");
        assert_eq!(failure["error"]["message"], "fixture failure");
        assert!(failure["run"]["recorded_at_unix"].as_u64().is_some());
        assert!(failure.get("baseline").is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        let directory = dir.path().join("directory");
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("kept"), b"previous").unwrap();
        assert!(
            write_report(&directory, &previous)
                .unwrap_err()
                .contains("promote")
        );
        assert_eq!(std::fs::read(directory.join("kept")).unwrap(), b"previous");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
        let previous_bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            report_failure(&path.join("impossible"), "run", "failure"),
            ExitCode::FAILURE
        );
        assert_eq!(std::fs::read(&path).unwrap(), previous_bytes);
    }
}
