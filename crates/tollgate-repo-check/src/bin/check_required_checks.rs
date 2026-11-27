use std::{env, process::ExitCode};

use tollgate_repo_check::cli::{Invocation, parse};
use tollgate_repo_check::required_checks::{REQUIRED, check};

const USAGE: &str = "Usage: check_required_checks [--] [REPOSITORY]\n\nReports every job in .github/workflows/ci.yml that\n.github/required-checks.txt does not require, and every listed check no job\nprovides. Defaults to the current directory.\n\nThis reads the repository only; audit_required_checks compares the list with\nlive branch protection.";

fn run() -> Result<bool, String> {
    let root = match parse(env::args_os().skip(1), "check_required_checks", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(true);
        }
        Invocation::Check(root) => root,
    };
    let found = check(&root)?;
    if found.is_empty() {
        println!("required checks: OK (every ci.yml job is listed in {REQUIRED})");
        return Ok(true);
    }
    eprintln!("required checks: {} disagreement(s).", found.len());
    for disagreement in &found {
        eprintln!("  {disagreement}");
    }
    Ok(false)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("required checks: {message}");
            ExitCode::FAILURE
        }
    }
}
