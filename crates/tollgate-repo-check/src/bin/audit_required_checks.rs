use std::{env, fs, io, process::ExitCode};

use tollgate_repo_check::cli::{Invocation, parse};
use tollgate_repo_check::required_checks::{REQUIRED, drift};

const USAGE: &str = "Usage: audit_required_checks [--] [REPOSITORY] < PROTECTION\n\nCompares .github/required-checks.txt with GitHub's required_status_checks\nresponse on standard input. Defaults to the current directory.\n\nRun it through scripts/audit_required_checks.sh, which fetches the response\nand fails if it cannot.";

fn run() -> Result<bool, String> {
    let root = match parse(env::args_os().skip(1), "audit_required_checks", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(true);
        }
        Invocation::Check(root) => root,
    };
    let list = fs::read_to_string(root.join(REQUIRED))
        .map_err(|error| format!("cannot read {REQUIRED}: {error}"))?;
    let protection = io::read_to_string(io::stdin())
        .map_err(|error| format!("cannot read standard input: {error}"))?;
    let found = drift(&protection, &list)?;
    if found.is_empty() {
        println!("required checks audit: OK (main requires exactly what {REQUIRED} lists)");
        return Ok(true);
    }
    eprintln!("required checks audit: {} difference(s).", found.len());
    for difference in &found {
        eprintln!("  {difference}");
    }
    Ok(false)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("required checks audit: {message}");
            ExitCode::FAILURE
        }
    }
}
