use std::{env, process::ExitCode};

use tollgate_repo_check::cli::{Invocation, parse};

const USAGE: &str = "Usage: check_invariant_witnesses [--] [REPOSITORY]\n\nChecks INVARIANTS.md against Rust and Lean declarations. Defaults to the current directory.";

fn run() -> Result<(), String> {
    let root = match parse(env::args_os().skip(1), "check_invariant_witnesses", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(());
        }
        Invocation::Check(root) => root,
    };
    let report = tollgate_repo_check::check(&root)?;
    println!(
        "invariant references: OK ({} resolved, {} external; declarations only, not test execution or proof checking)",
        report.resolved, report.external
    );
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("invariant references: {message}");
            ExitCode::FAILURE
        }
    }
}
