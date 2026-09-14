use std::{env, path::PathBuf, process::ExitCode};

fn run() -> Result<(), String> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    for arg in args.iter().take_while(|arg| *arg != "--") {
        match arg.to_str() {
            Some("--help" | "-h") => {
                println!(
                    "Usage: check_invariant_witnesses [--] [REPOSITORY]\n\nChecks INVARIANTS.md against Rust and Lean declarations. Defaults to the current directory."
                );
                return Ok(());
            }
            Some("--version" | "-V") => {
                println!("check_invariant_witnesses {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            _ => {}
        }
    }
    let mut positional = Vec::new();
    let mut terminated = false;
    for arg in args {
        if !terminated && arg == "--" {
            terminated = true;
        } else if !terminated && arg.to_string_lossy().starts_with('-') {
            return Err(format!("unknown option: {}", arg.to_string_lossy()));
        } else {
            positional.push(arg);
        }
    }
    if positional.len() > 1 {
        return Err("expected at most one repository path".into());
    }
    let root = positional
        .first()
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
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
        Err(error) => {
            eprintln!("invariant references: {error}");
            ExitCode::FAILURE
        }
    }
}
