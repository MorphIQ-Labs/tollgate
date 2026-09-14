use std::{env, process::ExitCode};

use tollgate_repo_check::cli::{Invocation, parse};

const MEMORY: &str = "crates/tollgate-store/tests/store_suite.rs";
const POSTGRES: &str = "crates/tollgate-store-postgres/tests/postgres_suite.rs";
const USAGE: &str = "Usage: check_backend_parity [--] [REPOSITORY]\n\nReports mirrored backend tests whose memory side drives a `MemoryStore`\ninherent helper where the PostgreSQL side drives the `AdminStore` trait.\nDefaults to the current directory.\n\nThis is not a name diff and not a proof of parity: it reports one mechanical\ndivergence, not that two mirrored bodies assert the same things.";

fn run() -> Result<bool, String> {
    let root = match parse(env::args_os().skip(1), "check_backend_parity", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(true);
        }
        Invocation::Check(root) => root,
    };
    let found = tollgate_repo_check::parity::diverging(&root.join(MEMORY), &root.join(POSTGRES))?;
    if found.is_empty() {
        println!(
            "backend parity: OK (mirrored scenarios drive the same contract on both backends; \
             this checks the inherent-helper divergence only, not that the bodies assert alike)"
        );
        return Ok(true);
    }
    eprintln!(
        "backend parity: {} mirrored test(s) drive different contracts on the two backends.",
        found.len()
    );
    for divergence in &found {
        eprintln!("  {divergence}");
    }
    eprintln!(
        "\nDrive `AdminStore::` on both sides. `PostgresStore` has no inherent helpers, so an\n\
         inherent call on the memory side means the two backends are not running the same\n\
         contract under one test name -- the memory trait body could be a stub and stay green."
    );
    Ok(false)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("backend parity: {message}");
            ExitCode::FAILURE
        }
    }
}
