use std::{env, process::ExitCode};

use tollgate_repo_check::cli::{Invocation, parse};

const USAGE: &str = "Usage: check_seam_contract [--] [REPOSITORY]\n\nReports public methods on the staged admission seam that `docs/DESIGN.md`\n\u{a7} \"Staged admission interface (GL-96)\" never names. Defaults to the current\ndirectory.\n\nThis is a name check, not a signature check: it catches a method published\ninto the seam with the contract never opened, not a signature that changed\nunder a name the section already carries.";

fn run() -> Result<bool, String> {
    let root = match parse(env::args_os().skip(1), "check_seam_contract", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(true);
        }
        Invocation::Check(root) => root,
    };
    let found = tollgate_repo_check::seam::undeclared(&root)?;
    if found.is_empty() {
        println!(
            "seam contract: OK (every published seam method is named in the contract; \
             names only, not signatures)"
        );
        return Ok(true);
    }
    eprintln!(
        "seam contract: {} published method(s) the contract does not name.",
        found.len()
    );
    for undeclared in &found {
        eprintln!("  {undeclared}");
    }
    eprintln!(
        "\nAmend `docs/DESIGN.md` \u{a7} \"Staged admission interface (GL-96)\" first -- that is the\n\
         section's own rule, and it exists so a consumer is not left adapting to a seam the\n\
         contract never described. Declaring the method in a code block is the usual answer;\n\
         naming it in prose as deliberately outside the seam is the other one, and is why\n\
         there is no allowlist to edit."
    );
    Ok(false)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("seam contract: {message}");
            ExitCode::FAILURE
        }
    }
}
