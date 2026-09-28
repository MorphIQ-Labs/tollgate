use std::{
    env,
    ffi::OsString,
    io::{Read as _, Write as _},
    process::ExitCode,
};

use tollgate_repo_check::{book, cli::information};

const USAGE: &str = "Usage: mdbook-repo-links [supports RENDERER]\n\nThe documentation site's mdBook preprocessor; book.toml runs it. Rewrites\nlinks that leave the book source to chapters or to the repository on GitHub,\nand fails the build on a missing path, a missing heading, or a document under\nthe book source that SUMMARY.md does not list.\n\nWith no arguments it reads mdBook's [context, book] JSON from stdin and\nwrites the book to stdout. `supports RENDERER` exits 0: the rewrite is the\nsame for every renderer.";

fn main() -> ExitCode {
    let args: Vec<OsString> = env::args_os().skip(1).collect();
    if let Some(text) = information(&args, "mdbook-repo-links", USAGE) {
        println!("{text}");
        return ExitCode::SUCCESS;
    }
    let positional: Vec<&OsString> = args.iter().filter(|arg| *arg != "--").collect();
    match positional.as_slice() {
        [] => {}
        [command, _renderer] if *command == "supports" => return ExitCode::SUCCESS,
        _ => {
            eprintln!("mdbook-repo-links: unexpected arguments\n\n{USAGE}");
            return ExitCode::from(2);
        }
    }
    let mut input = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut input) {
        eprintln!("mdbook-repo-links: cannot read stdin: {e}");
        return ExitCode::FAILURE;
    }
    match book::preprocess(&input) {
        Ok(output) => {
            let mut stdout = std::io::stdout().lock();
            if let Err(e) = stdout.write_all(output.as_bytes()) {
                eprintln!("mdbook-repo-links: cannot write stdout: {e}");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(errors) => {
            eprintln!("mdbook-repo-links: {} problem(s):", errors.len());
            for error in &errors {
                eprintln!("  {error}");
            }
            ExitCode::FAILURE
        }
    }
}
