//! The argument contract AGENTS.md requires of every binary, made observable.
use std::path::PathBuf;

use tollgate_repo_check::cli::{Invocation, parse};

fn go(args: &[&str]) -> Result<Invocation, String> {
    parse(args.iter().copied(), "probe", "USAGE")
}

#[test]
fn no_arguments_checks_the_current_directory() {
    assert_eq!(go(&[]), Ok(Invocation::Check(PathBuf::from("."))));
}

#[test]
fn a_positional_is_the_repository_root() {
    assert_eq!(
        go(&["/srv/repo"]),
        Ok(Invocation::Check(PathBuf::from("/srv/repo")))
    );
}

#[test]
fn help_and_version_print_rather_than_work() {
    assert_eq!(go(&["--help"]), Ok(Invocation::Print("USAGE".into())));
    assert_eq!(go(&["-h"]), Ok(Invocation::Print("USAGE".into())));
    let Ok(Invocation::Print(text)) = go(&["--version"]) else {
        panic!("--version prints");
    };
    assert!(text.starts_with("probe "), "it names the binary: {text}");
    assert_eq!(go(&["-V"]), go(&["--version"]));
}

/// The reason `--` is named in the rule alongside the two flags: a repository
/// path may begin with a dash, and without a terminator it is unreachable.
#[test]
fn a_terminator_makes_a_dashed_path_reachable() {
    assert_eq!(
        go(&["--", "-weird-path"]),
        Ok(Invocation::Check(PathBuf::from("-weird-path")))
    );
}

/// After `--`, a flag is a path — including one that would otherwise print.
#[test]
fn a_flag_after_the_terminator_is_a_path_not_a_flag() {
    assert_eq!(
        go(&["--", "--help"]),
        Ok(Invocation::Check(PathBuf::from("--help")))
    );
}

/// Help wins over a malformed option before the terminator: someone reaching
/// for `--help` is asking what the options are.
#[test]
fn help_is_answered_even_beside_an_unknown_option() {
    assert_eq!(
        go(&["--bogus", "--help"]),
        Ok(Invocation::Print("USAGE".into()))
    );
}

/// An unrecognised option is refused rather than taken as a path. Treating
/// `--repo` as a directory would check the wrong tree and report success.
#[test]
fn an_unknown_option_is_refused_not_treated_as_a_path() {
    assert_eq!(go(&["--bogus"]), Err("unknown option: --bogus".into()));
}

#[test]
fn more_than_one_repository_is_refused() {
    assert_eq!(
        go(&["one", "two"]),
        Err("expected at most one repository path".into())
    );
}
