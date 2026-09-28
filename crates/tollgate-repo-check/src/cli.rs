//! The argument contract both check binaries share.
//!
//! Extracted from the binaries rather than written twice, and extracted at all
//! because a `main`-side `run()` is unreachable from tests: AGENTS.md requires
//! every binary to handle `--help`, `--version` and `--` before validating
//! anything, and a rule nothing exercises is a rule that drifts. The mutation
//! gate said so first — every mutant of the inline parsing survived, because
//! nothing could observe it (GL-85).

use std::ffi::OsStr;
use std::path::PathBuf;

/// What a command line asked for.
#[derive(Debug, PartialEq, Eq)]
pub enum Invocation {
    /// `--help` or `--version`: the caller wants text, not work.
    Print(String),
    /// Check the repository rooted at this path.
    Check(PathBuf),
}

/// The text `--help`/`-h` or `--version`/`-V` asks for, if one of them comes
/// before `--`; the first one wins.
///
/// Help and version win over everything before `--`, including a malformed
/// option: someone reaching for `--help` is asking what the options are. A
/// binary with its own positional grammar calls this before parsing it.
pub fn information<S: AsRef<OsStr>>(args: &[S], name: &str, usage: &str) -> Option<String> {
    for arg in args.iter().take_while(|arg| arg.as_ref() != "--") {
        match arg.as_ref().to_str() {
            Some("--help" | "-h") => return Some(usage.to_owned()),
            Some("--version" | "-V") => {
                return Some(format!("{name} {}", env!("CARGO_PKG_VERSION")));
            }
            _ => {}
        }
    }
    None
}

/// Parse `args` (already excluding the program name).
///
/// `--` ends option processing, so a path that begins with a dash is still
/// reachable — which is the whole reason the rule names `--` alongside the two
/// flags. An option before `--` that is not recognised is an error rather than
/// a path, because silently treating `--repo` as a directory would check the
/// wrong tree and report success.
pub fn parse<I, S>(args: I, name: &str, usage: &str) -> Result<Invocation, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<PathBuf> = args
        .into_iter()
        .map(|a| PathBuf::from(a.as_ref()))
        .collect();
    if let Some(text) = information(&args, name, usage) {
        return Ok(Invocation::Print(text));
    }
    let mut positional = Vec::new();
    let mut terminated = false;
    for arg in args {
        if !terminated && arg.as_os_str() == "--" {
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
    Ok(Invocation::Check(
        positional
            .into_iter()
            .next()
            .unwrap_or_else(|| PathBuf::from(".")),
    ))
}
