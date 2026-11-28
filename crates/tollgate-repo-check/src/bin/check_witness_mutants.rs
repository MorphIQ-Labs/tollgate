use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use tollgate_repo_check::{
    cli::information,
    witness_mutants::{Mutant, Options, catalog::CATALOG, drift, run},
};

const USAGE: &str = "Usage: check_witness_mutants [--verify | --list] [--only ID]... [--shard K/N] [--] [REPOSITORY]\n\nDisables one background mechanism at a time, in a copy of the repository under\ntarget/witness-mutants, and requires each witness the catalogue names for it to\nfail. Every pattern must occur exactly once, every witness must pass unmodified,\nand every mutant must compile. Logs are written to reports/witness-mutants.\nDefaults to the current directory.\n\n  --verify     check only that the catalogue applies to the repository\n  --list       print each mutant and its witnesses\n  --only ID    run the named mutant; may be repeated\n  --shard K/N  run every Nth mutant starting at K, counting from 0\n\nWitnesses in tollgate-store-postgres require TOLLGATE_PG_URL.";

/// How long one built witness may run. The slowest take seconds; a witness
/// that waits this long for a disabled mechanism has no bound of its own.
const WITNESS_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Run,
    Verify,
    List,
}

#[derive(Debug, Default)]
struct Arguments {
    mode: Mode,
    only: Vec<String>,
    shard: Option<(usize, usize)>,
    root: Option<PathBuf>,
}

fn shard(text: &str) -> Option<(usize, usize)> {
    let (index, count) = text.split_once('/')?;
    let (index, count) = (index.parse().ok()?, count.parse().ok()?);
    (index < count).then_some((index, count))
}

fn arguments(args: Vec<OsString>) -> Result<Arguments, String> {
    let mut parsed = Arguments::default();
    let mut args = args.into_iter();
    let mut terminated = false;
    while let Some(arg) = args.next() {
        let text = arg.to_string_lossy().into_owned();
        let option = !terminated && text.starts_with('-');
        match text.as_str() {
            "--" if !terminated => terminated = true,
            "--verify" | "--list" if option => {
                if parsed.mode != Mode::Run {
                    return Err("give one of --verify and --list".into());
                }
                parsed.mode = if text == "--verify" {
                    Mode::Verify
                } else {
                    Mode::List
                };
            }
            "--only" if option => {
                let id = args.next().ok_or("--only needs a mutant id")?;
                parsed.only.push(id.to_string_lossy().into_owned());
            }
            "--shard" if option => {
                let value = args.next().ok_or("--shard needs K/N")?;
                parsed.shard = Some(
                    shard(&value.to_string_lossy()).ok_or("--shard must be K/N with 0 <= K < N")?,
                );
            }
            _ if option => return Err(format!("unknown option: {text}")),
            _ => {
                if parsed.root.replace(PathBuf::from(arg)).is_some() {
                    return Err("expected at most one repository path".into());
                }
            }
        }
    }
    // A partial verification or listing would read as the whole one.
    if parsed.mode != Mode::Run && (!parsed.only.is_empty() || parsed.shard.is_some()) {
        return Err("--only and --shard select what runs, not what is verified or listed".into());
    }
    Ok(parsed)
}

/// The mutants a command line selects. An unknown id is an error: a run that
/// silently selected nothing would pass.
fn select(only: &[String], shard: Option<(usize, usize)>) -> Result<Vec<Mutant>, String> {
    if let Some(unknown) = only
        .iter()
        .find(|id| !CATALOG.iter().any(|mutant| mutant.id == **id))
    {
        return Err(format!("no mutant is named `{unknown}`"));
    }
    let (index, count) = shard.unwrap_or((0, 1));
    let selected: Vec<Mutant> = CATALOG
        .iter()
        .filter(|mutant| only.is_empty() || only.iter().any(|id| id == mutant.id))
        .skip(index)
        .step_by(count)
        .copied()
        .collect();
    if selected.is_empty() {
        return Err("the selection holds no mutant".into());
    }
    Ok(selected)
}

fn check() -> Result<bool, String> {
    let args: Vec<OsString> = env::args_os().skip(1).collect();
    if let Some(text) = information(&args, "check_witness_mutants", USAGE) {
        println!("{text}");
        return Ok(true);
    }
    let parsed = arguments(args)?;
    let root = parsed.root.unwrap_or_else(|| PathBuf::from("."));
    match parsed.mode {
        Mode::List => {
            for mutant in CATALOG {
                println!("{}: {} ({})", mutant.id, mutant.disables, mutant.file);
                for witness in mutant.witnesses {
                    println!("  {}", witness.describe());
                }
            }
            Ok(true)
        }
        Mode::Verify => {
            let found = drift(&root, CATALOG);
            for problem in &found {
                eprintln!("witness mutants: {problem}");
            }
            if found.is_empty() {
                println!(
                    "witness mutants: OK ({} edits each apply exactly once; none was run)",
                    CATALOG.len()
                );
            }
            Ok(found.is_empty())
        }
        Mode::Run => execute(&root, &select(&parsed.only, parsed.shard)?),
    }
}

fn execute(root: &Path, selected: &[Mutant]) -> Result<bool, String> {
    let needs_postgres = selected
        .iter()
        .flat_map(|mutant| mutant.witnesses)
        .any(|witness| witness.needs_postgres());
    if needs_postgres && env::var_os("TOLLGATE_PG_URL").is_none() {
        return Err(
            "set TOLLGATE_PG_URL: PostgreSQL witnesses skip without it, and a skip is not a witness"
                .into(),
        );
    }
    let root = root
        .canonicalize()
        .map_err(|e| format!("cannot resolve {}: {e}", root.display()))?;
    let options = Options {
        work: root.join("target/witness-mutants"),
        logs: root.join("reports/witness-mutants"),
        cargo: env::var_os("CARGO").unwrap_or_else(|| "cargo".into()),
        witness_timeout: WITNESS_TIMEOUT,
        root,
    };
    let failures = run(&options, selected, &mut |line| {
        println!("witness mutants: {line}")
    })?;
    if failures.is_empty() {
        println!(
            "witness mutants: OK ({} mutants, every witness fails without its mechanism)",
            selected.len()
        );
        return Ok(true);
    }
    eprintln!("witness mutants: FAILED ({} problems)", failures.len());
    for failure in &failures {
        eprintln!("  {failure}");
    }
    Ok(false)
}

fn main() -> ExitCode {
    match check() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("witness mutants: {message}");
            ExitCode::FAILURE
        }
    }
}
