use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    sync::{Arc, Mutex},
};

use tollgate_repo_check::{
    cli::{Invocation, parse},
    lean_mutants::{self, Mutant, Outcome},
};

const USAGE: &str = "Usage: check_lean_mutants [--] [REPOSITORY]\n\nMutates every transition definition in formal/lean/Tollgate, one change at a\ntime, and requires a theorem to fail for each. A survivor fails the gate\nunless formal/lean/mutants-allowed.txt names it with a reason; an entry that\nmatches no survivor fails it too, so the allowlist cannot go stale. Requires\nthe Lean package to be built (`lake build`). Defaults to the current\ndirectory.";

const ALLOWLIST: &str = "formal/lean/mutants-allowed.txt";

type Results = Arc<Mutex<Vec<(Mutant, Result<Outcome, String>)>>>;

fn lean_path(lean_dir: &Path) -> Result<String, String> {
    let out = Command::new("lake")
        .args(["env", "printenv", "LEAN_PATH"])
        .current_dir(lean_dir)
        .output()
        .map_err(|e| format!("cannot run lake (install elan): {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "lake env failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Check one source text with Lean; returns the lines of its errors.
fn check(
    lean_dir: &Path,
    lean_path: &str,
    scratch: &Path,
    name: &str,
    source: &str,
) -> Result<Vec<usize>, String> {
    let file = scratch.join(format!("{name}.lean"));
    fs::write(&file, source).map_err(|e| format!("cannot write {}: {e}", file.display()))?;
    let out = Command::new("lean")
        .arg("--json")
        .arg(&file)
        .env("LEAN_PATH", lean_path)
        .current_dir(lean_dir)
        .output()
        .map_err(|e| format!("cannot run lean: {e}"))?;
    let lines = lean_mutants::error_lines(&String::from_utf8_lossy(&out.stdout));
    if lines.is_empty() && !out.status.success() {
        return Err(format!(
            "lean failed without a located error: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(lines)
}

fn run() -> Result<bool, String> {
    let root = match parse(env::args_os().skip(1), "check_lean_mutants", USAGE)? {
        Invocation::Print(text) => {
            println!("{text}");
            return Ok(true);
        }
        Invocation::Check(root) => root,
    };
    let lean_dir = root.join("formal/lean");
    let lean_path = lean_path(&lean_dir)?;
    let allowed =
        lean_mutants::allowlist(&fs::read_to_string(root.join(ALLOWLIST)).unwrap_or_default());

    // Every original must check clean here first: a module that fails on its
    // own would make every one of its mutants look killed.
    let baseline = env::temp_dir().join(format!("tollgate-lean-baseline-{}", std::process::id()));
    fs::create_dir_all(&baseline)
        .map_err(|e| format!("cannot create {}: {e}", baseline.display()))?;
    for file in lean_mutants::modules(&root)? {
        let name = file
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let source = fs::read_to_string(&file)
            .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
        let lines = check(&lean_dir, &lean_path, &baseline, &name, &source)?;
        if !lines.is_empty() {
            return Err(format!(
                "{name}.lean does not check unmutated (lines {lines:?}); run `lake build` first"
            ));
        }
    }
    fs::remove_dir_all(&baseline).ok();

    let mut work: Vec<(Mutant, Vec<lean_mutants::Kind>)> = Vec::new();
    for file in lean_mutants::modules(&root)? {
        let name = file
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let source = fs::read_to_string(&file)
            .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
        let kinds = lean_mutants::line_kinds(&source);
        for mutant in lean_mutants::mutants(&name, &source) {
            work.push((mutant, kinds.clone()));
        }
    }
    let total = work.len();
    let queue = Arc::new(Mutex::new(work));
    let results: Results = Arc::new(Mutex::new(Vec::new()));
    let scratch_root =
        env::temp_dir().join(format!("tollgate-lean-mutants-{}", std::process::id()));
    let threads = std::thread::available_parallelism().map_or(2, |n| n.get());
    let mut handles = Vec::new();
    for worker in 0..threads {
        let queue = Arc::clone(&queue);
        let results = Arc::clone(&results);
        let lean_dir = lean_dir.clone();
        let lean_path = lean_path.clone();
        let scratch: PathBuf = scratch_root.join(worker.to_string());
        handles.push(std::thread::spawn(move || {
            if let Err(e) = fs::create_dir_all(&scratch) {
                let failure = Err(format!("cannot create {}: {e}", scratch.display()));
                while let Some((mutant, _)) = queue.lock().expect("queue").pop() {
                    results
                        .lock()
                        .expect("results")
                        .push((mutant, failure.clone()));
                }
                return;
            }
            loop {
                let Some((mutant, kinds)) = queue.lock().expect("queue").pop() else {
                    break;
                };
                let outcome = check(
                    &lean_dir,
                    &lean_path,
                    &scratch,
                    &mutant.module,
                    &mutant.mutated_source,
                )
                .map(|lines| lean_mutants::classify(&kinds, &lines));
                results.lock().expect("results").push((mutant, outcome));
            }
        }));
    }
    for handle in handles {
        handle.join().map_err(|_| "a worker panicked".to_owned())?;
    }
    // Best effort: the results are already collected, and a leftover scratch
    // directory under the temp dir is harmless.
    fs::remove_dir_all(&scratch_root).ok();

    let mut results = Arc::try_unwrap(results)
        .expect("workers joined")
        .into_inner()
        .expect("results");
    results.sort_by(|a, b| (a.0.module.as_str(), a.0.line).cmp(&(b.0.module.as_str(), b.0.line)));
    let (mut killed, mut unviable) = (0, 0);
    let mut survivors = Vec::new();
    let mut failures = Vec::new();
    for (mutant, outcome) in &results {
        match outcome {
            Ok(Outcome::Killed) => killed += 1,
            Ok(Outcome::Unviable) => unviable += 1,
            Ok(Outcome::Survived) => survivors.push(mutant),
            Err(e) => failures.push(format!("{}: {e}", mutant.key())),
        }
    }
    let mut ok = failures.is_empty();
    for failure in &failures {
        eprintln!("lean mutants: {failure}");
    }
    let mut excused = 0;
    for mutant in &survivors {
        let key = mutant.key();
        if allowed.iter().any(|(k, _)| *k == key) {
            excused += 1;
        } else {
            ok = false;
            eprintln!("SURVIVED {}:{}  {key}", mutant.module, mutant.line);
        }
    }
    for (key, _) in &allowed {
        if !survivors.iter().any(|m| m.key() == *key) {
            ok = false;
            eprintln!("stale allowlist entry (no such survivor): {key}");
        }
    }
    println!(
        "lean mutants: {total} mutants: {killed} killed, {unviable} unviable, {} survived ({excused} allowlisted as equivalent)",
        survivors.len()
    );
    Ok(ok)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("lean mutants: {message}");
            ExitCode::FAILURE
        }
    }
}
