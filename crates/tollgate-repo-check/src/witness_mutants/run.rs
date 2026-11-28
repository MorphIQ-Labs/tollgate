//! Applying the catalogue to a copy of the repository and running witnesses.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs::{self, File},
    os::unix::process::CommandExt as _,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use super::{Mutant, Target, Verdict, Witness, drift};

/// Where a run reads, writes and builds.
#[derive(Debug, Clone)]
pub struct Options {
    /// The repository whose files are copied; never written to.
    pub root: PathBuf,
    /// Scratch space. Its `tree` directory is replaced on every run; `build`
    /// is the cargo target directory and is reused.
    pub work: PathBuf,
    /// One log per cargo invocation.
    pub logs: PathBuf,
    /// The cargo to drive.
    pub cargo: OsString,
    /// How long one witness may run once built.
    pub witness_timeout: Duration,
}

/// The cargo arguments selecting a witness's test binary.
fn selection(command: &mut Command, package: &str, target: Target) {
    command.args(["test", "--locked", "--all-features", "-p", package]);
    match target {
        Target::Lib => command.arg("--lib"),
        Target::Test(name) => command.args(["--test", name]),
    };
}

struct Runner<'a> {
    options: &'a Options,
    tree: PathBuf,
    invocations: u32,
}

struct Finished {
    /// `None` when the process was killed at its bound.
    status: Option<ExitStatus>,
    output: String,
    log: PathBuf,
}

impl Runner<'_> {
    /// Run cargo in the copied tree, sending both streams to a log file.
    fn cargo(
        &mut self,
        label: &str,
        bound: Option<Duration>,
        arguments: impl FnOnce(&mut Command),
    ) -> Result<Finished, String> {
        self.invocations += 1;
        let log = self
            .options
            .logs
            .join(format!("{:04}-{label}.log", self.invocations));
        let file =
            File::create(&log).map_err(|e| format!("cannot create {}: {e}", log.display()))?;
        let errors = file
            .try_clone()
            .map_err(|e| format!("cannot open {}: {e}", log.display()))?;
        let mut command = Command::new(&self.options.cargo);
        arguments(&mut command);
        command
            .current_dir(&self.tree)
            .env("CARGO_TARGET_DIR", self.options.work.join("build"))
            // Every mutant rebuilds one crate and its dependents, so keep
            // that rebuild small; the test profile inherits both settings.
            .env("CARGO_INCREMENTAL", "1")
            .env("CARGO_PROFILE_DEV_DEBUG", "0")
            .env("CARGO_PROFILE_TEST_DEBUG", "0")
            .env("CARGO_TERM_COLOR", "never")
            // A database that is configured but unusable must fail a witness,
            // not skip it.
            .env("TOLLGATE_REQUIRE_PG", "1")
            .stdout(file)
            .stderr(errors)
            // Its own process group, so a hung test binary dies with the
            // cargo that started it.
            .process_group(0);
        let mut child = command
            .spawn()
            .map_err(|e| format!("cannot run {}: {e}", self.options.cargo.to_string_lossy()))?;
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|e| format!("cannot wait for cargo: {e}"))?
            {
                break Some(status);
            }
            if bound.is_some_and(|bound| started.elapsed() >= bound) {
                // The whole group: cargo and the test binary it started. A
                // group that has just exited is not an error.
                Command::new("kill")
                    .args(["-KILL", "--", &format!("-{}", child.id())])
                    .status()
                    .map_err(|e| format!("cannot run kill: {e}"))?;
                // And the child itself, so the wait below cannot block if the
                // group signal was refused. Harmless on an exited child.
                child
                    .kill()
                    .map_err(|e| format!("cannot stop a hung witness: {e}"))?;
                child
                    .wait()
                    .map_err(|e| format!("cannot reap a hung witness: {e}"))?;
                break None;
            }
            thread::sleep(Duration::from_millis(20));
        };
        let output = fs::read(&log).map_err(|e| format!("cannot read {}: {e}", log.display()))?;
        Ok(Finished {
            status,
            output: String::from_utf8_lossy(&output).into_owned(),
            log,
        })
    }

    /// Build every test binary the witnesses live in, once each.
    fn build(&mut self, label: &str, witnesses: &[Witness]) -> Result<(), String> {
        let binaries: BTreeSet<_> = witnesses.iter().map(|w| (w.package, w.target)).collect();
        for (package, target) in binaries {
            let built = self.cargo(label, None, |command| {
                selection(command, package, target);
                command.arg("--no-run");
            })?;
            if !built.status.is_some_and(|status| status.success()) {
                return Err(format!(
                    "{package} does not build; see {}",
                    built.log.display()
                ));
            }
        }
        Ok(())
    }

    fn witness(&mut self, label: &str, witness: Witness) -> Result<(Verdict, PathBuf), String> {
        let bound = self.options.witness_timeout;
        let finished = self.cargo(label, Some(bound), |command| {
            selection(command, witness.package, witness.target);
            command.args(["--", "--exact", witness.test]);
        })?;
        let verdict = match finished.status {
            Some(status) => Verdict::read(&finished.output, status.success()),
            None => Verdict::TimedOut,
        };
        Ok((verdict, finished.log))
    }

    /// Run each witness and report those whose verdict is not `wanted`.
    fn expect(
        &mut self,
        label: &str,
        witnesses: &[Witness],
        wanted: Verdict,
    ) -> Result<Vec<String>, String> {
        let mut found = Vec::new();
        for &witness in witnesses {
            let (verdict, log) = self.witness(label, witness)?;
            if verdict != wanted {
                found.push(format!(
                    "{label}: `{}` {}, and must have {}; see {}",
                    witness.describe(),
                    verdict.name(),
                    wanted.name(),
                    log.display()
                ));
            }
        }
        Ok(found)
    }
}

/// Copy the tracked and unignored files of `root` into `tree`, which is
/// replaced. A tracked file deleted in the working tree is simply absent.
fn copy_tree(root: &Path, tree: &Path) -> Result<(), String> {
    let listed = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if !listed.status.success() {
        return Err(format!(
            "git ls-files failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&listed.stderr)
        ));
    }
    if tree.exists() {
        fs::remove_dir_all(tree).map_err(|e| format!("cannot replace {}: {e}", tree.display()))?;
    }
    let names = String::from_utf8(listed.stdout)
        .map_err(|_| "a repository path is not UTF-8".to_owned())?;
    for name in names.split('\0').filter(|name| !name.is_empty()) {
        let source = root.join(name);
        if !source.is_file() {
            continue;
        }
        let copy = tree.join(name);
        if let Some(parent) = copy.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        fs::copy(&source, &copy).map_err(|e| format!("cannot copy {name}: {e}"))?;
    }
    Ok(())
}

/// Run `catalog`, reporting progress through `report`. Returns every failure;
/// an empty list means each witness passed unmodified and failed under its
/// mutant.
///
/// A failing baseline stops the run: a witness that does not pass unmodified
/// would count as biting under any edit.
///
/// # Errors
///
/// The run itself could not be carried out.
pub fn run(
    options: &Options,
    catalog: &[Mutant],
    report: &mut dyn FnMut(&str),
) -> Result<Vec<String>, String> {
    let drifted = drift(&options.root, catalog);
    if !drifted.is_empty() {
        return Ok(drifted);
    }
    let tree = options.work.join("tree");
    copy_tree(&options.root, &tree)?;
    if options.logs.exists() {
        fs::remove_dir_all(&options.logs)
            .map_err(|e| format!("cannot replace {}: {e}", options.logs.display()))?;
    }
    fs::create_dir_all(&options.logs)
        .map_err(|e| format!("cannot create {}: {e}", options.logs.display()))?;
    let mut runner = Runner {
        options,
        tree,
        invocations: 0,
    };

    let baseline: Vec<Witness> = catalog
        .iter()
        .flat_map(|mutant| mutant.witnesses.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    report(&format!(
        "baseline: {} witnesses, unmodified",
        baseline.len()
    ));
    if let Err(error) = runner.build("baseline", &baseline) {
        return Ok(vec![format!("baseline: {error}")]);
    }
    let failing = runner.expect("baseline", &baseline, Verdict::Passed)?;
    if !failing.is_empty() {
        return Ok(failing);
    }

    let mut failures = Vec::new();
    for mutant in catalog {
        let path = runner.tree.join(mutant.file);
        let original = fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let edited = mutant.apply(&original)?;
        fs::write(&path, edited).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        let found = match runner.build(mutant.id, mutant.witnesses) {
            Ok(()) => runner.expect(mutant.id, mutant.witnesses, Verdict::Failed),
            Err(error) => Ok(vec![format!(
                "{}: the mutant is unviable: {error}",
                mutant.id
            )]),
        };
        fs::write(&path, original)
            .map_err(|e| format!("cannot restore {}: {e}", path.display()))?;
        let found = found?;
        if found.is_empty() {
            report(&format!(
                "{}: witnessed by {}",
                mutant.id,
                mutant.witnesses.len()
            ));
        } else {
            for failure in &found {
                report(failure);
            }
        }
        failures.extend(found);
    }
    Ok(failures)
}
