//! Witness mutants: each background mechanism has a test that fails without it.
//!
//! A periodic sweep, a reload, a retry, an enumeration or a push can be
//! switched off while the test named for it stays green, because another
//! mechanism covers for it or the test performs the step itself.
//! `cargo-mutants` does not delete a `select!` arm or substitute an argument,
//! so its verdict says nothing about this. The [`catalog`] does: each entry is
//! one source edit that disables one mechanism, with the tests that must then
//! fail.
//!
//! An entry is held to three things, and the gate fails on any other outcome:
//!
//! - its pattern occurs **exactly once** in its file, so a source change that
//!   moves or duplicates the site is reported rather than skipped;
//! - every witness **runs and passes** unmodified, so a renamed or ignored test
//!   cannot stand as evidence;
//! - with the edit applied the code **compiles** and every witness **runs and
//!   fails** within its bound. A mutant that does not compile, a witness that
//!   crashes or hangs, and a witness that still passes are all failures.
//!
//! The edits are applied to a copy of the repository under `target/`, never to
//! the working tree.

pub mod catalog;
mod run;

pub use run::{Options, run};

use std::{collections::BTreeSet, fs, path::Path};

/// The test binary a witness lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    /// The package's library unit tests.
    Lib,
    /// An integration test, `tests/<name>.rs`.
    Test(&'static str),
}

/// A test that must fail while its mutant is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Witness {
    pub package: &'static str,
    pub target: Target,
    /// The test's full path within its binary, as `--exact` takes it.
    pub test: &'static str,
}

impl Witness {
    /// Whether the test needs `TOLLGATE_PG_URL`. Without it the PostgreSQL
    /// suite skips and reports success, which is not evidence.
    pub fn needs_postgres(&self) -> bool {
        self.package == "tollgate-store-postgres"
    }

    /// `package target test`, for reports.
    pub fn describe(&self) -> String {
        let target = match self.target {
            Target::Lib => "--lib".to_owned(),
            Target::Test(name) => format!("--test {name}"),
        };
        format!("{} {target} {}", self.package, self.test)
    }
}

/// One edit that disables one background mechanism.
#[derive(Debug, Clone, Copy)]
pub struct Mutant {
    /// The name reports and `--only` use.
    pub id: &'static str,
    /// What the edited code no longer does.
    pub disables: &'static str,
    /// The edited file, relative to the repository root.
    pub file: &'static str,
    /// Source text that occurs exactly once in `file`.
    pub find: &'static str,
    /// What replaces it.
    pub replace: &'static str,
    /// Every one of these must fail with the edit applied.
    pub witnesses: &'static [Witness],
}

impl Mutant {
    /// `source` with this edit applied.
    ///
    /// # Errors
    ///
    /// The pattern does not occur exactly once. Overlapping occurrences count,
    /// so an edit can never be ambiguous about its site.
    pub fn apply(&self, source: &str) -> Result<String, String> {
        let found = source
            .char_indices()
            .filter(|(at, _)| source[*at..].starts_with(self.find))
            .count();
        if self.find.is_empty() || found != 1 {
            return Err(format!(
                "{}: the pattern must occur exactly once in {}, found {found} occurrences",
                self.id, self.file
            ));
        }
        Ok(source.replacen(self.find, self.replace, 1))
    }
}

/// Everything wrong with a catalogue on its own terms.
pub fn validate(catalog: &[Mutant]) -> Vec<String> {
    let mut found = Vec::new();
    if catalog.is_empty() {
        found.push("the catalogue is empty, so it witnesses nothing".to_owned());
    }
    let mut ids = BTreeSet::new();
    for mutant in catalog {
        let id = mutant.id;
        if id.is_empty()
            || !id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            found.push(format!(
                "`{id}`: an id is lower-case letters, digits and dashes"
            ));
        }
        if !ids.insert(id) {
            found.push(format!("`{id}` is declared twice"));
        }
        if mutant.disables.is_empty() {
            found.push(format!("{id}: say what the edit disables"));
        }
        if mutant.find == mutant.replace {
            found.push(format!("{id}: the replacement changes nothing"));
        }
        if mutant.witnesses.is_empty() {
            found.push(format!("{id}: no witness is named"));
        }
        let distinct: BTreeSet<_> = mutant.witnesses.iter().collect();
        if distinct.len() != mutant.witnesses.len() {
            found.push(format!("{id}: a witness is named twice"));
        }
    }
    found
}

/// [`validate`], and every edit applies to the repository at `root`.
///
/// This builds nothing, so drift is reported in seconds. Whether the witnesses
/// exist and bite is what [`run()`] establishes.
pub fn drift(root: &Path, catalog: &[Mutant]) -> Vec<String> {
    let mut found = validate(catalog);
    for mutant in catalog {
        match fs::read_to_string(root.join(mutant.file)) {
            Ok(source) => found.extend(mutant.apply(&source).err()),
            Err(error) => found.push(format!(
                "{}: cannot read {}: {error}",
                mutant.id, mutant.file
            )),
        }
    }
    found
}

/// What one run of one witness showed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Exactly this one test ran and passed.
    Passed,
    /// Exactly this one test ran and failed.
    Failed,
    /// Nothing ran: the name matches no test, or the test is ignored.
    NotRun,
    /// No usable verdict: a crash, an abort, or more than one test.
    Broken,
    /// The test outlived its bound and was killed.
    TimedOut,
}

impl Verdict {
    /// The verdict in one libtest run's output, filtered to one exact name.
    ///
    /// The exit status must agree with the summary: a summary that says one
    /// thing beside a status that says another is no verdict.
    pub fn read(output: &str, exited_successfully: bool) -> Self {
        let counts = output.lines().rev().find_map(|line| {
            let summary = line.strip_prefix("test result: ")?;
            let (_, counts) = summary.split_once(". ")?;
            let mut counts = counts.split("; ");
            let passed = counts.next()?.strip_suffix(" passed")?;
            let failed = counts.next()?.strip_suffix(" failed")?;
            Some((passed.parse::<u64>().ok()?, failed.parse::<u64>().ok()?))
        });
        match (counts, exited_successfully) {
            (Some((1, 0)), true) => Verdict::Passed,
            (Some((0, 1)), false) => Verdict::Failed,
            (Some((0, 0)), true) => Verdict::NotRun,
            _ => Verdict::Broken,
        }
    }

    /// How a report names it.
    pub fn name(self) -> &'static str {
        match self {
            Verdict::Passed => "passed",
            Verdict::Failed => "failed",
            Verdict::NotRun => "did not run",
            Verdict::Broken => "gave no verdict",
            Verdict::TimedOut => "timed out",
        }
    }
}
