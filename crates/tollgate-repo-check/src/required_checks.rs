//! Which status checks `main` must require (GH-88).
//!
//! GitHub reports a job skipped behind a failed dependency as passing, so a
//! job other jobs depend on blocks a merge only if it is itself required. The
//! list in `.github/required-checks.txt` is held to the workflow offline, on
//! every pull request, and to the live branch protection by an audit that an
//! administrator runs: reading protection needs a permission the workflow
//! token does not have.
//!
//! The workflow is read line by line rather than as YAML. Only the shapes this
//! repository writes are understood, and anything else is an error: a job the
//! reader missed would be a job nothing requires.

use std::collections::{BTreeMap, BTreeSet};
use std::{fs, path::Path};

/// The workflow whose jobs gate a pull request.
pub const WORKFLOW: &str = ".github/workflows/ci.yml";
/// The declared required checks.
pub const REQUIRED: &str = ".github/required-checks.txt";

#[derive(Debug, Default)]
struct Job {
    name: Option<String>,
    condition: String,
    needs: Vec<String>,
    matrix: bool,
}

/// The `jobs:` mapping, keyed by job id.
fn jobs(workflow: &str) -> Result<BTreeMap<String, Job>, String> {
    let mut jobs: BTreeMap<String, Job> = BTreeMap::new();
    let mut inside = false;
    let mut current: Option<String> = None;
    let mut in_strategy = false;
    for (number, line) in workflow.lines().enumerate() {
        let at = |what: &str| format!("{WORKFLOW}:{}: {what}", number + 1);
        let text = line.trim_start();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let indent = line.len() - text.len();
        if indent == 0 {
            inside = line.trim_end() == "jobs:";
            current = None;
            continue;
        }
        if !inside {
            continue;
        }
        if indent == 2 {
            let id = text
                .trim_end()
                .strip_suffix(':')
                .filter(|id| {
                    !id.is_empty()
                        && id
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                })
                .ok_or_else(|| at("expected a job id on a line of its own"))?;
            if jobs.insert(id.to_owned(), Job::default()).is_some() {
                return Err(at("this job id is declared twice"));
            }
            current = Some(id.to_owned());
            in_strategy = false;
            continue;
        }
        let job = current
            .as_ref()
            .and_then(|id| jobs.get_mut(id))
            .ok_or_else(|| at("expected jobs to be indented by two spaces"))?;
        if indent < 4 {
            return Err(at("expected job keys to be indented by four spaces"));
        }
        if indent == 4 {
            let (key, value) = text
                .split_once(':')
                .ok_or_else(|| at("expected `key: value`"))?;
            let value = value.trim();
            in_strategy = key == "strategy";
            match key {
                "name" => {
                    if value.is_empty() || value.starts_with(['"', '\'', '|', '>', '&', '*']) {
                        return Err(at("write a job name unquoted, on its own line"));
                    }
                    job.name = Some(value.to_owned());
                }
                "if" => job.condition = value.to_owned(),
                "needs" => {
                    let list = value
                        .strip_prefix('[')
                        .and_then(|list| list.strip_suffix(']'))
                        .ok_or_else(|| at("write `needs` as an inline list, `[a, b]`"))?;
                    job.needs = list.split(',').map(|id| id.trim().to_owned()).collect();
                }
                _ => {}
            }
        } else if in_strategy && indent == 6 && text.starts_with("matrix:") {
            job.matrix = true;
        }
    }
    if jobs.is_empty() {
        return Err(format!("{WORKFLOW}: no jobs found"));
    }
    Ok(jobs)
}

/// The declared names, in order, comments and blank lines dropped.
fn declared(list: &str) -> Vec<&str> {
    list.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

/// Every way the workflow and the declared list disagree.
///
/// Every job is required under its name. The one exception is a matrix an
/// aggregate summarises — a job that needs exactly it and runs `always()` —
/// which is required through that aggregate instead.
///
/// # Errors
///
/// A workflow this reader does not understand.
pub fn disagreements(workflow: &str, list: &str) -> Result<Vec<String>, String> {
    let jobs = jobs(workflow)?;
    let names = declared(list);
    let mut found = Vec::new();
    let mut listed = BTreeSet::new();
    for name in &names {
        if !listed.insert(*name) {
            found.push(format!("`{name}` is listed twice"));
        }
    }
    let mut expected = BTreeSet::new();
    for (id, job) in &jobs {
        for needed in &job.needs {
            if !jobs.contains_key(needed) {
                found.push(format!("job `{id}` needs `{needed}`, which is not a job"));
            }
        }
        let Some(name) = job.name.as_deref() else {
            found.push(format!("job `{id}` has no name to require it by"));
            continue;
        };
        if job.matrix {
            let summarised = jobs.values().any(|other| {
                other.needs.len() == 1
                    && other.needs[0] == *id
                    && other.condition.starts_with("always()")
            });
            if !summarised {
                found.push(format!(
                    "matrix job `{id}` has no aggregate (`needs: [{id}]`, `if: always() ...`) \
                     to be required through"
                ));
            }
            continue;
        }
        if !expected.insert(name) {
            found.push(format!("two jobs are named `{name}`"));
        }
        if !listed.contains(name) {
            found.push(format!("job `{id}` (`{name}`) is not listed in {REQUIRED}"));
        }
    }
    for name in listed.difference(&expected) {
        found.push(format!(
            "`{name}` is listed but no job is required by that name"
        ));
    }
    Ok(found)
}

/// [`disagreements`] for the repository at `root`.
///
/// # Errors
///
/// A file that cannot be read, or a workflow this reader does not understand.
pub fn check(root: &Path) -> Result<Vec<String>, String> {
    let read = |path: &str| {
        fs::read_to_string(root.join(path)).map_err(|error| format!("cannot read {path}: {error}"))
    };
    disagreements(&read(WORKFLOW)?, &read(REQUIRED)?)
}

/// Every way live branch protection differs from the declared list.
/// `protection` is GitHub's `required_status_checks` response.
///
/// # Errors
///
/// A response that is not that: an audit that could not read the setting has
/// not passed.
pub fn drift(protection: &str, list: &str) -> Result<Vec<String>, String> {
    let unreadable = |what: &str| format!("branch protection was not read: {what}");
    let response: serde_json::Value =
        serde_json::from_str(protection).map_err(|_| unreadable("the response is not JSON"))?;
    let strict = response["strict"]
        .as_bool()
        .ok_or_else(|| unreadable("no `strict` field"))?;
    let live = response["checks"]
        .as_array()
        .ok_or_else(|| unreadable("no `checks` field"))?
        .iter()
        .map(|check| {
            check["context"]
                .as_str()
                .ok_or_else(|| unreadable("a check has no `context`"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let wanted: BTreeSet<_> = declared(list).into_iter().collect();
    let mut found = Vec::new();
    if !strict {
        found.push("a branch need not be up to date to merge (`strict` is false)".to_owned());
    }
    for name in wanted.difference(&live) {
        found.push(format!("`{name}` is listed but not required on `main`"));
    }
    for name in live.difference(&wanted) {
        found.push(format!("`{name}` is required on `main` but not listed"));
    }
    Ok(found)
}
