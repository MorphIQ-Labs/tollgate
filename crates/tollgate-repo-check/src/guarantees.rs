//! The guarantees page's invariant map, checked against its sources.
//!
//! `docs/GUARANTEES.md` shows every invariant beside the Lean modules that
//! prove something about it. Both halves come from elsewhere: the invariants
//! from `INVARIANTS.md`, and the proofs from the theorems each invariant cites.
//! A table typed out once would drift the first time either changed, and a
//! showcase that overstated its proofs would be worse than none. So the table
//! is checked: its rows must be exactly the invariants, in order, each with
//! exactly the modules its text cites.
//!
//! An invariant cites a module by naming one of its theorems in backticks, or
//! by naming the `.lean` file. Definitions do not count: their names are
//! ordinary words (`commit`, `balance`) that would match prose by accident.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

const START: &str = "<!-- invariant-map:start -->";
const END: &str = "<!-- invariant-map:end -->";
const LEAN: &str = "formal/lean/Tollgate";

/// One invariant as the map shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub number: u32,
    pub title: String,
    pub modules: BTreeSet<String>,
}

impl Row {
    /// The table row the page must carry for this invariant.
    pub fn render(&self) -> String {
        let proofs = if self.modules.is_empty() {
            "—".to_owned()
        } else {
            self.modules
                .iter()
                .map(|module| format!("[{module}](../{LEAN}/{module}.lean)"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!("| {} | {} | {proofs} |", self.number, self.title)
    }
}

/// The rows the page must show, derived from the repository.
pub fn expected(root: &Path) -> Result<Vec<Row>, String> {
    let invariants = read(&root.join("INVARIANTS.md"))?;
    let (theorems, files) = theorems(&root.join(LEAN))?;
    Ok(items(&invariants)
        .into_iter()
        .map(|(number, title, body)| {
            let mut modules = BTreeSet::new();
            for span in backticked(&body) {
                for part in span.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.')) {
                    let last = part.rsplit('.').next().unwrap_or(part);
                    if let Some(module) = theorems.get(part).or_else(|| theorems.get(last)) {
                        modules.insert(module.clone());
                    }
                }
            }
            for word in body.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.')) {
                if let Some(stem) = word.strip_suffix(".lean")
                    && files.contains(stem)
                {
                    modules.insert(stem.to_owned());
                }
            }
            Row {
                number,
                title,
                modules,
            }
        })
        .collect())
}

/// Problems with the page's map; empty when it matches.
pub fn check(root: &Path) -> Result<Vec<String>, String> {
    let page = read(&root.join("docs/GUARANTEES.md"))?;
    let expected = expected(root)?;
    let Some(table) = page
        .split_once(START)
        .and_then(|(_, rest)| rest.split_once(END))
        .map(|(table, _)| table)
    else {
        return Ok(vec![format!(
            "docs/GUARANTEES.md has no {START} … {END} table"
        )]);
    };
    let actual: Vec<&str> = table
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with('|') && !line.starts_with("|---") && !line.starts_with("| #")
        })
        .collect();
    let wanted: Vec<String> = expected.iter().map(Row::render).collect();
    if actual == wanted.iter().map(String::as_str).collect::<Vec<_>>() {
        return Ok(Vec::new());
    }
    let mut problems = Vec::new();
    for (index, want) in wanted.iter().enumerate() {
        match actual.get(index) {
            Some(have) if *have == want => {}
            Some(have) => problems.push(format!(
                "row {}: expected\n  {want}\nfound\n  {have}",
                index + 1
            )),
            None => problems.push(format!("missing row: {want}")),
        }
    }
    for extra in actual.iter().skip(wanted.len()) {
        problems.push(format!("row with no invariant: {extra}"));
    }
    Ok(problems)
}

fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))
}

/// Theorem and lemma names, qualified and bare, to their module; and every
/// module, including one with no theorems, which an invariant may still name
/// by file.
type Theorems = (BTreeMap<String, String>, BTreeSet<String>);

fn theorems(dir: &Path) -> Result<Theorems, String> {
    let mut found = BTreeMap::new();
    let mut modules = BTreeSet::new();
    let mut files: Vec<_> = fs::read_dir(dir)
        .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "lean"))
        .collect();
    files.sort();
    for file in files {
        let module = file
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        modules.insert(module.clone());
        for line in read(&file)?.lines() {
            let line = line.trim_start();
            let Some(rest) = line
                .strip_prefix("theorem ")
                .or_else(|| line.strip_prefix("lemma "))
            else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                .collect();
            if name.is_empty() {
                continue;
            }
            let bare = name.rsplit('.').next().unwrap_or(&name).to_owned();
            found.insert(name, module.clone());
            found.insert(bare, module.clone());
        }
    }
    Ok((found, modules))
}

/// Each top-level numbered invariant: number, bold title (whitespace
/// collapsed), and full text.
fn items(text: &str) -> Vec<(u32, String, String)> {
    let mut starts = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if let Some((number, rest)) = line.split_once(". **")
            && let Ok(number) = number.parse::<u32>()
            && !rest.is_empty()
        {
            starts.push((offset, number));
        }
        offset += line.len();
    }
    starts
        .iter()
        .enumerate()
        .map(|(index, &(start, number))| {
            let end = starts.get(index + 1).map_or(text.len(), |next| next.0);
            let body = &text[start..end];
            let title = body
                .split_once("**")
                .and_then(|(_, rest)| rest.split_once("**"))
                .map(|(title, _)| title.split_whitespace().collect::<Vec<_>>().join(" "))
                .unwrap_or_default();
            (number, title, body.to_owned())
        })
        .collect()
}

fn backticked(text: &str) -> impl Iterator<Item = &str> {
    text.split('`').skip(1).step_by(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_split_on_top_level_numbers_and_join_wrapped_titles() {
        let text =
            "# x\n\n1. **One.** body `a`\n   1. nested, not an item\n2. **Two\n   wraps.** more\n";
        let found = items(text);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].1, "One.");
        assert_eq!(found[1].1, "Two wraps.");
        assert!(found[0].2.contains("nested"));
    }

    #[test]
    fn a_row_renders_its_modules_as_links_or_a_dash() {
        let mut row = Row {
            number: 7,
            title: "T.".into(),
            modules: BTreeSet::new(),
        };
        assert_eq!(row.render(), "| 7 | T. | — |");
        row.modules.insert("Conservation".into());
        assert_eq!(
            row.render(),
            "| 7 | T. | [Conservation](../formal/lean/Tollgate/Conservation.lean) |"
        );
    }
}
