//! The guarantees page shows exactly the invariants and the proofs they cite.
use std::path::Path;

#[test]
fn the_guarantees_invariant_map_matches_the_contract() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let problems = tollgate_repo_check::guarantees::check(&root).expect("sources are readable");
    if !problems.is_empty() {
        let expected = tollgate_repo_check::guarantees::expected(&root).expect("readable");
        let table: Vec<String> = expected.iter().map(|row| row.render()).collect();
        panic!(
            "{}\n\nThe table docs/GUARANTEES.md should carry:\n\n{}",
            problems.join("\n"),
            table.join("\n")
        );
    }
}

#[test]
fn the_stated_proof_figures_are_current() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let problems = tollgate_repo_check::guarantees::check_figures(&root).expect("readable");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn a_theorem_citation_is_a_proof_and_a_definition_name_is_not() {
    let dir = tempfile::tempdir().expect("scratch repository");
    let root = dir.path();
    std::fs::create_dir_all(root.join("formal/lean/Tollgate")).expect("lean dir");
    std::fs::write(
        root.join("formal/lean/Tollgate/Ledger.lean"),
        "def commit : Nat := 0\ntheorem ledger_closes : True := trivial\n",
    )
    .expect("lean");
    std::fs::write(
        root.join("formal/lean/Tollgate/Timing.lean"),
        "def x : Nat := 0\n",
    )
    .expect("lean");
    std::fs::write(
        root.join("INVARIANTS.md"),
        "1. **Proven.** `ledger_closes`\n2. **Word.** `commit`\n3. **File.** see Timing.lean\n",
    )
    .expect("invariants");
    let rows = tollgate_repo_check::guarantees::expected(root).expect("readable");
    let modules: Vec<Vec<&str>> = rows
        .iter()
        .map(|row| row.modules.iter().map(String::as_str).collect())
        .collect();
    assert_eq!(modules, vec![vec!["Ledger"], vec![], vec!["Timing"]]);
}

/// A repository with two invariants, one citing a theorem among other words,
/// and a guarantees page and README stating `figures`.
fn small_repo(table: &str, figures: (&str, &str)) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("scratch repository");
    let root = dir.path();
    std::fs::create_dir_all(root.join("formal/lean/Tollgate")).expect("lean dir");
    std::fs::create_dir_all(root.join("docs")).expect("docs dir");
    std::fs::write(
        root.join("formal/lean/Tollgate/Ledger.lean"),
        "theorem ledger_closes : True := trivial\n",
    )
    .expect("lean");
    std::fs::write(
        root.join("INVARIANTS.md"),
        "1. **Cited.** See `{ledger_closes, other_words}`.\n2. **Plain.** Tested only.\n",
    )
    .expect("invariants");
    std::fs::write(
        root.join("docs/GUARANTEES.md"),
        format!(
            "{} numbered statements.\n\n<!-- invariant-map:start -->\n| # | Invariant | Proofs |\n|---|---|---|\n{table}\n<!-- invariant-map:end -->\n",
            figures.1
        ),
    )
    .expect("page");
    std::fs::write(root.join("README.md"), figures.0).expect("readme");
    dir
}

const ROW_1: &str = "| 1 | Cited. | [Ledger](../formal/lean/Tollgate/Ledger.lean) |";
const ROW_2: &str = "| 2 | Plain. | — |";
const README_OK: &str = "2 numbered invariants, 1 Lean 4 modules with 1 theorems";
const PAGE_OK: &str = "2 numbered statements; 1 Lean 4 modules with 1 theorems; 1 of the 2";

#[test]
fn a_citation_among_other_words_in_one_span_still_counts() {
    let dir = small_repo(&format!("{ROW_1}\n{ROW_2}"), (README_OK, PAGE_OK));
    let rows = tollgate_repo_check::guarantees::expected(dir.path()).expect("readable");
    assert_eq!(rows[0].render(), ROW_1);
    assert_eq!(rows[1].render(), ROW_2);
}

#[test]
fn a_matching_table_has_no_problems_and_a_wrong_row_is_named_alone() {
    let good = small_repo(&format!("{ROW_1}\n{ROW_2}"), (README_OK, PAGE_OK));
    assert!(
        tollgate_repo_check::guarantees::check(good.path())
            .expect("readable")
            .is_empty()
    );

    let wrong = small_repo(
        &format!("{ROW_1}\n| 2 | Plain. | [Ledger](x) |"),
        (README_OK, PAGE_OK),
    );
    let problems = tollgate_repo_check::guarantees::check(wrong.path()).expect("readable");
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(problems[0].starts_with("row 2: expected"), "{problems:?}");
}

#[test]
fn stale_figures_are_each_reported() {
    let fresh = small_repo(&format!("{ROW_1}\n{ROW_2}"), (README_OK, PAGE_OK));
    assert!(
        tollgate_repo_check::guarantees::check_figures(fresh.path())
            .expect("readable")
            .is_empty()
    );
    let stale = small_repo(
        &format!("{ROW_1}\n{ROW_2}"),
        ("3 numbered invariants", PAGE_OK),
    );
    let problems = tollgate_repo_check::guarantees::check_figures(stale.path()).expect("readable");
    assert_eq!(
        problems,
        [
            "README.md must state \"2 numbered invariants\"",
            "README.md must state \"1 Lean 4 modules\"",
            "README.md must state \"with 1 theorems\"",
        ]
    );
}
