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
