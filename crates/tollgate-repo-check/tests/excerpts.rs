//! Every excerpting document in this repository quotes its program exactly.
use std::path::Path;

#[test]
fn documentation_excerpts_match_their_programs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let problems = tollgate_repo_check::excerpts::check(&root).expect("docs are readable");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn a_drifted_excerpt_is_reported_with_its_line() {
    let dir = tempfile::tempdir().expect("scratch repository");
    std::fs::create_dir_all(dir.path().join("docs")).expect("docs");
    std::fs::write(
        dir.path().join("prog.rs"),
        "fn main() {\n    let a = 1;\n}\n",
    )
    .expect("prog");
    std::fs::write(
        dir.path().join("docs/T.md"),
        "<!-- excerpts-of: prog.rs -->\n\n```rust\nlet a = 2;\n```\n",
    )
    .expect("doc");
    let problems = tollgate_repo_check::excerpts::check(dir.path()).expect("readable");
    assert_eq!(
        problems,
        ["docs/T.md:3: this rust block is not an excerpt of `prog.rs`"]
    );
}
