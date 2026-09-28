//! The documentation site's link contract, end to end through the
//! preprocessor's JSON interface and through the binary mdBook runs.
//!
//! The site fails its build rather than publishing a dead link, so the
//! contract is as much the refusals as the rewrites: each refusal below is a
//! link that would otherwise 404 on the site while working on GitHub, or the
//! reverse.
use serde_json::{Value, json};
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};

/// A throwaway repository: `files` written under it, `docs/` as the book.
fn repo(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("scratch repository");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("has a parent")).expect("create dirs");
        std::fs::write(full, body).expect("write file");
    }
    dir
}

fn chapter(path: &str, content: &str, sub_items: Vec<Value>) -> Value {
    json!({"Chapter": {
        "name": path, "content": content, "number": null, "sub_items": sub_items,
        "path": path, "source_path": path, "parent_names": []
    }})
}

/// The `[context, book]` pair mdBook writes, with chapters read from disk.
fn input(root: &Path, chapters: &[&str]) -> String {
    let sections: Vec<Value> = chapters
        .iter()
        .map(|path| {
            let content =
                std::fs::read_to_string(root.join("docs").join(path)).expect("chapter exists");
            chapter(path, &content, Vec::new())
        })
        .collect();
    json!([
        {
            "root": root, "renderer": "html", "mdbook_version": "0.4.52",
            "config": {
                "book": {"src": "docs"},
                "preprocessor": {"repo-links": {"repository": "https://github.com/o/r"}}
            }
        },
        {"sections": sections, "__non_exhaustive": null}
    ])
    .to_string()
}

fn contents(output: &str) -> Vec<String> {
    let book: Value = serde_json::from_str(output).expect("book JSON");
    let mut found = Vec::new();
    fn walk(sections: &Value, found: &mut Vec<String>) {
        for item in sections.as_array().into_iter().flatten() {
            if let Some(chapter) = item.get("Chapter") {
                found.push(chapter["content"].as_str().unwrap_or_default().to_owned());
                walk(&chapter["sub_items"], found);
            }
        }
    }
    walk(&book["sections"], &mut found);
    found
}

fn problems(root: &Path, chapters: &[&str]) -> Vec<String> {
    tollgate_repo_check::book::preprocess(&input(root, chapters)).expect_err("the build must fail")
}

#[test]
fn links_to_chapters_stay_in_the_book_and_the_rest_go_to_github() {
    let dir = repo(&[
        (
            "INVARIANTS.md",
            "# Invariants\n\n## Bounded spend\n\nSee [design](docs/DESIGN.md#why).\n",
        ),
        ("testing/evidence.json", "{}"),
        ("crates/x/src/lib.rs", ""),
        (
            "docs/EMBEDDING.md",
            "# Embedding\n\n[contract](../INVARIANTS.md#bounded-spend), \
             [why](DESIGN.md#why), [evidence](../testing/evidence.json), \
             [source](../crates/x/src), [web](https://example.com/a.md), [here](#embedding)\n",
        ),
        ("docs/DESIGN.md", "# Design\n\n## Why\n"),
        (
            "docs/site/invariants.md",
            "<!-- repo-page: INVARIANTS.md -->\n",
        ),
    ]);
    let output = tollgate_repo_check::book::preprocess(&input(
        dir.path(),
        &["EMBEDDING.md", "DESIGN.md", "site/invariants.md"],
    ))
    .expect("every link resolves");
    let pages = contents(&output);
    assert_eq!(
        pages[0],
        "# Embedding\n\n[contract](site/invariants.md#bounded-spend), \
         [why](DESIGN.md#why), [evidence](https://github.com/o/r/blob/main/testing/evidence.json), \
         [source](https://github.com/o/r/tree/main/crates/x/src), [web](https://example.com/a.md), [here](#embedding)\n"
    );
    // The repo page carries the file's content, and its links resolve from
    // the file's own directory, not from where the page is published.
    assert_eq!(
        pages[2],
        "# Invariants\n\n## Bounded spend\n\nSee [design](../DESIGN.md#why).\n"
    );
}

#[test]
fn a_missing_path_fails_the_build() {
    let dir = repo(&[("docs/A.md", "# A\n\n[gone](../testing/nothing.json)\n")]);
    let found = problems(dir.path(), &["A.md"]);
    assert_eq!(found.len(), 1);
    assert!(
        found[0].contains("testing/nothing.json` does not exist"),
        "{found:?}"
    );
}

#[test]
fn a_missing_heading_fails_the_build_in_and_out_of_the_book() {
    let dir = repo(&[
        ("SECURITY.md", "# Security\n"),
        (
            "docs/A.md",
            "# A\n\n[b](B.md#nope) [s](../SECURITY.md#nope) [self](#nope)\n",
        ),
        ("docs/B.md", "# B\n"),
    ]);
    let found = problems(dir.path(), &["A.md", "B.md"]);
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(found.iter().all(|problem| problem.contains("heading")));
}

#[test]
fn a_document_left_out_of_the_summary_fails_the_build() {
    let dir = repo(&[("docs/A.md", "# A\n"), ("docs/ORPHAN.md", "# Orphan\n")]);
    let found = problems(dir.path(), &["A.md"]);
    assert_eq!(
        found,
        ["docs/ORPHAN.md is not in docs/SUMMARY.md, so the site omits it"]
    );
}

#[test]
fn a_link_to_a_document_under_the_book_that_is_not_a_chapter_fails() {
    let dir = repo(&[
        ("docs/A.md", "# A\n\n[o](ORPHAN.md)\n"),
        ("docs/ORPHAN.md", ""),
    ]);
    let found = problems(dir.path(), &["A.md"]);
    assert!(
        found
            .iter()
            .any(|p| p.contains("is not a chapter of the book")),
        "{found:?}"
    );
}

#[test]
fn a_link_that_climbs_out_of_the_repository_fails() {
    let dir = repo(&[("docs/A.md", "# A\n\n[x](../../outside.md)\n")]);
    let found = problems(dir.path(), &["A.md"]);
    assert!(found[0].contains("leaves the repository"), "{found:?}");
}

#[test]
fn an_unreadable_repo_page_fails() {
    let dir = repo(&[("docs/site/x.md", "<!-- repo-page: MISSING.md -->\n")]);
    let found = problems(dir.path(), &["site/x.md"]);
    assert!(
        found[0].contains("repo-page `MISSING.md` cannot be read"),
        "{found:?}"
    );
}

#[test]
fn every_problem_is_reported_not_just_the_first() {
    let dir = repo(&[
        ("docs/A.md", "# A\n\n[x](../nothing-1) [y](../nothing-2)\n"),
        ("docs/B.md", "# B\n\n[z](../nothing-3)\n"),
    ]);
    assert_eq!(problems(dir.path(), &["A.md", "B.md"]).len(), 3);
}

#[test]
fn the_binary_supports_every_renderer_and_fails_on_a_dead_link() {
    let exe = env!("CARGO_BIN_EXE_mdbook-repo-links");
    let supports = Command::new(exe)
        .args(["supports", "html"])
        .status()
        .expect("run");
    assert!(supports.success());
    let help = Command::new(exe)
        .args(["--help", "junk"])
        .output()
        .expect("run");
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).starts_with("Usage: mdbook-repo-links"));
    let wrong = Command::new(exe).arg("junk").status().expect("run");
    assert_eq!(wrong.code(), Some(2));

    let dir = repo(&[("docs/A.md", "# A\n\n[gone](nothing.md)\n")]);
    let mut child = Command::new(exe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input(dir.path(), &["A.md"]).as_bytes())
        .expect("write input");
    let out = child.wait_with_output().expect("finish");
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "a failed build must not emit a book");
    assert!(String::from_utf8_lossy(&out.stderr).contains("nothing.md"));
}

/// Inline links and reference definitions are collected separately, so their
/// rewrites must be applied by position, not by collection order: applying a
/// later edit first shifts every earlier offset (#36 review).
#[test]
fn inline_links_and_reference_definitions_are_rewritten_together() {
    let dir = repo(&[
        ("asset.txt", "x"),
        (
            "docs/A.md",
            "# A\n\n[inline](../asset.txt)\n\n[r]: ../asset.txt\n",
        ),
    ]);
    let output = tollgate_repo_check::book::preprocess(&input(dir.path(), &["A.md"]))
        .expect("every link resolves");
    assert_eq!(
        contents(&output)[0],
        "# A\n\n[inline](https://github.com/o/r/blob/main/asset.txt)\n\n\
         [r]: https://github.com/o/r/blob/main/asset.txt\n"
    );
}
