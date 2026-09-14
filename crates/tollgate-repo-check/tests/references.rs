use std::{fs, path::Path, process::Command};
use tempfile::TempDir;

fn write(root: &Path, name: &str, source: &str) {
    let path = root.join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, source).unwrap();
}

fn repository(document: &str) -> TempDir {
    let root = tempfile::tempdir().unwrap();
    write(
        root.path(),
        "crates/demo/src/lib.rs",
        "mod tests { #[test] fn money_is_conserved() {} }",
    );
    write(root.path(), "examples/demo/src/main.rs", "fn main() {}");
    write(
        root.path(),
        "formal/lean/Tollgate/Ledger.lean",
        "namespace Tollgate.Ledger\ntheorem debit_is_bounded : True := by trivial\nend Tollgate.Ledger\n",
    );
    write(root.path(), "testing/invariant_external_symbols.json", "{}");
    write(root.path(), "INVARIANTS.md", document);
    root
}

fn failure(root: &Path) -> String {
    match tollgate_repo_check::check(root) {
        Ok(_) => panic!("expected an invalid citation to fail"),
        Err(error) => error,
    }
}

#[test]
fn references_resolve_across_brace_groups_macros_methods_fields_and_proofs() {
    let root = repository(
        "*Tests:* `tests::{money_is_conserved,\n property_holds}`.\n`Limits::batch_size`, `Limits::checked_quote`, `Tollgate.Ledger.debit_is_bounded`, `Ledger.debit_is_bounded`, `formal/lean/Tollgate/Ledger.lean`.",
    );
    write(
        root.path(),
        "crates/demo/src/limits.rs",
        r#"
        pub struct Limits { batch_size: usize }
        impl Limits { fn checked_quote(&self) {} }
        #[cfg(any())]
        mod tests {
            proptest::proptest! { #[test] fn property_holds(n in 0..10) { assert!(n < 10); } }
        }
    "#,
    );
    let report = tollgate_repo_check::check(root.path()).unwrap();
    assert_eq!(report.resolved, 7);
    assert_eq!(report.external, 0);
}

#[test]
fn a_stale_witness_and_a_wrong_qualifier_both_fail() {
    let root = repository(
        "# Contract\n\n`tests::{money_is_conserved, old_test_name}`\n`other::money_is_conserved`",
    );
    let error = failure(root.path());
    assert!(
        error.contains("INVARIANTS.md:3: unresolved reference `tests::old_test_name`"),
        "{error}"
    );
    assert!(
        error.contains("INVARIANTS.md:4: unresolved reference `other::money_is_conserved`"),
        "{error}"
    );
}

#[test]
fn comments_strings_and_unexpanded_generators_cannot_supply_a_witness() {
    let root = repository("`imaginary_test` and `imaginary_alias`");
    write(
        root.path(),
        "crates/demo/src/lib.rs",
        r####"
        // fn imaginary_test() {}
        /* fn imaginary_test() {} */
        const EXAMPLE: &str = "fn imaginary_test() {}";
        const RAW: &str = r###"fn imaginary_test() {}"###;
        quote! { fn imaginary_test() {} }
        proptest! {
            #[test] fn real_property(value in 0..10) {
                quote! { fn imaginary_test() {} }
            }
            type imaginary_alias = u8;
        }
    "####,
    );
    // Only the identifier that follows `fn` is a declaration. Every other
    // adjacent identifier pair in the token stream is ordinary syntax.
    let error = failure(root.path());
    assert!(
        error.contains("unresolved reference `imaginary_test`"),
        "{error}"
    );
    assert!(
        error.contains("unresolved reference `imaginary_alias`"),
        "{error}"
    );
}

#[test]
fn a_missing_or_misqualified_lean_witness_fails_even_when_mentioned_in_comments() {
    let root = repository("`Ledger.imaginary_proof` and `Other.debit_is_bounded`");
    write(
        root.path(),
        "formal/lean/Tollgate/Ledger.lean",
        r#"
        /- theorem imaginary_proof
           /- nested -/
           namespace Other
           theorem debit_is_bounded -/
        namespace Tollgate.Ledger
        -- theorem imaginary_proof
        def example := "theorem imaginary_proof"
        theorem debit_is_bounded : True := by trivial
        end Tollgate.Ledger
    "#,
    );
    let error = failure(root.path());
    assert!(error.contains("`Ledger.imaginary_proof`"), "{error}");
    assert!(error.contains("`Other.debit_is_bounded`"), "{error}");
}

#[test]
fn missing_proof_files_and_path_escape_fail() {
    for name in [
        "formal/lean/Tollgate/Missing.lean",
        "formal/../outside.lean",
    ] {
        let root = repository(&format!("`{name}`"));
        write(
            root.path(),
            "outside.lean",
            "theorem made_up : True := by trivial",
        );
        assert!(failure(root.path()).contains("missing proof file"));
    }
}

#[test]
fn external_references_require_a_reason_and_obsolete_exceptions_fail() {
    let root = repository("`catch_unwind` and `money_is_conserved`");
    write(
        root.path(),
        "testing/invariant_external_symbols.json",
        r#"{"catch_unwind":"std panic API"}"#,
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().external, 1);
    for manifest in [
        r#"{"catch_unwind":" "}"#,
        r#"{"catch_unwind":"std panic API","old_symbol":"removed"}"#,
        r#"{"catch_unwind":"std panic API","money_is_conserved":"already in source"}"#,
    ] {
        write(
            root.path(),
            "testing/invariant_external_symbols.json",
            manifest,
        );
        assert!(failure(root.path()).contains("unused or invalid external reference"));
    }
}

#[test]
fn malformed_input_and_missing_source_are_failures() {
    for document in [
        "`tests::{money_is_conserved,}`",
        "`tests::{money_is_conserved`",
        "`unclosed_name",
        "```rust\nnot_closed",
        "No named witnesses.",
    ] {
        let root = repository(document);
        assert!(!failure(root.path()).is_empty());
    }
    let root = repository("`money_is_conserved`");
    write(root.path(), "crates/demo/src/lib.rs", "fn broken(");
    assert!(failure(root.path()).contains("lib.rs:"));
    fs::remove_dir_all(root.path().join("crates")).unwrap();
    assert!(failure(root.path()).contains("crates:"));
}

#[test]
fn malformed_lean_or_external_metadata_cannot_produce_a_partial_success() {
    for source in [
        "/- never closed",
        "def text := \"never closed",
        "namespace Tollgate.Ledger\ntheorem debit_is_bounded : True := by trivial",
        "namespace Tollgate.Ledger\nend Other",
        "end",
    ] {
        let root = repository("`money_is_conserved`");
        write(root.path(), "formal/lean/Tollgate/Ledger.lean", source);
        assert!(failure(root.path()).contains("Ledger.lean:"));
    }
    let root = repository("`money_is_conserved`");
    write(
        root.path(),
        "testing/invariant_external_symbols.json",
        "{broken",
    );
    assert!(failure(root.path()).contains("invariant_external_symbols.json:"));
}

#[test]
fn sections_and_trait_methods_keep_their_declared_scopes() {
    let root = repository("`Ledger.debit_is_bounded`, `Ledger.other_bound`, `Api::checked_read`");
    write(
        root.path(),
        "crates/demo/src/lib.rs",
        "trait Api { fn checked_read(&self); }",
    );
    write(
        root.path(),
        "formal/lean/Tollgate/Ledger.lean",
        "namespace Tollgate\nnamespace Ledger\nsection Proofs\nprivate theorem debit_is_bounded : True := by trivial\nend Proofs\nprotected theorem other_bound : True := by trivial\nend Ledger\nend Tollgate\n",
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 3);
}

#[cfg(unix)]
#[test]
fn source_symlinks_fail_explicitly_instead_of_being_silently_omitted() {
    let root = repository("`money_is_conserved`");
    std::os::unix::fs::symlink("lib.rs", root.path().join("crates/demo/src/linked.rs")).unwrap();
    assert!(failure(root.path()).contains("source symlinks are not supported"));
}

#[test]
fn fenced_examples_are_not_witnesses_but_wrapped_code_spans_are() {
    let root = repository(
        "```rust\n`missing_test`\n```\n`tests::money_is_conserved`\n~~~text\n`other_missing_test`\n~~~\n``Ledger.debit_is_bounded``",
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 2);
}

#[test]
fn command_line_controls_precede_validation_and_terminator_is_respected() {
    let root = repository("`money_is_conserved`");
    let binary = env!("CARGO_BIN_EXE_check_invariant_witnesses");
    for option in ["--help", "--version"] {
        let output = Command::new(binary)
            .args(["--invalid", option])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }
    let valid = Command::new(binary)
        .arg("--")
        .arg(root.path())
        .output()
        .unwrap();
    assert!(valid.status.success(), "{valid:?}");
    for args in [vec!["--invalid"], vec!["--", "--help"], vec!["one", "two"]] {
        let output = Command::new(binary).args(args).output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{output:?}");
    }
    let output = Command::new(binary)
        .args(["--version", "--help"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("check_invariant_witnesses {}\n", env!("CARGO_PKG_VERSION"))
    );
    #[cfg(unix)]
    {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        for flag in ["--help", "--version"] {
            let output = Command::new(binary)
                .arg(OsString::from_vec(vec![0xff]))
                .arg(flag)
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
        }
    }
}

#[test]
fn files_without_the_scanned_extension_are_not_parsed_as_source() {
    let root = repository("`money_is_conserved`");
    write(root.path(), "crates/demo/src/NOTES.md", "Not Rust: fn (\n");
    write(
        root.path(),
        "formal/lean/Tollgate/README.md",
        "Not Lean: /- never closed\n",
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 1);
}

#[test]
fn a_lean_comment_or_string_holding_only_a_line_break_still_separates_declarations() {
    let root = repository(
        "`Ledger.debit_is_bounded`, `Ledger.second_bound`, `Ledger.string_label`, `Ledger.third_bound`",
    );
    write(
        root.path(),
        "formal/lean/Tollgate/Ledger.lean",
        "namespace Tollgate.Ledger\ntheorem debit_is_bounded : True := by trivial /-\n-/ theorem second_bound : True := by trivial\ndef string_label := \"\n\" theorem third_bound : True := by trivial\nend Tollgate.Ledger\n",
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 4);
}

#[test]
fn a_hyphen_in_lean_code_does_not_open_a_line_comment() {
    let root =
        repository("`Ledger.margin_of_error`, `Ledger.debit_is_bounded`, `Ledger.fake_bound`");
    write(
        root.path(),
        "formal/lean/Tollgate/Ledger.lean",
        "namespace Tollgate.Ledger\ndef margin_of_error := 1 - 2 /-\ntheorem fake_bound : True := by trivial\n-/\ntheorem debit_is_bounded : True := by trivial\nend Tollgate.Ledger\n",
    );
    // The subtraction must not swallow the block comment that follows it, or a
    // commented-out declaration would become a witness.
    let error = failure(root.path());
    assert!(
        error.contains("unresolved reference `Ledger.fake_bound`"),
        "{error}"
    );
    assert!(!error.contains("margin_of_error"), "{error}");
}

#[test]
fn spans_that_are_not_declaration_paths_are_not_citations() {
    let root = repository(
        "`_leading_underscore_helper`, `500_ms`, `Strict`, `formal/lean/Tollgate`, `cargo test -p tollgate-core reservation::tests::commit_cancel_race_one_winner`",
    );
    write(
        root.path(),
        "crates/demo/src/helpers.rs",
        "fn _leading_underscore_helper() {}",
    );
    // Prose spans -- durations, type names, directories and shell commands --
    // are not witness names; a leading underscore is still an identifier.
    let report = tollgate_repo_check::check(root.path()).unwrap();
    assert_eq!(report.resolved, 1);
    assert_eq!(report.external, 0);
}

#[test]
fn module_scopes_start_below_the_source_directory() {
    let root = repository(
        "`inner::deep::nested_witness`, `deep::nested_witness`, `harness::scenarios::integration_witness`",
    );
    write(
        root.path(),
        "crates/demo/src/inner/deep.rs",
        "fn nested_witness() {}",
    );
    write(
        root.path(),
        "crates/demo/tests/harness/scenarios.rs",
        "fn integration_witness() {}",
    );
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 3);

    let root = repository("`src::inner::deep::nested_witness`");
    write(
        root.path(),
        "crates/demo/src/inner/deep.rs",
        "fn nested_witness() {}",
    );
    // A witness path is a module path, so the crate and source directories
    // above the module tree are not part of it.
    assert!(
        failure(root.path()).contains("unresolved reference `src::inner::deep::nested_witness`")
    );
}

#[cfg(unix)]
#[test]
fn a_proof_file_resolving_outside_the_repository_is_rejected() {
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join("escaped.lean");
    fs::write(&target, "theorem made_up : True := by trivial").unwrap();
    let root = repository("`formal/escaped_proof.lean`");
    std::os::unix::fs::symlink(&target, root.path().join("formal/escaped_proof.lean")).unwrap();
    // The path exists and is a file; only canonicalization shows that it
    // leaves the repository.
    assert!(failure(root.path()).contains("missing proof file"));
}

#[test]
fn a_wrapped_code_span_does_not_move_later_reports_backwards() {
    let root = repository("# Contract\n\n`tests::\nmoney_is_conserved`\n\n`old_test_name`\n");
    assert!(failure(root.path()).contains("INVARIANTS.md:6: unresolved reference `old_test_name`"));
}

#[test]
fn the_repository_argument_is_used_and_defaults_to_the_working_directory() {
    let named = repository("`money_is_conserved`");
    let working = repository("`money_is_conserved` and `Ledger.debit_is_bounded`");
    let binary = env!("CARGO_BIN_EXE_check_invariant_witnesses");

    let output = Command::new(binary)
        .arg(named.path())
        .current_dir(working.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("(1 resolved"),
        "the named repository was not the one checked"
    );

    let output = Command::new(binary)
        .current_dir(working.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("(2 resolved"),
        "no argument must check the working directory"
    );

    let output = Command::new(binary)
        .arg("--invalid")
        .arg(named.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unknown option: --invalid"),
        "an option before the terminator is rejected as an option"
    );
}

#[test]
fn a_fence_is_closed_only_by_its_own_marker() {
    // A tilde run inside a backtick fence is fenced content, not a closing
    // delimiter; the span it wraps stays a prose example.
    let root = repository("```rust\n~~~\n`missing_witness`\n```\n`tests::money_is_conserved`\n");
    assert_eq!(tollgate_repo_check::check(root.path()).unwrap().resolved, 1);
}
