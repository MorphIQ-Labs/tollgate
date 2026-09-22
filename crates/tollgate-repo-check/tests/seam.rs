//! The seam-contract checker, against synthetic repositories.
//!
//! Fixtures are built inline rather than pointed at the real tree: a test that
//! asserted against `docs/DESIGN.md` would pass or fail for reasons belonging
//! to whatever else edited that file.

use std::{fs, path::Path};

use tempfile::TempDir;
use tollgate_repo_check::seam::undeclared;

/// The two files the seam list names, written under one root.
fn repository(design: &str, engine: &str, reservation: &str) -> TempDir {
    let root = tempfile::tempdir().expect("a temporary repository");
    write(root.path(), "docs/DESIGN.md", design);
    write(
        root.path(),
        "crates/tollgate-admission/src/engine.rs",
        engine,
    );
    write(
        root.path(),
        "crates/tollgate-core/src/reservation.rs",
        reservation,
    );
    root
}

fn write(root: &Path, name: &str, contents: &str) {
    let path = root.join(name);
    fs::create_dir_all(path.parent().expect("a parent directory")).expect("create the directory");
    fs::write(path, contents).expect("write the fixture");
}

/// Every seam type must exist, or the checker refuses rather than reporting a
/// clean seam, so each fixture carries a minimal body for all six.
fn engine_with(request_context: &str) -> String {
    format!(
        r"
        pub struct AdmissionEngine<M> {{ map: M }}
        impl<M> AdmissionEngine<M> {{
            pub fn begin(&self) {{}}
        }}
        pub struct RequestContext;
        impl RequestContext {{ {request_context} }}
        pub struct Pending<S>(S);
        impl<S> Pending<S> {{ pub fn quote(&self) {{}} }}
        pub struct ReadyToStart<S, P>(S, P);
        impl<S, P> ReadyToStart<S, P> {{ pub fn commit(self) {{}} }}
        pub struct Committed<S, P>(S, P);
        impl<S, P> Committed<S, P> {{ pub fn units(&self) {{}} }}
    "
    )
}

const RESERVATION: &str = r"
    pub struct CancelHandle;
    impl CancelHandle { pub fn cancel(&self) {} }
";

/// A contract naming every method the fixtures publish.
fn contract(extra: &str) -> String {
    format!(
        "## Staged admission interface (#96, 2026-08-28)\n\n```rust\n\
         pub fn begin();\npub fn quote();\npub fn commit();\npub fn units();\n\
         pub fn cancel();\n```\n{extra}\n\n## Next section\n\nUnrelated.\n"
    )
}

#[test]
fn a_published_method_the_contract_never_names_is_reported() {
    let root = repository(
        &contract(""),
        &engine_with("pub fn estimate_remaining(&self) {}"),
        RESERVATION,
    );
    let found = undeclared(root.path()).expect("the fixture is well formed");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].type_name, "RequestContext");
    assert_eq!(found[0].method, "estimate_remaining");
    assert!(
        found[0]
            .to_string()
            .contains("RequestContext::estimate_remaining")
    );
}

#[test]
fn a_method_declared_in_a_code_block_is_accepted() {
    let root = repository(
        &contract("```rust\npub fn estimate_remaining(&self) -> Option<CostUnits>;\n```"),
        &engine_with("pub fn estimate_remaining(&self) {}"),
        RESERVATION,
    );
    assert!(undeclared(root.path()).expect("well formed").is_empty());
}

/// The deliberate-exclusion path, and the reason there is no allowlist file:
/// an inline span in a sentence explaining why a method sits outside the seam
/// is itself the declaration.
#[test]
fn a_method_named_in_an_inline_span_is_accepted() {
    let root = repository(
        &contract("Deliberately outside the seam: `estimate_remaining` is advisory."),
        &engine_with("pub fn estimate_remaining(&self) {}"),
        RESERVATION,
    );
    assert!(undeclared(root.path()).expect("well formed").is_empty());
}

/// Prose alone does not declare. Without this the check goes quiet on exactly
/// the short, common names it is most likely to be wrong about — `new` and
/// `map` are ordinary English words, and the real section contains both.
#[test]
fn prose_that_merely_uses_the_word_does_not_declare_it() {
    let root = repository(
        &contract("The engine takes a new map when it is constructed."),
        &engine_with("pub fn new(&self) {} pub fn map(&self) {}"),
        RESERVATION,
    );
    let found = undeclared(root.path()).expect("well formed");
    let reported: Vec<&str> = found.iter().map(|u| u.method.as_str()).collect();
    assert_eq!(reported, ["map", "new"], "{found:?}");
}

/// The false positive that would make the checker unusable. `reservation.rs`
/// publishes `reserve_at_locality` and its siblings on types the seam does not
/// declare, and a file-scoped check would report every one of them.
#[test]
fn methods_on_types_outside_the_seam_are_ignored() {
    let reservation = r"
        pub struct CancelHandle;
        impl CancelHandle { pub fn cancel(&self) {} }
        pub struct Reservation;
        impl Reservation {
            pub fn reserve_at_locality(&self) {}
            pub fn commit_at_execution_start(&self) {}
        }
        pub struct SharedCharge;
        impl SharedCharge { pub fn cancel_handle(&self) {} }
    ";
    let root = repository(&contract(""), &engine_with(""), reservation);
    assert!(
        undeclared(root.path()).expect("well formed").is_empty(),
        "plumbing the seam is built from is not the seam"
    );
}

/// A trait `impl` publishes the trait's method, not the type's own.
#[test]
fn a_trait_impl_on_a_seam_type_declares_nothing_of_its_own() {
    let engine = format!(
        "{}\n pub trait Extra {{ fn extra(&self); }}\n impl Extra for RequestContext {{ fn extra(&self) {{}} }}",
        engine_with("")
    );
    let root = repository(&contract(""), &engine, RESERVATION);
    assert!(undeclared(root.path()).expect("well formed").is_empty());
}

#[test]
fn a_private_method_on_a_seam_type_need_not_be_declared() {
    let root = repository(
        &contract(""),
        &engine_with("fn hidden(&self) {} pub(crate) fn also_hidden(&self) {}"),
        RESERVATION,
    );
    assert!(undeclared(root.path()).expect("well formed").is_empty());
}

/// A renamed heading must fail loudly. Silence would declare nothing, which
/// reports every published method — or, if the seam list were also stale,
/// reports a clean seam that nobody checked.
#[test]
fn a_missing_contract_section_is_an_error_not_an_empty_contract() {
    let root = repository(
        "## Some other section\n\nNothing here.\n",
        &engine_with(""),
        RESERVATION,
    );
    let message = undeclared(root.path()).expect_err("a missing section must not pass");
    assert!(message.contains("no section starting"), "{message}");
}

/// A seam type that moved is a stale list, not a clean seam.
#[test]
fn a_seam_type_that_moved_is_a_stale_list() {
    let root = repository(&contract(""), "pub struct Renamed;", RESERVATION);
    let message = undeclared(root.path()).expect_err("a vanished type must not pass");
    assert!(message.contains("stale"), "{message}");
}

/// The section ends at the next heading: a method declared further down the
/// document is not declared *here*.
#[test]
fn a_declaration_after_the_section_ends_does_not_count() {
    let design = format!(
        "{}\n```rust\npub fn estimate_remaining();\n```\n",
        contract("")
    );
    let root = repository(
        &design,
        &engine_with("pub fn estimate_remaining(&self) {}"),
        RESERVATION,
    );
    let found = undeclared(root.path()).expect("well formed");
    assert_eq!(found.len(), 1, "{found:?}");
}

/// Whole-word, so a longer name containing this one is not a declaration.
#[test]
fn a_longer_name_containing_the_method_does_not_declare_it() {
    let root = repository(
        &contract("```rust\npub fn estimate_remaining_units();\n```"),
        &engine_with("pub fn estimate_remaining(&self) {}"),
        RESERVATION,
    );
    assert_eq!(undeclared(root.path()).expect("well formed").len(), 1);
}

/// The exit-code contract `scripts/check_seam_contract.sh` depends on.
///
/// CI reads the status, not the text, so "reports drift" and "fails the build"
/// are two claims and only one of them is about output. The parity checker's
/// `tests/binary.rs` exists for exactly this reason: its whole `run` could be
/// replaced with `Ok(true)` and every library test still passed (#85).
mod binary {
    use std::process::Command;

    use super::{RESERVATION, contract, engine_with, repository};

    fn status(root: &std::path::Path) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_check_seam_contract"))
            .arg("--")
            .arg(root)
            .output()
            .expect("the checker runs")
    }

    #[test]
    fn a_clean_seam_exits_zero_and_drift_exits_nonzero() {
        let clean = repository(&contract(""), &engine_with(""), RESERVATION);
        let out = status(clean.path());
        assert!(out.status.success(), "{out:?}");
        assert!(String::from_utf8_lossy(&out.stdout).contains("seam contract: OK"));

        let drifted = repository(
            &contract(""),
            &engine_with("pub fn estimate_remaining(&self) {}"),
            RESERVATION,
        );
        let out = status(drifted.path());
        assert!(!out.status.success(), "drift must fail the build");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("RequestContext::estimate_remaining"),
            "{stderr}"
        );
        assert!(stderr.contains("Amend"), "the failure must say what to do");
    }

    #[test]
    fn a_tool_error_exits_nonzero_rather_than_reporting_a_clean_seam() {
        let broken = repository("## Nothing here\n", &engine_with(""), RESERVATION);
        let out = status(broken.path());
        assert!(!out.status.success());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("no section starting"), "{stderr}");
        assert!(
            !String::from_utf8_lossy(&out.stdout).contains("OK"),
            "a tool error must never print the clean line"
        );
    }

    #[test]
    fn information_flags_precede_the_check() {
        for flag in ["--help", "--version"] {
            let out = Command::new(env!("CARGO_BIN_EXE_check_seam_contract"))
                .arg(flag)
                .output()
                .expect("the checker runs");
            assert!(out.status.success(), "{flag} must exit zero");
            assert!(!out.stdout.is_empty(), "{flag} must print something");
        }
    }
}
