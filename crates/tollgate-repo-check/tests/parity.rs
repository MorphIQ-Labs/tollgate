//! The parity check's own witnesses: it must fire on the divergence it names
//! and stay silent on the shapes that are not one.

use std::io::Write as _;
use tollgate_repo_check::parity::diverging;

/// Two suites written to a scratch directory, returned as their paths.
fn suites(
    memory: &str,
    postgres: &str,
) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("scratch directory");
    let mem = dir.path().join("store_suite.rs");
    let pg = dir.path().join("postgres_suite.rs");
    for (path, body) in [(&mem, memory), (&pg, postgres)] {
        let mut file = std::fs::File::create(path).expect("create suite");
        file.write_all(body.as_bytes()).expect("write suite");
    }
    (dir, mem, pg)
}

#[test]
fn an_inherent_call_against_a_trait_call_in_a_mirrored_test_is_reported() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { store.publish_snapshot(p, s).unwrap(); }",
        "async fn mirrored() { AdminStore::publish_snapshot(&*store, p, s).await.unwrap(); }",
    );
    let found = diverging(&mem, &pg).expect("parse");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].test, "mirrored");
    assert_eq!(found[0].operation, "publish_snapshot");
}

/// `try_create_account` is the fallible spelling of the same inherent helper,
/// so it maps onto `AdminStore::create_account` rather than a method of its
/// own name — which is how `creating_a_suspended_account_denies_from_birth`
/// evaded a check keyed on matching identifiers.
#[test]
fn the_fallible_spelling_of_an_inherent_helper_maps_to_its_trait_method() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { store.try_create_account(c).unwrap(); }",
        "async fn mirrored() { AdminStore::create_account(&*store, c).await.unwrap(); }",
    );
    let found = diverging(&mem, &pg).expect("parse");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].operation, "create_account");
}

#[test]
fn both_sides_driving_the_trait_is_silent() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
    );
    assert!(diverging(&mem, &pg).expect("parse").is_empty());
}

/// A memory test may use the helper *and* the trait — the trait call is what
/// the check cares about, so the helper alongside it is not a divergence.
#[test]
fn an_inherent_call_beside_a_trait_call_is_not_a_divergence() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { store.deposit(a, u).unwrap(); AdminStore::deposit(&*store, a, u).await.unwrap(); }",
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
    );
    assert!(diverging(&mem, &pg).expect("parse").is_empty());
}

/// Only mirrored tests are compared. A memory-only scenario has no PostgreSQL
/// counterpart to disagree with, and reporting it would make the check noise.
#[test]
fn a_test_with_no_counterpart_is_not_compared() {
    let (_dir, mem, pg) = suites(
        "async fn memory_only() { store.publish_snapshot(p, s).unwrap(); }",
        "async fn something_else() { AdminStore::publish_snapshot(&*store, p, s).await.unwrap(); }",
    );
    assert!(diverging(&mem, &pg).expect("parse").is_empty());
}

/// The reverse direction is not a divergence this check reports: `PostgresStore`
/// has no inherent helpers, so an inherent call can only appear on the memory
/// side. Stated as a test so the asymmetry is deliberate rather than an
/// oversight.
#[test]
fn the_postgres_side_has_no_inherent_helpers_to_report() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { AdminStore::deposit(&*store, a, u).await.unwrap(); }",
        "async fn mirrored() { store.deposit(a, u).unwrap(); }",
    );
    assert!(diverging(&mem, &pg).expect("parse").is_empty());
}

#[test]
fn a_call_nested_inside_a_closure_or_block_is_still_seen() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { for id in ids { if x { store.create_account(c); } } }",
        "async fn mirrored() { AdminStore::create_account(&*store, c).await.unwrap(); }",
    );
    assert_eq!(diverging(&mem, &pg).expect("parse").len(), 1);
}

#[test]
fn an_unparsable_suite_is_an_error_rather_than_a_silent_pass() {
    let (_dir, mem, pg) = suites("async fn broken( {", "async fn mirrored() {}");
    assert!(
        diverging(&mem, &pg).is_err(),
        "a parse failure must not read as parity"
    );
}

/// The PostgreSQL side has no inherent helper to shadow, so method syntax
/// there *is* the trait call. Keyed on `AdminStore::` alone this reported
/// nothing — a memory test using the helper whose mirror happens to use method
/// syntax would have passed silently. Six PostgreSQL tests use that spelling.
#[test]
fn a_postgres_mirror_written_in_method_syntax_still_counts_as_the_trait() {
    let (_dir, mem, pg) = suites(
        "async fn mirrored() { store.publish_snapshot(p, s).unwrap(); }",
        "async fn mirrored() { store.publish_snapshot(p, s).await.unwrap(); }",
    );
    let found = diverging(&mem, &pg).expect("parse");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].operation, "publish_snapshot");
}
