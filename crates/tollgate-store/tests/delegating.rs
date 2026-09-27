//! The shared store double's own contract (GL-83).
//!
//! `tests/support/delegating.rs` is included by test binaries in three crates,
//! and its whole job is to make the forward-or-inherit decision for a defaulted
//! trait method once instead of once per double. This file is what pins those
//! decisions, and what keeps the module compiled — and therefore linted — in
//! the crate that owns it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    PermissionBits, Principal, PublishableSnapshot, ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, MemoryStore, SnapshotSource, StoreError,
};

#[path = "support/delegating.rs"]
mod delegating;
use delegating::{DelegatingStore, rejecting};

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

const ACCOUNT: AccountId = AccountId(1);

fn store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
}

fn snapshot() -> PublishableSnapshot {
    PublishableSnapshot::try_new(Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            t(10_000),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .build(),
    ))
    .expect("test snapshot limits are valid")
}

/// The defect GL-83 names, stated as an assertion.
///
/// `SnapshotSource::principals` carries a default body returning `Ok(None)`,
/// the sentinel for "this source cannot enumerate". Every real store overrides
/// it. A double that delegates method by method and simply *omits* this one
/// inherits the sentinel and reports no catalogue while the store it wraps has
/// one — which is what `FlakyReclaimStore` did before this module existed, and
/// what makes a server built over it answer 501 on `/principals`.
#[tokio::test]
async fn an_unhooked_wrapper_reports_the_catalogue_its_store_has() {
    let inner = store();
    inner
        .publish_snapshot(Principal(7), snapshot())
        .expect("snapshot fixture matches its account");
    let double = DelegatingStore::wrapping(Arc::clone(&inner));

    assert_eq!(
        double.principals().await.unwrap(),
        Some(vec![Principal(7)]),
        "delegation must not fall back to the `Ok(None)` sentinel"
    );
    assert_eq!(
        double.principals().await.unwrap(),
        inner.principals().await.unwrap()
    );
}

/// The sentinel remains reachable — by name, never by omission.
#[tokio::test]
async fn a_source_that_cannot_enumerate_says_so_explicitly() {
    let double =
        DelegatingStore::wrapping(store()).on_principals(|_| async { Ok::<_, StoreError>(None) });

    assert_eq!(double.principals().await.unwrap(), None);
}

/// The opposite decision, and the reason it cannot be the same one.
///
/// `LeaseAllocator::reclaim_expired`'s default body is written over
/// `self.reclaim_expired_batch`. Running it against the wrapper re-enters the
/// wrapper's own hook; forwarding it to the inner store would rebind that
/// `self` and drop the hook on the floor. `tollgate-server`'s `/reclaim` route
/// calls this method, so the bypass would be live rather than theoretical.
#[tokio::test]
async fn the_full_drain_re_enters_the_batch_hook_rather_than_the_inner_store() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed = Arc::clone(&calls);
    let double =
        DelegatingStore::wrapping(store()).on_reclaim_expired_batch(move |_inner, _now, _limit| {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::AcqRel);
                Err(StoreError("injected batch failure".into()))
            }
        });

    let error = double.reclaim_expired(t(0)).await.unwrap_err();

    assert_eq!(error, StoreError("injected batch failure".into()));
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "the drain must reach the hook; forwarding to the inner store would not"
    );
}

/// Batches still compose: a saturated batch drives another round trip.
#[tokio::test]
async fn the_full_drain_keeps_calling_the_hook_until_a_batch_is_unsaturated() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed = Arc::clone(&calls);
    let double =
        DelegatingStore::wrapping(store()).on_reclaim_expired_batch(move |inner, now, limit| {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::AcqRel);
                LeaseAllocator::reclaim_expired_batch(&*inner, now, limit).await
            }
        });

    assert!(double.reclaim_expired(t(0)).await.unwrap().is_empty());
    assert_eq!(calls.load(Ordering::Acquire), 1);
}

/// Un-hooked methods on a rejecting double panic and name themselves, so a
/// path that grows a store call fails loudly instead of quietly succeeding.
#[tokio::test]
#[should_panic(expected = "readiness must not touch the store: AdminStore::remove_snapshot")]
async fn a_rejecting_double_names_the_method_it_was_not_given() {
    let double = rejecting("readiness must not touch the store");
    // The panic happens before this assertion is reached; it exists so the
    // `#[must_use]` result is consumed rather than discarded (GL-82).
    assert!(double.remove_snapshot(Principal(1)).await.is_ok());
}

/// Including the defaulted ones: on a rejecting double, forgetting to state a
/// default's behaviour is a panic rather than a silent answer.
#[tokio::test]
#[should_panic(expected = "readiness must not touch the store: SnapshotSource::principals")]
async fn a_rejecting_double_rejects_the_defaulted_methods_too() {
    assert!(
        rejecting("readiness must not touch the store")
            .principals()
            .await
            .is_ok()
    );
}

/// A hook is reached, and it can still consult the store it replaced.
#[tokio::test]
async fn a_hook_may_observe_the_inner_store_before_answering() {
    let double = DelegatingStore::wrapping(store())
        .on_ping(|_inner| async { Err(StoreError("store unreachable".into())) });

    assert!(tollgate_store::StoreHealth::ping(&double).await.is_err());
}
