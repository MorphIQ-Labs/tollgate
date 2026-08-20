//! The backend correctness suite (INVARIANTS.md #1, #4, #7, #9).
//!
//! Written against [`MemoryStore`] as the reference; the Postgres backend
//! must pass the same scenarios (its test file mirrors these by name).

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    PermissionBits, Principal, RequestId, ResolvedLimits, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AllocateError, GrantPolicy, LeaseAllocator, MemoryStore, SnapshotSource,
    UsageSink,
};

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

const TTL: SignedDuration = SignedDuration::from_secs(60);
const ACCOUNT: AccountId = AccountId(1);

/// Full-request grants (no adaptive shrink) for scenarios that need exact
/// lease sizes.
fn full_grant_policy() -> GrantPolicy {
    GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    }
}

fn store_with_balance(policy: GrantPolicy, balance: u64) -> Arc<MemoryStore> {
    let store = MemoryStore::new(policy);
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(balance),
        active: true,
    });
    store
}

fn usage(lease: &tollgate_core::LeaseGrant, request: u128, units: u64, at: i64) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request),
        account_id: lease.account_id,
        lease_id: lease.lease_id,
        fencing_token: lease.fencing_token,
        units: CostUnits(units),
        occurred_at: t(at),
    }
}

fn assert_conserved(store: &MemoryStore) {
    let conservation = store.conservation(ACCOUNT).unwrap();
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
}

#[tokio::test]
async fn adaptive_grant_shrinks_near_exhaustion() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);

    // Deep balance: capped at balance/2.
    let grant = store
        .acquire(ACCOUNT, CostUnits(600), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(grant.units, CostUnits(500));
    // Next holder is capped at the shrunken balance's half again.
    let grant = store
        .acquire(ACCOUNT, CostUnits(600), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(grant.units, CostUnits(250));
    // The tail is still grantable down to the last unit (min_grant floor).
    let mut drained = 0u64;
    loop {
        match store.acquire(ACCOUNT, CostUnits(600), TTL, t(0)).await {
            Ok(g) => drained += g.units.get(),
            Err(AllocateError::InsufficientBalance) => break,
            Err(other) => panic!("unexpected: {other}"),
        }
    }
    assert_eq!(drained, 250);
    assert_eq!(store.balance(ACCOUNT), CostUnits::ZERO);
    assert_conserved(&store);
}

/// INVARIANTS.md #1 at the allocator level: a concurrent acquire storm can
/// never hand out more units than the account holds — grants sum to exactly
/// the deposited balance once drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_double_spend_across_instances() {
    let store = store_with_balance(GrantPolicy::default(), 100_000);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            let mut granted = 0u64;
            loop {
                match store.acquire(ACCOUNT, CostUnits(1_000), TTL, t(0)).await {
                    Ok(g) => granted += g.units.get(),
                    Err(AllocateError::InsufficientBalance) => return granted,
                    Err(other) => panic!("unexpected: {other}"),
                }
            }
        }));
    }
    let mut total = 0u64;
    for task in tasks {
        total += task.await.unwrap();
    }
    assert_eq!(total, 100_000);
    assert_eq!(store.balance(ACCOUNT), CostUnits::ZERO);
    assert_conserved(&store);
}

/// INVARIANTS.md #4: stale holders are rejected everywhere — wrong token,
/// settled lease, and usage ingest alike.
#[tokio::test]
async fn fenced_out_holder_rejected() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let stale = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    // Wrong token on release: fenced.
    assert_eq!(
        store
            .release(stale.lease_id, FencingToken(999), CostUnits(400), t(1))
            .await
            .unwrap_err(),
        AllocateError::Fenced
    );

    // The holder partitions; its lease expires and is reclaimed; a
    // replacement lease carries a strictly newer token.
    let reclaimed = store.reclaim_expired(t(61)).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    let replacement = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(61))
        .await
        .unwrap();
    assert!(replacement.fencing_token > stale.fencing_token);

    // The stale holder reappears: release and usage are both refused.
    assert_eq!(
        store
            .release(stale.lease_id, stale.fencing_token, CostUnits(400), t(62))
            .await
            .unwrap_err(),
        AllocateError::LeaseNotActive
    );
    let report = store
        .ingest(&[usage(&stale, 1, 50, 62)], t(62))
        .await
        .unwrap();
    assert_eq!(report.rejected, 1);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
    assert_conserved(&store);
}

/// INVARIANTS.md #9: a crashed holder's unspent units return at TTL.
#[tokio::test]
async fn expired_lease_units_reclaimed() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT), CostUnits(500));

    // 120 units of usage arrive before the crash.
    let report = store
        .ingest(&[usage(&lease, 1, 70, 10), usage(&lease, 2, 50, 20)], t(20))
        .await
        .unwrap();
    assert_eq!(report.accepted, 2);

    // Not yet expired: sweep is a no-op.
    assert!(store.reclaim_expired(t(59)).await.unwrap().is_empty());

    // TTL lapses: the unspent 380 come back; the spent 120 stay billed.
    let reclaimed = store.reclaim_expired(t(60)).await.unwrap();
    assert_eq!(reclaimed[0].reclaimed, CostUnits(380));
    assert_eq!(store.balance(ACCOUNT), CostUnits(880));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(120));

    // Straggler usage for the settled lease is rejected, not double-counted.
    let report = store
        .ingest(&[usage(&lease, 3, 10, 61)], t(61))
        .await
        .unwrap();
    assert_eq!(report.rejected, 1);
    assert_conserved(&store);
}

/// INVARIANTS.md #7: replaying a batch never double-bills.
#[tokio::test]
async fn usage_replay_is_idempotent() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let batch = [usage(&lease, 1, 70, 10), usage(&lease, 2, 50, 10)];

    let first = store.ingest(&batch, t(10)).await.unwrap();
    assert_eq!((first.accepted, first.duplicate), (2, 0));
    let replay = store.ingest(&batch, t(11)).await.unwrap();
    assert_eq!((replay.accepted, replay.duplicate), (0, 2));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(120));
    assert_conserved(&store);
}

#[tokio::test]
async fn graceful_release_returns_unspent() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    store
        .ingest(&[usage(&lease, 1, 100, 5)], t(5))
        .await
        .unwrap();

    // Claiming more unspent than granted-minus-used is a surfaced bug.
    assert_eq!(
        store
            .release(lease.lease_id, lease.fencing_token, CostUnits(450), t(10))
            .await
            .unwrap_err(),
        AllocateError::InvalidRelease
    );

    // Honest release: flush happened, unspent = granted - used.
    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(400), t(10))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT), CostUnits(900));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(100));
    assert_conserved(&store);

    // Double release is refused.
    assert_eq!(
        store
            .release(lease.lease_id, lease.fencing_token, CostUnits(0), t(11))
            .await
            .unwrap_err(),
        AllocateError::LeaseNotActive
    );
}

/// Review finding #1, store half: reclaim waits out the grace window past
/// expiry, so work committed inside the holder's usability window (which
/// ends *before* expiry) has margin + grace to be flushed and billed; a
/// graceful release arriving during the grace window is honored, not
/// refused.
#[tokio::test]
async fn reclaim_waits_for_grace_and_release_works_within_it() {
    let store = store_with_balance(
        GrantPolicy {
            shrink_divisor: 1,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(300),
            reclaim_grace: SignedDuration::from_secs(30),
        },
        1_000,
    );
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap(); // expires t(60), reclaimable from t(90)

    // Inside the grace window: no reclaim yet, and late-arriving usage for
    // work committed before expiry still bills.
    assert!(store.reclaim_expired(t(60)).await.unwrap().is_empty());
    assert!(store.reclaim_expired(t(89)).await.unwrap().is_empty());
    let report = store
        .ingest(&[usage(&lease, 1, 120, 59)], t(65))
        .await
        .unwrap();
    assert_eq!(report.accepted, 1);

    // A slow graceful shutdown can still release within grace.
    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(380), t(70))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT), CostUnits(880));
    assert_conserved(&store);

    // A second lease left to lapse settles only once grace runs out.
    let lease2 = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(70))
        .await
        .unwrap(); // expires t(130)
    assert!(store.reclaim_expired(t(159)).await.unwrap().is_empty());
    let reclaimed = store.reclaim_expired(t(160)).await.unwrap();
    assert_eq!(reclaimed[0].lease_id, lease2.lease_id);
    assert_eq!(reclaimed[0].reclaimed, CostUnits(400));
    assert_conserved(&store);
}

/// Usage committed before a release but flushed after it converts the
/// release's provisional settlement loss into billed usage — the ordering
/// tolerance the client's quiescence-gated release relies on.
#[tokio::test]
async fn straggler_usage_after_release_is_billed() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();

    // The holder spent 30 units locally but only flushed after releasing
    // unspent = 470 (remaining on its counter). Provisional loss: 30.
    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(470), t(10))
        .await
        .unwrap();
    let c = store.conservation(ACCOUNT).unwrap();
    assert_eq!(c.settlement_loss, CostUnits(30));

    let report = store
        .ingest(&[usage(&lease, 1, 30, 5)], t(11))
        .await
        .unwrap();
    assert_eq!(report.accepted, 1);
    let c = store.conservation(ACCOUNT).unwrap();
    assert_eq!(c.settlement_loss, CostUnits::ZERO);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(30));
    assert!(c.holds());

    // But usage beyond the provisional gap still cannot fit — conservation
    // wins over a claim that would double-count.
    let report = store
        .ingest(&[usage(&lease, 2, 1, 12)], t(12))
        .await
        .unwrap();
    assert_eq!(report.rejected, 1);
}

#[tokio::test]
async fn inactive_account_refuses_leases() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    store.set_active(ACCOUNT, false);
    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
            .await
            .unwrap_err(),
        AllocateError::AccountInactive
    );
}

#[tokio::test]
async fn snapshot_publish_fetch_and_push() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(42);
    let snapshot = Arc::new(AccountSnapshot {
        account_id: ACCOUNT,
        key_id: None,
        generation: Generation(3),
        status: AccountStatus::Active,
        valid_until: t(10_000),
        permissions: PermissionBits::ALL,
        limits: ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: 1_000,
            rate_burst_units: 1_000,
        },
        cost_table: Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    });

    let mut updates = store.subscribe();
    assert!(store.snapshot(principal).await.unwrap().is_none());
    store.publish_snapshot(principal, Arc::clone(&snapshot));

    let fetched = store.snapshot(principal).await.unwrap().unwrap();
    assert_eq!(fetched.generation, Generation(3));
    let pushed = updates.recv().await.unwrap();
    assert_eq!(pushed.principal, principal);
    assert_eq!(pushed.snapshot.generation, Generation(3));
}
