//! The backend correctness suite (INVARIANTS.md #1, #4, #7, #9).
//!
//! Written against [`MemoryStore`] as the reference; the Postgres backend
//! must pass the same scenarios (its test file mirrors these by name).

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    KeyId, LeaseId, PermissionBits, Principal, PublishableSnapshot, RequestId, ResolvedLimits,
    UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, Conservation, CreateAccountError, GrantPolicy,
    KeyDirectory, KeyError, KeyRecord, LeaseAllocator, MemoryStore, PublishSnapshotError,
    ReclaimBatch, ReclaimedLease, Revocation, SetStatusError, SnapshotResolution, SnapshotSource,
    UsageSink,
};

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

const TTL: SignedDuration = SignedDuration::from_secs(60);
const ACCOUNT: AccountId = AccountId(1);

fn publishable(snapshot: Arc<AccountSnapshot>) -> PublishableSnapshot {
    PublishableSnapshot::try_new(snapshot).expect("test snapshot limits are valid")
}

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

#[test]
fn invalid_grant_policy_is_rejected() {
    let policy = GrantPolicy {
        reclaim_grace: SignedDuration::from_secs(-1),
        ..GrantPolicy::default()
    };
    assert!(MemoryStore::new(policy).is_err());

    let policy = GrantPolicy {
        max_ttl: SignedDuration::ZERO,
        ..GrantPolicy::default()
    };
    assert!(MemoryStore::new(policy).is_err());

    let invalid = GrantPolicy {
        shrink_divisor: 0,
        ..GrantPolicy::default()
    };
    assert_eq!(invalid.grant(CostUnits(10), CostUnits(100)), None);
    assert_eq!(
        GrantPolicy::default().grant(CostUnits::ZERO, CostUnits(100)),
        None
    );
}

fn store_with_balance(policy: GrantPolicy, balance: u64) -> Arc<MemoryStore> {
    let store = MemoryStore::new(policy).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(balance),
        status: AccountStatus::Active,
    });
    store
}

#[tokio::test]
async fn nonpositive_lease_ttl_is_rejected_without_debiting() {
    let store = store_with_balance(full_grant_policy(), 100);
    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(10), SignedDuration::ZERO, t(0))
            .await
            .unwrap_err(),
        AllocateError::InvalidTtl
    );
    assert_eq!(store.balance(ACCOUNT), CostUnits(100));

    assert!(
        store
            .acquire(ACCOUNT, CostUnits(10), TTL, Timestamp::MAX)
            .await
            .is_err()
    );
    assert_eq!(store.balance(ACCOUNT), CostUnits(100));
}

fn usage(lease: &tollgate_core::LeaseGrant, request: u128, units: u64, at: i64) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request),
        account_id: lease.account_id,
        source: UsageSource::Leased {
            lease_id: lease.lease_id,
            fencing_token: lease.fencing_token,
        },
        units: CostUnits(units),
        occurred_at: t(at),
    }
}

/// A charge with no lease behind it, as elastic admission produces.
fn overage_usage(account: AccountId, request: u128, units: u64, at: i64) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request),
        account_id: account,
        source: UsageSource::Overage,
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

/// INVARIANTS.md #4: allocation order is not an account-wide validity epoch.
/// Both simultaneously active leases retain their own capabilities.
#[tokio::test]
async fn newer_lease_does_not_invalidate_older_active_capability() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let older = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    let newer = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    assert!(newer.fencing_token > older.fencing_token);

    let report = store
        .ingest(&[usage(&older, 1, 50, 1)], t(1))
        .await
        .unwrap();
    assert_eq!((report.accepted, report.rejected), (1, 0));
    store
        .release(older.lease_id, older.fencing_token, CostUnits(350), t(2))
        .await
        .unwrap();

    let report = store
        .ingest(&[usage(&newer, 2, 25, 2)], t(2))
        .await
        .unwrap();
    assert_eq!((report.accepted, report.rejected), (1, 0));
    store
        .release(newer.lease_id, newer.fencing_token, CostUnits(375), t(3))
        .await
        .unwrap();

    assert_eq!(store.balance(ACCOUNT), CostUnits(925));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(75));
    assert_conserved(&store);
}

/// A wrong token refuses release without changing the lease. The immediate
/// sweep also witnesses that a failed store operation has completed before it
/// returns; the PostgreSQL mirror protects the awaited-rollback regression.
#[tokio::test]
async fn wrong_token_release_leaves_lease_reclaimable() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    assert_eq!(
        store
            .release(lease.lease_id, FencingToken(999), CostUnits(400), t(1))
            .await
            .unwrap_err(),
        AllocateError::Fenced
    );

    let reclaimed = store.reclaim_expired(t(61)).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].lease_id, lease.lease_id);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
    assert_conserved(&store);
}

/// Usage identifies a lease with the full `(lease, account, token)`
/// capability. Token and account mismatches are distinct rejection witnesses.
#[tokio::test]
async fn usage_rejects_mismatched_lease_capability() {
    const OTHER: AccountId = AccountId(2);
    let store = store_with_balance(full_grant_policy(), 1_000);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: OTHER,
            initial_balance: CostUnits(100),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();
    let lease = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    let mut wrong_token = usage(&lease, 1, 10, 1);
    wrong_token.source = UsageSource::Leased {
        lease_id: lease.lease_id,
        fencing_token: FencingToken(lease.fencing_token.0.checked_add(1).unwrap()),
    };
    let report = store.ingest(&[wrong_token], t(1)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 0, 1)
    );

    let mut wrong_account = usage(&lease, 2, 10, 1);
    wrong_account.account_id = OTHER;
    let report = store.ingest(&[wrong_account], t(1)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 0, 1)
    );

    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
    assert_eq!(store.usage_recorded(OTHER), CostUnits::ZERO);
    assert_conserved(&store);
    assert!(store.conservation(OTHER).unwrap().holds());
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

/// INVARIANTS.md #9: an outage-sized backlog is split into bounded atomic
/// settlements without capping how much legitimate quota eventually returns.
#[tokio::test]
async fn expired_backlog_is_reclaimed_in_bounded_batches() {
    const OTHER: AccountId = AccountId(2);
    let store = store_with_balance(full_grant_policy(), 200);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: OTHER,
            initial_balance: CostUnits(400),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();
    let leases = [
        store
            .acquire(ACCOUNT, CostUnits(200), TTL, t(0))
            .await
            .unwrap(),
        store
            .acquire(OTHER, CostUnits(200), TTL, t(0))
            .await
            .unwrap(),
        store
            .acquire(OTHER, CostUnits(200), TTL, t(0))
            .await
            .unwrap(),
    ];
    store
        .ingest(
            &[usage(&leases[0], 10, 25, 10), usage(&leases[2], 11, 50, 10)],
            t(10),
        )
        .await
        .unwrap();

    let limit = NonZeroUsize::new(2).unwrap();
    let first = store.reclaim_expired_batch(t(60), limit).await.unwrap();
    assert_eq!(first.len(), 2);
    assert!(first.is_saturated());

    let second = store.reclaim_expired_batch(t(60), limit).await.unwrap();
    assert_eq!(second.len(), 1);
    assert!(!second.is_saturated());

    let mut expected_ids: Vec<_> = leases.iter().map(|lease| lease.lease_id).collect();
    expected_ids.sort_by_key(|lease_id| lease_id.0);
    let mut reclaimed_ids: Vec<_> = first
        .reclaimed()
        .iter()
        .chain(second.reclaimed())
        .map(|lease| lease.lease_id)
        .collect();
    reclaimed_ids.sort_by_key(|lease_id| lease_id.0);
    assert_eq!(reclaimed_ids, expected_ids);
    assert_eq!(store.balance(ACCOUNT), CostUnits(175));
    assert_eq!(store.balance(OTHER), CostUnits(350));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
    assert_eq!(store.usage_recorded(OTHER), CostUnits(50));
    assert_conserved(&store);
    let other_conservation = store.conservation(OTHER).unwrap();
    assert!(
        other_conservation.holds(),
        "conservation violated: {other_conservation:?}"
    );
}

#[test]
fn reclaim_batch_rejects_backend_results_over_the_limit() {
    let reclaimed = (0..3)
        .map(|id| ReclaimedLease {
            lease_id: tollgate_core::LeaseId(id),
            account_id: ACCOUNT,
            reclaimed: CostUnits(1),
        })
        .collect();
    assert!(ReclaimBatch::try_new(reclaimed, NonZeroUsize::new(2).unwrap()).is_err());
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

    // A duplicate *within* one batch is also caught, once.
    let with_dup = [usage(&lease, 3, 10, 12), usage(&lease, 3, 10, 12)];
    let report = store.ingest(&with_dup, t(12)).await.unwrap();
    assert_eq!((report.accepted, report.duplicate), (1, 1));
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(130));
    assert_conserved(&store);
}

/// INVARIANTS.md #7: a successful mixed batch classifies every event once
/// while applying only accepted deltas across active and settled leases.
#[tokio::test]
async fn mixed_usage_batch_preserves_partial_acceptance() {
    const OTHER: AccountId = AccountId(2);
    let store = store_with_balance(full_grant_policy(), 1_000);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: OTHER,
            initial_balance: CostUnits(400),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();

    let active = store
        .acquire(ACCOUNT, CostUnits(300), TTL, t(0))
        .await
        .unwrap();
    let settled = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(0))
        .await
        .unwrap();
    let other = store
        .acquire(OTHER, CostUnits(300), TTL, t(0))
        .await
        .unwrap();
    store
        .release(
            settled.lease_id,
            settled.fencing_token,
            CostUnits(170),
            t(5),
        )
        .await
        .unwrap();

    let prior = usage(&active, 1, 10, 2);
    assert_eq!(store.ingest(&[prior], t(2)).await.unwrap().accepted, 1);
    let mut replay_with_unrepresentable_units = prior;
    replay_with_unrepresentable_units.units = CostUnits(u64::MAX);
    let repeated = usage(&other, 4, 40, 6);
    let mut wrong_fence = usage(&other, 6, 10, 6);
    wrong_fence.source = UsageSource::Leased {
        lease_id: other.lease_id,
        fencing_token: FencingToken(other.fencing_token.0.checked_add(1).unwrap()),
    };
    let mut unknown_lease = usage(&other, 7, 10, 6);
    unknown_lease.source = UsageSource::Leased {
        lease_id: LeaseId(u128::MAX),
        fencing_token: other.fencing_token,
    };
    let batch = [
        usage(&active, 2, 20, 6),
        replay_with_unrepresentable_units,
        wrong_fence,
        unknown_lease,
        usage(&settled, 3, 30, 4),
        repeated,
        repeated,
        usage(&active, 5, 15, 6),
        // Two overage events in the same batch as every other class, because
        // INVARIANTS.md #7 is about a *mixed* batch classifying each input
        // exactly once: one on a real account, one naming an account the
        // ledger has never heard of.
        overage_usage(ACCOUNT, 8, 25, 6),
        overage_usage(AccountId(u128::MAX), 9, 25, 6),
    ];

    let report = store.ingest(&batch, t(6)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (5, 2, 3)
    );
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(100));
    assert_eq!(store.usage_recorded(OTHER), CostUnits(40));
    assert_eq!(
        store.conservation(ACCOUNT).unwrap().settlement_loss,
        CostUnits::ZERO
    );
    assert_eq!(
        store.conservation(ACCOUNT).unwrap().overage_recorded,
        CostUnits(25),
        "the overage event funds the units it billed"
    );
    assert_conserved(&store);
    let other_conservation = store.conservation(OTHER).unwrap();
    assert!(
        other_conservation.holds(),
        "conservation violated: {other_conservation:?}"
    );
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

/// Review finding #7: recreating an account is refused and touches nothing —
/// balance, ledger totals, and the fencing sequence survive intact.
#[tokio::test]
async fn recreate_account_is_refused_and_nondestructive() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    assert_eq!(
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: ACCOUNT,
                initial_balance: CostUnits(5),
                status: AccountStatus::Active,
            },
        )
        .await
        .unwrap_err(),
        CreateAccountError::AlreadyExists
    );

    assert_eq!(store.balance(ACCOUNT), CostUnits(600));
    let replacement = store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(1))
        .await
        .unwrap();
    assert!(
        replacement.fencing_token > lease.fencing_token,
        "fencing sequence must survive a refused recreate"
    );
    assert_conserved(&store);
}

#[tokio::test]
async fn a_non_active_account_refuses_leases() {
    // Both non-Active statuses refuse, under one deny reason: no client acts
    // on the distinction (#51).
    for status in [AccountStatus::Suspended, AccountStatus::Closed] {
        let store = store_with_balance(GrantPolicy::default(), 1_000);
        // Through `AdminStore`, matching the PostgreSQL mirror. The two had
        // drifted: this side called the inherent method, so the trait
        // implementation could be replaced by `Ok(())` — suspending an account
        // and still serving it — with the whole suite green (#43).
        AdminStore::set_account_status(&*store, ACCOUNT, status)
            .await
            .unwrap();
        assert_eq!(
            store
                .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
                .await
                .unwrap_err(),
            AllocateError::AccountInactive,
            "{status:?} must refuse leases"
        );
    }
}

#[tokio::test]
async fn unknown_account_status_update_is_refused() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let unknown = AccountId(999);

    for status in [
        AccountStatus::Active,
        AccountStatus::Suspended,
        AccountStatus::Closed,
    ] {
        assert_eq!(
            AdminStore::set_account_status(&*store, unknown, status)
                .await
                .unwrap_err(),
            SetStatusError::UnknownAccount,
            "an unknown account cannot be set to {status:?}"
        );
    }

    store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
        .await
        .expect("refusing the unknown account leaves existing accounts active");
}

#[tokio::test]
async fn creating_a_suspended_account_denies_from_birth() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store
        .try_create_account(AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Suspended,
        })
        .expect("creation succeeds");

    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
            .await
            .unwrap_err(),
        AllocateError::AccountInactive,
        "a suspended account refuses from creation, not only after a transition"
    );
}

/// #48's enumeration seam, and the detail the whole removal-vs-revocation
/// distinction rests on: a revoked principal stays in the catalogue. Its
/// tombstone *is* the record of the revocation, so an instance must keep
/// tracking it — dropping it would make it indistinguishable from a principal
/// that never existed, which is the resurrection INVARIANTS.md #15 forbids.
#[tokio::test]
async fn enumerating_principals_includes_revoked_ones() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    assert_eq!(
        store.principals().await.unwrap(),
        Some(Vec::new()),
        "an empty catalogue is Some(empty), never None"
    );

    let snapshot = || {
        publishable(Arc::new(
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
    };
    let live = Principal(1);
    let revoked = Principal(2);
    store.publish_snapshot(live, snapshot());
    store.publish_snapshot(revoked, snapshot());
    store.remove_snapshot(revoked);

    let mut listed = store.principals().await.unwrap().expect("enumerable");
    listed.sort_by_key(|principal| principal.0);
    assert_eq!(listed, vec![live, revoked]);
}

/// `max_ttl` is the allocator's hard cap on how long any one lease may hold
/// units, which is what bounds the time a crashed holder can strand them
/// (INVARIANTS.md #9). Nothing asserted the clamp: a request for an hour
/// against a five-minute policy could have been honoured in full.
#[tokio::test]
async fn a_ttl_beyond_the_policy_maximum_is_clamped() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let lease = store
        .acquire(
            ACCOUNT,
            CostUnits(100),
            SignedDuration::from_secs(3_600),
            t(0),
        )
        .await
        .unwrap();
    assert_eq!(
        lease.expires_at,
        t(300),
        "the policy caps the lease at max_ttl, not at what the caller asked for"
    );

    // At or under the cap the caller's own TTL stands.
    let exact = store
        .acquire(
            ACCOUNT,
            CostUnits(100),
            SignedDuration::from_secs(300),
            t(0),
        )
        .await
        .unwrap();
    assert_eq!(exact.expires_at, t(300));
    let shorter = store
        .acquire(ACCOUNT, CostUnits(100), SignedDuration::from_secs(30), t(0))
        .await
        .unwrap();
    assert_eq!(shorter.expires_at, t(30));
}

/// Funding an account is the only way quota enters the system, and neither
/// suite exercised it: `deposit` could return `Ok(())` without moving a single
/// unit and nothing failed (#43). A silently ignored top-up means an account
/// that was paid for keeps being denied.
#[tokio::test]
async fn depositing_funds_the_account_and_the_ledger_agrees() {
    // Full grants, so the acquire below reflects the deposited balance
    // rather than the default policy's halving.
    let store = store_with_balance(full_grant_policy(), 100);
    assert_eq!(store.balance(ACCOUNT), CostUnits(100));

    AdminStore::deposit(&*store, ACCOUNT, CostUnits(400))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT), CostUnits(500));

    // Deposits move `deposited` too, or the conservation equation would read
    // the top-up as units appearing from nowhere.
    let conservation = store.conservation(ACCOUNT).unwrap();
    assert_eq!(conservation.deposited, CostUnits(500));
    assert!(conservation.holds(), "conservation: {conservation:?}");

    // And the new balance is spendable, which is the point of depositing.
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(lease.units, CostUnits(500));
    assert_conserved(&store);

    assert_eq!(
        AdminStore::deposit(&*store, AccountId(999), CostUnits(1))
            .await
            .unwrap_err(),
        AllocateError::UnknownAccount,
        "an unknown account is refused, not silently created"
    );
}

#[tokio::test]
async fn snapshot_publish_fetch_and_push() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(42);
    let snapshot = Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(3),
            AccountStatus::Active,
            t(10_000),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .build(),
    );

    let mut updates = store.subscribe();
    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Unknown
    ));
    store.publish_snapshot(principal, publishable(Arc::clone(&snapshot)));

    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.generation, Generation(3));
    let pushed = updates.recv().await.unwrap();
    assert_eq!(pushed.principal, principal);
    let SnapshotResolution::Present(pushed) = pushed.resolution else {
        panic!("publish push must be present");
    };
    assert_eq!(pushed.generation, Generation(3));

    // Generation monotonicity (backend parity with Postgres — review
    // finding #5): a replayed older publish neither replaces the row nor
    // pushes an update.
    let mut older = (*snapshot).clone();
    older.generation = Generation(2);
    store.publish_snapshot(principal, publishable(Arc::new(older)));
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("newest snapshot must remain present");
    };
    assert_eq!(fetched.generation, Generation(3));
    assert!(
        updates.try_recv().is_err(),
        "a discarded rollback must not be pushed"
    );

    // Revocation retains generation 3 as a tombstone. A delayed generation-2
    // publication cannot recreate the principal; generation 4 can.
    store.remove_snapshot(principal);
    let removed = updates.recv().await.unwrap();
    assert!(matches!(
        removed.resolution,
        SnapshotResolution::Revoked {
            generation: Generation(3)
        }
    ));
    let mut replayed = (*snapshot).clone();
    replayed.generation = Generation(2);
    store.publish_snapshot(principal, publishable(Arc::new(replayed)));
    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Revoked {
            generation: Generation(3)
        }
    ));
    assert!(updates.try_recv().is_err());

    let mut newer = (*snapshot).clone();
    newer.generation = Generation(4);
    store.publish_snapshot(principal, publishable(Arc::new(newer)));
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("newer snapshot must supersede revocation");
    };
    assert_eq!(fetched.generation, Generation(4));
}

#[tokio::test]
async fn staged_limits_round_trip_through_the_memory_store() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(43);
    let limits = ResolvedLimits::new(64)
        .with_weighted_rate_compatibility_fallback(1_000, 2_000)
        .with_request_rate(NonZeroU32::new(10).unwrap(), NonZeroU32::new(20).unwrap())
        .with_concurrency(
            NonZeroU32::new(4).unwrap(),
            Some(NonZeroU32::new(2).unwrap()),
        )
        .unwrap();
    let snapshot = AccountSnapshot::builder(
        ACCOUNT,
        Generation(1),
        AccountStatus::Active,
        t(10_000),
        PermissionBits::ALL,
        limits,
        Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .build();
    store.publish_snapshot(principal, publishable(Arc::new(snapshot)));

    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.limits, limits);
}

#[test]
#[cfg(feature = "wire")]
fn legacy_limit_wire_defaults_new_dimensions_without_changing_weighted_rate() {
    let limits: ResolvedLimits = serde_json::from_value(serde_json::json!({
        "max_items_per_request": 64,
        "rate_units_per_second": 1_000,
        "rate_burst_units": 2_000
    }))
    .unwrap();
    assert!(limits.weighted_rate().is_some());
    assert_eq!(limits.request_rate(), None);
    assert_eq!(limits.max_concurrent_requests(), None);
    assert_eq!(limits.principal_max_concurrent_requests(), None);
}

// ---- unified account suspension (#51) -------------------------------------
//
// The reference implementation of INVARIANTS.md #22. Every scenario drives
// `AdminStore` rather than an inherent helper: this pair of suites has already
// caught one divergence where the memory trait body could have been `Ok(())`
// with everything green (#43), and a status change that quietly did nothing is
// exactly the failure being fixed.

/// A snapshot for `account`, so a test can give one account several
/// principals and a second account one of its own.
fn account_snapshot(account: AccountId, generation: u64, status: AccountStatus) -> AccountSnapshot {
    AccountSnapshot::builder(
        account,
        Generation(generation),
        status,
        t(10_000),
        PermissionBits::ALL,
        ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
        Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .build()
}

async fn status_of(store: &MemoryStore, principal: Principal) -> (AccountStatus, Generation) {
    let SnapshotResolution::Present(snapshot) = store.snapshot(principal).await.unwrap() else {
        panic!("principal {principal} must be present");
    };
    (snapshot.status, snapshot.generation)
}

/// **The one that matters.** One operator action moves both records: the
/// ledger stops granting leases *and* every live snapshot of the account
/// carries the new status, at a higher generation.
///
/// Before #51 the first happened and the second did not, so an operator who
/// deactivated an account watched it keep serving.
#[tokio::test]
async fn suspending_an_account_stops_leases_and_republishes_its_snapshots() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let first = Principal(10);
    let second = Principal(11);
    for principal in [first, second] {
        AdminStore::publish_snapshot(
            &*store,
            principal,
            publishable(Arc::new(account_snapshot(
                ACCOUNT,
                3,
                AccountStatus::Active,
            ))),
        )
        .await
        .unwrap();
    }

    let change = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();
    assert_eq!(
        (change.republished, change.unreadable),
        (2, 0),
        "the reported blast radius is both live principals"
    );

    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
            .await
            .unwrap_err(),
        AllocateError::AccountInactive,
        "the ledger half"
    );
    for principal in [first, second] {
        assert_eq!(
            status_of(&store, principal).await,
            (AccountStatus::Suspended, Generation(4)),
            "the snapshot half, for {principal}"
        );
    }
}

/// The account predicate earns its place: a status change is scoped to one
/// account and must not touch a bystander's snapshots or bump their
/// generations.
#[tokio::test]
async fn suspension_republishes_only_the_suspended_accounts_snapshots() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let other_account = AccountId(2);
    store.create_account(AccountConfig {
        account_id: other_account,
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
    });
    let mine = Principal(10);
    let theirs = Principal(20);
    AdminStore::publish_snapshot(
        &*store,
        mine,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            3,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();
    AdminStore::publish_snapshot(
        &*store,
        theirs,
        publishable(Arc::new(account_snapshot(
            other_account,
            7,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();

    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();

    assert_eq!(
        status_of(&store, mine).await,
        (AccountStatus::Suspended, Generation(4))
    );
    assert_eq!(
        status_of(&store, theirs).await,
        (AccountStatus::Active, Generation(7)),
        "another account's snapshot is untouched, generation included"
    );
    store
        .acquire(other_account, CostUnits(100), TTL, t(0))
        .await
        .expect("and it can still lease");
}

/// Revocation stays a separate mechanism. Republishing a tombstone would
/// resurrect a revoked credential, which INVARIANTS.md #15 forbids, so a
/// status change must leave it revoked *at its original generation* — a bump
/// would shadow a later legitimate republish.
#[tokio::test]
async fn suspending_an_account_does_not_resurrect_revoked_principals() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let live = Principal(10);
    let revoked = Principal(11);
    for principal in [live, revoked] {
        AdminStore::publish_snapshot(
            &*store,
            principal,
            publishable(Arc::new(account_snapshot(
                ACCOUNT,
                3,
                AccountStatus::Active,
            ))),
        )
        .await
        .unwrap();
    }
    AdminStore::remove_snapshot(&*store, revoked).await.unwrap();

    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();

    assert_eq!(
        status_of(&store, live).await,
        (AccountStatus::Suspended, Generation(4))
    );
    assert!(
        matches!(
            store.snapshot(revoked).await.unwrap(),
            SnapshotResolution::Revoked {
                generation: Generation(3)
            }
        ),
        "a tombstone stays revoked, at its own generation"
    );
}

/// The return path, and the reason the transition is not one-way for
/// `Suspended`: reactivation restores admission and bumps again.
#[tokio::test]
async fn reactivating_an_account_restores_admission_and_bumps_generations() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(10);
    AdminStore::publish_snapshot(
        &*store,
        principal,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            3,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();

    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();
    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Active)
        .await
        .unwrap();

    assert_eq!(
        status_of(&store, principal).await,
        (AccountStatus::Active, Generation(5)),
        "each transition is its own generation; they never move backward"
    );
    store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
        .await
        .expect("reactivation restores leasing");
}

/// `Closed` is terminal, and the refusal changes *nothing* — not the ledger,
/// not one snapshot, not one generation. A refusal that half-applied would be
/// the divergence this whole mechanism exists to abolish.
#[tokio::test]
async fn a_closed_account_cannot_be_reactivated() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(10);
    AdminStore::publish_snapshot(
        &*store,
        principal,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            3,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();
    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Closed)
        .await
        .unwrap();
    let after_close = status_of(&store, principal).await;

    for status in [AccountStatus::Active, AccountStatus::Suspended] {
        assert_eq!(
            AdminStore::set_account_status(&*store, ACCOUNT, status)
                .await
                .unwrap_err(),
            SetStatusError::AccountClosed,
            "a closed account cannot become {status:?}"
        );
        assert_eq!(
            status_of(&store, principal).await,
            after_close,
            "and the refusal moved nothing"
        );
        assert_eq!(
            store
                .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
                .await
                .unwrap_err(),
            AllocateError::AccountInactive,
            "including the ledger"
        );
    }

    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Closed)
        .await
        .expect("Closed -> Closed is a no-op, not a refusal");
}

/// Convergent, not merely idempotent: a repeat rewrites nothing, so it bumps
/// no generation and emits no push. Generation churn on every retry would
/// invalidate every instance's cache for no change.
#[tokio::test]
async fn repeating_a_status_change_publishes_nothing_new() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(10);
    AdminStore::publish_snapshot(
        &*store,
        principal,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            3,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();
    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();

    let mut updates = store.subscribe();
    let change = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();
    assert_eq!(
        change.republished, 0,
        "a repeat reports zero, which is how an operator sees it changed nothing"
    );

    assert_eq!(
        status_of(&store, principal).await,
        (AccountStatus::Suspended, Generation(4)),
        "already at the target status, so not rewritten"
    );
    assert!(
        updates.try_recv().is_err(),
        "and nothing was pushed for a change that did not happen"
    );
}

/// Every republished principal is pushed, so an in-process instance learns in
/// milliseconds rather than at the refresh interval.
#[tokio::test]
async fn a_status_change_pushes_every_republished_principal() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let first = Principal(10);
    let second = Principal(11);
    let revoked = Principal(12);
    for principal in [first, second, revoked] {
        AdminStore::publish_snapshot(
            &*store,
            principal,
            publishable(Arc::new(account_snapshot(
                ACCOUNT,
                3,
                AccountStatus::Active,
            ))),
        )
        .await
        .unwrap();
    }
    AdminStore::remove_snapshot(&*store, revoked).await.unwrap();

    let mut updates = store.subscribe();
    let change = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();
    assert_eq!(
        change.republished, 2,
        "the tombstone is not republished, so it is not counted either"
    );

    let mut pushed = Vec::new();
    while let Ok(push) = updates.try_recv() {
        let SnapshotResolution::Present(snapshot) = push.resolution else {
            panic!("a status change republishes; it never revokes");
        };
        assert_eq!(snapshot.status, AccountStatus::Suspended);
        pushed.push(push.principal);
    }
    assert_eq!(
        pushed,
        vec![first, second],
        "one push per live principal, ordered, and none for the tombstone"
    );
}

/// The door the unification would otherwise leave open: publishing a snapshot
/// whose status contradicts the ledger would recreate the two-record
/// disagreement one principal at a time.
#[tokio::test]
async fn publishing_a_snapshot_that_contradicts_the_ledger_is_refused() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(10);
    AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();

    assert_eq!(
        AdminStore::publish_snapshot(
            &*store,
            principal,
            publishable(Arc::new(account_snapshot(
                ACCOUNT,
                3,
                AccountStatus::Active,
            ))),
        )
        .await
        .unwrap_err(),
        PublishSnapshotError::StatusMismatch {
            ledger: AccountStatus::Suspended,
            submitted: AccountStatus::Active,
        },
    );
    assert!(
        matches!(
            store.snapshot(principal).await.unwrap(),
            SnapshotResolution::Unknown
        ),
        "and the refusal wrote nothing"
    );

    AdminStore::publish_snapshot(
        &*store,
        principal,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            3,
            AccountStatus::Suspended,
        ))),
    )
    .await
    .expect("a publish carrying the ledger's status is fine");

    // An account the ledger does not hold publishes unchanged: this adds no
    // account-existence requirement to publication.
    AdminStore::publish_snapshot(
        &*store,
        Principal(99),
        publishable(Arc::new(account_snapshot(
            AccountId(4_242),
            1,
            AccountStatus::Active,
        ))),
    )
    .await
    .expect("an unknown account is not a mismatch");
}

/// A refusal moves *nothing*, including when it comes from the snapshot half.
///
/// A snapshot already at `Generation(u64::MAX)` cannot be republished, and the
/// tempting shape — write the ledger, then loop the snapshots — would leave the
/// account suspended with `Active` snapshots behind that overflow. That is the
/// divergence INVARIANTS.md #22 forbids, reachable from inside the mechanism
/// meant to prevent it, so the transition plans every write before applying
/// any.
#[tokio::test]
async fn a_status_change_that_cannot_republish_moves_neither_record() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    let principal = Principal(10);
    AdminStore::publish_snapshot(
        &*store,
        principal,
        publishable(Arc::new(account_snapshot(
            ACCOUNT,
            u64::MAX,
            AccountStatus::Active,
        ))),
    )
    .await
    .unwrap();

    let error = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap_err();
    assert!(
        matches!(error, SetStatusError::Storage(_)),
        "an unrepublishable snapshot surfaces, rather than being skipped: {error:?}"
    );

    assert_eq!(
        status_of(&store, principal).await,
        (AccountStatus::Active, Generation(u64::MAX)),
        "the snapshot half did not move"
    );
    store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
        .await
        .expect("and neither did the ledger half");
}

/// Overage bills and funds in the same transaction. Without the funding half
/// the equation would fail by exactly the overage, so the negative control is
/// the point of the test rather than decoration.
#[tokio::test]
async fn overage_usage_is_billed_and_funds_itself() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let before = store.conservation(ACCOUNT).unwrap();
    assert_eq!(before.overage_recorded, CostUnits::ZERO);

    let report = store
        .ingest(&[overage_usage(ACCOUNT, 1, 40, 1)], t(1))
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (1, 0, 0)
    );

    let after = store.conservation(ACCOUNT).unwrap();
    assert_eq!(after.overage_recorded, CostUnits(40));
    assert_eq!(after.settled_usage, CostUnits(40));
    assert_eq!(after.deposited, before.deposited, "no deposit was made");
    assert_eq!(after.balance, before.balance, "and no balance was spent");
    assert!(after.holds(), "conservation violated: {after:?}");
    assert!(
        !Conservation {
            overage_recorded: CostUnits::ZERO,
            ..after
        }
        .holds(),
        "the funding term must be what closes the equation"
    );
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(40));
}

/// The one thing an overage event is checked against. It has no capability to
/// verify and no lease capacity to fit inside, so an account that does not
/// exist is the only way it can be wrong.
#[tokio::test]
async fn overage_usage_for_an_unknown_account_is_rejected() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let report = store
        .ingest(&[overage_usage(AccountId(u128::MAX), 1, 40, 1)], t(1))
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 0, 1)
    );
    assert_conserved(&store);
}

/// `request_id` is the only idempotency axis and is independent of how the
/// units were funded, which is what lets INVARIANTS.md #7 survive the leaseless
/// event unchanged.
#[tokio::test]
async fn an_overage_replay_is_idempotent() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let event = overage_usage(ACCOUNT, 1, 40, 1);
    assert_eq!(store.ingest(&[event], t(1)).await.unwrap().accepted, 1);
    let report = store.ingest(&[event, event], t(2)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 2, 0)
    );
    assert_eq!(
        store.conservation(ACCOUNT).unwrap().overage_recorded,
        CostUnits(40),
        "a replay bills once, so it funds once"
    );
    assert_conserved(&store);
}

/// INVARIANTS.md #11 for the funding term: a total that cannot be represented
/// is surfaced, never wrapped and never quietly dropped. Both columns move
/// together or neither does, so an overflow in either is the same refusal.
///
/// Mirrored by `overage_accounting_overflow_is_surfaced` in the Postgres
/// suite, which reaches the same verdict through its own arithmetic.
#[tokio::test]
async fn overage_accounting_overflow_is_surfaced() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    // Fill the funding term to the top of its domain, which an accepted event
    // is allowed to do.
    assert_eq!(
        store
            .ingest(&[overage_usage(ACCOUNT, 1, u64::MAX, 1)], t(1))
            .await
            .unwrap()
            .accepted,
        1
    );
    let before = store.conservation(ACCOUNT).unwrap();
    assert_eq!(before.overage_recorded, CostUnits(u64::MAX));

    let error = store
        .ingest(&[overage_usage(ACCOUNT, 2, 1, 2)], t(2))
        .await
        .expect_err("a total that cannot be represented must be surfaced");
    assert!(
        error.to_string().contains("overage accounting overflow"),
        "unexpected error: {error}"
    );

    let after = store.conservation(ACCOUNT).unwrap();
    assert_eq!(
        (after.overage_recorded, after.settled_usage),
        (before.overage_recorded, before.settled_usage),
        "a refused event moves neither column"
    );
}

/// The regression this whole leaseless design exists to prevent. Attributing
/// overage to a real lease would drive `used` past `granted`, and settlement
/// would then compute a negative credit — a panic here and a permanently
/// failing sweep in Postgres. Release and reclaim must stay ordinary on an
/// account that has spent overage.
#[tokio::test]
async fn settlement_is_unaffected_by_an_account_carrying_overage() {
    let store = store_with_balance(full_grant_policy(), 1_000);
    let released = store
        .acquire(ACCOUNT, CostUnits(300), TTL, t(0))
        .await
        .unwrap();
    let expired = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(0))
        .await
        .unwrap();

    assert_eq!(
        store
            .ingest(
                &[
                    overage_usage(ACCOUNT, 1, 90, 1),
                    usage(&released, 2, 100, 1),
                ],
                t(1)
            )
            .await
            .unwrap()
            .accepted,
        2
    );

    // A graceful release still returns exactly the unspent units.
    store
        .release(
            released.lease_id,
            released.fencing_token,
            CostUnits(200),
            t(2),
        )
        .await
        .unwrap();

    // And reclaim still credits `granted - used` for the expired one.
    let reclaimed = store
        .reclaim_expired(
            expired
                .expires_at
                .checked_add(SignedDuration::from_secs(3_600))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);

    let conservation = store.conservation(ACCOUNT).unwrap();
    assert_eq!(conservation.overage_recorded, CostUnits(90));
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
}

// ---------------------------------------------------------------------------
// Credential lifecycle (#104). Mirrored scenario for scenario in the Postgres
// suite: a backend that holds credentials must agree with this one about what
// "active" means, or two instances projecting from different backends would
// verify different credential sets.
// ---------------------------------------------------------------------------

fn key(id: u128, principal: u128, digest_byte: u8, not_after: Option<Timestamp>) -> KeyRecord {
    KeyRecord {
        key_id: KeyId(id),
        account_id: ACCOUNT,
        principal: Principal(principal),
        digest: [digest_byte; 32],
        not_after,
    }
}

#[tokio::test]
async fn a_recorded_credential_is_active_until_it_is_revoked() {
    let store = store_with_balance(full_grant_policy(), 100);
    store.insert_key(key(1, 11, 0xa1, None)).await.unwrap();

    let active = store.active_keys(t(0)).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].key_id, KeyId(1));
    assert_eq!(active[0].principal, Principal(11));
    assert_eq!(active[0].digest, [0xa1; 32]);

    assert_eq!(
        store.revoke_key(KeyId(1), t(10)).await,
        Ok(Revocation::Retired)
    );
    assert!(
        store.active_keys(t(11)).await.unwrap().is_empty(),
        "a retired credential is never active again"
    );
}

#[tokio::test]
async fn revoking_reports_whether_anything_was_retired() {
    let store = store_with_balance(full_grant_policy(), 100);
    store.insert_key(key(1, 11, 0xa1, None)).await.unwrap();

    assert_eq!(
        store.revoke_key(KeyId(1), t(10)).await,
        Ok(Revocation::Retired)
    );
    assert_eq!(
        store.revoke_key(KeyId(1), t(20)).await,
        Ok(Revocation::AlreadyRetired),
        "the second call changed nothing and says so"
    );
    assert_eq!(
        store.revoke_key(KeyId(404), t(20)).await,
        Err(KeyError::UnknownKey),
        "revoking a key that never existed is a mistake, not a no-op"
    );
}

#[tokio::test]
async fn a_credential_expires_out_of_the_active_set_without_being_revoked() {
    let store = store_with_balance(full_grant_policy(), 100);
    store
        .insert_key(key(1, 11, 0xa1, Some(t(100))))
        .await
        .unwrap();

    assert_eq!(store.active_keys(t(99)).await.unwrap().len(), 1);
    assert!(
        store.active_keys(t(100)).await.unwrap().is_empty(),
        "expiry is exclusive, and needs no operator action"
    );
    assert_eq!(
        store.revoke_key(KeyId(1), t(200)).await,
        Ok(Revocation::Retired),
        "an expired credential is still revocable: expiry and retirement are different facts"
    );
}

#[tokio::test]
async fn issuance_is_never_destructive() {
    let store = store_with_balance(full_grant_policy(), 100);
    store.insert_key(key(1, 11, 0xa1, None)).await.unwrap();

    assert_eq!(
        store.insert_key(key(1, 22, 0xb2, None)).await,
        Err(KeyError::AlreadyExists),
        "an overwrite would retire a live credential whose digest cannot be recovered"
    );
    let active = store.active_keys(t(0)).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[0].digest, [0xa1; 32],
        "the refused insert changed nothing"
    );
}

#[tokio::test]
async fn a_credential_cannot_belong_to_an_account_that_does_not_exist() {
    let store = store_with_balance(full_grant_policy(), 100);
    let orphan = KeyRecord {
        account_id: AccountId(404),
        ..key(1, 11, 0xa1, None)
    };
    assert_eq!(
        store.insert_key(orphan).await,
        Err(KeyError::UnknownAccount),
        "a credential nothing can authenticate as is refused at issuance, not at every admission"
    );
    assert!(store.active_keys(t(0)).await.unwrap().is_empty());
}

#[tokio::test]
async fn the_active_set_is_ordered_so_two_instances_project_alike() {
    let store = store_with_balance(full_grant_policy(), 100);
    for id in [3u128, 1, 2] {
        store
            .insert_key(key(id, 100 + id, id as u8, None))
            .await
            .unwrap();
    }
    let ids: Vec<_> = store
        .active_keys(t(0))
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.key_id)
        .collect();
    assert_eq!(ids, [KeyId(1), KeyId(2), KeyId(3)]);
}

/// Two credentials cannot share a principal: that value is the identity
/// admission decides with, so a collision would let one account's revocation
/// withdraw another's credential.
#[tokio::test]
async fn two_credentials_cannot_share_a_principal() {
    let store = store_with_balance(full_grant_policy(), 100);
    store.insert_key(key(1, 11, 0xa1, None)).await.unwrap();
    assert_eq!(
        store.insert_key(key(2, 11, 0xb2, None)).await,
        Err(KeyError::AlreadyExists)
    );
    assert_eq!(store.active_keys(t(0)).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Failed operations move nothing (#57). `MemoryStore` is the executable
// reference for `PostgresStore`'s transactions, so an operation that returns
// `Err` must leave the ledger exactly as it found it. The Postgres suite
// mirrors these by name; the magnitudes differ because the backends' ceilings
// do — `CostUnits` is `u64` here and `BIGINT` there.
// ---------------------------------------------------------------------------

/// A deposit that cannot be represented moves neither column.
///
/// Conservation keeps `balance <= deposited`, so `deposited` reaches the
/// ceiling first: crediting `balance` before knowing `deposited` could move
/// left the ledger permanently short by the deposit, while the operator was
/// told it failed.
#[tokio::test]
async fn a_refused_deposit_moves_neither_column() {
    let store = MemoryStore::new(full_grant_policy()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(u64::MAX),
        status: AccountStatus::Active,
    });
    // Spend some balance so `balance < deposited`, which is the ordering that
    // makes `deposited` overflow while `balance` still has room.
    store
        .acquire(ACCOUNT, CostUnits(1_000), TTL, t(0))
        .await
        .unwrap();
    let before = store.conservation(ACCOUNT).unwrap();
    assert!(before.holds(), "fixture must start conserved: {before:?}");

    let refused = AdminStore::deposit(&*store, ACCOUNT, CostUnits(500)).await;
    assert!(refused.is_err(), "the deposit cannot be represented");

    let after = store.conservation(ACCOUNT).unwrap();
    assert_eq!(
        after.balance, before.balance,
        "a refused deposit credited balance anyway"
    );
    assert_eq!(after.deposited, before.deposited);
    assert!(
        after.holds(),
        "the ledger no longer conserves after a refused deposit: {after:?}"
    );
}

/// A batch that fails part-way applies none of it.
///
/// The overage branch can fail the whole batch, and returning from the middle
/// of an applying loop committed every earlier event while telling the caller
/// the batch failed. The replay below is the sharp end: an event the caller
/// was told did not land must not come back as a duplicate.
#[tokio::test]
async fn a_failed_ingest_batch_leaves_the_ledger_untouched() {
    let store = store_with_balance(full_grant_policy(), 10_000);
    let lease = store
        .acquire(ACCOUNT, CostUnits(1_000), TTL, t(0))
        .await
        .unwrap();
    let before = store.conservation(ACCOUNT).unwrap();

    let failed = store
        .ingest(
            &[
                usage(&lease, 1, 100, 0),
                overage_usage(ACCOUNT, 2, u64::MAX, 0),
            ],
            t(1),
        )
        .await;
    assert!(failed.is_err(), "the overage cannot be represented");

    assert_eq!(
        store.usage_recorded(ACCOUNT),
        CostUnits::ZERO,
        "the first event of a failed batch was applied anyway"
    );
    let after = store.conservation(ACCOUNT).unwrap();
    assert_eq!(after.settled_usage, before.settled_usage);
    assert!(
        after.holds(),
        "conservation after a failed batch: {after:?}"
    );

    // The decisive one: the caller was told the batch failed, so a replay must
    // accept the event rather than report it as already recorded.
    let replay = store
        .ingest(&[usage(&lease, 1, 100, 0)], t(2))
        .await
        .unwrap();
    assert_eq!(
        (replay.accepted, replay.duplicate),
        (1, 0),
        "an event from a failed batch was left indexed, so the replay saw a duplicate"
    );
}

/// Issue #58: a publish and a status transition racing each other must not
/// leave the two records disagreeing.
///
/// The check and the write were two lock acquisitions, so a suspension could
/// run to completion between them: it restamped every snapshot *then* live,
/// set the ledger, and reported success — and the publish then inserted a
/// principal `plan_republish` had never seen, because it did not exist yet.
/// The ledger said suspended, that principal's snapshot said active, and it
/// kept being admitted until someone repeated the transition. `PostgresStore`
/// holds `FOR SHARE` on the account row across the same pair, so it never had
/// the window.
///
/// Both orderings are legitimate outcomes — the publish may land before the
/// suspension or be refused by it. What is never legitimate is the two records
/// ending up different, which is what this asserts, whichever won.
///
/// **What this does and does not catch.** Measured against the original: two
/// bare acquisitions with no await between them passes this test, because the
/// window is a few instructions wide and only OS preemption can land inside
/// it. What it does catch, at round 0 and reliably, is the same split with any
/// scheduling point between the halves — a `.await` added later for a metric,
/// a push, a lookup — which is the realistic way this defect comes back. The
/// structural guarantee is the fix itself: `publish_locked` takes
/// `&mut Inner`, so it cannot acquire a lock and must be called by a caller
/// already holding one.
///
/// Rounds are therefore few and fixed: they exercise both orderings, not a
/// probability. Catching bare preemption would need a hook in production code
/// to stop between the halves, which costs more than this defect's remaining
/// risk justifies.
///
/// It also closes a coverage gap: memory's `AdminStore::publish_snapshot` had
/// almost none, because the existing snapshot tests drive the inherent helper
/// instead of the trait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_racing_a_suspension_never_leaves_the_records_disagreeing() {
    const ROUNDS: u128 = 32;
    for round in 0..ROUNDS {
        let store = store_with_balance(GrantPolicy::default(), 1_000);
        let principal = Principal(round + 100);

        let publisher = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                // Either outcome is legitimate — the publish may land before
                // the suspension or be refused by it — but no third one is.
                match AdminStore::publish_snapshot(
                    &*store,
                    principal,
                    publishable(Arc::new(account_snapshot(
                        ACCOUNT,
                        9,
                        AccountStatus::Active,
                    ))),
                )
                .await
                {
                    Ok(()) | Err(PublishSnapshotError::StatusMismatch { .. }) => {}
                    Err(other) => panic!("unexpected publish failure: {other}"),
                }
            }
        });
        let suspender = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
                    .await
                    .expect("an active account suspends");
            }
        });
        publisher.await.unwrap();
        suspender.await.unwrap();

        // Read the ledger through the refusal the trait already reports: a
        // mismatch names the status the ledger holds.
        let ledger = match AdminStore::publish_snapshot(
            &*store,
            Principal(round + 100_000),
            publishable(Arc::new(account_snapshot(
                ACCOUNT,
                1,
                AccountStatus::Active,
            ))),
        )
        .await
        {
            Ok(()) => AccountStatus::Active,
            Err(PublishSnapshotError::StatusMismatch { ledger, .. }) => ledger,
            Err(other) => panic!("the probe publish must not fail for storage: {other}"),
        };

        if let SnapshotResolution::Present(live) = store.snapshot(principal).await.unwrap() {
            assert_eq!(
                live.status, ledger,
                "round {round}: the ledger and principal {principal}'s live snapshot disagree, \
                 so that principal keeps being admitted against a suspended account"
            );
        }
    }
}
