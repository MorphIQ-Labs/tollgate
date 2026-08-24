//! The backend correctness suite (INVARIANTS.md #1, #4, #7, #9).
//!
//! Written against [`MemoryStore`] as the reference; the Postgres backend
//! must pass the same scenarios (its test file mirrors these by name).

use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    LeaseId, PermissionBits, Principal, PublishableSnapshot, RequestId, ResolvedLimits, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, CreateAccountError, GrantPolicy, LeaseAllocator,
    MemoryStore, ReclaimBatch, ReclaimedLease, SnapshotResolution, SnapshotSource, UsageSink,
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
        active: true,
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
            active: true,
        },
    )
    .await
    .unwrap();
    let lease = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    let mut wrong_token = usage(&lease, 1, 10, 1);
    wrong_token.fencing_token = FencingToken(lease.fencing_token.0.checked_add(1).unwrap());
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
            active: true,
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
            active: true,
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
    wrong_fence.fencing_token = FencingToken(other.fencing_token.0.checked_add(1).unwrap());
    let mut unknown_lease = usage(&other, 7, 10, 6);
    unknown_lease.lease_id = LeaseId(u128::MAX);
    let batch = [
        usage(&active, 2, 20, 6),
        replay_with_unrepresentable_units,
        wrong_fence,
        unknown_lease,
        usage(&settled, 3, 30, 4),
        repeated,
        repeated,
        usage(&active, 5, 15, 6),
    ];

    let report = store.ingest(&batch, t(6)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (4, 2, 2)
    );
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(75));
    assert_eq!(store.usage_recorded(OTHER), CostUnits(40));
    assert_eq!(
        store.conservation(ACCOUNT).unwrap().settlement_loss,
        CostUnits::ZERO
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
                active: true,
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
async fn inactive_account_refuses_leases() {
    let store = store_with_balance(GrantPolicy::default(), 1_000);
    // Through `AdminStore`, matching the PostgreSQL mirror. The two had
    // drifted: this side called the inherent method, so the trait
    // implementation could be replaced by `Ok(())` — suspending an account
    // and still serving it — with the whole suite green (#43).
    AdminStore::set_active(&*store, ACCOUNT, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
            .await
            .unwrap_err(),
        AllocateError::AccountInactive
    );
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
    store.publish_snapshot(
        principal,
        publishable(Arc::new(AccountSnapshot {
            generation: Generation(2),
            ..(*snapshot).clone()
        })),
    );
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
    store.publish_snapshot(
        principal,
        publishable(Arc::new(AccountSnapshot {
            generation: Generation(2),
            ..(*snapshot).clone()
        })),
    );
    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Revoked {
            generation: Generation(3)
        }
    ));
    assert!(updates.try_recv().is_err());

    store.publish_snapshot(
        principal,
        publishable(Arc::new(AccountSnapshot {
            generation: Generation(4),
            ..(*snapshot).clone()
        })),
    );
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("newer snapshot must supersede revocation");
    };
    assert_eq!(fetched.generation, Generation(4));
}
