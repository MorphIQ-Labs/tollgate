//! The backend correctness suite against a real PostgreSQL — mirrors
//! `tollgate-store/tests/store_suite.rs` scenario for scenario. A backend that
//! passes both proves the settlement rules are backend-independent
//! (INVARIANTS.md #1, #4, #7, #9).
//!
//! Env-gated: set `TOLLGATE_PG_URL` (see docker-compose.yml). Without it every
//! test prints a skip note and exits — never a silent green that pretends
//! Postgres was covered.
//!
//! Tests share one database, so they serialize on a global lock and truncate
//! before running.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tokio::sync::Mutex;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, BudgetSchedule, CostTable, CostUnits, FencingToken,
    Generation, KeyId, LeaseId, PermissionBits, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, BudgetError, Conservation, CreateAccountError,
    GrantPolicy, KeyDirectory, KeyError, KeyRecord, LeaseAllocator, PublishSnapshotError,
    Revocation, RolledAccount, SetStatusError, SnapshotResolution, SnapshotSource, UsageSink,
};
use tollgate_store_postgres::PostgresStore;

static DB_LOCK: Mutex<()> = Mutex::const_new(());

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

const TTL: SignedDuration = SignedDuration::from_secs(60);
const ACCOUNT: AccountId = AccountId(1);

fn publishable(snapshot: Arc<AccountSnapshot>) -> PublishableSnapshot {
    PublishableSnapshot::try_new(snapshot).expect("test snapshot limits are valid")
}

fn full_grant_policy() -> GrantPolicy {
    GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    }
}

/// INVARIANTS.md #16/#18: pool bounds are validated before any network use,
/// and an unbounded acquire is not an option a caller can pick by accident.
#[tokio::test]
async fn invalid_pool_config_is_rejected_before_connecting() {
    use tollgate_store_postgres::PoolConfig;

    for config in [
        PoolConfig {
            max_connections: 0,
            ..PoolConfig::default()
        },
        PoolConfig {
            acquire_timeout: std::time::Duration::ZERO,
            ..PoolConfig::default()
        },
    ] {
        assert!(config.validate().is_err());
        assert!(
            PostgresStore::connect_with("postgres://invalid", full_grant_policy(), config)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn invalid_grant_policy_is_rejected_before_connecting() {
    let policy = GrantPolicy {
        reclaim_grace: SignedDuration::from_secs(-1),
        ..GrantPolicy::default()
    };
    assert!(
        PostgresStore::connect("postgres://invalid", policy)
            .await
            .is_err()
    );
}

/// The URL with userinfo stripped, safe to put in a panic message.
fn redact_url(url: &str) -> String {
    match url.split_once('@') {
        Some((_, host)) => format!("postgres://<redacted>@{host}"),
        None => url.to_owned(),
    }
}

/// Connect (or skip), truncate, create the standard account.
///
/// `TOLLGATE_REQUIRE_PG` turns the local-development skip into a failure, so
/// the CI gate that sets it cannot go green without a live database — the
/// harness enforces this even if the CI YAML is refactored out from under it.
async fn store_with_balance(policy: GrantPolicy, balance: u64) -> Option<Arc<PostgresStore>> {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        assert!(
            std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
            "TOLLGATE_PG_URL is unset but TOLLGATE_REQUIRE_PG is set; \
             the Postgres gate must not pass without a database"
        );
        eprintln!("SKIPPED: TOLLGATE_PG_URL not set (see docker-compose.yml)");
        return None;
    };
    let store = PostgresStore::connect(&url, policy)
        .await
        .unwrap_or_else(|e| panic!("postgres unreachable at {}: {e}", redact_url(&url)));
    store.truncate_all().await.unwrap();
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(balance),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();
    Some(store)
}

#[tokio::test]
async fn nonpositive_lease_ttl_is_rejected_without_debiting() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    assert_eq!(
        store
            .acquire(ACCOUNT, CostUnits(10), SignedDuration::ZERO, t(0))
            .await
            .unwrap_err(),
        AllocateError::InvalidTtl
    );
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(100));
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

async fn assert_conserved(store: &PostgresStore) {
    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
}

#[tokio::test]
async fn adaptive_grant_shrinks_near_exhaustion() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };

    let grant = store
        .acquire(ACCOUNT, CostUnits(600), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(grant.units, CostUnits(500));
    let grant = store
        .acquire(ACCOUNT, CostUnits(600), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(grant.units, CostUnits(250));
    let mut drained = 0u64;
    loop {
        match store.acquire(ACCOUNT, CostUnits(600), TTL, t(0)).await {
            Ok(g) => drained += g.units.get(),
            Err(AllocateError::InsufficientBalance) => break,
            Err(other) => panic!("unexpected: {other}"),
        }
    }
    assert_eq!(drained, 250);
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits::ZERO);
    assert_conserved(&store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_double_spend_across_instances() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 100_000).await else {
        return;
    };
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
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits::ZERO);
    assert_conserved(&store).await;
}

#[tokio::test]
async fn newer_lease_does_not_invalidate_older_active_capability() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(925));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(75));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn wrong_token_release_leaves_lease_reclaimable() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

    // A failed release must finish rolling back before it returns. Reclaim
    // uses SKIP LOCKED, so a drop-queued rollback could otherwise make this
    // immediately following sweep miss the stale lease intermittently.
    let reclaimed = store.reclaim_expired(t(61)).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].lease_id, lease.lease_id);
    assert_eq!(
        store.usage_recorded(ACCOUNT).await.unwrap(),
        CostUnits::ZERO
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn usage_rejects_mismatched_lease_capability() {
    const OTHER: AccountId = AccountId(2);
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

    assert_eq!(
        store.usage_recorded(ACCOUNT).await.unwrap(),
        CostUnits::ZERO
    );
    assert_eq!(store.usage_recorded(OTHER).await.unwrap(), CostUnits::ZERO);
    assert_conserved(&store).await;
    assert!(store.conservation(OTHER).await.unwrap().unwrap().holds());
}

#[tokio::test]
async fn expired_lease_units_reclaimed() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));

    let report = store
        .ingest(&[usage(&lease, 1, 70, 10), usage(&lease, 2, 50, 20)], t(20))
        .await
        .unwrap();
    assert_eq!(report.accepted, 2);

    assert!(store.reclaim_expired(t(59)).await.unwrap().is_empty());

    let reclaimed = store.reclaim_expired(t(60)).await.unwrap();
    assert_eq!(reclaimed[0].reclaimed, CostUnits(380));
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(880));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(120));

    let report = store
        .ingest(&[usage(&lease, 3, 10, 61)], t(61))
        .await
        .unwrap();
    assert_eq!(report.rejected, 1);
    assert_conserved(&store).await;
}

/// INVARIANTS.md #9: the production backend commits an outage backlog in
/// bounded chunks while preserving exact per-account conservation.
#[tokio::test]
async fn expired_backlog_is_reclaimed_in_bounded_batches() {
    const OTHER: AccountId = AccountId(2);
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 200).await else {
        return;
    };
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
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(175));
    assert_eq!(store.balance(OTHER).await.unwrap(), CostUnits(350));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(25));
    assert_eq!(store.usage_recorded(OTHER).await.unwrap(), CostUnits(50));
    assert_conserved(&store).await;
    let other_conservation = store.conservation(OTHER).await.unwrap().unwrap();
    assert!(
        other_conservation.holds(),
        "conservation violated: {other_conservation:?}"
    );
}

#[tokio::test]
async fn usage_replay_is_idempotent() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let batch = [usage(&lease, 1, 70, 10), usage(&lease, 2, 50, 10)];

    let first = store.ingest(&batch, t(10)).await.unwrap();
    assert_eq!((first.accepted, first.duplicate), (2, 0));
    let replay = store.ingest(&batch, t(11)).await.unwrap();
    assert_eq!((replay.accepted, replay.duplicate), (0, 2));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(120));

    // A duplicate *within* one batch is also caught, once.
    let with_dup = [usage(&lease, 3, 10, 12), usage(&lease, 3, 10, 12)];
    let report = store.ingest(&with_dup, t(12)).await.unwrap();
    assert_eq!((report.accepted, report.duplicate), (1, 1));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(130));
    assert_conserved(&store).await;
}

/// INVARIANTS.md #7: a successful mixed batch classifies every event once
/// while applying only accepted deltas across active and settled leases.
#[tokio::test]
async fn mixed_usage_batch_preserves_partial_acceptance() {
    const OTHER: AccountId = AccountId(2);
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(100));
    assert_eq!(store.usage_recorded(OTHER).await.unwrap(), CostUnits(40));
    assert_eq!(
        store
            .conservation(ACCOUNT)
            .await
            .unwrap()
            .unwrap()
            .overage_recorded,
        CostUnits(25),
        "the overage event funds the units it billed"
    );
    assert_eq!(
        store
            .conservation(ACCOUNT)
            .await
            .unwrap()
            .unwrap()
            .settlement_loss,
        CostUnits::ZERO
    );
    assert_conserved(&store).await;
    let other_conservation = store.conservation(OTHER).await.unwrap().unwrap();
    assert!(
        other_conservation.holds(),
        "conservation violated: {other_conservation:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_multi_account_batches_use_stable_lock_order() {
    const OTHER: AccountId = AccountId(2);
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: OTHER,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();

    let a1 = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    let a2 = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    let b1 = store
        .acquire(OTHER, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    let b2 = store
        .acquire(OTHER, CostUnits(400), TTL, t(0))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let left = tokio::spawn({
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        async move {
            for i in 0..64u128 {
                barrier.wait().await;
                store
                    .ingest(
                        &[usage(&a1, 10_000 + i, 1, 1), usage(&b1, 20_000 + i, 1, 1)],
                        t(1),
                    )
                    .await?;
            }
            Ok::<(), tollgate_store::IngestError>(())
        }
    });
    let right = tokio::spawn({
        let store = Arc::clone(&store);
        async move {
            for i in 0..64u128 {
                barrier.wait().await;
                store
                    .ingest(
                        &[usage(&b2, 30_000 + i, 1, 1), usage(&a2, 40_000 + i, 1, 1)],
                        t(1),
                    )
                    .await?;
            }
            Ok::<(), tollgate_store::IngestError>(())
        }
    });

    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(128));
    assert_eq!(store.usage_recorded(OTHER).await.unwrap(), CostUnits(128));
}

#[tokio::test]
async fn graceful_release_returns_unspent() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    store
        .ingest(&[usage(&lease, 1, 100, 5)], t(5))
        .await
        .unwrap();

    assert_eq!(
        store
            .release(lease.lease_id, lease.fencing_token, CostUnits(450), t(10))
            .await
            .unwrap_err(),
        AllocateError::InvalidRelease
    );

    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(400), t(10))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(900));
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(100));
    assert_conserved(&store).await;

    assert_eq!(
        store
            .release(lease.lease_id, lease.fencing_token, CostUnits(0), t(11))
            .await
            .unwrap_err(),
        AllocateError::LeaseNotActive
    );
}

/// Review finding #1, store half: reclaim waits out the grace window past
/// expiry; a graceful release during grace is honored (mirrors the memory
/// suite scenario for scenario).
#[tokio::test]
async fn reclaim_waits_for_grace_and_release_works_within_it() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(
        GrantPolicy {
            shrink_divisor: 1,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(300),
            reclaim_grace: SignedDuration::from_secs(30),
        },
        1_000,
    )
    .await
    else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap(); // expires t(60), reclaimable from t(90)

    assert!(store.reclaim_expired(t(60)).await.unwrap().is_empty());
    assert!(store.reclaim_expired(t(89)).await.unwrap().is_empty());
    let report = store
        .ingest(&[usage(&lease, 1, 120, 59)], t(65))
        .await
        .unwrap();
    assert_eq!(report.accepted, 1);

    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(380), t(70))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(880));
    assert_conserved(&store).await;

    let lease2 = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(70))
        .await
        .unwrap(); // expires t(130)
    assert!(store.reclaim_expired(t(159)).await.unwrap().is_empty());
    let reclaimed = store.reclaim_expired(t(160)).await.unwrap();
    assert_eq!(reclaimed[0].lease_id, lease2.lease_id);
    assert_eq!(reclaimed[0].reclaimed, CostUnits(400));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn straggler_usage_after_release_is_billed() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();

    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(470), t(10))
        .await
        .unwrap();
    let c = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(c.settlement_loss, CostUnits(30));

    let report = store
        .ingest(&[usage(&lease, 1, 30, 5)], t(11))
        .await
        .unwrap();
    assert_eq!(report.accepted, 1);
    let c = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(c.settlement_loss, CostUnits::ZERO);
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(30));
    assert!(c.holds());

    let report = store
        .ingest(&[usage(&lease, 2, 1, 12)], t(12))
        .await
        .unwrap();
    assert_eq!(report.rejected, 1);
}

/// A publish that changed the row must reach subscribers, and one that was
/// superseded must not: push latency is the difference between propagating
/// in milliseconds and waiting for the next refresh interval.
#[tokio::test]
async fn publish_pushes_to_subscribers_only_when_the_row_changes() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let principal = Principal(4242);
    let mut updates = SnapshotSource::subscribe(&*store);

    let snapshot = |generation: u64| {
        publishable(Arc::new(
            AccountSnapshot::builder(
                ACCOUNT,
                Generation(generation),
                AccountStatus::Active,
                t(10_000),
                PermissionBits::ALL,
                ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        ))
    };

    store
        .publish_snapshot(principal, snapshot(5))
        .await
        .unwrap();
    let pushed = updates.recv().await.expect("a new generation is pushed");
    assert_eq!(pushed.principal, principal);

    // Superseded: the row is unchanged, so there is nothing to tell anyone.
    store
        .publish_snapshot(principal, snapshot(4))
        .await
        .unwrap();
    store.remove_snapshot(principal).await.unwrap();
    let pushed = updates.recv().await.expect("the revocation is pushed");
    assert!(
        matches!(pushed.resolution, SnapshotResolution::Revoked { .. }),
        "the stale publish must not have produced a push of its own"
    );
}

/// Review finding #7 (mirrors the memory suite).
#[tokio::test]
async fn recreate_account_is_refused_and_nondestructive() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(600));
    let replacement = store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(1))
        .await
        .unwrap();
    assert!(replacement.fencing_token > lease.fencing_token);
    assert_conserved(&store).await;
}

/// Mirrors `store_suite::a_non_active_account_refuses_leases`.
#[tokio::test]
async fn a_non_active_account_refuses_leases() {
    let _guard = DB_LOCK.lock().await;
    for status in [AccountStatus::Suspended, AccountStatus::Closed] {
        let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
            return;
        };
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

/// Mirrors `store_suite::unknown_account_status_update_is_refused`.
#[tokio::test]
async fn unknown_account_status_update_is_refused() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
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

/// Mirrors `store_suite::creating_a_suspended_account_denies_from_birth`.
#[tokio::test]
async fn creating_a_suspended_account_denies_from_birth() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let suspended = AccountId(4_242);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: suspended,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Suspended,
        },
    )
    .await
    .expect("creation succeeds");

    assert_eq!(
        store
            .acquire(suspended, CostUnits(100), TTL, t(0))
            .await
            .unwrap_err(),
        AllocateError::AccountInactive,
        "a suspended account refuses from creation, not only after a transition"
    );
}

/// Mirrors `store_suite::enumerating_principals_includes_revoked_ones`.
#[tokio::test]
async fn enumerating_principals_includes_revoked_ones() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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
    AdminStore::publish_snapshot(&*store, live, snapshot())
        .await
        .unwrap();
    AdminStore::publish_snapshot(&*store, revoked, snapshot())
        .await
        .unwrap();
    AdminStore::remove_snapshot(&*store, revoked).await.unwrap();

    let mut listed = store.principals().await.unwrap().expect("enumerable");
    listed.sort_by_key(|principal| principal.0);
    assert_eq!(listed, vec![live, revoked]);
}

/// Mirrors `store_suite::a_ttl_beyond_the_policy_maximum_is_clamped`.
#[tokio::test]
async fn a_ttl_beyond_the_policy_maximum_is_clamped() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

/// Mirrors `store_suite::depositing_funds_the_account_and_the_ledger_agrees`.
/// Neither suite exercised `deposit` before #43 found it on the memory side,
/// and a mirror that covers a scenario on only one backend is how the two
/// drift apart in the first place.
#[tokio::test]
async fn depositing_funds_the_account_and_the_ledger_agrees() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(100));

    AdminStore::deposit(&*store, ACCOUNT, CostUnits(400))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));

    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(conservation.deposited, CostUnits(500));
    assert!(conservation.holds(), "conservation: {conservation:?}");

    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    assert_eq!(lease.units, CostUnits(500));
    assert_conserved(&store).await;

    assert_eq!(
        AdminStore::deposit(&*store, AccountId(999), CostUnits(1))
            .await
            .unwrap_err(),
        AllocateError::UnknownAccount,
        "an unknown account is refused, not silently created"
    );
}

#[tokio::test]
async fn snapshot_publish_fetch_and_generation_monotonicity() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(42);
    let snapshot = |generation: u64| {
        publishable(Arc::new(
            AccountSnapshot::builder(
                ACCOUNT,
                Generation(generation),
                AccountStatus::Active,
                t(10_000),
                PermissionBits::ALL,
                ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        ))
    };

    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Unknown
    ));
    store
        .publish_snapshot(principal, snapshot(3))
        .await
        .unwrap();
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.generation, Generation(3));

    // Replayed older generation must not roll the row back.
    store
        .publish_snapshot(principal, snapshot(2))
        .await
        .unwrap();
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("newest snapshot must remain present");
    };
    assert_eq!(fetched.generation, Generation(3));

    store.remove_snapshot(principal).await.unwrap();
    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Revoked {
            generation: Generation(3)
        }
    ));

    // The deleted row is a generation-3 tombstone, so a delayed older
    // publication cannot resurrect it.
    store
        .publish_snapshot(principal, snapshot(2))
        .await
        .unwrap();
    assert!(matches!(
        store.snapshot(principal).await.unwrap(),
        SnapshotResolution::Revoked {
            generation: Generation(3)
        }
    ));
    store
        .publish_snapshot(principal, snapshot(4))
        .await
        .unwrap();
    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("newer snapshot must supersede revocation");
    };
    assert_eq!(fetched.generation, Generation(4));

    assert!(
        store
            .publish_snapshot(principal, snapshot(i64::MAX as u64 + 1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn staged_limits_round_trip_through_postgres() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
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
    store
        .publish_snapshot(principal, publishable(Arc::new(snapshot)))
        .await
        .unwrap();

    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.limits, limits);
}

#[test]
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

#[tokio::test]
async fn snapshot_json_preserves_legacy_numbers_and_encodes_high_ids_exactly() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let key = KeyId((1u128 << 127) | 2);
    let principal = Principal((1u128 << 127) | 3);
    let snapshot = publishable(Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            t(10_000),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .key_id(key)
        .build(),
    ));
    store.publish_snapshot(principal, snapshot).await.unwrap();

    let pool = corruption_pool().await;
    let row = sqlx::query("SELECT snapshot FROM tollgate_snapshots WHERE principal = $1")
        .bind(principal.0.to_be_bytes().to_vec())
        .fetch_one(&pool)
        .await
        .unwrap();
    let stored: serde_json::Value = sqlx::Row::get(&row, 0);
    assert_eq!(stored["account_id"], serde_json::json!(ACCOUNT.0));
    assert_eq!(stored["key_id"], serde_json::json!(key.to_string()));
    // The generation lives in the column and nowhere else (#54). This is the
    // witness that matters: a test that only pins *which* copy wins would keep
    // passing if a second copy came back, and the duplication would be
    // restored with a green suite.
    assert!(
        stored.get("generation").is_none(),
        "the column is the only stored copy of the generation: {stored}"
    );

    let SnapshotResolution::Present(fetched) = store.snapshot(principal).await.unwrap() else {
        panic!("the storage-local representation must decode");
    };
    assert_eq!(fetched.account_id, ACCOUNT);
    assert_eq!(fetched.key_id, Some(key));
}

#[tokio::test]
async fn legacy_invalid_snapshot_is_rejected_on_read() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(47);
    let pool = corruption_pool().await;
    let snapshot = serde_json::json!({
        "account_id": 1,
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1000,
            "rate_burst_units": 113
        },
        "cost_table": {
            "fixed_request": 50,
            "minimum_charge": 50,
            "weights": [1]
        }
    });
    sqlx::query(
        "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
         VALUES ($1, 1, $2, FALSE)",
    )
    .bind(principal.0.to_be_bytes().to_vec())
    .bind(snapshot)
    .execute(&pool)
    .await
    .unwrap();

    let error = store.snapshot(principal).await.unwrap_err();
    assert!(
        error.0.contains("invalid stored snapshot") && error.0.contains("exceeding the burst"),
        "unexpected error: {error}"
    );
}

/// Issue #12: `conservation` is the reconciliation primitive, and its
/// active-lease sum filtered on `account_id` with nothing indexing it.
///
/// The mechanism is not the one the issue describes. `tollgate_leases_expiry`
/// is already partial on `state = 0`, so PostgreSQL scans *that* and discards
/// non-matching accounts — the cost never grew with the table's lifetime rows,
/// it grew with the number of live leases **fleet-wide**. A per-account
/// reconciliation query paying for every other account's live set.
///
/// So the property to pin is not "no sequential scan" (there was not one) but
/// that the account predicate is answered *by an index* rather than by reading
/// rows and throwing them away. Without the migration `account_id` can only
/// ever be a `Filter`, since no other index leads with it; with it, the
/// predicate becomes an `Index Cond`. The fixture spreads live leases across
/// many accounts because that is the only shape in which the two plans differ.
#[tokio::test]
async fn the_account_filter_is_answered_by_an_index_not_by_discarding_rows() {
    /// Live leases held by *other* accounts. These are what the expiry index
    /// makes this query read and discard when nothing indexes `account_id`.
    const OTHER_ACCOUNTS: u128 = 40;
    const LIVE_PER_ACCOUNT: usize = 25;
    /// Rotations on the measured account, settled and left behind: the table
    /// is never pruned, so this is the shape a long-running fleet has.
    const SETTLED: usize = 500;

    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000_000).await else {
        return;
    };
    for id in 2..=(OTHER_ACCOUNTS + 1) {
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: AccountId(id),
                initial_balance: CostUnits(1_000_000),
                status: AccountStatus::Active,
            },
        )
        .await
        .unwrap();
    }

    let long = SignedDuration::from_secs(300);
    let short = SignedDuration::from_secs(60);
    for _ in 0..SETTLED {
        store
            .acquire(ACCOUNT, CostUnits(1), short, t(0))
            .await
            .unwrap();
    }
    for account in std::iter::once(ACCOUNT).chain((2..=(OTHER_ACCOUNTS + 1)).map(AccountId)) {
        for _ in 0..LIVE_PER_ACCOUNT {
            store
                .acquire(account, CostUnits(1), long, t(0))
                .await
                .unwrap();
        }
    }
    let settled = store.reclaim_expired(t(120)).await.unwrap();
    assert_eq!(settled.len(), SETTLED, "only the short-TTL leases are due");

    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
    assert_eq!(
        conservation.active_lease_grants,
        CostUnits(LIVE_PER_ACCOUNT as u64),
        "the measured account holds its own live leases and no one else's"
    );

    let plan = store.explain_active_lease_sum(ACCOUNT).await.unwrap();
    assert!(
        plan.contains("tollgate_leases_account_active"),
        "the reconciliation query must reach its index; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Filter: (account_id"),
        "the account predicate is still being applied by discarding rows other \
         accounts own, which is the cost #12 exists to remove; plan was:\n{plan}"
    );
}

// ---- stored-value corruption surfacing (issues #15, #45) ------------------
//
// A negative unit column or fence counter is corruption the store must
// refuse loudly — never clamp or alias to zero, which would let
// `Conservation::holds()` pass over exactly the discrepancy it exists to
// expose (or misattribute a corrupt fence to the caller as `Fenced`).
// These tests plant corrupt states with
// a raw connection and assert every read path errors and every write path
// rolls back. The memory backend has no mirror: `CostUnits` is `u64` there,
// so negative state is unrepresentable by construction.

/// Raw connection for planting corrupt states the store API cannot produce.
async fn corruption_pool() -> sqlx::PgPool {
    let url = std::env::var("TOLLGATE_PG_URL").expect("caller already gated on TOLLGATE_PG_URL");
    sqlx::PgPool::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("postgres unreachable at {}: {e}", redact_url(&url)))
}

fn account_bytes() -> Vec<u8> {
    ACCOUNT.0.to_be_bytes().to_vec()
}

/// Suspend every CHECK constraint on `table` whose definition mentions
/// `column`, except `keep`, and report what was dropped so it can be restored.
///
/// Matched by definition text rather than by a name convention, because a
/// column is no longer guarded only by its own `{table}_{column}_nonneg`: #97
/// added cross-column checks (`allowance_balance <= balance`,
/// `from_allowance <= granted`), and a corruption fixture that dropped one
/// constraint and tripped another would fail on the schema rather than on the
/// behaviour it is testing. Matching the text means a check added later is
/// suspended without anyone remembering to list it here.
async fn suspend_checks(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    keep: Option<&str>,
) -> Vec<(String, String)> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT conname, pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conrelid = $1::regclass AND contype = 'c'
           AND pg_get_constraintdef(oid) LIKE '%' || $2 || '%'",
    )
    .bind(table)
    .bind(column)
    .fetch_all(pool)
    .await
    .unwrap();
    let dropped: Vec<_> = rows
        .into_iter()
        .filter(|(name, _)| Some(name.as_str()) != keep)
        .collect();
    for (name, _) in &dropped {
        sqlx::raw_sql(&format!("ALTER TABLE {table} DROP CONSTRAINT {name}"))
            .execute(pool)
            .await
            .unwrap();
    }
    dropped
}

/// Put back what [`suspend_checks`] dropped, `NOT VALID` so a planted row
/// survives while every future write stays checked.
async fn restore_checks(pool: &sqlx::PgPool, table: &str, dropped: Vec<(String, String)>) {
    for (name, definition) in dropped {
        sqlx::raw_sql(&format!(
            "ALTER TABLE {table} ADD CONSTRAINT {name} {definition} NOT VALID"
        ))
        .execute(pool)
        .await
        .unwrap();
    }
}

/// Set one checked column to a value its constraints forbid, so a read path
/// can be shown to surface the corruption rather than absorb it.
async fn plant_value(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    id_column: &str,
    id: Vec<u8>,
    value: i64,
) {
    let dropped = suspend_checks(pool, table, column, None).await;
    sqlx::query(&format!(
        "UPDATE {table} SET {column} = $1 WHERE {id_column} = $2"
    ))
    .bind(value)
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    restore_checks(pool, table, dropped).await;
}

async fn set_account_column(pool: &sqlx::PgPool, column: &str, value: i64) {
    plant_value(
        pool,
        "tollgate_accounts",
        column,
        "account_id",
        account_bytes(),
        value,
    )
    .await;
}

async fn set_lease_column(pool: &sqlx::PgPool, lease: u128, column: &str, value: i64) {
    plant_value(
        pool,
        "tollgate_leases",
        column,
        "lease_id",
        lease.to_be_bytes().to_vec(),
        value,
    )
    .await;
}

#[tokio::test]
async fn negative_account_column_fails_conservation_read() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let pool = corruption_pool().await;

    // (column, restore value): the standard account starts at
    // balance = deposited = 100, usage_recorded = settlement_loss = 0.
    for (column, restore) in [
        ("deposited", 100),
        ("balance", 100),
        ("usage_recorded", 0),
        ("settlement_loss", 0),
        ("overage_recorded", 0),
    ] {
        set_account_column(&pool, column, -1).await;
        let err = store.conservation(ACCOUNT).await.unwrap_err();
        assert!(
            err.0.contains(column) && err.0.contains("negative"),
            "conservation over negative {column} must name it, got: {err}"
        );
        set_account_column(&pool, column, restore).await;
    }
    assert_conserved(&store).await;
}

#[tokio::test]
async fn negative_account_column_fails_balance_and_usage_reads() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let pool = corruption_pool().await;

    set_account_column(&pool, "balance", -1).await;
    let err = store.balance(ACCOUNT).await.unwrap_err();
    assert!(err.0.contains("account balance"), "got: {err}");
    set_account_column(&pool, "balance", 100).await;

    set_account_column(&pool, "usage_recorded", -1).await;
    let err = store.usage_recorded(ACCOUNT).await.unwrap_err();
    assert!(err.0.contains("account usage_recorded"), "got: {err}");
}

#[tokio::test]
async fn negative_lease_sum_fails_conservation_read() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;

    set_lease_column(&pool, lease.lease_id.0, "granted", -1).await;
    let err = store.conservation(ACCOUNT).await.unwrap_err();
    assert!(err.0.contains("active lease grants"), "got: {err}");

    set_lease_column(&pool, lease.lease_id.0, "granted", 500).await;
    set_lease_column(&pool, lease.lease_id.0, "used", -1).await;
    let err = store.conservation(ACCOUNT).await.unwrap_err();
    assert!(err.0.contains("active lease usage"), "got: {err}");
}

/// Issue #56: a reconciliation read must see one instant, even while the
/// ledger is moving under it.
///
/// The two halves of the equation move together under `ingest`: it raises the
/// account's `usage_recorded` and the lease's `used` in one transaction. Read
/// as two statements on two pooled connections, an `ingest` committing between
/// them paired a pre-write total with a post-write sum — reporting corruption
/// on a correct ledger, or underflowing the subtraction and panicking inside a
/// library call on a read-only path.
///
/// The existing `no_double_spend_across_instances` cannot catch this: it
/// reconciles only after every task has joined, at quiescence, where any
/// interleaving reads the same answer. This one reads *during* the writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconciliation_read_never_observes_a_torn_ledger() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(50_000), TTL, t(0))
        .await
        .unwrap();

    const WRITES: u32 = 200;
    // The writer's progress, so the reader can prove it read *during* the
    // writes. `conservation`'s own output cannot show that: `ingest` raises
    // `usage_recorded` and the lease's `used` together, so every field this
    // read returns is invariant under the writes. Without this counter the
    // test would pass just as happily against a quiesced ledger, which is the
    // one thing it must not do.
    let written = Arc::new(std::sync::atomic::AtomicU32::new(0));

    // Writer: each ingest moves `usage_recorded` and the lease's `used`
    // together in one transaction, so every commit is a window a torn read
    // could fall into.
    let writers = tokio::spawn({
        let store = Arc::clone(&store);
        let written = Arc::clone(&written);
        async move {
            for request in 0..u128::from(WRITES) {
                UsageSink::ingest(&*store, &[usage(&lease, request, 1, 0)], t(1))
                    .await
                    .unwrap();
                written.fetch_add(1, std::sync::atomic::Ordering::Release);
            }
        }
    });

    // Reader: reconcile continuously against the moving ledger. Before the
    // fix this either panicked or returned `holds() == false`.
    let reader = tokio::spawn({
        let store = Arc::clone(&store);
        let written = Arc::clone(&written);
        async move {
            let mut overlapping = 0u32;
            let mut reads = 0u32;
            loop {
                let before = written.load(std::sync::atomic::Ordering::Acquire);
                let observed = store
                    .conservation(ACCOUNT)
                    .await
                    .expect("a reconciliation read must not fail on a healthy ledger")
                    .expect("the account exists");
                assert!(
                    observed.holds(),
                    "reconciliation reported corruption on a correct ledger: {observed:?}"
                );
                reads += 1;
                // A read that began after the first commit and ended before
                // the last one straddles at least one commit boundary.
                let after = written.load(std::sync::atomic::Ordering::Acquire);
                if before > 0 && after < WRITES {
                    overlapping += 1;
                }
                // Terminates on the writer's own progress rather than on a
                // completion flag, so at least one read always happens
                // whatever the scheduler does. A reader that never ran would
                // otherwise fail this test for timing rather than for the
                // defect it exists to catch.
                if after >= WRITES || reads > 10_000 {
                    break;
                }
            }
            (reads, overlapping)
        }
    });

    writers.await.unwrap();
    let (reads, overlapping) = reader.await.unwrap();
    assert!(reads > 0, "the reader must have run at all");
    assert!(
        overlapping > 0,
        "every read landed outside the write sequence, so nothing was raced: \
         {reads} reads, {overlapping} overlapping"
    );
    assert_conserved(&store).await;
}

/// Issue #56: usage recorded below the live leases' own usage is corruption to
/// report, not to panic on."""
///
/// The subtraction runs over two stored columns of a database this process
/// does not exclusively own, so it is the "surface it" case (INVARIANTS.md
/// #11), not a documented internal invariant a caller could not violate.
/// `MemoryStore` keeps its `expect` because there the counters are maintained
/// by one process under one lock, and the state is unrepresentable.
#[tokio::test]
async fn usage_below_the_live_lease_sum_fails_the_conservation_read() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;

    // The lease has spent 100 of its grant while the account's total says
    // nothing was ever spent — the shape a torn two-statement read produced
    // on a correct ledger, and which a restore or an external write can
    // produce on a real one.
    set_lease_column(&pool, lease.lease_id.0, "used", 100).await;

    let err = store
        .conservation(ACCOUNT)
        .await
        .expect_err("a read that cannot subtract must report, not panic");
    assert!(
        err.0.contains("exceeds recorded usage"),
        "the error must name the discrepancy it found; got: {err}"
    );
}

#[tokio::test]
async fn acquire_surfaces_negative_stored_balance() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let pool = corruption_pool().await;
    set_account_column(&pool, "balance", -5).await;

    let err = store
        .acquire(ACCOUNT, CostUnits(10), TTL, t(0))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, AllocateError::Storage(e) if e.0.contains("account balance")),
        "corrupt balance must surface as storage corruption, got: {err}"
    );
}

#[tokio::test]
async fn reclaim_refuses_negative_credit() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;
    // used > granted makes the reclaim credit negative; paying it out would
    // silently debit the account.
    sqlx::query("UPDATE tollgate_leases SET used = 600 WHERE lease_id = $1")
        .bind(lease.lease_id.0.to_be_bytes().to_vec())
        .execute(&pool)
        .await
        .unwrap();

    let err = store.reclaim_expired(t(120)).await.unwrap_err();
    assert!(err.0.contains("reclaim credit"), "got: {err}");
    // The transaction rolled back: no credit or debit landed.
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));
}

#[tokio::test]
async fn straggler_exceeding_recorded_loss_fails_ingest() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    store
        .release(lease.lease_id, lease.fencing_token, CostUnits(470), t(10))
        .await
        .unwrap();
    let pool = corruption_pool().await;
    // Understate the recorded loss (10 < the lease's provisional 30) so the
    // straggler's settlement would drive the column negative.
    set_account_column(&pool, "settlement_loss", 10).await;

    let err = store
        .ingest(&[usage(&lease, 1, 30, 5)], t(11))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("settlement_loss underflow"),
        "got: {err}"
    );
    // The whole batch rolled back: nothing was billed.
    assert_eq!(
        store.usage_recorded(ACCOUNT).await.unwrap(),
        CostUnits::ZERO
    );
}

#[tokio::test]
async fn acquire_surfaces_negative_stored_fence() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let pool = corruption_pool().await;
    set_account_column(&pool, "next_fence", -1).await;

    let err = store
        .acquire(ACCOUNT, CostUnits(10), TTL, t(0))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, AllocateError::Storage(e) if e.0.contains("fencing token")),
        "corrupt fence must surface as storage corruption, got: {err}"
    );
}

#[tokio::test]
async fn release_and_ingest_surface_negative_stored_fence() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(500), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;
    set_lease_column(&pool, lease.lease_id.0, "fencing_token", -1).await;

    // A corrupt stored fence is a storage error, not a Fenced rejection that
    // misattributes the cause to the caller.
    let err = store
        .release(lease.lease_id, lease.fencing_token, CostUnits(500), t(1))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, AllocateError::Storage(e) if e.0.contains("fencing token")),
        "got: {err}"
    );

    let err = store
        .ingest(&[usage(&lease, 1, 10, 2)], t(2))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("fencing token"), "got: {err}");
}

#[tokio::test]
async fn checked_ledger_columns_reject_negative_writes() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(10), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;

    let account_columns = [
        "deposited",
        "balance",
        "usage_recorded",
        "settlement_loss",
        "overage_recorded",
        "next_fence",
    ]
    .map(|c| ("tollgate_accounts", c, "account_id", account_bytes()));
    let lease_columns = ["fencing_token", "granted", "used", "credited"].map(|c| {
        (
            "tollgate_leases",
            c,
            "lease_id",
            lease.lease_id.0.to_be_bytes().to_vec(),
        )
    });
    for (table, column, id_column, id) in account_columns.into_iter().chain(lease_columns) {
        // Only the column's own guard is left standing. A negative balance
        // also violates `allowance_balance <= balance`, and whichever of the
        // two PostgreSQL happened to evaluate first would decide the message —
        // so the cross-column checks step aside and the assertion stays about
        // the one constraint this test is named for.
        let nonneg = format!("{table}_{column}_nonneg");
        let dropped = suspend_checks(&pool, table, column, Some(&nonneg)).await;
        let result = sqlx::query(&format!(
            "UPDATE {table} SET {column} = -1 WHERE {id_column} = $1"
        ))
        .bind(id)
        .execute(&pool)
        .await;
        // Restored before the assertion, never after. The suite shares one
        // database across every test, so a panic between the drop and the
        // restore would leave the schema missing a constraint for the rest of
        // the run — and the failures that followed would point at the wrong
        // code entirely.
        restore_checks(&pool, table, dropped).await;
        let err = result.expect_err("a negative write must be refused");
        assert!(
            err.to_string().contains(&nonneg),
            "negative {table}.{column} write must violate its CHECK constraint, got: {err}"
        );
    }
}

/// The cross-column guards #97 added: the allowance portion is part of the
/// balance, and a lease's allowance funding part of its grant. Neither is
/// derivable from a per-column check, and a split that drifts is exactly how
/// an account would come to expire units it never had.
#[tokio::test]
async fn the_allowance_split_cannot_exceed_what_it_is_part_of() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(10), TTL, t(0))
        .await
        .unwrap();
    let pool = corruption_pool().await;

    for (table, column, id_column, id, value, constraint) in [
        (
            "tollgate_accounts",
            "allowance_balance",
            "account_id",
            account_bytes(),
            1_000,
            "tollgate_accounts_allowance_within_balance",
        ),
        (
            "tollgate_leases",
            "from_allowance",
            "lease_id",
            lease.lease_id.0.to_be_bytes().to_vec(),
            1_000,
            "tollgate_leases_from_allowance_within_grant",
        ),
    ] {
        let err = sqlx::query(&format!(
            "UPDATE {table} SET {column} = $1 WHERE {id_column} = $2"
        ))
        .bind(value)
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains(constraint),
            "{table}.{column} beyond its whole must violate {constraint}, got: {err}"
        );
    }
}

// ---- unified account suspension (#51) -------------------------------------
//
// Mirrors `store_suite`'s section scenario for scenario. This is where the
// generated `account_id` column and the `jsonb_set` republish are actually
// exercised: the memory backend can find an account's snapshots by scanning,
// while PostgreSQL has to have been given a way to ask.

/// Mirrors `store_suite::account_snapshot`.
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

async fn status_of(store: &PostgresStore, principal: Principal) -> (AccountStatus, Generation) {
    let SnapshotResolution::Present(snapshot) = store.snapshot(principal).await.unwrap() else {
        panic!("principal {principal} must be present");
    };
    (snapshot.status, snapshot.generation)
}

async fn publish(store: &PostgresStore, principal: Principal, snapshot: AccountSnapshot) {
    AdminStore::publish_snapshot(store, principal, publishable(Arc::new(snapshot)))
        .await
        .unwrap();
}

/// Mirrors `store_suite::suspending_an_account_stops_leases_and_republishes_its_snapshots`.
#[tokio::test]
async fn suspending_an_account_stops_leases_and_republishes_its_snapshots() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let first = Principal(10);
    let second = Principal(11);
    for principal in [first, second] {
        publish(
            &store,
            principal,
            account_snapshot(ACCOUNT, 3, AccountStatus::Active),
        )
        .await;
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

/// Mirrors `store_suite::suspension_republishes_only_the_suspended_accounts_snapshots`.
#[tokio::test]
async fn suspension_republishes_only_the_suspended_accounts_snapshots() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let other_account = AccountId(2);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: other_account,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();
    let mine = Principal(10);
    let theirs = Principal(20);
    publish(
        &store,
        mine,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;
    publish(
        &store,
        theirs,
        account_snapshot(other_account, 7, AccountStatus::Active),
    )
    .await;

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

/// Mirrors `store_suite::suspending_an_account_does_not_resurrect_revoked_principals`.
#[tokio::test]
async fn suspending_an_account_does_not_resurrect_revoked_principals() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let live = Principal(10);
    let revoked = Principal(11);
    for principal in [live, revoked] {
        publish(
            &store,
            principal,
            account_snapshot(ACCOUNT, 3, AccountStatus::Active),
        )
        .await;
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

/// Mirrors `store_suite::reactivating_an_account_restores_admission_and_bumps_generations`.
#[tokio::test]
async fn reactivating_an_account_restores_admission_and_bumps_generations() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(10);
    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;

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

/// Mirrors `store_suite::a_closed_account_cannot_be_reactivated`.
#[tokio::test]
async fn a_closed_account_cannot_be_reactivated() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(10);
    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;
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

/// Mirrors `store_suite::repeating_a_status_change_publishes_nothing_new`.
#[tokio::test]
async fn repeating_a_status_change_publishes_nothing_new() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(10);
    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;
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

/// Mirrors `store_suite::a_status_change_pushes_every_republished_principal`.
#[tokio::test]
async fn a_status_change_pushes_every_republished_principal() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let first = Principal(10);
    let second = Principal(11);
    let revoked = Principal(12);
    for principal in [first, second, revoked] {
        publish(
            &store,
            principal,
            account_snapshot(ACCOUNT, 3, AccountStatus::Active),
        )
        .await;
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

/// Mirrors `store_suite::publishing_a_snapshot_that_contradicts_the_ledger_is_refused`.
#[tokio::test]
async fn publishing_a_snapshot_that_contradicts_the_ledger_is_refused() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
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
                AccountStatus::Active
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

    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, 3, AccountStatus::Suspended),
    )
    .await;

    publish(
        &store,
        Principal(99),
        account_snapshot(AccountId(4_242), 1, AccountStatus::Active),
    )
    .await;
}

/// The dual-representation trap, exercised end to end (#51).
///
/// `StoredId` writes an id that fits `u64` as a JSON *number* and anything
/// larger as a 32-hex *string*, so the derived `account_id` column has to
/// handle both. A predicate that matched only one spelling would silently skip
/// half an account's credentials — this issue's own defect, one layer down.
/// Asserted through behaviour rather than by reading the column, so it stays
/// true whichever way the column is derived.
#[tokio::test]
async fn the_account_column_is_derived_for_both_stored_id_spellings() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    // Above u64::MAX, so `StoredId` spells it as a string; and one in the
    // legacy u64 range, spelled as a number.
    let high_account = AccountId((1u128 << 127) | 5);
    let legacy_account = AccountId(u128::from(u64::MAX));
    for account in [high_account, legacy_account] {
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: account,
                initial_balance: CostUnits(1_000),
                status: AccountStatus::Active,
            },
        )
        .await
        .unwrap();
    }
    let high_principal = Principal(30);
    let legacy_principal = Principal(31);
    publish(
        &store,
        high_principal,
        account_snapshot(high_account, 3, AccountStatus::Active),
    )
    .await;
    publish(
        &store,
        legacy_principal,
        account_snapshot(legacy_account, 3, AccountStatus::Active),
    )
    .await;

    for (account, principal) in [
        (high_account, high_principal),
        (legacy_account, legacy_principal),
    ] {
        AdminStore::set_account_status(&*store, account, AccountStatus::Suspended)
            .await
            .unwrap();
        assert_eq!(
            status_of(&store, principal).await,
            (AccountStatus::Suspended, Generation(4)),
            "the status change must reach {account}, whichever way its id is spelled"
        );
    }
}

/// Every `AccountStatus` variant survives the ledger column, so the SQL CHECK
/// constraint and `AccountStatus::as_str` cover the same three values.
///
/// The Rust side is pinned by `account_status_text_matches_its_serde_spelling`;
/// this is the half that only a real database can answer. A variant the CHECK
/// rejected would fail here rather than in production, and a spelling drift
/// between the two would show up as a constraint violation on write.
#[tokio::test]
async fn every_account_status_variant_round_trips_the_status_column() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(10);
    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;

    // Active last would be refused out of Closed, so walk the reachable order
    // and let the terminal one end it.
    for (step, status) in [
        AccountStatus::Suspended,
        AccountStatus::Active,
        AccountStatus::Closed,
    ]
    .into_iter()
    .enumerate()
    {
        AdminStore::set_account_status(&*store, ACCOUNT, status)
            .await
            .unwrap_or_else(|e| panic!("{status:?} must be storable: {e}"));
        let (stored, _) = status_of(&store, principal).await;
        assert_eq!(
            stored, status,
            "step {step}: {status:?} must round-trip the ledger column and the JSONB"
        );
    }
}

/// A status column outside the vocabulary is a storage error, never a silent
/// default to `Active`. Corruption must not be served as service — the rule
/// INVARIANTS.md #11 applies to the ledger's numbers, applied to its status.
#[tokio::test]
async fn an_unrecognized_status_column_is_a_storage_error() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let pool = corruption_pool().await;
    // The CHECK constraint is what normally makes this unrepresentable, so it
    // has to be dropped to plant the value at all — which is itself the
    // evidence that the constraint covers the vocabulary.
    //
    // Restored *before* the assertions, and dropped with IF EXISTS, following
    // `plant_value`. These tests share one database, so a helper that only
    // restores on the happy path leaves every later run broken — which is
    // exactly what happened when a mutant made the assertion below fail.
    sqlx::raw_sql(
        "ALTER TABLE tollgate_accounts DROP CONSTRAINT IF EXISTS tollgate_accounts_status_known",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE tollgate_accounts SET status = 'Bogus' WHERE account_id = $1")
        .bind(account_bytes())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "ALTER TABLE tollgate_accounts ADD CONSTRAINT tollgate_accounts_status_known \
         CHECK (status IN ('Active', 'Suspended', 'Closed')) NOT VALID",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
        .await
        .unwrap_err();
    assert!(
        matches!(error, AllocateError::Storage(_)),
        "an unrecognized status must surface, not be read as Active: {error:?}"
    );
}

/// Mirrors `store_suite::a_status_change_that_cannot_republish_moves_neither_record`.
///
/// The ceiling differs by backend — `u64::MAX` in memory, `BIGINT`'s `i64::MAX`
/// here, since `publish_snapshot` refuses anything above it — but the pinned
/// behaviour is identical: a transition that cannot republish moves neither
/// record. Here the transaction is what guarantees it; in memory it is the
/// plan-then-apply split.
#[tokio::test]
async fn a_status_change_that_cannot_republish_moves_neither_record() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(10);
    let ceiling = i64::MAX as u64;
    publish(
        &store,
        principal,
        account_snapshot(ACCOUNT, ceiling, AccountStatus::Active),
    )
    .await;

    let error = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap_err();
    assert!(
        matches!(error, SetStatusError::Storage(_)),
        "an unrepublishable snapshot surfaces, rather than being skipped: {error:?}"
    );

    assert_eq!(
        status_of(&store, principal).await,
        (AccountStatus::Active, Generation(ceiling)),
        "the snapshot half did not move"
    );
    store
        .acquire(ACCOUNT, CostUnits(100), TTL, t(0))
        .await
        .expect("and neither did the ledger half");
}

/// A status change reports the rows it changed but could not push, rather than
/// letting a partial result look like a clean one.
///
/// The republish deliberately does not fail whole over one corrupt credential:
/// that row was already unreadable before the transition touched it, and
/// refusing to suspend an account because of it is the worse outcome. But the
/// row *did* change durably and got no push, so its principal converges only at
/// the next refresh — which is exactly the "partial data is surfaced, never
/// silently absorbed" case, and why `StatusChange::unreadable` exists.
///
/// Mutation testing is what caught this being untested: `unreadable += 1` could
/// be flipped to `-=` or `*=` and every other scenario stayed green, because
/// none of them plants a row that cannot decode.
///
/// Memory has no mirror: it holds validated snapshots rather than encoded ones,
/// so an undecodable record is unrepresentable there and `unreadable` is
/// structurally zero.
#[tokio::test]
async fn a_status_change_reports_rows_it_changed_but_could_not_push() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let readable = Principal(60);
    let undecodable = Principal(61);
    publish(
        &store,
        readable,
        account_snapshot(ACCOUNT, 3, AccountStatus::Active),
    )
    .await;

    // `rate_burst_units` below the worst quote, so `PublishableSnapshot::try_new`
    // refuses it on read — the same shape `legacy_invalid_snapshot_is_rejected_on_read`
    // plants. It has to go in behind the API, because the API is what rejects it.
    let pool = corruption_pool().await;
    sqlx::query(
        "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
         VALUES ($1, 3, $2, FALSE)",
    )
    .bind(undecodable.0.to_be_bytes().to_vec())
    .bind(serde_json::json!({
        "account_id": ACCOUNT.0,
        "key_id": null,
        "generation": 3,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1000,
            "rate_burst_units": 113
        },
        "cost_table": { "fixed_request": 50, "minimum_charge": 50, "weights": [1] }
    }))
    .execute(&pool)
    .await
    .unwrap();

    let mut updates = store.subscribe();
    let change = AdminStore::set_account_status(&*store, ACCOUNT, AccountStatus::Suspended)
        .await
        .unwrap();

    assert_eq!(
        (change.republished, change.unreadable),
        (2, 1),
        "both rows changed durably; one of them could not be pushed"
    );

    let mut pushed = Vec::new();
    while let Ok(push) = updates.try_recv() {
        pushed.push(push.principal);
    }
    assert_eq!(
        pushed,
        vec![readable],
        "only the decodable principal is pushed; the other waits for a refresh"
    );

    // The undecodable row still changed: it is suspended in the ledger's sense,
    // and still refused on read for the reason it was refused before.
    assert_eq!(
        status_of(&store, readable).await,
        (AccountStatus::Suspended, Generation(4))
    );
    assert!(
        store.snapshot(undecodable).await.is_err(),
        "the corrupt row is still refused on read, as it was before the change"
    );
}

/// A row whose column and JSONB generation disagree resolves from the column,
/// on both branches (#54).
///
/// No writer in this repository can produce such a row — the copies were kept
/// equal by caller discipline, not by construction — so this plants one the way
/// the corruption block plants every other impossible state. It is what an
/// existing database's rows look like from the reader's point of view once the
/// writer stops maintaining the JSONB copy: a vestigial key that must be
/// ignored.
///
/// Before #54 the same row answered *two different generations* depending on
/// which branch you reached: the live read decoded 1 from the JSONB, while
/// revoking it reported 7 from the column. Making the two branches agree is the
/// point of the change, and this is the test that says so.
#[tokio::test]
async fn a_vestigial_jsonb_generation_is_ignored_in_favour_of_the_column() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(70);
    let pool = corruption_pool().await;
    sqlx::query(
        "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
         VALUES ($1, 7, $2, FALSE)",
    )
    .bind(principal.0.to_be_bytes().to_vec())
    .bind(serde_json::json!({
        "account_id": ACCOUNT.0,
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1000,
            "rate_burst_units": 1000
        },
        "cost_table": { "fixed_request": 50, "minimum_charge": 50, "weights": [1] }
    }))
    .execute(&pool)
    .await
    .unwrap();

    let SnapshotResolution::Present(live) = store.snapshot(principal).await.unwrap() else {
        panic!("the planted row is live");
    };
    assert_eq!(
        live.generation,
        Generation(7),
        "the live read resolves from the column, not the vestigial JSONB key"
    );

    // And the tombstone branch, which always read the column, still agrees --
    // that agreement is what did not exist before.
    AdminStore::remove_snapshot(&*store, principal)
        .await
        .unwrap();
    assert!(
        matches!(
            store.snapshot(principal).await.unwrap(),
            SnapshotResolution::Revoked {
                generation: Generation(7)
            }
        ),
        "both branches report the same generation for the same row"
    );
}

/// Mirrors `overage_usage_is_billed_and_funds_itself` in the memory suite.
#[tokio::test]
async fn overage_usage_is_billed_and_funds_itself() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let before = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(before.overage_recorded, CostUnits::ZERO);

    let report = store
        .ingest(&[overage_usage(ACCOUNT, 1, 40, 1)], t(1))
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (1, 0, 0)
    );

    let after = store.conservation(ACCOUNT).await.unwrap().unwrap();
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
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(40));
}

/// Mirrors `overage_usage_for_an_unknown_account_is_rejected`.
///
/// The backends reach the same verdict by different routes: memory looks the
/// account up directly, while this one probes for existence before
/// classification because a leased event proves its account exists by
/// resolving a lease row and an overage event has no such proof.
#[tokio::test]
async fn overage_usage_for_an_unknown_account_is_rejected() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let report = store
        .ingest(&[overage_usage(AccountId(u128::MAX), 1, 40, 1)], t(1))
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 0, 1)
    );
    assert_conserved(&store).await;
}

/// Mirrors `an_overage_replay_is_idempotent`.
#[tokio::test]
async fn an_overage_replay_is_idempotent() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let event = overage_usage(ACCOUNT, 1, 40, 1);
    assert_eq!(store.ingest(&[event], t(1)).await.unwrap().accepted, 1);
    let report = store.ingest(&[event, event], t(2)).await.unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.rejected),
        (0, 2, 0)
    );
    assert_eq!(
        store
            .conservation(ACCOUNT)
            .await
            .unwrap()
            .unwrap()
            .overage_recorded,
        CostUnits(40),
        "a replay bills once, so it funds once"
    );
    assert_conserved(&store).await;
}

/// Mirrors `overage_accounting_overflow_is_surfaced` in the memory suite.
///
/// This backend's domain is `i64` rather than `u64` — the ledger columns are
/// `BIGINT` — so the top of the domain is `i64::MAX`. The verdict is the same
/// one for the same reason: an unrepresentable total is corruption of a
/// monotonic column, surfaced as a store error rather than wrapped, and the
/// whole batch rolls back so neither column moves.
#[tokio::test]
async fn overage_accounting_overflow_is_surfaced() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let ceiling = u64::try_from(i64::MAX).unwrap();
    assert_eq!(
        store
            .ingest(&[overage_usage(ACCOUNT, 1, ceiling, 1)], t(1))
            .await
            .unwrap()
            .accepted,
        1
    );
    let before = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(before.overage_recorded, CostUnits(ceiling));

    let error = store
        .ingest(&[overage_usage(ACCOUNT, 2, 1, 2)], t(2))
        .await
        .expect_err("a total that cannot be represented must be surfaced");
    assert!(
        error.to_string().contains("overflow"),
        "unexpected error: {error}"
    );

    let after = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(
        (after.overage_recorded, after.settled_usage),
        (before.overage_recorded, before.settled_usage),
        "a rolled-back batch moves neither column"
    );
}

/// Mirrors `settlement_is_unaffected_by_an_account_carrying_overage`.
///
/// This backend is where the failure would have been worst: a negative
/// reclaim credit aborts the sweep transaction, and it would abort again on
/// every retry, so the account's expired leases would never be reclaimed.
#[tokio::test]
async fn settlement_is_unaffected_by_an_account_carrying_overage() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
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

    store
        .release(
            released.lease_id,
            released.fencing_token,
            CostUnits(200),
            t(2),
        )
        .await
        .unwrap();

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

    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(conservation.overage_recorded, CostUnits(90));
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
}

/// A row with half a capability satisfies neither ingest path, and the schema
/// makes it unrepresentable — the storage half of `UsageSource`'s guarantee.
#[tokio::test]
async fn a_usage_row_cannot_carry_half_a_capability() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let pool = corruption_pool().await;
    for (request, lease_id, fencing_token) in [
        (901u128, Some(7u128.to_be_bytes().to_vec()), None::<i64>),
        (902u128, None::<Vec<u8>>, Some(3i64)),
    ] {
        let error = sqlx::query(
            "INSERT INTO tollgate_usage_events
             (request_id, account_id, lease_id, fencing_token, units, occurred_at_us)
             VALUES ($1, $2, $3, $4, 1, 0)",
        )
        .bind(request.to_be_bytes().to_vec())
        .bind(account_bytes())
        .bind(&lease_id)
        .bind(fencing_token)
        .execute(&pool)
        .await
        .expect_err("half a capability must be refused");
        assert!(
            error.to_string().contains("lease_all_or_nothing"),
            "unexpected error: {error}"
        );
    }
    assert_conserved(&store).await;
}

// ---------------------------------------------------------------------------
// Credential lifecycle (#104). Mirrors `store_suite.rs` scenario for scenario:
// a projection built from either backend must see the same live credential
// set, so both must agree on what "active" means.
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
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

/// A database refusal this code cannot classify must stay a storage failure,
/// never a domain answer.
///
/// `insert_key` maps two specific constraint violations to `UnknownAccount`
/// and `AlreadyExists`. Everything else the database can refuse — a statement
/// timeout, a deadlock, an admin shutdown — is *not* a statement about the
/// credential, and reporting one as "already exists" would send an operator
/// looking for a key that was never issued. Same rule the lease manager holds
/// for a hung allocator: a timeout is not a refusal.
///
/// No memory mirror: `MemoryStore` has no storage layer to fail, so the
/// distinction is unrepresentable there.
///
/// The constraint is dropped before any assertion runs, so a failure here
/// cannot leave it behind for the tests that follow.
#[tokio::test]
async fn an_unclassified_database_refusal_is_storage_not_a_domain_answer() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    let pool = corruption_pool().await;
    sqlx::raw_sql(
        "ALTER TABLE tollgate_credential_keys
         ADD CONSTRAINT tollgate_credential_keys_test_refuse CHECK (false) NOT VALID",
    )
    .execute(&pool)
    .await
    .unwrap();

    let refused = store.insert_key(key(1, 11, 0xa1, None)).await;

    sqlx::raw_sql(
        "ALTER TABLE tollgate_credential_keys
         DROP CONSTRAINT tollgate_credential_keys_test_refuse",
    )
    .execute(&pool)
    .await
    .unwrap();

    match refused {
        Err(KeyError::Storage(_)) => {}
        other => panic!(
            "an unclassifiable database refusal must be Storage, not a domain answer: {other:?}"
        ),
    }
}

/// Two credentials cannot share a principal: that value is the identity
/// admission decides with, so a collision would let one account's revocation
/// withdraw another's credential.
#[tokio::test]
async fn two_credentials_cannot_share_a_principal() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    store.insert_key(key(1, 11, 0xa1, None)).await.unwrap();
    assert_eq!(
        store.insert_key(key(2, 11, 0xb2, None)).await,
        Err(KeyError::AlreadyExists)
    );
    assert_eq!(store.active_keys(t(0)).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Failed operations move nothing (#57). Mirrors `store_suite.rs` by name. This
// backend was already correct — `deposit` moves both columns in one statement
// and `ingest` runs the batch in one transaction — so these assert the parity
// the reference implementation had drifted from, at this backend's ceiling:
// `BIGINT`, where memory's is `u64`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_deposit_moves_neither_column() {
    let _guard = DB_LOCK.lock().await;
    let ceiling = u64::try_from(i64::MAX).unwrap();
    let Some(store) = store_with_balance(full_grant_policy(), ceiling).await else {
        return;
    };
    // Spend some balance so `balance < deposited`, the ordering that makes
    // `deposited` overflow while `balance` still has room.
    store
        .acquire(ACCOUNT, CostUnits(1_000), TTL, t(0))
        .await
        .unwrap();
    let before = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert!(before.holds(), "fixture must start conserved: {before:?}");

    let refused = AdminStore::deposit(&*store, ACCOUNT, CostUnits(500)).await;
    assert!(refused.is_err(), "the deposit cannot be represented");

    let after = store.conservation(ACCOUNT).await.unwrap().unwrap();
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

#[tokio::test]
async fn a_failed_ingest_batch_leaves_the_ledger_untouched() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 10_000).await else {
        return;
    };
    let lease = store
        .acquire(ACCOUNT, CostUnits(1_000), TTL, t(0))
        .await
        .unwrap();
    let before = store.conservation(ACCOUNT).await.unwrap().unwrap();

    let ceiling = u64::try_from(i64::MAX).unwrap();
    let failed = UsageSink::ingest(
        &*store,
        &[
            usage(&lease, 1, 100, 0),
            overage_usage(ACCOUNT, 2, ceiling, 0),
        ],
        t(1),
    )
    .await;
    assert!(failed.is_err(), "the overage cannot be represented");

    assert_eq!(
        store.usage_recorded(ACCOUNT).await.unwrap(),
        CostUnits::ZERO,
        "the first event of a failed batch was applied anyway"
    );
    let after = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(after.settled_usage, before.settled_usage);
    assert!(
        after.holds(),
        "conservation after a failed batch: {after:?}"
    );

    // The decisive one: the caller was told the batch failed, so a replay must
    // accept the event rather than report it as already recorded.
    let replay = UsageSink::ingest(&*store, &[usage(&lease, 1, 100, 0)], t(2))
        .await
        .unwrap();
    assert_eq!(
        (replay.accepted, replay.duplicate),
        (1, 0),
        "an event from a failed batch was left indexed, so the replay saw a duplicate"
    );
}

// --- Periodic budgets (#97) -------------------------------------------------
//
// Mirrors `store_suite.rs`'s budget block scenario for scenario. The rules are
// the same; only the mechanism differs — a row lock and a `CASE` where the
// reference backend uses a mutex and an `if`.

/// 2026-01-01T00:00:00Z.
const JAN: i64 = 1_767_225_600;
/// 2026-02-01T00:00:00Z.
const FEB: i64 = 1_769_904_000;
/// 2026-03-01T00:00:00Z.
const MAR: i64 = 1_772_323_200;

fn monthly(allowance: u64) -> BudgetSchedule {
    BudgetSchedule::monthly(CostUnits(allowance))
}

/// Roll every due account, and report what the pass did to `ACCOUNT`.
async fn roll(store: &PostgresStore, now: i64) -> Option<RolledAccount> {
    let batch = AdminStore::roll_due_periods(store, t(now), NonZeroUsize::new(64).unwrap())
        .await
        .expect("the rollover pass succeeds");
    batch
        .rolled()
        .iter()
        .find(|rolled| rolled.account_id == ACCOUNT)
        .copied()
}

#[tokio::test]
async fn setting_a_schedule_deposits_nothing() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();

    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(100));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn setting_a_schedule_on_an_unknown_account_is_refused() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    assert_eq!(
        AdminStore::set_budget_schedule(&*store, AccountId(999), Some(monthly(500)))
            .await
            .unwrap_err(),
        BudgetError::UnknownAccount
    );
}

#[tokio::test]
async fn racing_passes_cross_a_boundary_exactly_once() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();

    // Genuinely concurrent, on two pooled connections: the row lock is the
    // whole mechanism, and a sequential pair would not exercise it.
    let (first, second) = tokio::join!(roll(&store, FEB), roll(&store, FEB));

    let rolled = [first, second];
    let winners: Vec<_> = rolled.iter().flatten().collect();
    assert_eq!(winners.len(), 1, "exactly one pass crosses the boundary");
    assert_eq!(winners[0].deposited, CostUnits(500));
    assert_eq!(winners[0].expired, CostUnits::ZERO);
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(500),
        "the loser must not deposit a second allowance"
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_pass_inside_the_current_period_changes_nothing() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, FEB).await;

    assert_eq!(roll(&store, FEB + 10 * 86_400).await, None);
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));
}

#[tokio::test]
async fn an_account_without_a_schedule_is_untouched() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    assert_eq!(roll(&store, FEB).await, None);
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(100));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_top_up_survives_rollover_but_the_allowance_does_not() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;
    AdminStore::deposit(&*store, ACCOUNT, CostUnits(70))
        .await
        .unwrap();
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(570));

    assert_eq!(
        roll(&store, FEB).await,
        Some(RolledAccount {
            account_id: ACCOUNT,
            deposited: CostUnits(500),
            expired: CostUnits(500),
        }),
        "the whole unspent allowance expires; the top-up is not touched"
    );
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(570),
        "one fresh allowance plus the surviving top-up"
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_missed_period_does_not_accrue_a_backlog() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;

    // Two boundaries have passed (February and March).
    assert_eq!(
        roll(&store, MAR + 86_400).await,
        Some(RolledAccount {
            account_id: ACCOUNT,
            deposited: CostUnits(500),
            expired: CostUnits(500),
        })
    );
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_lease_from_the_closed_period_expires_its_unspent_allowance() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;

    let lease = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(FEB - 30))
        .await
        .unwrap();
    roll(&store, FEB).await;

    store
        .ingest(&[usage(&lease, 1, 50, FEB + 5)], t(FEB + 5))
        .await
        .unwrap();
    store
        .release(
            lease.lease_id,
            lease.fencing_token,
            CostUnits(150),
            t(FEB + 10),
        )
        .await
        .unwrap();

    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(500),
        "the released units belonged to the closed period; only the new allowance remains"
    );
    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(
        conservation.expired,
        CostUnits(450),
        "300 unspent at the boundary plus the lease's 150"
    );
    assert_eq!(
        conservation.settled_usage,
        CostUnits(50),
        "the straggler bills against the period the lease was granted in"
    );
    assert!(conservation.holds(), "{conservation:?}");
}

#[tokio::test]
async fn a_lease_funded_by_a_top_up_is_unaffected_by_a_boundary() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 300).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();

    let lease = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(FEB - 30))
        .await
        .unwrap();
    roll(&store, FEB).await;
    store
        .release(
            lease.lease_id,
            lease.fencing_token,
            CostUnits(200),
            t(FEB + 10),
        )
        .await
        .unwrap();

    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(800),
        "the 300 top-up is intact and the new allowance sits beside it"
    );
    assert_eq!(
        store.conservation(ACCOUNT).await.unwrap().unwrap().expired,
        CostUnits::ZERO
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_split_funded_lease_charges_the_allowance_half_first() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(60)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;

    // 160 available: 60 allowance, 100 top-up. The grant takes all of it.
    let lease = store
        .acquire(ACCOUNT, CostUnits(160), TTL, t(FEB - 30))
        .await
        .unwrap();
    roll(&store, FEB).await;
    store
        .ingest(&[usage(&lease, 1, 10, FEB + 1)], t(FEB + 1))
        .await
        .unwrap();
    store
        .release(
            lease.lease_id,
            lease.fencing_token,
            CostUnits(150),
            t(FEB + 2),
        )
        .await
        .unwrap();

    let conservation = store.conservation(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(
        conservation.expired,
        CostUnits(50),
        "the 10 units spent came out of the 60-unit allowance half, leaving 50 to expire"
    );
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(160),
        "the whole top-up survives, beside the new allowance"
    );
    assert!(conservation.holds(), "{conservation:?}");
}

#[tokio::test]
async fn reclaim_expires_a_closed_period_lease_it_sweeps() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;
    let lease = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(FEB - 30))
        .await
        .unwrap();
    roll(&store, FEB).await;

    let batch = store
        .reclaim_expired_batch(t(FEB + 3_600), NonZeroUsize::new(8).unwrap())
        .await
        .unwrap();

    assert_eq!(batch.len(), 1);
    assert_eq!(batch.reclaimed()[0].lease_id, lease.lease_id);
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(500),
        "the swept units belonged to the closed period"
    );
    assert_eq!(
        store.conservation(ACCOUNT).await.unwrap().unwrap().expired,
        CostUnits(500)
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn clearing_a_schedule_leaves_the_balance_alone() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;
    AdminStore::set_budget_schedule(&*store, ACCOUNT, None)
        .await
        .unwrap();

    assert_eq!(roll(&store, FEB).await, None);
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));
    assert_conserved(&store).await;
}

#[tokio::test]
async fn the_rollover_pass_is_bounded_and_saturation_says_there_is_more() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    for id in 2..=5u128 {
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: AccountId(id),
                initial_balance: CostUnits::ZERO,
                status: AccountStatus::Active,
            },
        )
        .await
        .unwrap();
    }
    for id in 1..=5u128 {
        AdminStore::set_budget_schedule(&*store, AccountId(id), Some(monthly(100)))
            .await
            .unwrap();
    }
    let limit = NonZeroUsize::new(2).unwrap();

    let mut seen = Vec::new();
    let mut batches = 0;
    loop {
        let batch = AdminStore::roll_due_periods(&*store, t(FEB), limit)
            .await
            .unwrap();
        batches += 1;
        assert!(batch.len() <= limit.get(), "the backend honoured the limit");
        seen.extend(batch.rolled().iter().map(|rolled| rolled.account_id));
        if !batch.is_saturated() {
            break;
        }
        assert!(batches < 10, "the drain must terminate");
    }

    assert_eq!(seen.len(), 5, "every due account is rolled exactly once");
    seen.sort_unstable_by_key(|account| account.0);
    seen.dedup();
    assert_eq!(seen.len(), 5, "no account is rolled twice across batches");
    assert_eq!(
        batches, 3,
        "two saturated batches of two, then the partial one that ends the drain"
    );
}

#[tokio::test]
async fn an_unscheduled_account_is_never_selected() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 10).await else {
        return;
    };
    for id in 2..=3u128 {
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: AccountId(id),
                initial_balance: CostUnits(10),
                status: AccountStatus::Active,
            },
        )
        .await
        .unwrap();
    }
    AdminStore::set_budget_schedule(&*store, AccountId(2), Some(monthly(100)))
        .await
        .unwrap();

    let batch = AdminStore::roll_due_periods(&*store, t(FEB), NonZeroUsize::new(64).unwrap())
        .await
        .unwrap();

    assert_eq!(batch.len(), 1);
    assert_eq!(batch.rolled()[0].account_id, AccountId(2));
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(10));
    assert_eq!(store.balance(AccountId(3)).await.unwrap(), CostUnits(10));
}

#[tokio::test]
async fn a_grant_spends_the_expiring_allowance_before_a_top_up() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 100).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(60)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;

    // 160 available: 60 allowance, 100 top-up. This takes less than either.
    let lease = store
        .acquire(ACCOUNT, CostUnits(40), TTL, t(JAN + 2))
        .await
        .unwrap();
    store
        .ingest(&[usage(&lease, 1, 40, JAN + 3)], t(JAN + 3))
        .await
        .unwrap();
    store
        .release(
            lease.lease_id,
            lease.fencing_token,
            CostUnits::ZERO,
            t(JAN + 4),
        )
        .await
        .unwrap();
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(120),
        "60 - 40 allowance, plus the untouched top-up"
    );

    assert_eq!(
        roll(&store, FEB).await.map(|rolled| rolled.expired),
        Some(CostUnits(20)),
        "the 40 spent came out of the allowance, so only 20 of it was left to expire"
    );
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(160),
        "the whole 100 top-up survives beside the new allowance"
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_lease_released_inside_its_own_period_expires_nothing() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;

    let lease = store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(JAN + 2))
        .await
        .unwrap();
    store
        .release(
            lease.lease_id,
            lease.fencing_token,
            CostUnits(200),
            t(JAN + 3),
        )
        .await
        .unwrap();

    assert_eq!(
        store.conservation(ACCOUNT).await.unwrap().unwrap().expired,
        CostUnits::ZERO
    );
    assert_eq!(
        store.balance(ACCOUNT).await.unwrap(),
        CostUnits(500),
        "the allowance is whole again"
    );
    assert_conserved(&store).await;
}

#[tokio::test]
async fn a_lease_reclaimed_inside_its_own_period_expires_nothing() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 0).await else {
        return;
    };
    AdminStore::set_budget_schedule(&*store, ACCOUNT, Some(monthly(500)))
        .await
        .unwrap();
    roll(&store, JAN + 1).await;
    store
        .acquire(ACCOUNT, CostUnits(200), TTL, t(JAN + 2))
        .await
        .unwrap();

    let batch = store
        .reclaim_expired_batch(t(JAN + 3_600), NonZeroUsize::new(8).unwrap())
        .await
        .unwrap();

    assert_eq!(batch.len(), 1);
    assert_eq!(
        store.conservation(ACCOUNT).await.unwrap().unwrap().expired,
        CostUnits::ZERO
    );
    assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(500));
    assert_conserved(&store).await;
}
