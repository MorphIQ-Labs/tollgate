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

use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tokio::sync::Mutex;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    LeaseId, PermissionBits, Principal, PublishableSnapshot, RequestId, ResolvedLimits, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, CreateAccountError, GrantPolicy, LeaseAllocator,
    SnapshotResolution, SnapshotSource, UsageSink,
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
            active: true,
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
        lease_id: lease.lease_id,
        fencing_token: lease.fencing_token,
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
async fn fenced_out_holder_rejected() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(full_grant_policy(), 1_000).await else {
        return;
    };
    let stale = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(0))
        .await
        .unwrap();

    assert_eq!(
        store
            .release(stale.lease_id, FencingToken(999), CostUnits(400), t(1))
            .await
            .unwrap_err(),
        AllocateError::Fenced
    );

    // A failed release must finish rolling back before it returns. Reclaim
    // uses SKIP LOCKED, so a drop-queued rollback could otherwise make this
    // immediately following sweep miss the stale lease intermittently.
    let reclaimed = store.reclaim_expired(t(61)).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    let replacement = store
        .acquire(ACCOUNT, CostUnits(400), TTL, t(61))
        .await
        .unwrap();
    assert!(replacement.fencing_token > stale.fencing_token);

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
    assert_eq!(
        store.usage_recorded(ACCOUNT).await.unwrap(),
        CostUnits::ZERO
    );
    assert_conserved(&store).await;
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
    assert_eq!(store.usage_recorded(ACCOUNT).await.unwrap(), CostUnits(75));
    assert_eq!(store.usage_recorded(OTHER).await.unwrap(), CostUnits(40));
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
            active: true,
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
            Ok::<(), tollgate_store::StoreError>(())
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
            Ok::<(), tollgate_store::StoreError>(())
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
        publishable(Arc::new(AccountSnapshot {
            account_id: ACCOUNT,
            key_id: None,
            generation: Generation(generation),
            status: AccountStatus::Active,
            valid_until: t(10_000),
            permissions: PermissionBits::ALL,
            limits: ResolvedLimits {
                max_items_per_request: 64,
                rate_units_per_second: 1_000,
                rate_burst_units: 1_000,
            },
            cost_table: Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        }))
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
                active: true,
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

#[tokio::test]
async fn inactive_account_refuses_leases() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
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

#[tokio::test]
async fn snapshot_publish_fetch_and_generation_monotonicity() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store_with_balance(GrantPolicy::default(), 1_000).await else {
        return;
    };
    let principal = Principal(42);
    let snapshot = |generation: u64| {
        publishable(Arc::new(AccountSnapshot {
            account_id: ACCOUNT,
            key_id: None,
            generation: Generation(generation),
            status: AccountStatus::Active,
            valid_until: t(10_000),
            permissions: PermissionBits::ALL,
            limits: ResolvedLimits {
                max_items_per_request: 64,
                rate_units_per_second: 1_000,
                rate_burst_units: 1_000,
            },
            cost_table: Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        }))
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
                active: true,
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

/// Set one checked column, dropping and re-adding its non-negative CHECK
/// constraint around the write (re-added NOT VALID so the planted row
/// survives while future writes stay checked). Constraint names follow the
/// migrations' `{table}_{column}_nonneg` convention.
async fn plant_value(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    id_column: &str,
    id: Vec<u8>,
    value: i64,
) {
    let constraint = format!("{table}_{column}_nonneg");
    sqlx::raw_sql(&format!(
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "UPDATE {table} SET {column} = $1 WHERE {id_column} = $2"
    ))
    .bind(value)
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql(&format!(
        "ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK ({column} >= 0) NOT VALID"
    ))
    .execute(pool)
    .await
    .unwrap();
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
    assert!(err.0.contains("settlement_loss underflow"), "got: {err}");
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
    assert!(err.0.contains("fencing token"), "got: {err}");
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
        let err = sqlx::query(&format!(
            "UPDATE {table} SET {column} = -1 WHERE {id_column} = $1"
        ))
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains(&format!("{column}_nonneg")),
            "negative {table}.{column} write must violate its CHECK constraint, got: {err}"
        );
    }
}
