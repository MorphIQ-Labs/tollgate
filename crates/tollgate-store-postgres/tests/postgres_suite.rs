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

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tokio::sync::Mutex;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    PermissionBits, Principal, RequestId, ResolvedLimits, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, GrantPolicy, LeaseAllocator, SnapshotSource,
    UsageSink,
};
use tollgate_store_postgres::PostgresStore;

static DB_LOCK: Mutex<()> = Mutex::const_new(());

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

const TTL: SignedDuration = SignedDuration::from_secs(60);
const ACCOUNT: AccountId = AccountId(1);

fn full_grant_policy() -> GrantPolicy {
    GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    }
}

/// Connect (or skip), truncate, create the standard account.
async fn store_with_balance(policy: GrantPolicy, balance: u64) -> Option<Arc<PostgresStore>> {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        eprintln!("SKIPPED: TOLLGATE_PG_URL not set (see docker-compose.yml)");
        return None;
    };
    let store = PostgresStore::connect(&url, policy)
        .await
        .expect("postgres reachable");
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
    assert_conserved(&store).await;
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
        Arc::new(AccountSnapshot {
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
        })
    };

    assert!(store.snapshot(principal).await.unwrap().is_none());
    store
        .publish_snapshot(principal, snapshot(3))
        .await
        .unwrap();
    let fetched = store.snapshot(principal).await.unwrap().unwrap();
    assert_eq!(fetched.generation, Generation(3));

    // Replayed older generation must not roll the row back.
    store
        .publish_snapshot(principal, snapshot(2))
        .await
        .unwrap();
    let fetched = store.snapshot(principal).await.unwrap().unwrap();
    assert_eq!(fetched.generation, Generation(3));

    store.remove_snapshot(principal).await.unwrap();
    assert!(store.snapshot(principal).await.unwrap().is_none());
}
