//! PostgreSQL usage-ingest scaling at the writer's production batch bound.
//!
//! Deliberately not part of `testing/perf_thresholds.json`: database latency
//! is host- and connection-dependent. Run the same benchmark against the base
//! and candidate revisions with `TOLLGATE_PG_URL` set; the portable evidence
//! is the before/after ratio and the constant query shape, not an absolute
//! threshold from a shared runner.
//!
//! The fixture lives in a uniquely named temporary schema and drops only that
//! schema afterward; it never truncates tables in the URL's existing search
//! path.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, KeyId, PolicyRevision, Principal,
    RequestId, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, KeyDirectory, KeyRecord, LeaseAllocator, UsageSink,
};
use tollgate_store_postgres::PostgresStore;

const BATCH_SIZE: usize = 256;

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

fn policy() -> GrantPolicy {
    GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    }
}

fn bench_ingest(c: &mut Criterion) {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        eprintln!("SKIPPED: TOLLGATE_PG_URL is not set");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (admin_pool, schema, store, leases) = runtime.block_on(async {
        let admin_pool = sqlx::PgPool::connect(&url).await.unwrap();
        // Generated locally from a UUID, so the unquoted identifier contains
        // only ASCII letters, digits, and underscores.
        let schema = format!("tollgate_bench_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin_pool)
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let schema_url = format!("{url}{separator}options=-csearch_path%3D{schema}");
        let store = PostgresStore::connect(&schema_url, policy()).await.unwrap();
        let mut leases = Vec::with_capacity(BATCH_SIZE);
        for index in 0..BATCH_SIZE {
            let account_id = AccountId(u128::try_from(index + 1).unwrap());
            AdminStore::create_account(
                &*store,
                AccountConfig {
                    account_id,
                    initial_balance: CostUnits(1_000_000_000),
                    status: AccountStatus::Active,
                    capacity_class: CapacityClass::Assured,
                },
            )
            .await
            .unwrap();
            let mut digest = [0x55; 32];
            digest[..16].copy_from_slice(&account_id.0.to_be_bytes());
            store
                .insert_key(KeyRecord {
                    key_id: KeyId(account_id.0),
                    account_id,
                    principal: Principal(account_id.0),
                    digest,
                    not_after: None,
                })
                .await
                .unwrap();
            leases.push(
                store
                    .acquire(
                        account_id,
                        CostUnits(1_000_000_000),
                        SignedDuration::from_secs(60),
                        timestamp(0),
                    )
                    .await
                    .unwrap()
                    .grant,
            );
        }
        (admin_pool, schema, store, leases)
    });
    let next_batch = AtomicU64::new(0);
    let expected_accepted = u64::try_from(BATCH_SIZE).unwrap();
    let mut group = c.benchmark_group("postgres_ingest");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    for (name, distinct, attributed) in [
        ("256_distinct_leases_accounts", true, false),
        ("256_distinct_attributed", true, true),
        ("256_one_account_unattributed", false, false),
        ("256_one_credential", false, true),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                    (0..BATCH_SIZE)
                        .map(|index| {
                            let lease = &leases[if distinct { index } else { 0 }];
                            UsageEvent::new(
                                RequestId((u128::from(batch) << 64) | index as u128),
                                lease.account_id,
                                UsageSource::Leased {
                                    lease_id: lease.lease_id,
                                    fencing_token: lease.fencing_token,
                                },
                                CostUnits(1),
                                // Every batch advances activity, exercising actual
                                // writes instead of repeatedly timing equal no-ops.
                                Timestamp::from_microsecond(i64::try_from(batch).unwrap() + 1)
                                    .unwrap(),
                                PolicyRevision::UNSTATED,
                                if attributed {
                                    Some(KeyId(lease.account_id.0))
                                } else {
                                    None
                                },
                            )
                        })
                        .collect::<Vec<_>>()
                },
                |events| {
                    let report = runtime
                        .block_on(store.ingest(&events, timestamp(1)))
                        .unwrap();
                    assert_eq!(report.accepted, expected_accepted);
                    black_box(report)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    drop(leases);
    drop(store);
    runtime.block_on(async {
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;
    });
}

criterion_group!(benches, bench_ingest);
criterion_main!(benches);
