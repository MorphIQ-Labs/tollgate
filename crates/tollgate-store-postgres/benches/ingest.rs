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

use tollgate_core::{AccountId, CostUnits, RequestId, UsageEvent};
use tollgate_store::{AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, UsageSink};
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
                    active: true,
                },
            )
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
                    .unwrap(),
            );
        }
        (admin_pool, schema, store, leases)
    });
    let next_batch = AtomicU64::new(0);
    let expected_accepted = u64::try_from(BATCH_SIZE).unwrap();
    let mut group = c.benchmark_group("postgres_ingest");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(15));
    group.bench_function("256_distinct_leases_accounts", |b| {
        b.iter_batched(
            || {
                let batch = u128::from(next_batch.fetch_add(1, Ordering::Relaxed));
                leases
                    .iter()
                    .enumerate()
                    .map(|(index, lease)| UsageEvent {
                        request_id: RequestId((batch << 64) | u128::try_from(index).unwrap()),
                        account_id: lease.account_id,
                        lease_id: lease.lease_id,
                        fencing_token: lease.fencing_token,
                        units: CostUnits(1),
                        occurred_at: timestamp(1),
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
