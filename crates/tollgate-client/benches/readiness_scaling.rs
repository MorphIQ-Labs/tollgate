//! How a full snapshot refresh scales with the tracked-principal count.
//!
//! Deliberately **not** part of `testing/perf_thresholds.json`: that manifest
//! gates request-path latency on a controlled host, and this is control-plane
//! work whose absolute numbers are host-dependent. The point here is the
//! *shape* of the curve, not the value — before #22 the sweep recomputed
//! readiness by scanning every resolution once per completed fetch, so cost
//! grew with the square of the principal count.
//!
//! This drives the manager through its public surface rather than measuring
//! the index directly, so it measures the thing the issue complains about: a
//! real initial load over N principals against an in-memory source.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{MapEntry, SnapshotMap};
use tollgate_client::{SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, Generation, OpIndex,
    PermissionBits, Principal, PublishableSnapshot,
};
use tollgate_store::{GrantPolicy, MemoryStore};

/// A map that stores nothing.
///
/// Installing into the real `ArcSwapSnapshotMap` is itself super-linear in a
/// bulk load — its per-account limiter registry rescans every account on each
/// install (issue #8) — and that cost swamps everything else here. Since the
/// question is how the *manager's sweep* scales, the map is stubbed out; #8
/// is measured by its own issue, not conflated with this one.
#[derive(Debug, Default)]
struct NullMap;

impl SnapshotMap for NullMap {
    fn get(&self, _principal: &Principal) -> Option<MapEntry> {
        None
    }

    fn install(
        &self,
        _principal: Principal,
        _snapshot: Arc<AccountSnapshot>,
        _lease: Arc<tollgate_admission::LeaseSlot>,
    ) {
    }

    fn install_negative(&self, _principal: Principal, _until: Timestamp) {}

    fn install_negative_at_generation(
        &self,
        _principal: Principal,
        _until: Timestamp,
        _generation: Option<Generation>,
    ) {
    }

    fn remove(&self, _principal: &Principal) {}
}

#[derive(Clone, Copy)]
struct PriceOp;
impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

fn far_future() -> Timestamp {
    Timestamp::from_second(4_102_444_800).unwrap() // 2100-01-01
}

/// A store holding one published snapshot per principal, so the sweep resolves
/// every principal on its first pass and the measurement is the sweep itself.
fn source(principals: usize) -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let cost_table = Arc::new(
        CostTable::builder(CostUnits(50), CostUnits(50))
            .weight(&PriceOp, CostUnits(1))
            .build(),
    );
    for index in 0..principals as u128 {
        let snapshot = AccountSnapshot {
            account_id: AccountId(index),
            key_id: None,
            generation: Generation(1),
            status: AccountStatus::Active,
            valid_until: far_future(),
            permissions: PermissionBits::bit(0),
            limits: tollgate_core::ResolvedLimits {
                max_items_per_request: 64,
                rate_units_per_second: 1_000_000,
                rate_burst_units: 1_000_000,
            },
            cost_table: Arc::clone(&cost_table),
        };
        store.publish_snapshot(
            Principal(index),
            PublishableSnapshot::try_new(Arc::new(snapshot)).expect("fixture snapshot is valid"),
        );
    }
    store
}

fn config(principals: usize) -> SnapshotManagerConfig {
    SnapshotManagerConfig {
        principals: (0..principals as u128).map(Principal).collect(),
        // Short, so a refresh sweep follows the initial load immediately:
        // the refresh is what this benchmark times.
        refresh_interval: std::time::Duration::from_millis(1),
        negative_ttl: SignedDuration::from_secs(30),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 64,
    }
}

/// Time one full *refresh* — not the initial load.
///
/// The distinction matters and is easy to get wrong: during an initial load
/// the resolution map is still empty, so the readiness scans this change
/// replaces have nothing to scan and cost nothing. The quadratic only appears
/// once the map is populated, which is every refresh after the first. An
/// initial-load benchmark measures the same time before and after the fix.
///
/// Progress is read from the manager's own `refresh_attempts` counter, so the
/// measurement ends when a whole sweep has been made rather than after a
/// guessed sleep.
fn bench_refresh(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("snapshot_manager");
    group.sample_size(10);

    for principals in [512usize, 4_096, 16_384] {
        let store: Arc<dyn tollgate_store::SnapshotSource> = source(principals);
        let config = config(principals);
        group.bench_function(format!("refresh_{principals}"), |b| {
            b.iter_batched(
                || {
                    // Setup, untimed: a manager that has completed its initial
                    // load, so the map below is full when the sweep runs.
                    runtime.block_on(async {
                        let manager = SnapshotManager::spawn(
                            Arc::clone(&store),
                            Arc::new(NullMap),
                            SlotRegistry::new(),
                            Arc::new(SystemClock),
                            config.clone(),
                        )
                        .unwrap();
                        let mut ready = manager.ready();
                        while !*ready.borrow() {
                            ready.changed().await.unwrap();
                        }
                        manager
                    })
                },
                |manager| {
                    runtime.block_on(async {
                        let counters = manager.counters();
                        let target = counters.snapshot().refresh_attempts + principals as u64;
                        while counters.snapshot().refresh_attempts < target {
                            tokio::task::yield_now().await;
                        }
                        black_box(manager.shutdown().await)
                    })
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_refresh);
criterion_main!(benches);
