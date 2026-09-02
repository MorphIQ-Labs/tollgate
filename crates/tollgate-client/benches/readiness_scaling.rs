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

// Exercises the deprecated one-shot surface on purpose: it is supported
// for a minor and must keep working.
#![allow(deprecated)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{MapEntry, SnapshotMap};
use tollgate_client::{
    SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock, TrackedPrincipals,
};
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
struct NullMap {
    counters: Arc<tollgate_admission::AdmissionCounters>,
}

impl SnapshotMap for NullMap {
    fn get(&self, _principal: &Principal) -> Option<MapEntry> {
        None
    }

    fn counters(&self) -> &Arc<tollgate_admission::AdmissionCounters> {
        &self.counters
    }

    fn install(
        &self,
        _principal: Principal,
        _snapshot: Arc<AccountSnapshot>,
        _lease: Arc<tollgate_admission::LeaseSlot>,
    ) {
    }

    fn install_revoked(&self, _principal: Principal, _until: Timestamp, _generation: Generation) {}

    fn install_unknown(&self, _principal: Principal, _until: Timestamp) {}

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
        let snapshot = AccountSnapshot::builder(
            AccountId(index),
            Generation(1),
            AccountStatus::Active,
            far_future(),
            PermissionBits::bit(0),
            tollgate_core::ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
            Arc::clone(&cost_table),
        )
        .build();
        store.publish_snapshot(
            Principal(index),
            PublishableSnapshot::try_new(Arc::new(snapshot)).expect("fixture snapshot is valid"),
        );
    }
    store
}

/// A *churned* catalogue: a live minority among tombstones, which is what a
/// service accumulates over years of signups and cancellations.
///
/// The all-live fixture above cannot show #52's problem at all — every entry
/// costs a fetch and every entry is one an instance can serve, so cost and
/// value scale together. Here they come apart: the catalogue is `principals`
/// entries and only `live` of them are serviceable.
fn churned_source(principals: usize, live: usize) -> Arc<MemoryStore> {
    let store = source(principals);
    for index in live as u128..principals as u128 {
        store.remove_snapshot(Principal(index));
    }
    store
}

fn config(principals: usize) -> SnapshotManagerConfig {
    config_for(TrackedPrincipals::Fixed(
        (0..principals as u128).map(Principal).collect(),
    ))
}

/// The same cadence with the tracked set discovered rather than configured
/// (#48), so the two benchmarks differ only in where the set comes from.
fn discovering_config() -> SnapshotManagerConfig {
    config_for(TrackedPrincipals::All { seed: Vec::new() })
}

fn config_for(principals: TrackedPrincipals) -> SnapshotManagerConfig {
    SnapshotManagerConfig {
        principals,
        // Short, so a refresh sweep follows the initial load immediately:
        // the refresh is what this benchmark times.
        refresh_interval: std::time::Duration::from_millis(1),
        unknown_ttl: SignedDuration::from_secs(30),
        revoked_ttl: SignedDuration::from_secs(3_600),
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
        // A tenth live, the rest tombstoned.
        let churned: Arc<dyn tollgate_store::SnapshotSource> =
            churned_source(principals, principals / 10);
        let live = principals / 10;
        for (label, source, config, per_sweep) in [
            (
                format!("refresh_{principals}"),
                Arc::clone(&store),
                config(principals),
                principals,
            ),
            (
                format!("refresh_discovering_{principals}"),
                Arc::clone(&store),
                discovering_config(),
                principals,
            ),
            // Same catalogue size, a tenth of it serviceable: the sweep
            // fetches only the live entries, so that is what one pass costs.
            (
                format!("refresh_churned_{principals}"),
                Arc::clone(&churned),
                discovering_config(),
                live,
            ),
        ] {
            group.bench_function(label, |b| {
                b.iter_batched(
                    || {
                        // Setup, untimed: a manager that has completed its initial
                        // load, so the map below is full when the sweep runs.
                        runtime.block_on(async {
                            let manager = SnapshotManager::spawn(
                                Arc::clone(&source),
                                Arc::new(NullMap::default()),
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
                            let target = counters.snapshot().refresh_attempts + per_sweep as u64;
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
    }
    group.finish();
}

criterion_group!(benches, bench_refresh);
criterion_main!(benches);
