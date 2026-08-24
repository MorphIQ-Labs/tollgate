//! Admission-layer benchmarks, gated against `testing/perf_thresholds.json`.
//!
//! `snapshot_lookup_*` races the two map candidates. `full_check` is the
//! whole pipeline (lookup → checks → quote → rate token → lease debit →
//! reservation, then cancel to keep the lease balanced), and
//! `full_check_contended_8` runs it with seven background threads hammering
//! the same account. Both measure the shipped default topology — the one
//! nearly every deployment runs — and their `_sharded` counterparts repeat
//! them under the opt-in eight-shard layout, so the manifest gates the
//! default and prices the option separately rather than confusing the two.
//! `full_check_denied` measures the refusal path, which none of the others
//! take.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use jiff::Timestamp;

use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, MokaSnapshotMap, Principal,
    SnapshotMap,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    LeaseGrant, LeaseId, LocalLease, LocalSharding, OpIndex, PermissionBits, PublishableSnapshot,
    ResolvedLimits,
};

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

fn snapshot() -> Arc<AccountSnapshot> {
    Arc::new(AccountSnapshot {
        account_id: AccountId(1),
        key_id: None,
        generation: Generation(1),
        status: AccountStatus::Active,
        valid_until: far_future(),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 4_096,
            // Effectively unlimited so the bench measures mechanism cost,
            // not deny paths.
            rate_units_per_second: u64::from(u32::MAX),
            rate_burst_units: u64::from(u32::MAX),
        },
        cost_table: Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    })
}

/// The same snapshot under a named account, so a benchmark can put N
/// principals across N accounts rather than all under `AccountId(1)`.
fn snapshot_for_account(account: u128) -> Arc<AccountSnapshot> {
    Arc::new(AccountSnapshot {
        account_id: AccountId(account),
        ..(*snapshot()).clone()
    })
}

fn big_lease(sharding: LocalSharding) -> Arc<LocalLease> {
    Arc::new(LocalLease::with_sharding(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(u64::MAX / 2),
            expires_at: far_future(),
        },
        CostUnits::ZERO,
        jiff::SignedDuration::ZERO,
        sharding,
    ))
}

fn populate(map: &impl SnapshotMap) {
    // A realistic working set: the benched principal among hundreds.
    let sharding = map.local_sharding();
    for i in 0..512u128 {
        let slot = LeaseSlot::with_sharding(sharding);
        slot.install(big_lease(sharding));
        map.install_publishable(
            Principal(i),
            PublishableSnapshot::try_new(snapshot()).unwrap(),
            slot,
        );
    }
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    let principal = Principal(97);

    let arc_swap = ArcSwapSnapshotMap::new();
    populate(&arc_swap);
    group.bench_function("snapshot_lookup_arc_swap", |b| {
        b.iter(|| arc_swap.get(black_box(&principal)).unwrap())
    });

    let moka = MokaSnapshotMap::new(4_096);
    populate(&moka);
    group.bench_function("snapshot_lookup_moka", |b| {
        b.iter(|| moka.get(black_box(&principal)).unwrap())
    });

    group.finish();
}

fn engine(sharding: LocalSharding) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
    populate(engine.map());
    engine
}

fn admit_once(engine: &AdmissionEngine<ArcSwapSnapshotMap>, principal: Principal, now: Timestamp) {
    let admitted = engine
        .admit(
            AdmissionRequest {
                principal,
                required: PermissionBits::bit(0),
                op: &PriceOp,
                items: 64,
            },
            now,
        )
        .unwrap();
    // Cancel instead of commit so the giant lease never drains during a run.
    black_box(admitted.reservation.cancel());
}

/// Seven background admitters on the account under measurement, stopped and
/// joined when the guard drops so one benchmark's load never leaks into the
/// next one's numbers.
struct Contended {
    engine: Arc<AdmissionEngine<ArcSwapSnapshotMap>>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Contended {
    fn engine(&self) -> &AdmissionEngine<ArcSwapSnapshotMap> {
        &self.engine
    }
}

impl Drop for Contended {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

fn contended_engine(sharding: LocalSharding) -> Contended {
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let engine = Arc::new(engine(sharding));
    let stop = Arc::new(AtomicBool::new(false));
    let workers = (0..7)
        .map(|_| {
            let engine = Arc::clone(&engine);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    admit_once(&engine, Principal(97), now);
                }
            })
        })
        .collect();
    Contended {
        engine,
        stop,
        workers,
    }
}

fn bench_full_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let sharded = LocalSharding::new(NonZeroUsize::new(8).unwrap());

    // `full_check` and `full_check_contended_8` are the gated ids, and they
    // measure the *shipped default* topology. Sharding is opt-in, so pointing
    // them at it would leave the layout nearly every deployment runs with no
    // threshold at all — and would silently redefine two manifest entries
    // whose calibration was taken against the single-counter path.
    let uncontended = engine(LocalSharding::SINGLE);
    group.bench_function("full_check", |b| {
        b.iter(|| admit_once(&uncontended, black_box(Principal(97)), now))
    });

    let uncontended_sharded = engine(sharded);
    group.bench_function("full_check_sharded", |b| {
        b.iter(|| admit_once(&uncontended_sharded, black_box(Principal(97)), now))
    });

    // Contended: seven background threads on the same account as the
    // foreground measurement. Under the opt-in layout each normally lands on
    // one local shard; under the default they all share one counter, which is
    // the contention the layout exists to remove.
    // Scoped, so each contended run's background threads are stopped and
    // joined before the next one starts measuring.
    {
        let contended = contended_engine(LocalSharding::SINGLE);
        group.bench_function("full_check_contended_8", |b| {
            b.iter(|| admit_once(contended.engine(), black_box(Principal(97)), now))
        });
    }
    {
        let contended = contended_engine(sharded);
        group.bench_function("full_check_contended_8_sharded", |b| {
            b.iter(|| admit_once(contended.engine(), black_box(Principal(97)), now))
        });
    }

    // The deny path, which nothing measured before #37 — both benches above
    // set limits high enough that only the admit path runs. An unknown
    // principal is the cheapest refusal there is: one map lookup and a
    // return, with no quote, no rate token and no lease debit to hide behind.
    // That makes it the most sensitive place to detect the per-outcome
    // counter, since it is the largest fraction of the smallest path.
    group.bench_function("full_check_denied", |b| {
        b.iter(|| {
            let denied = uncontended.admit(
                AdmissionRequest {
                    principal: black_box(Principal(9_999)),
                    required: PermissionBits::bit(0),
                    op: &PriceOp,
                    items: 64,
                },
                now,
            );
            black_box(denied.unwrap_err())
        })
    });

    group.finish();
}

/// Control-plane write amplification (review finding #9): loading 512
/// principals one-by-one on a copy-on-write map vs one bulk install.
/// Measured for visibility; not part of the threshold gate (control-plane
/// cost, not request-path cost).
fn bench_bulk_install(c: &mut Criterion) {
    use criterion::BatchSize;
    let mut group = c.benchmark_group("admission_control_plane");
    group.sample_size(20);

    let entries = || {
        (0..512u128)
            .map(|i| {
                let slot = LeaseSlot::empty();
                (Principal(i), snapshot(), slot)
            })
            .collect::<Vec<_>>()
    };
    group.bench_function("install_loop_512_arc_swap", |b| {
        b.iter_batched(
            || (ArcSwapSnapshotMap::new(), entries()),
            |(map, entries)| {
                for (principal, snapshot, lease) in entries {
                    map.install(principal, snapshot, lease);
                }
                map
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("install_many_512_arc_swap", |b| {
        b.iter_batched(
            || (ArcSwapSnapshotMap::new(), entries()),
            |(map, entries)| {
                map.install_many(entries);
                map
            },
            BatchSize::SmallInput,
        )
    });

    // The two above share one account, so the limiter registry holds a single
    // entry and its dead-entry sweep is O(1) — they isolate map-clone cost and
    // cannot see the registry rescan at all (#8). These give every principal
    // its own account, so the registry holds N entries and a per-lookup sweep
    // costs O(N) each time. Both sizes are measured: the point is the shape of
    // the curve, since the defect is quadratic rather than merely slow.
    for accounts in [512u128, 2_048] {
        let distinct = move || {
            (0..accounts)
                .map(|i| (Principal(i), snapshot_for_account(i), LeaseSlot::empty()))
                .collect::<Vec<_>>()
        };
        group.bench_function(format!("install_many_{accounts}_distinct_accounts"), |b| {
            b.iter_batched(
                || (ArcSwapSnapshotMap::new(), distinct()),
                |(map, entries)| {
                    map.install_many(entries);
                    map
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.bench_function("install_loop_512_distinct_accounts", |b| {
        let distinct = || {
            (0..512u128)
                .map(|i| (Principal(i), snapshot_for_account(i), LeaseSlot::empty()))
                .collect::<Vec<_>>()
        };
        b.iter_batched(
            || (ArcSwapSnapshotMap::new(), distinct()),
            |(map, entries)| {
                for (principal, snapshot, lease) in entries {
                    map.install(principal, snapshot, lease);
                }
                map
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, bench_lookup, bench_full_check, bench_bulk_install);
criterion_main!(benches);
