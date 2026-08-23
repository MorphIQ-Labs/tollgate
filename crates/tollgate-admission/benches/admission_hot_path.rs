//! Admission-layer benchmarks, gated against `testing/perf_thresholds.json`.
//!
//! `snapshot_lookup_*` races the two map candidates; `full_check` is the
//! whole pipeline (lookup → checks → quote → rate token → lease debit →
//! reservation, then cancel to keep the lease balanced); `full_check_contended_8`
//! runs the same pipeline with seven background threads hammering the same
//! account, measuring cross-core contention on the shared limiter and lease;
//! `full_check_denied` measures the refusal path, which the other two never
//! take.

use std::hint::black_box;
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
    LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, ResolvedLimits,
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

fn big_lease() -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(u64::MAX / 2),
            expires_at: far_future(),
        },
        CostUnits::ZERO,
    ))
}

fn populate(map: &impl SnapshotMap) {
    // A realistic working set: the benched principal among hundreds.
    for i in 0..512u128 {
        let slot = LeaseSlot::empty();
        slot.install(big_lease());
        map.install(Principal(i), snapshot(), slot);
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

fn engine() -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
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

fn bench_full_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    let now = Timestamp::from_second(1_755_600_000).unwrap();

    let uncontended = engine();
    group.bench_function("full_check", |b| {
        b.iter(|| admit_once(&uncontended, black_box(Principal(97)), now))
    });

    // Contended: seven background threads on the same account as the
    // foreground measurement, sharing one limiter and one lease counter.
    let contended = Arc::new(engine());
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..7)
        .map(|_| {
            let engine = Arc::clone(&contended);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    admit_once(&engine, Principal(97), now);
                }
            })
        })
        .collect();
    group.bench_function("full_check_contended_8", |b| {
        b.iter(|| admit_once(&contended, black_box(Principal(97)), now))
    });
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
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
    group.finish();
}

criterion_group!(benches, bench_lookup, bench_full_check, bench_bulk_install);
criterion_main!(benches);
