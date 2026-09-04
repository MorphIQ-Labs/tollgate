//! Hot-path microbenchmarks, gated by `scripts/check_perf_thresholds.sh`
//! against `testing/perf_thresholds.json`.
//!
//! Benchmark ids are part of the gate contract: renaming a group or function
//! here requires the matching manifest change.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use jiff::Timestamp;

use tollgate_core::{
    AccountId, AccountOverage, AccountSnapshot, AccountStatus, CancelHandle, CancelOutcome,
    CommitError, CommitFunding, CostTable, CostUnits, FencingToken, Generation, KeyId, LeaseGrant,
    LeaseId, LocalLease, OpIndex, PermissionBits, Reservation, ResolvedLimits, SharedCharge,
};

#[derive(Clone, Copy)]
enum Op {
    Price,
    Greeks,
}

impl OpIndex for Op {
    fn index(&self) -> usize {
        *self as usize
    }
}

#[derive(Clone, Copy)]
struct DenseOp(usize);

impl OpIndex for DenseOp {
    fn index(&self) -> usize {
        self.0
    }
}

fn cost_table() -> Arc<CostTable> {
    Arc::new(
        CostTable::builder(CostUnits(50), CostUnits(50))
            .weight(&Op::Price, CostUnits(1))
            .weight(&Op::Greeks, CostUnits(5))
            .build(),
    )
}

fn snapshot() -> AccountSnapshot {
    AccountSnapshot::builder(
        AccountId(1),
        Generation(1),
        AccountStatus::Active,
        Timestamp::from_second(4_102_444_800).unwrap(), // 2100-01-01
        PermissionBits::bit(0).union(PermissionBits::bit(1)),
        ResolvedLimits::new(1024).with_weighted_rate(100_000, 500_000),
        cost_table(),
    )
    .key_id(KeyId(2))
    .build()
}

fn big_lease() -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(3),
            // Large enough that no realistic sample count exhausts it.
            units: CostUnits(u64::MAX / 2),
            expires_at: Timestamp::from_second(4_102_444_800).unwrap(),
        },
        CostUnits::ZERO,
    ))
}

/// A lease usable at `now` but not one second later, so a reservation opens
/// normally and its commit takes the elastic fallback branch.
fn lapsed_lease(now: Timestamp) -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(8),
            account_id: AccountId(3),
            fencing_token: FencingToken(4),
            units: CostUnits(u64::MAX / 2),
            expires_at: lapsed_at(now),
        },
        CostUnits::ZERO,
    ))
}

fn lapsed_at(now: Timestamp) -> Timestamp {
    Timestamp::from_second(now.as_second() + 1).unwrap()
}

fn bench_cost_table(c: &mut Criterion) {
    let table = cost_table();
    let dense_table = (0..4_096).fold(
        CostTable::builder(CostUnits(50), CostUnits(50)),
        |builder, index| builder.weight(&DenseOp(index), CostUnits(5)),
    );
    let dense_table = dense_table.build();
    let mut group = c.benchmark_group("cost_table");
    group.bench_function("quote", |b| {
        b.iter(|| table.quote(black_box(&Op::Greeks), black_box(64)).unwrap())
    });
    // Quoting the last entry of a 4,096-class table must cost the same as the
    // two-class table above. A scan introduced into the request path makes
    // this same-run ratio fail by orders of magnitude.
    group.bench_function("quote_4096_classes", |b| {
        b.iter(|| {
            dense_table
                .quote(black_box(&DenseOp(4_095)), black_box(64))
                .unwrap()
        })
    });
    // Heterogeneous quoting is the shape a mixed-model batch produces. The
    // three sizes exist so the per-class cost is a measured ratio rather than
    // a claim: the fold is O(distinct classes), so 8 classes must cost roughly
    // eight times one class and nothing like a scan of the table.
    group.bench_function("quote_workload_1", |b| {
        b.iter(|| {
            dense_table
                .quote_workload(black_box(&[(DenseOp(0), 64)]))
                .unwrap()
        })
    });
    group.bench_function("quote_workload_2", |b| {
        b.iter(|| {
            dense_table
                .quote_workload(black_box(&[(DenseOp(0), 32), (DenseOp(1), 32)]))
                .unwrap()
        })
    });
    group.bench_function("quote_workload_8", |b| {
        b.iter(|| {
            dense_table
                .quote_workload(black_box(&[
                    (DenseOp(0), 8),
                    (DenseOp(1), 8),
                    (DenseOp(2), 8),
                    (DenseOp(3), 8),
                    (DenseOp(4), 8),
                    (DenseOp(5), 8),
                    (DenseOp(6), 8),
                    (DenseOp(7), 8),
                ]))
                .unwrap()
        })
    });
    group.finish();
}

fn bench_snapshot(c: &mut Criterion) {
    let snap = snapshot();
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let required = PermissionBits::bit(0);
    let mut group = c.benchmark_group("snapshot");
    group.bench_function("admit", |b| {
        b.iter(|| snap.admit(black_box(now), black_box(required)).unwrap())
    });
    group.finish();
}

fn bench_lease(c: &mut Criterion) {
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let mut group = c.benchmark_group("lease");
    // The full per-request quota interaction: debit, open the state machine,
    // commit at execution start. Committed units stay spent, so the giant
    // lease guarantees the counter never exhausts within a run.
    let lease = big_lease();
    group.bench_function("reserve_commit", |b| {
        b.iter(|| {
            let r = Reservation::reserve(black_box(&lease), CostUnits(100), now).unwrap();
            r.commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap();
            r
        })
    });
    // The cancellation path: debit then release (net zero on the counter).
    let lease2 = big_lease();
    group.bench_function("reserve_cancel", |b| {
        b.iter(|| {
            let r = Reservation::reserve(black_box(&lease2), CostUnits(100), now).unwrap();
            r.cancel()
        })
    });

    // The elastic commit transition includes the publication marker that
    // prevents observers from treating an irrevocable debit as refundable.
    // Committed units remain occupied, so the maximal cap keeps the fixture
    // out of the refusal branch for any realistic sample count.
    let overage = Arc::new(AccountOverage::new(AccountId(1)));
    group.bench_function("overage_reserve_commit", |b| {
        b.iter(|| {
            let r = Reservation::reserve_overage(
                black_box(&overage),
                CostUnits(100),
                CostUnits(u64::MAX),
            )
            .unwrap();
            r.commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap();
            r
        })
    });

    // The commit-time elastic fallback: the cold branch a lapsed lease takes
    // under `Elastic`. It costs one extra tentative debit and one lease credit
    // on top of the ordinary claim, and it is measured separately precisely so
    // that cost stays visible rather than being averaged into the path every
    // request takes. `lease/reserve_commit` above is the unlapsed comparison.
    let fallback_overage = Arc::new(AccountOverage::new(AccountId(3)));
    let lapsed = lapsed_lease(now);
    group.bench_function("overage_commit_fallback", |b| {
        b.iter(|| {
            let r = Reservation::reserve(black_box(&lapsed), CostUnits(100), now).unwrap();
            r.commit_at_execution_start(
                lapsed_at(now),
                CommitFunding::OverageFallback {
                    overage: black_box(&fallback_overage),
                    cap: CostUnits(u64::MAX),
                },
            )
            .unwrap();
            r
        })
    });

    // The stable committed-saturation path is where cap classification
    // performs its zero-delta publication RMW. Keep that synchronization cost
    // visible independently of the successful elastic path above.
    let full_overage = Arc::new(AccountOverage::new(AccountId(2)));
    Reservation::reserve_overage(&full_overage, CostUnits(100), CostUnits(100))
        .unwrap()
        .commit_at_execution_start(now, CommitFunding::LeaseOnly)
        .unwrap();
    group.bench_function("overage_refusal_committed", |b| {
        b.iter(|| {
            Reservation::reserve_overage(black_box(&full_overage), CostUnits(1), CostUnits(100))
                .unwrap_err()
        })
    });

    let contended = LeaseContended::new(now);
    let mut commit = false;
    group.bench_function("reserve_commit_contended_8", |b| {
        b.iter(|| {
            commit = !commit;
            reserve_and_resolve(black_box(&contended.lease), now, commit)
        })
    });
    group.finish();
}

fn reserve_and_resolve(lease: &Arc<LocalLease>, now: Timestamp, commit: bool) {
    let reservation = Reservation::reserve(lease, CostUnits(100), now).unwrap();
    if commit {
        black_box(
            reservation
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap(),
        );
    } else {
        black_box(reservation.cancel());
    }
}

struct LeaseContended {
    lease: Arc<LocalLease>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl LeaseContended {
    fn new(now: Timestamp) -> Self {
        let lease = big_lease();
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..7)
            .map(|worker| {
                let lease = Arc::clone(&lease);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut commit = worker % 2 == 0;
                    while !stop.load(Ordering::Relaxed) {
                        reserve_and_resolve(&lease, now, commit);
                        commit = !commit;
                    }
                })
            })
            .collect();
        Self {
            lease,
            stop,
            workers,
        }
    }
}

impl Drop for LeaseContended {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

const RACE_BATCH: usize = 4_096;

fn pending_batch(lease: &Arc<LocalLease>, now: Timestamp) -> Vec<Reservation> {
    (0..RACE_BATCH)
        .map(|_| Reservation::reserve(lease, CostUnits(1), now).unwrap())
        .collect()
}

/// The shared-charge race as a consumer runs it: a worker thread committing
/// through the shared reservation while an asynchronous waiter cancels through
/// its handle. Same single phase word as `run_commit_cancel_race`, reached
/// through one extra indirection.
fn run_split_cancel_race(
    split: &[(Arc<SharedCharge>, CancelHandle)],
    now: Timestamp,
) -> (usize, usize) {
    let start = Barrier::new(3);
    let (commit_wins, cancel_saw_committed) = std::thread::scope(|scope| {
        let commit = scope.spawn(|| {
            start.wait();
            split
                .iter()
                .filter(|(shared, _)| {
                    shared
                        .reservation()
                        .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                        .is_ok()
                })
                .count()
        });
        let cancel = scope.spawn(|| {
            start.wait();
            split
                .iter()
                .filter(|(_, handle)| {
                    matches!(handle.cancel(), CancelOutcome::AlreadyCommitted { .. })
                })
                .count()
        });
        start.wait();
        (commit.join().unwrap(), cancel.join().unwrap())
    });
    assert_eq!(commit_wins, cancel_saw_committed);
    (commit_wins, split.len() - commit_wins)
}

fn run_commit_cancel_race(reservations: &[Reservation], now: Timestamp) -> (usize, usize) {
    let start = Barrier::new(3);
    let (commit_wins, cancel_saw_committed) = std::thread::scope(|scope| {
        let commit = scope.spawn(|| {
            start.wait();
            reservations
                .iter()
                .filter(|reservation| {
                    reservation
                        .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                        .is_ok()
                })
                .count()
        });
        let cancel = scope.spawn(|| {
            start.wait();
            reservations
                .iter()
                .filter(|reservation| {
                    matches!(reservation.cancel(), CancelOutcome::AlreadyCommitted { .. })
                })
                .count()
        });
        start.wait();
        (commit.join().unwrap(), cancel.join().unwrap())
    });
    assert_eq!(commit_wins, cancel_saw_committed);
    (commit_wins, reservations.len() - commit_wins)
}

fn run_atomic_race(phases: &[AtomicU8]) -> (usize, usize) {
    let start = Barrier::new(3);
    let (first_wins, second_wins) = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            start.wait();
            phases
                .iter()
                .filter(|phase| {
                    phase
                        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                })
                .count()
        });
        let second = scope.spawn(|| {
            start.wait();
            phases
                .iter()
                .filter(|phase| {
                    phase
                        .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                })
                .count()
        });
        start.wait();
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_eq!(first_wins + second_wins, phases.len());
    (first_wins, second_wins)
}

fn bench_reservation(c: &mut Criterion) {
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let lease = big_lease();
    let mut group = c.benchmark_group("reservation");

    // The opt-in shared cancel state (#93): split, then commit through the
    // shared reservation as a worker thread would. One `Arc` and one extra
    // indirection over the unsplit `lease/reserve_commit` path, which stays
    // the allocation-free comparison a consumer gets when it does not need a
    // timeout/worker race.
    group.bench_function("commit_split", |b| {
        b.iter(|| {
            let (shared, handle) = Reservation::reserve(black_box(&lease), CostUnits(1), now)
                .unwrap()
                .split();
            shared
                .reservation()
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap();
            black_box(handle.is_cancelled());
            (shared, handle)
        })
    });
    // The contended half of #93's "uncontended and contended" requirement: a
    // worker committing while a waiter cancels, over the same phase word.
    // `commit_cancel_race_control_2` is the raw-atomic control it is measured
    // against.
    group.bench_function("commit_split_race_contended_2", |b| {
        b.iter_batched(
            || {
                (0..RACE_BATCH)
                    .map(|_| {
                        Reservation::reserve(&lease, CostUnits(1), now)
                            .unwrap()
                            .split()
                    })
                    .collect::<Vec<_>>()
            },
            |split| black_box(run_split_cancel_race(&split, now)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("cancel_after_commit", |b| {
        b.iter_batched(
            || Reservation::reserve(&lease, CostUnits(1), now).unwrap(),
            |reservation| {
                reservation
                    .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                    .unwrap();
                black_box(reservation.cancel())
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("commit_after_cancel", |b| {
        b.iter_batched(
            || Reservation::reserve(&lease, CostUnits(1), now).unwrap(),
            |reservation| {
                assert_eq!(reservation.cancel(), CancelOutcome::ZeroCharged);
                assert_eq!(
                    black_box(reservation.commit_at_execution_start(now, CommitFunding::LeaseOnly)),
                    Err(CommitError::AlreadyReleased)
                );
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Elements(RACE_BATCH as u64));
    group.bench_function("commit_cancel_race_control_2", |b| {
        b.iter_batched(
            || {
                (0..RACE_BATCH)
                    .map(|_| AtomicU8::new(0))
                    .collect::<Vec<_>>()
            },
            |phases| black_box(run_atomic_race(&phases)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function("commit_cancel_race_contended_2", |b| {
        b.iter_batched(
            || pending_batch(&lease, now),
            |reservations| black_box(run_commit_cancel_race(&reservations, now)),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_cost_table,
    bench_snapshot,
    bench_lease,
    bench_reservation
);
criterion_main!(benches);
