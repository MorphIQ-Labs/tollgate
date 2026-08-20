//! Hot-path microbenchmarks, gated by `scripts/check_perf_thresholds.sh`
//! against `testing/perf_thresholds.json`.
//!
//! Benchmark ids are part of the gate contract: renaming a group or function
//! here requires the matching manifest change.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use jiff::Timestamp;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    KeyId, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Reservation, ResolvedLimits,
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

fn cost_table() -> Arc<CostTable> {
    Arc::new(
        CostTable::builder(CostUnits(50), CostUnits(50))
            .weight(&Op::Price, CostUnits(1))
            .weight(&Op::Greeks, CostUnits(5))
            .build(),
    )
}

fn snapshot() -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId(1),
        key_id: Some(KeyId(2)),
        generation: Generation(1),
        status: AccountStatus::Active,
        valid_until: Timestamp::from_second(4_102_444_800).unwrap(), // 2100-01-01
        permissions: PermissionBits::bit(0).union(PermissionBits::bit(1)),
        limits: ResolvedLimits {
            max_items_per_request: 1024,
            rate_units_per_second: 100_000,
            rate_burst_units: 500_000,
        },
        cost_table: cost_table(),
    }
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

fn bench_cost_table(c: &mut Criterion) {
    let table = cost_table();
    let mut group = c.benchmark_group("cost_table");
    group.bench_function("quote", |b| {
        b.iter(|| table.quote(black_box(&Op::Greeks), black_box(64)).unwrap())
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
            r.commit_at_execution_start(now).unwrap();
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
    group.finish();
}

criterion_group!(benches, bench_cost_table, bench_snapshot, bench_lease);
criterion_main!(benches);
