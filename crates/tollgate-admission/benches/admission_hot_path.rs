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
//! The `distinct_accounts` pair separates account-local contention from the
//! engine-global counter line that #99 will remove. `full_check_denied`
//! measures the refusal path, which none of the others take.

use std::hint::black_box;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use criterion::{Criterion, criterion_group, criterion_main};
use jiff::Timestamp;

use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, CapacityGate, ExecutionCapacityGate,
    ExecutionCapacityMode, LeaseSlot, MokaSnapshotMap, NoGate, Pending, Principal,
    PublishableSnapshotUpdate, SnapshotMap,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, EnforcementMode, FencingToken,
    Generation, KeyId, LeaseGrant, LeaseId, LocalLease, LocalSharding, Locality, OpIndex,
    PermissionBits, PublishableSnapshot, RequestId, ResolvedLimits, UsageEvent, UsageSlot,
};

#[derive(Debug)]
struct BenchUsageSlot;

impl UsageSlot for BenchUsageSlot {
    fn record(self, _event: UsageEvent) {}
}

/// Unlike the cancellation fixtures, this sink makes the complete billing
/// event observable, including the copy performed when the guard emits it.
#[derive(Debug)]
struct EmissionSlot;

impl UsageSlot for EmissionSlot {
    fn record(self, event: UsageEvent) {
        black_box(event);
    }
}

fn bench_commit_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    for source in ["leased", "overage", "fallback"] {
        for key in [None, Some(KeyId(7))] {
            for split in [false, true] {
                let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
                let mut snapshot = (*contention_snapshot()).clone();
                snapshot.key_id = key;
                if source != "leased" {
                    snapshot.enforcement_mode = EnforcementMode::Elastic {
                        overage_cap: CostUnits(u64::MAX),
                    };
                }
                let slot = LeaseSlot::for_account(AccountId(1));
                if source != "overage" {
                    let expires_at = if source == "fallback" {
                        now.checked_add(jiff::SignedDuration::from_secs(1)).unwrap()
                    } else {
                        far_future()
                    };
                    drop(slot.replace(Arc::new(LocalLease::new(
                        LeaseGrant {
                            lease_id: LeaseId(7),
                            account_id: AccountId(1),
                            fencing_token: FencingToken(1),
                            units: CostUnits(u64::MAX / 2),
                            expires_at,
                        },
                        CostUnits::ZERO,
                    ))));
                }
                engine
                    .map()
                    .install_publishable(
                        Principal(1),
                        PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
                        slot,
                    )
                    .unwrap();
                let commit_at = if source == "fallback" {
                    now.checked_add(jiff::SignedDuration::from_secs(1)).unwrap()
                } else {
                    now
                };
                let key_name = if key.is_some() { "key" } else { "unattributed" };
                let ownership = if split { "split" } else { "owned" };
                group.bench_function(
                    format!("commit_usage_{source}_{key_name}_{ownership}"),
                    |b| {
                        b.iter(|| {
                            let ready = engine
                                .begin(Principal(1), PermissionBits::bit(0), now)
                                .unwrap()
                                .admit(&[(PriceOp, 1)], EmissionSlot, now)
                                .unwrap()
                                .acquire_capacity(&NoGate)
                                .unwrap();
                            if split {
                                let (ready, handle) = ready.split();
                                drop(ready.commit(RequestId(1), commit_at).unwrap());
                                black_box(handle.is_cancelled());
                            } else {
                                drop(ready.commit(RequestId(1), commit_at).unwrap());
                            }
                        });
                    },
                );
            }
        }
    }
    group.finish();
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

fn snapshot() -> Arc<AccountSnapshot> {
    Arc::new(
        AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            far_future(),
            PermissionBits::bit(0),
            ResolvedLimits::new(4_096).with_weighted_rate(
                // Effectively unlimited so the bench measures mechanism cost,
                // not deny paths.
                u64::from(u32::MAX),
                u64::from(u32::MAX),
            ),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    )
}

/// The sustained contention fixture keeps the weighted-token mechanism in
/// the measured path, but quotes one unit so it cannot consume governor's
/// maximum bucket faster than its one-nanosecond replenishment quantum on a
/// fast CI host. The regular single-thread and refusal fixtures retain their
/// original 114-unit quote, preserving their recorded baseline.
fn contention_snapshot() -> Arc<AccountSnapshot> {
    let mut snapshot = (*snapshot()).clone();
    snapshot.cost_table = Arc::new(
        CostTable::builder(CostUnits(1), CostUnits(1))
            .weight(&PriceOp, CostUnits(0))
            .build(),
    );
    Arc::new(snapshot)
}

/// The same snapshot under a named account, so a benchmark can put N
/// principals across N accounts rather than all under `AccountId(1)`.
fn snapshot_for_account(account: u128) -> Arc<AccountSnapshot> {
    let mut snapshot = (*contention_snapshot()).clone();
    snapshot.account_id = AccountId(account);
    Arc::new(snapshot)
}

fn big_lease(sharding: LocalSharding) -> Arc<LocalLease> {
    big_lease_for(AccountId(1), sharding)
}

fn big_lease_for(account_id: AccountId, sharding: LocalSharding) -> Arc<LocalLease> {
    Arc::new(LocalLease::with_sharding(
        LeaseGrant {
            lease_id: LeaseId(account_id.0 + 7),
            account_id,
            fencing_token: FencingToken(1),
            units: CostUnits(u64::MAX / 2),
            expires_at: far_future(),
        },
        CostUnits::ZERO,
        jiff::SignedDuration::ZERO,
        sharding,
    ))
}

/// A live lease with nothing left: the `LeaseExhausted` case, and the one
/// elastic mode is most often reached through.
fn empty_lease() -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits::ZERO,
            expires_at: far_future(),
        },
        CostUnits::ZERO,
    ))
}

fn populate(map: &impl SnapshotMap) {
    // A realistic working set: the benched principal among hundreds.
    let sharding = map.local_sharding();
    for i in 0..512u128 {
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(big_lease(sharding)));
        map.install_publishable(
            Principal(i),
            PublishableSnapshot::try_new(snapshot()).unwrap(),
            slot,
        )
        .unwrap();
    }
}

fn populate_contention(map: &impl SnapshotMap) {
    let sharding = map.local_sharding();
    for i in 0..512u128 {
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(big_lease(sharding)));
        map.install_publishable(
            Principal(i),
            PublishableSnapshot::try_new(contention_snapshot()).unwrap(),
            slot,
        )
        .unwrap();
    }
}

fn populate_with_limits(map: &impl SnapshotMap, limits: ResolvedLimits) {
    let sharding = map.local_sharding();
    let mut configured = (*contention_snapshot()).clone();
    configured.limits = limits;
    let configured = PublishableSnapshot::try_new(Arc::new(configured)).unwrap();
    for i in 0..512u128 {
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(big_lease(sharding)));
        map.install_publishable(Principal(i), configured.clone(), slot)
            .unwrap();
    }
}

fn populate_distinct(map: &impl SnapshotMap) {
    let sharding = map.local_sharding();
    for i in 0..512u128 {
        let account_id = AccountId(i + 1);
        let slot = LeaseSlot::with_sharding(account_id, sharding);
        drop(slot.replace(big_lease_for(account_id, sharding)));
        map.install_publishable(
            Principal(i),
            PublishableSnapshot::try_new(snapshot_for_account(account_id.0)).unwrap(),
            slot,
        )
        .unwrap();
    }
}

/// Moka's configured bound in `bench_lookup`, and the size of the at-capacity
/// working set below: exactly `max_capacity` principals resident, because that
/// is the largest legitimate population for a cache built with it. A power of
/// two, so rotating through it costs a mask rather than a division.
const AT_CAPACITY: usize = 4_096;
const AT_CAPACITY_MASK: usize = AT_CAPACITY - 1;

/// A fixed permutation of `0..AT_CAPACITY`, shared by both maps so the two
/// at-capacity rows differ in their map and in nothing else.
///
/// The stride is odd, so `i -> i * STRIDE mod 2^12` is a bijection: every
/// principal is visited exactly once per rotation, in an order no hardware
/// prefetcher walks ahead of, and without a random number generator whose seed
/// would be one more thing the two rows could disagree about.
fn at_capacity_working_set() -> Vec<Principal> {
    const STRIDE: usize = 2_654_435_761;
    let order: Vec<Principal> = (0..AT_CAPACITY)
        .map(|i| Principal((i.wrapping_mul(STRIDE) & AT_CAPACITY_MASK) as u128))
        .collect();
    let mut seen = vec![false; AT_CAPACITY];
    for principal in &order {
        let index = principal.0 as usize;
        assert!(!seen[index], "the stride must permute the working set");
        seen[index] = true;
    }
    order
}

/// `populate`'s sibling rather than a parameter on it: five other fixtures are
/// baselined at 512 principals and must not move.
///
/// One batch instead of a loop of installs, because the copy-on-write map
/// clones the whole map per install — 4,096 individual installs is the O(N^2)
/// shape `install_many` exists to avoid. One shared `LeaseSlot`, because these
/// principals share `AccountId(1)` and a slot is per account; the lookup path
/// never reads it.
fn populate_working_set(map: &impl SnapshotMap, working_set: &[Principal]) {
    let sharding = map.local_sharding();
    let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
    drop(slot.replace(big_lease(sharding)));
    let snapshot = PublishableSnapshot::try_new(snapshot()).unwrap();
    map.apply_publishable_many(
        working_set
            .iter()
            .map(|principal| PublishableSnapshotUpdate::Present {
                principal: *principal,
                snapshot: snapshot.clone(),
                lease: Arc::clone(&slot),
            })
            .collect(),
    )
    .unwrap();
}

/// Read the whole working set `rounds` times and report the hits.
///
/// Outside every timed loop, and it does two jobs. It warms: one rotation is
/// sixty-four times moka's read-log flush point, so the first housekeeper
/// drain, the frequency sketch's enablement and crossbeam-epoch's lazy init
/// are all behind us before a sample is taken. And it probes: the timed loops
/// take no branch on a miss, so this is what makes a fixture that silently
/// lost entries fail loudly instead of being measured — a moka miss is
/// *cheaper* than a hit, so a broken fixture would otherwise report a faster
/// moka rather than a broken benchmark.
fn sweep(map: &impl SnapshotMap, working_set: &[Principal], rounds: usize) -> usize {
    let mut hits = 0;
    for _ in 0..rounds {
        for principal in working_set {
            if black_box(map.get(black_box(principal))).is_some() {
                hits += 1;
            }
        }
    }
    hits
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

    // The pair above reads one principal out of 512 in a cache sized 4,096: a
    // permanently resident key in a cache one-eighth full, where moka evicts
    // nothing and — below half of `max_capacity` — does not enable its
    // frequency sketch at all. The pair below holds exactly `max_capacity`
    // principals and rotates across all of them, which is the configuration a
    // bounded cache exists for. Both walk the same permutation of the same
    // working set, so the ratio between them is a statement about the two maps
    // and nothing else.
    //
    // It is a full-cache *read* measurement, not an eviction measurement:
    // `get` never inserts, so nothing in the timed loop can push the cache
    // over capacity. What it prices over the pair above is the enabled sketch,
    // full access-order deques, and a working set past L1.
    let working_set = at_capacity_working_set();

    let arc_swap_full = ArcSwapSnapshotMap::new();
    populate_working_set(&arc_swap_full, &working_set);
    let moka_full = MokaSnapshotMap::new(AT_CAPACITY as u64);
    populate_working_set(&moka_full, &working_set);

    assert_eq!(
        sweep(&arc_swap_full, &working_set, 4),
        4 * AT_CAPACITY,
        "the arc-swap fixture must hold the whole working set"
    );
    assert_eq!(
        sweep(&moka_full, &working_set, 4),
        4 * AT_CAPACITY,
        "moka must hold all {AT_CAPACITY} principals before it is timed"
    );

    group.bench_function("snapshot_lookup_arc_swap_at_capacity", |b| {
        let mut cursor = 0usize;
        b.iter(|| {
            let hit = black_box(arc_swap_full.get(black_box(&working_set[cursor]))).is_some();
            // A true data dependency on the result, without perturbing the
            // order: `hit` is 1 on every iteration, so the walk is exactly the
            // permutation, but the address of the next load cannot be computed
            // until this one has landed. That keeps this a latency measurement
            // like the pair above rather than letting the core overlap
            // independent lookups, and it is also why the rotation cannot be
            // hoisted out of the loop.
            cursor = (cursor + usize::from(hit)) & AT_CAPACITY_MASK;
        });
    });
    group.bench_function("snapshot_lookup_moka_at_capacity", |b| {
        let mut cursor = 0usize;
        b.iter(|| {
            let hit = black_box(moka_full.get(black_box(&working_set[cursor]))).is_some();
            cursor = (cursor + usize::from(hit)) & AT_CAPACITY_MASK;
        });
    });

    // Afterwards as well as before: nothing in the timed loop writes, so
    // nothing should have evicted, and this is what says so rather than
    // assuming it.
    assert_eq!(
        sweep(&moka_full, &working_set, 1),
        AT_CAPACITY,
        "moka evicted during a read-only benchmark"
    );

    group.finish();
}

fn engine(sharding: LocalSharding) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
    populate(engine.map());
    engine
}

fn distinct_engine(sharding: LocalSharding) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
    populate_distinct(engine.map());
    engine
}

fn contention_engine(sharding: LocalSharding) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
    populate_contention(engine.map());
    engine
}

fn engine_with_limits(limits: ResolvedLimits) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    populate_with_limits(engine.map(), limits);
    engine
}

fn admit_once(engine: &AdmissionEngine<ArcSwapSnapshotMap>, principal: Principal, now: Timestamp) {
    let admitted = staged_admission(engine, principal, 64, now).unwrap();
    // Cancel instead of commit so the giant lease never drains during a run.
    black_box(admitted.cancel());
}

/// `admit_once` plus the capacity gate, then cancel — the same shape, one
/// stage further along. Cancelling from `ReadyToStart` releases the permit and
/// the lease debit together, so a sustained run neither drains the lease nor
/// leaks capacity.
fn admit_and_start_once<G: CapacityGate>(
    engine: &AdmissionEngine<ArcSwapSnapshotMap>,
    gate: &G,
    principal: Principal,
    now: Timestamp,
) {
    let admitted = staged_admission(engine, principal, 64, now).unwrap();
    let ready = admitted.acquire_capacity(gate).unwrap();
    black_box(ready.cancel());
}

fn staged_admission(
    engine: &AdmissionEngine<ArcSwapSnapshotMap>,
    principal: Principal,
    items: u64,
    now: Timestamp,
) -> Result<Pending<BenchUsageSlot>, tollgate_core::DenyReason> {
    engine
        .begin(principal, PermissionBits::bit(0), now)
        .and_then(|context| context.admit(&[(PriceOp, items)], BenchUsageSlot, now))
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

/// Advance the process-global locality counter until the next thread to take
/// a number will land one shard past the foreground thread.
///
/// A probe consumes exactly one number and reports where it landed, which is
/// the only way to read a counter that exposes no reader. Probes are taken
/// one at a time and nothing else in the process is running by the time a
/// fixture is built — the previous fixture's contenders are stopped and
/// joined in `Contended::drop` — so the indices walk consecutively and one of
/// them is the foreground's within a single cycle. The bound is generous
/// rather than exact because a fixture that could not align must say so
/// instead of timing a mapping it did not arrange.
fn align_next_locality_after(sharding: LocalSharding, foreground: usize) {
    for _ in 0..(4 * sharding.get()) {
        let landed = std::thread::spawn(move || Locality::current().index(sharding))
            .join()
            .expect("a locality probe thread has nothing to panic on");
        if landed == foreground {
            return;
        }
    }
    panic!(
        "could not align the locality counter onto shard {foreground} of {}: something else in \
         this process is consuming localities, and the contended fixtures cannot pin their \
         thread-to-shard mapping while it does",
        sharding.get()
    );
}

fn spawn_contenders(
    engine: AdmissionEngine<ArcSwapSnapshotMap>,
    principals: impl IntoIterator<Item = Principal>,
) -> Contended {
    let sharding = engine.map().local_sharding();
    spawn_contenders_with(engine, sharding, principals, admit_once)
}

/// The background load is a parameter because #99's capacity fixtures need
/// their contenders to go through the gate too: measuring a gated foreground
/// request against ungated background threads would price the gate's
/// uncontended path and call it contention. The existing `admit_once`
/// contenders are left exactly as they were — their manifest rows are
/// calibrated against background threads that do not call a gate, and
/// silently changing the load would redefine those baselines.
///
/// `sharding` is the layout the fixture's threads are spread across, passed
/// rather than read from the engine because the two are independent axes: the
/// capacity rows run a single-counter map against an eight-way sharded
/// *pool*, and it is the pool the threads must spread across there.
///
/// # Pinning the thread-to-shard mapping
///
/// [`Locality`] is assigned to a thread on its *first access*, not at spawn,
/// from one process-global counter. Which shard each thread of a contended
/// fixture lands on therefore depends on how many threads earlier benchmarks
/// in the same Criterion process already consumed — and the foreground thread
/// took its number in an earlier row, at an unrelated position in that
/// counter, so it is not adjacent to the contenders spawned here.
///
/// Left to chance that made two rows measure the lottery rather than the
/// sharded path (#123). Whether one contender happened to share the
/// foreground thread's shard moved `full_check_contended_8_sharded` between
/// 176 ns and 862 ns on a 24-core x86_64 host — a 4.9x step with no overlap,
/// recurring with period 8 as the counter's offset walked — and the
/// `distinct_accounts` row between 172 ns and 238 ns. Seven offsets in eight
/// collide, so the rows usually measured a collision and occasionally did
/// not, and their recorded medians sat between two values neither row ever
/// produced.
///
/// So the counter is aligned before the contenders are spawned, putting them
/// on the shards the foreground thread does not occupy, and the mapping they
/// actually realized is asserted before anything is timed. Pinning it is the
/// point: these rows exist to price the opt-in layout working as designed,
/// and question 2 of #123 — whether a deployment's threads map as cleanly —
/// is a property of the deployment, not something a benchmark can average
/// its way to. Asserting it is the same discipline `sweep` applies to the
/// at-capacity maps: a fixture that silently stopped being the thing the row
/// names fails loudly instead of being measured.
///
/// The readiness barrier comes with the assertion and is worth having on its
/// own — it is what makes the contenders provably running, rather than
/// probably running because Criterion warms up first.
fn spawn_contenders_with(
    engine: AdmissionEngine<ArcSwapSnapshotMap>,
    sharding: LocalSharding,
    principals: impl IntoIterator<Item = Principal>,
    work: impl Fn(&AdmissionEngine<ArcSwapSnapshotMap>, Principal, Timestamp) + Clone + Send + 'static,
) -> Contended {
    let now = Timestamp::from_second(1_755_600_000).unwrap();
    let principals: Vec<Principal> = principals.into_iter().collect();

    let foreground = Locality::current().index(sharding);
    align_next_locality_after(sharding, foreground);

    let engine = Arc::new(engine);
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(principals.len() + 1));
    let occupied = Arc::new(Mutex::new(vec![foreground]));
    let workers: Vec<_> = principals
        .into_iter()
        .map(|principal| {
            let engine = Arc::clone(&engine);
            let stop = Arc::clone(&stop);
            let ready = Arc::clone(&ready);
            let occupied = Arc::clone(&occupied);
            let work = work.clone();
            std::thread::spawn(move || {
                // Taken here rather than inside the loop so the number this
                // thread reports is the one it then runs on: `Locality` is
                // assigned once per thread and never moves.
                let shard = Locality::current().index(sharding);
                occupied
                    .lock()
                    .expect("no contender panics holding this")
                    .push(shard);
                ready.wait();
                while !stop.load(Ordering::Relaxed) {
                    work(&engine, principal, now);
                }
            })
        })
        .collect();
    ready.wait();

    // Built before the mapping is judged, so a fixture that failed to arrange
    // the one it wanted still stops and joins its contenders as it unwinds
    // rather than leaving them spinning on a benchmark nobody is running.
    let contended = Contended {
        engine,
        stop,
        workers,
    };

    let mut occupied = occupied
        .lock()
        .expect("every contender released this before the barrier")
        .clone();
    let threads = occupied.len();
    occupied.sort_unstable();
    occupied.dedup();
    // As widely as the layout allows: one shard each while there are shards
    // to go round, and every shard occupied once there are not. Stated
    // against the arithmetic so it holds for the single-counter rows too,
    // where all eight threads share shard zero by construction, rather than
    // as a second assertion that could disagree with this one.
    assert_eq!(
        occupied.len(),
        threads.min(sharding.get()),
        "{threads} contended-fixture threads on {} shard(s) must occupy {} of \
         them, not {occupied:?}",
        sharding.get(),
        threads.min(sharding.get()),
    );

    contended
}

fn contended_engine(sharding: LocalSharding) -> Contended {
    spawn_contenders(
        contention_engine(sharding),
        std::iter::repeat_n(Principal(97), 7),
    )
}

fn distinct_account_contended_engine(sharding: LocalSharding) -> Contended {
    // Principal 0 is reserved for the measured foreground request. The seven
    // background workers each hit a different account, so any shared line or
    // lock here is engine-global rather than account-local.
    spawn_contenders(distinct_engine(sharding), (1..=7).map(Principal))
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
    group.bench_function("begin", |b| {
        b.iter(|| {
            black_box(
                uncontended
                    .begin(black_box(Principal(97)), PermissionBits::bit(0), now)
                    .unwrap(),
            )
        })
    });
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

    // Eight simultaneous requests spread across eight accounts. The current
    // engine-global AdmissionCounters still bounce one cache line here; #99
    // moves them behind the map and must improve this preserved baseline.
    {
        let contended = distinct_account_contended_engine(LocalSharding::SINGLE);
        group.bench_function("full_check_contended_8_distinct_accounts", |b| {
            b.iter(|| admit_once(contended.engine(), black_box(Principal(0)), now))
        });
    }
    {
        let contended = distinct_account_contended_engine(sharded);
        group.bench_function("full_check_contended_8_distinct_accounts_sharded", |b| {
            b.iter(|| admit_once(contended.engine(), black_box(Principal(0)), now))
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
            let denied = staged_admission(&uncontended, black_box(Principal(9_999)), 64, now);
            black_box(denied.unwrap_err())
        })
    });

    // The lease-exhausted path, which nothing measured before #1: `admit_once`
    // deliberately uses a giant lease and cancels, so the quota step's failure
    // branch never ran under a benchmark. Elastic mode adds a branch there, so
    // it would otherwise land on the one path the perf gate does not watch.
    //
    // Two functions, one shape. `strict` is the regression guard — the added
    // match must not cost a strict account anything — and `elastic` is what
    // the new work actually costs, an atomic compare-exchange on a counter
    // that is warm in the same cache line as the slot the lookup just read.
    let strict = exhausted_engine(EnforcementMode::Strict);
    group.bench_function("full_check_lease_exhausted_strict", |b| {
        b.iter(|| {
            let denied = staged_admission(&strict, black_box(Principal(97)), 64, now);
            black_box(denied.unwrap_err())
        })
    });

    let elastic = exhausted_engine(EnforcementMode::Elastic {
        overage_cap: CostUnits(u64::MAX),
    });
    group.bench_function("full_check_lease_exhausted_elastic", |b| {
        b.iter(|| {
            let admitted = staged_admission(&elastic, black_box(Principal(97)), 64, now).unwrap();
            // Cancel, as `admit_once` does, so the cap never drains: this
            // measures the debit-and-refund pair, not a one-shot admission.
            black_box(admitted.cancel());
        })
    });

    // The two #91 mechanism witnesses keep the rest of the pipeline
    // identical and disable the weighted bucket, isolating the added mutable
    // state. The request bucket is uncontended; the concurrency gauge uses the
    // same sustained eight-thread harness as the existing contention gates.
    let request_rate = engine_with_limits(ResolvedLimits::new(4_096).with_request_rate(
        NonZeroU32::new(u32::MAX).unwrap(),
        NonZeroU32::new(u32::MAX).unwrap(),
    ));
    group.bench_function("request_rate_token", |b| {
        b.iter(|| admit_once(&request_rate, black_box(Principal(97)), now))
    });

    {
        let limits = ResolvedLimits::new(4_096)
            .with_concurrency(NonZeroU32::new(u32::MAX).unwrap(), None)
            .unwrap();
        let contended = spawn_contenders(
            engine_with_limits(limits),
            std::iter::repeat_n(Principal(97), 7),
        );
        group.bench_function("concurrency_acquire", |b| {
            b.iter(|| admit_once(contended.engine(), black_box(Principal(97)), now))
        });
    }

    group.finish();
}

/// Execution capacity (#99), gated as its own `capacity` group.
///
/// The group exists so a product that selects `Disabled` is never charged for
/// a feature it did not enable, and so the enabled modes are priced
/// separately rather than averaged into one "capacity" number.
///
/// `disabled` is deliberately `admission/full_check` plus one call to
/// `acquire_capacity`, on the same engine and the same fixture. `full_check`
/// does not reach the gate at all, so it is a genuine denominator rather than
/// a second measurement of the same path — the trap #102 removed. The
/// manifest ratio between them is what holds `Disabled` to costing nothing.
fn bench_capacity(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity");
    let now = Timestamp::from_second(1_755_600_000).unwrap();

    let sharded = LocalSharding::new(NonZeroUsize::new(8).unwrap());
    let enabled = |mode| {
        ExecutionCapacityGate::new(mode, LocalSharding::SINGLE)
            .expect("a valid capacity configuration")
            .expect("an enabled mode builds a gate")
    };
    let units = |units: u32| NonZeroU32::new(units).unwrap();

    // The same engine and the same 114-unit quote as `admission/full_check`,
    // so the only difference between that row and `capacity/disabled` is the
    // gate call itself.
    let uncontended = engine(LocalSharding::SINGLE);
    group.bench_function("disabled", |b| {
        b.iter(|| admit_and_start_once(&uncontended, &NoGate, black_box(Principal(97)), now))
    });

    let uniform = enabled(ExecutionCapacityMode::Uniform {
        total: units(4_096),
    });
    group.bench_function("uniform", |b| {
        b.iter(|| admit_and_start_once(&uncontended, &uniform, black_box(Principal(97)), now))
    });

    // Reserved with room in shared: the path essentially every request under
    // this mode takes, and the one the reserve is supposed to leave alone.
    let reserved = enabled(ExecutionCapacityMode::Reserved {
        total: units(4_096),
        assured_reserve: units(64),
    });
    assert_eq!(
        reserved.occupancy().shared_available,
        4_096 - 64,
        "reserved_shared must measure an acquisition shared can serve"
    );
    group.bench_function("reserved_shared", |b| {
        b.iter(|| admit_and_start_once(&uncontended, &reserved, black_box(Principal(97)), now))
    });

    // Reserved with shared exhausted: the assured fallback, which is the only
    // path in the system where a class changes an outcome. Shared is one unit
    // and a single admission holds it for the whole benchmark, so every
    // measured acquisition fails the shared compare-exchange and reaches the
    // reserve. Saturating through a real admission rather than a fabricated
    // permit is not incidental: `CapacityEvidence` has no public constructor,
    // which is exactly the property that stops caller input claiming a class.
    {
        let saturated = enabled(ExecutionCapacityMode::Reserved {
            total: units(2),
            assured_reserve: units(1),
        });
        let holding_shared = staged_admission(&uncontended, Principal(97), 64, now)
            .unwrap()
            .acquire_capacity(&saturated)
            .unwrap();
        // A fixture that silently measured the shared path would be
        // indistinguishable from `reserved_shared` and would report the
        // fallback as free. The pool says which path this is.
        assert_eq!(
            saturated.occupancy().shared_available,
            0,
            "reserved_fallback must measure the reserve, not a shared pool that still has room"
        );
        group.bench_function("reserved_fallback", |b| {
            b.iter(|| admit_and_start_once(&uncontended, &saturated, black_box(Principal(97)), now))
        });
        drop(holding_shared);
    }

    // Eight simultaneous requests across eight accounts, every one of them
    // through the gate, against `admission/full_check_contended_8_distinct_
    // accounts` as the same-workload denominator.
    //
    // The map stays on its shipped default single-counter topology, matching
    // that denominator exactly. The *pool* is sharded, because the two are
    // independent axes: the map partitions per-account state, while the gate
    // is one instance-global pool every request of every account touches, so
    // sharding it is not an opt-in layout but the configuration it is meant
    // to run in — the reason #99 sharded it at all. The unsharded pool is
    // measured too, and recorded in `docs/DESIGN.md` as what the sharding
    // buys rather than gated as a topology nobody should deploy.
    for (label, mode) in [
        (
            "full_check_contended_8_distinct_accounts_uniform",
            ExecutionCapacityMode::Uniform {
                total: units(4_096),
            },
        ),
        (
            "full_check_contended_8_distinct_accounts_reserved",
            ExecutionCapacityMode::Reserved {
                total: units(4_096),
                assured_reserve: units(64),
            },
        ),
    ] {
        let gate = Arc::new(
            ExecutionCapacityGate::new(mode, sharded)
                .expect("a valid capacity configuration")
                .expect("an enabled mode builds a gate"),
        );
        let background = Arc::clone(&gate);
        // Principal 0 stays reserved for the foreground request, as in the
        // ungated pair, so what is measured is engine-global contention.
        // Pinned against the *pool's* layout, not the map's: the map is on
        // its shipped single-counter default here, and the eight-way axis
        // these threads must spread across is the gate they all acquire from.
        let contended = spawn_contenders_with(
            distinct_engine(LocalSharding::SINGLE),
            sharded,
            (1..=7).map(Principal),
            move |engine, principal, now| {
                admit_and_start_once(engine, background.as_ref(), principal, now);
            },
        );
        group.bench_function(label, |b| {
            b.iter(|| {
                admit_and_start_once(
                    contended.engine(),
                    gate.as_ref(),
                    black_box(Principal(0)),
                    now,
                )
            })
        });
    }

    group.finish();
}

/// A populated map whose benched principal holds a live but *empty* lease, so
/// every admission reaches the quota step and fails it.
fn exhausted_engine(mode: EnforcementMode) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    let mut snapshot = AccountSnapshot::clone(&snapshot());
    snapshot.enforcement_mode = mode;
    let snapshot = Arc::new(snapshot);
    for i in 0..512u128 {
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(empty_lease()));
        engine
            .map()
            .install(Principal(i), Arc::clone(&snapshot), slot)
            .unwrap();
    }
    engine
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
                let slot = LeaseSlot::for_account(AccountId(1));
                (Principal(i), snapshot(), slot)
            })
            .collect::<Vec<_>>()
    };
    group.bench_function("install_loop_512_arc_swap", |b| {
        b.iter_batched(
            || (ArcSwapSnapshotMap::new(), entries()),
            |(map, entries)| {
                for (principal, snapshot, lease) in entries {
                    map.install(principal, snapshot, lease).unwrap();
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
                map.install_many(entries).unwrap();
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
                .map(|i| {
                    (
                        Principal(i),
                        snapshot_for_account(i),
                        LeaseSlot::for_account(AccountId(i)),
                    )
                })
                .collect::<Vec<_>>()
        };
        group.bench_function(format!("install_many_{accounts}_distinct_accounts"), |b| {
            b.iter_batched(
                || (ArcSwapSnapshotMap::new(), distinct()),
                |(map, entries)| {
                    map.install_many(entries).unwrap();
                    map
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.bench_function("install_loop_512_distinct_accounts", |b| {
        let distinct = || {
            (0..512u128)
                .map(|i| {
                    (
                        Principal(i),
                        snapshot_for_account(i),
                        LeaseSlot::for_account(AccountId(i)),
                    )
                })
                .collect::<Vec<_>>()
        };
        b.iter_batched(
            || (ArcSwapSnapshotMap::new(), distinct()),
            |(map, entries)| {
                for (principal, snapshot, lease) in entries {
                    map.install(principal, snapshot, lease).unwrap();
                }
                map
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_lookup,
    bench_full_check,
    bench_capacity,
    bench_bulk_install,
    bench_commit_usage
);
criterion_main!(benches);
