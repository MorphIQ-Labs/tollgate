//! Execution-capacity admission: whether this instance should *start* an
//! already-valid request with the compute it has right now (GL-99).
//!
//! A different question from funding, and the separation is the point. Quota
//! answers whether an account may pay for work; this answers whether the
//! machine can afford to begin it. A best-effort flood must not consume the
//! capacity kept for assured traffic, and assured traffic must be able to use
//! the whole instance when best-effort traffic is absent.
//!
//! # Startup composition, not a request-path branch
//!
//! [`ExecutionCapacityMode`] is a *configuration* value that selects a gate
//! **type**; the service monomorphizes its request stack over that type once,
//! at startup. A single runtime enum matched inside every request could not
//! honestly promise that a product which disables the feature pays nothing for
//! it — there would always be a branch. [`NoGate`] is zero-sized and its
//! `acquire` is an `Ok` with no state to touch, so disabled really is absent.
//!
//! # Why the pools are sharded
//!
//! This gate is global to the instance: unlike a lease or a rate bucket, every
//! request of every account touches it. A single atomic would put every core on
//! one cache line at exactly the moment the feature exists to handle. The
//! recorded evidence for that failure mode is in this workspace — the
//! same-account admission path measures ~103 ns uncontended and 2.74–2.80 µs
//! under eight-way contention, which locality sharding brings back to
//! ~446–618 ns.
//!
//! So a pool is partitioned across cache-isolated shards exactly as
//! [`LocalLease`](tollgate_core::LocalLease) partitions a grant: shard sums
//! equal the pool by construction, a request takes one unit from its sticky
//! local shard, and a saturated local shard falls back to siblings before
//! refusing. Conservation is therefore structural rather than checked.
//!
//! Shard count is capped at the pool's unit count. Splitting eight units across
//! ten localities would leave shards holding zero, and every acquisition would
//! degrade to a full sibling scan — the fast path never fast.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tollgate_core::{CapacityClass, DenyReason, Generation, LocalSharding, Locality};

mod private {
    pub trait Sealed {}
}

/// Tollgate-owned proof that execution capacity was acquired for one request.
///
/// Sealed. An application's own compute permit is not one of these and cannot
/// stand in for Tollgate's class decision — the permit is what
/// [`ReadyToStart`](crate::ReadyToStart) requires to exist, so a type outside
/// this crate satisfying it would be a way to start work the gate refused.
pub trait CapacityPermit: private::Sealed + Send + 'static {}

/// A startup-selected execution-capacity policy.
///
/// Sealed around the three built-in implementations for the same reason the
/// permit is.
pub trait CapacityGate: private::Sealed + Send + Sync + 'static {
    /// The proof of acquisition this gate issues; holding it keeps the
    /// capacity taken, and dropping it returns the capacity.
    type Permit: CapacityPermit;

    /// Acquire capacity for one request, or refuse.
    ///
    /// Synchronous, fail-fast, allocation-free, lock-free, and clock-free.
    /// There is deliberately no queue: a request that cannot start now is
    /// refused now, because holding it would convert a capacity bound into an
    /// unbounded latency tail.
    fn acquire(&self, evidence: CapacityEvidence) -> Result<Self::Permit, DenyReason>;
}

/// What the gate is allowed to know about a request.
///
/// Carries the class and generation from the pinned snapshot that authorized
/// and priced the request, and has no public constructor: external input
/// cannot claim [`CapacityClass::Assured`] through a header, and an embedder
/// cannot assemble evidence for a request the engine did not admit. It never
/// leaves [`Pending`](crate::Pending), which is what stops a permit obtained
/// for an assured request being spent by a best-effort one.
#[derive(Debug, Clone, Copy)]
pub struct CapacityEvidence {
    class: CapacityClass,
    generation: Generation,
    locality: Locality,
}

impl CapacityEvidence {
    /// Build evidence from a pinned snapshot. Crate-private: the engine is the
    /// only thing that has a pinned snapshot to build it from.
    pub(crate) const fn new(
        class: CapacityClass,
        generation: Generation,
        locality: Locality,
    ) -> Self {
        Self {
            class,
            generation,
            locality,
        }
    }

    /// The class the account's compiled policy assigned.
    #[must_use]
    pub const fn class(self) -> CapacityClass {
        self.class
    }

    /// The generation this decision is pinned to, so a gate's own diagnostics
    /// can name the policy that produced it.
    #[must_use]
    pub const fn generation(self) -> Generation {
        self.generation
    }
}

/// Disabled execution-capacity policy: a zero-sized startup choice.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoGate;

/// Permit produced by [`NoGate`]; callers cannot construct one directly.
#[derive(Debug)]
pub struct NoCapacityPermit(());

impl private::Sealed for NoGate {}
impl private::Sealed for NoCapacityPermit {}
impl CapacityPermit for NoCapacityPermit {}

impl CapacityGate for NoGate {
    type Permit = NoCapacityPermit;

    #[inline]
    fn acquire(&self, _evidence: CapacityEvidence) -> Result<Self::Permit, DenyReason> {
        Ok(NoCapacityPermit(()))
    }
}

/// How much execution capacity this instance has, and how it is divided.
///
/// Not a runtime branch — see the module docs. Changing mode requires a
/// restart: a live resize would need its own contract for permits already
/// outstanding, and reinterpreting existing counters in place is exactly the
/// silent-semantics change the repository guidelines forbid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionCapacityMode {
    /// Current behaviour: no service-wide execution gate at all. The default,
    /// so a product that never configures capacity keeps exactly the admission
    /// it has today. Snapshot classes are inert.
    #[default]
    Disabled,
    /// Bound total execution without differentiating classes. Fail-fast
    /// overload protection for a product that wants a cap but no fast lane.
    Uniform {
        /// Execution slots the instance may hold at once, across all classes.
        total: NonZeroU32,
    },
    /// Bound total execution and keep part of it for assured work. The only
    /// mode in which [`CapacityClass`] changes an outcome.
    Reserved {
        /// Execution slots the instance may hold at once, reserve included.
        total: NonZeroU32,
        /// Slots within `total` that only [`CapacityClass::Assured`] work may
        /// use, once the shared remainder is full. Must be smaller than
        /// `total`; [`ExecutionCapacityGate::new`] rejects it otherwise.
        assured_reserve: NonZeroU32,
    },
}

/// Why a capacity configuration cannot start.
///
/// Rejected before the runtime starts rather than at the first request: a
/// configuration that can refuse every request is an outage, and an outage
/// discovered by serving traffic is worse than one discovered by failing to
/// boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityConfigError {
    /// The reserve is not smaller than the total, so shared capacity would be
    /// zero and no best-effort request could ever start.
    ReserveLeavesNoSharedCapacity {
        /// The configured total, in execution slots.
        total: u32,
        /// The configured assured reserve, in execution slots.
        assured_reserve: u32,
    },
}

impl std::fmt::Display for CapacityConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapacityConfigError::ReserveLeavesNoSharedCapacity {
                total,
                assured_reserve,
            } => write!(
                f,
                "an assured reserve of {assured_reserve} leaves no shared capacity out of \
                 {total}; best-effort work could never start"
            ),
        }
    }
}

impl std::error::Error for CapacityConfigError {}

/// One pool's shard, on its own cache line.
///
/// 128-byte alignment for the reason [`LeaseShard`] uses it: the supported
/// Apple Silicon hosts report 128-byte lines, and aligning to 64 would still
/// let adjacent shards invalidate one another there.
///
/// [`LeaseShard`]: tollgate_core::LocalLease
#[repr(align(128))]
#[derive(Debug)]
struct CapacityShard {
    free: AtomicU32,
}

/// A partitioned count of execution slots.
///
/// The partition is exact — shard capacities sum to the pool's total — which
/// is what makes "live permits never exceed the configured total" a property
/// of the construction rather than something to check.
#[derive(Debug)]
struct Pool {
    shards: Box<[CapacityShard]>,
    /// The shard count as the non-zero it always is.
    ///
    /// Stored rather than recovered from `shards.len()` per acquisition. The
    /// count is fixed at construction and cannot be zero, so carrying the
    /// proof removes both a request-path zero test and the cold panic arm an
    /// `expect` would have compiled in — an internal invariant made
    /// unrepresentable instead of checked.
    sharding: LocalSharding,
    total: u32,
}

impl Pool {
    /// Split `total` units across at most `sharding` shards.
    ///
    /// Capped at `total`, so no shard is created empty. An empty shard is not
    /// merely wasteful: every acquisition that landed on it would fall through
    /// to a sibling scan, turning the one-CAS fast path into a walk.
    fn new(total: NonZeroU32, sharding: LocalSharding) -> Self {
        let total = total.get();
        let shards = sharding.get().min(total as usize).max(1);
        let partitioned = (0..shards)
            .map(|index| CapacityShard {
                free: AtomicU32::new(partition(total, shards, index)),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards: partitioned,
            sharding: LocalSharding::new(
                NonZeroUsize::new(shards).expect("the shard count is clamped to at least one"),
            ),
            total,
        }
    }

    /// Take one unit, starting at the caller's sticky shard and walking
    /// siblings in a stable order.
    ///
    /// The routine path is one compare-exchange on the local shard. The walk
    /// exists because a partition is not a reservation: capacity idle on
    /// another shard is still this instance's capacity, and refusing while it
    /// sits there would be the sharding losing capacity that the unsharded
    /// design would have found.
    #[inline]
    fn try_acquire(&self, locality: Locality) -> Option<usize> {
        let count = self.shards.len();
        // The walk wraps by comparison rather than by `% count`. A modulo on
        // a count only known at runtime compiles to a division, and it would
        // sit on the fast path — the first, usually only, iteration — to
        // compute an index that is already `first`.
        let mut index = locality.index(self.sharding);
        for _ in 0..count {
            if self.take(index) {
                return Some(index);
            }
            index += 1;
            if index == count {
                index = 0;
            }
        }
        None
    }

    #[inline]
    fn take(&self, index: usize) -> bool {
        let shard = &self.shards[index];
        let mut current = shard.free.load(Ordering::Acquire);
        loop {
            // A failed subtraction touches nothing, so a refusal costs no
            // write and cannot strand a unit.
            let Some(next) = current.checked_sub(1) else {
                return false;
            };
            match shard.free.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    /// Return one unit to the shard it came from.
    ///
    /// To *that* shard, not to any shard: the shards are a partition, and a
    /// release landing elsewhere would let one shard exceed the capacity it
    /// was allotted while another sat permanently short.
    #[inline]
    fn release(&self, index: usize) {
        let prior = self.shards[index].free.fetch_add(1, Ordering::AcqRel);
        debug_assert!(
            prior < self.total,
            "released more capacity than the pool was configured with"
        );
    }

    /// Units currently free, summed across shards.
    ///
    /// A live read, so it is an estimate for the same reason a sharded lease's
    /// aggregate is: the walk is not one instant. Used for metrics, never for
    /// a decision.
    fn available(&self) -> u32 {
        self.shards
            .iter()
            .map(|shard| shard.free.load(Ordering::Relaxed))
            .sum::<u32>()
            .min(self.total)
    }

    const fn total(&self) -> u32 {
        self.total
    }
}

/// Spread the remainder over the first shards, so the parts sum to the whole.
///
/// The same rule a sharded lease uses. Exactness is the point: it is what lets
/// "no interleaving exceeds the configured total" hold by construction.
fn partition(total: u32, count: usize, index: usize) -> u32 {
    let count = count as u32;
    let index = index as u32;
    total / count + u32::from(index < total % count)
}

/// Which pool funded a permit, so its release returns to the right one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolKind {
    Shared,
    Reserve,
}

/// An acquired execution slot, released exactly once when dropped.
///
/// Holds the pool and shard that funded it — the receipt idea a sharded
/// lease's debit uses, at permit scale. It is `Send` so it can move to a
/// worker thread, and deliberately not `Clone`: a duplicated permit would be
/// capacity the pool never issued.
#[derive(Debug)]
pub struct ExecutionPermit {
    gate: Arc<Pools>,
    pool: PoolKind,
    shard: usize,
    class: CapacityClass,
}

impl ExecutionPermit {
    /// The class this permit was issued to, for per-class accounting.
    #[must_use]
    pub const fn class(&self) -> CapacityClass {
        self.class
    }
}

impl Drop for ExecutionPermit {
    fn drop(&mut self) {
        self.gate.pool(self.pool).release(self.shard);
    }
}

impl private::Sealed for ExecutionPermit {}
impl CapacityPermit for ExecutionPermit {}

/// The pools an enabled gate owns.
#[derive(Debug)]
struct Pools {
    shared: Pool,
    /// `None` under `Uniform`: one pool, and class changes no outcome.
    reserve: Option<Pool>,
}

impl Pools {
    fn pool(&self, kind: PoolKind) -> &Pool {
        match kind {
            PoolKind::Shared => &self.shared,
            PoolKind::Reserve => self
                .reserve
                .as_ref()
                .expect("a reserve permit implies a reserve pool"),
        }
    }
}

/// An enabled execution-capacity gate.
///
/// One type serves both `Uniform` and `Reserved`: uniform is the reserved
/// shape with no reserve pool, which keeps one acquisition path rather than
/// two that must agree about conservation. The mode still selects the *type*
/// at startup — `Disabled` is [`NoGate`], a different type entirely — so the
/// disabled path has no gate to branch in.
#[derive(Debug, Clone)]
pub struct ExecutionCapacityGate {
    pools: Arc<Pools>,
}

impl ExecutionCapacityGate {
    /// Build a gate for an enabled mode.
    ///
    /// Returns `Ok(None)` for [`ExecutionCapacityMode::Disabled`]: the caller
    /// composes [`NoGate`] instead, and getting `None` rather than a gate that
    /// always admits is what keeps disabled from allocating pool state.
    pub fn new(
        mode: ExecutionCapacityMode,
        sharding: LocalSharding,
    ) -> Result<Option<Self>, CapacityConfigError> {
        let (shared, reserve) = match mode {
            ExecutionCapacityMode::Disabled => return Ok(None),
            ExecutionCapacityMode::Uniform { total } => (total, None),
            ExecutionCapacityMode::Reserved {
                total,
                assured_reserve,
            } => {
                // Rejected here rather than at the first request: a reserve
                // that swallows the whole instance means no best-effort work
                // can ever start, which is an outage dressed as a policy.
                let shared = total
                    .get()
                    .checked_sub(assured_reserve.get())
                    .filter(|s| *s > 0);
                let Some(shared) = shared.and_then(NonZeroU32::new) else {
                    return Err(CapacityConfigError::ReserveLeavesNoSharedCapacity {
                        total: total.get(),
                        assured_reserve: assured_reserve.get(),
                    });
                };
                (shared, Some(assured_reserve))
            }
        };
        Ok(Some(Self {
            pools: Arc::new(Pools {
                shared: Pool::new(shared, sharding),
                reserve: reserve.map(|units| Pool::new(units, sharding)),
            }),
        }))
    }

    /// Total configured capacity, and what is free right now. Metrics only —
    /// the live read is an estimate, and no decision reads it.
    #[must_use]
    pub fn occupancy(&self) -> CapacityOccupancy {
        let reserve = self.pools.reserve.as_ref();
        CapacityOccupancy {
            shared_total: self.pools.shared.total(),
            shared_available: self.pools.shared.available(),
            reserve_total: reserve.map_or(0, Pool::total),
            reserve_available: reserve.map_or(0, Pool::available),
        }
    }
}

/// A bounded-cardinality view of a gate's configuration and occupancy.
///
/// Pool sizes and counts only. Account ids are deliberately absent: a metric
/// labelled by account is a cardinality incident waiting for a busy tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityOccupancy {
    /// Slots in the shared pool, which every class draws from first: the
    /// whole total under `Uniform`, total minus the reserve under `Reserved`.
    pub shared_total: u32,
    /// Shared slots free at the time of the read; an estimate, since the
    /// shards are summed one at a time.
    pub shared_available: u32,
    /// Zero under `Uniform`, which has no reserve rather than an empty one.
    pub reserve_total: u32,
    /// Reserve slots free at the time of the read; an estimate, and zero
    /// under `Uniform`.
    pub reserve_available: u32,
}

impl private::Sealed for ExecutionCapacityGate {}

impl CapacityGate for ExecutionCapacityGate {
    type Permit = ExecutionPermit;

    #[inline]
    fn acquire(&self, evidence: CapacityEvidence) -> Result<Self::Permit, DenyReason> {
        let class = evidence.class();
        let locality = evidence.locality;

        // Both classes try shared first, and that ordering is the whole
        // utilization argument: assured work reaches the reserve only once
        // shared is full, so an instance with no best-effort traffic is not
        // partitioned against itself — assured work uses shared *and* reserve,
        // the entire instance.
        if let Some(shard) = self.pools.shared.try_acquire(locality) {
            return Ok(ExecutionPermit {
                gate: Arc::clone(&self.pools),
                pool: PoolKind::Shared,
                shard,
                class,
            });
        }

        // Only assured work falls through. This single condition is the
        // isolation guarantee: a best-effort flood can exhaust shared and
        // never touch the reserve, so the configured number of assured starts
        // remains reachable at every instant.
        if class.may_use_assured_reserve()
            && let Some(reserve) = self.pools.reserve.as_ref()
            && let Some(shard) = reserve.try_acquire(locality)
        {
            return Ok(ExecutionPermit {
                gate: Arc::clone(&self.pools),
                pool: PoolKind::Reserve,
                shard,
                class,
            });
        }

        Err(DenyReason::CapacityUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn evidence(class: CapacityClass) -> CapacityEvidence {
        CapacityEvidence::new(class, Generation(1), Locality::current())
    }

    fn uniform(total: u32, sharding: usize) -> ExecutionCapacityGate {
        gate(
            ExecutionCapacityMode::Uniform {
                total: NonZeroU32::new(total).unwrap(),
            },
            sharding,
        )
    }

    fn reserved(total: u32, reserve: u32, sharding: usize) -> ExecutionCapacityGate {
        gate(
            ExecutionCapacityMode::Reserved {
                total: NonZeroU32::new(total).unwrap(),
                assured_reserve: NonZeroU32::new(reserve).unwrap(),
            },
            sharding,
        )
    }

    fn gate(mode: ExecutionCapacityMode, sharding: usize) -> ExecutionCapacityGate {
        ExecutionCapacityGate::new(
            mode,
            LocalSharding::new(std::num::NonZeroUsize::new(sharding).unwrap()),
        )
        .expect("a valid configuration")
        .expect("an enabled mode yields a gate")
    }

    /// Take until refused, whatever the shard layout. A sharded pool must not
    /// strand capacity: an idle sibling shard is still this instance's.
    fn drain(gate: &ExecutionCapacityGate, class: CapacityClass) -> Vec<ExecutionPermit> {
        let mut held = Vec::new();
        while let Ok(permit) = gate.acquire(evidence(class)) {
            held.push(permit);
        }
        held
    }

    /// Disabled allocates no pool state and cannot refuse. The class is inert:
    /// a best-effort account is treated exactly as an assured one, which is
    /// what "disabled means absent" has to mean for a product that never
    /// enabled the feature.
    #[test]
    fn disabled_never_refuses_and_ignores_the_class() {
        assert!(
            ExecutionCapacityGate::new(ExecutionCapacityMode::Disabled, LocalSharding::SINGLE)
                .expect("disabled is valid")
                .is_none(),
            "disabled builds no gate, so it can hold no capacity state"
        );
        for _ in 0..1_000 {
            for class in [CapacityClass::Assured, CapacityClass::BestEffort] {
                assert!(NoGate.acquire(evidence(class)).is_ok());
            }
        }
    }

    /// Uniform bounds the instance and treats both classes identically — the
    /// mode for a product that wants a cap but no fast lane.
    #[test]
    fn uniform_bounds_total_and_treats_both_classes_alike() {
        for class in [CapacityClass::Assured, CapacityClass::BestEffort] {
            let gate = uniform(4, 1);
            let held = drain(&gate, class);
            assert_eq!(held.len(), 4, "{class:?} may use the whole instance");
            assert!(gate.acquire(evidence(class)).is_err());
            drop(held);
            assert_eq!(
                drain(&gate, class).len(),
                4,
                "every permit returned its unit"
            );
        }
    }

    /// **The isolation guarantee.** A best-effort flood exhausts shared and
    /// cannot touch the reserve, so the configured number of assured starts
    /// stays reachable — which is the entire reason the feature exists.
    #[test]
    fn a_best_effort_flood_cannot_consume_the_assured_reserve() {
        let gate = reserved(10, 3, 1);
        let flood = drain(&gate, CapacityClass::BestEffort);
        assert_eq!(flood.len(), 7, "best-effort may use only shared capacity");
        assert!(
            gate.acquire(evidence(CapacityClass::BestEffort)).is_err(),
            "shared is exhausted"
        );

        let assured = drain(&gate, CapacityClass::Assured);
        assert_eq!(
            assured.len(),
            3,
            "the whole reserve remains reachable under a best-effort flood"
        );
    }

    /// **The utilization guarantee**, and the reason both classes try shared
    /// first. With no best-effort traffic, assured work uses shared *and*
    /// reserve — the instance is not partitioned against itself.
    #[test]
    fn assured_work_reaches_shared_and_reserve_when_alone() {
        let gate = reserved(10, 3, 1);
        assert_eq!(drain(&gate, CapacityClass::Assured).len(), 10);
    }

    /// Assured work takes shared before the reserve, so the reserve is still
    /// there for the next assured request. Taking the reserve first would
    /// spend the guarantee on the traffic that did not need it.
    #[test]
    fn assured_work_spends_shared_before_its_reserve() {
        let gate = reserved(10, 3, 1);
        let first = drain(&gate, CapacityClass::Assured);
        assert_eq!(first.len(), 10);
        drop(first);

        // Seven assured requests must leave the reserve untouched, which is
        // observable: a best-effort request can then still take nothing, and
        // three more assured ones fit.
        let assured: Vec<_> = (0..7)
            .map(|_| {
                gate.acquire(evidence(CapacityClass::Assured))
                    .expect("shared")
            })
            .collect();
        assert!(
            gate.acquire(evidence(CapacityClass::BestEffort)).is_err(),
            "shared is spent, so best-effort is shed"
        );
        assert_eq!(
            drain(&gate, CapacityClass::Assured).len(),
            3,
            "the reserve was untouched by the first seven"
        );
        drop(assured);
    }

    /// A sharded pool must not strand capacity. Every unit is reachable from
    /// one locality even when the partition put it on another shard, which is
    /// what the sibling walk buys.
    #[test]
    fn a_sharded_pool_strands_no_capacity() {
        for (total, shards) in [(16, 8), (10, 4), (7, 3), (3, 8), (1, 8)] {
            let gate = uniform(total, shards);
            assert_eq!(
                drain(&gate, CapacityClass::Assured).len() as u32,
                total,
                "{total} units across {shards} shards must all be reachable"
            );
        }
    }

    /// Shards are capped at the pool's units, so none is created empty. An
    /// empty shard would make every acquisition landing on it fall through to
    /// a sibling scan — the fast path never fast.
    #[test]
    fn shards_are_capped_at_the_pools_units() {
        let pool = Pool::new(
            NonZeroU32::new(3).unwrap(),
            LocalSharding::new(std::num::NonZeroUsize::new(16).unwrap()),
        );
        assert_eq!(pool.shards.len(), 3);
        assert!(
            pool.shards
                .iter()
                .all(|s| s.free.load(Ordering::Relaxed) > 0)
        );
        assert_eq!(pool.available(), 3);
    }

    /// The partition is exact, which is what makes conservation structural
    /// rather than checked.
    #[test]
    fn a_partition_sums_to_the_whole() {
        for total in 1u32..64 {
            for count in 1usize..=16 {
                let count = count.min(total as usize);
                let sum: u32 = (0..count).map(|i| partition(total, count, i)).sum();
                assert_eq!(sum, total, "{total} across {count} shards");
            }
        }
    }

    /// A reserve that leaves no shared capacity is refused before startup, not
    /// discovered when every best-effort request is shed.
    #[test]
    fn a_reserve_that_swallows_the_instance_is_refused() {
        for (total, reserve) in [(4, 4), (4, 5)] {
            let error = ExecutionCapacityGate::new(
                ExecutionCapacityMode::Reserved {
                    total: NonZeroU32::new(total).unwrap(),
                    assured_reserve: NonZeroU32::new(reserve).unwrap(),
                },
                LocalSharding::SINGLE,
            )
            .expect_err("a reserve at or above total leaves no shared capacity");
            assert_eq!(
                error,
                CapacityConfigError::ReserveLeavesNoSharedCapacity {
                    total,
                    assured_reserve: reserve,
                }
            );
        }
    }

    /// Occupancy is reported for metrics and never read for a decision. Under
    /// `Uniform` the reserve is absent rather than zero-sized, and says so.
    #[test]
    fn occupancy_reports_configuration_and_free_units() {
        let gate = reserved(10, 3, 1);
        assert_eq!(
            gate.occupancy(),
            CapacityOccupancy {
                shared_total: 7,
                shared_available: 7,
                reserve_total: 3,
                reserve_available: 3,
            }
        );
        let _held = gate.acquire(evidence(CapacityClass::BestEffort)).unwrap();
        assert_eq!(gate.occupancy().shared_available, 6);

        let uniform = uniform(4, 1);
        let occupancy = uniform.occupancy();
        assert_eq!(
            (occupancy.reserve_total, occupancy.reserve_available),
            (0, 0)
        );
    }

    /// Under contention no interleaving exceeds the configured total, and
    /// every permit returns exactly one unit to the pool it came from.
    #[test]
    fn concurrent_acquisition_never_exceeds_the_total() {
        const TOTAL: u32 = 32;
        let gate = reserved(TOTAL, 8, 8);
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for worker in 0..8 {
                let gate = &gate;
                let live = &live;
                let peak = &peak;
                scope.spawn(move || {
                    let class = if worker % 2 == 0 {
                        CapacityClass::Assured
                    } else {
                        CapacityClass::BestEffort
                    };
                    for _ in 0..2_000 {
                        if let Ok(permit) = gate.acquire(evidence(class)) {
                            let now = live.fetch_add(1, Ordering::AcqRel) + 1;
                            peak.fetch_max(now, Ordering::AcqRel);
                            live.fetch_sub(1, Ordering::AcqRel);
                            drop(permit);
                        }
                    }
                });
            }
        });

        assert!(
            peak.load(Ordering::Acquire) <= TOTAL as usize,
            "live permits peaked at {} against a total of {TOTAL}",
            peak.load(Ordering::Acquire)
        );
        assert_eq!(
            gate.occupancy().shared_available + gate.occupancy().reserve_available,
            TOTAL,
            "every permit returned its unit"
        );
    }

    /// A best-effort flood running concurrently still cannot reach the
    /// reserve: the isolation guarantee has to hold under interleaving, not
    /// only in a sequential drain.
    #[test]
    fn concurrent_best_effort_load_leaves_the_reserve_reachable() {
        let gate = reserved(12, 4, 4);
        let stop = std::sync::atomic::AtomicBool::new(false);

        std::thread::scope(|scope| {
            for _ in 0..4 {
                let gate = &gate;
                let stop = &stop;
                scope.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(permit) = gate.acquire(evidence(CapacityClass::BestEffort)) {
                            std::hint::spin_loop();
                            drop(permit);
                        }
                    }
                });
            }

            // The reserve is four units, and best-effort work cannot hold any
            // of them however hard it tries.
            for _ in 0..200 {
                let held = drain(&gate, CapacityClass::Assured);
                assert!(
                    held.len() >= 4,
                    "the reserve must stay reachable under a best-effort flood; got {}",
                    held.len()
                );
            }
            stop.store(true, Ordering::Relaxed);
        });
    }
}
