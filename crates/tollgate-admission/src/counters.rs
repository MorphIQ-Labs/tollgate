//! What the request path is allowed to say about itself.
//!
//! A denial returns a [`DenyReason`] to its caller and is then gone: an
//! instance serving no traffic and one refusing every request look identical
//! from outside. The control plane closes that gap with structured events,
//! but this plane cannot — INVARIANTS.md #5 forbids I/O, locks and clock
//! reads on the request path, and a logging call is all three. What remains
//! affordable is a counter: no allocation, no formatting, and only the fixed
//! single-vs-sharded layout branch selected at engine construction.
//!
//! [`DenyReason`] is a closed enum, so the tally is a fixed array indexed by
//! [`DenyReason::index`] — never a map, never a string key. That is what
//! bounds both the cost (a direct index) and the cardinality
//! ([`DenyReason::COUNT`] series, whatever the traffic).
//!
//! Scope is per engine instance, like the limiter registry in
//! [`crate::state`]: these count what *this* process admitted and refused, and
//! a fleet view is the scrape's job to aggregate.

use std::sync::atomic::{AtomicU64, Ordering};

use tollgate_core::{CapacityClass, CommitError, CostUnits, DenyReason, LocalSharding, Locality};

/// How many execution-capacity classes exist, and their stable label order.
///
/// Two, and the labels are the enum tags alone — the whole bounded-cardinality
/// rule for capacity metrics (#99). A dense pair rather than a
/// `DenyReason`-shaped table, for the reason `CommitRefusal` gives.
pub const CAPACITY_CLASS_COUNT: usize = 2;

/// The class labels, in slot order.
pub const CAPACITY_CLASS_NAMES: [&str; CAPACITY_CLASS_COUNT] = ["Assured", "BestEffort"];

/// The counter slot for a class. Exhaustive by construction: a third class
/// would fail to compile until it had one.
const fn class_slot(class: CapacityClass) -> usize {
    match class {
        CapacityClass::Assured => 0,
        CapacityClass::BestEffort => 1,
    }
}

/// One counter on its own cache line.
///
/// Without the padding the whole array shares a handful of lines, so eight
/// cores counting eight *different* reasons would serialise on the same line
/// for no reason at all — the classic false-sharing tax. Apple Silicon uses
/// 128-byte cache lines; over-aligning on 64-byte-line x86-64 is harmless.
#[repr(align(128))]
#[derive(Debug)]
struct Padded(AtomicU64);

impl Padded {
    /// A constructor rather than a `const ZERO` item: a constant holding an
    /// atomic is *copied* at each use, so incrementing through it would bump
    /// a temporary and discard the result. A function leaves no such constant
    /// to misuse, which is why clippy's `declare_interior_mutable_const` is
    /// satisfied here by construction rather than by an allow.
    const fn zero() -> Self {
        Padded(AtomicU64::new(0))
    }

    #[inline]
    fn bump(&self, by: u64) {
        // Relaxed throughout: nothing is published through these counters, so
        // no other memory needs to become visible with them. Atomicity still
        // holds — concurrent increments never lose an update — and dropping
        // the fences is what keeps the increment near-free on the hot path.
        // Matches the `Unaccounted` tally in tollgate-client.
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Why a request that already held pending funding was refused at execution
/// start.
///
/// A small dedicated vocabulary rather than a second [`DenyReason`]-shaped
/// array. Only four refusals can reach commit, and mirroring the twenty-one
/// slot table would add roughly 2.7 KB per counter set for seventeen slots
/// nothing can ever bump. It keeps `DenyReason`'s forcing function, though:
/// [`index`](CommitRefusal::index) is an exhaustive match, so a new variant
/// fails to compile until it has a slot, and [`NAMES`](CommitRefusal::NAMES)
/// gives every slot a stable label.
///
/// These are counted separately from `denials` on purpose. A request refused
/// here was already counted under `admitted`, and bumping the pre-admission
/// denial total a second time would make one request both an admission and a
/// member of the same flat refusal sum (INVARIANTS.md #20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitRefusal {
    /// The funding lease's window lapsed and no overage fallback applied.
    FundingExpired,
    /// An elastic fallback could not fit inside the overage cap, and pending
    /// refunds would not make room.
    OverageCapExhausted,
    /// The same, but refundable occupancy is the reason it did not fit.
    OverageCapTemporarilyExhausted,
    /// A cancellation won the race for the phase.
    Cancelled,
}

impl CommitRefusal {
    /// Stable label per slot, in [`index`](Self::index) order.
    pub const NAMES: [&'static str; Self::COUNT] = [
        "funding_expired",
        "overage_cap_exhausted",
        "overage_cap_temporarily_exhausted",
        "cancelled",
    ];

    /// How many slots the tally has.
    pub const COUNT: usize = 4;

    /// This refusal's dense slot. Exhaustive by construction.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::FundingExpired => 0,
            Self::OverageCapExhausted => 1,
            Self::OverageCapTemporarilyExhausted => 2,
            Self::Cancelled => 3,
        }
    }

    /// Classify a commit outcome, or `None` if it did not refuse.
    ///
    /// `AlreadyCommitted` is a programming error rather than an outcome the
    /// request path produces, and it is deliberately not given a slot: a
    /// counter for it would read as an operational condition.
    #[must_use]
    pub fn from_commit_error(error: &CommitError) -> Option<Self> {
        match error {
            CommitError::Cancelled | CommitError::AlreadyReleased => Some(Self::Cancelled),
            CommitError::Denied(DenyReason::FundingExpiredAtStart) => Some(Self::FundingExpired),
            CommitError::Denied(DenyReason::OverageCapExhausted { .. }) => {
                Some(Self::OverageCapExhausted)
            }
            CommitError::Denied(DenyReason::OverageCapTemporarilyExhausted { .. })
            | CommitError::Denied(DenyReason::OverageCommitInProgress { .. }) => {
                Some(Self::OverageCapTemporarilyExhausted)
            }
            CommitError::Denied(_) | CommitError::AlreadyCommitted => None,
        }
    }
}

/// Per-instance admission tallies, written from the request path and read by
/// whatever exports them.
///
/// Counters are monotonic and wrap at [`u64::MAX`]. Unlike cost and lease
/// arithmetic — which is checked, because a wrapped charge is a wrong bill
/// (INVARIANTS.md #11) — a wrapped monitoring counter is not a correctness
/// event, and rate-of-change is how counters are read anyway. At a billion
/// admissions a second the `admitted` counter would need roughly 584 years to
/// reach the wrap; adding a branch on the request path to guard against it
/// would cost more than it could ever save.
/// # Layout
///
/// `repr(C)`, so the declaration order below *is* the layout. Without it the
/// compiler arranges the fields however it likes, and adding one silently
/// rearranges the rest — which is not theoretical: #99's two per-class arrays
/// moved the request-path counters and cost `admission/full_check` 3.5%
/// (122.30 ns to 126.55 ns, three runs each on the controlled host) while
/// touching no code that benchmark executes. Restoring the order gave all of
/// it back.
///
/// So the arrangement below is *calibrated*, not derived: it is the order the
/// recorded baselines were measured against, and new counters are appended
/// after every field that predates them rather than filed next to the ones
/// they relate to. Hoisting the two per-request counters to the front was
/// tried and measured 1.5% worse than leaving them where they are, which is
/// the point — this is not a layout to reason about from first principles.
/// The padding buys false-sharing isolation between counters; `repr(C)` is
/// what stops the arrangement itself from being a lottery each new field
/// re-enters.
#[derive(Debug)]
#[repr(C)]
pub struct AdmissionCounters {
    admitted: Padded,
    units_admitted: Padded,
    admitted_overage: Padded,
    units_admitted_overage: Padded,
    denials: [Padded; DenyReason::COUNT],
    // Post-admission phase outcomes (INVARIANTS.md #20). None of these touch
    // `denials`: a request counted here was already counted under `admitted`,
    // and adding it to the pre-admission refusal total a second time would
    // give one request two contradictory identities.
    //
    // `execution_started` and `canceled_before_start` shard with `admitted`,
    // because every request reaches exactly one of them and they run at the
    // same rate as admission itself. The rest stay inline: a shed, an
    // abandoned context, a commit refusal, and a commit-time fallback are
    // bounded exceptions, and the fallback already serializes on the overage
    // counter's own compare-exchange one step earlier.
    // The sharded layout keeps these two in `CounterShard`; these inline
    // copies serve the single-locality layout, exactly as `admitted` does.
    execution_started: Padded,
    canceled_before_start: Padded,
    contexts_abandoned: Padded,
    capacity_shed: Padded,
    committed_at_overage: Padded,
    units_committed_at_overage: Padded,
    commit_refusals: [Padded; CommitRefusal::COUNT],
    /// The per-class breakdown of the two capacity outcomes (#99).
    ///
    /// Two classes, so a small dense array rather than a `DenyReason`-shaped
    /// table — the reasoning `CommitRefusal` records. Inline rather than
    /// sharded: a shed is a bounded exception, and an execution start already
    /// shards through `execution_started`, so these carry the *breakdown*
    /// beside totals that are already partitioned.
    capacity_shed_by_class: [Padded; CAPACITY_CLASS_COUNT],
    execution_started_by_class: [Padded; CAPACITY_CLASS_COUNT],
    shards: Option<Box<[CounterShard]>>,
}

/// One locality's complete tally. Different localities never share a cache
/// line; counters within one locality are deliberately compact because the
/// same request updates them.
#[repr(align(128))]
#[derive(Debug)]
struct CounterShard {
    admitted: AtomicU64,
    units_admitted: AtomicU64,
    execution_started: AtomicU64,
    canceled_before_start: AtomicU64,
    denials: [AtomicU64; DenyReason::COUNT],
}

impl CounterShard {
    fn zero() -> Self {
        Self {
            admitted: AtomicU64::new(0),
            units_admitted: AtomicU64::new(0),
            execution_started: AtomicU64::new(0),
            canceled_before_start: AtomicU64::new(0),
            denials: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AdmissionCounters {
    #[must_use]
    pub const fn new() -> Self {
        AdmissionCounters {
            admitted: Padded::zero(),
            units_admitted: Padded::zero(),
            admitted_overage: Padded::zero(),
            units_admitted_overage: Padded::zero(),
            denials: [const { Padded::zero() }; DenyReason::COUNT],
            execution_started: Padded::zero(),
            canceled_before_start: Padded::zero(),
            contexts_abandoned: Padded::zero(),
            capacity_shed: Padded::zero(),
            capacity_shed_by_class: [const { Padded::zero() }; CAPACITY_CLASS_COUNT],
            execution_started_by_class: [const { Padded::zero() }; CAPACITY_CLASS_COUNT],
            committed_at_overage: Padded::zero(),
            units_committed_at_overage: Padded::zero(),
            commit_refusals: [const { Padded::zero() }; CommitRefusal::COUNT],
            shards: None,
        }
    }

    /// Create counters partitioned by the same sticky locality used by lease
    /// and rate state. One shard preserves the inline historical layout.
    #[must_use]
    pub fn with_sharding(sharding: LocalSharding) -> Self {
        if sharding == LocalSharding::SINGLE {
            return Self::new();
        }
        Self {
            admitted: Padded::zero(),
            units_admitted: Padded::zero(),
            admitted_overage: Padded::zero(),
            units_admitted_overage: Padded::zero(),
            denials: [const { Padded::zero() }; DenyReason::COUNT],
            execution_started: Padded::zero(),
            canceled_before_start: Padded::zero(),
            contexts_abandoned: Padded::zero(),
            capacity_shed: Padded::zero(),
            capacity_shed_by_class: [const { Padded::zero() }; CAPACITY_CLASS_COUNT],
            execution_started_by_class: [const { Padded::zero() }; CAPACITY_CLASS_COUNT],
            committed_at_overage: Padded::zero(),
            units_committed_at_overage: Padded::zero(),
            commit_refusals: [const { Padded::zero() }; CommitRefusal::COUNT],
            shards: Some(
                (0..sharding.get())
                    .map(|_| CounterShard::zero())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        }
    }

    /// Record an admitted request and the units it was quoted.
    #[inline]
    pub fn record_admit(&self, units: CostUnits) {
        self.record_admit_at(units, Locality::current());
    }

    #[inline]
    pub(crate) fn record_admit_at(&self, units: CostUnits, locality: Locality) {
        if let Some(shards) = &self.shards {
            let shard = &shards[locality.index(LocalSharding::new(
                std::num::NonZeroUsize::new(shards.len()).expect("sharded counters are non-empty"),
            ))];
            shard.admitted.fetch_add(1, Ordering::Relaxed);
            shard
                .units_admitted
                .fetch_add(units.get(), Ordering::Relaxed);
            return;
        }
        self.admitted.bump(1);
        self.units_admitted.bump(units.get());
    }

    /// Record an admission that no lease funded.
    ///
    /// A *subset* of `admitted`, not a sibling of it: the request is counted
    /// in both, so `admitted` remains the total and needs no reader to add two
    /// numbers to get it. `admitted_overage` answers a different question —
    /// how much of that total the account is being trusted for — and it is the
    /// number an operator watches climb before an invoice does.
    ///
    /// Deliberately not a `DenyReason`-style dense outcome table. There is one
    /// admit outcome plus one qualifier, and building the mirror of
    /// [`DenyReason::index`] for two counters would add the machinery without
    /// the forcing function that justifies it there.
    ///
    /// The qualifier pair stays unsharded while `admitted` shards. Sharding
    /// buys nothing here: this path runs only when the account had no lease
    /// able to fund the quote, and it already serializes on the overage
    /// counter's own compare-exchange one step earlier. Two more relaxed
    /// bumps on that path add no contention class that the cap has not
    /// already imposed.
    #[inline]
    pub fn record_admit_overage(&self, units: CostUnits) {
        self.record_admit_overage_at(units, Locality::current());
    }

    #[inline]
    pub(crate) fn record_admit_overage_at(&self, units: CostUnits, locality: Locality) {
        self.record_admit_at(units, locality);
        self.admitted_overage.bump(1);
        self.units_admitted_overage.bump(units.get());
    }

    /// Record a context that was resolved before stage two ever ran.
    ///
    /// `begin` authorized the principal and pinned its generation, and then
    /// the request ended — a body read that failed, a client that
    /// disconnected, a decode that was refused. No pending funding was created
    /// and no admission outcome was decided, so this is neither an admission
    /// nor a denial. It is counted anyway because the alternative is silence:
    /// an instance authenticating a flood of requests that never reach stage
    /// two would otherwise be indistinguishable from one serving none, which
    /// is exactly the blindness INVARIANTS.md #20 exists to prevent.
    #[inline]
    pub fn record_context_abandoned(&self) {
        self.contexts_abandoned.bump(1);
    }

    /// Record a request shed by the execution-capacity gate.
    ///
    /// Pending funding existed and was released for zero. Counted here rather
    /// than in `denials` because the request was already counted under
    /// `admitted`; #99 adds the per-class breakdown on top of this total.
    #[inline]
    pub fn record_capacity_shed(&self) {
        self.record_capacity_shed_for(CapacityClass::Assured);
    }

    /// Record a shed against the class whose work was refused (#99).
    #[inline]
    pub(crate) fn record_capacity_shed_for(&self, class: CapacityClass) {
        self.capacity_shed.bump(1);
        self.capacity_shed_by_class[class_slot(class)].bump(1);
    }

    /// Record an execution start against the class that started it (#99).
    #[inline]
    pub(crate) fn record_execution_started_for(&self, class: CapacityClass, locality: Locality) {
        self.record_execution_started_at(locality);
        self.execution_started_by_class[class_slot(class)].bump(1);
    }

    /// Record a request that resolved for zero after admission and before
    /// execution start — a cancellation, or an abandoned pending state.
    #[inline]
    pub(crate) fn record_canceled_before_start_at(&self, locality: Locality) {
        if let Some(shard) = self.shard_at(locality) {
            shard.canceled_before_start.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.canceled_before_start.bump(1);
    }

    /// Record a request whose kernel was cleared to run.
    #[inline]
    pub(crate) fn record_execution_started_at(&self, locality: Locality) {
        if let Some(shard) = self.shard_at(locality) {
            shard.execution_started.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.execution_started.bump(1);
    }

    /// Record a commit that settled against overage because its lease lapsed.
    ///
    /// Disjoint from `admitted_overage`, which counts admissions no lease
    /// could fund. This request *was* funded at admission and changed funding
    /// at execution start, so adding the two would double-count nothing and
    /// separating them is what lets an operator tell the two elastic paths
    /// apart.
    #[inline]
    pub(crate) fn record_committed_at_overage(&self, units: CostUnits) {
        self.committed_at_overage.bump(1);
        self.units_committed_at_overage.bump(units.get());
    }

    /// Record a refusal at execution start against its own slot.
    #[inline]
    pub(crate) fn record_commit_refusal(&self, refusal: CommitRefusal) {
        self.commit_refusals[refusal.index()].bump(1);
    }

    #[inline]
    fn shard_at(&self, locality: Locality) -> Option<&CounterShard> {
        let shards = self.shards.as_ref()?;
        Some(
            &shards[locality.index(LocalSharding::new(
                std::num::NonZeroUsize::new(shards.len()).expect("sharded counters are non-empty"),
            ))],
        )
    }

    /// Record a refusal against its reason's slot.
    ///
    /// Public because not every refusal originates in
    /// [`AdmissionEngine::admit`](crate::AdmissionEngine::admit):
    /// `AccountingBackpressure` is decided by the embedder *before* admission
    /// (INVARIANTS.md #8 sheds on a full usage queue), so a service that
    /// sheds there records it here. A slot that could only ever read zero
    /// because nothing can reach it would be worse than absent — it would
    /// read as "this never happens".
    #[inline]
    pub fn record_deny(&self, reason: &DenyReason) {
        self.record_deny_at(reason, Locality::current());
    }

    #[inline]
    pub(crate) fn record_deny_at(&self, reason: &DenyReason, locality: Locality) {
        if let Some(shards) = &self.shards {
            let shard = &shards[locality.index(LocalSharding::new(
                std::num::NonZeroUsize::new(shards.len()).expect("sharded counters are non-empty"),
            ))];
            shard.denials[reason.index()].fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.denials[reason.index()].bump(1);
    }

    /// Read every counter.
    ///
    /// Lock-free, so the result is not a single instant: counters read later
    /// may include increments that landed after the earlier ones were read.
    /// The alternative is a lock on the request path, which INVARIANTS.md #5
    /// forbids outright — and skew between counters read microseconds apart
    /// does not survive the scrape interval that consumes them.
    #[must_use]
    /// Units quoted by every admitted request on this instance, including the
    /// overage ones (`record_admit_overage` counts through `record_admit`).
    ///
    /// A focused read, because its one caller wants this number and not the
    /// twenty-odd in [`snapshot`](Self::snapshot): the balance estimate
    /// subtracts it from what the ledger last reported, and building a whole
    /// `CountersSnapshot` to reach one field would walk every denial slot as
    /// well. Wrapping-summed across shards for the same reason `snapshot` is —
    /// see the type docs on why a monitoring counter does not use checked
    /// arithmetic.
    pub fn units_admitted(&self) -> u64 {
        match &self.shards {
            Some(shards) => shards.iter().fold(0u64, |total, shard| {
                total.wrapping_add(shard.units_admitted.load(Ordering::Relaxed))
            }),
            None => self.units_admitted.get(),
        }
    }

    pub fn snapshot(&self) -> CountersSnapshot {
        if let Some(shards) = &self.shards {
            let mut snapshot = CountersSnapshot {
                admitted: 0,
                units_admitted: 0,
                // Not summed from the shards: the overage qualifier is one
                // inline pair under every layout, because the path that bumps
                // it is the one with no lease to shard.
                admitted_overage: self.admitted_overage.get(),
                units_admitted_overage: self.units_admitted_overage.get(),
                denials: [0; DenyReason::COUNT],
                execution_started: 0,
                canceled_before_start: 0,
                contexts_abandoned: self.contexts_abandoned.get(),
                capacity_shed: self.capacity_shed.get(),
                capacity_shed_by_class: std::array::from_fn(|slot| {
                    self.capacity_shed_by_class[slot].get()
                }),
                execution_started_by_class: std::array::from_fn(|slot| {
                    self.execution_started_by_class[slot].get()
                }),
                committed_at_overage: self.committed_at_overage.get(),
                units_committed_at_overage: self.units_committed_at_overage.get(),
                commit_refusals: std::array::from_fn(|slot| self.commit_refusals[slot].get()),
            };
            for shard in shards {
                snapshot.admitted = snapshot
                    .admitted
                    .wrapping_add(shard.admitted.load(Ordering::Relaxed));
                snapshot.units_admitted = snapshot
                    .units_admitted
                    .wrapping_add(shard.units_admitted.load(Ordering::Relaxed));
                snapshot.execution_started = snapshot
                    .execution_started
                    .wrapping_add(shard.execution_started.load(Ordering::Relaxed));
                snapshot.canceled_before_start = snapshot
                    .canceled_before_start
                    .wrapping_add(shard.canceled_before_start.load(Ordering::Relaxed));
                for (total, counter) in snapshot.denials.iter_mut().zip(&shard.denials) {
                    *total = total.wrapping_add(counter.load(Ordering::Relaxed));
                }
            }
            return snapshot;
        }
        CountersSnapshot {
            admitted: self.admitted.get(),
            units_admitted: self.units_admitted.get(),
            admitted_overage: self.admitted_overage.get(),
            units_admitted_overage: self.units_admitted_overage.get(),
            denials: std::array::from_fn(|slot| self.denials[slot].get()),
            execution_started: self.execution_started.get(),
            canceled_before_start: self.canceled_before_start.get(),
            contexts_abandoned: self.contexts_abandoned.get(),
            capacity_shed: self.capacity_shed.get(),
            capacity_shed_by_class: std::array::from_fn(|slot| {
                self.capacity_shed_by_class[slot].get()
            }),
            execution_started_by_class: std::array::from_fn(|slot| {
                self.execution_started_by_class[slot].get()
            }),
            committed_at_overage: self.committed_at_overage.get(),
            units_committed_at_overage: self.units_committed_at_overage.get(),
            commit_refusals: std::array::from_fn(|slot| self.commit_refusals[slot].get()),
        }
    }
}

impl Default for AdmissionCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A read of [`AdmissionCounters`]: plain values, safe to serialise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// Requests admitted.
    pub admitted: u64,
    /// Units *quoted* by admitted requests.
    ///
    /// Not units billed. Admission opens a pending debit that a cancellation
    /// releases and a commit charges, and usage events — not this counter —
    /// are billing truth. Reading this as revenue would over-count every
    /// request that was admitted and then cancelled before execution.
    pub units_admitted: u64,
    /// Admitted requests that no lease funded, under
    /// [`EnforcementMode::Elastic`]. Included in `admitted`, never instead of
    /// it.
    ///
    /// [`EnforcementMode::Elastic`]: tollgate_core::EnforcementMode::Elastic
    pub admitted_overage: u64,
    /// Units quoted by those requests — unfunded credit this instance has
    /// extended, before any commit or cancellation. The billed figure is
    /// `Conservation::overage_recorded`, which lags this one and is smaller by
    /// whatever was cancelled before execution.
    pub units_admitted_overage: u64,
    /// Refusals per [`DenyReason::index`] slot.
    ///
    /// Pre-admission only. Every field below describes a *later* phase and
    /// none of them appear here, so [`denied`](Self::denied) keeps its meaning
    /// and one request is never counted as both an admission and a denial.
    pub denials: [u64; DenyReason::COUNT],
    /// Contexts that `begin` produced and that were dropped before `admit`
    /// consumed them: no pending funding, no admission outcome.
    pub contexts_abandoned: u64,
    /// Admitted requests refused by the execution-capacity gate, released for
    /// zero. #99 adds the per-class breakdown.
    pub capacity_shed: u64,
    /// Sheds and starts split by class (#99), in [`CAPACITY_CLASS_NAMES`]
    /// order. Each sums to the total beside it; neither replaces it, so a
    /// reader never has to add two numbers to get one.
    pub capacity_shed_by_class: [u64; CAPACITY_CLASS_COUNT],
    pub execution_started_by_class: [u64; CAPACITY_CLASS_COUNT],
    /// Admitted requests that resolved for zero between admission and
    /// execution start — cancelled, or abandoned while pending.
    pub canceled_before_start: u64,
    /// Requests whose kernel was cleared to run. Together with
    /// `canceled_before_start`, `capacity_shed`, and `commit_refusals`, this
    /// accounts for every request that reached `admitted`.
    pub execution_started: u64,
    /// Commits that settled against overage because their funding lease
    /// lapsed after admission.
    ///
    /// Disjoint from `admitted_overage`: that counts admissions no lease could
    /// fund, this counts admissions a lease *did* fund whose window then
    /// closed. An account can produce either without the other.
    pub committed_at_overage: u64,
    /// Units those commits moved to the overage counter.
    pub units_committed_at_overage: u64,
    /// Refusals at execution start, per [`CommitRefusal::index`] slot.
    pub commit_refusals: [u64; CommitRefusal::COUNT],
}

impl CountersSnapshot {
    /// Refusals paired with their stable labels, in slot order.
    pub fn denials_by_name(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        DenyReason::NAMES
            .iter()
            .copied()
            .zip(self.denials.iter().copied())
    }

    /// Every refusal, whatever the reason.
    ///
    /// Pre-admission refusals only. A request refused at execution start was
    /// already counted under `admitted`, so adding it here would give one
    /// request two contradictory identities; read
    /// [`refused_at_start`](Self::refused_at_start) for that phase.
    #[must_use]
    pub fn denied(&self) -> u64 {
        self.denials.iter().sum()
    }

    /// Refusals at execution start paired with their stable labels.
    pub fn commit_refusals_by_name(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        CommitRefusal::NAMES
            .iter()
            .copied()
            .zip(self.commit_refusals.iter().copied())
    }

    /// Every refusal at execution start, whatever the reason.
    #[must_use]
    pub fn refused_at_start(&self) -> u64 {
        self.commit_refusals.iter().sum()
    }

    /// Execution starts paired with their class labels, in slot order (#99).
    pub fn execution_started_by_class_name(
        &self,
    ) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        CAPACITY_CLASS_NAMES
            .iter()
            .copied()
            .zip(self.execution_started_by_class.iter().copied())
    }

    /// Capacity sheds paired with their class labels, in slot order (#99).
    pub fn capacity_shed_by_class_name(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        CAPACITY_CLASS_NAMES
            .iter()
            .copied()
            .zip(self.capacity_shed_by_class.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The padding is the whole point of the type: if it ever stops applying,
    /// counters for unrelated reasons start contending on one cache line and
    /// the cost shows up only under concurrency, where it is hardest to
    /// attribute.
    #[test]
    fn each_counter_occupies_its_own_cache_line() {
        assert_eq!(align_of::<Padded>(), 128);
        assert_eq!(size_of::<Padded>(), 128);
        let counters = AdmissionCounters::new();
        let first = std::ptr::from_ref(&counters.denials[0]).addr();
        let second = std::ptr::from_ref(&counters.denials[1]).addr();
        assert_eq!(second - first, 128, "adjacent slots must not share a line");
    }

    const ALL_REFUSALS: [CommitRefusal; CommitRefusal::COUNT] = [
        CommitRefusal::FundingExpired,
        CommitRefusal::OverageCapExhausted,
        CommitRefusal::OverageCapTemporarilyExhausted,
        CommitRefusal::Cancelled,
    ];

    /// The same forcing function `DenyReason` has, at a size that fits four
    /// outcomes: the mapping is total and no two refusals share a slot.
    #[test]
    fn commit_refusal_indices_cover_every_slot_exactly_once() {
        let mut seen = [false; CommitRefusal::COUNT];
        for refusal in ALL_REFUSALS {
            let index = refusal.index();
            assert!(
                index < CommitRefusal::COUNT,
                "{refusal:?} indexes out of range"
            );
            assert!(!seen[index], "{refusal:?} shares slot {index}");
            seen[index] = true;
        }
        assert!(seen.iter().all(|hit| *hit), "every slot must belong to one");
    }

    /// The slots are a contract with whatever reads the exported counters, so
    /// they are pinned by literal. Renumbering `index()` and `NAMES` together
    /// stays internally consistent while silently re-attributing a counter a
    /// consumer already reads — which is exactly what this catches.
    #[test]
    fn shipped_commit_refusal_slots_and_labels_never_move() {
        assert_eq!(CommitRefusal::FundingExpired.index(), 0);
        assert_eq!(CommitRefusal::OverageCapExhausted.index(), 1);
        assert_eq!(CommitRefusal::OverageCapTemporarilyExhausted.index(), 2);
        assert_eq!(CommitRefusal::Cancelled.index(), 3);
        assert_eq!(CommitRefusal::NAMES[0], "funding_expired");
        assert_eq!(CommitRefusal::NAMES[1], "overage_cap_exhausted");
        assert_eq!(CommitRefusal::NAMES[2], "overage_cap_temporarily_exhausted");
        assert_eq!(CommitRefusal::NAMES[3], "cancelled");
    }

    /// A counter added later sits after every counter that predates it.
    ///
    /// `repr(C)` makes the declaration order binding; this makes *appending*
    /// binding. Both are needed, and the failure mode is silent: #99's two
    /// per-class arrays were first filed beside the totals they break down,
    /// which moved the counters a request touches on every admission and cost
    /// `admission/full_check` 3.5% while executing no new code on that path.
    /// Appending them gave it back.
    #[test]
    fn later_counters_are_appended_after_the_ones_they_break_down() {
        use std::mem::offset_of;
        let head = offset_of!(AdmissionCounters, admitted);
        assert_eq!(head, 0, "the first counter anchors the calibrated layout");
        for (name, offset) in [
            (
                "capacity_shed_by_class",
                offset_of!(AdmissionCounters, capacity_shed_by_class),
            ),
            (
                "execution_started_by_class",
                offset_of!(AdmissionCounters, execution_started_by_class),
            ),
        ] {
            for (older, older_offset) in [
                ("admitted", head),
                (
                    "canceled_before_start",
                    offset_of!(AdmissionCounters, canceled_before_start),
                ),
                (
                    "capacity_shed",
                    offset_of!(AdmissionCounters, capacity_shed),
                ),
                (
                    "commit_refusals",
                    offset_of!(AdmissionCounters, commit_refusals),
                ),
            ] {
                assert!(
                    offset > older_offset,
                    "{name} was filed ahead of {older}, moving a counter calibrated where it is"
                );
            }
        }
    }

    /// The class slots are a contract with whatever reads the exported
    /// counters, pinned by literal for the reason the refusal slots are.
    /// Renumbering `class_slot` and `CAPACITY_CLASS_NAMES` together stays
    /// internally consistent while silently re-attributing every best-effort
    /// shed to the assured dashboard.
    #[test]
    fn shipped_capacity_class_slots_and_labels_never_move() {
        assert_eq!(class_slot(CapacityClass::Assured), 0);
        assert_eq!(class_slot(CapacityClass::BestEffort), 1);
        assert_eq!(CAPACITY_CLASS_NAMES[0], "Assured");
        assert_eq!(CAPACITY_CLASS_NAMES[1], "BestEffort");
    }

    /// Each class lands in its own slot, and each per-class pair sums to the
    /// total beside it.
    ///
    /// Mutation testing is what made this necessary: `class_slot` could be
    /// replaced with a constant `0` or `1` — collapsing both classes onto one
    /// counter — and every test stayed green, because the only assertions
    /// anywhere read the *totals*. A breakdown that does not break anything
    /// down is worse than no breakdown: an operator reads it to decide whether
    /// the reserve is doing its job.
    #[test]
    fn every_capacity_class_tallies_in_its_own_slot() {
        let counters = AdmissionCounters::new();
        let locality = Locality::current();
        counters.record_capacity_shed_for(CapacityClass::Assured);
        for _ in 0..3 {
            counters.record_capacity_shed_for(CapacityClass::BestEffort);
        }
        for _ in 0..2 {
            counters.record_execution_started_for(CapacityClass::Assured, locality);
        }
        counters.record_execution_started_for(CapacityClass::BestEffort, locality);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.capacity_shed_by_class, [1, 3]);
        assert_eq!(snapshot.execution_started_by_class, [2, 1]);
        // The labelled views are what an exporter reads, and they must carry
        // the same numbers under the same names — an exporter reading an empty
        // or invented pair would show a reserve doing nothing.
        assert_eq!(
            snapshot.capacity_shed_by_class_name().collect::<Vec<_>>(),
            vec![("Assured", 1), ("BestEffort", 3)]
        );
        assert_eq!(
            snapshot
                .execution_started_by_class_name()
                .collect::<Vec<_>>(),
            vec![("Assured", 2), ("BestEffort", 1)]
        );
        // The breakdown never replaces the total: a reader must not have to
        // add two numbers to get one.
        assert_eq!(
            snapshot.capacity_shed,
            snapshot.capacity_shed_by_class.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.execution_started,
            snapshot.execution_started_by_class.iter().sum::<u64>()
        );
    }

    /// Every funding refusal core can produce at execution start reaches a
    /// slot. A refusal that classified to `None` would be counted nowhere
    /// while still refusing the request.
    #[test]
    fn every_commit_time_funding_refusal_reaches_a_slot() {
        let cap = CostUnits(10);
        let spent = CostUnits(10);
        for error in [
            CommitError::Denied(DenyReason::FundingExpiredAtStart),
            CommitError::Denied(DenyReason::OverageCapExhausted {
                spent,
                overage_cap: cap,
            }),
            CommitError::Denied(DenyReason::OverageCapTemporarilyExhausted {
                spent,
                overage_cap: cap,
            }),
            CommitError::Denied(DenyReason::OverageCommitInProgress {
                spent,
                overage_cap: cap,
            }),
            CommitError::Cancelled,
            CommitError::AlreadyReleased,
        ] {
            assert!(
                CommitRefusal::from_commit_error(&error).is_some(),
                "{error:?} must reach a counter slot"
            );
        }
        // A programming error is not an operational outcome, so it
        // deliberately has no slot that could read as one.
        assert_eq!(
            CommitRefusal::from_commit_error(&CommitError::AlreadyCommitted),
            None
        );
    }

    /// Post-admission outcomes never move the pre-admission refusal total.
    #[test]
    fn transition_counters_never_move_the_denied_total() {
        let counters = AdmissionCounters::new();
        counters.record_admit(CostUnits(10));
        counters.record_context_abandoned();
        counters.record_capacity_shed();
        counters.record_execution_started_at(Locality::current());
        counters.record_canceled_before_start_at(Locality::current());
        counters.record_committed_at_overage(CostUnits(10));
        for refusal in ALL_REFUSALS {
            counters.record_commit_refusal(refusal);
        }

        let snapshot = counters.snapshot();
        assert_eq!(
            snapshot.denied(),
            0,
            "no post-admission outcome is a denial"
        );
        assert_eq!(snapshot.refused_at_start(), CommitRefusal::COUNT as u64);
        assert_eq!(snapshot.contexts_abandoned, 1);
        assert_eq!(snapshot.capacity_shed, 1);
        assert_eq!(snapshot.execution_started, 1);
        assert_eq!(snapshot.canceled_before_start, 1);
        assert_eq!(snapshot.committed_at_overage, 1);
        assert_eq!(snapshot.units_committed_at_overage, 10);
    }

    /// The sharded layout must report the same totals as the inline one, for
    /// the transition counters as much as for `admitted`.
    #[test]
    fn a_sharded_layout_reports_the_same_transition_totals() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let counters = AdmissionCounters::with_sharding(sharding);
        for _ in 0..5 {
            counters.record_execution_started_at(Locality::current());
            counters.record_canceled_before_start_at(Locality::current());
        }
        counters.record_capacity_shed();
        counters.record_context_abandoned();

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.execution_started, 5);
        assert_eq!(snapshot.canceled_before_start, 5);
        assert_eq!(snapshot.capacity_shed, 1);
        assert_eq!(snapshot.contexts_abandoned, 1);
    }

    #[test]
    fn configured_sharding_selects_the_matching_counter_layout() {
        assert!(
            AdmissionCounters::with_sharding(LocalSharding::SINGLE)
                .shards
                .is_none()
        );
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        assert_eq!(
            AdmissionCounters::with_sharding(sharding)
                .shards
                .as_ref()
                .map(|shards| shards.len()),
            Some(8)
        );
    }

    /// Each reason must land in its own slot and leave the rest alone.
    #[test]
    fn every_reason_tallies_separately() {
        let counters = AdmissionCounters::new();
        counters.record_deny(&DenyReason::RateLimited);
        counters.record_deny(&DenyReason::RateLimited);
        counters.record_deny(&DenyReason::LeaseExhausted {
            remaining: CostUnits(4),
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.denied(), 3);
        let named: Vec<_> = snapshot
            .denials_by_name()
            .filter(|(_, count)| *count > 0)
            .collect();
        assert_eq!(named, vec![("rate_limited", 2), ("lease_exhausted", 1)]);
        assert_eq!(snapshot.admitted, 0, "a denial is not an admission");
    }

    /// Admissions accumulate units; denials must add none, since a refusal
    /// charges zero (INVARIANTS.md #5).
    #[test]
    fn admissions_accumulate_units_and_denials_do_not() {
        let counters = AdmissionCounters::new();
        counters.record_admit(CostUnits(51));
        counters.record_admit(CostUnits(64));
        counters.record_deny(&DenyReason::UnknownPrincipal);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 2);
        assert_eq!(snapshot.units_admitted, 115);
        assert_eq!(snapshot.denied(), 1);
    }

    /// The overage qualifier is a *subset* of `admitted`, so one call has to
    /// move both pairs. Recording it as a sibling instead would make
    /// `admitted` stop being the total, and every dashboard that reads it
    /// would quietly under-count elastic traffic.
    ///
    /// This also covers the public wrapper itself. `record_admit`,
    /// `record_deny`, and `record_admit_overage` are three
    /// `Locality::current()` wrappers over their `_at` forms; the first two
    /// are exercised by the tests above, and this one was not once the engine
    /// started calling `record_admit_overage_at` directly. A wrapper with no
    /// caller and no test is a wrapper that can be a no-op, which is what the
    /// mutation gate reported (!96).
    #[test]
    fn an_overage_admission_counts_in_both_the_total_and_the_qualifier() {
        let counters = AdmissionCounters::new();
        counters.record_admit(CostUnits(51));
        counters.record_admit_overage(CostUnits(64));

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 2, "the qualifier is not a sibling");
        assert_eq!(snapshot.units_admitted, 115);
        assert_eq!(snapshot.admitted_overage, 1);
        assert_eq!(snapshot.units_admitted_overage, 64);
    }

    /// The qualifier stays unsharded while `admitted` shards, so a sharded
    /// snapshot has to read the two pairs from different places: `admitted`
    /// summed across shards, the qualifier from the inline counters. Reading
    /// the qualifier out of the shards — the obvious symmetry — would report
    /// zero overage on every sharded instance.
    #[test]
    fn a_sharded_snapshot_still_reports_the_unsharded_overage_qualifier() {
        let counters = AdmissionCounters::with_sharding(LocalSharding::new(
            std::num::NonZeroUsize::new(8).unwrap(),
        ));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| counters.record_admit_overage(CostUnits(10)));
            }
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 4, "sharded totals still sum");
        assert_eq!(snapshot.units_admitted, 40);
        assert_eq!(
            snapshot.admitted_overage, 4,
            "the qualifier survives a layout that does not shard it"
        );
        assert_eq!(snapshot.units_admitted_overage, 40);
    }

    /// Concurrent increments must not lose updates — the one guarantee
    /// `Relaxed` still owes us.
    #[test]
    fn concurrent_increments_are_not_lost() {
        let counters = AdmissionCounters::with_sharding(LocalSharding::new(
            std::num::NonZeroUsize::new(8).unwrap(),
        ));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        counters.record_admit(CostUnits(2));
                        counters.record_deny(&DenyReason::RateLimited);
                    }
                });
            }
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 8_000);
        assert_eq!(snapshot.units_admitted, 16_000);
        assert_eq!(snapshot.denials[DenyReason::RateLimited.index()], 8_000);
    }
}
