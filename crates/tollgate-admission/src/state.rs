//! Per-account admission state and the snapshot-map abstraction.

use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering, fence};

use arc_swap::{ArcSwap, ArcSwapOption, Guard};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use jiff::Timestamp;

use tollgate_core::{
    AccountId, AccountOverage, AccountRatePolicy, AccountSnapshot, CostUnits, DenyReason,
    Generation, LocalLease, LocalSharding, Locality, PublishableSnapshot, ResolvedLimits,
};

use crate::counters::AdmissionCounters;

pub use tollgate_core::Principal;

/// The slot a background refill task installs leases into, and the account's
/// overage counter. Shared between the request path (load) and the refill
/// plane (store); per account, and shared by every principal of that account.
///
/// A `None` lease is the cold-start / lost-lease state. Under
/// [`EnforcementMode::Strict`] it denies (`LeaseUnavailable`), keeping
/// INVARIANTS.md GL-5 and GL-10 honest; under `Elastic` it is one of the states
/// the overage counter answers for.
///
/// Every mutation returns the displaced lease through [`Self::replace`] or
/// [`Self::take`]. There is no convenience method that silently drops it.
///
/// ```compile_fail,E0599
/// # use std::sync::Arc;
/// # use tollgate_admission::LeaseSlot;
/// # use tollgate_core::LocalLease;
/// fn rotate(slot: &LeaseSlot, fresh: Arc<LocalLease>) {
///     slot.install(fresh);
/// }
/// ```
///
/// ```compile_fail,E0599
/// # use tollgate_admission::LeaseSlot;
/// fn retire(slot: &LeaseSlot) {
///     slot.clear();
/// }
/// ```
///
/// **The overage counter lives here, and that is a decision worth stating.**
/// It has to outlive individual leases — the account is elastic precisely when
/// it has no usable lease — so it cannot hang off `LocalLease`. It also has to
/// outlive snapshot installs, which rules out `AccountAdmissionState`, rebuilt
/// on every publish. And it must not live beside the rate-limiter registry,
/// whose entries are `Weak` and swept: that is safe only because a rebuilt
/// limiter costs a full bucket, whereas a rebuilt overage counter silently
/// resets a spend cap. This slot is held by a strong `Arc` in the client's
/// `SlotRegistry`, created on first use and never reclaimed, which is exactly
/// the lifetime a spend cap needs.
///
/// **The counter stays unsharded while the lease views shard**, and that is
/// also deliberate. Sharding the lease splits a *grant* N ways, which is
/// sound because the shards' remainders sum to the grant. A spend cap is not
/// divisible the same way: N per-locality caps summing to the configured cap
/// would refuse an elastic request on a saturated core while headroom sat
/// unreachable on another, and one cap of `N × cap` would silently raise it.
/// The counter is therefore one atomic per account, contended only on the
/// path that by definition has no usable lease to spend from.
///
/// [`EnforcementMode::Strict`]: tollgate_core::EnforcementMode::Strict
#[derive(Debug)]
pub struct LeaseSlot {
    current: LeaseSlotCurrent,
    sharding: LocalSharding,
    overage: Arc<AccountOverage>,
    funding: std::sync::Mutex<FundingObservation>,
    /// Floored second before which `evidence_remaining` is authoritative;
    /// `i64::MIN` when no evidence is live.
    evidence_until: AtomicI64,
    evidence_remaining: AtomicU64,
    /// Seqlock word for the pair above: odd while a publication writes it.
    /// Deadlines cannot serve as the sequence, because two publications may
    /// share one and bracket a third (see [`LeaseSlot::funding_evidence`]).
    evidence_sequence: AtomicU64,
    /// Lost debit races handed over by leases that have left this slot: the
    /// account's contention history, which outlives any one grant. Written on
    /// the control plane only, at publication.
    contention_retired: AtomicU64,
    /// Lost exchanges on the account's concurrency gauges (GL-139), which live
    /// in the admission state rather than here; recorded only when nonzero.
    /// On a line of its own: it is written exactly when the account's threads
    /// contend, and an unpadded field could share a line with `current`, which
    /// every request reads.
    contention_gauges: PaddedCount,
}

/// A counter on a cache line of its own.
#[repr(align(128))]
#[derive(Debug, Default)]
struct PaddedCount(AtomicU64);

/// Control-plane publication state. The identity token prevents an older
/// allocator response from undoing a later grant or funding snapshot.
#[derive(Debug, Default)]
struct FundingObservation {
    epoch: Arc<()>,
    last_grant: u64,
    snapshot: Option<(
        Option<tollgate_core::BudgetView>,
        tollgate_core::EnforcementMode,
    )>,
}

/// An allocator attempt tied to this account's current funding observation.
/// Dropping an unanswered attempt publishes nothing; only a verified domain
/// response may publish funding evidence. All synchronization here is control
/// plane.
#[derive(Debug)]
pub struct FundingAttempt<'a> {
    slot: &'a LeaseSlot,
    epoch: Arc<()>,
}

impl FundingAttempt<'_> {
    /// Record a refusal's authoritative evidence unless a newer funding
    /// observation won. Exhaustion is the zero-remaining case.
    pub fn shortfall(self, evidence: tollgate_core::BalanceShortfall) {
        let current = self
            .slot
            .funding
            .lock()
            .expect("funding publication poisoned");
        if Arc::ptr_eq(&current.epoch, &self.epoch) {
            self.slot.publish_evidence(evidence);
        }
    }

    /// Install the lease this attempt was granted, with the evidence that
    /// came with it, as one control-plane step.
    ///
    /// A new grant invalidates older evidence, as [`LeaseSlot::replace`] does;
    /// the grant's own evidence is then published under the same lock, so the
    /// install cannot clear it. It is published only if no funding change was
    /// accepted while the call was outstanding: a top-up the allocator had not
    /// yet seen would otherwise be hidden behind an older, smaller remaining.
    #[must_use = "retain the superseded lease for quiesced release, or explicitly abandon it"]
    pub fn granted(
        self,
        lease: Arc<LocalLease>,
        evidence: Option<tollgate_core::BalanceShortfall>,
    ) -> Option<Arc<LocalLease>> {
        let mut current = self
            .slot
            .funding
            .lock()
            .expect("funding publication poisoned");
        let current_epoch = Arc::ptr_eq(&current.epoch, &self.epoch);
        self.slot.observe_grant(&mut current, &lease);
        if current_epoch && let Some(evidence) = evidence {
            self.slot.publish_evidence(evidence);
        }
        self.slot.publish(Some(lease))
    }
}

/// One comparison for both deadline reads, so the unsynchronized fast-path
/// read and the sequence-checked read cannot disagree about the boundary.
fn evidence_live(now_second: i64, until: i64) -> bool {
    now_second < until
}

// Jiff truncates negative fractional timestamps toward zero. Floor both
// sides of the deadline comparison instead. Timestamp's civil range is far
// inside i64 seconds, so subtracting one cannot overflow.
fn funding_second(at: Timestamp) -> i64 {
    at.as_second() - i64::from(at.subsec_nanosecond() < 0)
}

#[derive(Debug)]
enum LeaseSlotCurrent {
    Single(ArcSwapOption<LocalLease>),
    /// One published view per locality, plus the mutex that keeps a
    /// multi-view publication indivisible *between mutators*.
    ///
    /// A single-view slot publishes in one swap, so two mutators can only
    /// order themselves. N swaps cannot: without this lock a `take` racing a
    /// `replace` would interleave and leave some localities empty and others
    /// holding the fresh lease — a mixed state the single-view slot cannot
    /// reach, and one that would let a locality keep spending after a
    /// revocation. Publication is control-plane work; `load_at` never takes
    /// the lock, so the request path is unaffected.
    Sharded {
        publish: std::sync::Mutex<()>,
        views: Box<[ArcSwapOption<LocalLease>]>,
    },
}

impl LeaseSlotCurrent {
    fn sharded(shards: usize) -> Self {
        Self::Sharded {
            publish: std::sync::Mutex::new(()),
            views: (0..shards)
                .map(|_| ArcSwapOption::const_empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

impl LeaseSlot {
    /// An empty single-view slot for `account`, with its overage counter at
    /// zero.
    ///
    /// Takes the account id because the overage counter is the billing
    /// identity for spend that has no lease to borrow one from: an overage
    /// usage event names this account and nothing else.
    #[must_use]
    pub fn for_account(account: AccountId) -> Arc<Self> {
        Self::with_sharding(account, LocalSharding::SINGLE)
    }

    /// Create an empty slot for `account` whose refill manager will install
    /// leases with the selected instance-local layout.
    #[must_use]
    pub fn with_sharding(account: AccountId, sharding: LocalSharding) -> Arc<Self> {
        let current = if sharding == LocalSharding::SINGLE {
            LeaseSlotCurrent::Single(ArcSwapOption::const_empty())
        } else {
            LeaseSlotCurrent::sharded(sharding.get())
        };
        Arc::new(Self {
            current,
            sharding,
            overage: Arc::new(AccountOverage::new(account)),
            funding: std::sync::Mutex::new(FundingObservation::default()),
            evidence_until: AtomicI64::new(i64::MIN),
            evidence_remaining: AtomicU64::new(0),
            evidence_sequence: AtomicU64::new(0),
            contention_retired: AtomicU64::new(0),
            contention_gauges: PaddedCount::default(),
        })
    }

    /// Capture before starting an allocator call; no request-path caller
    /// needs this control-plane operation.
    pub fn funding_attempt(&self) -> FundingAttempt<'_> {
        let current = self.funding.lock().expect("funding publication poisoned");
        FundingAttempt {
            slot: self,
            epoch: Arc::clone(&current.epoch),
        }
    }

    fn invalidate_funding(&self, current: &mut FundingObservation) {
        current.epoch = Arc::new(());
        self.evidence_until.store(i64::MIN, Ordering::Release);
    }

    /// Caller holds `funding`, so writers are serialized: a seqlock writer.
    /// The sequence is odd for the whole write, which is what lets
    /// [`Self::funding_evidence`] reject a pair that straddles a publication.
    fn publish_evidence(&self, evidence: tollgate_core::BalanceShortfall) {
        // Floor to seconds: a subsecond boundary can only invalidate the
        // evidence early, never extend its lifetime. Admission makes no
        // calendar or clock call to read it.
        let until = evidence.period_end.map_or(i64::MAX, funding_second);
        let sequence = self.evidence_sequence.load(Ordering::Relaxed);
        self.evidence_sequence
            .store(sequence.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.evidence_until.store(until, Ordering::Relaxed);
        self.evidence_remaining
            .store(evidence.remaining.get(), Ordering::Relaxed);
        self.evidence_sequence
            .store(sequence.wrapping_add(2), Ordering::Release);
    }

    fn observe_grant(&self, current: &mut FundingObservation, lease: &LocalLease) {
        let fence = lease.grant().fencing_token.0;
        if fence > current.last_grant {
            current.last_grant = fence;
            self.invalidate_funding(current);
        }
    }

    fn observe_funding(&self, snapshot: &AccountSnapshot) {
        let mut current = self.funding.lock().expect("funding publication poisoned");
        // The map has already accepted this publication for its principal.
        // Generations from different principals cannot be ordered here: this
        // slot is shared by the account, whose funding changes invalidate all
        // outstanding attempts regardless of the publishing principal.
        let next = (snapshot.budget, snapshot.enforcement_mode);
        if current.snapshot != Some(next) {
            self.invalidate_funding(&mut current);
        }
        current.snapshot = Some(next);
    }

    /// The account's evidenced remaining funding, read only after local
    /// funding has refused a request. An upper bound on what the account can
    /// still spend; zero is confirmed exhaustion.
    ///
    /// One atomic load when no evidence is live, which is every refusal in an
    /// account nobody has attested: a torn "no evidence" answer is always
    /// safe, because it only falls back to the lease refusal's own advice.
    /// Live evidence is read as a seqlock reader reads, four more loads, and a
    /// pair that straddled a publication is discarded the same way.
    /// Invalidation clears only the deadline, so a read racing it is ordered
    /// before it; publication is what the sequence guards.
    #[must_use]
    pub fn funding_evidence(&self, now: Timestamp) -> Option<CostUnits> {
        self.evidence_pair(funding_second(now))
            .map(|(_, remaining)| CostUnits(remaining))
    }

    /// The accepted `(deadline, remaining)` pair; split out so tests can check
    /// that both halves came from one publication.
    fn evidence_pair(&self, now_second: i64) -> Option<(i64, u64)> {
        if !evidence_live(now_second, self.evidence_until.load(Ordering::Relaxed)) {
            return None;
        }
        let before = self.evidence_sequence.load(Ordering::Acquire);
        let until = self.evidence_until.load(Ordering::Relaxed);
        let remaining = self.evidence_remaining.load(Ordering::Relaxed);
        fence(Ordering::Acquire);
        let after = self.evidence_sequence.load(Ordering::Relaxed);
        (before == after && before.is_multiple_of(2) && evidence_live(now_second, until))
            .then_some((until, remaining))
    }

    /// The instance-local layout of this slot's lease views, fixed at
    /// construction; an installed lease is published to every view.
    #[must_use]
    pub fn sharding(&self) -> LocalSharding {
        self.sharding
    }

    /// The account's unfunded-spend counter, shared by every principal of the
    /// account, by every lease that passes through this slot, and by every
    /// locality this slot publishes to.
    #[must_use]
    pub fn overage(&self) -> &Arc<AccountOverage> {
        &self.overage
    }

    /// Install `lease`, returning the previously installed lease if one was
    /// present. The refill plane uses this to retain every superseded grant
    /// until it can be released safely.
    ///
    /// Retain the returned handle until every in-flight reservation and local
    /// view has quiesced, then release its unspent units to the allocator.
    /// Dropping it instead deliberately leaves recovery to TTL reclamation.
    ///
    /// ```compile_fail
    /// #![deny(unused_must_use)]
    /// # use std::sync::Arc;
    /// # use tollgate_admission::LeaseSlot;
    /// # use tollgate_core::LocalLease;
    /// fn rotate(slot: &LeaseSlot, fresh: Arc<LocalLease>) {
    ///     slot.replace(fresh); // The superseded grant needs a disposition.
    /// }
    /// ```
    #[must_use = "retain the superseded lease for quiesced release, or explicitly abandon it"]
    pub fn replace(&self, lease: Arc<LocalLease>) -> Option<Arc<LocalLease>> {
        let mut funding = self.funding.lock().expect("funding publication poisoned");
        // Restoring the same lease after a refused consolidation is not a
        // funding change. Serialize the clear and publication with evidence.
        self.observe_grant(&mut funding, &lease);
        self.publish(Some(lease))
    }

    /// Remove every published local view and return one handle to their
    /// shared lease state. A concurrent request may already have loaded a
    /// view, exactly as it could have loaded the single-view slot before its
    /// swap; quiescence detection accounts for that handle.
    ///
    /// As with [`Self::replace`], retain it for quiesced release. An explicit
    /// `drop(slot.take())` abandons it to TTL reclamation; removal alone does
    /// not return units to the allocator. The overage counter is untouched:
    /// losing a lease is not a funding event and cannot reset a spend cap.
    ///
    /// ```compile_fail
    /// #![deny(unused_must_use)]
    /// # use tollgate_admission::LeaseSlot;
    /// fn retire(slot: &LeaseSlot) {
    ///     slot.take(); // The removed grant needs a disposition.
    /// }
    /// ```
    #[must_use = "retain the removed lease for quiesced release, or explicitly abandon it"]
    pub fn take(&self) -> Option<Arc<LocalLease>> {
        self.publish(None)
    }

    /// Publish `next` to every local view, returning one handle to whatever
    /// the slot held before.
    ///
    /// Mutators are serialized, so a slot never ends a publication holding a
    /// mixture of two leases: readers see the outgoing or the incoming lease
    /// per locality while one publication is in flight, exactly as a
    /// single-view slot's readers straddle its one swap, and every locality
    /// has converged on the incoming lease by the time this returns.
    ///
    /// A sharded slot publishes an independently reference-counted handle to
    /// each view rather than sharing one: that is what keeps a routine
    /// request's `Arc` traffic off the other localities' cache lines, and
    /// quiescence still sees through every alias to the shared inner state.
    fn publish(&self, next: Option<Arc<LocalLease>>) -> Option<Arc<LocalLease>> {
        let replaced = self.swap_views(next);
        // The outgoing lease's contention joins the account's history here,
        // on the one path every rotation and removal takes. The lease itself
        // remembers what it handed over, so reinstalling it later cannot count
        // the same races twice.
        if let Some(outgoing) = &replaced {
            self.contention_retired
                .fetch_add(outgoing.take_unreported_contention(), Ordering::Relaxed);
        }
        replaced
    }

    /// How many of this account's admission exchanges lost a compare-exchange
    /// to another writer: lease debits across every lease the slot has held
    /// (GL-134), overage debits, and principal and account concurrency-gauge
    /// acquisitions (GL-139).
    ///
    /// The account-level contention signal: nonzero means one of the account's
    /// admission lines was written from more than one core at the same moment. It is a
    /// **lower bound** — see [`LocalLease::contended_debits`] for what cannot
    /// be observed — and a read that races a rotation can transiently miss
    /// the outgoing lease's newest races; it never counts one twice. Read it
    /// as a rate between two reads, not as an absolute. Control plane only.
    #[must_use]
    pub fn contended_exchanges(&self) -> u64 {
        // Retired first, current second: a rotation between the two reads
        // then drops the outgoing lease's newest races rather than adding
        // them to both terms.
        let retired = self.contention_retired.load(Ordering::Relaxed);
        retired
            .saturating_add(
                self.load_observed()
                    .map_or(0, |lease| lease.unreported_contention()),
            )
            .saturating_add(self.overage.contended_debits())
            .saturating_add(self.contention_gauges.0.load(Ordering::Relaxed))
    }

    /// Record `lost` exchanges from a retry loop outside the lease, if any.
    #[inline]
    pub(crate) fn note_contention(&self, lost: u64) {
        if lost != 0 {
            self.contention_gauges.0.fetch_add(lost, Ordering::Relaxed);
        }
    }

    fn swap_views(&self, next: Option<Arc<LocalLease>>) -> Option<Arc<LocalLease>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => current.swap(next),
            LeaseSlotCurrent::Sharded { publish, views } => {
                let _publishing = publish.lock().expect("lease slot publication poisoned");
                let mut replaced = None;
                for view in views {
                    let view_lease = next.as_ref().map(|lease| Arc::new((**lease).clone()));
                    let old = view.swap(view_lease);
                    if replaced.is_none() {
                        replaced = old;
                    }
                }
                replaced
            }
        }
    }

    /// Every published view, in locality order. Publication is only
    /// meaningfully indivisible if a test can look at all of them at once.
    #[cfg(test)]
    fn published_views(&self) -> Vec<Option<Arc<LocalLease>>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => vec![current.load_full()],
            LeaseSlotCurrent::Sharded { views, .. } => views
                .iter()
                .map(arc_swap::ArcSwapOption::load_full)
                .collect(),
        }
    }

    /// The installed lease as seen from the calling thread's locality, or
    /// `None` when no lease is installed (the cold-start or lost-lease
    /// state). Resolves [`Locality::current`], which assigns the thread an
    /// affinity on first use; a control-plane reader should use
    /// [`load_observed`](Self::load_observed) instead.
    #[must_use]
    pub fn load(&self) -> Option<Arc<LocalLease>> {
        self.load_at(Locality::current())
    }

    /// Read the slot without claiming an affinity for the reading thread.
    ///
    /// [`load`](Self::load) resolves [`Locality::current`], which *assigns* a
    /// number the first time a thread asks. That is right for a request, which
    /// wants the shard it will keep using, and wrong for the control plane,
    /// which is only asking whether a grant is present and until when — facts
    /// every view answers identically, since the views of one slot share their
    /// accounting and their grant. A background task that reads through
    /// `load` on a thread that never serves a request spends an affinity on
    /// that answer, and every affinity spent moves a request-serving thread one
    /// step closer to sharing a shard with a peer (GL-124).
    #[must_use]
    pub fn load_observed(&self) -> Option<Arc<LocalLease>> {
        self.load_at(Locality::OBSERVER)
    }

    #[must_use]
    pub(crate) fn load_at(&self, locality: Locality) -> Option<Arc<LocalLease>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => current.load_full(),
            LeaseSlotCurrent::Sharded { views, .. } => {
                views[locality.index(self.sharding)].load_full()
            }
        }
    }
}

type AccountRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[repr(align(128))]
#[derive(Debug)]
pub(crate) struct RateShard(AccountRateLimiter);

#[derive(Debug)]
pub(crate) enum Buckets {
    Single(AccountRateLimiter),
    Sharded(Box<[RateShard]>),
}

impl Buckets {
    pub(crate) fn check_n_at(
        &self,
        n: std::num::NonZeroU32,
        locality: Locality,
    ) -> Result<
        Result<(), governor::NotUntil<governor::clock::QuantaInstant>>,
        governor::InsufficientCapacity,
    > {
        match self {
            Self::Single(limiter) => limiter.check_n(n),
            Self::Sharded(shards) => {
                let sharding = LocalSharding::new(
                    std::num::NonZeroUsize::new(shards.len())
                        .expect("a sharded rate state is non-empty"),
                );
                let first = locality.index(sharding);
                let mut denied = None;
                let mut insufficient = None;
                for offset in 0..shards.len() {
                    let index = (first + offset) % shards.len();
                    match shards[index].0.check_n(n) {
                        Ok(Ok(())) => return Ok(Ok(())),
                        // Keep the first refillable denial in the
                        // locality-first scan. If the local shard cannot
                        // hold n, a sibling supplies it instead. Admission
                        // exposes only the rate-limit category and discards
                        // governor's timing; no earliest retry is promised.
                        Ok(Err(denial)) => {
                            denied.get_or_insert(denial);
                        }
                        // A shard too small to ever hold `n` says nothing
                        // about its siblings — the remainder of a partition
                        // leaves them up to one unit larger. Keep looking,
                        // and report this only if no shard can take the
                        // request now or later (INVARIANTS.md GL-5).
                        Err(error) => insufficient = Some(error),
                    }
                }
                match denied {
                    Some(denial) => Ok(Err(denial)),
                    None => Err(insufficient.expect("a sharded rate state is non-empty")),
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn shard_count(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Sharded(shards) => shards.len(),
        }
    }
}

/// One immutable rate configuration and its mutable governor buckets.
///
/// The account limiter publishes this behind one account-wide indirection.
/// Every request loads that indirection once, so replacing an immutable
/// governor quota cannot leave principals spending from independently
/// refillable old and new buckets.
#[derive(Debug)]
pub(crate) struct RateState {
    policy: AccountRatePolicy,
    weighted: Option<Arc<ConfiguredBuckets>>,
    requests: Option<Arc<ConfiguredBuckets>>,
}

/// One rate dimension's immutable parameters and mutable governor state.
///
/// Keeping these together lets publication retain the exact mutable bucket
/// when, and only when, that dimension's enforced parameters are unchanged.
#[derive(Debug)]
struct ConfiguredBuckets {
    params: BucketParams,
    buckets: Buckets,
}

/// The account-wide policy authority loaded once by each request.
///
/// Rate configuration and the account concurrency ceiling share a generation
/// winner and one publication point. Principal-local shaping and concurrency
/// remain in the principal snapshot.
#[derive(Debug)]
pub(crate) struct AccountPolicyState {
    rate: Arc<RateState>,
    max_concurrent_requests: Option<std::num::NonZeroU32>,
}

impl AccountPolicyState {
    pub(crate) fn rate(&self) -> &RateState {
        &self.rate
    }

    pub(crate) fn max_concurrent_requests(&self) -> Option<std::num::NonZeroU32> {
        self.max_concurrent_requests
    }
}

impl RateState {
    pub(crate) fn policy(&self) -> AccountRatePolicy {
        self.policy
    }

    pub(crate) fn weighted(&self) -> Option<&Buckets> {
        self.weighted
            .as_deref()
            .map(|configured| &configured.buckets)
    }

    pub(crate) fn requests(&self) -> Option<&Buckets> {
        self.requests
            .as_deref()
            .map(|configured| &configured.buckets)
    }
}

const CONCURRENCY_SHARDED: u8 = 0;
const CONCURRENCY_DRAINING: u8 = 1;
const CONCURRENCY_CENTRAL: u8 = 2;

/// Exact release evidence for one concurrency acquisition.
///
/// `usize::MAX` denotes the central bounded counter; every other value is the
/// locality shard incremented by an unbounded acquisition.
#[derive(Debug, Clone, Copy)]
struct GaugePermit(usize);

impl GaugePermit {
    const CENTRAL: Self = Self(usize::MAX);

    const fn shard(index: usize) -> Self {
        Self(index)
    }

    const fn shard_index(self) -> Option<usize> {
        if self.0 == usize::MAX {
            None
        } else {
            Some(self.0)
        }
    }
}

/// One cache-isolated occupancy counter.
#[derive(Debug, Default)]
#[repr(align(128))]
struct ConcurrencyCounter(AtomicU32);

impl ConcurrencyCounter {
    /// `lost` gains one for every compare-exchange another writer won (GL-139).
    /// A register the caller owns, so an uncontended increment writes nothing
    /// more than it did.
    fn try_increment(&self, limit: Option<std::num::NonZeroU32>, lost: &mut u64) -> bool {
        let mut current = self.0.load(Ordering::Relaxed);
        loop {
            if limit.is_some_and(|limit| current >= limit.get()) {
                return false;
            }
            let Some(next) = current.checked_add(1) else {
                return false;
            };
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => {
                    *lost += 1;
                    current = observed;
                }
            }
        }
    }

    fn decrement(&self) -> u32 {
        let previous = self.0.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0, "a concurrency permit releases exactly once");
        previous - 1
    }

    fn load(&self) -> u32 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Locality-partitioned occupancy used while no ceiling has ever activated.
#[derive(Debug)]
enum ConcurrencyShards {
    Single(ConcurrencyCounter),
    Sharded {
        sharding: LocalSharding,
        counters: Box<[ConcurrencyCounter]>,
    },
}

impl ConcurrencyShards {
    fn new(sharding: LocalSharding) -> Self {
        if sharding == LocalSharding::SINGLE {
            return Self::Single(ConcurrencyCounter::default());
        }
        Self::Sharded {
            sharding,
            counters: (0..sharding.get())
                .map(|_| ConcurrencyCounter::default())
                .collect(),
        }
    }

    fn select(&self, locality: Locality) -> (usize, &ConcurrencyCounter) {
        match self {
            Self::Single(counter) => (0, counter),
            Self::Sharded { sharding, counters } => {
                let index = locality.index(*sharding);
                (index, &counters[index])
            }
        }
    }

    fn get(&self, index: usize) -> &ConcurrencyCounter {
        match self {
            Self::Single(counter) => {
                debug_assert_eq!(index, 0);
                counter
            }
            Self::Sharded { counters, .. } => &counters[index],
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Single(counter) => counter.load() == 0,
            Self::Sharded { counters, .. } => counters.iter().all(|counter| counter.load() == 0),
        }
    }

    /// Outstanding permits across every shard.
    ///
    /// Only the activation handoff reads this: ordinary acquisition and
    /// release stay direct-indexed on one counter.
    fn total(&self) -> u32 {
        match self {
            Self::Single(counter) => counter.load(),
            Self::Sharded { counters, .. } => counters
                .iter()
                .fold(0u32, |total, counter| total.saturating_add(counter.load())),
        }
    }
}

/// Stable occupancy with an explicit sharded-to-central activation handoff.
///
/// Unlimited traffic increments only its locality shard. First activation
/// closes new *shard* acquisitions and admits centrally against the ceiling
/// less the shard residue, so publishing a ceiling narrows admission to that
/// ceiling instead of suspending it for the lifetime of the longest request
/// already running. Draining those exact permits publishes the central
/// CAS-bounded counter. Once central, disabling a limit keeps tracking there
/// so re-enabling cannot manufacture a fresh zero.
#[derive(Debug)]
struct ConcurrencyGauge {
    phase: AtomicU8,
    shards: ConcurrencyShards,
    central: ConcurrencyCounter,
}

impl ConcurrencyGauge {
    fn new(sharding: LocalSharding, limited: bool) -> Self {
        Self {
            phase: AtomicU8::new(if limited {
                CONCURRENCY_CENTRAL
            } else {
                CONCURRENCY_SHARDED
            }),
            shards: ConcurrencyShards::new(sharding),
            central: ConcurrencyCounter::default(),
        }
    }

    /// Prepare the stable gauge before publishing a newly enabled ceiling.
    fn configure_limit(&self, limited: bool) {
        if limited {
            let _transition = self.phase.compare_exchange(
                CONCURRENCY_SHARDED,
                CONCURRENCY_DRAINING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.promote_if_drained();
        } else {
            // A gauge that reached central remains there: its count is the
            // evidence a later re-enable must retain. Only an activation that
            // has not admitted central work can return to sharded tracking.
            let _transition = self.phase.compare_exchange(
                CONCURRENCY_DRAINING,
                CONCURRENCY_SHARDED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }

    fn promote_if_drained(&self) {
        if self.phase.load(Ordering::SeqCst) == CONCURRENCY_DRAINING && self.shards.is_empty() {
            let _transition = self.phase.compare_exchange(
                CONCURRENCY_DRAINING,
                CONCURRENCY_CENTRAL,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }

    fn try_acquire(
        &self,
        limit: Option<std::num::NonZeroU32>,
        locality: Locality,
        lost: &mut u64,
    ) -> Option<GaugePermit> {
        loop {
            match self.phase.load(Ordering::SeqCst) {
                CONCURRENCY_SHARDED if limit.is_none() => {
                    let (index, counter) = self.shards.select(locality);
                    if !counter.try_increment(None, lost) {
                        return None;
                    }
                    if self.phase.load(Ordering::SeqCst) == CONCURRENCY_SHARDED {
                        return Some(GaugePermit::shard(index));
                    }
                    // An activation raced this increment, so the permit is not
                    // one the handoff can account for. Undo it and re-enter
                    // through the phase that is now published; a caller with
                    // no ceiling of its own must not be denied by someone
                    // else's activation.
                    self.release_shard(index);
                }
                CONCURRENCY_SHARDED => {
                    self.configure_limit(true);
                }
                CONCURRENCY_DRAINING => {
                    // Activation is a handoff, not an outage. Work already
                    // counted on the locality shards is real in-flight work
                    // and occupies the ceiling being published; everything it
                    // leaves over is admitted centrally. Shards only shrink
                    // while draining, so a scan that races a release is
                    // conservative and a bounded central counter plus the
                    // scanned residue can never exceed the ceiling.
                    let Some(limit) = limit else {
                        return self
                            .central
                            .try_increment(None, lost)
                            .then_some(GaugePermit::CENTRAL);
                    };
                    let headroom = limit.get().saturating_sub(self.shards.total());
                    return std::num::NonZeroU32::new(headroom)
                        .is_some_and(|headroom| self.central.try_increment(Some(headroom), lost))
                        .then_some(GaugePermit::CENTRAL);
                }
                CONCURRENCY_CENTRAL => {
                    return self
                        .central
                        .try_increment(limit, lost)
                        .then_some(GaugePermit::CENTRAL);
                }
                _ => unreachable!("concurrency phase is internal"),
            }
        }
    }

    fn release(&self, permit: GaugePermit) {
        match permit.shard_index() {
            Some(index) => self.release_shard(index),
            None => {
                self.central.decrement();
            }
        }
    }

    fn release_shard(&self, index: usize) {
        if self.shards.get(index).decrement() == 0
            && self.phase.load(Ordering::SeqCst) == CONCURRENCY_DRAINING
        {
            // Only the last permit on each formerly active shard scans, and
            // only during the one-time activation handoff. Ordinary request
            // acquisition remains direct-indexed.
            self.promote_if_drained();
        }
    }

    #[cfg(test)]
    fn in_flight(&self) -> u32 {
        self.shards.total().saturating_add(self.central.load())
    }
}

/// Stable principal-local occupancy. The map registry holds only a `Weak`;
/// installed and in-flight states keep the gauge alive until occupancy is
/// back at zero, including across cache eviction and reinstall.
#[derive(Debug)]
pub(crate) struct PrincipalGauge(ConcurrencyGauge);

impl PrincipalGauge {
    fn new(sharding: LocalSharding, limited: bool) -> Self {
        Self(ConcurrencyGauge::new(sharding, limited))
    }
}

/// Stable per-account indirection shared by every principal.
///
/// Rate state is replaced because governor quotas are immutable, while the
/// concurrency gauge stays here so a publication cannot reset live occupancy.
#[derive(Debug)]
pub(crate) struct AccountLimiter {
    current: ArcSwap<AccountPolicyState>,
    config: std::sync::Mutex<LimiterConfig>,
    concurrency: ConcurrencyGauge,
}

#[derive(Debug)]
struct LimiterConfig {
    /// Highest generation whose limits are installed below.
    generation: Generation,
    /// Exact account-rate policy represented by `installed` and `current`.
    policy: AccountRatePolicy,
    /// Exact account-wide concurrency ceiling represented by `current`.
    max_concurrent_requests: Option<std::num::NonZeroU32>,
    installed: RateParams,
    /// Finest split every principal of this account can still spend in.
    ///
    /// The bucket is account-wide but the largest quote is a property of one
    /// principal's cost table, so the split is only safe at the *most*
    /// constraining principal's ceiling. Sizing it from whichever snapshot
    /// installed last would leave a principal with a heavier table unable to
    /// spend its largest quote in any shard — a permanent
    /// `UnpriceableUnderLimits` for a request the account's burst can hold.
    /// Maps accept the principal generation before resolving this state.
    /// Among accepted snapshots, an older account-policy generation can
    /// still supply quote evidence for another admitting principal and tighten
    /// the split. A rejected replay never reaches this update.
    ///
    /// It never widens again. Doing so would need per-principal evidence with
    /// its own eviction story for churned credentials, and the whole cost of
    /// holding the floor is a coarser split — less cache isolation for an
    /// account that once carried a heavier table, never a wrong admission
    /// decision. A limiter is dropped with its last principal, so the floor
    /// does not outlive the account's presence in the map.
    shard_ceiling: usize,
}

impl AccountLimiter {
    fn new(
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) -> Self {
        let shard_ceiling = shard_ceiling(limits, maximum_quote);
        let params = rate_params(limits, sharding, shard_ceiling);
        let policy = limits.account_rate_policy();
        let rate = Arc::new(build_rate_state(policy, params));
        let max_concurrent_requests = limits.max_concurrent_requests();
        Self {
            current: ArcSwap::from_pointee(AccountPolicyState {
                rate,
                max_concurrent_requests,
            }),
            config: std::sync::Mutex::new(LimiterConfig {
                generation,
                policy,
                max_concurrent_requests,
                installed: params,
                shard_ceiling,
            }),
            concurrency: ConcurrencyGauge::new(sharding, max_concurrent_requests.is_some()),
        }
    }

    fn update(
        &self,
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) -> Arc<AccountPolicyState> {
        let mut config = self.config.lock().expect("limiter config poisoned");
        config.shard_ceiling = config
            .shard_ceiling
            .min(shard_ceiling(limits, maximum_quote));
        // Strictly newer, so the first accepted snapshot installed at a
        // generation owns its account policy. If another principal at that
        // generation differs, one stable account answer beats oscillating
        // with install order; every request loads that answer below.
        let (policy, max_concurrent_requests, params) = if generation > config.generation {
            config.generation = generation;
            (
                limits.account_rate_policy(),
                limits.max_concurrent_requests(),
                rate_params(limits, sharding, config.shard_ceiling),
            )
        } else {
            // Too old to install its policy, but its maximum quote is still
            // valid safety evidence for a currently admitting principal. It
            // may tighten the layout published for subsequent requests.
            (
                config.policy,
                config.max_concurrent_requests,
                RateParams {
                    weighted: config.installed.weighted.map(|params| BucketParams {
                        shards: shard_count(params.rate, sharding, config.shard_ceiling),
                        ..params
                    }),
                    ..config.installed
                },
            )
        };
        if policy != config.policy
            || max_concurrent_requests != config.max_concurrent_requests
            || params != config.installed
        {
            if max_concurrent_requests != config.max_concurrent_requests {
                // Close or relax the stable occupancy state before publishing
                // the policy that asks request threads to enforce it.
                self.concurrency
                    .configure_limit(max_concurrent_requests.is_some());
            }
            let current = self.current.load_full();
            let rate = if policy == config.policy && params == config.installed {
                Arc::clone(&current.rate)
            } else {
                Arc::new(update_rate_state(policy, params, &current.rate))
            };
            let next = Arc::new(AccountPolicyState {
                rate,
                max_concurrent_requests,
            });
            self.current.store(Arc::clone(&next));
            config.policy = policy;
            config.max_concurrent_requests = max_concurrent_requests;
            config.installed = params;
            return next;
        }
        self.current.load_full()
    }

    pub(crate) fn load(&self) -> Guard<Arc<AccountPolicyState>> {
        self.current.load()
    }

    #[cfg(test)]
    pub(crate) fn current(&self) -> Arc<AccountPolicyState> {
        self.current.load_full()
    }
}

/// Everything the request path needs for one principal, resolved to a single
/// `Arc`: the compiled principal snapshot, stable account/principal state,
/// and the account's lease slot.
#[derive(Debug)]
#[repr(align(128))]
pub struct AccountAdmissionState {
    /// The compiled principal snapshot this state was built from; immutable
    /// for the state's lifetime.
    pub snapshot: Arc<AccountSnapshot>,
    pub(crate) counters: Arc<AdmissionCounters>,
    pub(crate) limiter: Arc<AccountLimiter>,
    pub(crate) principal_gauge: Arc<PrincipalGauge>,
    /// The account's lease slot, shared by every principal of the account and
    /// surviving snapshot installs.
    pub lease: Arc<LeaseSlot>,
    /// The instance's admitted-unit total at the instant this snapshot was
    /// installed, so the balance estimate can subtract only what has been
    /// spent *since* the ledger reported it (GL-97).
    ///
    /// Captured here rather than reset on the counter because the counter is
    /// account-wide, monotonic, and shared by every principal and every
    /// generation — an exported tally nothing may rewind. A per-snapshot
    /// baseline gets the same subtraction without touching it, and the
    /// baseline and `snapshot.budget` are then true of exactly the same
    /// instant, which is the only thing that makes their difference mean
    /// anything.
    units_admitted_at_publish: u64,
}

impl AccountAdmissionState {
    /// Compile the runtime state for a snapshot. The limiter comes from the
    /// map's per-account registry, never per principal — see
    /// [`AdmissionStateRegistry`]. Account-wide rate and concurrency policy is
    /// deliberately not copied into this principal-pinned object: each request
    /// loads the limiter's single current authority instead.
    #[must_use]
    pub(crate) fn new(
        snapshot: Arc<AccountSnapshot>,
        counters: Arc<AdmissionCounters>,
        lease: Arc<LeaseSlot>,
        limiter: Arc<AccountLimiter>,
        principal_gauge: Arc<PrincipalGauge>,
    ) -> Arc<Self> {
        lease.observe_funding(&snapshot);
        Arc::new(AccountAdmissionState {
            units_admitted_at_publish: counters.units_admitted(),
            snapshot,
            counters,
            limiter,
            principal_gauge,
            lease,
        })
    }

    /// What the account can still spend this period, as well as this instance
    /// can know it.
    ///
    /// `None` when the control plane published no budget view — an older
    /// control plane, or a snapshot that did not come from a store. A
    /// fabricated zero would tell every caller they were out of quota.
    ///
    /// **An estimate, and the name is the contract.** It is what the ledger
    /// last reported minus what this instance has admitted since, so it is
    /// wrong by two bounded terms: the fleet's spend elsewhere since the
    /// publication, and this instance's own admissions that were later
    /// cancelled. The first dominates, and its bound is the snapshot refresh
    /// interval times the fleet's spend rate — so an operator sizing a
    /// customer-visible number tightens it by refreshing more often, not by
    /// asking here for a guarantee this cannot give.
    ///
    /// Both errors point the same way: cancellations are counted as spent and
    /// other instances' spend is missed, so this reads low against the ledger
    /// far more often than high. Under-reporting remaining quota is the safe
    /// direction for a number a customer acts on.
    ///
    /// It is never an authorization input. Admission denies from the lease and
    /// the ledger (INVARIANTS.md GL-1), never from this — a stale estimate that
    /// could deny would turn a refresh delay into an outage.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        let budget = self.snapshot.budget?;
        let spent = self
            .counters
            .units_admitted()
            .wrapping_sub(self.units_admitted_at_publish);
        Some(CostUnits(
            budget.balance_at_publish.get().saturating_sub(spent),
        ))
    }

    /// Record principal occupancy followed by account occupancy, enforcing
    /// either ceiling when configured. The returned guard owns this exact
    /// state, keeping both gauges strongly reachable until its one `Drop`
    /// releases them. Occupancy is also recorded while a ceiling is absent so
    /// a later publication can enable it without overlooking existing work.
    ///
    /// A refusal hands the state back, because the staged caller still needs
    /// it to tally the denial against the map-owned counters.
    pub(crate) fn acquire_concurrency(
        state: Arc<Self>,
        account_limit: Option<std::num::NonZeroU32>,
        locality: Locality,
    ) -> Result<ConcurrencyGuard, (DenyReason, Arc<Self>)> {
        let principal_limit = state.snapshot.limits.principal_max_concurrent_requests();
        // Lost exchanges on either gauge are the account's contention (GL-139),
        // recorded on its slot once, and only when there were any.
        let mut lost = 0u64;

        let Some(principal_permit) =
            state
                .principal_gauge
                .0
                .try_acquire(principal_limit, locality, &mut lost)
        else {
            state.lease.note_contention(lost);
            return Err((DenyReason::ConcurrencyLimited, state));
        };
        let Some(account_permit) =
            state
                .limiter
                .concurrency
                .try_acquire(account_limit, locality, &mut lost)
        else {
            state.lease.note_contention(lost);
            state.principal_gauge.0.release(principal_permit);
            return Err((DenyReason::ConcurrencyLimited, state));
        };
        state.lease.note_contention(lost);

        Ok(ConcurrencyGuard {
            state,
            principal_permit,
            account_permit,
            locality,
            terminal_recorded: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn account_concurrency_in_flight(&self) -> u32 {
        self.limiter.concurrency.in_flight()
    }
}

/// RAII proof that this admission owns any configured concurrency slots.
///
/// There is intentionally no public `release`: one owner and one `Drop` make
/// a second decrement unrepresentable through the admission API.
#[derive(Debug)]
pub(crate) struct ConcurrencyGuard {
    state: Arc<AccountAdmissionState>,
    principal_permit: GaugePermit,
    account_permit: GaugePermit,
    locality: Locality,
    terminal_recorded: bool,
}

impl ConcurrencyGuard {
    pub(crate) fn state(&self) -> &AccountAdmissionState {
        &self.state
    }

    /// A later phase has already tallied this request's terminal outcome, so
    /// `Drop` must not tally it again.
    ///
    /// The guard is the natural home for that decision because it is already
    /// the one value every admitted request holds exactly once, and it holds
    /// the state the counters live on — so making the tally exact costs no
    /// extra allocation, no extra `Arc`, and no second ownership story
    /// (INVARIANTS.md GL-20).
    pub(crate) fn mark_terminal_recorded(&mut self) {
        self.terminal_recorded = true;
    }
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        // Nothing later in the lifecycle claimed this request, so it ended
        // before execution for zero charge: an explicit cancellation, or a
        // pending state simply abandoned. Both are the same outcome, and
        // counting them here is what makes "every admitted request reaches
        // exactly one terminal counter" true by construction rather than by
        // every call site remembering.
        if !self.terminal_recorded {
            self.state
                .counters
                .record_canceled_before_start_at(self.locality);
        }
        // Construction succeeds only after both occupancy increments. The
        // guard is therefore the complete release proof; no mutable policy
        // lookup or detachable reservation can change what Drop must undo.
        self.state.limiter.concurrency.release(self.account_permit);
        self.state.principal_gauge.0.release(self.principal_permit);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BucketParams {
    rate: u32,
    burst: u32,
    shards: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RateParams {
    weighted: Option<BucketParams>,
    requests: Option<BucketParams>,
}

fn rate_params(limits: &ResolvedLimits, sharding: LocalSharding, ceiling: usize) -> RateParams {
    RateParams {
        weighted: limits.weighted_rate().map(|weighted| {
            let rate = narrow(weighted.units_per_second());
            BucketParams {
                rate,
                burst: narrow(weighted.burst_units()),
                shards: shard_count(rate, sharding, ceiling),
            }
        }),
        requests: limits.request_rate().map(|requests| {
            let rate = requests.requests_per_second().get();
            let burst = requests.burst_requests().get();
            BucketParams {
                rate,
                burst,
                shards: shard_count(
                    rate,
                    sharding,
                    shard_ceiling_for(burst, std::num::NonZeroU32::MIN),
                ),
            }
        }),
    }
}

/// Governor counts in `u32`; a limit beyond that is clamped, never wrapped,
/// and a zero limit would make an empty quota.
fn narrow(limit: u64) -> u32 {
    u32::try_from(limit).unwrap_or(u32::MAX).max(1)
}

fn shard_count(rate: u32, sharding: LocalSharding, ceiling: usize) -> usize {
    sharding
        .get()
        .min(usize::try_from(rate).unwrap_or(usize::MAX))
        .min(ceiling)
        .max(1)
}

/// How finely this snapshot's burst may be split while every shard still
/// admits the largest quote the snapshot can produce.
///
/// A split of `n` gives the smallest shard `floor(burst / n)` units, so
/// `n <= burst / maximum_quote` is exactly the condition that keeps even that
/// shard able to hold one maximum quote. Without a publication proof there is
/// no bound on the quote, and the only defensible split is none at all.
fn shard_ceiling(limits: &ResolvedLimits, maximum_quote: Option<CostUnits>) -> usize {
    let Some(maximum_quote) = maximum_quote else {
        return 1;
    };
    shard_ceiling_for(
        narrow(limits.legacy_weighted_rate().burst_units()),
        std::num::NonZeroU32::new(narrow(maximum_quote.get()))
            .expect("narrow always returns nonzero"),
    )
}

fn shard_ceiling_for(burst: u32, maximum_weight: std::num::NonZeroU32) -> usize {
    usize::try_from(burst / maximum_weight.get())
        .unwrap_or(usize::MAX)
        .max(1)
}

/// Runtime-state registry owned by each snapshot map.
///
/// The advertised limits are *account* limits: every principal (API key) of
/// an account must draw from one bucket, or N keys would multiply the
/// account's allowance N-fold (review finding GL-4). Reinstalling snapshots
/// with unchanged parameters keeps each existing dimension's bucket and
/// consumed tokens. A genuine dimension change builds only that dimension's
/// replacement; every principal loads the resulting account authority on its
/// next request.
/// Stable account and principal gauges keep separate weak-registry entries.
///
/// Scope note: this registry is per admission-engine instance, so the limit
/// is enforced *per service instance*, not aggregated across a fleet —
/// consistent with every other local mechanism here (leases aggregate spend
/// globally; rate and concurrency limits do not). Dead weak entries are
/// swept with amortized O(1) work; live entries are bounded by installed and
/// in-flight accounts/principals.
pub(crate) struct AdmissionStateRegistry {
    inner: std::sync::Mutex<Registry>,
    sharding: LocalSharding,
}

#[derive(Default)]
struct Registry {
    accounts: WeakRegistry<AccountId, AccountLimiter>,
    principals: WeakRegistry<Principal, PrincipalGauge>,
}

/// The stable runtime objects one installed principal retains.
#[derive(Clone)]
pub(crate) struct ResolvedAdmissionState {
    pub(crate) limiter: Arc<AccountLimiter>,
    pub(crate) principal_gauge: Arc<PrincipalGauge>,
}

struct WeakRegistry<K, V> {
    entries: std::collections::HashMap<K, std::sync::Weak<V>>,
    /// Size at the last sweep, so dead entries are reclaimed in proportion to
    /// how many have accumulated rather than on every single lookup.
    swept_at: usize,
    /// Entries walked across every sweep so far.
    ///
    /// The amortised bound is the whole point of GL-8, and it is a claim about
    /// total *work*, not sweep count: many tiny sweeps of a registry that
    /// keeps emptying are cheap, while one sweep per install over a registry
    /// full of live accounts is the quadratic. Only the entries-walked total
    /// distinguishes them.
    #[cfg(test)]
    swept_entries: usize,
}

impl<K, V> Default for WeakRegistry<K, V> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            swept_at: 0,
            #[cfg(test)]
            swept_entries: 0,
        }
    }
}

/// Below this many entries a sweep is too cheap to be worth deferring, and
/// deferring it would let a tiny registry hold dead entries indefinitely.
const SWEEP_FLOOR: usize = 8;

impl<K: Eq + Hash, V> WeakRegistry<K, V> {
    /// Reclaim dead entries, but only once the registry has grown enough since
    /// the last sweep to be worth walking.
    ///
    /// Sweeping on every lookup made a bulk install O(N·A): every one of N
    /// entries walked all A accounts (GL-8). Since the sweep reclaims memory and
    /// nothing else — a dead `Weak` upgrades to `None`, which the lookup below
    /// already handles by building a fresh limiter — it can be deferred freely.
    /// Doubling keeps dead entries within a constant factor of live ones and
    /// makes the amortised cost per lookup O(1).
    fn sweep_if_overgrown(&mut self) {
        // Doubling is what makes the amortisation work: each sweep costs the
        // registry's size, and the next one cannot come until that size has
        // doubled, so the total walked across N installs stays proportional to
        // N. The floor keeps a small registry from being walked repeatedly on
        // the way up from empty.
        let threshold = self.swept_at.saturating_mul(2).max(SWEEP_FLOOR);
        if self.entries.len() <= threshold {
            return;
        }
        #[cfg(test)]
        {
            self.swept_entries += self.entries.len();
        }
        #[allow(
            clippy::disallowed_methods,
            reason = "the predicate is pure and total -- every dead entry goes, whatever order they are visited in"
        )]
        self.entries.retain(|_, value| value.strong_count() > 0);
        self.swept_at = self.entries.len();
    }

    fn resolve_with(&mut self, key: K, build: impl FnOnce() -> Arc<V>) -> Arc<V> {
        if let Some(value) = self.entries.get(&key).and_then(std::sync::Weak::upgrade) {
            return value;
        }
        let value = build();
        self.entries.insert(key, Arc::downgrade(&value));
        value
    }
}

impl Registry {
    fn sweep_if_overgrown(&mut self) {
        self.accounts.sweep_if_overgrown();
        self.principals.sweep_if_overgrown();
    }

    fn resolve(
        &mut self,
        principal: Principal,
        account: AccountId,
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) -> ResolvedAdmissionState {
        let existing = self
            .accounts
            .entries
            .get(&account)
            .and_then(std::sync::Weak::upgrade);
        let limiter = if let Some(limiter) = existing {
            limiter.update(generation, limits, maximum_quote, sharding);
            limiter
        } else {
            let limiter = Arc::new(AccountLimiter::new(
                generation,
                limits,
                maximum_quote,
                sharding,
            ));
            self.accounts
                .entries
                .insert(account, Arc::downgrade(&limiter));
            limiter
        };
        let principal_limited = limits.principal_max_concurrent_requests().is_some();
        let principal_gauge = self.principals.resolve_with(principal, || {
            Arc::new(PrincipalGauge::new(sharding, principal_limited))
        });
        principal_gauge.0.configure_limit(principal_limited);
        ResolvedAdmissionState {
            limiter,
            principal_gauge,
        }
    }
}

impl AdmissionStateRegistry {
    pub(crate) fn new(sharding: LocalSharding) -> Self {
        Self {
            inner: std::sync::Mutex::new(Registry::default()),
            sharding,
        }
    }

    /// Resolve a whole batch under one lock.
    ///
    /// The bulk paths used to take and release the registry mutex once per
    /// entry; a batch of N took it N times. `resolve` still runs per entry —
    /// two principals of one account can arrive in the same batch carrying
    /// different generations, and de-duplicating by account would silently
    /// drop one of them. What is shared is the lock and the sweep decision,
    /// not the update.
    pub(crate) fn states_for<T>(
        &self,
        batch: impl IntoIterator<Item = T>,
        mut key: impl FnMut(&T) -> (Principal, AccountId, Generation),
        mut limits: impl FnMut(&T) -> ResolvedLimits,
        mut maximum_quote: impl FnMut(&T) -> Option<CostUnits>,
    ) -> Vec<(T, ResolvedAdmissionState)> {
        let mut inner = self.inner.lock().expect("admission registry poisoned");
        inner.sweep_if_overgrown();
        batch
            .into_iter()
            .map(|item| {
                let (principal, account, generation) = key(&item);
                let state = inner.resolve(
                    principal,
                    account,
                    generation,
                    &limits(&item),
                    maximum_quote(&item),
                    self.sharding,
                );
                (item, state)
            })
            .collect()
    }

    /// How many account entries the registry is holding, live or not yet
    /// reclaimed.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("admission registry poisoned")
            .accounts
            .entries
            .len()
    }

    /// Total entries walked across every sweep — the work the amortisation
    /// exists to bound.
    #[cfg(test)]
    pub(crate) fn swept_entries(&self) -> usize {
        let inner = self.inner.lock().expect("admission registry poisoned");
        inner.accounts.swept_entries + inner.principals.swept_entries
    }
}

impl Default for AdmissionStateRegistry {
    fn default() -> Self {
        Self::new(LocalSharding::SINGLE)
    }
}

fn build_limiter(rate: u32, burst: u32) -> AccountRateLimiter {
    // governor buckets are u32-denominated. Rates/bursts beyond u32::MAX are
    // clamped rather than wrapped; a per-account rate of 4.29e9 units/sec is
    // beyond any current schedule by orders of magnitude.
    //
    // These clamps decide bucket *construction* only, never a verdict. A
    // schedule whose burst cannot hold a request is refused upstream in
    // `RequestContext::admit`, comparing the quote against
    // `rate_burst_units` in full width — so a burst above u32::MAX admits by
    // the same comparison it was configured with (GL-40). `narrow` supplies the
    // `max(1)` these conversions rely on because governor requires a nonzero
    // quota, not to repair a configured value.
    //
    // `burst = 0` cannot arrive through publication: `PublishableSnapshot`
    // rejects it as `WeightedRateOutsideGovernorDomain`. It reaches here only
    // through the unvalidated `SnapshotMap::install` seam, and the full-width
    // comparison denies every priced request as `UnpriceableUnderLimits`
    // rather than letting the clamped bucket behave like a burst of one.
    //
    // Both arguments are nonzero before this call: the weighted dimension
    // through `narrow`, the request dimension by its `NonZeroU32` type, and a
    // shard's `partition` share because `shard_count` never exceeds the value
    // it splits.
    let quota = Quota::per_second(rate.try_into().expect("nonzero before this call"))
        .allow_burst(burst.try_into().expect("nonzero before this call"));
    RateLimiter::direct(quota)
}

fn build_buckets(params: BucketParams) -> Buckets {
    if params.shards == 1 {
        return Buckets::Single(build_limiter(params.rate, params.burst));
    }
    let shards = (0..params.shards)
        .map(|index| {
            let rate = u32::try_from(partition(u64::from(params.rate), params.shards, index))
                .expect("a partition of u32 fits u32");
            let burst = u32::try_from(partition(u64::from(params.burst), params.shards, index))
                .expect("a partition of u32 fits u32");
            RateShard(build_limiter(rate, burst))
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Buckets::Sharded(shards)
}

fn build_rate_state(policy: AccountRatePolicy, params: RateParams) -> RateState {
    RateState {
        policy,
        weighted: params.weighted.map(build_configured_buckets),
        requests: params.requests.map(build_configured_buckets),
    }
}

fn update_rate_state(
    policy: AccountRatePolicy,
    params: RateParams,
    current: &RateState,
) -> RateState {
    RateState {
        policy,
        weighted: update_rate_dimension(current.weighted.as_ref(), params.weighted),
        requests: update_rate_dimension(current.requests.as_ref(), params.requests),
    }
}

fn update_rate_dimension(
    current: Option<&Arc<ConfiguredBuckets>>,
    params: Option<BucketParams>,
) -> Option<Arc<ConfiguredBuckets>> {
    match (current, params) {
        (Some(current), Some(params)) if current.params == params => Some(Arc::clone(current)),
        (_, Some(params)) => Some(build_configured_buckets(params)),
        (_, None) => None,
    }
}

fn build_configured_buckets(params: BucketParams) -> Arc<ConfiguredBuckets> {
    Arc::new(ConfiguredBuckets {
        params,
        buckets: build_buckets(params),
    })
}

fn partition(total: u64, count: usize, index: usize) -> u64 {
    let count = u64::try_from(count).expect("shard count fits u64");
    let index = u64::try_from(index).expect("shard index fits u64");
    total / count + u64::from(index < total % count)
}

/// One resolved lookup result.
#[derive(Debug, Clone)]
pub enum MapEntry {
    /// A compiled account is installed.
    Present(Arc<AccountAdmissionState>),
    /// The control plane recently confirmed this principal unknown; deny
    /// without consulting anything else. `until` is control-plane metadata:
    /// the request path deliberately does not read a clock and denies both a
    /// live negative and a missing entry. Generation watermarks live outside
    /// this evictable entry so expiry cannot enable stale resurrection.
    NegativeUntil {
        /// When the negative answer lapses and the control plane should
        /// resolve the principal again.
        until: Timestamp,
    },
}

/// One control-plane mutation. Mixed positive and negative batches let a
/// copy-on-write map apply an entire refresh with one clone.
#[derive(Debug, Clone)]
pub enum SnapshotUpdate {
    /// Install a positive snapshot, subject to generation monotonicity.
    Present {
        /// The principal the snapshot authorizes.
        principal: Principal,
        /// The compiled snapshot to install.
        snapshot: Arc<AccountSnapshot>,
        /// The account's lease slot, shared by all of the account's
        /// principals.
        lease: Arc<LeaseSlot>,
    },
    /// A revocation the source published, which always carries its generation.
    Revoked {
        /// The revoked principal.
        principal: Principal,
        /// When the negative entry lapses.
        until: Timestamp,
        /// The generation revoked; a positive at or below it is refused.
        generation: Generation,
    },
    /// An absent row, which carries no generation and asserts nothing about
    /// any (GL-53).
    Unknown {
        /// The principal the source has no row for.
        principal: Principal,
        /// When the negative entry lapses.
        until: Timestamp,
    },
}

/// A control-plane mutation whose positive snapshots carry their validated
/// publication evidence through to the admission map. The separate type
/// keeps the existing raw installation API available for defensive tests and
/// embedders while letting trusted sources avoid discarding and re-deriving
/// the proof at the boundary.
#[derive(Debug, Clone)]
pub enum PublishableSnapshotUpdate {
    /// Install a positive snapshot, subject to generation monotonicity.
    Present {
        /// The principal the snapshot authorizes.
        principal: Principal,
        /// The snapshot, with its publication-validation proof.
        snapshot: PublishableSnapshot,
        /// The account's lease slot, shared by all of the account's
        /// principals.
        lease: Arc<LeaseSlot>,
    },
    /// A revocation the source published, which always carries its generation.
    Revoked {
        /// The revoked principal.
        principal: Principal,
        /// When the negative entry lapses.
        until: Timestamp,
        /// The generation revoked; a positive at or below it is refused.
        generation: Generation,
    },
    /// An absent row, which carries no generation and asserts nothing about
    /// any (GL-53).
    Unknown {
        /// The principal the source has no row for.
        principal: Principal,
        /// When the negative entry lapses.
        until: Timestamp,
    },
}

impl PublishableSnapshotUpdate {
    /// The principal this update targets, whatever its variant.
    #[must_use]
    pub fn principal(&self) -> Principal {
        match self {
            Self::Present { principal, .. }
            | Self::Revoked { principal, .. }
            | Self::Unknown { principal, .. } => *principal,
        }
    }
}

/// The pluggable snapshot map. Implementations must make `get` lock-free (or
/// as close as their backing store allows) and safe for concurrent `install`.
/// "As close as allows" is a real allowance, not a formality: a bounded cache
/// has to do its bookkeeping somewhere, and [`crate::MokaSnapshotMap`] pays
/// it inline under a non-blocking `try_lock` on roughly every sixty-fourth
/// lookup. What an implementation may never do is *block* a request behind
/// another thread's maintenance, or make a request wait on the control plane.
///
/// Generation monotonicity is part of the contract: installing a snapshot
/// older than the one present must be a no-op, so replayed or reordered
/// control-plane pushes can never roll an account back.
///
/// # Adding a defaulted method
///
/// Fifteen of these twenty-one methods carry a default body, and a *wrapper*
/// around another map inherits every one it does not override — silently, with
/// no compile error. Whether that is correct depends entirely on what the body
/// is written over, and the two answers are opposite:
///
/// - **Written over other `Self` methods** — `contains_cached`, `get_at`,
///   `install_publishable`, `remove_many`, `install_many`, `apply_many`,
///   `apply_many_at`, `apply_publishable_many`, `apply_publishable_many_at`,
///   `apply_refreshed_many_at`. A wrapper must **inherit** these. Forwarding
///   one to the inner map rebinds `self` and discards the wrapper's own
///   overrides — a counting wrapper would stop counting installs that arrive
///   through `apply_many`.
/// - **A constant or sentinel** — `local_sharding` (`SINGLE`),
///   `generation_capacity` (`MAX`), `needs_refresh` (`false`), `history_stats`
///   (`None`), `prepare_refreshes` (unfenced reads for everything). A wrapper
///   must **forward** these. Inheriting one makes the wrapper lie about the map
///   behind it: a sharded map reported as `SINGLE`, a bounded one as unbounded,
///   a real fenced refresh plan replaced by unfenced reads.
///
/// A new default belongs in one of those two lists, decided by reading the body
/// rather than the name. `the_arc_delegation_forwards_every_bulk_write` in
/// `maps.rs` witnesses the constant case for the blanket `Arc<T>` delegation —
/// the one the request path actually dispatches through — and
/// `tests/map_defaults.rs` pins what each default does when it *is* inherited.
/// This is the same rule `tollgate-store`'s `drain_reclaim_expired` records for
/// `LeaseAllocator::reclaim_expired` (GL-83, GL-120).
pub trait SnapshotMap: Send + Sync {
    /// Control-plane visibility probe. Cache implementations should avoid
    /// changing request-frequency bookkeeping for a background refresh.
    fn contains_cached(&self, principal: &Principal) -> bool {
        self.get(principal).is_some()
    }
    /// Control-plane occupancy; custom maps may not expose retention statistics.
    fn history_stats(&self) -> Option<crate::SnapshotHistoryStats> {
        None
    }
    /// Maximum retained generation entries, including pending source reads.
    /// Custom maps without reclamation retain the compatibility default.
    fn generation_capacity(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::MAX
    }

    /// Whether a push must be replaced by a new authoritative source read.
    fn needs_refresh(&self, _principal: Principal) -> bool {
        false
    }

    /// Reserve a batch before invoking the authoritative source. Reclaimed
    /// principals are already absent from request-visible state on return.
    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<crate::RefreshBatch, crate::PublicationError> {
        Ok(crate::RefreshBatch {
            reads: principals
                .iter()
                .map(|&principal| crate::SnapshotRefresh::unfenced(principal))
                .collect(),
            evicted: Vec::new(),
        })
    }

    /// Publish authoritative responses only while their retained incarnations
    /// still match. Implementations reclaiming history must validate every fence
    /// before any batch member can change account policy or visible state.
    fn apply_refreshed_many_at(
        &self,
        updates: Vec<crate::Refreshed<PublishableSnapshotUpdate>>,
        now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        self.apply_publishable_many_at(
            updates
                .into_iter()
                .map(crate::Refreshed::into_value)
                .collect(),
            now,
        )
    }

    /// The request-visible entry for `principal`, or `None` when the map
    /// holds none. The admission engine denies both `None` and a negative
    /// entry as an unknown principal. Must not block or perform I/O.
    fn get(&self, principal: &Principal) -> Option<MapEntry>;

    /// Lookup using locality already resolved by the admission engine, so a
    /// request reads its affinity once rather than at each stage.
    ///
    /// An implementation that owns per-locality state must select it from
    /// this argument: resolving locality again during retrieval ties the
    /// choice to whichever thread happened to be running, which for a cache
    /// that clones on write is the thread that installed the entry. The
    /// compatibility default delegates to [`Self::get`], which is correct
    /// precisely because such a map has one state per principal to return.
    fn get_at(&self, principal: &Principal, _locality: Locality) -> Option<MapEntry> {
        self.get(principal)
    }

    /// Instance-local sharding selected for the map and every hot-path state
    /// object it owns. The default preserves the historical single-counter
    /// layout.
    fn local_sharding(&self) -> LocalSharding {
        LocalSharding::SINGLE
    }

    /// The one per-map counter set shared by every installed request state.
    fn counters(&self) -> &Arc<AdmissionCounters>;

    /// Install (or refresh) the state for a principal, respecting generation
    /// monotonicity. `lease` is the account's slot, shared across the
    /// account's principals by the caller.
    fn install(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), crate::PublicationError>;

    /// Install a publication-validated snapshot without discarding its
    /// maximum-quote proof. Implementations that shard the rate limiter
    /// override this method; the compatibility default installs one bucket.
    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), crate::PublicationError> {
        self.install(principal, snapshot.into_inner(), lease)
    }

    /// Record a revocation the source published at `generation`, denying until
    /// `until`.
    ///
    /// The generation is not optional: a revocation always carries one, and it
    /// is what refuses a replayed snapshot at or below it (INVARIANTS.md GL-15).
    fn install_revoked(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    ) -> Result<(), crate::PublicationError>;

    /// Record that the source has no row for this principal, denying until
    /// `until`.
    ///
    /// Deliberately takes no generation, because an absence has none to give —
    /// the 404 path cannot supply one, and inventing one from what this
    /// instance last saw is GL-53. It therefore leaves any existing watermark
    /// exactly as it was rather than raising or re-tagging it.
    fn install_unknown(
        &self,
        principal: Principal,
        until: Timestamp,
    ) -> Result<(), crate::PublicationError>;

    /// Evict a principal outright. Revocations must use
    /// [`SnapshotMap::install_revoked`] so their generation watermark survives
    /// reordered control-plane messages.
    ///
    /// Eviction keeps the watermark, but keeping it no longer means the
    /// principal cannot be reinstalled at the same generation: since GL-53 a
    /// watermark left by a *positive* refuses only strictly older snapshots, so
    /// re-fetching the evicted generation repairs the entry. That is
    /// deliberate — it is what lets a bounded map recover from capacity
    /// pressure. A watermark left by a *revocation* still refuses its own
    /// generation, evicted or not.
    fn remove(&self, principal: &Principal);

    /// Evict a catalogue slice while retaining generation watermarks.
    /// Copy-on-write maps override this to clone once for the whole removal.
    fn remove_many(&self, principals: &[Principal]) {
        for principal in principals {
            self.remove(principal);
        }
    }

    /// Install a batch in one logical write. The default loops over
    /// [`install`](SnapshotMap::install); copy-on-write implementations
    /// override it to pay their clone cost once per batch instead of once
    /// per entry (review finding GL-9 — loading N principals individually is
    /// O(N²) on a whole-map-clone structure).
    fn install_many(
        &self,
        entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>,
    ) -> Result<(), crate::PublicationError> {
        self.apply_many(
            entries
                .into_iter()
                .map(|(principal, snapshot, lease)| SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                })
                .collect(),
        )
    }

    /// Apply a mixed batch of positive and negative updates. The default
    /// applies each through [`install`](SnapshotMap::install),
    /// [`install_revoked`](SnapshotMap::install_revoked) or
    /// [`install_unknown`](SnapshotMap::install_unknown) in order and stops at
    /// the first error, leaving earlier updates applied. The maps in this
    /// crate override it to validate the whole batch before changing
    /// anything; the copy-on-write map also clones once per batch rather than
    /// once per update.
    fn apply_many(&self, updates: Vec<SnapshotUpdate>) -> Result<(), crate::PublicationError> {
        for update in updates {
            match update {
                SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => self.install(principal, snapshot, lease)?,
                SnapshotUpdate::Revoked {
                    principal,
                    until,
                    generation,
                } => self.install_revoked(principal, until, generation)?,
                SnapshotUpdate::Unknown { principal, until } => {
                    self.install_unknown(principal, until)?;
                }
            }
        }
        Ok(())
    }

    /// Apply a control-plane batch at an explicit time. Implementations with
    /// expiry maintenance can combine the sweep and batch in one write;
    /// implementations without time-based maintenance use the default.
    fn apply_many_at(
        &self,
        updates: Vec<SnapshotUpdate>,
        _now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        self.apply_many(updates)
    }

    /// [`apply_many`](SnapshotMap::apply_many) for updates whose positive
    /// snapshots carry their publication proof; the default installs them
    /// through [`install_publishable`](SnapshotMap::install_publishable) and
    /// likewise stops at the first error.
    fn apply_publishable_many(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
    ) -> Result<(), crate::PublicationError> {
        for update in updates {
            match update {
                PublishableSnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => self.install_publishable(principal, snapshot, lease)?,
                PublishableSnapshotUpdate::Revoked {
                    principal,
                    until,
                    generation,
                } => self.install_revoked(principal, until, generation)?,
                PublishableSnapshotUpdate::Unknown { principal, until } => {
                    self.install_unknown(principal, until)?
                }
            }
        }
        Ok(())
    }

    /// [`apply_publishable_many`](SnapshotMap::apply_publishable_many) at an
    /// explicit time, as [`apply_many_at`](SnapshotMap::apply_many_at) is to
    /// [`apply_many`](SnapshotMap::apply_many). The default ignores `now`.
    fn apply_publishable_many_at(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
        _now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        self.apply_publishable_many(updates)
    }
}

// A shared map is still a map: lets an `AdmissionEngine<Arc<M>>` and a
// background snapshot manager hold the same map instance.
impl<T: SnapshotMap + ?Sized> SnapshotMap for Arc<T> {
    fn contains_cached(&self, principal: &Principal) -> bool {
        (**self).contains_cached(principal)
    }
    fn history_stats(&self) -> Option<crate::SnapshotHistoryStats> {
        (**self).history_stats()
    }
    fn generation_capacity(&self) -> std::num::NonZeroUsize {
        (**self).generation_capacity()
    }
    fn needs_refresh(&self, principal: Principal) -> bool {
        (**self).needs_refresh(principal)
    }
    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<crate::RefreshBatch, crate::PublicationError> {
        (**self).prepare_refreshes(principals)
    }
    fn apply_refreshed_many_at(
        &self,
        updates: Vec<crate::Refreshed<PublishableSnapshotUpdate>>,
        now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        (**self).apply_refreshed_many_at(updates, now)
    }

    fn remove_many(&self, principals: &[Principal]) {
        (**self).remove_many(principals);
    }
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        (**self).get(principal)
    }

    fn get_at(&self, principal: &Principal, locality: Locality) -> Option<MapEntry> {
        (**self).get_at(principal, locality)
    }

    fn local_sharding(&self) -> LocalSharding {
        (**self).local_sharding()
    }

    fn counters(&self) -> &Arc<AdmissionCounters> {
        (**self).counters()
    }

    fn install(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), crate::PublicationError> {
        (**self).install(principal, snapshot, lease)
    }

    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), crate::PublicationError> {
        (**self).install_publishable(principal, snapshot, lease)
    }

    fn install_revoked(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    ) -> Result<(), crate::PublicationError> {
        (**self).install_revoked(principal, until, generation)
    }

    fn install_unknown(
        &self,
        principal: Principal,
        until: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        (**self).install_unknown(principal, until)
    }

    fn remove(&self, principal: &Principal) {
        (**self).remove(principal);
    }

    fn install_many(
        &self,
        entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>,
    ) -> Result<(), crate::PublicationError> {
        (**self).install_many(entries)
    }

    fn apply_many(&self, updates: Vec<SnapshotUpdate>) -> Result<(), crate::PublicationError> {
        (**self).apply_many(updates)
    }

    fn apply_many_at(
        &self,
        updates: Vec<SnapshotUpdate>,
        now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        (**self).apply_many_at(updates, now)
    }

    fn apply_publishable_many(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
    ) -> Result<(), crate::PublicationError> {
        (**self).apply_publishable_many(updates)
    }

    fn apply_publishable_many_at(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
        now: Timestamp,
    ) -> Result<(), crate::PublicationError> {
        (**self).apply_publishable_many_at(updates, now)
    }
}

#[cfg(test)]
#[path = "../../tollgate-core/tests/support/isolated.rs"]
mod isolated;

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::num::{NonZeroU32, NonZeroUsize};
    use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseGrant, LeaseId};

    fn lease(units: u64) -> Arc<LocalLease> {
        identified_lease(LeaseId(1), units)
    }

    fn identified_lease(lease_id: LeaseId, units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id,
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at: Timestamp::from_second(10_000).unwrap(),
            },
            CostUnits::ZERO,
        ))
    }

    /// Drive eight writers through `lease` until at least one debit lost a
    /// race. Bounded so a single-core host cannot hang the suite.
    fn contend(lease: &LocalLease) {
        let now = Timestamp::from_second(0).unwrap();
        for _ in 0..50 {
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| {
                        for _ in 0..20_000 {
                            lease.try_debit(CostUnits(1), now).unwrap();
                        }
                    });
                }
            });
            if lease.contended_debits() > 0 {
                return;
            }
        }
        panic!("eight writers on one line never lost a race");
    }

    /// The account's contention outlives its leases, and a lease that leaves
    /// and returns — a refused consolidation restores the old grant — is never
    /// counted twice.
    #[test]
    fn account_contention_survives_rotation_without_double_counting() {
        let slot = LeaseSlot::for_account(AccountId(1));
        let first = identified_lease(LeaseId(1), u64::MAX / 2);
        assert!(slot.replace(Arc::clone(&first)).is_none());
        assert_eq!(slot.contended_exchanges(), 0);
        contend(&first);
        let seen = slot.contended_exchanges();
        assert_eq!(
            seen,
            first.contended_debits(),
            "the current lease is counted"
        );

        let second = identified_lease(LeaseId(2), u64::MAX / 2);
        let outgoing = slot.replace(Arc::clone(&second)).unwrap();
        assert_eq!(
            slot.contended_exchanges(),
            seen,
            "rotation keeps the history"
        );

        // Restore the first grant, as a refused consolidation does.
        drop(slot.replace(outgoing));
        assert_eq!(
            slot.contended_exchanges(),
            seen,
            "reinstalling does not recount"
        );

        contend(&first);
        let more = first.contended_debits();
        assert!(more > seen);
        assert_eq!(
            slot.contended_exchanges(),
            more,
            "new races on a reinstalled lease count once"
        );
        drop(slot.take());
        assert_eq!(
            slot.contended_exchanges(),
            more,
            "removal keeps the history"
        );
        assert_eq!(second.contended_debits(), 0);
    }

    proptest! {
        /// Whatever account policy generation wins, the one request-loaded
        /// authority carries its exact rate and concurrency policy.
        #[test]
        fn resolved_account_authority_carries_the_generation_winners_policy(
            first_generation in any::<u64>(),
            incoming_generation in any::<u64>(),
            first_enabled in any::<bool>(),
            incoming_enabled in any::<bool>(),
            first_requests_enabled in any::<bool>(),
            incoming_requests_enabled in any::<bool>(),
            first_concurrency_enabled in any::<bool>(),
            incoming_concurrency_enabled in any::<bool>(),
            first_rate in 1u64..=u64::from(u32::MAX),
            first_burst in 1u64..=u64::from(u32::MAX),
            incoming_rate in 1u64..=u64::from(u32::MAX),
            incoming_burst in 1u64..=u64::from(u32::MAX),
            first_request_rate in any::<NonZeroU32>(),
            first_request_burst in any::<NonZeroU32>(),
            incoming_request_rate in any::<NonZeroU32>(),
            incoming_request_burst in any::<NonZeroU32>(),
            first_concurrency in any::<NonZeroU32>(),
            incoming_concurrency in any::<NonZeroU32>(),
        ) {
            let policy = |weighted_enabled,
                          requests_enabled,
                          concurrency_enabled,
                          rate,
                          burst,
                          request_rate,
                          request_burst,
                          concurrency| {
                let limits = if weighted_enabled {
                    ResolvedLimits::new(64).with_weighted_rate(rate, burst)
                } else {
                    ResolvedLimits::new(64)
                        .with_weighted_rate_compatibility_fallback(rate, burst)
                };
                let limits = if requests_enabled {
                    limits.with_request_rate(request_rate, request_burst)
                } else {
                    limits
                };
                if concurrency_enabled {
                    limits.with_concurrency(concurrency, None).unwrap()
                } else {
                    limits
                }
            };
            let first = policy(
                first_enabled,
                first_requests_enabled,
                first_concurrency_enabled,
                first_rate,
                first_burst,
                first_request_rate,
                first_request_burst,
                first_concurrency,
            );
            let incoming = policy(
                incoming_enabled,
                incoming_requests_enabled,
                incoming_concurrency_enabled,
                incoming_rate,
                incoming_burst,
                incoming_request_rate,
                incoming_request_burst,
                incoming_concurrency,
            );
            let limiter = AccountLimiter::new(
                Generation(first_generation),
                &first,
                None,
                LocalSharding::SINGLE,
            );
            let before = limiter.current();

            let resolved = limiter.update(
                Generation(incoming_generation),
                &incoming,
                None,
                LocalSharding::SINGLE,
            );
            let expected = if incoming_generation > first_generation {
                incoming.account_rate_policy()
            } else {
                first.account_rate_policy()
            };
            prop_assert_eq!(resolved.rate().policy(), expected);
            let expected_concurrency = if incoming_generation > first_generation {
                incoming.max_concurrent_requests()
            } else {
                first.max_concurrent_requests()
            };
            prop_assert_eq!(resolved.max_concurrent_requests(), expected_concurrency);

            let expected_limits = if incoming_generation > first_generation {
                &incoming
            } else {
                &first
            };
            let before_params = rate_params(&first, LocalSharding::SINGLE, 1);
            let expected_params = rate_params(expected_limits, LocalSharding::SINGLE, 1);
            if before_params.weighted == expected_params.weighted {
                match (&before.rate.weighted, &resolved.rate.weighted) {
                    (Some(before), Some(resolved)) => {
                        prop_assert!(Arc::ptr_eq(before, resolved));
                    }
                    (None, None) => {}
                    _ => prop_assert!(false, "equal weighted configuration changed presence"),
                }
            }
            if before_params.requests == expected_params.requests {
                match (&before.rate.requests, &resolved.rate.requests) {
                    (Some(before), Some(resolved)) => {
                        prop_assert!(Arc::ptr_eq(before, resolved));
                    }
                    (None, None) => {}
                    _ => prop_assert!(false, "equal request configuration changed presence"),
                }
            }
        }

        /// Occupancy is tracked even without a configured ceiling. Publishing
        /// a ceiling later therefore applies to the exact work already in
        /// flight instead of starting from a fresh zero — and the handoff
        /// admits against that ceiling rather than suspending admission until
        /// the pre-existing work drains.
        #[test]
        fn enabling_a_concurrency_ceiling_observes_unbounded_occupancy(
            existing in 0u32..100,
            limit in any::<NonZeroU32>(),
        ) {
            let gauge = ConcurrencyGauge::new(LocalSharding::SINGLE, false);
            let mut permits = Vec::new();
            for _ in 0..existing {
                permits.push(
                    gauge
                        .try_acquire(None, Locality::current(), &mut 0)
                        .expect("the unbounded representation has room"),
                );
            }

            let during_handoff = gauge.try_acquire(Some(limit), Locality::current(), &mut 0);
            prop_assert_eq!(during_handoff.is_some(), limit.get() > existing);
            if let Some(permit) = during_handoff {
                prop_assert!(permit.shard_index().is_none());
                prop_assert!(gauge.in_flight() <= limit.get().max(existing));
                gauge.release(permit);
            }
            // A caller that carries no ceiling of its own is never denied by
            // another policy's activation.
            let unbounded = gauge
                .try_acquire(None, Locality::current(), &mut 0)
                .expect("the handoff does not close unlimited admission");
            gauge.release(unbounded);
            for permit in permits {
                gauge.release(permit);
            }
            let after_drain = gauge
                .try_acquire(Some(limit), Locality::current(), &mut 0)
                .expect("a nonzero ceiling admits after old occupancy drains");
            gauge.release(after_drain);
            prop_assert_eq!(gauge.in_flight(), 0);
        }
    }

    /// Taking a slot withdraws the published lease and returns its handle for
    /// release; no locality may keep obtaining the removed grant afterwards.
    #[test]
    fn a_cleared_slot_stops_the_instance_spending() {
        let slot = LeaseSlot::for_account(AccountId(1));
        assert!(slot.load().is_none(), "a cold slot denies");

        drop(slot.replace(lease(100)));
        assert!(slot.load().is_some());

        let retired = slot.take().expect("the installed lease is returned");
        assert_eq!(retired.remaining(), CostUnits(100));
        assert!(
            slot.load().is_none(),
            "an instance with invalidated lease state must hold no lease"
        );
    }

    /// `replace` hands back the superseded grant so the refill plane can
    /// release it; `take` removes and returns in one step. Losing either
    /// return value strands units until TTL reclaim.
    #[test]
    fn superseding_a_lease_hands_back_the_old_one() {
        for shards in [1, 8] {
            let slot = LeaseSlot::with_sharding(
                AccountId(1),
                LocalSharding::new(NonZeroUsize::new(shards).unwrap()),
            );
            assert!(
                slot.replace(identified_lease(LeaseId(1), 100)).is_none(),
                "nothing was installed, so there is nothing to give back"
            );

            let superseded = slot
                .replace(identified_lease(LeaseId(2), 200))
                .expect("the first lease");
            assert_eq!(superseded.grant().lease_id, LeaseId(1));
            assert_eq!(superseded.remaining(), CostUnits(100));
            assert!(superseded.is_only_local_view());
            assert_eq!(
                slot.load().expect("the second lease").remaining(),
                CostUnits(200)
            );

            let taken = slot.take().expect("the second lease");
            assert_eq!(taken.grant().lease_id, LeaseId(2));
            assert_eq!(taken.remaining(), CostUnits(200));
            assert!(taken.is_only_local_view());
            assert!(slot.load().is_none(), "take leaves the slot empty");
            assert!(slot.take().is_none(), "and taking again yields nothing");
        }
    }

    /// A sharded slot publishes N views, so two mutators that interleave
    /// could leave half the localities empty and half holding the fresh lease
    /// — a state one swap cannot reach, and one that would let a locality
    /// keep spending after a revocation cleared the slot.
    #[test]
    fn racing_mutators_never_leave_a_slot_holding_two_answers() {
        let sharding = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        for round in 0..500 {
            let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
            drop(slot.replace(identified_lease(LeaseId(1), 100)));

            let replacer = {
                let slot = Arc::clone(&slot);
                std::thread::spawn(move || slot.replace(identified_lease(LeaseId(2), 100)))
            };
            let taker = {
                let slot = Arc::clone(&slot);
                std::thread::spawn(move || slot.take())
            };
            let replaced = replacer.join().unwrap();
            let taken = taker.join().unwrap();

            let published: Vec<_> = slot
                .published_views()
                .into_iter()
                .map(|view| view.map(|lease| lease.grant().lease_id))
                .collect();
            assert!(
                published.iter().all(|view| *view == published[0]),
                "round {round}: localities disagree about the slot: {published:?}"
            );
            let mut retained: Vec<_> = [replaced, taken, slot.load()]
                .into_iter()
                .flatten()
                .map(|lease| lease.grant().lease_id)
                .collect();
            retained.sort_by_key(|id| id.0);
            assert_eq!(
                retained,
                vec![LeaseId(1), LeaseId(2)],
                "each grant is returned or still published exactly once"
            );
        }
    }

    /// A control-plane read of the slot answers the same question from any
    /// view, and must not claim an affinity to ask it (GL-124).
    ///
    /// `load` resolves `Locality::current()`, which *assigns* on a thread's
    /// first access. The runtime's health reporting, lease rotation and
    /// readiness checks all read the slot for presence and timing — facts every
    /// view of one slot answers identically — so paying an affinity for them
    /// only displaces the request-serving threads that come after.
    ///
    /// The counter is checked in an isolated process, on untouched threads,
    /// so one accidental first-use claim fails without racing other tests.
    #[test]
    fn an_observed_slot_read_answers_without_claiming_an_affinity() {
        if isolated::rerun_in_child() {
            return;
        }
        let sharding = LocalSharding::new(NonZeroUsize::new(4).unwrap());
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(100)));

        for _ in 0..2 {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let before = Locality::assigned();
                        for _ in 0..2 {
                            let observed = slot.load_observed().expect("the slot is stocked");
                            assert_eq!(observed.grant().account_id, AccountId(1));
                            assert_eq!(observed.remaining(), CostUnits(100));
                            assert_eq!(Locality::assigned(), before, "observing spends nothing");
                        }
                        // Positive control: this fresh thread's request read
                        // must still claim exactly one affinity.
                        assert!(slot.load().is_some());
                        assert_eq!(Locality::assigned(), before + 1);
                    })
                    .join()
                    .unwrap();
            });
        }

        // Every view answers this identically, which is what makes a fixed
        // affinity the right one to ask with rather than a lucky one. Read
        // from separate threads, since each takes its own affinity and they
        // land on different views of the same slot.
        let grants: Vec<_> = (0..sharding.get())
            .map(|_| {
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            let view = slot.load().expect("the slot is stocked");
                            (*view.grant(), view.remaining())
                        })
                        .join()
                        .expect("a slot read cannot panic")
                })
            })
            .collect();
        let view = slot.load_observed().expect("the slot is stocked");
        let observed = (*view.grant(), view.remaining());
        assert!(
            grants.iter().all(|answer| *answer == observed),
            "every view answers the control plane's question identically"
        );

        // And the request-path read is unchanged: it still resolves the
        // caller's own affinity, because that is the shard it will keep using.
        assert!(slot.load().is_some());
    }

    #[test]
    fn sharded_slot_keeps_release_parked_while_any_local_view_is_held() {
        let sharding = LocalSharding::new(NonZeroUsize::new(4).unwrap());
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(100)));
        let sibling = match &slot.current {
            LeaseSlotCurrent::Sharded { views, .. } => views[1].load_full().unwrap(),
            LeaseSlotCurrent::Single(_) => unreachable!(),
        };

        let parked = slot.take().unwrap();
        assert!(!parked.is_only_local_view());
        drop(sibling);
        assert!(parked.is_only_local_view());
    }

    #[test]
    fn rate_shards_partition_one_account_burst_without_multiplying_it() {
        assert_eq!(align_of::<RateShard>(), 128);
        let limits = ResolvedLimits::new(1).with_weighted_rate(8, 800);
        let limiter = AccountLimiter::new(
            Generation(1),
            &limits,
            Some(CostUnits(100)),
            LocalSharding::new(NonZeroUsize::new(8).unwrap()),
        );
        let current = limiter.current();
        let weighted = current.rate().weighted().expect("weighted rate configured");
        assert_eq!(weighted.shard_count(), 8);

        let request = NonZeroU32::new(100).unwrap();
        for _ in 0..8 {
            assert_eq!(
                weighted.check_n_at(request, Locality::current()),
                Ok(Ok(()))
            );
        }
        assert!(matches!(
            weighted.check_n_at(request, Locality::current()),
            Ok(Err(_))
        ));
    }

    #[test]
    fn rate_partition_preserves_quotient_and_remainder_exactly() {
        let shares: Vec<_> = (0..4).map(|index| partition(10, 4, index)).collect();
        assert_eq!(shares, [3, 3, 2, 2]);
        assert_eq!(shares.into_iter().sum::<u64>(), 10);

        let unequal_quotient_and_remainder: Vec<_> =
            (0..4).map(|index| partition(11, 4, index)).collect();
        assert_eq!(unequal_quotient_and_remainder, [3, 3, 3, 2]);
        assert_eq!(unequal_quotient_and_remainder.into_iter().sum::<u64>(), 11);
    }

    #[test]
    fn maximum_quote_limits_shards_to_buckets_that_can_admit_it() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(8, 500);
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());

        let ceiling = shard_ceiling(&limits, Some(CostUnits(300)));
        assert_eq!(ceiling, 1);
        assert_eq!(
            rate_params(&limits, eight, ceiling)
                .weighted
                .expect("weighted rate configured")
                .shards,
            1
        );

        // The bound is exact, not conservative: five shards of a 500-unit
        // burst are each 100 units, which is precisely one maximum quote.
        let ceiling = shard_ceiling(&limits, Some(CostUnits(100)));
        assert_eq!(ceiling, 5);
        assert_eq!(
            rate_params(&limits, eight, ceiling)
                .weighted
                .expect("weighted rate configured")
                .shards,
            5
        );
    }

    /// The limiter is shared by every principal of the account, but the
    /// largest quote belongs to one principal's cost table. A split sized
    /// from whichever snapshot happened to install first left a principal
    /// with a heavier table unable to spend its largest quote in any shard —
    /// a permanent `UnpriceableUnderLimits` for a request the account's whole
    /// burst can hold, and one that was admitted before the split existed.
    #[test]
    fn a_heavier_principal_resplits_the_account_bucket_it_shares() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(800, 800);
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());

        // A light principal installs first: eight buckets of 100 units.
        let limiter = AccountLimiter::new(Generation(7), &limits, Some(CostUnits(10)), eight);
        assert_eq!(
            limiter.current().rate().weighted().unwrap().shard_count(),
            8
        );

        // A second principal of the same account, at the same generation,
        // prices a 500-unit request. Its quote fits the account's burst, so
        // publication accepted it and admission must too.
        let narrowed = limiter.update(Generation(7), &limits, Some(CostUnits(500)), eight);
        let weighted = narrowed.rate().weighted().unwrap();
        assert_eq!(weighted.shard_count(), 1);
        assert_eq!(
            weighted.check_n_at(NonZeroU32::new(500).unwrap(), Locality::current()),
            Ok(Ok(())),
            "a quote within the account's burst must be admissible"
        );
    }

    /// A raw install carries no publication proof, so nothing bounds its
    /// quotes; the account it shares must fall back to one bucket even if a
    /// publishable sibling already split it.
    #[test]
    fn an_unproven_principal_collapses_the_account_split() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(800, 800);
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let limiter = AccountLimiter::new(Generation(1), &limits, Some(CostUnits(10)), eight);
        assert_eq!(
            limiter.current().rate().weighted().unwrap().shard_count(),
            8
        );

        let collapsed = limiter.update(Generation(2), &limits, None, eight);
        assert_eq!(collapsed.rate().weighted().unwrap().shard_count(), 1);
    }

    /// Only a *strictly* newer generation installs its limits, so the first
    /// snapshot at a generation owns them. Accepting an equal generation
    /// would make the account's bucket depend on which principal was
    /// published last.
    #[test]
    fn an_equal_generation_does_not_reinstall_the_account_limits() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(800, 800);
        let limiter = AccountLimiter::new(
            Generation(5),
            &limits,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        let widened = ResolvedLimits::new(1).with_weighted_rate(800, 8_000);
        limiter.update(
            Generation(5),
            &widened,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        assert_eq!(
            limiter
                .config
                .lock()
                .unwrap()
                .installed
                .weighted
                .unwrap()
                .burst,
            800,
            "a same-generation snapshot cannot rewrite the installed limits"
        );
    }

    /// `update` arms the gauge *before* it stores the policy, so no request
    /// can load a ceiling the occupancy state is not yet tracking. Ordering
    /// is the whole contract here: enforcement that arrives after publication
    /// leaves a window in which the published ceiling is unenforceable, which
    /// is INVARIANTS.md GL-25's failure mode rather than a slow start.
    #[test]
    fn publishing_a_ceiling_arms_the_gauge_before_the_policy_is_readable() {
        let unlimited = ResolvedLimits::new(1);
        let limiter = AccountLimiter::new(
            Generation(1),
            &unlimited,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );
        assert_eq!(
            limiter.concurrency.phase.load(Ordering::SeqCst),
            CONCURRENCY_SHARDED,
            "an account with no ceiling tracks occupancy on the locality shards"
        );

        let limited = ResolvedLimits::new(1)
            .with_concurrency(NonZeroU32::new(4).unwrap(), None)
            .expect("an account ceiling with no principal ceiling is valid");
        limiter.update(
            Generation(2),
            &limited,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        assert_eq!(
            limiter.concurrency.phase.load(Ordering::SeqCst),
            CONCURRENCY_CENTRAL,
            "an idle gauge reaches the central ceiling within the publishing call"
        );
        assert_eq!(
            limiter
                .current
                .load_full()
                .max_concurrent_requests()
                .map(NonZeroU32::get),
            Some(4),
            "and the policy readers load names that same ceiling"
        );
    }

    /// The relax half of the same publication point. An operator who
    /// withdraws a ceiling while its activation handoff is still draining
    /// must get the gauge back to sharded tracking; a gauge left draining
    /// keeps routing unlimited callers through the central counter, which is
    /// the contended path the shards exist to avoid.
    ///
    /// A gauge that already reached *central* deliberately stays there — its
    /// count is the evidence a later re-enable retains — so this covers only
    /// the transition `configure_limit` is allowed to undo.
    #[test]
    fn withdrawing_a_ceiling_mid_handoff_returns_the_gauge_to_sharded_tracking() {
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let unlimited = ResolvedLimits::new(1);
        let limiter = AccountLimiter::new(Generation(1), &unlimited, Some(CostUnits(10)), eight);

        // Old work holds a shard, so the activation below cannot complete its
        // handoff and the gauge stays draining.
        let draining = limiter
            .concurrency
            .try_acquire(None, Locality::current(), &mut 0)
            .expect("unbounded tracking admits");
        assert!(draining.shard_index().is_some());

        let limited = ResolvedLimits::new(1)
            .with_concurrency(NonZeroU32::new(4).unwrap(), None)
            .expect("an account ceiling with no principal ceiling is valid");
        limiter.update(Generation(2), &limited, Some(CostUnits(10)), eight);
        assert_eq!(
            limiter.concurrency.phase.load(Ordering::SeqCst),
            CONCURRENCY_DRAINING,
            "the outstanding shard permit holds the handoff open"
        );

        limiter.update(Generation(3), &unlimited, Some(CostUnits(10)), eight);
        assert_eq!(
            limiter.concurrency.phase.load(Ordering::SeqCst),
            CONCURRENCY_SHARDED,
            "withdrawing the ceiling before central work exists undoes the arming"
        );
        assert!(
            limiter
                .current
                .load_full()
                .max_concurrent_requests()
                .is_none(),
            "and the published policy no longer carries a ceiling"
        );

        limiter.concurrency.release(draining);
        assert_eq!(limiter.concurrency.in_flight(), 0);
    }

    /// Reinstalling the same limits at a newer generation is a no-op, and the
    /// published `AccountPolicyState` must be the *same* `Arc` afterwards.
    ///
    /// Every request loads this pointer, so republishing on an idempotent
    /// re-install would churn the arc-swap readers see for no behavioral
    /// reason. The three-way inequality above is what decides that, and until
    /// now its only witness was the randomized proptest below: it kills a
    /// mutation of the first comparison about eleven runs in twelve, which
    /// made the mutation gate report a survivor or not depending on the seed.
    /// A generated witness proves the property over a range; it cannot be the
    /// gate for one branch.
    #[test]
    fn reinstalling_identical_limits_does_not_republish_the_policy() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(800, 800);
        let limiter = AccountLimiter::new(
            Generation(1),
            &limits,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );
        let before = limiter.current.load_full();

        limiter.update(
            Generation(2),
            &limits,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        let after = limiter.current.load_full();
        assert!(
            Arc::ptr_eq(&before, &after),
            "an update that changes no limit must not republish the policy readers load"
        );
    }

    /// The ceiling is account-wide safety evidence, not a limit value, so a
    /// snapshot too old to install its rate and burst still narrows the
    /// split: that principal is admitting requests now.
    #[test]
    fn a_stale_snapshot_still_narrows_the_split_it_cannot_widen() {
        let limits = ResolvedLimits::new(1).with_weighted_rate(800, 800);
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let limiter = AccountLimiter::new(Generation(9), &limits, Some(CostUnits(10)), eight);

        // Ten times the burst, but a quote so heavy that only two shards of
        // it could hold one: stale limits, binding evidence.
        let stale = ResolvedLimits::new(1).with_weighted_rate(800, 8_000);
        limiter.update(Generation(4), &stale, Some(CostUnits(4_000)), eight);

        let config = limiter.config.lock().unwrap();
        assert_eq!(config.generation, Generation(9), "older limits stay out");
        let weighted = config.installed.weighted.unwrap();
        assert_eq!(weighted.burst, 800, "and so does the older burst");
        assert_eq!(weighted.shards, 2, "but its ceiling still binds");
    }

    #[test]
    fn concurrent_gauge_never_exceeds_its_ceiling_and_releases_exactly_once() {
        let gauge = Arc::new(ConcurrencyGauge::new(LocalSharding::SINGLE, true));
        let maximum_seen = Arc::new(AtomicU32::new(0));
        let limit = NonZeroU32::new(3).unwrap();

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let gauge = Arc::clone(&gauge);
                let maximum_seen = Arc::clone(&maximum_seen);
                scope.spawn(move || {
                    for _ in 0..2_000 {
                        if let Some(permit) =
                            gauge.try_acquire(Some(limit), Locality::current(), &mut 0)
                        {
                            maximum_seen.fetch_max(gauge.in_flight(), Ordering::Relaxed);
                            std::thread::yield_now();
                            gauge.release(permit);
                        }
                    }
                });
            }
        });

        assert!(maximum_seen.load(Ordering::Relaxed) <= limit.get());
        assert_eq!(gauge.in_flight(), 0);
    }

    #[test]
    fn unbounded_gauge_fails_closed_at_its_representation_limit() {
        let gauge = ConcurrencyGauge::new(LocalSharding::SINGLE, true);
        gauge.central.0.store(u32::MAX, Ordering::Relaxed);

        assert!(
            gauge
                .try_acquire(None, Locality::current(), &mut 0)
                .is_none()
        );
        assert_eq!(gauge.in_flight(), u32::MAX);
    }

    #[test]
    fn activation_drains_shard_permits_before_using_the_central_counter() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let gauge = ConcurrencyGauge::new(sharding, false);
        let old = gauge
            .try_acquire(None, Locality::current(), &mut 0)
            .expect("unbounded tracking admits");
        assert!(old.shard_index().is_some());

        gauge.configure_limit(true);
        assert_eq!(gauge.phase.load(Ordering::SeqCst), CONCURRENCY_DRAINING);
        assert!(
            gauge
                .try_acquire(Some(NonZeroU32::MIN), Locality::current(), &mut 0)
                .is_none(),
            "the shard permit already occupies a ceiling of one"
        );

        gauge.release(old);
        assert_eq!(gauge.phase.load(Ordering::SeqCst), CONCURRENCY_CENTRAL);
        let central = gauge
            .try_acquire(Some(NonZeroU32::MIN), Locality::current(), &mut 0)
            .expect("the drained handoff activates the central ceiling");
        assert!(central.shard_index().is_none());
        gauge.release(central);
        assert_eq!(gauge.in_flight(), 0);
    }

    /// The regression the draining phase used to carry: enabling a ceiling
    /// denied *every* caller, whatever its own policy said, until the longest
    /// request already running finished. A slow handler therefore turned an
    /// operator publishing a limit into an account-wide outage of unbounded
    /// length, reported as ordinary saturation.
    #[test]
    fn activation_admits_within_the_new_ceiling_while_old_work_drains() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let gauge = ConcurrencyGauge::new(sharding, false);
        let slow = gauge
            .try_acquire(None, Locality::current(), &mut 0)
            .expect("unbounded tracking admits");

        gauge.configure_limit(true);
        assert_eq!(gauge.phase.load(Ordering::SeqCst), CONCURRENCY_DRAINING);

        let limit = NonZeroU32::new(3).unwrap();
        let mut admitted = Vec::new();
        while let Some(permit) = gauge.try_acquire(Some(limit), Locality::current(), &mut 0) {
            assert!(permit.shard_index().is_none());
            admitted.push(permit);
            assert!(admitted.len() < 8, "the handoff must still bound admission");
        }
        assert_eq!(
            admitted.len(),
            2,
            "the draining shard permit occupies one of the three slots"
        );
        assert_eq!(gauge.in_flight(), limit.get());

        // A principal still on the older, unlimited policy is not denied by
        // the account's activation either.
        let unlimited = gauge
            .try_acquire(None, Locality::current(), &mut 0)
            .expect("no ceiling, no denial");
        gauge.release(unlimited);

        gauge.release(slow);
        assert_eq!(
            gauge.phase.load(Ordering::SeqCst),
            CONCURRENCY_CENTRAL,
            "the last old shard permit still promotes"
        );
        assert!(
            gauge
                .try_acquire(Some(limit), Locality::current(), &mut 0)
                .is_some_and(|permit| {
                    gauge.release(permit);
                    true
                }),
            "the drained slot is reusable under the central ceiling"
        );
        for permit in admitted {
            gauge.release(permit);
        }
        assert_eq!(gauge.in_flight(), 0);
    }

    /// Activation racing acquisition must not manufacture a denial for a
    /// caller whose own policy carries no ceiling.
    #[test]
    fn a_concurrent_activation_never_denies_an_unlimited_caller() {
        let gauge = Arc::new(ConcurrencyGauge::new(LocalSharding::SINGLE, false));
        let denials = Arc::new(AtomicU32::new(0));

        std::thread::scope(|scope| {
            for _ in 0..4 {
                let gauge = Arc::clone(&gauge);
                let denials = Arc::clone(&denials);
                scope.spawn(move || {
                    for _ in 0..5_000 {
                        match gauge.try_acquire(None, Locality::current(), &mut 0) {
                            Some(permit) => gauge.release(permit),
                            None => {
                                denials.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
            }
            let gauge = Arc::clone(&gauge);
            scope.spawn(move || {
                for _ in 0..5_000 {
                    gauge.configure_limit(true);
                    gauge.configure_limit(false);
                }
            });
        });

        assert_eq!(denials.load(Ordering::Relaxed), 0);
        assert_eq!(gauge.in_flight(), 0);
    }

    /// The guard is held for a request's whole lifetime, so what it carries is
    /// a deliberate choice rather than a convenience. It is the occupied state
    /// plus its two permits, and — since GL-93 — the pinned locality and the
    /// one-bit record of whether a later phase already tallied this request's
    /// terminal outcome.
    ///
    /// Those two were added rather than tallying at each call site because the
    /// guard is the only value every admitted request holds exactly once: it
    /// is what makes "exactly one terminal counter per admitted request" true
    /// by construction, including for a pending state that is simply abandoned
    /// (INVARIANTS.md GL-20). Carrying the locality keeps that tally in the same
    /// counter shard the admission used, which re-reading the thread-local in
    /// `Drop` would not after a worker hop.
    #[test]
    fn concurrency_guard_carries_the_exact_occupied_state() {
        let word = std::mem::size_of::<Arc<AccountAdmissionState>>();
        assert_eq!(
            std::mem::size_of::<ConcurrencyGuard>(),
            // state + principal permit + account permit + locality + the
            // padded terminal-outcome flag.
            5 * word,
            "the execution-lifetime guard grew: justify what it now carries"
        );
    }
}

#[cfg(test)]
mod funding_deadline_tests {
    use super::*;

    fn shortfall(remaining: u64, period_end: Option<Timestamp>) -> tollgate_core::BalanceShortfall {
        tollgate_core::BalanceShortfall {
            remaining: CostUnits(remaining),
            period_end,
        }
    }

    #[test]
    fn exhaustion_deadlines_are_conservative_at_subsecond_and_domain_boundaries() {
        for shards in [1, 8] {
            let slot = LeaseSlot::with_sharding(
                AccountId(1),
                LocalSharding::new(std::num::NonZeroUsize::new(shards).unwrap()),
            );
            assert_eq!(slot.funding_evidence(Timestamp::MIN), None);
            slot.funding_attempt().shortfall(shortfall(0, None));
            assert_eq!(slot.funding_evidence(Timestamp::MIN), Some(CostUnits::ZERO));
            assert_eq!(slot.funding_evidence(Timestamp::MAX), Some(CostUnits::ZERO));
            for nanos in [-1_500_000_000i128, 1_500_000_000] {
                let end = Timestamp::from_nanosecond(nanos).unwrap();
                slot.funding_attempt().shortfall(shortfall(7, Some(end)));
                let floor =
                    Timestamp::from_second(i64::try_from(nanos.div_euclid(1_000_000_000)).unwrap())
                        .unwrap();
                assert_eq!(
                    slot.funding_evidence(
                        floor
                            .checked_sub(jiff::SignedDuration::from_nanos(1))
                            .unwrap()
                    ),
                    Some(CostUnits(7))
                );
                assert_eq!(slot.funding_evidence(floor), None);
                assert_eq!(slot.funding_evidence(end), None);
            }
        }
    }

    /// Two writers alternate publications whose deadline and remaining are
    /// tied together. Every accepted read must be one publication's pair, never
    /// a live deadline beside another publication's remaining. Deadlines
    /// repeat on purpose: equal deadlines bracketing a different publication
    /// are the case a deadline cannot detect and the sequence must.
    #[test]
    fn paired_evidence_reads_are_conservative() {
        let slot = LeaseSlot::for_account(AccountId(1));
        let stop = std::sync::atomic::AtomicBool::new(false);
        // Publication k has remaining k and a deadline derived from it, so a
        // torn pair is recognisable from the values alone.
        let second = |k: u64| 1_000 + i64::try_from(k % 3).unwrap();
        let deadline = |k: u64| Timestamp::from_second(second(k)).unwrap();
        std::thread::scope(|scope| {
            for writer in 0..2u64 {
                let (slot, stop) = (&slot, &stop);
                scope.spawn(move || {
                    let mut k = writer;
                    while !stop.load(Ordering::Relaxed) {
                        slot.funding_attempt()
                            .shortfall(shortfall(k, Some(deadline(k))));
                        k = (k + 2) % 500;
                    }
                });
            }
            // Stop the writers however the reader exits, so a failed
            // assertion reports instead of leaving the scope joining forever.
            struct StopOnDrop<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for StopOnDrop<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Relaxed);
                }
            }
            let _stop = StopOnDrop(&stop);
            // On a loaded host the reader can finish its whole loop before
            // either writer is first scheduled, so the liveness check below
            // would measure the scheduler. Wait, boundedly, for a first
            // publication; that read is held to the pairing rule as well.
            let started = std::time::Instant::now();
            let (until, remaining) = loop {
                if let Some(pair) = slot.evidence_pair(i64::MIN + 1) {
                    break pair;
                }
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(30),
                    "no writer published within thirty seconds"
                );
                std::thread::yield_now();
            };
            assert_eq!(
                until,
                second(remaining),
                "a deadline was paired with another publication's remaining"
            );
            let mut accepted = 0u32;
            for _ in 0..200_000 {
                if let Some((until, remaining)) = slot.evidence_pair(i64::MIN + 1) {
                    accepted += 1;
                    assert_eq!(
                        until,
                        second(remaining),
                        "a deadline was paired with another publication's remaining"
                    );
                }
            }
            assert!(accepted > 0, "the reader never observed live evidence");
        });
    }
}
