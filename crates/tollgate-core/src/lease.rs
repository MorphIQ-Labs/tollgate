//! Local quota leases: centrally allocated capacity, locally decremented.
//!
//! A [`LeaseGrant`] is what an allocator (the store or the quota server)
//! returns after atomically debiting an account's balance. A [`LocalLease`]
//! is the instance-side runtime form: one atomic counter by default, or an
//! explicitly configured set of cache-isolated counters. Requests reserve
//! units with CAS loops — no lock, no I/O — which is how one
//! database transaction amortizes across thousands of requests. Central
//! allocation bounds spend; the grant's lease-scoped capability prevents
//! release or usage from being attributed to a different lease
//! (INVARIANTS.md #1, #4).

use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use jiff::Timestamp;

use crate::deny::DenyReason;
use crate::ids::{AccountId, FencingToken, LeaseId};
use crate::sharding::{LocalSharding, Locality};
use crate::units::CostUnits;

/// An allocator's record of one lease: `units` were debited from
/// `account_id`'s balance and belong exclusively to the holder until
/// `expires_at`, after which the allocator reclaims whatever the holder did
/// not spend (INVARIANTS.md #9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaseGrant {
    pub lease_id: LeaseId,
    pub account_id: AccountId,
    /// Capability token for this lease record, not an account-wide epoch.
    pub fencing_token: FencingToken,
    pub units: CostUnits,
    pub expires_at: Timestamp,
}

/// An account's unfunded spend on this instance, under
/// [`EnforcementMode::Elastic`].
///
/// Three atomic counters, with the total shaped exactly like [`LocalLease`]'s:
/// a CAS loop, no lock, no I/O, no clock. A lease counts *down* through units
/// someone already paid for; `spent` counts *up* through units nobody has.
/// `committed` distinguishes durable spend from pending reservations that can
/// still be refunded. `commit_publications` closes the otherwise torn
/// reservation-phase/occupancy transition, so a cap refusal never advertises
/// irrevocable units as refundable. The ledger settles the difference by
/// treating overage as a second funding term, so per-account conservation
/// still closes exactly (INVARIANTS.md #1, #3).
///
/// **The cap is a parameter, not a field.** It arrives from the snapshot the
/// request has already read, which means two things: a republished cap takes
/// effect on the very next request with no reconciliation step, and — the
/// load-bearing half — the cap comparison happens *inside* the same
/// compare-exchange that claims the units. Checking a cap and then claiming
/// against it in two steps would let two cores each observe room for a request
/// that only one of them can have.
///
/// **Its lifetime is the account's, not a lease's.** It hangs off the
/// per-account lease slot, which is created on first use and held for the
/// life of the process. That is deliberate and is the difference between this
/// and the rate-limiter registry: a limiter rebuilt after eviction costs a
/// full bucket, but an overage counter rebuilt after eviction silently resets
/// a spend cap.
///
/// [`EnforcementMode::Elastic`]: crate::snapshot::EnforcementMode::Elastic
#[derive(Debug)]
pub struct AccountOverage {
    account_id: AccountId,
    spent: AtomicU64,
    committed: AtomicU64,
    commit_publications: AtomicUsize,
}

/// Evidence that an overage commit publication is visible to cap observers.
///
/// The only way to publish committed occupancy is through this guard. Its
/// lifetime surrounds the reservation's phase CAS and the occupancy update;
/// dropping it is the release publication that makes the stable counters
/// observable again.
struct OverageCommitPublication<'a> {
    overage: &'a AccountOverage,
}

impl OverageCommitPublication<'_> {
    fn publish(self, units: CostUnits) {
        let prior = self
            .overage
            .committed
            .fetch_add(units.get(), Ordering::AcqRel);
        debug_assert!(
            prior
                .checked_add(units.get())
                .is_some_and(|committed| committed <= self.overage.spent.load(Ordering::Acquire)),
            "committed overage exceeds total recorded spend"
        );
    }
}

impl Drop for OverageCommitPublication<'_> {
    fn drop(&mut self) {
        let prior = self
            .overage
            .commit_publications
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prior > 0, "overage commit publication count underflowed");
    }
}

impl AccountOverage {
    #[must_use]
    pub fn new(account_id: AccountId) -> Self {
        AccountOverage {
            account_id,
            spent: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            commit_publications: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Unfunded units extended on this instance so far.
    #[must_use]
    pub fn spent(&self) -> CostUnits {
        CostUnits(self.spent.load(Ordering::Acquire))
    }

    /// Units still extendable under `cap`, saturating at zero.
    ///
    /// Read by readiness: an elastic account with headroom here is admissible
    /// even when its lease is empty or absent, which is the whole point of the
    /// mode (INVARIANTS.md #10).
    #[must_use]
    pub fn headroom(&self, cap: CostUnits) -> CostUnits {
        CostUnits(cap.get().saturating_sub(self.spent.load(Ordering::Acquire)))
    }

    /// Extend `units` of unfunded credit if `cap` has room. Lock-free; the CAS
    /// loop retries only under concurrent overage on the same account.
    ///
    /// Fails closed on both boundaries: a total that exceeds the cap and a
    /// total that cannot be represented are the same refusal, because a
    /// wrapped total would read as a tiny spend and reopen the cap
    /// (INVARIANTS.md #11).
    #[inline]
    pub(crate) fn try_debit(&self, units: CostUnits, cap: CostUnits) -> Result<(), DenyReason> {
        let want = units.get();
        let mut current = self.spent.load(Ordering::Acquire);
        loop {
            let refused = || {
                // A zero-delta RMW, rather than a load, places this observer
                // in the marker's modification order. It therefore either
                // precedes publication (when the reservation is still
                // refundable), overlaps it, or acquires the completed
                // committed update; a stale zero cannot skip an already
                // linearized publication start.
                if self.commit_publications.fetch_add(0, Ordering::AcqRel) != 0 {
                    return DenyReason::OverageCommitInProgress {
                        spent: CostUnits(current),
                        overage_cap: cap,
                    };
                }
                let committed = self.committed.load(Ordering::Acquire);
                if committed
                    .checked_add(want)
                    .is_some_and(|next| next <= cap.get())
                {
                    DenyReason::OverageCapTemporarilyExhausted {
                        spent: CostUnits(current),
                        overage_cap: cap,
                    }
                } else {
                    DenyReason::OverageCapExhausted {
                        spent: CostUnits(current),
                        overage_cap: cap,
                    }
                }
            };
            let Some(next) = current.checked_add(want) else {
                return Err(refused());
            };
            if next > cap.get() {
                return Err(refused());
            }
            match self.spent.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Publish the reservation's phase claim and committed occupancy as one
    /// observer-safe transition.
    ///
    /// `claim` owns the single commit-vs-cancel CAS. The publication marker is
    /// installed before that closure can run and removed only after a winning
    /// claim has advanced `committed`. A cap observer that overlaps either
    /// half therefore sees `OverageCommitInProgress`, never a stable claim
    /// that the already-irrevocable units remain refundable.
    ///
    /// **Precondition: the caller already owns `units` in `spent`, and this
    /// function never credits them back.** Publication is not a debit. There
    /// are exactly two owners that satisfy that, and both are in this crate: a
    /// pending overage [`Reservation`], whose cancel/drop refunds, and a
    /// [`TentativeOverage`] guard, which refunds on drop. A caller that
    /// publishes without holding one leaves committed occupancy above recorded
    /// spend, which the `publish` assertion catches in debug and which silently
    /// reopens the cap in release. Reach it through
    /// [`TentativeOverage::publish_commit`] unless a pending reservation is
    /// already the owner.
    ///
    /// [`Reservation`]: crate::reservation::Reservation
    #[inline]
    pub(crate) fn publish_claim<T, E>(
        &self,
        units: CostUnits,
        claim: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        // One marker belongs to one live call stack, so exhausting usize would
        // require more simultaneously executing publications than the process
        // can address. The fetch is the bounded, lock-free hot-path operation.
        let prior = self.commit_publications.fetch_add(1, Ordering::AcqRel);
        debug_assert_ne!(
            prior,
            usize::MAX,
            "live overage commit publications exceed the address space"
        );
        let publication = OverageCommitPublication { overage: self };
        match claim() {
            Ok(value) => {
                publication.publish(units);
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Withdraw previously extended units (release of an uncommitted overage
    /// reservation). Callers must return only units they extended, exactly
    /// once — the reservation state machine guarantees this.
    ///
    /// Plain `fetch_sub`, and the choice of what happens if that contract were
    /// ever broken is deliberate rather than accidental. Underflow wraps the
    /// counter to near `u64::MAX`, which makes every later request exceed the
    /// cap and deny: wrong, but wrong in the fail-closed direction. Clamping
    /// to zero would be the fail-*open* direction — it would under-report
    /// spend and hand the account a fresh cap — so the safer-looking
    /// arithmetic is the more dangerous one here.
    #[inline]
    pub(crate) fn credit(&self, units: CostUnits) {
        let prior = self.spent.fetch_sub(units.get(), Ordering::AcqRel);
        debug_assert!(
            prior >= units.get(),
            "overage credit of {} exceeds recorded spend {prior}",
            units.get()
        );
    }

    /// Extend `units` as a *revocable* debit, owned by the returned guard.
    ///
    /// This is the commit-time half of elastic funding: a leased reservation
    /// whose window lapsed needs overage capacity *before* it can claim the
    /// phase, because a won claim with no funding term breaks the ledger
    /// equation by exactly these units. But it cannot know yet whether it will
    /// win — a canceller may already be resolving the same reservation — so
    /// the debit must be revocable until the claim resolves.
    ///
    /// The guard is what makes "debited but never resolved" unrepresentable.
    /// Without it the units sit in `spent` with no owner between the debit and
    /// the claim; a panic unwinding through that window (or any early return a
    /// later edit adds) leaves the reservation's own `Drop` refunding the
    /// *lease* while these units are stranded for the life of the process,
    /// silently shrinking the account's cap.
    #[inline]
    pub(crate) fn debit_tentatively(
        &self,
        units: CostUnits,
        cap: CostUnits,
    ) -> Result<TentativeOverage<'_>, DenyReason> {
        self.try_debit(units, cap)?;
        Ok(TentativeOverage {
            overage: self,
            units,
        })
    }
}

/// A revocable overage debit, held between the claim's funding and its
/// resolution. Dropping it returns the units; [`publish_commit`] is the only
/// way to make them irrevocable.
///
/// [`publish_commit`]: TentativeOverage::publish_commit
#[derive(Debug)]
pub(crate) struct TentativeOverage<'a> {
    overage: &'a AccountOverage,
    units: CostUnits,
}

impl TentativeOverage<'_> {
    /// Publish `claim` and this debit as one observer-safe transition.
    ///
    /// Consuming `self` is the ordering guarantee: the debit is already in
    /// `spent` before the marker is installed, so the publication's occupancy
    /// assertion holds exactly as it does for a natively admitted overage, and
    /// a cap observer that overlaps either half sees `OverageCommitInProgress`
    /// rather than a stable claim that irrevocable units are refundable. A
    /// caller cannot claim without first holding a debit, because there is no
    /// other way to reach this function.
    ///
    /// A winning claim retains the units; a losing claim credits them back
    /// before returning, because the winner was a cancellation that refunded
    /// its own funding source and owes nothing for these.
    #[inline]
    pub(crate) fn publish_commit<T, E>(self, claim: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        // The refund is this function's responsibility from here, so disarm
        // the guard rather than letting it double-credit a retained debit.
        let this = ManuallyDrop::new(self);
        match this.overage.publish_claim(this.units, claim) {
            Ok(value) => Ok(value),
            Err(error) => {
                this.overage.credit(this.units);
                Err(error)
            }
        }
    }
}

impl Drop for TentativeOverage<'_> {
    fn drop(&mut self) {
        self.overage.credit(self.units);
    }
}

/// Somewhere for a draining lease to say so, without this crate learning what
/// a task, a runtime, or a waker is.
///
/// The refill plane supplies the implementation; the request path only calls
/// it. That keeps the hot-path crate's dependency policy intact — a trait
/// declaration is not an async dependency — and leaves the wake mechanism free
/// to change without touching a line of request-path code.
///
/// **Contract:** [`request_refill`](RefillSignal::request_refill) is invoked
/// from inside a debit, on the request path. It must not block, wait on a
/// lock, allocate, or perform I/O (INVARIANTS.md #5, #6). A single-counter
/// lease calls it at most once. A sharded lease calls it at most once per
/// shard between aggregate checks; an early shard signal is re-armed if the
/// aggregate has not reached low water yet.
pub trait RefillSignal: Send + Sync + core::fmt::Debug {
    /// This lease has crossed its low-water mark and wants replacing.
    fn request_refill(&self);
}

/// What the refill plane should do about a lease, from
/// [`LocalLease::refill_due_or_rearm`].
///
/// The two active verdicts are not degrees of the same thing. `Draining` says
/// the lease is still serving and should be replaced *before* it refuses
/// anything; `Refused` says it already refused work the account could fund,
/// which is a statement about the grant's size rather than its depletion and
/// needs the holder's unspent units folded back in before the next one is
/// sized (INVARIANTS.md #1, #6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefillVerdict {
    /// The lease is serving and has asked for nothing.
    Idle,
    /// Spending crossed low water. Rotate: acquire the next lease while this
    /// one keeps serving, then release this one once it quiesces.
    Draining,
    /// A debit was refused for want of units. However far this lease is from
    /// its low-water mark, it is too small for the work being offered, and
    /// the units it still holds are the ones the next grant needs. Rotate by
    /// *consolidating*: return them and re-grant against the restored
    /// balance, atomically, so the exchange cannot shrink the holder or lose
    /// the units to another instance in between.
    Refused,
}

/// One lease shard on its own cache line.
///
/// The supported Apple Silicon hosts report 128-byte lines; aligning to 64
/// would still let adjacent shards invalidate one another there. Over-aligning
/// on a 64-byte-line target costs memory but does not weaken isolation.
#[repr(align(128))]
#[derive(Debug)]
struct LeaseShard {
    remaining: AtomicU64,
    low_water: u64,
    signalled: AtomicBool,
    /// Debit compare-exchanges on `remaining` that lost to another writer.
    ///
    /// On this shard's own line, in padding it already had: the counter costs
    /// no memory, and the only thread that writes it is one already contending
    /// for this line. Added once per contended debit, never per failure, so a
    /// contended debit adds one write rather than one per lost race.
    contended: AtomicU64,
}

impl LeaseShard {
    fn new(remaining: u64, low_water: u64) -> Self {
        Self {
            remaining: AtomicU64::new(remaining),
            low_water,
            signalled: AtomicBool::new(false),
            contended: AtomicU64::new(0),
        }
    }

    /// Record `lost` failed exchanges, if any. The branch is the whole cost
    /// of the signal on an uncontended debit.
    #[inline]
    fn note_contention(&self, lost: u64) {
        if lost != 0 {
            self.contended.fetch_add(lost, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
enum LeaseBalance {
    Single(LeaseShard),
    Sharded(Box<[LeaseShard]>),
}

impl LeaseBalance {
    fn as_slice(&self) -> &[LeaseShard] {
        match self {
            Self::Single(shard) => std::slice::from_ref(shard),
            Self::Sharded(shards) => shards,
        }
    }
}

/// Fixed-size evidence for returning a pending debit.
///
/// A fragmented debit may draw from several counters, but cancellation may
/// return the exact aggregate to any one counter: every counter is merely a
/// partition of the same lease bound. Keeping only the refund destination
/// avoids allocating a variable-length receipt on the request path.
#[derive(Debug)]
pub(crate) struct LeaseDebit {
    shard: usize,
    units: u64,
}

/// Instance-side lease state: the grant plus live remaining-unit counters.
///
/// Shared as `Arc<LocalLease>` between the request path (reserve/return) and
/// the background refill task (`needs_refill`). Never mutated otherwise; a
/// refill installs a *new* `LocalLease` rather than growing this one, so the
/// request path never observes a counter that jumps upward mid-reservation.
///
/// Every fresh lease begins with clear shard-local refill signals. The refill
/// plane re-arms an early signal only after checking the aggregate and closes
/// the clear/debit race with a second aggregate read.
#[derive(Debug)]
struct LeaseInner {
    grant: LeaseGrant,
    balance: LeaseBalance,
    /// Whom to tell when spending crosses `low_water`, if anyone. `None` for
    /// a lease nobody refills — a test fixture, or a caller driving the
    /// counter directly.
    refill: OnceLock<Arc<dyn RefillSignal>>,
    /// Refill trigger: when `remaining` falls to or below this, the holder
    /// should acquire its next lease — in the background, never inline.
    low_water: u64,
    /// Set by a debit this lease could not fund, cleared by the refill plane
    /// when it reads the verdict.
    ///
    /// Lease-wide rather than per-shard, because it reports a fact about the
    /// *grant* and not about one counter: a refusal already walked every
    /// shard (see [`LocalLease::try_reserve_at`]), so no sibling is holding
    /// the units that would have funded it. It doubles as the refusal
    /// doorbell's once-token, which is why a refusal wakes the plane even
    /// when a low-water crossing already spent this lease's shard flags.
    refused: AtomicBool,
    /// The largest quote this lease refused for want of units: the demand a
    /// consolidation may grow to (#131). Raised before the doorbell's swap,
    /// which publishes it, and read by the plane only at quiescence. Zero
    /// until a refusal, and never set by an expiry refusal, which rotates.
    refused_quote: AtomicU64,
    /// How much of the shards' contention a [`LocalLease::take_unreported_contention`]
    /// caller has already been handed. Raised with `fetch_max`, so a lease that
    /// is taken out of its slot and reinstalled — which a refused consolidation
    /// does — is never reported twice.
    contention_reported: AtomicU64,
    /// Local end of life: `expires_at - safety margin`. Debits and commits
    /// stop here, *before* the server-stamped expiry, so clock skew between
    /// allocator and holder plus in-flight request time fit inside the
    /// margin. Together with the allocator's reclaim grace (which starts
    /// *after* `expires_at`) this closes the expiry race: the holder stops
    /// spending strictly before the server starts reclaiming.
    usable_until: Timestamp,
}

/// A local view of one lease's shared counters and metadata.
///
/// Sharded slots create one outer `Arc<LocalLease>` per locality. Those
/// independently reference-counted handles all point to this shared inner
/// state, so acquiring and dropping a routine request handle does not contend
/// with other localities. The refill plane still observes every live alias
/// through the inner `Arc` count before releasing a superseded lease.
#[derive(Debug, Clone)]
#[repr(align(128))]
pub struct LocalLease {
    inner: Arc<LeaseInner>,
}

fn usable_until(expires_at: Timestamp, margin: jiff::SignedDuration) -> Timestamp {
    if margin < jiff::SignedDuration::ZERO {
        // A negative margin would extend local use past allocator expiry and
        // invert the expiry-safety protocol. Fail closed even if a caller
        // bypasses validated LeaseManager configuration.
        Timestamp::MIN
    } else {
        expires_at
            .checked_sub(margin)
            // A margin longer than the lease's life fails closed: never
            // usable, settled by refill/reclaim.
            .unwrap_or(Timestamp::MIN)
    }
}

fn partition(total: u64, count: usize, index: usize) -> u64 {
    let count = u64::try_from(count).expect("local shard count fits u64");
    let index = u64::try_from(index).expect("local shard index fits u64");
    total / count + u64::from(index < total % count)
}

impl LocalLease {
    /// Wrap a grant for local spending with no safety margin (usable right
    /// up to the grant's expiry). Prefer [`LocalLease::with_safety_margin`]
    /// whenever the grant's clock is not the local clock.
    ///
    /// `low_water` is where background refill should begin; it must be below
    /// the grant size to be useful, but any value is accepted (0 disables
    /// early refill).
    #[must_use]
    pub fn new(grant: LeaseGrant, low_water: CostUnits) -> Self {
        Self::with_safety_margin(grant, low_water, jiff::SignedDuration::ZERO)
    }

    /// Wrap a grant, refusing debits and commits once within `margin` of the
    /// grant's expiry. Size the margin to cover worst-case allocator/holder
    /// clock skew plus the longest request the service executes.
    #[must_use]
    pub fn with_safety_margin(
        grant: LeaseGrant,
        low_water: CostUnits,
        margin: jiff::SignedDuration,
    ) -> Self {
        let usable_until = usable_until(grant.expires_at, margin);
        LocalLease {
            inner: Arc::new(LeaseInner {
                balance: LeaseBalance::Single(LeaseShard::new(grant.units.get(), low_water.get())),
                low_water: low_water.get(),
                refused: AtomicBool::new(false),
                refused_quote: AtomicU64::new(0),
                contention_reported: AtomicU64::new(0),
                usable_until,
                refill: OnceLock::new(),
                grant,
            }),
        }
    }

    /// Wrap a grant in an explicitly sharded local layout.
    ///
    /// Grant units and the low-water threshold are partitioned exactly across
    /// `sharding`; their sums remain the original values. A one-shard request
    /// retains the inline representation used by [`Self::with_safety_margin`].
    #[must_use]
    pub fn with_sharding(
        grant: LeaseGrant,
        low_water: CostUnits,
        margin: jiff::SignedDuration,
        sharding: LocalSharding,
    ) -> Self {
        if sharding == LocalSharding::SINGLE {
            return Self::with_safety_margin(grant, low_water, margin);
        }
        let usable_until = usable_until(grant.expires_at, margin);
        let count = sharding.get();
        let shards = (0..count)
            .map(|index| {
                LeaseShard::new(
                    partition(grant.units.get(), count, index),
                    partition(low_water.get(), count, index),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            inner: Arc::new(LeaseInner {
                grant,
                balance: LeaseBalance::Sharded(shards),
                refill: OnceLock::new(),
                low_water: low_water.get(),
                refused: AtomicBool::new(false),
                refused_quote: AtomicU64::new(0),
                contention_reported: AtomicU64::new(0),
                usable_until,
            }),
        }
    }

    /// Attach the signal to raise when spending crosses `low_water`.
    ///
    /// Without one, a lease still records the crossing in `needs_refill` and
    /// waits to be polled — which is the behaviour every caller had before
    /// refill became demand-driven, and remains correct, just later.
    #[must_use]
    pub fn with_refill(self, signal: Arc<dyn RefillSignal>) -> Self {
        // The first attachment owns the doorbell for every local view. A
        // repeated attachment keeps that established signal; construction
        // remains safe even if a caller created views before wiring refill.
        let _already_attached = self.inner.refill.set(signal);
        self
    }

    /// The instant this lease stops accepting debits and commits locally.
    #[must_use]
    pub fn usable_until(&self) -> Timestamp {
        self.inner.usable_until
    }

    #[must_use]
    pub fn grant(&self) -> &LeaseGrant {
        &self.inner.grant
    }

    /// Whether this is the only independently reference-counted local view of
    /// the lease. The refill plane combines this with the outer `Arc` count;
    /// only then can no request still hold any locality's handle.
    #[must_use]
    pub fn is_only_local_view(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    /// The largest quote this lease refused for want of units, or zero.
    ///
    /// Demand the refill plane has proof of: a consolidation may grow the
    /// replacement to it when the account can fund it (#131). Exact only once
    /// the lease has quiesced, the same condition [`Self::remaining`] needs.
    #[must_use]
    pub fn largest_refused_quote(&self) -> CostUnits {
        CostUnits(self.inner.refused_quote.load(Ordering::Acquire))
    }

    /// Debits that lost a compare-exchange on a shard to another writer,
    /// summed across shards, since this lease was created.
    ///
    /// Evidence that the account's funding line is being written from more
    /// than one core at once, and a **lower bound** on contention rather than
    /// a measure of its cost: only the debit loops can observe a lost race.
    /// The rate bucket, reference counts and settlement use read-modify-write
    /// operations that pay for a contended line without ever failing, and a
    /// debit whose exchange lands between two rivals' records nothing. On a
    /// load-linked/store-conditional target a spurious exchange failure also
    /// counts; x86-64 and aarch64 with LSE atomics have none.
    ///
    /// A control-plane read that walks every shard. No decision reads it.
    #[must_use]
    pub fn contended_debits(&self) -> u64 {
        self.inner
            .balance
            .as_slice()
            .iter()
            .fold(0u64, |total, shard| {
                total.saturating_add(shard.contended.load(Ordering::Relaxed))
            })
    }

    /// The contention recorded since the last call, handed out exactly once.
    ///
    /// A slot folds this into its account's running total when the lease
    /// leaves it. Idempotent across removal and reinstallation of the same
    /// lease: what was handed out is remembered on the lease itself, and a
    /// concurrent or repeated call receives only what no earlier call did.
    #[must_use]
    pub fn take_unreported_contention(&self) -> u64 {
        let total = self.contended_debits();
        let reported = self
            .inner
            .contention_reported
            .fetch_max(total, Ordering::AcqRel);
        total.saturating_sub(reported)
    }

    /// The contention a slot has not yet been handed, without handing it out.
    #[must_use]
    pub fn unreported_contention(&self) -> u64 {
        self.contended_debits()
            .saturating_sub(self.inner.contention_reported.load(Ordering::Acquire))
    }

    /// Units still spendable, aggregated across every shard.
    ///
    /// Exact for a single-counter lease, and for a sharded lease once it has
    /// quiesced — which is the state every accounting use requires, and the
    /// one `release_quiesced` establishes before settling a grant.
    ///
    /// Under concurrency a sharded read is an *estimate in both directions*,
    /// and deliberately so: a shard-by-shard walk is not one atomic instant,
    /// and a failed fragmented reservation returns its whole aggregate to one
    /// shard rather than to the shards it drew from (see
    /// `Self::try_reserve_at`). A refund landing on an already-visited
    /// shard is counted twice; one landing on a shard the walk has passed is
    /// missed. Hence the clamp: the sum can exceed the grant, so it is
    /// saturated and bounded rather than asserted, and no reader of a live
    /// lease may treat the result as an exact balance.
    #[must_use]
    pub fn remaining(&self) -> CostUnits {
        let remaining = self
            .inner
            .balance
            .as_slice()
            .iter()
            .fold(0u64, |total, shard| {
                total.saturating_add(shard.remaining.load(Ordering::Acquire))
            })
            .min(self.inner.grant.units.get());
        CostUnits(remaining)
    }

    /// True once spending has crossed the low-water mark. Monotonic in
    /// practice only between refills; the refill task polls or checks after
    /// each reservation.
    #[must_use]
    pub fn needs_refill(&self) -> bool {
        self.remaining().get() <= self.inner.low_water
    }

    /// What the refill plane should do about this lease, clearing whatever
    /// the lease was holding to tell it.
    ///
    /// [`RefillVerdict::Refused`] is tested first and outranks a low-water
    /// crossing, because the two verdicts ask for different actions and only
    /// one of them is still preventable. A crossing is an *anticipatory*
    /// signal — the lease can still serve, so the plane acquires alongside it
    /// and no request is refused between ticks. A refusal is the failure that
    /// crossing exists to avoid, already happened: there is nothing left to
    /// preserve, and acquiring alongside a grant that could not fund the work
    /// would install a *smaller* one beside it (see
    /// `Self::signal_refusal`).
    ///
    /// Re-arming: for the crossing case the second aggregate read closes the
    /// race with a debit that observed a still-set flag just before this
    /// method cleared it — that debit is either included in the recheck, or a
    /// later debit sees the cleared flag and rings the doorbell itself.
    #[must_use]
    pub fn refill_due_or_rearm(&self) -> RefillVerdict {
        // Acquires the refused debit's counter reads before the plane acts on
        // them, and consumes the doorbell so a plane that answers this
        // verdict is not woken again for the same refusal.
        if self.inner.refused.swap(false, Ordering::AcqRel) {
            return RefillVerdict::Refused;
        }
        if self.needs_refill() {
            return RefillVerdict::Draining;
        }
        for shard in self.inner.balance.as_slice() {
            // Reading `true` acquires the debit published by the signal's
            // release RMW before clearing its doorbell.
            shard.signalled.swap(false, Ordering::AcqRel);
        }
        if self.needs_refill() {
            RefillVerdict::Draining
        } else {
            RefillVerdict::Idle
        }
    }

    /// Debit `units` if the lease is live and has capacity. Lock-free; the
    /// CAS loop retries only under concurrent reservations on the same lease.
    ///
    /// This is the raw counter operation. Request code should prefer
    /// [`crate::reservation::Reservation::reserve`], which pairs the debit
    /// with the commit/release state machine.
    #[inline]
    pub fn try_debit(&self, units: CostUnits, now: Timestamp) -> Result<(), DenyReason> {
        self.try_reserve_at(units, now, Locality::current())
            .map(|_| ())
    }

    #[inline]
    pub(crate) fn try_reserve_at(
        &self,
        units: CostUnits,
        now: Timestamp,
        locality: Locality,
    ) -> Result<LeaseDebit, DenyReason> {
        if now >= self.inner.usable_until {
            // The same silence as the exhaustion exit below, for the same
            // reason: a lease that can no longer serve must say so rather
            // than wait to be discovered. Rotation keeps the poll interval as
            // its backstop for a lease no request touches (INVARIANTS.md #6).
            self.signal_refusal();
            return Err(DenyReason::LeaseExpired);
        }
        let want = units.get();
        let shards = self.inner.balance.as_slice();
        let first = locality.index(LocalSharding::new(
            std::num::NonZeroUsize::new(shards.len()).expect("lease has at least one shard"),
        ));

        // The routine path: one CAS on the caller's stable shard. Before
        // assembling a split receipt, try every sibling for the whole debit;
        // an idle shard can therefore be stolen without allocation.
        for offset in 0..shards.len() {
            let index = (first + offset) % shards.len();
            if let Some(part) = self.try_whole(index, want) {
                return Ok(part);
            }
        }

        // Genuine fragmentation: reserve pieces in a stable circular order.
        // The receipt remains fixed-size: all pieces belong to one lease, so
        // cancellation can restore their exact aggregate to one shard without
        // changing the lease bound. A failed attempt does the same before it
        // returns, so denial remains zero-charge and no capacity is stranded.
        let mut needed = want;
        for offset in 0..shards.len() {
            let index = (first + offset) % shards.len();
            needed -= self.take_up_to(index, needed);
            if needed == 0 {
                // The first shard was necessarily exhausted (or already
                // empty), so it is a sufficient shard-local refill doorbell
                // for this cold fragmented path.
                let next = shards[first].remaining.load(Ordering::Acquire);
                self.maybe_signal_refill(first, next);
                return Ok(LeaseDebit {
                    shard: first,
                    units: want,
                });
            }
        }

        self.credit_to(first, want - needed);
        // Reported after the rollback, so the plane that answers this refusal
        // reads the restored aggregate rather than a torn one. The quote is
        // raised first so the doorbell's release carries it.
        self.inner.refused_quote.fetch_max(want, Ordering::Relaxed);
        self.signal_refusal();
        Err(DenyReason::LeaseExhausted {
            remaining: self.remaining(),
        })
    }

    #[inline]
    fn try_whole(&self, shard_index: usize, want: u64) -> Option<LeaseDebit> {
        let shard = &self.inner.balance.as_slice()[shard_index];
        let mut current = shard.remaining.load(Ordering::Acquire);
        // Counted in a register and recorded once on every exit, so an
        // uncontended debit pays one untaken branch.
        let mut lost = 0u64;
        loop {
            let Some(next) = current.checked_sub(want) else {
                shard.note_contention(lost);
                return None;
            };
            match shard.remaining.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    shard.note_contention(lost);
                    self.maybe_signal_refill(shard_index, next);
                    return Some(LeaseDebit {
                        shard: shard_index,
                        units: want,
                    });
                }
                Err(observed) => {
                    lost += 1;
                    current = observed;
                }
            }
        }
    }

    #[cold]
    fn take_up_to(&self, shard_index: usize, want: u64) -> u64 {
        let shard = &self.inner.balance.as_slice()[shard_index];
        let mut current = shard.remaining.load(Ordering::Acquire);
        let mut lost = 0u64;
        loop {
            if current == 0 {
                shard.note_contention(lost);
                return 0;
            }
            let taken = current.min(want);
            let next = current - taken;
            match shard.remaining.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    shard.note_contention(lost);
                    return taken;
                }
                Err(observed) => {
                    lost += 1;
                    current = observed;
                }
            }
        }
    }

    #[inline]
    fn maybe_signal_refill(&self, shard_index: usize, next: u64) {
        if next <= self.inner.balance.as_slice()[shard_index].low_water {
            self.signal_refill(shard_index);
        }
    }

    /// Report a debit this lease could not fund, and ring the doorbell the
    /// first time.
    ///
    /// A refusal is the one lease event that *proves* the grant can no longer
    /// serve the work being offered, so it is the one event the refill plane
    /// most needs and, before #109, the only one it was never told about.
    /// Low water cannot stand in for it: the mark counts units and the
    /// refusal is about a quote, so a lease holding more units than its mark
    /// can refuse every request until its TTL while the account has the
    /// balance to fund them.
    ///
    /// Cold, and once per lease: the flag is both the report the plane reads
    /// and the token that keeps a refusal storm from becoming a wake storm.
    /// A refusal arriving while an earlier one is still unanswered adds
    /// nothing — the plane's response does not depend on how many there were.
    #[cold]
    #[inline(never)]
    fn signal_refusal(&self) {
        // Release publishes the preceding counter reads to the plane that
        // clears this flag; a swap that observes `true` means an unanswered
        // report already stands.
        if self.inner.refused.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(signal) = self.inner.refill.get() {
            signal.request_refill();
        }
    }

    /// Raise the refill signal at most once per shard between control-plane
    /// aggregate checks.
    ///
    /// Out of line and `#[cold]`: every debit tests the branch above, but only
    /// one debit per lease ever arrives here, so none of this belongs in the
    /// hot path's instruction stream.
    #[cold]
    #[inline(never)]
    fn signal_refill(&self, shard_index: usize) {
        let Some(signal) = self.inner.refill.get() else {
            return;
        };
        // Release publishes the preceding remaining-counter CAS to the
        // control plane when it clears this doorbell. The implementation
        // behind `request_refill` remains responsible for its own wake state.
        if !self.inner.balance.as_slice()[shard_index]
            .signalled
            .swap(true, Ordering::Release)
        {
            signal.request_refill();
        }
    }

    /// Return previously debited units (release of an uncommitted
    /// reservation). Callers must return only units they debited, exactly
    /// once — the reservation state machine guarantees this.
    #[inline]
    pub(crate) fn credit(&self, debit: &LeaseDebit) {
        self.credit_to(debit.shard, debit.units);
    }

    fn credit_to(&self, shard: usize, units: u64) {
        self.inner.balance.as_slice()[shard]
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(units)
            })
            .expect("a debit receipt cannot credit beyond its lease grant");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn overage() -> AccountOverage {
        AccountOverage::new(AccountId(1))
    }

    /// The tentative debit's whole reason for existing: units taken but never
    /// resolved come back on their own. Without the guard they would sit in
    /// `spent` with no owner — a panic or an early return between the debit
    /// and the claim would strand them for the life of the process, silently
    /// shrinking the account's cap.
    #[test]
    fn dropping_an_unresolved_tentative_debit_returns_the_credit() {
        let o = overage();
        {
            let tentative = o.debit_tentatively(CostUnits(40), CostUnits(100)).unwrap();
            assert_eq!(o.spent(), CostUnits(40));
            drop(tentative);
        }
        assert_eq!(o.spent(), CostUnits::ZERO);
        assert_eq!(o.headroom(CostUnits(100)), CostUnits(100));
    }

    /// A losing claim credits the debit back before returning, so the cap is
    /// immediately reusable and committed occupancy never moved.
    #[test]
    fn a_tentative_debit_whose_claim_loses_returns_the_credit() {
        let o = overage();
        let tentative = o.debit_tentatively(CostUnits(40), CostUnits(100)).unwrap();
        assert_eq!(o.spent(), CostUnits(40));

        let lost: Result<(), ()> = tentative.publish_commit(|| Err(()));
        assert!(lost.is_err());
        assert_eq!(o.spent(), CostUnits::ZERO);
        assert_eq!(o.headroom(CostUnits(100)), CostUnits(100));
    }

    /// A winning claim retains the debit and advances committed occupancy,
    /// which is what makes those units irrevocable to a cap observer.
    #[test]
    fn a_tentative_debit_whose_claim_wins_becomes_irrevocable() {
        let o = overage();
        let tentative = o.debit_tentatively(CostUnits(40), CostUnits(100)).unwrap();
        let won: Result<(), ()> = tentative.publish_commit(|| Ok(()));
        assert!(won.is_ok());
        assert_eq!(o.spent(), CostUnits(40));
        // Committed occupancy moved with it, so the remaining cap is stably
        // exhausted rather than temporarily so.
        assert_eq!(
            o.try_debit(CostUnits(61), CostUnits(100)),
            Err(DenyReason::OverageCapExhausted {
                spent: CostUnits(40),
                overage_cap: CostUnits(100),
            })
        );
    }

    #[test]
    fn committed_overage_accumulates_up_to_the_cap_and_then_refuses() {
        let o = overage();
        o.try_debit(CostUnits(40), CostUnits(100)).unwrap();
        o.publish_claim(CostUnits(40), || Ok::<_, ()>(())).unwrap();
        o.try_debit(CostUnits(60), CostUnits(100)).unwrap();
        o.publish_claim(CostUnits(60), || Ok::<_, ()>(())).unwrap();
        assert_eq!(o.spent(), CostUnits(100));
        assert_eq!(o.headroom(CostUnits(100)), CostUnits::ZERO);
        assert_eq!(
            o.try_debit(CostUnits(1), CostUnits(100)),
            Err(DenyReason::OverageCapExhausted {
                spent: CostUnits(100),
                overage_cap: CostUnits(100),
            })
        );
        assert_eq!(o.spent(), CostUnits(100), "a refusal claims nothing");
    }

    /// A pending debit changes which local overage state is reported when
    /// returning all pending credit would make this request fit. A request
    /// larger than the whole cap is stable local saturation, but it remains
    /// retryable because a background lease grant can fund it.
    #[test]
    fn pending_overage_is_transient_only_when_its_refund_would_make_room() {
        let o = overage();
        o.try_debit(CostUnits(60), CostUnits(100)).unwrap();

        let pending_saturation = o.try_debit(CostUnits(50), CostUnits(100)).unwrap_err();
        assert_eq!(
            pending_saturation,
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(60),
                overage_cap: CostUnits(100),
            }
        );
        assert_eq!(pending_saturation.retry(), crate::deny::Retry::Transient);

        let request_exceeds_cap = o.try_debit(CostUnits(101), CostUnits(100)).unwrap_err();
        assert_eq!(
            request_exceeds_cap,
            DenyReason::OverageCapExhausted {
                spent: CostUnits(60),
                overage_cap: CostUnits(100),
            }
        );
        assert_eq!(request_exceeds_cap.retry(), crate::deny::Retry::Transient);
    }

    /// The cap is a parameter, so lowering it below what an account has
    /// already spent refuses immediately rather than waiting for the counter
    /// to catch up — and `headroom` saturates instead of underflowing.
    #[test]
    fn lowering_the_cap_below_current_spend_refuses_at_once() {
        let o = overage();
        o.try_debit(CostUnits(80), CostUnits(100)).unwrap();
        assert_eq!(o.headroom(CostUnits(50)), CostUnits::ZERO);
        assert!(o.try_debit(CostUnits(1), CostUnits(50)).is_err());
        o.try_debit(CostUnits(1), CostUnits(100)).unwrap();
    }

    /// A total that cannot be represented is the same refusal as one that
    /// exceeds the cap: never a wrap to a small spend, which would reopen the
    /// cap (INVARIANTS.md #11).
    #[test]
    fn an_unrepresentable_total_refuses_rather_than_wrapping() {
        let o = overage();
        o.try_debit(CostUnits(u64::MAX - 1), CostUnits(u64::MAX))
            .unwrap();
        assert!(o.try_debit(CostUnits(2), CostUnits(u64::MAX)).is_err());
        assert_eq!(o.spent(), CostUnits(u64::MAX - 1));
    }

    #[test]
    fn credit_returns_headroom_to_the_cap() {
        let o = overage();
        o.try_debit(CostUnits(100), CostUnits(100)).unwrap();
        o.credit(CostUnits(30));
        assert_eq!(o.spent(), CostUnits(70));
        assert_eq!(o.headroom(CostUnits(100)), CostUnits(30));
        o.try_debit(CostUnits(30), CostUnits(100)).unwrap();
        assert!(o.try_debit(CostUnits(1), CostUnits(100)).is_err());
    }

    /// Concurrent debits that individually fit the cap must not jointly
    /// exceed it. The cap comparison lives inside the compare-exchange for
    /// exactly this reason; a check-then-claim would let both threads through.
    #[test]
    fn concurrent_debits_never_exceed_the_cap() {
        const THREADS: usize = 8;
        const EACH: usize = 500;
        const CAP: u64 = 1_000;
        let o = Arc::new(overage());
        let admitted = Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let o = Arc::clone(&o);
                let admitted = Arc::clone(&admitted);
                scope.spawn(move || {
                    for _ in 0..EACH {
                        if o.try_debit(CostUnits(1), CostUnits(CAP)).is_ok() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(o.spent(), CostUnits(CAP));
        assert_eq!(
            admitted.load(Ordering::Relaxed),
            CAP,
            "every admitted unit is one the counter recorded, and vice versa"
        );
    }

    fn lease(units: u64, expires: i64, low_water: u64) -> LocalLease {
        LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(3),
                units: CostUnits(units),
                expires_at: t(expires),
            },
            CostUnits(low_water),
        )
    }

    fn sharded_lease(units: u64, low_water: u64, shards: usize) -> LocalLease {
        LocalLease::with_sharding(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(3),
                units: CostUnits(units),
                expires_at: t(1_000),
            },
            CostUnits(low_water),
            jiff::SignedDuration::ZERO,
            LocalSharding::new(NonZeroUsize::new(shards).unwrap()),
        )
    }

    #[test]
    fn sharded_grant_and_low_water_partitions_are_exact() {
        assert_eq!(align_of::<LeaseShard>(), 128);
        assert_eq!(size_of::<LeaseShard>(), 128);
        assert_eq!(align_of::<LocalLease>(), 128);

        let l = sharded_lease(10, 3, 4);
        let shards = l.inner.balance.as_slice();
        assert_eq!(shards.len(), 4);
        assert_eq!(
            shards
                .iter()
                .map(|shard| shard.remaining.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            [3, 3, 2, 2]
        );
        assert_eq!(
            shards
                .iter()
                .map(|shard| shard.low_water)
                .collect::<Vec<_>>(),
            [1, 1, 1, 0]
        );
        assert_eq!(l.remaining(), CostUnits(10));
    }

    #[test]
    fn cloned_local_views_are_visible_to_quiescence_detection() {
        let lease = sharded_lease(10, 0, 2);
        assert!(lease.is_only_local_view());
        let sibling = lease.clone();
        assert!(!lease.is_only_local_view());
        drop(sibling);
        assert!(lease.is_only_local_view());
    }

    #[test]
    fn fragmented_reservation_refunds_without_stranding_capacity() {
        let l = Arc::new(sharded_lease(10, 0, 4));
        assert_eq!(size_of::<LeaseDebit>(), 16, "the receipt is fixed-size");

        let reservation = crate::Reservation::reserve(&l, CostUnits(8), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits(2));
        assert_eq!(reservation.cancel(), crate::CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(10));
    }

    #[test]
    fn failed_fragmented_debit_reports_true_remaining_and_rolls_back() {
        let l = sharded_lease(10, 0, 4);

        assert_eq!(
            l.try_debit(CostUnits(11), t(0)),
            Err(DenyReason::LeaseExhausted {
                remaining: CostUnits(10)
            })
        );
        assert_eq!(l.remaining(), CostUnits(10));
    }

    #[test]
    fn rebalanced_refunds_are_exact_at_the_u64_boundary() {
        let l = Arc::new(sharded_lease(u64::MAX, 0, 2));
        let reservation = crate::Reservation::reserve(&l, CostUnits(u64::MAX), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits::ZERO);
        assert_eq!(reservation.cancel(), crate::CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(u64::MAX));

        // The refund is deliberately allowed to concentrate the grant on one
        // shard. A second maximum-sized reserve proves that representation is
        // spendable and cannot overflow its refund destination.
        let reservation = crate::Reservation::reserve(&l, CostUnits(u64::MAX), t(0)).unwrap();
        assert_eq!(reservation.cancel(), crate::CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(u64::MAX));
    }

    /// No rival, no lost exchange: a debit that won its first compare-exchange
    /// records nothing, on the single and the sharded layout alike. Limited to
    /// targets whose weak exchange cannot fail spuriously; on a
    /// load-linked/store-conditional target a spurious failure also counts.
    #[cfg(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "lse")
    ))]
    #[test]
    fn an_uncontended_debit_records_no_contention() {
        for l in [lease(1_000, 1_000, 0), sharded_lease(1_000, 0, 4)] {
            for _ in 0..100 {
                let debit = l
                    .try_reserve_at(CostUnits(3), t(0), Locality::current())
                    .unwrap();
                l.credit(&debit);
            }
            // Fragmented debits walk `take_up_to` instead of `try_whole`.
            let fragmented = l
                .try_reserve_at(CostUnits(1_000), t(0), Locality::current())
                .unwrap();
            l.credit(&fragmented);
            assert_eq!(l.contended_debits(), 0);
        }
    }

    /// Many writers on one line lose exchanges, and each loss is recorded on
    /// the shard it was lost on without disturbing the balance.
    #[test]
    fn contended_debits_are_recorded_without_disturbing_the_balance() {
        const THREADS: u64 = 8;
        const DEBITS: u64 = 20_000;
        let l = lease(THREADS * DEBITS, 1_000, 0);
        // Retries are a race, so the stress repeats until one is seen. Eight
        // threads on one line lose races within the first round on any
        // multi-core host; the bound keeps a single-core host from hanging.
        for _ in 0..50 {
            std::thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        for _ in 0..DEBITS {
                            let debit = l
                                .try_reserve_at(CostUnits(1), t(0), Locality::current())
                                .unwrap();
                            l.credit(&debit);
                        }
                    });
                }
            });
            if l.contended_debits() > 0 {
                break;
            }
        }
        assert!(
            l.contended_debits() > 0,
            "eight writers on one line never lost a race"
        );
        assert_eq!(
            l.remaining(),
            CostUnits(THREADS * DEBITS),
            "credits restored every debit"
        );
    }

    /// The hand-out is exact and idempotent. A slot that takes the lease out,
    /// reinstalls it, and takes it out again must not report the same
    /// contention twice.
    #[test]
    fn unreported_contention_is_handed_out_exactly_once() {
        let l = sharded_lease(100, 0, 2);
        let shards = l.inner.balance.as_slice();
        shards[0].note_contention(3);
        shards[1].note_contention(4);
        assert_eq!(l.contended_debits(), 7);
        assert_eq!(l.unreported_contention(), 7);
        assert_eq!(l.take_unreported_contention(), 7);
        assert_eq!(
            l.take_unreported_contention(),
            0,
            "reinstalled and taken again"
        );
        assert_eq!(l.unreported_contention(), 0);
        shards[1].note_contention(2);
        assert_eq!(l.unreported_contention(), 2);
        assert_eq!(l.take_unreported_contention(), 2);
        assert_eq!(
            l.contended_debits(),
            9,
            "the lease's own total never resets"
        );
        // A handle cloned into another locality view shares the same record.
        assert_eq!(l.clone().take_unreported_contention(), 0);
        shards[0].note_contention(0);
        assert_eq!(l.contended_debits(), 9, "recording zero writes nothing");
    }

    #[test]
    fn sharded_lease_spends_to_exact_exhaustion_without_stranding() {
        let l = sharded_lease(17, 0, 8);
        for _ in 0..17 {
            l.try_debit(CostUnits(1), t(0)).unwrap();
        }
        assert_eq!(l.remaining(), CostUnits::ZERO);
        assert_eq!(
            l.try_debit(CostUnits(1), t(0)),
            Err(DenyReason::LeaseExhausted {
                remaining: CostUnits::ZERO
            })
        );
    }

    #[test]
    fn debit_search_starts_at_the_supplied_locality_and_wraps_in_order() {
        let l = sharded_lease(16, 0, 4);
        let debit = l
            .try_reserve_at(CostUnits(1), t(0), Locality::for_test(3))
            .unwrap();
        let remaining: Vec<_> = l
            .inner
            .balance
            .as_slice()
            .iter()
            .map(|shard| shard.remaining.load(Ordering::Relaxed))
            .collect();
        assert_eq!(remaining, [4, 4, 4, 3]);
        l.credit(&debit);

        let fragmented = l
            .try_reserve_at(CostUnits(5), t(0), Locality::for_test(3))
            .unwrap();
        let remaining: Vec<_> = l
            .inner
            .balance
            .as_slice()
            .iter()
            .map(|shard| shard.remaining.load(Ordering::Relaxed))
            .collect();
        assert_eq!(remaining, [3, 4, 4, 0]);
        l.credit(&fragmented);
        assert_eq!(l.remaining(), CostUnits(16));
    }

    /// A sibling that can satisfy the whole debit is the routine fallback,
    /// not fragmentation. Keeping the debit on one counter is what avoids an
    /// O(shards) gather and concentrates its cancellation on the counter that
    /// actually paid it.
    #[test]
    fn a_whole_sibling_is_used_before_fragmenting() {
        let l = sharded_lease(16, 0, 4);

        // Leave locality 3 with three units while locality 0 still has four.
        let first = l
            .try_reserve_at(CostUnits(1), t(0), Locality::for_test(3))
            .unwrap();
        let sibling = l
            .try_reserve_at(CostUnits(4), t(0), Locality::for_test(3))
            .unwrap();

        let remaining: Vec<_> = l
            .inner
            .balance
            .as_slice()
            .iter()
            .map(|shard| shard.remaining.load(Ordering::Relaxed))
            .collect();
        assert_eq!(
            remaining,
            [0, 4, 4, 3],
            "the whole sibling pays; the undersized local shard is untouched"
        );

        l.credit(&sibling);
        l.credit(&first);
        assert_eq!(l.remaining(), CostUnits(16));
    }

    #[test]
    fn a_torn_aggregate_above_the_grant_reads_as_the_grant() {
        let l = sharded_lease(4, 0, 2);
        // The state a concurrent walk can observe: the reader counted shard 0
        // at two units, then a failed fragmented reservation drained both
        // shards and returned their whole aggregate to shard 1 — which the
        // reader has not visited yet. Its sum is six against a grant of four.
        let LeaseBalance::Sharded(shards) = &l.inner.balance else {
            panic!("a two-shard lease is sharded");
        };
        shards[1].remaining.store(4, Ordering::Release);

        assert_eq!(
            l.remaining(),
            CostUnits(4),
            "an over-counted walk is clamped to the grant, never asserted"
        );
    }

    #[test]
    fn a_fragmenting_rollback_never_panics_a_concurrent_aggregate_read() {
        // The rollback path concentrates a failed reservation's aggregate on
        // one shard, so a reader partway through its walk can count the same
        // units twice. Before the clamp that tripped `remaining`'s debug
        // assertion here, and its checked-add in release.
        let l = Arc::new(sharded_lease(6_400, 0, 64));
        let stop = Arc::new(AtomicBool::new(false));
        let spenders: Vec<_> = (48..52)
            .map(|locality| {
                let l = Arc::clone(&l);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        // One unit more than the whole grant: every attempt
                        // drains all sixty-four shards, fails, and rolls the
                        // aggregate back onto this locality's own shard —
                        // far enough along the walk to be reached after a
                        // reader has already counted the shards before it.
                        drop(l.try_reserve_at(
                            CostUnits(6_401),
                            t(0),
                            Locality::for_test(locality),
                        ));
                    }
                })
            })
            .collect();
        for _ in 0..200_000 {
            assert!(l.remaining() <= CostUnits(6_400));
        }
        stop.store(true, Ordering::Relaxed);
        for spender in spenders {
            spender.join().unwrap();
        }
        assert_eq!(
            l.remaining(),
            CostUnits(6_400),
            "no units were lost or created"
        );
    }

    #[test]
    fn an_early_shard_signal_is_rearmed_until_the_aggregate_crosses() {
        let signal = Arc::new(CountingSignal::default());
        let l = sharded_lease(100, 20, 2).with_refill(signal.clone());

        l.try_debit(CostUnits(40), t(0)).unwrap();
        assert_eq!(signal.count(), 1, "the first shard crossed its share");
        assert_eq!(
            l.refill_due_or_rearm(),
            RefillVerdict::Idle,
            "sixty aggregate units remain"
        );

        l.try_debit(CostUnits(1), t(0)).unwrap();
        assert_eq!(signal.count(), 2, "the control-plane check rearmed it");
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Idle);

        l.try_debit(CostUnits(40), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits(19));
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Draining);
    }

    #[test]
    fn debit_decrements_and_credit_restores() {
        let l = lease(100, 1_000, 25);
        let debit = l
            .try_reserve_at(CostUnits(60), t(0), Locality::current())
            .unwrap();
        assert_eq!(l.remaining(), CostUnits(40));
        l.credit(&debit);
        assert_eq!(l.remaining(), CostUnits(100));
    }

    #[test]
    fn exhaustion_denies_with_remaining() {
        let l = lease(10, 1_000, 0);
        assert_eq!(
            l.try_debit(CostUnits(11), t(0)),
            Err(DenyReason::LeaseExhausted {
                remaining: CostUnits(10)
            })
        );
        // Exact spend-to-zero is allowed.
        l.try_debit(CostUnits(10), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits::ZERO);
    }

    #[test]
    fn expiry_boundary_is_exclusive_of_expires_at() {
        let l = lease(10, 500, 0);
        assert_eq!(
            l.try_debit(CostUnits(1), t(500)),
            Err(DenyReason::LeaseExpired)
        );
        l.try_debit(CostUnits(1), t(499)).unwrap();
    }

    #[test]
    fn low_water_triggers_refill_signal() {
        let l = lease(100, 1_000, 25);
        assert!(!l.needs_refill());
        l.try_debit(CostUnits(75), t(0)).unwrap();
        assert!(l.needs_refill());
    }

    /// Counts calls so the exactly-once contract can be asserted rather than
    /// assumed.
    #[derive(Debug, Default)]
    struct CountingSignal(AtomicU64);

    impl RefillSignal for CountingSignal {
        fn request_refill(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl CountingSignal {
        fn count(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// The signal fires on the debit that *crosses* low water — not before,
    /// and, because the threshold is "at or below", not one debit late.
    #[test]
    fn the_crossing_debit_raises_the_signal() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(100, 1_000, 25).with_refill(signal.clone());

        l.try_debit(CostUnits(74), t(0)).unwrap();
        assert_eq!(signal.count(), 0, "26 remaining is above low water");
        l.try_debit(CostUnits(1), t(0)).unwrap();
        assert_eq!(signal.count(), 1, "landing exactly on low water crosses it");
    }

    /// #109: the refusal is the one lease event that *proves* the grant can
    /// no longer serve the work offered, and it was the one event the refill
    /// plane was never told about. A low-water crossing cannot stand in for
    /// it: this lease is comfortably above its mark and still cannot fund the
    /// quote, so nothing crosses, and before this the plane learned nothing.
    #[test]
    fn a_refused_debit_tells_the_refill_plane_rather_than_waiting_to_be_polled() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(49, 1_000, 25).with_refill(signal.clone());

        assert!(matches!(
            l.try_debit(CostUnits(51), t(0)),
            Err(DenyReason::LeaseExhausted { .. })
        ));
        assert_eq!(signal.count(), 1, "the refusal rang the doorbell");
        assert!(
            !l.needs_refill(),
            "and it rang from above the mark, which is the whole point"
        );
        assert_eq!(
            l.refill_due_or_rearm(),
            RefillVerdict::Refused,
            "the plane is told the grant is mis-sized, not that it is draining"
        );
        assert_eq!(
            l.refill_due_or_rearm(),
            RefillVerdict::Idle,
            "reading the verdict consumes it; one refusal is one rotation"
        );
        assert_eq!(
            l.remaining(),
            CostUnits(49),
            "a refusal is still zero-charge"
        );
    }

    /// #131: a consolidation may grow only to demand the lease has proven, so
    /// the refusal records its quote, keeps the largest across a storm, and an
    /// expiry refusal records nothing because it rotates rather than folds.
    #[test]
    fn a_refused_debit_records_its_largest_quote() {
        for l in [lease(49, 1_000, 25), sharded_lease(49, 25, 4)] {
            assert_eq!(l.largest_refused_quote(), CostUnits::ZERO);
            for quote in [51, 90, 60] {
                assert!(l.try_debit(CostUnits(quote), t(0)).is_err());
            }
            assert_eq!(l.largest_refused_quote(), CostUnits(90));
            l.try_debit(CostUnits(10), t(0)).unwrap();
            assert_eq!(
                l.largest_refused_quote(),
                CostUnits(90),
                "a funded debit is not demand the grant failed"
            );
        }
        let expired = lease(49, 1_000, 25);
        assert!(matches!(
            expired.try_debit(CostUnits(51), t(1_000)),
            Err(DenyReason::LeaseExpired)
        ));
        assert_eq!(expired.largest_refused_quote(), CostUnits::ZERO);
    }

    /// The doorbell is rung once however hard the caller retries: the plane's
    /// response does not depend on how many requests were refused, and a
    /// refusal storm must not become a wake storm on the refill task.
    #[test]
    fn a_refusal_storm_rings_once_and_reports_once() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(49, 1_000, 25).with_refill(signal.clone());

        for _ in 0..1_000 {
            assert!(l.try_debit(CostUnits(51), t(0)).is_err());
        }
        assert_eq!(signal.count(), 1);
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);

        // Cleared, so a later refusal is a fresh report the plane must act on.
        assert!(l.try_debit(CostUnits(51), t(0)).is_err());
        assert_eq!(signal.count(), 2);
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
    }

    /// A refusal outranks a crossing because the two ask for different things
    /// and only one is still preventable. Rotating *alongside* a lease that
    /// already refused work would size the next grant against a balance this
    /// lease's unspent units are missing from (#109).
    #[test]
    fn a_refusal_outranks_a_low_water_crossing() {
        let l = lease(100, 1_000, 90);

        // One debit both crosses low water and leaves too little for the next.
        l.try_debit(CostUnits(20), t(0)).unwrap();
        assert!(l.needs_refill(), "80 remaining is below the 90 mark");
        assert!(l.try_debit(CostUnits(81), t(0)).is_err());

        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
        assert_eq!(
            l.refill_due_or_rearm(),
            RefillVerdict::Draining,
            "the crossing is still there once the refusal has been answered"
        );
    }

    /// The expiry refusal was silent for the same reason the exhaustion one
    /// was, and is the same defect. Rollover keeps the poll interval as its
    /// backstop for a lease no request touches; a lease requests *are*
    /// reaching now says so on the first one (INVARIANTS.md #6).
    #[test]
    fn an_expired_lease_reports_its_refusal_rather_than_waiting_for_the_tick() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(100, 1_000, 25).with_refill(signal.clone());

        assert_eq!(
            l.try_debit(CostUnits(1), t(2_000)),
            Err(DenyReason::LeaseExpired)
        );
        assert_eq!(signal.count(), 1);
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
    }

    /// A lease nobody refills still records the refusal, exactly as it records
    /// a crossing: the verdict is lease state, and attaching a doorbell only
    /// decides whether the plane hears about it sooner.
    #[test]
    fn a_lease_with_no_doorbell_still_records_its_refusal() {
        let l = lease(49, 1_000, 25);
        assert!(l.try_debit(CostUnits(51), t(0)).is_err());
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
    }

    /// A sharded lease refuses only after walking every sibling, so the report
    /// is about the grant and not about one counter — which is why the flag is
    /// lease-wide and one refusal is one report however many shards it tried.
    #[test]
    fn a_sharded_refusal_reports_once_for_the_whole_grant() {
        let signal = Arc::new(CountingSignal::default());
        let l = sharded_lease(40, 4, 4).with_refill(signal.clone());

        assert!(matches!(
            l.try_debit(CostUnits(41), t(0)),
            Err(DenyReason::LeaseExhausted { .. })
        ));
        assert_eq!(signal.count(), 1);
        assert_eq!(l.remaining(), CostUnits(40), "the rollback restored it all");
        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
    }

    /// A lease is replaced rather than refilled, so "at most once" needs no
    /// reset protocol — but it does need proving, since every later debit on
    /// a drained lease still tests the branch.
    #[test]
    fn a_lease_signals_at_most_once_however_long_it_drains() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(100, 1_000, 25).with_refill(signal.clone());

        // Eighty single-unit debits against a hundred units: every one is
        // admissible, so a failure here would be the test lying, not the
        // lease refusing.
        for _ in 0..80 {
            l.try_debit(CostUnits(1), t(0)).unwrap();
        }
        assert_eq!(l.remaining(), CostUnits(20));
        assert_eq!(
            signal.count(),
            1,
            "one crossing, however many debits followed it"
        );

        // A fresh lease is a fresh flag: this is the whole reset mechanism.
        let next = lease(100, 1_000, 25).with_refill(signal.clone());
        next.try_debit(CostUnits(80), t(0)).unwrap();
        assert_eq!(signal.count(), 2);
    }

    /// A refused debit changes no counter, so it must never claim a
    /// *crossing* — and it must still report the refusal.
    ///
    /// This test used to assert the silence outright (`a_refused_debit_never
    /// _signals`), on the reasoning that a debit which moved no counter has
    /// nothing to announce. The premise held for crossings and hid #109: it
    /// read the doorbell as "low water was crossed" when what the refill
    /// plane needs is "act on this lease", and those differ exactly here.
    /// Both facts are now representable at once, so neither has to be given
    /// up: the verdict distinguishes them and the shard flags stay clear.
    #[test]
    fn a_refused_debit_reports_a_refusal_and_never_a_crossing() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(100, 1_000, 25).with_refill(signal.clone());

        assert!(l.try_debit(CostUnits(500), t(0)).is_err(), "exhausted");
        assert_eq!(signal.count(), 1, "the plane is told once");
        assert!(
            l.try_debit(CostUnits(10), t(10_000)).is_err(),
            "past the usability window"
        );
        assert_eq!(
            signal.count(),
            1,
            "and not again while that report still stands"
        );
        assert_eq!(l.remaining(), CostUnits(100), "still zero-charge");

        assert_eq!(l.refill_due_or_rearm(), RefillVerdict::Refused);
        assert_eq!(
            l.refill_due_or_rearm(),
            RefillVerdict::Idle,
            "seventy-five units above the mark: no crossing was ever claimed"
        );
    }

    /// A lease with no signal attached is the pre-#10 behaviour: the crossing
    /// is still recorded for the poll loop, it simply arrives later.
    #[test]
    fn a_lease_without_a_signal_still_reports_the_crossing() {
        let l = lease(100, 1_000, 25);
        l.try_debit(CostUnits(80), t(0)).unwrap();
        assert!(l.needs_refill());
    }

    #[test]
    fn negative_safety_margin_fails_closed() {
        let grant = LeaseGrant {
            lease_id: LeaseId(8),
            account_id: AccountId(1),
            fencing_token: FencingToken(4),
            units: CostUnits(10),
            expires_at: t(100),
        };
        let l = LocalLease::with_safety_margin(
            grant,
            CostUnits::ZERO,
            jiff::SignedDuration::from_secs(-10),
        );
        assert_eq!(
            l.try_debit(CostUnits(1), t(99)),
            Err(DenyReason::LeaseExpired)
        );
    }

    #[test]
    fn concurrent_debits_never_overspend() {
        use std::sync::Arc;
        let l = Arc::new(lease(1_000, 1_000, 0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let l = Arc::clone(&l);
            handles.push(std::thread::spawn(move || {
                let mut granted = 0u64;
                for _ in 0..1_000 {
                    if l.try_debit(CostUnits(1), t(0)).is_ok() {
                        granted += 1;
                    }
                }
                granted
            }));
        }
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // 8000 attempts against 1000 units: exactly the lease size is granted.
        assert_eq!(total, 1_000);
        assert_eq!(l.remaining(), CostUnits::ZERO);
    }
}
