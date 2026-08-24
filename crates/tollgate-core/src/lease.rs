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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
}

impl LeaseShard {
    fn new(remaining: u64, low_water: u64) -> Self {
        Self {
            remaining: AtomicU64::new(remaining),
            low_water,
            signalled: AtomicBool::new(false),
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
    /// [`Self::try_reserve_at`]). A refund landing on an already-visited
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

    /// Re-arm shard-local low-water signals after the refill plane proves an
    /// early shard crossing did not yet mean the aggregate was low.
    ///
    /// The second aggregate read closes the race with a debit that observed a
    /// still-set flag just before this method cleared it: that debit is either
    /// included in the recheck, or a later debit sees the cleared flag and
    /// rings the doorbell itself.
    #[must_use]
    pub fn refill_due_or_rearm(&self) -> bool {
        if self.needs_refill() {
            return true;
        }
        for shard in self.inner.balance.as_slice() {
            // Reading `true` acquires the debit published by the signal's
            // release RMW before clearing its doorbell.
            shard.signalled.swap(false, Ordering::AcqRel);
        }
        self.needs_refill()
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
        Err(DenyReason::LeaseExhausted {
            remaining: self.remaining(),
        })
    }

    #[inline]
    fn try_whole(&self, shard_index: usize, want: u64) -> Option<LeaseDebit> {
        let shard = &self.inner.balance.as_slice()[shard_index];
        let mut current = shard.remaining.load(Ordering::Acquire);
        loop {
            let next = current.checked_sub(want)?;
            match shard.remaining.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.maybe_signal_refill(shard_index, next);
                    return Some(LeaseDebit {
                        shard: shard_index,
                        units: want,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    #[cold]
    fn take_up_to(&self, shard_index: usize, want: u64) -> u64 {
        let shard = &self.inner.balance.as_slice()[shard_index];
        let mut current = shard.remaining.load(Ordering::Acquire);
        loop {
            if current == 0 {
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
                Ok(_) => return taken,
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    fn maybe_signal_refill(&self, shard_index: usize, next: u64) {
        if next <= self.inner.balance.as_slice()[shard_index].low_water {
            self.signal_refill(shard_index);
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
        assert!(!l.refill_due_or_rearm(), "sixty aggregate units remain");

        l.try_debit(CostUnits(1), t(0)).unwrap();
        assert_eq!(signal.count(), 2, "the control-plane check rearmed it");
        assert!(!l.refill_due_or_rearm());

        l.try_debit(CostUnits(40), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits(19));
        assert!(l.refill_due_or_rearm());
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

    /// A refused debit changes no counter, so it must not claim a crossing.
    #[test]
    fn a_refused_debit_never_signals() {
        let signal = Arc::new(CountingSignal::default());
        let l = lease(100, 1_000, 25).with_refill(signal.clone());

        assert!(l.try_debit(CostUnits(500), t(0)).is_err(), "exhausted");
        assert!(
            l.try_debit(CostUnits(10), t(10_000)).is_err(),
            "past the usability window"
        );
        assert_eq!(signal.count(), 0);
        assert_eq!(l.remaining(), CostUnits(100));
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
