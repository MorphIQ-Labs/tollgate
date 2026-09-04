//! The reservation state machine: `pending → committed-at-execution-start`
//! or `pending → released`.
//!
//! The charging rules this encodes (mirroring ferro-risk's `ChargePhase`
//! invariants, service/INVARIANTS.md 5–8 there):
//!
//! - Admission debits the lease immediately, but the charge is only *pending*.
//! - Execution start commits the full quoted charge — for success, domain
//!   failure, or timeout alike.
//! - Anything that ends the request before execution (validation failure,
//!   client cancellation, shedding, drop) releases the units for zero charge.
//! - Commit and cancel race on one atomic compare-exchange: exactly one wins
//!   (INVARIANTS.md #3 here). A canceller that loses learns the committed
//!   charge; a committer that loses must not execute.
//! - Under `Elastic`, a lease whose usability window lapsed between admission
//!   and execution start settles against overage instead of refusing — still
//!   one compare-exchange, so the canceller and the committer still race for
//!   a single phase rather than for two separate reservations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use jiff::Timestamp;

use crate::deny::DenyReason;
use crate::ids::{AccountId, RequestId};
use crate::lease::{AccountOverage, LeaseDebit, LocalLease};
use crate::sharding::Locality;
use crate::snapshot::EnforcementMode;
use crate::units::CostUnits;
use crate::usage::{UsageEvent, UsageSource};

// The phase word is the single authority for how a reservation *resolved*,
// and it names the funding that resolution settled against. `ChargeSource`
// stays the immutable funding *receipt*: it routes refunds, owns the lease
// window and capability, and supplies the initial phase value — but it is not
// the billing statement, because a leased reservation whose window lapses at
// execution start can settle against overage instead (INVARIANTS.md #3).
//
//     PENDING_LEASE ─┬→ COMMITTED_LEASE
//                    ├→ COMMITTED_OVERAGE   (Elastic commit-time fallback)
//                    └→ RELEASED
//     PENDING_OVERAGE ─→ COMMITTED_OVERAGE | RELEASED
const PENDING_LEASE: u8 = 0;
const PENDING_OVERAGE: u8 = 1;
const COMMITTED_LEASE: u8 = 2;
const COMMITTED_OVERAGE: u8 = 3;
const RELEASED: u8 = 4;

/// Outcome of [`Reservation::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// Cancellation won (or the reservation was already released): zero units
    /// charged, units returned to the lease.
    ZeroCharged,
    /// Execution had already started; the full charge stands.
    AlreadyCommitted { units: CostUnits },
}

/// Error from [`Reservation::commit_at_execution_start`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitError {
    /// Cancellation won before execution started.
    Cancelled,
    /// Funding could not remain valid through execution start. The
    /// reservation has been released (units returned, zero charged) and the
    /// caller must not execute the work.
    ///
    /// `DenyReason::FundingExpiredAtStart` is the lease's usability window
    /// lapsing between reserve and commit: past that point the allocator may
    /// reclaim and re-grant the capacity, so committing would perform work
    /// that can never be billed. Under
    /// [`EnforcementMode::Elastic`](crate::snapshot::EnforcementMode::Elastic)
    /// the same lapse first attempts the overage fallback, and any of
    /// `OverageCapExhausted`, `OverageCapTemporarilyExhausted`, or
    /// `OverageCommitInProgress` may be reported instead. Each carries its own
    /// [`Retry`](crate::deny::Retry) classification and none may be collapsed
    /// into another: telling a caller to retry immediately against units that
    /// are already irrevocable is precisely the lie the classifier exists to
    /// prevent.
    Denied(DenyReason),
    /// Cancellation won the race. The caller must not execute the work; the
    /// request reports zero units.
    AlreadyReleased,
    /// The reservation was committed before — committing twice is a caller
    /// bug, surfaced rather than silently absorbed.
    AlreadyCommitted,
}

/// What funding the reservation may settle against at execution start.
///
/// Built from the request's *pinned* snapshot at the point of commit rather
/// than stored in the [`Reservation`]. The counter is shared by every
/// principal, lease, and locality of the account, so its refcount is one cache
/// line written by every core serving that account; cloning an `Arc` to it per
/// admission would put a contended atomic on the request path for a capability
/// most requests never use, and grow every staged type by its width. Passing
/// it at commit costs nothing, because the whole fallback lives inside the
/// already-cold "window lapsed" branch and folds away entirely at a
/// [`LeaseOnly`](CommitFunding::LeaseOnly) call site.
///
/// Reading the mode at commit is not a staleness risk: the caller holds the
/// same immutable snapshot for the request's whole life, so this is the
/// generation admission itself read.
#[derive(Debug, Clone, Copy)]
pub enum CommitFunding<'a> {
    /// [`EnforcementMode::Strict`](crate::snapshot::EnforcementMode::Strict):
    /// a lapsed lease releases for zero and the kernel must not run.
    LeaseOnly,
    /// [`EnforcementMode::Elastic`](crate::snapshot::EnforcementMode::Elastic):
    /// a lapsed lease may settle against overage instead, bounded by `cap`.
    OverageFallback {
        overage: &'a AccountOverage,
        cap: CostUnits,
    },
}

impl<'a> CommitFunding<'a> {
    /// The one place enforcement mode chooses a commit-time funding rule.
    ///
    /// Taking the counter alongside the mode keeps them from being sourced
    /// separately: `overage` must be the account's own counter — the one
    /// reached through the same lease slot that funded the reservation — and
    /// commit debug-asserts that it names the same account.
    #[must_use]
    #[inline]
    pub fn from_mode(mode: EnforcementMode, overage: &'a AccountOverage) -> Self {
        match mode.overage_cap() {
            None => Self::LeaseOnly,
            Some(cap) => Self::OverageFallback { overage, cap },
        }
    }
}

/// What a reservation debited, and therefore what a release must refund.
///
/// A reservation cannot hold a plain `Arc<LocalLease>` any more, because the
/// case elastic mode exists to serve includes *having no lease at all* — a
/// cold start, or an instance whose lease lapsed before refill replaced it.
/// The discriminant is what lets a release find its way back to the counter it
/// came from without either counter having to know about the other.
#[derive(Debug)]
enum ChargeSource {
    /// Units debited from a lease the allocator granted, with the evidence
    /// naming the shard counter they came from: a release must return them to
    /// that counter, not merely to the lease.
    Lease {
        lease: Arc<LocalLease>,
        debit: LeaseDebit,
    },
    /// Unfunded units extended under [`EnforcementMode::Elastic`].
    ///
    /// [`EnforcementMode::Elastic`]: crate::snapshot::EnforcementMode::Elastic
    Overage(Arc<AccountOverage>),
}

impl ChargeSource {
    /// The phase this receipt opens in.
    ///
    /// Deriving it from the receipt, rather than storing a second `pending`
    /// field, is what keeps construction, `cancel`, and `Drop` from ever
    /// disagreeing about the value to compare-exchange *from*. A stored copy
    /// that drifted would make `Drop` fail its CAS and silently skip the
    /// refund — stranding capacity with no diagnostic.
    const fn pending_phase(&self) -> u8 {
        match self {
            Self::Lease { .. } => PENDING_LEASE,
            Self::Overage(_) => PENDING_OVERAGE,
        }
    }

    /// The phase a commit against this receipt's own funding settles in.
    ///
    /// The commit-time fallback is the one transition that does *not* use
    /// this: it names `COMMITTED_OVERAGE` explicitly, inside the `Lease` arm
    /// that holds the receipt it refunds. Because that is the only other place
    /// the constant appears, no expression in this module pairs "compare from
    /// `PENDING_OVERAGE`" with "credit a lease", so `PENDING_OVERAGE ->
    /// COMMITTED_LEASE` is unrepresentable rather than merely untested.
    const fn committed_phase(&self) -> u8 {
        match self {
            Self::Lease { .. } => COMMITTED_LEASE,
            Self::Overage(_) => COMMITTED_OVERAGE,
        }
    }
}

/// One request's debited-but-not-yet-committed units.
///
/// Created by [`Reservation::reserve`] or
/// [`Reservation::reserve_overage`]; resolved by exactly one of
/// [`commit_at_execution_start`](Reservation::commit_at_execution_start),
/// [`cancel`](Reservation::cancel), or drop (which releases a pending
/// reservation — INVARIANTS.md #2).
///
/// The charging rules are identical whichever funded it: zero charge before
/// execution, full charge from execution start, and one compare-exchange
/// deciding the commit/cancel race. Elastic mode changes *whether* a request
/// is admitted, never how the units it consumes are accounted for.
#[derive(Debug)]
pub struct Reservation {
    source: ChargeSource,
    units: CostUnits,
    phase: AtomicU8,
}

impl Reservation {
    /// Debit `units` from `lease` and open a pending reservation.
    ///
    /// This is the quota step of the admission pipeline; it fails closed on
    /// lease expiry or exhaustion and performs no I/O.
    #[inline]
    pub fn reserve(
        lease: &Arc<LocalLease>,
        units: CostUnits,
        now: Timestamp,
    ) -> Result<Reservation, DenyReason> {
        Self::reserve_at_locality(lease, units, now, Locality::current())
    }

    /// Reserve using locality already resolved by the enclosing admission
    /// pipeline, avoiding repeated thread-local lookups between stages.
    #[doc(hidden)]
    #[inline]
    pub fn reserve_at_locality(
        lease: &Arc<LocalLease>,
        units: CostUnits,
        now: Timestamp,
        locality: Locality,
    ) -> Result<Reservation, DenyReason> {
        let debit = lease.try_reserve_at(units, now, locality)?;
        Ok(Reservation {
            source: ChargeSource::Lease {
                lease: Arc::clone(lease),
                debit,
            },
            units,
            phase: AtomicU8::new(PENDING_LEASE),
        })
    }

    /// Extend `units` of unfunded credit against `cap` and open a pending
    /// reservation, for an account whose lease could not fund the quote.
    ///
    /// Takes no `now`, and the absence is the design rather than an omission.
    /// A lease has a usability window because the allocator will reclaim and
    /// re-grant its units, so work committed outside that window could never
    /// be billed. Overage was never granted and is never reclaimed: there is
    /// no window to race. Staleness and status are still enforced — by the
    /// snapshot checks that run before this step, under every mode.
    #[inline]
    pub fn reserve_overage(
        overage: &Arc<AccountOverage>,
        units: CostUnits,
        cap: CostUnits,
    ) -> Result<Reservation, DenyReason> {
        overage.try_debit(units, cap)?;
        Ok(Reservation {
            source: ChargeSource::Overage(Arc::clone(overage)),
            units,
            phase: AtomicU8::new(PENDING_OVERAGE),
        })
    }

    #[must_use]
    pub fn units(&self) -> CostUnits {
        self.units
    }

    /// True when **admission** found no lease to fund these units.
    ///
    /// Reserve-time truth, and deliberately so: it decides the
    /// `admitted_overage` qualifier at the stage that admitted the request
    /// (INVARIANTS.md #20). It is *not* the billing statement — a leased
    /// admission whose window lapsed at execution start settles against
    /// overage without ever having been an overage admission. The billing
    /// statement is [`UsageEvent::source`], which reads the terminal phase.
    #[must_use]
    pub fn admitted_as_overage(&self) -> bool {
        matches!(self.source, ChargeSource::Overage(_))
    }

    fn account_id(&self) -> AccountId {
        match &self.source {
            ChargeSource::Lease { lease, .. } => lease.grant().account_id,
            ChargeSource::Overage(overage) => overage.account_id(),
        }
    }

    /// Whether the funding source's local usability window has lapsed.
    ///
    /// Only a lease has one. See [`reserve_overage`](Self::reserve_overage).
    fn window_lapsed(&self, now: Timestamp) -> bool {
        match &self.source {
            ChargeSource::Lease { lease, .. } => now >= lease.usable_until(),
            ChargeSource::Overage(_) => false,
        }
    }

    /// Return the units to whichever counter they came from.
    fn refund(&self) {
        match &self.source {
            ChargeSource::Lease { lease, debit } => lease.credit(debit),
            ChargeSource::Overage(overage) => overage.credit(self.units),
        }
    }

    /// Release for zero and report `reason`, or report whoever resolved the
    /// reservation first.
    #[cold]
    fn release_for_zero(&self, reason: DenyReason) -> Result<CostUnits, CommitError> {
        match self.phase.compare_exchange(
            self.source.pending_phase(),
            RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.refund();
                Err(CommitError::Denied(reason))
            }
            // Someone else already resolved it; report that outcome.
            Err(RELEASED) => Err(CommitError::AlreadyReleased),
            Err(_) => Err(CommitError::AlreadyCommitted),
        }
    }

    /// Commit the charge because execution is starting. From this point the
    /// full quote stands regardless of how execution ends.
    ///
    /// Rechecks the lease's local usability window: a reservation opened just
    /// before the window closed must not commit against that lease after it —
    /// the allocator's reclaim grace only protects work committed *inside* the
    /// window, and past it the capacity may be reclaimed and re-granted, so
    /// the charge could never be billed.
    ///
    /// What a lapse then means is `funding`'s decision.
    /// [`CommitFunding::LeaseOnly`] releases the units and reports
    /// `FundingExpiredAtStart`; the caller must not execute.
    /// [`CommitFunding::OverageFallback`] instead settles the same reservation
    /// against overage — **one** transition `PENDING_LEASE ->
    /// COMMITTED_OVERAGE`, never a release followed by a second reservation,
    /// which would give a canceller one phase to win while the worker
    /// committed another. If the overage debit itself is refused, the
    /// reservation releases for zero and reports that refusal verbatim.
    #[inline]
    pub fn commit_at_execution_start(
        &self,
        now: Timestamp,
        funding: CommitFunding<'_>,
    ) -> Result<CostUnits, CommitError> {
        if self.window_lapsed(now) {
            return self.commit_after_lapse(funding);
        }
        let claim = || {
            self.phase.compare_exchange(
                self.source.pending_phase(),
                self.source.committed_phase(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
        };
        let transition = match &self.source {
            // The pending overage units are already in `spent` and owned by
            // this reservation, so publication must not credit them; cancel
            // and drop remain their refund.
            ChargeSource::Overage(overage) => overage.publish_claim(self.units, claim),
            ChargeSource::Lease { .. } => claim(),
        };
        match transition {
            Ok(_) => Ok(self.units),
            Err(RELEASED) => Err(CommitError::AlreadyReleased),
            Err(_) => Err(CommitError::AlreadyCommitted),
        }
    }

    /// The lapsed-window branch: cold, and the only place a reservation's
    /// funding source may change.
    #[cold]
    fn commit_after_lapse(&self, funding: CommitFunding<'_>) -> Result<CostUnits, CommitError> {
        let (CommitFunding::OverageFallback { overage, cap }, ChargeSource::Lease { lease, debit }) =
            (funding, &self.source)
        else {
            return self.release_for_zero(DenyReason::FundingExpiredAtStart);
        };
        debug_assert_eq!(
            overage.account_id(),
            self.account_id(),
            "commit-time overage fallback must use the reservation's own account counter"
        );
        // A cheap probe that narrows the window in which a doomed reservation
        // takes a debit it will immediately return. It does not close the
        // race — a canceller can still win after this load — which is why the
        // guard below, not this branch, is what guarantees the credit.
        if self.phase.load(Ordering::Acquire) != PENDING_LEASE {
            return self.release_for_zero(DenyReason::FundingExpiredAtStart);
        }
        // A refused debit claims nothing, so the reservation is simply
        // released and the refusal reported with its own retry class.
        let tentative = match overage.debit_tentatively(self.units, cap) {
            Ok(tentative) => tentative,
            Err(refused) => return self.release_for_zero(refused),
        };
        // The debit is funded *before* the claim, and consuming the guard is
        // what enforces that order: a won claim can never name overage the
        // account never recorded, which would break the ledger equation by
        // exactly these units.
        match tentative.publish_commit(|| {
            self.phase.compare_exchange(
                PENDING_LEASE,
                COMMITTED_OVERAGE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
        }) {
            // Won. The lease receipt is refunded only here, strictly after the
            // claim: crediting it before would double-refund granted capacity
            // alongside a canceller who won the race and refunded it too.
            Ok(_) => {
                lease.credit(debit);
                Ok(self.units)
            }
            // Lost. The guard already credited the tentative overage on its
            // way out and the winning canceller refunded the lease, so nothing
            // is owed here.
            Err(RELEASED) => Err(CommitError::AlreadyReleased),
            Err(_) => Err(CommitError::AlreadyCommitted),
        }
    }

    /// Cancel before execution if possible. Idempotent: cancelling an already
    /// released reservation reports [`CancelOutcome::ZeroCharged`] without
    /// crediting the lease a second time (the compare-exchange transitions at
    /// most once).
    #[inline]
    pub fn cancel(&self) -> CancelOutcome {
        match self.phase.compare_exchange(
            self.source.pending_phase(),
            RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.refund();
                CancelOutcome::ZeroCharged
            }
            // Either committed phase means execution started. A leased
            // reservation that settled against overage still charges its full
            // quote — the fallback changed which counter funds it, not whether
            // the work is billed.
            Err(COMMITTED_LEASE | COMMITTED_OVERAGE) => {
                CancelOutcome::AlreadyCommitted { units: self.units }
            }
            Err(_) => CancelOutcome::ZeroCharged,
        }
    }

    /// The billing record, available only once committed. `request_id` is the
    /// idempotency key (INVARIANTS.md #7); emitting the same event twice is
    /// therefore harmless downstream.
    /// The billing statement reads the *phase*, not the receipt, and that is
    /// load-bearing rather than stylistic. A commit-time fallback happens
    /// precisely because the lease's window lapsed, so by the time the event
    /// flushes the allocator may already have reclaimed that lease — and the
    /// sink rejects a `Leased` event naming a reclaimed lease, because the
    /// reclaim credited its full remainder and the units would double-count.
    /// Billing the fallback against its receipt would therefore silently drop
    /// the charge for work that ran, on the exact path elastic mode exists to
    /// serve. The terminal phase is the funding statement.
    #[must_use]
    pub fn usage_event(&self, request_id: RequestId, now: Timestamp) -> Option<UsageEvent> {
        let source = match self.phase.load(Ordering::Acquire) {
            // Both a natively admitted overage and a commit-time fallback.
            COMMITTED_OVERAGE => UsageSource::Overage,
            COMMITTED_LEASE => match &self.source {
                ChargeSource::Lease { lease, .. } => {
                    let grant = lease.grant();
                    UsageSource::Leased {
                        lease_id: grant.lease_id,
                        fencing_token: grant.fencing_token,
                    }
                }
                // Unreachable: `COMMITTED_LEASE` is only ever written by
                // `committed_phase()` on a `Lease` receipt. Billing against no
                // capability at least preserves the charge; dropping the event
                // would lose it outright.
                ChargeSource::Overage(_) => {
                    debug_assert!(false, "a committed-lease phase requires a lease receipt");
                    UsageSource::Overage
                }
            },
            _ => return None,
        };
        Some(UsageEvent {
            request_id,
            account_id: self.account_id(),
            source,
            units: self.units,
            occurred_at: now,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // An abandoned pending reservation charges zero: same transition as
        // cancel(), so a reservation resolved earlier is untouched.
        if self
            .phase
            .compare_exchange(
                self.source.pending_phase(),
                RELEASED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.refund();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AccountId, FencingToken, LeaseId};
    use crate::lease::LeaseGrant;
    use crate::usage::UsageSource;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn overage() -> Arc<AccountOverage> {
        Arc::new(AccountOverage::new(AccountId(1)))
    }

    /// An overage charge bills like any other, and says so on the wire: the
    /// event names the account and carries no lease capability, because there
    /// is no lease to name.
    #[test]
    fn a_committed_overage_bills_against_no_lease() {
        let o = overage();
        let r = Reservation::reserve_overage(&o, CostUnits(30), CostUnits(100)).unwrap();
        assert!(r.admitted_as_overage());
        assert_eq!(o.spent(), CostUnits(30));
        assert_eq!(
            r.commit_at_execution_start(t(1), CommitFunding::LeaseOnly)
                .unwrap(),
            CostUnits(30)
        );
        let event = r.usage_event(RequestId(7), t(1)).unwrap();
        assert_eq!(event.account_id, AccountId(1));
        assert_eq!(event.units, CostUnits(30));
        assert_eq!(event.source, UsageSource::Overage);
        assert_eq!(event.source.lease_id(), None);
        assert_eq!(event.source.fencing_token(), None);
    }

    /// Zero charge before execution applies identically to credit: the units
    /// go back to the counter they came from, not to some lease.
    #[test]
    fn cancelling_an_overage_returns_the_credit() {
        let o = overage();
        let r = Reservation::reserve_overage(&o, CostUnits(30), CostUnits(100)).unwrap();
        assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(o.spent(), CostUnits::ZERO);
        assert_eq!(r.usage_event(RequestId(7), t(1)), None);
    }

    #[test]
    fn dropping_a_pending_overage_returns_the_credit() {
        let o = overage();
        drop(Reservation::reserve_overage(&o, CostUnits(30), CostUnits(100)).unwrap());
        assert_eq!(o.spent(), CostUnits::ZERO);
    }

    /// A lease reservation cannot commit past its usability window because the
    /// allocator may reclaim and re-grant those units. Overage was never
    /// granted and is never reclaimed, so there is no window to race — and a
    /// commit arbitrarily far past the timestamp it was reserved at still
    /// stands.
    #[test]
    fn overage_has_no_usability_window_to_lapse() {
        let leased = Reservation::reserve(&lease(100), CostUnits(30), t(0)).unwrap();
        assert_eq!(
            leased.commit_at_execution_start(t(1_000), CommitFunding::LeaseOnly),
            Err(CommitError::Denied(DenyReason::FundingExpiredAtStart))
        );

        let o = overage();
        let r = Reservation::reserve_overage(&o, CostUnits(30), CostUnits(100)).unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(1_000_000), CommitFunding::LeaseOnly)
                .unwrap(),
            CostUnits(30)
        );
        assert_eq!(o.spent(), CostUnits(30), "a commit keeps the credit spent");
    }

    #[test]
    fn an_overage_beyond_the_cap_is_refused_and_claims_nothing() {
        let o = overage();
        assert_eq!(
            Reservation::reserve_overage(&o, CostUnits(101), CostUnits(100)).unwrap_err(),
            DenyReason::OverageCapExhausted {
                spent: CostUnits::ZERO,
                overage_cap: CostUnits(100),
            }
        );
        assert_eq!(o.spent(), CostUnits::ZERO);
    }

    fn lease(units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(3),
                units: CostUnits(units),
                expires_at: t(1_000),
            },
            CostUnits::ZERO,
        ))
    }

    #[test]
    fn commit_charges_and_keeps_units_spent() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly),
            Ok(CostUnits(30))
        );
        assert_eq!(l.remaining(), CostUnits(70));
        assert!(r.usage_event(RequestId(9), t(1)).is_some());
        drop(r);
        // Drop of a committed reservation must not refund.
        assert_eq!(l.remaining(), CostUnits(70));
    }

    /// `units()` is how a caller learns what a pending reservation will charge
    /// before deciding to commit it, and no test called it — it could report
    /// zero for any reservation with the suite green (#43). A caller checking
    /// the quote before execution would have been told everything is free.
    ///
    /// Asserted against what the reservation actually does with those units,
    /// not just against the constructor argument: the debit taken at reserve,
    /// the charge returned by commit, and the units billed on the usage event
    /// must all be the number `units()` advertises.
    #[test]
    fn a_reservation_reports_the_units_it_will_charge() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        assert_eq!(r.units(), CostUnits(30));
        assert_eq!(
            l.remaining(),
            CostUnits(70),
            "the pending debit is the advertised amount"
        );

        assert_eq!(
            r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly),
            Ok(r.units())
        );
        assert_eq!(
            r.usage_event(RequestId(1), t(1)).unwrap().units,
            r.units(),
            "and the billing event carries it too"
        );
    }

    #[test]
    fn cancel_charges_zero_and_refunds() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(r.usage_event(RequestId(9), t(1)), None);
        // Idempotent, and no double credit.
        assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(100));
    }

    #[test]
    fn drop_releases_pending() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits(70));
        drop(r);
        assert_eq!(l.remaining(), CostUnits(100));
    }

    #[test]
    fn cancel_after_commit_reports_full_charge() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            .unwrap();
        assert_eq!(
            r.cancel(),
            CancelOutcome::AlreadyCommitted {
                units: CostUnits(30)
            }
        );
        assert_eq!(l.remaining(), CostUnits(70));
    }

    #[test]
    fn commit_after_cancel_is_refused() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(
            r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly),
            Err(CommitError::AlreadyReleased)
        );
        assert_eq!(l.remaining(), CostUnits(100));
    }

    #[test]
    fn double_commit_is_a_surfaced_error() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            .unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly),
            Err(CommitError::AlreadyCommitted)
        );
    }

    /// Review finding #1 regression: a reservation opened inside the
    /// usability window cannot commit after the window closes — it releases
    /// for zero charge instead, so reclaimed-and-re-granted capacity can
    /// never be double-worked.
    #[test]
    fn commit_after_window_closes_releases_for_zero() {
        let l = lease(100); // usable until t(1_000) (no margin)
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        assert_eq!(l.remaining(), CostUnits(70));
        // The window lapses between reserve and commit.
        assert_eq!(
            r.commit_at_execution_start(t(1_000), CommitFunding::LeaseOnly),
            Err(CommitError::Denied(DenyReason::FundingExpiredAtStart))
        );
        // Units returned; no usage event can exist; later commit is refused.
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(r.usage_event(RequestId(1), t(1_001)), None);
        assert_eq!(
            r.commit_at_execution_start(t(999), CommitFunding::LeaseOnly),
            Err(CommitError::AlreadyReleased)
        );
    }

    /// The commit-time elastic fallback, end to end: a lease that lapsed
    /// between admission and execution start settles against overage instead
    /// of refusing, and the resulting bill names *no lease capability*.
    ///
    /// The missing capability is the point, not a detail. The sink rejects a
    /// leased event whose lease has been reclaimed — and this lease lapsed, so
    /// reclaim is exactly what happens next. Billing against the receipt would
    /// silently drop the charge for work that ran.
    #[test]
    fn an_elastic_lapse_at_execution_start_bills_as_overage_with_no_lease_capability() {
        let l = lease(100);
        let o = overage();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        assert!(
            !r.admitted_as_overage(),
            "admission was funded by the lease"
        );
        assert_eq!(l.remaining(), CostUnits(70));

        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        assert_eq!(
            r.commit_at_execution_start(t(1_000), funding),
            Ok(CostUnits(30))
        );

        // One funding term, not two: the lease receipt came back whole and the
        // overage counter holds the charge as irrevocable committed occupancy.
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(o.spent(), CostUnits(30));

        let event = r.usage_event(RequestId(7), t(1_000)).unwrap();
        assert_eq!(event.units, CostUnits(30));
        assert_eq!(event.account_id, AccountId(1));
        assert_eq!(event.source, UsageSource::Overage);
        assert_eq!(event.source.lease_id(), None);
        assert_eq!(event.source.fencing_token(), None);

        // Reserve-time truth is unchanged by how the request settled.
        assert!(!r.admitted_as_overage());
    }

    /// The same lapse under `Strict` charges zero and forbids execution. This
    /// is the pair to the test above: one enforcement mode, one outcome.
    #[test]
    fn a_strict_lapse_at_execution_start_releases_for_zero_and_yields_no_event() {
        let l = lease(100);
        let o = overage();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();

        assert_eq!(
            r.commit_at_execution_start(t(1_000), CommitFunding::LeaseOnly),
            Err(CommitError::Denied(DenyReason::FundingExpiredAtStart))
        );
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(r.usage_event(RequestId(7), t(1_000)), None);
        // Strict took no overage: the counter was never touched.
        assert_eq!(o.spent(), CostUnits::ZERO);
    }

    /// A fallback that cannot fit inside the cap releases the lease for zero
    /// and reports the refusal *verbatim*, keeping its own retry class.
    #[test]
    fn an_elastic_lapse_with_no_committed_headroom_releases_the_lease_for_zero() {
        let l = lease(100);
        let o = overage();
        // Spend the whole cap irrevocably, so no refund could make room.
        let held = Reservation::reserve_overage(&o, CostUnits(100), CostUnits(100)).unwrap();
        held.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            .unwrap();

        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        assert_eq!(
            r.commit_at_execution_start(t(1_000), funding),
            Err(CommitError::Denied(DenyReason::OverageCapExhausted {
                spent: CostUnits(100),
                overage_cap: CostUnits(100),
            }))
        );
        // Released for zero, and the refused debit claimed nothing.
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(o.spent(), CostUnits(100));
        assert_eq!(r.usage_event(RequestId(7), t(1_000)), None);
    }

    /// When the cap is occupied by a *refundable* sibling, the refusal is the
    /// transient one: a later cancel really can make this request fit.
    #[test]
    fn an_elastic_lapse_blocked_by_refundable_occupancy_is_temporarily_exhausted() {
        let l = lease(100);
        let o = overage();
        // Pending, therefore still refundable.
        let _sibling = Reservation::reserve_overage(&o, CostUnits(100), CostUnits(100)).unwrap();

        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        let denied = r.commit_at_execution_start(t(1_000), funding).unwrap_err();
        assert_eq!(
            denied,
            CommitError::Denied(DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(100),
                overage_cap: CostUnits(100),
            })
        );
        let CommitError::Denied(reason) = denied else {
            unreachable!("the fallback reports a classified denial")
        };
        assert_eq!(reason.retry(), crate::deny::Retry::Transient);
        assert_eq!(l.remaining(), CostUnits(100));
    }

    /// The third refusal `try_debit` can produce, which the fallback must
    /// report rather than collapse into one of the other two: those are
    /// `Transient`, this is `AfterInFlight`, and telling a caller to retry
    /// immediately against units that are already irrevocable is exactly the
    /// lie the classifier exists to prevent.
    #[test]
    fn an_elastic_lapse_overlapping_a_sibling_publication_reports_an_in_flight_commit() {
        let l = lease(100);
        let o = overage();
        // The sibling holds the whole cap, so the fallback's own debit cannot
        // fit; overlapping its publication is what makes the refusal
        // `AfterInFlight` rather than one of the stable-occupancy reasons.
        let sibling = Reservation::reserve_overage(&o, CostUnits(100), CostUnits(100)).unwrap();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();

        // Drive a sibling publication and attempt the fallback from inside it,
        // exactly as `overage_commit_publication_never_looks_refundable` does.
        o.publish_claim(sibling.units, || {
            let claimed = sibling.phase.compare_exchange(
                PENDING_OVERAGE,
                COMMITTED_OVERAGE,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            assert!(claimed.is_ok(), "the sibling wins its own phase claim");

            let funding = CommitFunding::OverageFallback {
                overage: &o,
                cap: CostUnits(100),
            };
            let denied = r.commit_at_execution_start(t(1_000), funding).unwrap_err();
            let CommitError::Denied(reason) = denied else {
                unreachable!("the fallback reports a classified denial")
            };
            assert!(
                matches!(reason, DenyReason::OverageCommitInProgress { .. }),
                "expected an in-flight publication refusal, got {reason:?}"
            );
            assert_eq!(reason.retry(), crate::deny::Retry::AfterInFlight);
            Ok::<_, ()>(())
        })
        .unwrap();

        // The refused fallback released its lease for zero and claimed nothing.
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(o.spent(), CostUnits(100));
    }

    /// The lease receipt is refunded exactly once by a winning fallback. A
    /// late cancel and the eventual drop must both find nothing left to do.
    #[test]
    fn a_fallback_commit_refunds_its_lease_exactly_once() {
        let l = lease(100);
        let o = overage();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        r.commit_at_execution_start(t(1_000), funding).unwrap();
        assert_eq!(l.remaining(), CostUnits(100));

        assert_eq!(
            r.cancel(),
            CancelOutcome::AlreadyCommitted {
                units: CostUnits(30)
            }
        );
        drop(r);
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(o.spent(), CostUnits(30));
    }

    /// A fallback that loses the race to a canceller must strand no overage
    /// capacity: the tentative debit is revocable until the claim resolves,
    /// and the full cap must be reusable afterwards.
    #[test]
    fn a_fallback_that_loses_to_cancel_strands_no_overage_capacity() {
        let l = lease(100);
        let o = overage();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();

        // The canceller wins first, deterministically.
        assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(l.remaining(), CostUnits(100));

        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        assert_eq!(
            r.commit_at_execution_start(t(1_000), funding),
            Err(CommitError::AlreadyReleased)
        );
        // Nothing stranded: the whole cap is still available.
        assert_eq!(o.spent(), CostUnits::ZERO);
        let fresh = Reservation::reserve_overage(&o, CostUnits(100), CostUnits(100));
        assert!(fresh.is_ok(), "the full cap must remain reusable");
        // And the lease was credited once, by the canceller alone.
        assert_eq!(l.remaining(), CostUnits(100));
    }

    /// Overage has no usability window, so a natively admitted overage
    /// reservation never reaches the fallback and never takes a second debit.
    #[test]
    fn a_native_overage_reservation_ignores_a_commit_time_fallback() {
        let o = overage();
        let r = Reservation::reserve_overage(&o, CostUnits(30), CostUnits(100)).unwrap();
        assert_eq!(o.spent(), CostUnits(30));

        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        assert_eq!(
            r.commit_at_execution_start(t(9_999), funding),
            Ok(CostUnits(30))
        );
        // One debit, taken at admission — not a second one at commit.
        assert_eq!(o.spent(), CostUnits(30));
        assert_eq!(
            r.usage_event(RequestId(1), t(9_999)).unwrap().source,
            UsageSource::Overage
        );
    }

    /// Committing twice remains a surfaced programming error after a fallback,
    /// not a second charge.
    #[test]
    fn a_second_commit_after_a_fallback_is_a_surfaced_error() {
        let l = lease(100);
        let o = overage();
        let r = Reservation::reserve(&l, CostUnits(30), t(999)).unwrap();
        let funding = CommitFunding::OverageFallback {
            overage: &o,
            cap: CostUnits(100),
        };
        r.commit_at_execution_start(t(1_000), funding).unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(1_000), funding),
            Err(CommitError::AlreadyCommitted)
        );
        assert_eq!(o.spent(), CostUnits(30));
        assert_eq!(l.remaining(), CostUnits(100));
    }

    /// The safety margin closes the window early: commits stop at
    /// `expires_at - margin`, not at `expires_at`.
    #[test]
    fn safety_margin_closes_window_before_expiry() {
        let l = Arc::new(LocalLease::with_safety_margin(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(3),
                units: CostUnits(100),
                expires_at: t(1_000),
            },
            CostUnits::ZERO,
            jiff::SignedDuration::from_secs(10),
        ));
        assert_eq!(l.usable_until(), t(990));
        // Debits stop at the margin boundary too.
        assert_eq!(
            Reservation::reserve(&l, CostUnits(1), t(990)).unwrap_err(),
            DenyReason::LeaseExpired
        );
        let r = Reservation::reserve(&l, CostUnits(1), t(989)).unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(990), CommitFunding::LeaseOnly),
            Err(CommitError::Denied(DenyReason::FundingExpiredAtStart))
        );
    }

    /// INVARIANTS.md #3: commit and cancel race — exactly one wins, and the
    /// lease balance reflects the winner.
    #[test]
    fn commit_cancel_race_one_winner() {
        for _ in 0..500 {
            let l = lease(100);
            let r = Arc::new(Reservation::reserve(&l, CostUnits(10), t(0)).unwrap());
            let rc = Arc::clone(&r);
            let committer = std::thread::spawn(move || {
                rc.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            });
            let canceller = std::thread::spawn({
                let rc = Arc::clone(&r);
                move || rc.cancel()
            });
            let commit = committer.join().unwrap();
            let cancel = canceller.join().unwrap();
            match (commit, cancel) {
                (Ok(units), CancelOutcome::AlreadyCommitted { units: seen }) => {
                    assert_eq!(units, seen);
                    assert_eq!(l.remaining(), CostUnits(90));
                }
                (Err(CommitError::AlreadyReleased), CancelOutcome::ZeroCharged) => {
                    assert_eq!(l.remaining(), CostUnits(100));
                }
                other => panic!("impossible race outcome: {other:?}"),
            }
        }
    }

    /// The fallback races a canceller for the same single phase, so exactly
    /// one funding term survives. This is the property the "one transition,
    /// not a second reservation" rule exists for: releasing and re-reserving
    /// would give the canceller one phase to win while the worker committed
    /// another, and both could report success.
    #[test]
    fn fallback_commit_and_cancel_leave_exactly_one_funding_term() {
        for _ in 0..500 {
            let l = lease(100);
            let o = overage();
            let r = Arc::new(Reservation::reserve(&l, CostUnits(10), t(999)).unwrap());
            let committer = std::thread::spawn({
                let rc = Arc::clone(&r);
                let oc = Arc::clone(&o);
                move || {
                    rc.commit_at_execution_start(
                        t(1_000),
                        CommitFunding::OverageFallback {
                            overage: &oc,
                            cap: CostUnits(100),
                        },
                    )
                }
            });
            let canceller = std::thread::spawn({
                let rc = Arc::clone(&r);
                move || rc.cancel()
            });
            let commit = committer.join().unwrap();
            let cancel = canceller.join().unwrap();
            match (commit, cancel) {
                // The worker won: the charge is funded by overage alone, and
                // the lease receipt came back whole.
                (Ok(units), CancelOutcome::AlreadyCommitted { units: seen }) => {
                    assert_eq!(units, seen);
                    assert_eq!(l.remaining(), CostUnits(100));
                    assert_eq!(o.spent(), CostUnits(10));
                    assert_eq!(
                        r.usage_event(RequestId(1), t(1_000)).unwrap().source,
                        UsageSource::Overage
                    );
                }
                // The canceller won: zero charge, and — the part the guard
                // buys — the tentative debit left nothing behind.
                (Err(CommitError::AlreadyReleased), CancelOutcome::ZeroCharged) => {
                    assert_eq!(l.remaining(), CostUnits(100));
                    assert_eq!(o.spent(), CostUnits::ZERO);
                    assert_eq!(r.usage_event(RequestId(1), t(1_000)), None);
                }
                other => panic!("impossible race outcome: {other:?}"),
            }
        }
    }

    /// Concurrent fallbacks are still bounded by the cap, because the debit
    /// that funds each one goes through the same compare-exchange every other
    /// overage claim uses.
    #[test]
    fn concurrent_fallbacks_never_exceed_the_overage_cap() {
        let o = overage();
        // Ten workers, ten units each, but only room for six.
        let cap = CostUnits(60);
        let reservations: Vec<_> = (0..10)
            .map(|_| {
                let l = lease(100);
                let r = Reservation::reserve(&l, CostUnits(10), t(999)).unwrap();
                (l, r)
            })
            .collect();

        let committed = std::thread::scope(|scope| {
            let handles: Vec<_> = reservations
                .iter()
                .map(|(_, r)| {
                    let oc = Arc::clone(&o);
                    scope.spawn(move || {
                        r.commit_at_execution_start(
                            t(1_000),
                            CommitFunding::OverageFallback { overage: &oc, cap },
                        )
                        .is_ok()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("no worker panics"))
                .filter(|won| *won)
                .count()
        });

        assert!(o.spent() <= cap, "overage spend exceeded its cap");
        assert_eq!(o.spent(), CostUnits(committed as u64 * 10));
        assert!(committed <= 6, "at most six ten-unit charges fit in sixty");
        // Every loser released its lease for zero; every winner returned it.
        for (l, _) in &reservations {
            assert_eq!(l.remaining(), CostUnits(100));
        }
    }

    /// The overage counter has a second transition to publish after the
    /// reservation CAS: committed occupancy. Once both racers return, the
    /// winning phase and the local occupancy reason must agree exactly —
    /// cancellation restores headroom, while commit leaves stable saturation.
    #[test]
    fn overage_commit_cancel_race_preserves_retry_classification() {
        for _ in 0..500 {
            let o = overage();
            let r =
                Arc::new(Reservation::reserve_overage(&o, CostUnits(10), CostUnits(10)).unwrap());
            let committer = std::thread::spawn({
                let r = Arc::clone(&r);
                move || r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            });
            let canceller = std::thread::spawn({
                let r = Arc::clone(&r);
                move || r.cancel()
            });

            match (committer.join().unwrap(), canceller.join().unwrap()) {
                (Ok(units), CancelOutcome::AlreadyCommitted { units: seen }) => {
                    assert_eq!(units, seen);
                    assert_eq!(o.spent(), CostUnits(10));
                    let denied =
                        Reservation::reserve_overage(&o, CostUnits(1), CostUnits(10)).unwrap_err();
                    assert!(matches!(denied, DenyReason::OverageCapExhausted { .. }));
                    assert_eq!(denied.retry(), crate::deny::Retry::Transient);
                }
                (Err(CommitError::AlreadyReleased), CancelOutcome::ZeroCharged) => {
                    assert_eq!(o.spent(), CostUnits::ZERO);
                    drop(
                        Reservation::reserve_overage(&o, CostUnits(10), CostUnits(10))
                            .expect("cancelled credit is immediately reusable"),
                    );
                }
                other => panic!("impossible overage race outcome: {other:?}"),
            }
        }
    }

    /// A commit has become irrevocable once it wins the phase transition. A
    /// cap observer must never describe those units as refundable while the
    /// committed-occupancy publication is still catching up.
    #[test]
    fn overage_commit_publication_never_looks_refundable() {
        let overage = overage();
        let reservation =
            Reservation::reserve_overage(&overage, CostUnits(100), CostUnits(100)).unwrap();

        overage
            .publish_claim(reservation.units, || {
                let claimed = reservation.phase.compare_exchange(
                    PENDING_OVERAGE,
                    COMMITTED_OVERAGE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                assert!(claimed.is_ok(), "this commit wins the phase claim");
                assert_eq!(
                    {
                        let denied =
                            Reservation::reserve_overage(&overage, CostUnits(1), CostUnits(100))
                                .unwrap_err();
                        assert_eq!(denied.retry(), crate::deny::Retry::AfterInFlight);
                        denied
                    },
                    DenyReason::OverageCommitInProgress {
                        spent: CostUnits(100),
                        overage_cap: CostUnits(100),
                    },
                    "an irrevocable commit is identified as an in-flight publication"
                );
                claimed
            })
            .unwrap();

        assert!(matches!(
            Reservation::reserve_overage(&overage, CostUnits(1), CostUnits(100)).unwrap_err(),
            DenyReason::OverageCapExhausted { .. }
        ));
    }
}
