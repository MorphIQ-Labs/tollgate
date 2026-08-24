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

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use jiff::Timestamp;

use crate::deny::DenyReason;
use crate::ids::RequestId;
use crate::lease::LocalLease;
use crate::units::CostUnits;
use crate::usage::UsageEvent;

const PENDING: u8 = 0;
const COMMITTED: u8 = 1;
const RELEASED: u8 = 2;

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
    /// Cancellation won the race. The caller must not execute the work; the
    /// request reports zero units.
    AlreadyReleased,
    /// The reservation was committed before — committing twice is a caller
    /// bug, surfaced rather than silently absorbed.
    AlreadyCommitted,
    /// The lease's local usability window lapsed between reserve and commit.
    /// The reservation has been released (units returned, zero charged) and
    /// the caller must not execute the work: past this point the allocator
    /// may reclaim and re-grant the capacity, so committing would perform
    /// work that can never be billed (review finding #1).
    LeaseExpired,
}

/// One request's debited-but-not-yet-committed units.
///
/// Created by [`Reservation::reserve`]; resolved by exactly one of
/// [`commit_at_execution_start`](Reservation::commit_at_execution_start),
/// [`cancel`](Reservation::cancel), or drop (which releases a pending
/// reservation — INVARIANTS.md #2).
#[derive(Debug)]
pub struct Reservation {
    lease: Arc<LocalLease>,
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
        lease.try_debit(units, now)?;
        Ok(Reservation {
            lease: Arc::clone(lease),
            units,
            phase: AtomicU8::new(PENDING),
        })
    }

    #[must_use]
    pub fn units(&self) -> CostUnits {
        self.units
    }

    #[must_use]
    pub fn lease(&self) -> &Arc<LocalLease> {
        &self.lease
    }

    /// Commit the charge because execution is starting. From this point the
    /// full quote stands regardless of how execution ends.
    ///
    /// Rechecks the lease's local usability window: a reservation opened
    /// just before the window closed must not commit after it — the
    /// allocator's reclaim grace only protects work committed *inside* the
    /// window. An expired commit releases the units and reports
    /// [`CommitError::LeaseExpired`]; the caller must not execute.
    #[inline]
    pub fn commit_at_execution_start(&self, now: Timestamp) -> Result<CostUnits, CommitError> {
        if now >= self.lease.usable_until() {
            return match self.phase.compare_exchange(
                PENDING,
                RELEASED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.lease.credit(self.units);
                    Err(CommitError::LeaseExpired)
                }
                // Someone else already resolved it; report that outcome.
                Err(RELEASED) => Err(CommitError::AlreadyReleased),
                Err(_) => Err(CommitError::AlreadyCommitted),
            };
        }
        match self
            .phase
            .compare_exchange(PENDING, COMMITTED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(self.units),
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
        match self
            .phase
            .compare_exchange(PENDING, RELEASED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.lease.credit(self.units);
                CancelOutcome::ZeroCharged
            }
            Err(COMMITTED) => CancelOutcome::AlreadyCommitted { units: self.units },
            Err(_) => CancelOutcome::ZeroCharged,
        }
    }

    /// The billing record, available only once committed. `request_id` is the
    /// idempotency key (INVARIANTS.md #7); emitting the same event twice is
    /// therefore harmless downstream.
    #[must_use]
    pub fn usage_event(&self, request_id: RequestId, now: Timestamp) -> Option<UsageEvent> {
        if self.phase.load(Ordering::Acquire) != COMMITTED {
            return None;
        }
        let grant = self.lease.grant();
        Some(UsageEvent {
            request_id,
            account_id: grant.account_id,
            lease_id: grant.lease_id,
            fencing_token: grant.fencing_token,
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
            .compare_exchange(PENDING, RELEASED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.lease.credit(self.units);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AccountId, FencingToken, LeaseId};
    use crate::lease::LeaseGrant;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
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
        assert_eq!(r.commit_at_execution_start(t(0)), Ok(CostUnits(30)));
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

        assert_eq!(r.commit_at_execution_start(t(0)), Ok(r.units()));
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
        r.commit_at_execution_start(t(0)).unwrap();
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
            r.commit_at_execution_start(t(0)),
            Err(CommitError::AlreadyReleased)
        );
        assert_eq!(l.remaining(), CostUnits(100));
    }

    #[test]
    fn double_commit_is_a_surfaced_error() {
        let l = lease(100);
        let r = Reservation::reserve(&l, CostUnits(30), t(0)).unwrap();
        r.commit_at_execution_start(t(0)).unwrap();
        assert_eq!(
            r.commit_at_execution_start(t(0)),
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
            r.commit_at_execution_start(t(1_000)),
            Err(CommitError::LeaseExpired)
        );
        // Units returned; no usage event can exist; later commit is refused.
        assert_eq!(l.remaining(), CostUnits(100));
        assert_eq!(r.usage_event(RequestId(1), t(1_001)), None);
        assert_eq!(
            r.commit_at_execution_start(t(999)),
            Err(CommitError::AlreadyReleased)
        );
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
            r.commit_at_execution_start(t(990)),
            Err(CommitError::LeaseExpired)
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
            let committer = std::thread::spawn(move || rc.commit_at_execution_start(t(0)));
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
}
