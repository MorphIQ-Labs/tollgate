//! Local quota leases: centrally allocated capacity, locally decremented.
//!
//! A [`LeaseGrant`] is what an allocator (the store or the quota server)
//! returns after atomically debiting an account's balance. A [`LocalLease`]
//! is the instance-side runtime form: one atomic counter. Requests reserve
//! units from it with a CAS loop — no lock, no I/O — which is how one
//! database transaction amortizes across thousands of requests while fencing
//! (INVARIANTS.md #4) keeps two instances from spending the same units.

use std::sync::atomic::{AtomicU64, Ordering};

use jiff::Timestamp;

use crate::deny::DenyReason;
use crate::ids::{AccountId, FencingToken, LeaseId};
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
    pub fencing_token: FencingToken,
    pub units: CostUnits,
    pub expires_at: Timestamp,
}

/// Instance-side lease state: the grant plus a live remaining-units counter.
///
/// Shared as `Arc<LocalLease>` between the request path (reserve/return) and
/// the background refill task (`needs_refill`). Never mutated otherwise; a
/// refill installs a *new* `LocalLease` rather than growing this one, so the
/// request path never observes a counter that jumps upward mid-reservation.
#[derive(Debug)]
pub struct LocalLease {
    grant: LeaseGrant,
    remaining: AtomicU64,
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
        let usable_until = if margin < jiff::SignedDuration::ZERO {
            // A negative margin would extend local use past allocator expiry
            // and invert the fencing protocol. Fail closed even if a caller
            // bypasses the validated LeaseManager configuration.
            Timestamp::MIN
        } else {
            grant
                .expires_at
                .checked_sub(margin)
                // A margin longer than the lease's life fails closed: never
                // usable, settled by refill/reclaim.
                .unwrap_or(Timestamp::MIN)
        };
        LocalLease {
            remaining: AtomicU64::new(grant.units.get()),
            low_water: low_water.get(),
            usable_until,
            grant,
        }
    }

    /// The instant this lease stops accepting debits and commits locally.
    #[must_use]
    pub fn usable_until(&self) -> Timestamp {
        self.usable_until
    }

    #[must_use]
    pub fn grant(&self) -> &LeaseGrant {
        &self.grant
    }

    #[must_use]
    pub fn remaining(&self) -> CostUnits {
        CostUnits(self.remaining.load(Ordering::Acquire))
    }

    /// True once spending has crossed the low-water mark. Monotonic in
    /// practice only between refills; the refill task polls or checks after
    /// each reservation.
    #[must_use]
    pub fn needs_refill(&self) -> bool {
        self.remaining.load(Ordering::Acquire) <= self.low_water
    }

    /// Debit `units` if the lease is live and has capacity. Lock-free; the
    /// CAS loop retries only under concurrent reservations on the same lease.
    ///
    /// This is the raw counter operation. Request code should prefer
    /// [`crate::reservation::Reservation::reserve`], which pairs the debit
    /// with the commit/release state machine.
    #[inline]
    pub fn try_debit(&self, units: CostUnits, now: Timestamp) -> Result<(), DenyReason> {
        if now >= self.usable_until {
            return Err(DenyReason::LeaseExpired);
        }
        let want = units.get();
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_sub(want) else {
                return Err(DenyReason::LeaseExhausted {
                    remaining: CostUnits(current),
                });
            };
            match self.remaining.compare_exchange_weak(
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

    /// Return previously debited units (release of an uncommitted
    /// reservation). Callers must return only units they debited, exactly
    /// once — the reservation state machine guarantees this.
    #[inline]
    pub(crate) fn credit(&self, units: CostUnits) {
        self.remaining.fetch_add(units.get(), Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn debit_decrements_and_credit_restores() {
        let l = lease(100, 1_000, 25);
        l.try_debit(CostUnits(60), t(0)).unwrap();
        assert_eq!(l.remaining(), CostUnits(40));
        l.credit(CostUnits(60));
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
