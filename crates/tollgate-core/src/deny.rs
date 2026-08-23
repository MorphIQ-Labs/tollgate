//! The single vocabulary of admission denials.
//!
//! Every deny is local and fail-closed: none of these variants may ever be
//! "handled" by falling through to synchronous I/O on the request path
//! (INVARIANTS.md #5). The variants carry enough data for a service to render
//! a stable machine-readable error without further lookups.

use core::fmt;

use crate::units::CostUnits;

/// Why a request was refused admission. All refusals charge zero units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DenyReason {
    /// No snapshot is installed for the principal. Includes the negative-cache
    /// case: a principal recently confirmed unknown stays denied until a
    /// snapshot arrives from the control plane.
    UnknownPrincipal,
    /// The account exists but is administratively suspended.
    AccountSuspended,
    /// The account exists but is closed; unlike suspension this is terminal.
    AccountClosed,
    /// The installed snapshot's validity window has lapsed and no replacement
    /// has arrived. Staleness denies; it never triggers an inline fetch.
    SnapshotExpired,
    /// The operation requires permission bits the snapshot does not grant.
    MissingPermission,
    /// The request's item count exceeds the account's resolved batch cap.
    RequestTooLarge {
        /// The account's `max_items_per_request` at the time of refusal.
        max_items: u64,
    },
    /// The operation is not priced in the account's cost table. Unpriced work
    /// cannot be charged, so it cannot be admitted.
    UnpricedOperation,
    /// The account's local rate limiter has no capacity for this request's
    /// weight *right now*. Transient: the bucket refills.
    RateLimited,
    /// The request's quoted weight exceeds the account's entire burst
    /// capacity, so no amount of waiting can admit it. A schedule whose batch
    /// cap admits a quote larger than its burst is misconfigured, not
    /// throttled — telling the caller to retry would be a lie.
    UnpriceableUnderLimits {
        /// The quote that could not fit.
        weight: CostUnits,
        /// The account's whole burst capacity at the time of refusal.
        burst_units: CostUnits,
    },
    /// No lease is currently installed for the account — the instance has not
    /// yet acquired one (cold start) or lost it. Fail closed; the background
    /// refill task is responsible for recovery.
    LeaseUnavailable,
    /// The local lease's validity window has lapsed and refill has not yet
    /// replaced it.
    LeaseExpired,
    /// The local lease lacks sufficient units for the quoted cost.
    LeaseExhausted {
        /// Units still available on the lease at the time of refusal.
        remaining: CostUnits,
    },
    /// Cost arithmetic overflowed. A quote that cannot be represented is
    /// refused rather than wrapped (INVARIANTS.md #11).
    CostOverflow,
    /// The usage-accounting queue is full; admitting more work would either
    /// drop billing events or block. Shedding here is INVARIANTS.md #8.
    AccountingBackpressure,
}

impl DenyReason {
    /// Stable metric labels, in [`index`](DenyReason::index) order.
    ///
    /// These are the *variant* names, not [`Display`](fmt::Display) output.
    /// Three variants interpolate runtime values into their message
    /// (`max_items`, `weight`/`burst_units`, `remaining`), so using the
    /// rendered text as a label would mint a fresh time series per distinct
    /// value — unbounded cardinality from a fixed enum. The label says which
    /// reason; the payload belongs in the response to the caller.
    pub const NAMES: [&'static str; Self::COUNT] = [
        "unknown_principal",
        "account_suspended",
        "account_closed",
        "snapshot_expired",
        "missing_permission",
        "request_too_large",
        "unpriced_operation",
        "rate_limited",
        "unpriceable_under_limits",
        "lease_unavailable",
        "lease_expired",
        "lease_exhausted",
        "cost_overflow",
        "accounting_backpressure",
    ];

    /// How many distinct reasons exist — the width of any per-reason array.
    pub const COUNT: usize = 14;

    /// This reason's dense slot, for direct-indexed per-reason tallies.
    ///
    /// Three variants carry data, so there is no discriminant to cast: the
    /// mapping is written out, and the match is exhaustive on purpose. A new
    /// variant fails to compile until it is given a slot, which is what stops
    /// a reason from silently landing in another's bucket — the same forcing
    /// function [`OpIndex`](crate::OpIndex) gives the cost table.
    #[must_use]
    pub const fn index(&self) -> usize {
        match self {
            DenyReason::UnknownPrincipal => 0,
            DenyReason::AccountSuspended => 1,
            DenyReason::AccountClosed => 2,
            DenyReason::SnapshotExpired => 3,
            DenyReason::MissingPermission => 4,
            DenyReason::RequestTooLarge { .. } => 5,
            DenyReason::UnpricedOperation => 6,
            DenyReason::RateLimited => 7,
            DenyReason::UnpriceableUnderLimits { .. } => 8,
            DenyReason::LeaseUnavailable => 9,
            DenyReason::LeaseExpired => 10,
            DenyReason::LeaseExhausted { .. } => 11,
            DenyReason::CostOverflow => 12,
            DenyReason::AccountingBackpressure => 13,
        }
    }

    /// This reason's stable metric label.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        Self::NAMES[self.index()]
    }
}

impl fmt::Display for DenyReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DenyReason::UnknownPrincipal => f.write_str("unknown principal"),
            DenyReason::AccountSuspended => f.write_str("account suspended"),
            DenyReason::AccountClosed => f.write_str("account closed"),
            DenyReason::SnapshotExpired => f.write_str("account snapshot expired"),
            DenyReason::MissingPermission => f.write_str("missing permission"),
            DenyReason::RequestTooLarge { max_items } => {
                write!(f, "request exceeds batch cap ({max_items} items)")
            }
            DenyReason::UnpricedOperation => f.write_str("operation is not priced"),
            DenyReason::RateLimited => f.write_str("rate limited"),
            DenyReason::UnpriceableUnderLimits {
                weight,
                burst_units,
            } => write!(
                f,
                "request weight {weight} exceeds the account's whole burst capacity \
                 ({burst_units} units); retrying cannot help"
            ),
            DenyReason::LeaseUnavailable => f.write_str("no quota lease available"),
            DenyReason::LeaseExpired => f.write_str("quota lease expired"),
            DenyReason::LeaseExhausted { remaining } => {
                write!(f, "quota lease exhausted ({remaining} units remaining)")
            }
            DenyReason::CostOverflow => f.write_str("cost arithmetic overflow"),
            DenyReason::AccountingBackpressure => f.write_str("usage accounting backpressure"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, once. Sized by `COUNT`, so adding a reason without
    /// widening this array fails to compile — which is what keeps the
    /// coverage claims below honest rather than aspirational.
    const ALL: [DenyReason; DenyReason::COUNT] = [
        DenyReason::UnknownPrincipal,
        DenyReason::AccountSuspended,
        DenyReason::AccountClosed,
        DenyReason::SnapshotExpired,
        DenyReason::MissingPermission,
        DenyReason::RequestTooLarge { max_items: 64 },
        DenyReason::UnpricedOperation,
        DenyReason::RateLimited,
        DenyReason::UnpriceableUnderLimits {
            weight: CostUnits(9),
            burst_units: CostUnits(4),
        },
        DenyReason::LeaseUnavailable,
        DenyReason::LeaseExpired,
        DenyReason::LeaseExhausted {
            remaining: CostUnits(3),
        },
        DenyReason::CostOverflow,
        DenyReason::AccountingBackpressure,
    ];

    /// The indices must be a permutation of `0..COUNT`. Two reasons sharing a
    /// slot would silently merge their tallies, and a slot no reason maps to
    /// would export a counter that can never move.
    #[test]
    fn indices_cover_every_slot_exactly_once() {
        let mut seen = [false; DenyReason::COUNT];
        for reason in ALL {
            let index = reason.index();
            assert!(index < DenyReason::COUNT, "{reason} indexes out of range");
            assert!(!seen[index], "{reason} shares slot {index}");
            seen[index] = true;
        }
        assert!(
            seen.iter().all(|hit| *hit),
            "every slot must belong to some reason: {seen:?}"
        );
    }

    /// A label is read by whatever scrapes the counters, so it is a contract:
    /// distinct per reason, and free of the runtime values `Display` carries.
    /// Interpolating those would turn fourteen counters into an unbounded set
    /// of time series.
    #[test]
    fn labels_are_distinct_and_payload_free() {
        for (position, reason) in ALL.iter().enumerate() {
            assert_eq!(
                reason.name(),
                DenyReason::NAMES[position],
                "NAMES must be in index order"
            );
            assert!(
                !reason.name().contains(|c: char| c.is_ascii_digit()),
                "{reason} label carries a runtime value"
            );
        }
        let mut names = DenyReason::NAMES;
        names.sort_unstable();
        let unique = names.len();
        names.iter().reduce(|previous, next| {
            assert_ne!(previous, next, "duplicate label {next}");
            next
        });
        assert_eq!(unique, DenyReason::COUNT);
    }

    /// The payload-carrying variants must index by variant, not by value —
    /// otherwise a single reason would scatter across slots as its data
    /// varied, and no slot would total that reason.
    #[test]
    fn payload_does_not_affect_the_slot() {
        assert_eq!(
            DenyReason::RequestTooLarge { max_items: 1 }.index(),
            DenyReason::RequestTooLarge {
                max_items: u64::MAX
            }
            .index()
        );
        assert_eq!(
            DenyReason::LeaseExhausted {
                remaining: CostUnits::ZERO
            }
            .index(),
            DenyReason::LeaseExhausted {
                remaining: CostUnits(u64::MAX)
            }
            .index()
        );
    }
}
