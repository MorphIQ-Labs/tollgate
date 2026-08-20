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
    /// The account's local rate limiter has no capacity for this request's
    /// weight.
    RateLimited,
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

impl fmt::Display for DenyReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DenyReason::UnknownPrincipal => f.write_str("unknown principal"),
            DenyReason::AccountSuspended => f.write_str("account suspended"),
            DenyReason::AccountClosed => f.write_str("account closed"),
            DenyReason::SnapshotExpired => f.write_str("account snapshot expired"),
            DenyReason::MissingPermission => f.write_str("missing permission"),
            DenyReason::RateLimited => f.write_str("rate limited"),
            DenyReason::LeaseExpired => f.write_str("quota lease expired"),
            DenyReason::LeaseExhausted { remaining } => {
                write!(f, "quota lease exhausted ({remaining} units remaining)")
            }
            DenyReason::CostOverflow => f.write_str("cost arithmetic overflow"),
            DenyReason::AccountingBackpressure => f.write_str("usage accounting backpressure"),
        }
    }
}
