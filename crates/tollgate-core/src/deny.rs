//! The single vocabulary of admission denials.
//!
//! Every deny is local and fail-closed: none of these variants may ever be
//! "handled" by falling through to synchronous I/O on the request path
//! (INVARIANTS.md #5). The variants carry enough data for a service to render
//! a stable machine-readable error without further lookups.
//!
//! A reason lands in the same change as the code that produces it. The enum is
//! exhaustive for embedders and [`index`](DenyReason::index) hands every later
//! variant a dense counter slot, so a reason added ahead of its producer
//! renumbers exported counters and forces every embedder to write an arm for a
//! refusal that cannot occur — a compatibility break bought with nothing.

use core::fmt;

use crate::units::CostUnits;

/// Whether retrying a denied request can become useful without changing the
/// request itself.
///
/// This classification belongs to the denial vocabulary so embedders cannot
/// drift into contradictory retry policies for the same reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// The same request can become admissible when capacity or freshness
    /// recovers, without an account funding change.
    Transient,
    /// Retry after a concurrent admission decision finishes publishing. This
    /// does not promise admission: the stable retry may then report transient
    /// capacity.
    AfterInFlight,
    /// Retrying the same request under the current policy and funding cannot
    /// make it admissible. New funding or a new budget period can change that.
    Never,
}

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
    /// weight *right now*. Classifies as [`Retry::Transient`] because the
    /// bucket refills; it carries no retry instant or delay.
    RateLimited,
    /// The account's request-count bucket has no token available right now.
    /// Classifies as [`Retry::Transient`] and carries no retry instant or delay.
    RequestRateLimited,
    /// An account or principal in-flight request ceiling is saturated.
    ConcurrencyLimited,
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
    /// An elastic request cannot fit inside the account's `overage_cap` even
    /// if every refundable pending reservation releases its credit.
    ///
    /// This proves only that the instance's local overage allowance cannot
    /// fund the request. It does not prove central account exhaustion: the
    /// lease refusal that led to this fallback can recover through an ordinary
    /// background grant, so the combined admission state remains transient.
    OverageCapExhausted {
        /// Unfunded units already extended on this instance.
        spent: CostUnits,
        /// The account's whole per-instance allowance at the time of refusal.
        overage_cap: CostUnits,
    },
    /// Cost arithmetic overflowed. A quote that cannot be represented is
    /// refused rather than wrapped (INVARIANTS.md #11).
    CostOverflow,
    /// The usage-accounting queue is full; admitting more work would either
    /// drop billing events or block. Shedding here is INVARIANTS.md #8.
    AccountingBackpressure,
    /// Pending overage reservations currently occupy enough of the account's
    /// cap to refuse this request, but cancellation or drop can return that
    /// credit without a funding or policy change.
    OverageCapTemporarilyExhausted {
        /// Unfunded units currently extended on this instance.
        spent: CostUnits,
        /// The account's whole per-instance allowance at the time of refusal.
        overage_cap: CostUnits,
    },
    /// A reservation is atomically publishing its transition from refundable
    /// pending overage to committed occupancy. The request is refused until
    /// that publication settles; retrying then receives the stable temporary
    /// or committed-saturation answer.
    OverageCommitInProgress {
        /// Unfunded units currently extended on this instance.
        spent: CostUnits,
        /// The account's whole per-instance allowance at the time of refusal.
        overage_cap: CostUnits,
    },
    /// The staged request carried no priceable work. Produced by
    /// `compile_workload`, which is the first point that sees the workload:
    /// stage one runs before the body is decoded.
    EmptyWorkload,
    /// The funding reserved at admission had expired by the time execution
    /// started. Only the staged lifecycle can produce this — the one-shot API
    /// leaves no interval between admission and start for it to happen in.
    FundingExpiredAtStart,
    /// This instance has no execution capacity to start the request with
    /// (#99). The account is valid, funded, and within every limit of its
    /// own — the instance simply cannot afford to begin the work now.
    ///
    /// Deliberately not aliased to `RateLimited`, `LeaseExhausted`, or
    /// `AccountingBackpressure`: those say something about the *account*, and
    /// a caller told one of them would look at its own quota for a condition
    /// that has nothing to do with it. Transient, and honestly so — capacity
    /// is returned by every request that finishes.
    CapacityUnavailable,
    /// The allocator confirmed that account funding is exhausted. Retry
    /// requires new funding or a new budget period; this is not a lease gap.
    BalanceExhausted,
}

impl DenyReason {
    /// Stable metric labels, in [`index`](DenyReason::index) order.
    ///
    /// These are the *variant* names, not [`Display`](fmt::Display) output.
    /// Six variants interpolate runtime values into their message
    /// (`max_items`, `weight`/`burst_units`, `remaining`, `spent`), so using the
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
        "request_rate_limited",
        "concurrency_limited",
        "unpriceable_under_limits",
        "lease_unavailable",
        "lease_expired",
        "lease_exhausted",
        "overage_cap_exhausted",
        "cost_overflow",
        "accounting_backpressure",
        "overage_cap_temporarily_exhausted",
        "overage_commit_in_progress",
        "empty_workload",
        "funding_expired_at_start",
        "capacity_unavailable",
        "balance_exhausted",
    ];

    /// How many distinct reasons exist — the width of any per-reason array.
    pub const COUNT: usize = 23;

    /// This reason's dense slot, for direct-indexed per-reason tallies.
    ///
    /// Six variants carry data, so there is no discriminant to cast: the
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
            DenyReason::RequestRateLimited => 8,
            DenyReason::ConcurrencyLimited => 9,
            DenyReason::UnpriceableUnderLimits { .. } => 10,
            DenyReason::LeaseUnavailable => 11,
            DenyReason::LeaseExpired => 12,
            DenyReason::LeaseExhausted { .. } => 13,
            DenyReason::OverageCapExhausted { .. } => 14,
            DenyReason::CostOverflow => 15,
            DenyReason::AccountingBackpressure => 16,
            DenyReason::OverageCapTemporarilyExhausted { .. } => 17,
            DenyReason::OverageCommitInProgress { .. } => 18,
            DenyReason::EmptyWorkload => 19,
            DenyReason::FundingExpiredAtStart => 20,
            DenyReason::CapacityUnavailable => 21,
            DenyReason::BalanceExhausted => 22,
        }
    }

    /// This reason's stable metric label.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        Self::NAMES[self.index()]
    }

    /// The stable retry classification for this refusal.
    #[must_use]
    pub const fn retry(&self) -> Retry {
        match self {
            DenyReason::RateLimited
            | DenyReason::RequestRateLimited
            | DenyReason::ConcurrencyLimited
            | DenyReason::SnapshotExpired
            | DenyReason::LeaseUnavailable
            | DenyReason::LeaseExpired
            | DenyReason::LeaseExhausted { .. }
            | DenyReason::AccountingBackpressure
            | DenyReason::OverageCapExhausted { .. }
            | DenyReason::OverageCapTemporarilyExhausted { .. }
            | DenyReason::CapacityUnavailable => Retry::Transient,
            DenyReason::OverageCommitInProgress { .. } => Retry::AfterInFlight,
            DenyReason::FundingExpiredAtStart => Retry::Transient,
            DenyReason::UnknownPrincipal
            | DenyReason::AccountSuspended
            | DenyReason::AccountClosed
            | DenyReason::MissingPermission
            | DenyReason::RequestTooLarge { .. }
            | DenyReason::UnpricedOperation
            | DenyReason::UnpriceableUnderLimits { .. }
            | DenyReason::CostOverflow
            | DenyReason::EmptyWorkload
            | DenyReason::BalanceExhausted => Retry::Never,
        }
    }
}

impl fmt::Display for DenyReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DenyReason::BalanceExhausted => f.write_str("account balance exhausted"),
            DenyReason::UnknownPrincipal => f.write_str("unknown principal"),
            DenyReason::AccountSuspended => f.write_str("account suspended"),
            DenyReason::AccountClosed => f.write_str("account closed"),
            DenyReason::SnapshotExpired => f.write_str("account snapshot expired"),
            DenyReason::MissingPermission => f.write_str("missing permission"),
            DenyReason::RequestTooLarge { max_items } => {
                write!(f, "request exceeds batch cap ({max_items} items)")
            }
            DenyReason::UnpricedOperation => f.write_str("operation is not priced"),
            DenyReason::CapacityUnavailable => {
                f.write_str("no execution capacity available on this instance")
            }
            DenyReason::RateLimited => f.write_str("rate limited"),
            DenyReason::RequestRateLimited => f.write_str("request rate limited"),
            DenyReason::ConcurrencyLimited => f.write_str("concurrency limit reached"),
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
            DenyReason::OverageCapExhausted { spent, overage_cap } => write!(
                f,
                "overage cap reached ({spent} of {overage_cap} unfunded units extended \
                 on this instance); a background lease refill may restore capacity"
            ),
            DenyReason::CostOverflow => f.write_str("cost arithmetic overflow"),
            DenyReason::AccountingBackpressure => f.write_str("usage accounting backpressure"),
            DenyReason::OverageCapTemporarilyExhausted { spent, overage_cap } => write!(
                f,
                "overage cap temporarily reached ({spent} of {overage_cap} unfunded units \
                 extended on this instance); pending work may release credit"
            ),
            DenyReason::OverageCommitInProgress { spent, overage_cap } => write!(
                f,
                "overage commit publication in progress ({spent} of {overage_cap} unfunded \
                 units extended on this instance)"
            ),
            DenyReason::EmptyWorkload => f.write_str("request carried no priceable work"),
            DenyReason::FundingExpiredAtStart => {
                f.write_str("reserved funding expired before execution started")
            }
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
        DenyReason::RequestRateLimited,
        DenyReason::ConcurrencyLimited,
        DenyReason::UnpriceableUnderLimits {
            weight: CostUnits(9),
            burst_units: CostUnits(4),
        },
        DenyReason::LeaseUnavailable,
        DenyReason::LeaseExpired,
        DenyReason::LeaseExhausted {
            remaining: CostUnits(3),
        },
        DenyReason::OverageCapExhausted {
            spent: CostUnits(20),
            overage_cap: CostUnits(20),
        },
        DenyReason::CostOverflow,
        DenyReason::AccountingBackpressure,
        DenyReason::OverageCapTemporarilyExhausted {
            spent: CostUnits(20),
            overage_cap: CostUnits(20),
        },
        DenyReason::OverageCommitInProgress {
            spent: CostUnits(20),
            overage_cap: CostUnits(20),
        },
        DenyReason::EmptyWorkload,
        DenyReason::FundingExpiredAtStart,
        DenyReason::CapacityUnavailable,
        DenyReason::BalanceExhausted,
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

    /// Every slot and label that has shipped, written out as literals.
    ///
    /// The permutation test above proves the mapping is *internally*
    /// consistent, and `labels_are_distinct_and_payload_free` proves `ALL` is
    /// in index order. Neither pins a reason to a particular slot: renumbering
    /// `index()`, `NAMES`, and `ALL` together satisfies both while moving
    /// every counter a consumer already reads. `AdmissionCounters::denials` is
    /// exported through the public `CountersSnapshot` as dense positions, so
    /// the number is the contract, not an implementation detail.
    ///
    /// That is not hypothetical. Declaring three reasons before their
    /// producers existed shifted `LeaseExhausted` from 11 to 13 and
    /// `AccountingBackpressure` from 14 to 16; a consumer holding exported
    /// indices would have mis-attributed both across the upgrade, for refusals
    /// that could not occur. Removing them shifted everything back. Both moves
    /// passed every other test in this module.
    ///
    /// This change is the case that argument was written for. `EmptyWorkload`
    /// and `FundingExpiredAtStart` return with the staged lifecycle that
    /// produces them, and they take slots 19 and 20 — not the 17 and 18 they
    /// carried before the removal, which now belong to
    /// `OverageCapTemporarilyExhausted` and `OverageCommitInProgress`. A new
    /// reason appends and extends this table; editing a number already in it
    /// is a breaking change for every consumer holding exported indices.
    #[test]
    fn shipped_slots_and_labels_never_move() {
        // (reason, slot, label) — append only.
        let shipped: [(DenyReason, usize, &str); DenyReason::COUNT] = [
            (DenyReason::UnknownPrincipal, 0, "unknown_principal"),
            (DenyReason::AccountSuspended, 1, "account_suspended"),
            (DenyReason::AccountClosed, 2, "account_closed"),
            (DenyReason::SnapshotExpired, 3, "snapshot_expired"),
            (DenyReason::MissingPermission, 4, "missing_permission"),
            (
                DenyReason::RequestTooLarge { max_items: 64 },
                5,
                "request_too_large",
            ),
            (DenyReason::UnpricedOperation, 6, "unpriced_operation"),
            (DenyReason::RateLimited, 7, "rate_limited"),
            (DenyReason::RequestRateLimited, 8, "request_rate_limited"),
            (DenyReason::ConcurrencyLimited, 9, "concurrency_limited"),
            (
                DenyReason::UnpriceableUnderLimits {
                    weight: CostUnits(9),
                    burst_units: CostUnits(4),
                },
                10,
                "unpriceable_under_limits",
            ),
            (DenyReason::LeaseUnavailable, 11, "lease_unavailable"),
            (DenyReason::LeaseExpired, 12, "lease_expired"),
            (
                DenyReason::LeaseExhausted {
                    remaining: CostUnits(3),
                },
                13,
                "lease_exhausted",
            ),
            (
                DenyReason::OverageCapExhausted {
                    spent: CostUnits(20),
                    overage_cap: CostUnits(20),
                },
                14,
                "overage_cap_exhausted",
            ),
            (DenyReason::CostOverflow, 15, "cost_overflow"),
            (
                DenyReason::AccountingBackpressure,
                16,
                "accounting_backpressure",
            ),
            (
                DenyReason::OverageCapTemporarilyExhausted {
                    spent: CostUnits(20),
                    overage_cap: CostUnits(20),
                },
                17,
                "overage_cap_temporarily_exhausted",
            ),
            (
                DenyReason::OverageCommitInProgress {
                    spent: CostUnits(20),
                    overage_cap: CostUnits(20),
                },
                18,
                "overage_commit_in_progress",
            ),
            (DenyReason::EmptyWorkload, 19, "empty_workload"),
            (
                DenyReason::FundingExpiredAtStart,
                20,
                "funding_expired_at_start",
            ),
            (DenyReason::CapacityUnavailable, 21, "capacity_unavailable"),
            (DenyReason::BalanceExhausted, 22, "balance_exhausted"),
        ];

        for (reason, slot, label) in shipped {
            assert_eq!(
                reason.index(),
                slot,
                "{reason} moved off its exported slot {slot}"
            );
            assert_eq!(
                reason.name(),
                label,
                "{reason} changed the label scrapers key on"
            );
        }

        assert_eq!(
            DenyReason::COUNT,
            23,
            "COUNT may only grow, and only by appending"
        );
    }

    /// A label is read by whatever scrapes the counters, so it is a contract:
    /// distinct per reason, and free of the runtime values `Display` carries.
    /// Interpolating those would turn a fixed counter set into an unbounded
    /// number of time series.
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
        assert_eq!(
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits::ZERO,
                overage_cap: CostUnits(1),
            }
            .index(),
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(u64::MAX),
                overage_cap: CostUnits(u64::MAX),
            }
            .index()
        );
        assert_eq!(
            DenyReason::OverageCommitInProgress {
                spent: CostUnits::ZERO,
                overage_cap: CostUnits(1),
            }
            .index(),
            DenyReason::OverageCommitInProgress {
                spent: CostUnits(u64::MAX),
                overage_cap: CostUnits(u64::MAX),
            }
            .index()
        );
    }

    #[test]
    fn every_reason_has_the_expected_retry_class() {
        for reason in ALL {
            let expected = match reason {
                DenyReason::OverageCommitInProgress { .. } => Retry::AfterInFlight,
                DenyReason::FundingExpiredAtStart => Retry::Transient,
                DenyReason::EmptyWorkload | DenyReason::BalanceExhausted => Retry::Never,
                DenyReason::RateLimited
                | DenyReason::RequestRateLimited
                | DenyReason::ConcurrencyLimited
                | DenyReason::SnapshotExpired
                | DenyReason::LeaseUnavailable
                | DenyReason::LeaseExpired
                | DenyReason::LeaseExhausted { .. }
                | DenyReason::AccountingBackpressure
                | DenyReason::OverageCapExhausted { .. }
                | DenyReason::OverageCapTemporarilyExhausted { .. }
                | DenyReason::CapacityUnavailable => Retry::Transient,
                DenyReason::UnknownPrincipal
                | DenyReason::AccountSuspended
                | DenyReason::AccountClosed
                | DenyReason::MissingPermission
                | DenyReason::RequestTooLarge { .. }
                | DenyReason::UnpricedOperation
                | DenyReason::UnpriceableUnderLimits { .. }
                | DenyReason::CostOverflow => Retry::Never,
            };
            assert_eq!(reason.retry(), expected, "{reason}");
        }
    }
}
