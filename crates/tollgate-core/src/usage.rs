//! Usage events: the billing record.
//!
//! Leases *bound* spend; usage events *are* what gets billed. Reconciliation
//! compares the two ledgers and steady-state drift is zero (INVARIANTS.md,
//! ledger-roles note). Events are idempotent on `request_id`, so batched
//! writers may retry whole batches freely (INVARIANTS.md #7).

use jiff::Timestamp;

use crate::ids::{AccountId, FencingToken, LeaseId, RequestId};
use crate::units::CostUnits;

/// Pre-reserved capacity for exactly one usage event.
///
/// Implementations must record without fallible I/O: obtaining a slot is the
/// backpressure decision, while consuming it is the committed-charge path.
pub trait UsageSlot: Send + 'static {
    fn record(self, event: UsageEvent);
}

/// What funded the units in a [`UsageEvent`].
///
/// Two variants, because two things fund spend and they are validated by
/// opposite rules. A leased charge arrives with a capability the sink checks
/// before it touches the ledger; an overage charge has no capability to check,
/// because no lease existed to issue one.
///
/// Making this a sum type rather than a pair of `Option`s is deliberate. A
/// half-formed capability — a lease id with no token, or a token naming no
/// lease — is the shape a sink would have to defend against, and here it
/// cannot be written down. The storage schema mirrors the same constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UsageSource {
    /// Spent from a lease. The sink requires the stored
    /// `(lease_id, account_id, fencing_token)` triple to match before the
    /// event may change either ledger; token age relative to another active
    /// lease is irrelevant (INVARIANTS.md #4).
    Leased {
        lease_id: LeaseId,
        fencing_token: FencingToken,
    },
    /// Admitted under [`EnforcementMode::Elastic`] with no lease behind it.
    ///
    /// **It carries no lease id on purpose, and must never be given one.**
    /// Attributing unfunded spend to a real lease drives that lease's `used`
    /// past its `granted`, which makes reclaim's `granted - used` credit
    /// negative — a panic in the memory backend and a permanently failing
    /// transaction in Postgres, so the account's expired leases would never be
    /// reclaimed again. Overage stands outside lease accounting entirely and
    /// is funded by its own ledger term.
    ///
    /// [`EnforcementMode::Elastic`]: crate::snapshot::EnforcementMode::Elastic
    Overage,
}

impl UsageSource {
    /// The lease this charge was spent from, or `None` for overage.
    #[must_use]
    pub const fn lease_id(self) -> Option<LeaseId> {
        match self {
            UsageSource::Leased { lease_id, .. } => Some(lease_id),
            UsageSource::Overage => None,
        }
    }

    /// The referenced lease's capability token, or `None` for overage.
    #[must_use]
    pub const fn fencing_token(self) -> Option<FencingToken> {
        match self {
            UsageSource::Leased { fencing_token, .. } => Some(fencing_token),
            UsageSource::Overage => None,
        }
    }
}

/// One committed charge. Produced only from a committed
/// [`crate::reservation::Reservation`]; there is deliberately no public
/// constructor path for uncommitted work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UsageEvent {
    /// Idempotency key: a sink must treat a replayed `request_id` as the same
    /// charge, not a new one.
    ///
    /// The only field of this struct that is independent of how the units were
    /// funded, which is what lets overage replay under the same rule as any
    /// other event (INVARIANTS.md #7).
    pub request_id: RequestId,
    pub account_id: AccountId,
    /// What funded the units, and the evidence the sink validates.
    pub source: UsageSource,
    pub units: CostUnits,
    pub occurred_at: Timestamp,
}
