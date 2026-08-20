//! Usage events: the billing record.
//!
//! Leases *bound* spend; usage events *are* what gets billed. Reconciliation
//! compares the two ledgers and steady-state drift is zero (INVARIANTS.md,
//! ledger-roles note). Events are idempotent on `request_id`, so batched
//! writers may retry whole batches freely (INVARIANTS.md #7).

use jiff::Timestamp;

use crate::ids::{AccountId, FencingToken, LeaseId, RequestId};
use crate::units::CostUnits;

/// One committed charge. Produced only from a committed
/// [`crate::reservation::Reservation`]; there is deliberately no public
/// constructor path for uncommitted work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UsageEvent {
    /// Idempotency key: a sink must treat a replayed `request_id` as the same
    /// charge, not a new one.
    pub request_id: RequestId,
    pub account_id: AccountId,
    /// The lease the units were spent from, for lease-vs-usage
    /// reconciliation.
    pub lease_id: LeaseId,
    /// The spender's fencing token; a sink rejects tokens older than the
    /// newest it has accepted for the account (INVARIANTS.md #4).
    pub fencing_token: FencingToken,
    pub units: CostUnits,
    pub occurred_at: Timestamp,
}
