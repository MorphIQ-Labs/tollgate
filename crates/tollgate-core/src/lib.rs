//! Zero-I/O, clock-free domain layer for quota admission and accounting.
//!
//! This crate is the request hot path. Its rules, in order:
//!
//! - **No I/O, no clock reads.** Every operation is a function of its
//!   arguments; callers pass `now`. This is what makes the layer benchmarkable
//!   in isolation and embeddable in a service whose whole request budget is a
//!   few microseconds.
//! - **Fail closed.** Unknown, expired, exhausted, or overflowing states deny;
//!   nothing here ever falls back to a slower path, because there is no slower
//!   path to fall back to.
//! - **Checked arithmetic only.** Cost math never wraps (INVARIANTS.md GL-11).
//! - **Domain-agnostic.** Cost units, operations, and permissions are generic;
//!   consumers map their own vocabulary onto them at startup.
//!
//! The pieces compose in request order: an [`AccountSnapshot`] admits the
//! principal, a [`CostTable`] quotes the work, a [`LocalLease`] reserves the
//! units, and the resulting [`Reservation`] either commits at execution start
//! or releases for zero charge — producing a [`UsageEvent`] only when
//! committed. See `INVARIANTS.md` at the workspace root.
//!
//! Tollgate releases the workspace as one unit: every published crate carries
//! the same version, and the crates depend on each other at exactly that
//! version.

#![deny(missing_docs)]

pub mod budget;
pub mod cost_table;
pub mod deny;
pub mod ids;
pub mod lease;
pub mod reservation;
pub mod sharding;
pub mod snapshot;
pub mod units;
pub mod usage;

pub use budget::{
    BalanceExhaustion, BalanceShortfall, BudgetSchedule, BudgetView, Period, Rollover,
};
pub use cost_table::{CostQuote, CostTable, CostTableBuilder, OpIndex, QuoteError};
pub use deny::{DenyReason, Retry};
pub use ids::{
    AccountId, FencingToken, Generation, KeyId, LeaseId, ParseIdError, PolicyRevision, Principal,
    RequestId,
};
pub use lease::{AccountOverage, LeaseGrant, LocalLease, RefillSignal, RefillVerdict};
pub use reservation::{
    CancelHandle, CancelOutcome, CommitError, CommitFunding, Reservation, SharedCharge,
};
pub use sharding::{LocalSharding, Locality, ShardOccupancy};
pub use snapshot::{
    AccountRatePolicy, AccountSnapshot, AccountSnapshotBuilder, AccountStatus, CapacityClass,
    EnforcementMode, PermissionBits, PublishableSnapshot, RequestRateLimit, ResolvedLimits,
    ResolvedLimitsError, SnapshotValidationError, WeightedRateLimit,
};
pub use units::CostUnits;
pub use usage::{DiscardedUsage, DiscardedUsageSlot, UsageEvent, UsageSlot, UsageSource};

// Compiles and runs the README's examples as doctests without adding them to
// the rendered documentation, so the README cannot drift from the API.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
