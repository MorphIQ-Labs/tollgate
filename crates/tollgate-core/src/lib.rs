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
//! - **Checked arithmetic only.** Cost math never wraps (INVARIANTS.md #11).
//! - **Domain-agnostic.** Cost units, operations, and permissions are generic;
//!   consumers (e.g. FerroRisk) map their own vocabulary onto them at startup.
//!
//! The pieces compose in request order: an [`AccountSnapshot`] admits the
//! principal, a [`CostTable`] quotes the work, a [`LocalLease`] reserves the
//! units, and the resulting [`Reservation`] either commits at execution start
//! or releases for zero charge — producing a [`UsageEvent`] only when
//! committed. See `INVARIANTS.md` at the workspace root.

pub mod cost_table;
pub mod deny;
pub mod ids;
pub mod lease;
pub mod reservation;
pub mod snapshot;
pub mod units;
pub mod usage;

pub use cost_table::{CostQuote, CostTable, CostTableBuilder, OpIndex, QuoteError};
pub use deny::DenyReason;
pub use ids::{AccountId, FencingToken, Generation, KeyId, LeaseId, Principal, RequestId};
pub use lease::{LeaseGrant, LocalLease};
pub use reservation::{CancelOutcome, CommitError, Reservation};
pub use snapshot::{AccountSnapshot, AccountStatus, PermissionBits, ResolvedLimits};
pub use units::CostUnits;
pub use usage::UsageEvent;
