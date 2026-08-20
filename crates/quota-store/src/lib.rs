//! Storage abstraction for quota-service.
//!
//! Three narrow traits cover everything the data plane needs from a backend:
//!
//! - [`LeaseAllocator`] — atomically debit an account's balance into fenced,
//!   TTL-bounded leases; settle them by release or expiry reclaim.
//! - [`SnapshotSource`] — fetch compiled account snapshots and subscribe to
//!   pushes.
//! - [`UsageSink`] — idempotent, fencing-checked batch ingest of usage
//!   events.
//!
//! Every method takes `now` as an argument: the store, like the core, never
//! reads a clock. That keeps backends deterministic under test and puts the
//! clock decision in exactly one place (the client runtime / server).
//!
//! [`MemoryStore`] is the reference implementation: it exists to prove the
//! traits aren't secretly shaped like any particular database, to make the
//! correctness suite run without infrastructure, and to serve as executable
//! documentation of the settlement rules a real backend must reproduce.

pub mod memory;
pub mod traits;

pub use memory::{AccountConfig, MemoryStore};
pub use traits::{
    AllocateError, GrantPolicy, IngestReport, LeaseAllocator, ReclaimedLease, SnapshotPush,
    SnapshotSource, StoreError, UsageSink,
};
