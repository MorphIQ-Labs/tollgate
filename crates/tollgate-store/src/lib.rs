//! Storage abstraction for tollgate.
//!
//! Narrow traits separate the data plane from lifecycle authority:
//!
//! - [`LeaseAllocator`] — atomically debit an account's balance into fenced,
//!   TTL-bounded leases; settle them by release or expiry reclaim.
//! - [`SnapshotSource`] — fetch compiled account snapshots and subscribe to
//!   pushes.
//! - [`UsageSink`] — idempotent, fencing-checked batch ingest of usage
//!   events.
//! - [`KeySource`] — validated, revisioned pages of active credential digests.
//! - [`KeyDirectory`] — durable credential lifecycle; instances do not need
//!   this mutation authority.
//!
//! Every method takes `now` as an argument: the store, like the core, never
//! reads a clock. That keeps backends deterministic under test and puts the
//! clock decision in exactly one place (the client runtime / server).
//!
//! [`MemoryStore`] is the reference implementation: it exists to prove the
//! traits aren't secretly shaped like any particular database, to make the
//! correctness suite run without infrastructure, and to serve as executable
//! documentation of the settlement rules a real backend must reproduce.

pub mod audit;
pub mod clock;
pub mod credentials;
mod leases;
pub mod memory;
pub mod traits;
#[cfg(feature = "wire")]
pub mod wire;

pub use audit::{AdminReceipt, AdminState};
pub use clock::{Clock, ManualClock, SystemClock};
pub use credentials::{
    CredentialRecord, CredentialSet, DEFAULT_KEY_PAGE_LIMIT, KeyPage, KeySource,
    MAX_KEY_PAGE_LIMIT, MAX_KEY_REVISION, validate_key_page_limit,
};
pub use memory::{MemoryStore, StoredRecords};
pub use traits::{
    AccountConfig, AdminStore, AllocateError, BudgetError, Conservation, CreateAccountError,
    CredentialActivity, CredentialActivityState, DEFAULT_RECLAIM_BATCH_LIMIT,
    DEFAULT_ROLLOVER_BATCH_LIMIT, GrantPolicy, GrantPolicyError, IngestError, IngestReport,
    KeyDirectory, KeyError, KeyRecord, LeaseAllocator, MAX_INGEST_BATCH, PUSH_CHANNEL_CAPACITY,
    PublishSnapshotError, ReclaimBatch, ReclaimedLease, Revocation, RolledAccount, RolloverBatch,
    SetStatusError, SnapshotPush, SnapshotResolution, SnapshotSource, StatusChange, StoreError,
    StoreHealth, UsageSink, pushes_exceed_capacity,
};
