//! Instance-side quota runtime.
//!
//! Everything here runs *off* the request path (INVARIANTS.md #6): a
//! [`LeaseManager`] task keeps an account's [`LeaseSlot`] stocked from a
//! [`LeaseAllocator`], and a [`UsageWriter`] task drains a bounded channel of
//! usage events into a [`UsageSink`] in idempotent batches. The request path
//! touches only the slot (lock-free load) and the channel (permit
//! reservation) — when the channel is full, admission sheds *before* work is
//! accepted (INVARIANTS.md #8) via [`UsageRecorder::try_reserve`].
//!
//! Timestamps come from a [`Clock`] so every behavior is testable with a
//! manual clock; production uses [`SystemClock`].
//!
//! [`LeaseAllocator`]: tollgate_store::LeaseAllocator
//! [`UsageSink`]: tollgate_store::UsageSink
//! [`LeaseSlot`]: tollgate_admission::LeaseSlot

pub mod charge_guard;
pub mod lease_manager;
pub mod snapshot_manager;
pub mod usage_writer;

#[cfg(feature = "http")]
pub mod http;

pub use charge_guard::ChargeGuard;
pub use tollgate_store::{Clock, ManualClock, SystemClock};

#[cfg(feature = "http")]
pub use http::HttpStore;
pub use lease_manager::{LeaseManager, LeaseManagerConfig, LeaseManagerConfigError};
pub use snapshot_manager::{
    SlotRegistry, SnapshotManager, SnapshotManagerConfig, SnapshotManagerConfigError,
};
pub use usage_writer::{UsagePermit, UsageRecorder, UsageWriter, UsageWriterConfig, WriterStats};
