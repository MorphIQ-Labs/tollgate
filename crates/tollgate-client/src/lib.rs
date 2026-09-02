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
//! Graceful shutdown has one safe order: stop admitting, quiesce the request
//! tasks still holding permits or committed
//! [`tollgate_admission::Committed`] guards, await
//! [`UsageWriter::shutdown`] (which refuses new reservations, then drains
//! outstanding permits under its configured deadline and reports anything
//! unresolved), and only then shut the [`LeaseManager`] down — usage events
//! must land while their lease is live (INVARIANTS.md #12).
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

#[allow(deprecated)]
pub use charge_guard::ChargeGuard;
pub use tollgate_store::{Clock, ManualClock, SystemClock};

#[cfg(feature = "http")]
pub use http::HttpStore;
pub use lease_manager::{
    LeaseCounters, LeaseManager, LeaseManagerConfig, LeaseManagerConfigError, LeaseManagerReport,
    LeaseStats,
};
pub use snapshot_manager::{
    SlotRegistry, SnapshotCounters, SnapshotManager, SnapshotManagerConfig,
    SnapshotManagerConfigError, SnapshotManagerReport, SnapshotStats, TrackedPrincipals,
};
pub use usage_writer::{
    UsagePermit, UsageRecorder, UsageWriter, UsageWriterConfig, UsageWriterConfigError,
    WriterCounters, WriterHealth, WriterShutdownError, WriterStats,
};

/// Set a watch channel, reporting the one way it can fail.
///
/// A `watch` send fails only when every receiver has been dropped, which
/// means the observer this signal was for is already gone. That is never
/// actionable by itself — the caller's own report carries the outcome — but
/// it is a breadcrumb, and discarding it silently is the habit issue #36
/// exists to break.
pub(crate) fn signal(tx: &tokio::sync::watch::Sender<bool>, value: bool, signal: &'static str) {
    if tx.send(value).is_err() {
        tracing::debug!(signal, value, "no receivers remain for signal");
    }
}
