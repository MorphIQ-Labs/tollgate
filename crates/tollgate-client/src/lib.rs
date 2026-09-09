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
//! [`InstanceRuntime`] owns discovery, stable account slots, dynamically
//! supervised lease managers, and the bounded usage writer. Its cloneable
//! [`RuntimeHandle`] provides staged admission, readiness, and reports; retain
//! the unique runtime owner and await its shutdown. Lower-level managers remain
//! available for specialized embeddings.
//! Direct-store applications with budget schedules also own a [`PeriodRoller`]
//! beside the admission runtime. Its monitor reports rollover health and
//! confirmed progress; its shutdown is independent of usage and lease cleanup.
//! HTTP-backed applications leave period maintenance to `tollgate-server`.
//!
//! The runtime enforces one total shutdown deadline. It closes accounting
//! admission, pauses refills, drains issued permits and guards, and releases
//! account leases concurrently. Embedders stop their HTTP listeners when they
//! request runtime shutdown and bound their own request-task quiescence by
//! the returned deadline.
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

pub mod lease_manager;
pub mod period_roller;
mod registry;
pub mod runtime;
pub mod snapshot_manager;
pub mod usage_writer;

#[cfg(feature = "http")]
pub mod http;

pub use period_roller::{
    PeriodRoller, PeriodRollerConfig, PeriodRollerConfigError, PeriodRollerHealth,
    PeriodRollerMonitor, PeriodRollerReport, PeriodRollerShutdownReport, PeriodRollerStats,
};
pub use runtime::RuntimeFundingReport;
pub use runtime::{
    AccountPhase, AccountReport, InstanceRuntime, InstanceRuntimeConfig,
    InstanceRuntimeConfigError, RuntimeHandle, RuntimeReadiness, RuntimeReport,
    RuntimeShutdownReport, RuntimeWriterError,
};
pub use tollgate_store::{Clock, ManualClock, SystemClock};

#[cfg(feature = "http")]
pub use http::HttpStore;
pub use lease_manager::{
    AccountLeaseConfig, LeaseCounters, LeaseManager, LeaseManagerConfig, LeaseManagerConfigError,
    LeaseManagerReport, LeaseStats,
};
pub use snapshot_manager::{
    SlotRegistry, SnapshotCounters, SnapshotManager, SnapshotManagerConfig,
    SnapshotManagerConfigError, SnapshotManagerReport, SnapshotStats, TrackedPrincipals,
};
pub use usage_writer::{
    UsagePermit, UsageRecorder, UsageWriter, UsageWriterConfig, UsageWriterConfigError,
    WriterCounters, WriterHealth, WriterShutdownError, WriterStats,
};

/// A control-plane deadline shared with the task that performs cleanup.
/// Setting it can only shorten the remaining budget. Never read by requests.
#[derive(Default)]
pub(crate) struct ShutdownDeadline(std::sync::Mutex<Option<tokio::time::Instant>>);

impl ShutdownDeadline {
    pub(crate) fn constrain(&self, deadline: tokio::time::Instant) {
        let mut current = self.0.lock().expect("shutdown deadline poisoned");
        *current = Some(current.map_or(deadline, |old| old.min(deadline)));
    }

    pub(crate) fn within(&self, budget: std::time::Duration) -> tokio::time::Instant {
        let local = tokio::time::Instant::now() + budget;
        self.0
            .lock()
            .expect("shutdown deadline poisoned")
            .map_or(local, |deadline| deadline.min(local))
    }
}

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

#[cfg(test)]
mod deadline_tests {
    use super::ShutdownDeadline;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn a_shared_shutdown_deadline_can_only_shorten_a_components_budget() {
        let deadline = ShutdownDeadline::default();
        let now = Instant::now();
        let second = Duration::from_secs(1);
        assert_eq!(deadline.within(second), now + second);
        deadline.constrain(now + second * 3);
        deadline.constrain(now + second * 2);
        deadline.constrain(now + second * 4);
        assert_eq!(deadline.within(second * 10), now + second * 2);
        assert_eq!(deadline.within(second), now + second);
        tokio::time::advance(second * 2).await;
        assert_eq!(deadline.within(second * 10), now + second * 2);
    }
}
