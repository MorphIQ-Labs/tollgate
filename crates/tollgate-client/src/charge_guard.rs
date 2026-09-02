//! Couples a committed charge to guaranteed usage emission (review
//! finding #3).
//!
//! Once a reservation commits, the lease units are spent — so the billing
//! event must reach the usage queue no matter how execution ends. Committing
//! through [`ChargeGuard::commit`] binds the event to the pre-reserved
//! [`UsagePermit`] at commit time and takes ownership of the complete admitted
//! request, including its concurrency permits. The guard's `Drop` performs
//! the actual enqueue and only then releases occupancy. Normal completion,
//! early return, panic unwind, and task abort at an await point all run `Drop`,
//! so a committed charge can no longer be spent-but-unbilled or disappear
//! from concurrency accounting because the handler died mid-execution.

use jiff::Timestamp;

use tollgate_admission::{Admitted, CommittedAdmission};
use tollgate_core::{CommitError, CostUnits, RequestId, UsageEvent};

use crate::usage_writer::UsagePermit;

/// A committed execution whose billing event is emitted and whose concurrency
/// occupancy is released on drop.
///
/// Discarding execution-start evidence must be a compile-time error under the
/// standard `unused_must_use` lint:
///
/// ```compile_fail
/// # #![deny(unused_must_use)]
/// # #![allow(deprecated)]
/// # fn discard(
/// #     admitted: tollgate_admission::Admitted,
/// #     permit: tollgate_client::UsagePermit,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<(), tollgate_core::CommitError> {
/// tollgate_client::ChargeGuard::commit(admitted, permit, request_id, now)?;
/// # Ok(())
/// # }
/// ```
///
/// Its companion: binding the guard compiles, so the refusal above is a
/// refusal to *discard* the guard rather than a refusal of a call that
/// stopped type-checking.
///
/// ```
/// # #![allow(deprecated)]
/// # fn hold(
/// #     admitted: tollgate_admission::Admitted,
/// #     permit: tollgate_client::UsagePermit,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<tollgate_client::ChargeGuard, tollgate_core::CommitError> {
/// tollgate_client::ChargeGuard::commit(admitted, permit, request_id, now)
/// # }
/// ```
#[must_use = "hold this guard for the full execution lifetime"]
#[deprecated(since = "0.9.0", note = "use tollgate_admission::ReadyToStart::commit")]
pub struct ChargeGuard {
    event: Option<UsageEvent>,
    permit: Option<UsagePermit>,
    units: CostUnits,
    // Dropped after `Drop::drop` enqueues the event, so concurrency remains
    // occupied throughout the complete execution and emission handoff.
    _admission: CommittedAdmission,
}

#[allow(deprecated)]
impl ChargeGuard {
    /// Consume `admitted` at execution start and bind its billing event to
    /// `permit`. The returned guard owns the complete committed admission,
    /// including its concurrency permits.
    ///
    /// On failure the permit is released (its queue slot frees) and zero
    /// units are charged — the deny paths behave exactly as before. On
    /// success the returned guard owns emission: hold it across execution
    /// and let it drop.
    pub fn commit(
        admitted: Admitted,
        permit: UsagePermit,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<ChargeGuard, CommitError> {
        let admission = admitted.commit(request_id, now)?;
        let units = admission.units();
        let event = admission.usage_event();
        Ok(ChargeGuard {
            event: Some(event),
            permit: Some(permit),
            units,
            _admission: admission,
        })
    }

    /// The full charge committed at execution start.
    #[must_use]
    pub const fn units(&self) -> CostUnits {
        self.units
    }
}

#[allow(deprecated)]
impl Drop for ChargeGuard {
    fn drop(&mut self) {
        if let (Some(event), Some(permit)) = (self.event.take(), self.permit.take()) {
            permit.record(event);
        }
    }
}
