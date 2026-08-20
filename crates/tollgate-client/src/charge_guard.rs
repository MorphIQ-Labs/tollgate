//! Couples a committed charge to guaranteed usage emission (review
//! finding #3).
//!
//! Once a reservation commits, the lease units are spent — so the billing
//! event must reach the usage queue no matter how execution ends. Committing
//! through [`ChargeGuard::commit`] binds the event to the pre-reserved
//! [`UsagePermit`] at commit time; the guard's `Drop` performs the actual
//! enqueue. Normal completion, early return, panic unwind, and task abort at
//! an await point all run `Drop`, so a committed charge can no longer be
//! spent-but-unbilled because the handler died between commit and record.

use jiff::Timestamp;

use tollgate_core::{CommitError, CostUnits, RequestId, Reservation, UsageEvent};

use crate::usage_writer::UsagePermit;

/// A committed charge whose billing event is emitted on drop.
pub struct ChargeGuard {
    event: Option<UsageEvent>,
    permit: Option<UsagePermit>,
}

impl ChargeGuard {
    /// Commit `reservation` at execution start and bind its billing event to
    /// `permit`.
    ///
    /// On failure the permit is released (its queue slot frees) and zero
    /// units are charged — the deny paths behave exactly as before. On
    /// success the returned guard owns emission: hold it across execution
    /// and let it drop.
    pub fn commit(
        reservation: &Reservation,
        permit: UsagePermit,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<(ChargeGuard, CostUnits), CommitError> {
        let units = reservation.commit_at_execution_start(now)?;
        let event = reservation
            .usage_event(request_id, now)
            .expect("committed reservation always yields its event");
        Ok((
            ChargeGuard {
                event: Some(event),
                permit: Some(permit),
            },
            units,
        ))
    }
}

impl Drop for ChargeGuard {
    fn drop(&mut self) {
        if let (Some(event), Some(permit)) = (self.event.take(), self.permit.take()) {
            permit.record(event);
        }
    }
}
