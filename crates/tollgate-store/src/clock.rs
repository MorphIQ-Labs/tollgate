//! The single place wall-clock time enters the system.

use std::sync::Mutex;

use jiff::{SignedDuration, Timestamp};

/// Decode durable microseconds, including the fractional final second of
/// Timestamp::MAX. The seconds/nanoseconds constructor checks the full domain;
/// Jiff's microsecond constructor omits that final fraction from its bound.
/// Euclidean division preserves pre-epoch timestamps too.
pub fn timestamp_from_micros(value: i64) -> Result<Timestamp, crate::StoreError> {
    Timestamp::new(
        value.div_euclid(1_000_000),
        (value.rem_euclid(1_000_000) * 1_000) as i32,
    )
    .map_err(|_| crate::StoreError("stored microseconds exceed the timestamp domain".into()))
}

/// Supplies `now` to the background planes. The core and admission layers
/// take timestamps as arguments; implementations of this trait are the only
/// code that decides what those timestamps are.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

/// Production clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        #[allow(
            clippy::disallowed_methods,
            reason = "this is the one production read of the business clock; every other caller takes a Timestamp from a Clock, which is what makes the rest of the system replayable"
        )]
        Timestamp::now()
    }
}

/// Deterministic test clock: starts where you set it, moves when you say so.
#[derive(Debug)]
pub struct ManualClock(Mutex<Timestamp>);

impl ManualClock {
    #[must_use]
    pub fn new(start: Timestamp) -> Self {
        ManualClock(Mutex::new(start))
    }

    pub fn set(&self, to: Timestamp) {
        *self.0.lock().expect("manual clock poisoned") = to;
    }

    pub fn advance(&self, by: SignedDuration) {
        let mut guard = self.0.lock().expect("manual clock poisoned");
        *guard = guard.checked_add(by).expect("manual clock overflow");
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().expect("manual clock poisoned")
    }
}
