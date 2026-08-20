//! The single place wall-clock time enters the system.

use std::sync::Mutex;

use jiff::{SignedDuration, Timestamp};

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
