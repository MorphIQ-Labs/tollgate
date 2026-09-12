//! Ownership and health of the server's maintenance task. The worker is the
//! only outcome publisher; the owner can permanently withdraw readiness before
//! aborting it. A late success cannot undo that terminal withdrawal.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

/// Three consecutive failed scheduled passes constitute a persistent outage
/// for alerting. This is not a claim that a particular lease TTL has elapsed;
/// readiness falls on the first failure, independently of log escalation.
pub(crate) const PERSISTENT_FAILURES: u64 = 3;

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    Reclaim,
    Rollover,
}

#[derive(Clone, Copy, Default)]
struct Pass {
    healthy: bool,
    failures: u64,
}

#[derive(Clone, Copy, Default)]
struct State {
    reclaim: Pass,
    rollover: Pass,
}

#[derive(Clone)]
pub(crate) struct Monitor {
    state: watch::Receiver<State>,
    stopping: Arc<AtomicBool>,
}

impl Monitor {
    pub(crate) fn healthy(&self) -> bool {
        let exited = self.state.has_changed().is_err();
        let state = *self.state.borrow();
        !exited
            && !self.stopping.load(Ordering::Acquire)
            && state.reclaim.healthy
            && state.rollover.healthy
    }

    /// A router without a maintenance owner cannot advertise server readiness.
    pub(crate) fn unmanaged() -> Self {
        let (_, state) = watch::channel(State::default());
        Self {
            state,
            stopping: Arc::new(AtomicBool::new(true)),
        }
    }
}

pub(crate) struct Publisher(watch::Sender<State>);

pub(crate) struct Outcome {
    pub(crate) failures: u64,
    pub(crate) recovered_after: u64,
}

impl Publisher {
    /// Publish each operation independently, before the next one can suspend.
    /// Counter exhaustion remains unhealthy and terminates the worker rather
    /// than wrapping an outage counter into a reassuring small value.
    pub(crate) fn record(&mut self, operation: Operation, succeeded: bool) -> Result<Outcome, ()> {
        let mut state = *self.0.borrow();
        let pass = match operation {
            Operation::Reclaim => &mut state.reclaim,
            Operation::Rollover => &mut state.rollover,
        };
        let previous = pass.failures;
        pass.healthy = succeeded;
        let next = if succeeded {
            Some(0)
        } else {
            previous.checked_add(1)
        };
        pass.failures = next.unwrap_or(previous);
        self.0.send_replace(state);
        next.map(|failures| Outcome {
            failures,
            recovered_after: if succeeded { previous } else { 0 },
        })
        .ok_or(())
    }
}

#[derive(Clone)]
pub(crate) struct Stop {
    stopping: Arc<AtomicBool>,
    abort: AbortHandle,
}

impl Stop {
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.abort.abort();
    }

    pub(crate) fn requested(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }
}

pub(crate) struct Task {
    pub(crate) join: JoinHandle<()>,
    pub(crate) monitor: Monitor,
    pub(crate) stop: Stop,
}

impl Task {
    pub(crate) fn spawn<F, Fut>(run: F) -> Self
    where
        F: FnOnce(Publisher) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (publisher, state) = watch::channel(State::default());
        let stopping = Arc::new(AtomicBool::new(false));
        let join = tokio::spawn(run(Publisher(publisher)));
        let stop = Stop {
            stopping: Arc::clone(&stopping),
            abort: join.abort_handle(),
        };
        Self {
            join,
            monitor: Monitor { state, stopping },
            stop,
        }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        self.stop.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed(state: State) -> (Publisher, Monitor) {
        let (publisher, state) = watch::channel(state);
        (
            Publisher(publisher),
            Monitor {
                state,
                stopping: Arc::new(AtomicBool::new(false)),
            },
        )
    }

    #[test]
    fn readiness_matches_both_operation_outcomes_for_every_short_trace() {
        // An independent last-observation oracle, including startup and a
        // terminal stop followed by arbitrarily successful publications.
        for trace in 0_u32..4096 {
            let (mut publisher, monitor) = observed(State::default());
            let mut last = [None, None];
            let mut failures = [0, 0];
            assert!(!monitor.healthy());
            for shift in (0..12).step_by(2) {
                let event = (trace >> shift) & 3;
                let index = (event / 2) as usize;
                let succeeded = event % 2 == 0;
                let previous = failures[index];
                failures[index] = if succeeded { 0 } else { previous + 1 };
                last[index] = Some(succeeded);
                let operation = [Operation::Reclaim, Operation::Rollover][index];
                let report = publisher.record(operation, succeeded).unwrap();
                assert_eq!(report.failures, failures[index]);
                assert_eq!(report.recovered_after, if succeeded { previous } else { 0 });
                assert_eq!(monitor.healthy(), last == [Some(true), Some(true)]);
            }
            monitor.stopping.store(true, Ordering::Release);
            for operation in [Operation::Reclaim, Operation::Rollover] {
                publisher.record(operation, true).unwrap();
            }
            assert!(!monitor.healthy(), "late outcomes cannot undo stopping");
        }
    }

    #[test]
    fn closed_publication_cannot_preserve_a_healthy_last_value() {
        let (mut publisher, monitor) = observed(State::default());
        publisher.record(Operation::Reclaim, true).unwrap();
        publisher.record(Operation::Rollover, true).unwrap();
        assert!(monitor.healthy());
        drop(publisher);
        assert!(!monitor.healthy());
    }

    #[test]
    fn failure_counter_exhaustion_withdraws_health_without_wrapping() {
        for operation in [Operation::Reclaim, Operation::Rollover] {
            let (mut publisher, monitor) = observed(State {
                reclaim: Pass {
                    healthy: true,
                    failures: u64::MAX,
                },
                rollover: Pass {
                    healthy: true,
                    failures: u64::MAX,
                },
            });
            assert!(publisher.record(operation, false).is_err());
            assert!(!monitor.healthy());
            let state = *monitor.state.borrow();
            assert_eq!(state.reclaim.failures, u64::MAX);
            assert_eq!(state.rollover.failures, u64::MAX);
        }
    }

    #[tokio::test]
    async fn owner_drop_withdraws_readiness_before_abort_is_polled() {
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = Task::spawn(move |mut publisher| async move {
            publisher.record(Operation::Reclaim, true).unwrap();
            publisher.record(Operation::Rollover, true).unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(publisher);
        });
        started.await.unwrap();
        let monitor = task.monitor.clone();
        assert!(monitor.healthy());
        drop(task);
        assert!(!monitor.healthy());
        // No await separates owner drop and observation: channel closure
        // alone would still advertise healthy until the child is polled.
        assert!(monitor.state.has_changed().is_ok());
    }

    #[tokio::test]
    async fn cancellation_without_a_stop_request_closes_maintenance_health() {
        let (ready, started) = tokio::sync::oneshot::channel();
        let mut task = Task::spawn(move |mut publisher| async move {
            publisher.record(Operation::Reclaim, true).unwrap();
            publisher.record(Operation::Rollover, true).unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(publisher);
        });
        started.await.unwrap();
        assert!(task.monitor.healthy());
        task.join.abort();
        assert!((&mut task.join).await.unwrap_err().is_cancelled());
        assert!(!task.stop.requested());
        assert!(!task.monitor.healthy());
    }
}
