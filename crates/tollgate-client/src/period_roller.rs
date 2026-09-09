//! Periodic allowance maintenance for direct-store embeddings (#107).
//!
//! The store owns calendar arithmetic, transactionality and idempotency. This
//! task owns only scheduling: one frozen cutoff per bounded pass, bounded
//! batches, and observable failure. It has no admission-path responsibilities.
//!
//! Start one beside `InstanceRuntime` when the application reaches an
//! `AdminStore` directly. HTTP-backed instances leave rollover to the server.
//! Multiple direct-store replicas may run it: the store arbitrates the boundary.
//! Retain its monitor in operational state and its owner in shutdown state:
//!
//! ```no_run
//! use std::sync::Arc;
//! use tollgate_client::{PeriodRoller, PeriodRollerConfig, PeriodRollerShutdownReport};
//! use tollgate_store::{AdminStore, Clock};
//!
//! # async fn maintain(store: Arc<dyn AdminStore>, clock: Arc<dyn Clock>,
//! #     stop: impl Future<Output = ()>)
//! #     -> Result<PeriodRollerShutdownReport, tollgate_client::PeriodRollerConfigError> {
//! let roller = PeriodRoller::spawn(store, clock, PeriodRollerConfig::default())?;
//! let monitor = roller.monitor(); // clone into readiness/metrics state
//! // monitor.report() includes task health and confirmed progress.
//! stop.await;
//! let shutdown = roller.shutdown().await;
//! # Ok(shutdown)
//! # }
//! ```
//!
//! Shutdown can run concurrently with admission-runtime shutdown: this manager
//! holds no leases or usage events. Its report does not replace the runtime's
//! accounting drain report. Polling starts immediately, so startup health stays
//! `Starting` until the first pass ends; admission readiness remains the
//! embedder's composition of its runtime and maintenance policy.
//!
//! Timeouts and aborts are cooperative, as with Tokio's other task deadlines:
//! an implementation must not block the executor while polling a store future.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tollgate_store::{AdminStore, Clock, DEFAULT_ROLLOVER_BATCH_LIMIT, RolloverBatch};

/// Cadence is measured from the end of a pass, so a slow pass or outage cannot
/// cause catch-up bursts. The first pass starts without waiting for this interval.
#[derive(Debug, Clone)]
pub struct PeriodRollerConfig {
    pub poll_interval: Duration,
    pub batch_limit: NonZeroUsize,
    pub call_timeout: Duration,
    /// One budget across all batches, including yields between them.
    pub pass_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for PeriodRollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            batch_limit: DEFAULT_ROLLOVER_BATCH_LIMIT,
            call_timeout: Duration::from_secs(5),
            pass_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeriodRollerConfigError(pub &'static str);
impl std::fmt::Display for PeriodRollerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for PeriodRollerConfigError {}

impl PeriodRollerConfig {
    pub fn validate(&self) -> Result<(), PeriodRollerConfigError> {
        for duration in [
            self.poll_interval,
            self.call_timeout,
            self.pass_timeout,
            self.shutdown_timeout,
        ] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(PeriodRollerConfigError(
                    "period roller durations must be positive and fit the monotonic clock",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodRollerHealth {
    Starting,
    /// The last pass reached a partial batch. This is maintenance health,
    /// not proof that every account is current (other replicas may hold locks).
    Healthy,
    Degraded,
    Stopped,
    Failed,
}

/// Confirmed observations, not a second ledger. An unanswered call may have
/// committed more work than these counters can report. Unit totals use u128
/// because a single batch can contain several full u64 allowances.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PeriodRollerStats {
    pub passes_started: u64,
    pub passes_completed: u64,
    pub passes_incomplete: u64,
    pub batches: u64,
    pub accounts_rolled: u64,
    pub deposited_units: u128,
    pub expired_units: u128,
    pub failures: u64,
    pub call_timeouts: u64,
    pub pass_timeouts: u64,
    /// Calls with unknown effects: store errors, timeouts, or interrupted calls.
    pub uncertain_calls: u64,
}

impl PeriodRollerStats {
    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            passes_started: self.passes_started.checked_add(other.passes_started)?,
            passes_completed: self.passes_completed.checked_add(other.passes_completed)?,
            passes_incomplete: self
                .passes_incomplete
                .checked_add(other.passes_incomplete)?,
            batches: self.batches.checked_add(other.batches)?,
            accounts_rolled: self.accounts_rolled.checked_add(other.accounts_rolled)?,
            deposited_units: self.deposited_units.checked_add(other.deposited_units)?,
            expired_units: self.expired_units.checked_add(other.expired_units)?,
            failures: self.failures.checked_add(other.failures)?,
            call_timeouts: self.call_timeouts.checked_add(other.call_timeouts)?,
            pass_timeouts: self.pass_timeouts.checked_add(other.pass_timeouts)?,
            uncertain_calls: self.uncertain_calls.checked_add(other.uncertain_calls)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeriodRollerReport {
    pub health: PeriodRollerHealth,
    pub stats: PeriodRollerStats,
    pub last_successful_cutoff: Option<Timestamp>,
    /// Totals are incomplete after overflow, never silently wrapped.
    pub counter_overflow: bool,
}

#[derive(Clone, Copy)]
struct State {
    report: PeriodRollerReport,
    in_pass: bool,
    pending: bool,
}

impl State {
    fn add(&mut self, delta: PeriodRollerStats) -> bool {
        if let Some(stats) = self.report.stats.checked_add(delta) {
            self.report.stats = stats;
            true
        } else {
            self.report.counter_overflow = true;
            self.report.health = PeriodRollerHealth::Degraded;
            tracing::error!("period roller progress counter overflowed");
            false
        }
    }

    fn interrupt(&mut self) {
        self.add(PeriodRollerStats {
            passes_incomplete: u64::from(self.in_pass),
            uncertain_calls: u64::from(self.pending),
            ..PeriodRollerStats::default()
        });
        self.in_pass = false;
        self.pending = false;
    }
}

/// A read-only observer that outlives the owner. Task liveness is read from
/// channel closure here, not left for each embedder to combine with a health bit.
#[derive(Clone)]
pub struct PeriodRollerMonitor {
    state: watch::Receiver<State>,
}

impl PeriodRollerMonitor {
    #[must_use]
    pub fn report(&self) -> PeriodRollerReport {
        // Check closure before reading: if closed, the last publication is final.
        let exited = self.state.has_changed().is_err();
        let mut state = *self.state.borrow();
        if exited && state.report.health != PeriodRollerHealth::Stopped {
            state.interrupt();
            state.report.health = PeriodRollerHealth::Failed;
        }
        state.report
    }

    /// Wait for progress or task exit. Read `report()` after either outcome.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.state.changed().await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeriodRollerShutdownReport {
    pub report: PeriodRollerReport,
    pub deadline_expired: bool,
}

/// Retain the owner and call `shutdown`; dropping it aborts the task.
#[must_use = "retain the period roller to own its background task"]
pub struct PeriodRoller {
    shutdown: watch::Sender<bool>,
    monitor: PeriodRollerMonitor,
    task: Option<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl PeriodRoller {
    pub fn spawn(
        store: Arc<dyn AdminStore>,
        clock: Arc<dyn Clock>,
        config: PeriodRollerConfig,
    ) -> Result<Self, PeriodRollerConfigError> {
        config.validate()?;
        let (shutdown, stopping) = watch::channel(false);
        let (progress, state) = watch::channel(State {
            report: PeriodRollerReport {
                health: PeriodRollerHealth::Starting,
                stats: PeriodRollerStats::default(),
                last_successful_cutoff: None,
                counter_overflow: false,
            },
            in_pass: false,
            pending: false,
        });
        let shutdown_timeout = config.shutdown_timeout;
        let task = tokio::spawn(run(store, clock, config, stopping, progress));
        Ok(Self {
            shutdown,
            monitor: PeriodRollerMonitor { state },
            task: Some(task),
            shutdown_timeout,
        })
    }

    #[must_use]
    pub fn monitor(&self) -> PeriodRollerMonitor {
        self.monitor.clone()
    }

    /// Stop new batches and interrupt an unanswered call. Dropping the store
    /// future is not proof of rollback; its effects remain explicitly uncertain.
    /// The backend's idempotent boundary guard makes a later retry safe.
    pub async fn shutdown(mut self) -> PeriodRollerShutdownReport {
        crate::signal(&self.shutdown, true, "period-roller shutdown");
        let task = self.task.as_mut().expect("period roller owns its task");
        let deadline_expired = match tokio::time::timeout(self.shutdown_timeout, task).await {
            Ok(Ok(())) => false,
            Ok(Err(error)) => {
                tracing::error!(%error, "period roller task died");
                false
            }
            Err(_) => {
                tracing::error!("period roller shutdown deadline expired");
                true
            }
        };
        let mut report = self.monitor.report();
        if deadline_expired {
            // The last state still owns any pending call until abort is polled.
            // Report that uncertainty immediately, without an unbounded join.
            let mut state = *self.monitor.state.borrow();
            state.interrupt();
            report = state.report;
            report.health = PeriodRollerHealth::Failed;
        }
        PeriodRollerShutdownReport {
            report,
            deadline_expired,
        }
    }
}

impl Drop for PeriodRoller {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct Progress {
    state: State,
    publisher: watch::Sender<State>,
}

impl Progress {
    fn publish(&self) {
        self.publisher.send_replace(self.state);
    }

    fn batch(&mut self, batch: &RolloverBatch) -> bool {
        // No per-account retained history: only the current bounded batch.
        let delta = batch.rolled().iter().try_fold(
            PeriodRollerStats {
                batches: 1,
                ..PeriodRollerStats::default()
            },
            |mut sum, account| {
                sum.accounts_rolled = sum.accounts_rolled.checked_add(1)?;
                sum.deposited_units = sum
                    .deposited_units
                    .checked_add(u128::from(account.deposited.get()))?;
                sum.expired_units = sum
                    .expired_units
                    .checked_add(u128::from(account.expired.get()))?;
                Some(sum)
            },
        );
        match delta {
            Some(delta) => self.state.add(delta),
            None => {
                self.state.report.counter_overflow = true;
                self.state.report.health = PeriodRollerHealth::Degraded;
                tracing::error!("period roller batch totals overflowed");
                false
            }
        }
    }
}

enum PassEnd {
    Complete,
    Failed,
    Stop,
}

async fn pass(
    store: &dyn AdminStore,
    cutoff: Timestamp,
    config: &PeriodRollerConfig,
    stopping: &mut watch::Receiver<bool>,
    progress: &mut Progress,
) -> PassEnd {
    let deadline = Instant::now() + config.pass_timeout;
    progress.state.in_pass = true;
    if !progress.state.add(PeriodRollerStats {
        passes_started: 1,
        ..PeriodRollerStats::default()
    }) {
        return PassEnd::Failed;
    }
    loop {
        if *stopping.borrow() {
            return PassEnd::Stop;
        }
        let now = Instant::now();
        if now >= deadline {
            progress.state.add(PeriodRollerStats {
                pass_timeouts: 1,
                ..PeriodRollerStats::default()
            });
            return PassEnd::Failed;
        }
        let call_deadline = (now + config.call_timeout).min(deadline);
        progress.state.pending = true;
        progress.publish();
        let result = tokio::select! {
            biased;
            _ = stopping.changed() => return PassEnd::Stop,
            result = tokio::time::timeout_at(call_deadline, store.roll_due_periods(cutoff, config.batch_limit)) => result,
        };
        progress.state.pending = false;
        match result {
            Ok(Ok(batch)) => {
                if !progress.batch(&batch) {
                    return PassEnd::Failed;
                }
                if !batch.is_saturated() {
                    return PassEnd::Complete;
                }
                progress.publish();
                tokio::task::yield_now().await;
            }
            Ok(Err(error)) => {
                progress.state.add(PeriodRollerStats {
                    failures: 1,
                    uncertain_calls: 1,
                    ..PeriodRollerStats::default()
                });
                tracing::warn!(%error, %cutoff, "period rollover failed; confirmed batches remain committed, this call has unknown effects");
                return PassEnd::Failed;
            }
            Err(_) => {
                let pass_expired = call_deadline == deadline;
                progress.state.add(PeriodRollerStats {
                    call_timeouts: u64::from(!pass_expired),
                    pass_timeouts: u64::from(pass_expired),
                    uncertain_calls: 1,
                    ..PeriodRollerStats::default()
                });
                tracing::warn!(%cutoff, pass_expired, "period rollover timed out; the unanswered batch may have committed");
                return PassEnd::Failed;
            }
        }
    }
}

async fn run(
    store: Arc<dyn AdminStore>,
    clock: Arc<dyn Clock>,
    config: PeriodRollerConfig,
    mut stopping: watch::Receiver<bool>,
    publisher: watch::Sender<State>,
) {
    let state = *publisher.borrow();
    let mut progress = Progress { state, publisher };
    while !*stopping.borrow() {
        let cutoff = clock.now();
        match pass(
            store.as_ref(),
            cutoff,
            &config,
            &mut stopping,
            &mut progress,
        )
        .await
        {
            PassEnd::Stop => break,
            PassEnd::Complete => {
                progress.state.in_pass = false;
                if progress.state.add(PeriodRollerStats {
                    passes_completed: 1,
                    ..PeriodRollerStats::default()
                }) {
                    progress.state.report.health = PeriodRollerHealth::Healthy;
                    progress.state.report.last_successful_cutoff = Some(cutoff);
                }
            }
            PassEnd::Failed => {
                progress.state.interrupt();
                progress.state.report.health = PeriodRollerHealth::Degraded;
            }
        }
        progress.publish();
        if progress.state.report.counter_overflow {
            break;
        }
        tokio::select! {
            biased;
            _ = stopping.changed() => break,
            () = tokio::time::sleep(config.poll_interval) => {}
        }
    }
    progress.state.interrupt();
    progress.state.report.health = PeriodRollerHealth::Stopped;
    progress.publish();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        State {
            report: PeriodRollerReport {
                health: PeriodRollerHealth::Starting,
                stats: PeriodRollerStats::default(),
                last_successful_cutoff: None,
                counter_overflow: false,
            },
            in_pass: false,
            pending: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_monitor_waits_for_progress_and_detects_channel_closure() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let (publisher, receiver) = watch::channel(state());
        let mut monitor = PeriodRollerMonitor { state: receiver };
        let mut changed = Box::pin(monitor.changed());
        assert!(matches!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(changed);
        publisher.send_modify(|s| s.report.health = PeriodRollerHealth::Healthy);
        monitor.changed().await.unwrap();
        assert_eq!(monitor.report().health, PeriodRollerHealth::Healthy);
        drop(publisher);
        assert!(monitor.changed().await.is_err());
        assert_eq!(monitor.report().health, PeriodRollerHealth::Failed);
        assert_eq!(monitor.report().stats.uncertain_calls, 0);
    }

    #[test]
    fn counter_overflow_preserves_the_last_known_totals_and_reports_incompleteness() {
        // Every cumulative counter has the same external contract, including
        // the wider unit totals. The table covers each field's domain boundary.
        for field in 0..11 {
            let mut state = state();
            let mut delta = PeriodRollerStats::default();
            macro_rules! fill {
                ($field:ident, $max:expr) => {{
                    state.report.stats.$field = $max;
                    delta.$field = 1;
                }};
            }
            match field {
                0 => fill!(passes_started, u64::MAX),
                1 => fill!(passes_completed, u64::MAX),
                2 => fill!(passes_incomplete, u64::MAX),
                3 => fill!(batches, u64::MAX),
                4 => fill!(accounts_rolled, u64::MAX),
                5 => fill!(deposited_units, u128::MAX),
                6 => fill!(expired_units, u128::MAX),
                7 => fill!(failures, u64::MAX),
                8 => fill!(call_timeouts, u64::MAX),
                9 => fill!(pass_timeouts, u64::MAX),
                _ => fill!(uncertain_calls, u64::MAX),
            }
            let before = state.report.stats;
            assert!(!state.add(delta));
            assert_eq!(state.report.stats, before);
            assert!(state.report.counter_overflow);
            assert_eq!(state.report.health, PeriodRollerHealth::Degraded);
        }
    }

    #[test]
    fn one_batch_can_report_more_than_u64_units() {
        use tollgate_core::{AccountId, CostUnits};
        use tollgate_store::RolledAccount;
        let state = state();
        let (publisher, _) = watch::channel(state);
        let mut progress = Progress { state, publisher };
        let batch = RolloverBatch::try_new(
            vec![
                RolledAccount {
                    account_id: AccountId(1),
                    deposited: CostUnits(u64::MAX),
                    expired: CostUnits(u64::MAX),
                },
                RolledAccount {
                    account_id: AccountId(2),
                    deposited: CostUnits(u64::MAX),
                    expired: CostUnits(u64::MAX),
                },
            ],
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap();
        assert!(progress.batch(&batch));
        assert_eq!(progress.state.report.stats.accounts_rolled, 2);
        assert_eq!(
            progress.state.report.stats.deposited_units,
            u128::from(u64::MAX) * 2
        );
        assert_eq!(
            progress.state.report.stats.expired_units,
            u128::from(u64::MAX) * 2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_has_a_total_deadline_even_if_its_task_does_not_observe_the_signal() {
        let mut state = state();
        state.in_pass = true;
        state.pending = true;
        state.report.stats.passes_started = 1;
        let (publisher, receiver) = watch::channel(state);
        let (shutdown, _stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _publisher = publisher;
            std::future::pending::<()>().await;
        });
        let mut monitor = PeriodRollerMonitor { state: receiver };
        let roller = PeriodRoller {
            shutdown,
            monitor: monitor.clone(),
            task: Some(task),
            shutdown_timeout: Duration::from_secs(3),
        };
        let started = Instant::now();
        let stopped = roller.shutdown().await;
        assert_eq!(Instant::now() - started, Duration::from_secs(3));
        assert!(stopped.deadline_expired);
        assert_eq!(stopped.report.health, PeriodRollerHealth::Failed);
        assert_eq!(stopped.report.stats.uncertain_calls, 1);
        assert_eq!(stopped.report.stats.passes_incomplete, 1);
        tokio::time::timeout(Duration::from_secs(10), async {
            while monitor.changed().await.is_ok() {}
        })
        .await
        .expect("deadline expiry must abort the owned task");
        assert_eq!(monitor.report(), stopped.report);
    }
}
