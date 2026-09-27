//! Scheduling evidence for GL-107; calendar and ledger rules remain store-owned.
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jiff::Timestamp;
use tokio::sync::Notify;
use tollgate_client::{
    ManualClock, PeriodRoller, PeriodRollerConfig, PeriodRollerHealth, PeriodRollerMonitor,
    PeriodRollerReport,
};
use tollgate_core::{AccountId, AccountStatus, BudgetSchedule, CapacityClass, CostUnits};
use tollgate_store::{AccountConfig, AdminStore, GrantPolicy, MemoryStore, StoreError};

#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::DelegatingStore;

fn t(value: &str) -> Timestamp {
    value.parse().unwrap()
}
fn january() -> Timestamp {
    t("2026-01-01T00:00:00Z")
}
fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}
fn config() -> PeriodRollerConfig {
    PeriodRollerConfig {
        poll_interval: secs(10),
        batch_limit: NonZeroUsize::new(2).unwrap(),
        call_timeout: secs(5),
        pass_timeout: secs(12),
        shutdown_timeout: secs(2),
    }
}
async fn store(count: u128) -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    for id in 1..=count {
        AdminStore::create_account(
            &*store,
            AccountConfig {
                account_id: AccountId(id),
                initial_balance: CostUnits(7),
                status: AccountStatus::Active,
                capacity_class: CapacityClass::Assured,
            },
        )
        .await
        .unwrap();
        AdminStore::set_budget_schedule(
            &*store,
            AccountId(id),
            Some(BudgetSchedule::monthly(CostUnits(100))),
        )
        .await
        .unwrap();
    }
    store
}
async fn observe(
    monitor: &mut PeriodRollerMonitor,
    predicate: impl Fn(PeriodRollerReport) -> bool,
) -> PeriodRollerReport {
    tokio::time::timeout(secs(120), async {
        loop {
            let report = monitor.report();
            if predicate(report) {
                return report;
            }
            monitor
                .changed()
                .await
                .expect("task exited before the expected observation");
        }
    })
    .await
    .expect("period roller made no expected progress")
}

async fn closed(monitor: &mut PeriodRollerMonitor) {
    tokio::time::timeout(secs(120), async {
        while monitor.changed().await.is_ok() {}
    })
    .await
    .expect("the owned task did not exit");
}

#[derive(Clone, Copy)]
enum Action {
    Normal,
    Fail,
    Hang,
    CommitThenHang,
    CommitThenFail,
    Delay(Duration),
    Advance(Timestamp),
    Panic,
}
/// The rollover script and everything the tests observe about it.
///
/// This used to implement `AdminStore` itself, and spent 47 of its 89 lines on
/// seven `unreachable!()` stubs for methods a period roller never calls. It is
/// now just the state; [`rolling`] pairs it with a store.
struct Scripted {
    clock: Arc<ManualClock>,
    actions: Mutex<VecDeque<Action>>,
    calls: Mutex<Vec<(Timestamp, usize)>>,
    notified: Notify,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl Scripted {
    /// The observation handle and the store to hand [`PeriodRoller::spawn`].
    fn new(
        store: Arc<MemoryStore>,
        clock: Arc<ManualClock>,
        actions: impl IntoIterator<Item = Action>,
    ) -> (Arc<Self>, Arc<DelegatingStore<MemoryStore>>) {
        let scripted = Arc::new(Self {
            clock,
            actions: Mutex::new(actions.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
            notified: Notify::new(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        });
        (Arc::clone(&scripted), rolling(store, &scripted))
    }

    fn calls(&self) -> Vec<(Timestamp, usize)> {
        self.calls.lock().unwrap().clone()
    }

    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(secs(120), async {
            while self.calls().len() < count {
                self.notified.notified().await;
            }
        })
        .await
        .unwrap();
    }
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A store whose `roll_due_periods` follows `scripted`, and whose every other
/// method is the wrapped store's.
fn rolling(store: Arc<MemoryStore>, scripted: &Arc<Scripted>) -> Arc<DelegatingStore<MemoryStore>> {
    let scripted = Arc::clone(scripted);
    Arc::new(
        DelegatingStore::wrapping(store).on_roll_due_periods(move |inner, now, limit| {
            let scripted = Arc::clone(&scripted);
            async move {
                let active = scripted.active.fetch_add(1, Ordering::SeqCst) + 1;
                scripted.max_active.fetch_max(active, Ordering::SeqCst);
                let _active = Active(&scripted.active);
                scripted.calls.lock().unwrap().push((now, limit.get()));
                scripted.notified.notify_one();
                let action = scripted
                    .actions
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Action::Normal);
                match action {
                    Action::Fail => return Err(StoreError("roll unavailable".into())),
                    Action::Hang => return std::future::pending().await,
                    Action::Delay(duration) => tokio::time::sleep(duration).await,
                    Action::Advance(to) => scripted.clock.set(to),
                    Action::Panic => panic!("injected rollover task failure"),
                    Action::Normal | Action::CommitThenHang | Action::CommitThenFail => {}
                }
                let batch = AdminStore::roll_due_periods(&*inner, now, limit).await?;
                match action {
                    Action::CommitThenHang => std::future::pending().await,
                    Action::CommitThenFail => Err(StoreError("reply lost after commit".into())),
                    _ => Ok(batch),
                }
            }
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn startup_drains_saturated_batches_without_waiting_for_a_tick() {
    let store = store(5).await;
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) = Scripted::new(store.clone(), clock.clone(), []);
    let started = tokio::time::Instant::now();
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    assert_eq!(monitor.report().health, PeriodRollerHealth::Starting);
    let report = observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    assert_eq!(tokio::time::Instant::now(), started);
    assert_eq!(scripted.calls(), vec![(january(), 2); 3]);
    assert_eq!(report.health, PeriodRollerHealth::Healthy);
    assert_eq!(report.last_successful_cutoff, Some(january()));
    assert_eq!(report.stats.passes_started, 1);
    assert_eq!(report.stats.batches, 3);
    assert_eq!(report.stats.accounts_rolled, 5);
    assert_eq!(report.stats.deposited_units, 500);
    assert_eq!(report.stats.expired_units, 0);
    assert_eq!(scripted.max_active.load(Ordering::SeqCst), 1);
    assert!(!roller.shutdown().await.deadline_expired);
    assert_eq!(monitor.report().health, PeriodRollerHealth::Stopped);
}

#[tokio::test(start_paused = true)]
async fn a_pass_freezes_its_cutoff_even_when_the_clock_crosses_another_boundary() {
    let store = store(5).await;
    let clock = Arc::new(ManualClock::new(january()));
    let march = t("2026-03-01T00:00:00Z");
    let (scripted, rolling) = Scripted::new(store.clone(), clock.clone(), [Action::Advance(march)]);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    assert_eq!(scripted.calls(), vec![(january(), 2); 3]);
    let report = observe(&mut monitor, |r| r.stats.passes_completed == 2).await;
    assert_eq!(scripted.calls()[3..], [(march, 2); 3]);
    assert_eq!(report.stats.accounts_rolled, 10);
    assert_eq!(
        report.stats.deposited_units, 1_000,
        "missed February does not accrue"
    );
    assert_eq!(report.stats.expired_units, 500);
    assert_eq!(
        store.balance(AccountId(1)),
        CostUnits(107),
        "top-ups survive"
    );
    assert!(store.conservation(AccountId(1)).unwrap().holds());
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unchanged_periods_and_removed_schedules_do_not_deposit_again() {
    let store = store(1).await;
    let clock = Arc::new(ManualClock::new(january()));
    let roller = PeriodRoller::spawn(store.clone(), clock.clone(), config()).unwrap();
    let mut monitor = roller.monitor();
    observe(&mut monitor, |r| r.stats.passes_completed == 2).await;
    assert_eq!(monitor.report().stats.accounts_rolled, 1);
    AdminStore::set_budget_schedule(&*store, AccountId(1), None)
        .await
        .unwrap();
    clock.set(t("2026-02-01T00:00:00Z"));
    let report = observe(&mut monitor, |r| r.stats.passes_completed == 3).await;
    assert_eq!(report.stats.accounts_rolled, 1);
    assert_eq!(report.stats.batches, 3);
    assert_eq!(store.balance(AccountId(1)), CostUnits(107));
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn the_exact_boundary_renews_one_allowance_and_preserves_topups() {
    let store = store(1).await;
    let clock = Arc::new(ManualClock::new(january()));
    let roller = PeriodRoller::spawn(store.clone(), clock.clone(), config()).unwrap();
    let mut monitor = roller.monitor();
    observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    clock.set(t("2026-01-31T23:59:59.999999999Z"));
    observe(&mut monitor, |r| r.stats.passes_completed == 2).await;
    assert_eq!(monitor.report().stats.accounts_rolled, 1);
    clock.set(t("2026-02-01T00:00:00Z"));
    let report = observe(&mut monitor, |r| r.stats.passes_completed == 3).await;
    assert_eq!(report.stats.deposited_units, 200);
    assert_eq!(report.stats.expired_units, 100);
    assert_eq!(store.balance(AccountId(1)), CostUnits(107));
    assert!(store.conservation(AccountId(1)).unwrap().holds());
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn racing_rollers_do_not_duplicate_an_allowance() {
    let store = store(9).await;
    let clock = Arc::new(ManualClock::new(january()));
    let a = PeriodRoller::spawn(store.clone(), clock.clone(), config()).unwrap();
    let b = PeriodRoller::spawn(store.clone(), clock, config()).unwrap();
    let mut am = a.monitor();
    let mut bm = b.monitor();
    observe(&mut am, |r| r.stats.passes_completed == 1).await;
    observe(&mut bm, |r| r.stats.passes_completed == 1).await;
    assert_eq!(
        am.report().stats.accounts_rolled + bm.report().stats.accounts_rolled,
        9
    );
    for id in 1..=9 {
        assert_eq!(store.balance(AccountId(id)), CostUnits(107));
        assert!(store.conservation(AccountId(id)).unwrap().holds());
    }
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failure_preserves_confirmed_progress_and_waits_before_retrying() {
    let store = store(5).await;
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) = Scripted::new(store, clock.clone(), [Action::Normal, Action::Fail]);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    let failed = observe(&mut monitor, |r| r.stats.failures == 1).await;
    assert_eq!(failed.health, PeriodRollerHealth::Degraded);
    assert_eq!(failed.last_successful_cutoff, None);
    assert_eq!(failed.stats.accounts_rolled, 2);
    assert_eq!(failed.stats.passes_incomplete, 1);
    assert_eq!(failed.stats.uncertain_calls, 1);
    tokio::time::advance(secs(9)).await;
    assert_eq!(scripted.calls().len(), 2);
    let recovered = observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    assert_eq!(recovered.health, PeriodRollerHealth::Healthy);
    assert_eq!(recovered.stats.passes_started, 2);
    assert_eq!(recovered.stats.accounts_rolled, 5);
    assert_eq!(recovered.stats.batches, 3);
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_hung_call_times_out_and_the_next_pass_recovers() {
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) = Scripted::new(store(1).await, clock.clone(), [Action::Hang]);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    let timed_out = observe(&mut monitor, |r| r.stats.call_timeouts == 1).await;
    assert_eq!(timed_out.stats.pass_timeouts, 0);
    assert_eq!(timed_out.stats.uncertain_calls, 1);
    assert_eq!(timed_out.stats.passes_incomplete, 1);
    assert_eq!(scripted.active.load(Ordering::SeqCst), 0);
    let report = observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    assert_eq!(report.stats.accounts_rolled, 1);
    assert_eq!(scripted.max_active.load(Ordering::SeqCst), 1);
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn one_pass_budget_bounds_every_batch_and_preserves_its_progress() {
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) =
        Scripted::new(store(8).await, clock.clone(), [Action::Delay(secs(4)); 4]);
    let started = tokio::time::Instant::now();
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    let report = observe(&mut monitor, |r| r.stats.pass_timeouts == 1).await;
    assert_eq!(tokio::time::Instant::now() - started, secs(12));
    assert_eq!(report.stats.call_timeouts, 0);
    assert_eq!(report.stats.passes_incomplete, 1);
    assert_eq!(report.stats.accounts_rolled, 6);
    assert_eq!(
        report.stats.uncertain_calls, 0,
        "budget ended between calls"
    );
    assert_eq!(scripted.calls().len(), 3);
    let recovered = observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
    assert_eq!(recovered.stats.accounts_rolled, 8);
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_pass_deadline_can_interrupt_a_call_before_its_own_timeout() {
    let clock = Arc::new(ManualClock::new(january()));
    let (_scripted, rolling) = Scripted::new(
        store(5).await,
        clock.clone(),
        [Action::Delay(secs(4)), Action::Hang],
    );
    let mut cfg = config();
    cfg.pass_timeout = secs(6);
    let started = tokio::time::Instant::now();
    let roller = PeriodRoller::spawn(rolling, clock, cfg).unwrap();
    let mut monitor = roller.monitor();
    let report = observe(&mut monitor, |r| r.stats.pass_timeouts == 1).await;
    assert_eq!(tokio::time::Instant::now() - started, secs(6));
    assert_eq!(report.stats.accounts_rolled, 2);
    assert_eq!(report.stats.uncertain_calls, 1);
    assert_eq!(report.stats.call_timeouts, 0);
    roller.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn uncertain_commits_remain_visible_after_recovery_and_shutdown() {
    for action in [Action::CommitThenHang, Action::CommitThenFail] {
        let store = store(1).await;
        let clock = Arc::new(ManualClock::new(january()));
        let (_scripted, rolling) = Scripted::new(store.clone(), clock.clone(), [action]);
        let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
        let mut monitor = roller.monitor();
        observe(&mut monitor, |r| r.stats.passes_incomplete == 1).await;
        let report = observe(&mut monitor, |r| r.stats.passes_completed == 1).await;
        assert_eq!(
            report.stats.accounts_rolled, 0,
            "no reply confirmed the committed work"
        );
        assert_eq!(report.stats.uncertain_calls, 1);
        assert_eq!(store.balance(AccountId(1)), CostUnits(107));
        assert_eq!(roller.shutdown().await.report.stats.uncertain_calls, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_a_hung_call_and_reports_its_uncertainty() {
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) =
        Scripted::new(store(1).await, clock.clone(), [Action::CommitThenHang]);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let monitor = roller.monitor();
    scripted.wait_calls(1).await;
    let started = tokio::time::Instant::now();
    let stopped = roller.shutdown().await;
    assert_eq!(tokio::time::Instant::now(), started);
    assert!(!stopped.deadline_expired);
    assert_eq!(stopped.report.health, PeriodRollerHealth::Stopped);
    assert_eq!(stopped.report.stats.uncertain_calls, 1);
    assert_eq!(stopped.report.stats.passes_incomplete, 1);
    assert_eq!(monitor.report(), stopped.report);
    assert_eq!(scripted.active.load(Ordering::SeqCst), 0);
    tokio::time::advance(secs(100)).await;
    assert_eq!(scripted.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn dropping_the_owner_or_an_unpolled_shutdown_aborts_the_task() {
    for cancel_shutdown in [false, true] {
        let clock = Arc::new(ManualClock::new(january()));
        let (scripted, rolling) = Scripted::new(store(1).await, clock.clone(), [Action::Hang]);
        let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
        let mut monitor = roller.monitor();
        scripted.wait_calls(1).await;
        if cancel_shutdown {
            drop(roller.shutdown());
        } else {
            drop(roller);
        }
        // Channel closure, not a sleep, witnesses task destruction.
        closed(&mut monitor).await;
        let report = monitor.report();
        assert_eq!(report.health, PeriodRollerHealth::Failed);
        assert_eq!(report.stats.uncertain_calls, 1);
        assert_eq!(report.stats.passes_incomplete, 1);
        assert_eq!(
            monitor.report(),
            report,
            "reads cannot double-count the interrupted call"
        );
        assert_eq!(scripted.active.load(Ordering::SeqCst), 0);
        tokio::time::advance(secs(100)).await;
        assert_eq!(scripted.calls().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_task_that_dies_after_a_healthy_pass_is_reported_failed() {
    let clock = Arc::new(ManualClock::new(january()));
    let (_scripted, rolling) = Scripted::new(
        store(1).await,
        clock.clone(),
        [Action::Normal, Action::Panic],
    );
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    observe(&mut monitor, |r| r.health == PeriodRollerHealth::Healthy).await;
    closed(&mut monitor).await;
    let report = monitor.report();
    assert_eq!(report.health, PeriodRollerHealth::Failed);
    assert_eq!(report.stats.passes_completed, 1);
    assert_eq!(report.stats.passes_incomplete, 1);
    assert_eq!(report.stats.uncertain_calls, 1);
    assert_eq!(report.last_successful_cutoff, Some(january()));
    assert_eq!(roller.shutdown().await.report, report);
}

#[test]
fn invalid_durations_are_rejected_before_a_task_can_start() {
    for value in [Duration::ZERO, Duration::MAX] {
        for field in 0..4 {
            let mut cfg = config();
            match field {
                0 => cfg.poll_interval = value,
                1 => cfg.call_timeout = value,
                2 => cfg.pass_timeout = value,
                _ => cfg.shutdown_timeout = value,
            }
            let error = cfg.validate().unwrap_err();
            assert!(!error.to_string().is_empty());
            // No runtime exists here: invalid configuration must be returned
            // before spawning, not panic in Tokio or start authoritative work.
            assert!(
                PeriodRoller::spawn(
                    MemoryStore::new(GrantPolicy::default()).unwrap(),
                    Arc::new(ManualClock::new(january())),
                    cfg,
                )
                .is_err()
            );
        }
    }
    PeriodRollerConfig::default().validate().unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_before_the_task_starts_makes_no_store_call() {
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) = Scripted::new(store(1).await, clock.clone(), []);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let report = roller.shutdown().await;
    assert_eq!(report.report.health, PeriodRollerHealth::Stopped);
    assert_eq!(report.report.stats.passes_started, 0);
    assert_eq!(report.report.stats.passes_incomplete, 0);
    assert_eq!(report.report.stats.uncertain_calls, 0);
    assert!(scripted.calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_polled_shutdown_keeps_ownership_of_the_task() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let clock = Arc::new(ManualClock::new(january()));
    let (scripted, rolling) = Scripted::new(store(1).await, clock.clone(), [Action::Hang]);
    let roller = PeriodRoller::spawn(rolling, clock, config()).unwrap();
    let mut monitor = roller.monitor();
    scripted.wait_calls(1).await;
    let mut shutdown = Box::pin(roller.shutdown());
    assert!(matches!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(shutdown);
    closed(&mut monitor).await;
    assert_eq!(monitor.report().health, PeriodRollerHealth::Failed);
    assert_eq!(monitor.report().stats.uncertain_calls, 1);
    assert_eq!(scripted.active.load(Ordering::SeqCst), 0);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn generated_period_and_failure_traces_preserve_funding_and_observations(
        events in proptest::collection::vec((1u32..=6, 0u8..4), 1..25)
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let store = store(1).await;
            let clock = Arc::new(ManualClock::new(january()));
            let (scripted, rolling) = Scripted::new(store.clone(), clock.clone(), []);
            let roller = PeriodRoller::spawn(rolling, clock.clone(), config()).unwrap();
            let mut monitor = roller.monitor();
            let mut period = 0;
            let mut deposits = 0;
            let mut confirmed = 0;
            let mut completed = 0;
            let mut failed = 0;
            let mut timeouts = 0;
            for (index, (month, outcome)) in events.into_iter().enumerate() {
                clock.set(t(&format!("2026-{month:02}-01T00:00:00Z")));
                let action = match outcome {
                    0 => { completed += 1; Action::Normal },
                    1 => { failed += 1; Action::Fail },
                    2 => { failed += 1; Action::CommitThenFail },
                    _ => { timeouts += 1; Action::Hang },
                };
                scripted.actions.lock().unwrap().push_back(action);
                if (outcome == 0 || outcome == 2) && month > period {
                    period = month;
                    deposits += 1;
                    if outcome == 0 { confirmed += 1; }
                }
                let report = observe(&mut monitor, |r| r.stats.passes_completed + r.stats.passes_incomplete == index as u64 + 1).await;
                assert_eq!(report.stats.passes_started, index as u64 + 1);
                assert_eq!(report.stats.passes_completed, completed);
                assert_eq!(report.stats.passes_incomplete, failed + timeouts);
                assert_eq!(report.stats.failures, failed);
                assert_eq!(report.stats.call_timeouts, timeouts);
                assert_eq!(report.stats.uncertain_calls, failed + timeouts);
                assert_eq!(report.stats.accounts_rolled, confirmed);
                assert_eq!(report.stats.deposited_units, u128::from(confirmed) * 100);
                assert_eq!(report.health, if outcome == 0 { PeriodRollerHealth::Healthy } else { PeriodRollerHealth::Degraded });
                let ledger = store.conservation(AccountId(1)).unwrap();
                assert_eq!(ledger.deposited, CostUnits(7 + deposits * 100));
                assert_eq!(ledger.expired, CostUnits(deposits.saturating_sub(1) * 100));
                assert!(ledger.holds());
            }
            let stopped = roller.shutdown().await;
            assert_eq!(stopped.report.stats.uncertain_calls, failed + timeouts);
            assert_eq!(scripted.max_active.load(Ordering::SeqCst), 1);
        });
    }
}
