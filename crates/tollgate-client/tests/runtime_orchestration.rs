use jiff::{SignedDuration, Timestamp};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tollgate_admission::NoGate;
use tollgate_client::{
    AccountLeaseConfig, AccountPhase, InstanceRuntime, InstanceRuntimeConfig, ManualClock,
    RuntimeHandle, SnapshotManagerConfig, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits,
    EnforcementMode, Generation, OpIndex, PermissionBits, Principal, PublishableSnapshot,
    RequestId,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
#[path = "../../tollgate-core/tests/support/isolated.rs"]
mod isolated;
use delegating::{DelegatingStore, RejectingStore, rejecting};

use tollgate_store::{AccountConfig, AllocateError, GrantPolicy, LeaseAllocator, MemoryStore};

#[derive(Clone, Copy)]
struct Op;
impl OpIndex for Op {
    fn index(&self) -> usize {
        0
    }
}
fn t(n: i64) -> Timestamp {
    Timestamp::from_second(n).unwrap()
}
fn config() -> InstanceRuntimeConfig {
    InstanceRuntimeConfig {
        snapshot_history_capacity:
            tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
        snapshots: SnapshotManagerConfig {
            principals: TrackedPrincipals::All { seed: vec![] },
            refresh_interval: Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(1),
            revoked_ttl: SignedDuration::from_secs(1),
            retry_backoff: Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: Duration::from_millis(100),
            enumeration_timeout: Duration::from_millis(100),
        },
        leases: AccountLeaseConfig {
            target_grant: CostUnits(500),
            low_water: CostUnits(20),
            lease_ttl: SignedDuration::from_secs(300),
            expiry_safety_margin: SignedDuration::from_secs(2),
            poll_interval: Duration::from_millis(5),
            store_call_timeout: Duration::from_millis(100),
            shutdown_release_deadline: Duration::from_millis(100),
        },
        usage: UsageWriterConfig {
            queue_capacity: 32,
            max_batch: 8,
            flush_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            shutdown_drain_deadline: Duration::from_millis(100),
            ingest_timeout: Duration::from_millis(100),
        },
        sharding: tollgate_core::LocalSharding::SINGLE,
        idle_account_linger: Duration::from_millis(30),
        manager_restart_backoff: Duration::from_millis(20),
        shutdown_deadline: Duration::from_millis(250),
    }
}
fn store() -> Arc<MemoryStore> {
    MemoryStore::new(GrantPolicy::default()).unwrap()
}
fn account(store: &MemoryStore, id: u128, balance: u64) {
    store.create_account(AccountConfig {
        account_id: AccountId(id),
        initial_balance: CostUnits(balance),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
}
fn publish(
    store: &MemoryStore,
    principal: u128,
    account: u128,
    generation: u64,
    mode: EnforcementMode,
) {
    publish_until(store, principal, account, generation, mode, t(10_000));
}
fn publish_until(
    store: &MemoryStore,
    principal: u128,
    account: u128,
    generation: u64,
    mode: EnforcementMode,
    until: Timestamp,
) {
    let snapshot = AccountSnapshot::builder(
        AccountId(account),
        Generation(generation),
        AccountStatus::Active,
        until,
        PermissionBits::bit(0),
        tollgate_core::ResolvedLimits::new(
            mode.overage_cap().map_or(100, |cap| cap.get().min(100)),
        ),
        Arc::new(
            CostTable::builder(CostUnits(0), CostUnits(0))
                .weight(&Op, CostUnits(1))
                .build(),
        ),
    )
    .enforcement_mode(mode)
    .build();
    store
        .publish_snapshot(
            Principal(principal),
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
        )
        .expect("snapshot fixture matches its account and credential");
}
fn start(
    store: &Arc<MemoryStore>,
    config: InstanceRuntimeConfig,
) -> (InstanceRuntime, RuntimeHandle) {
    InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config,
    )
    .unwrap()
}
async fn wait(mut condition: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(condition(), "runtime did not reach the expected state");
}
fn charge(handle: &RuntimeHandle, principal: u128, id: u128, units: u64) {
    let now = t(100);
    let context = handle
        .begin(Principal(principal), PermissionBits::bit(0), now)
        .unwrap();
    let pending = context
        .admit(
            &[(Op, units)],
            handle.recorder().try_reserve().unwrap(),
            now,
        )
        .unwrap();
    drop(
        pending
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(id), now)
            .unwrap(),
    );
}

#[tokio::test(start_paused = true)]
async fn a_discovered_account_is_funded_without_restarting_the_instance() {
    let store = store();
    let mut cfg = config();
    cfg.sharding = tollgate_core::LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    wait(|| handle.report().managed_accounts == 1 && handle.readiness(t(100)).is_ready()).await;
    charge(&handle, 11, 1, 7);
    account(&store, 2, 1_000);
    publish(&store, 22, 2, 1, EnforcementMode::Strict);
    wait(|| {
        handle.report().managed_accounts == 2
            && handle.account_reports(t(100)).iter().all(|a| a.fundable)
    })
    .await;
    assert_eq!(handle.funding(t(100)).total_lease_remaining, Some(993));
    assert_eq!(
        handle.funding(t(100)).earliest_lease_usable_until,
        Some(t(398))
    );
    charge(&handle, 22, 2, 9);
    let report = runtime.shutdown().await.unwrap();
    assert_eq!(report.usage.unwrap().accepted, 2);
    assert_eq!(report.accounts.len(), 2);
    assert!(!report.deadline_expired);
    assert_eq!(handle.report().uncertain_acquires, 0);
    assert!(!handle.report().counter_overflow);
    assert_eq!(store.balance(AccountId(1)), CostUnits(993));
    assert_eq!(store.balance(AccountId(2)), CostUnits(991));
    assert!(!handle.readiness(t(100)).is_ready());
}

#[tokio::test(start_paused = true)]
async fn sibling_principals_share_one_manager_and_refresh_does_not_restart_it() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    publish(&store, 12, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    for generation in 2..20 {
        publish(&store, 11, 1, generation, EnforcementMode::Strict);
    }
    store.remove_snapshot(Principal(12));
    wait(|| {
        handle
            .begin(Principal(12), PermissionBits::bit(0), t(100))
            .is_err()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(handle.report().managed_accounts, 1);
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    assert_eq!(handle.report().manager_restarts, 0);
    charge(&handle, 11, 1, 5);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
}

#[tokio::test(start_paused = true)]
async fn reactivation_during_linger_reuses_the_manager_and_later_reactivation_releases_then_refills()
 {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    store.remove_snapshot(Principal(11));
    wait(|| handle.report().lingering_accounts == 1).await;
    assert_eq!(handle.report().managed_accounts, 1);
    publish(&store, 11, 1, 3, EnforcementMode::Strict);
    wait(|| handle.report().lingering_accounts == 0 && handle.report().managed_accounts == 1).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    store.remove_snapshot(Principal(11));
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    assert_eq!(handle.report().retained_accounts, 1);
    publish(&store, 11, 1, 5, EnforcementMode::Strict);
    wait(|| handle.readiness(t(100)).is_ready() && handle.report().managed_accounts == 1).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 2);
    assert_eq!(handle.report().refill.unwrap().released, 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn retiring_an_elastic_account_does_not_reset_its_spend_cap() {
    let store = store();
    account(&store, 1, 0);
    let mode = EnforcementMode::Elastic {
        overage_cap: CostUnits(5),
    };
    publish(&store, 11, 1, 1, mode);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    wait(|| {
        handle.report().refill.is_some_and(|stats| {
            stats.acquire_refused[AllocateError::BalanceExhausted(
                tollgate_core::BalanceExhaustion { period_end: None },
            )
            .index()]
                > 0
        })
    })
    .await;
    charge(&handle, 11, 1, 3);
    store.remove_snapshot(Principal(11));
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    publish(&store, 11, 1, 3, mode);
    wait(|| handle.report().managed_accounts == 1 && handle.readiness(t(100)).is_ready()).await;
    let context = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap();
    assert!(matches!(
        context.admit(&[(Op, 3)], handle.recorder().try_reserve().unwrap(), t(100)),
        Err(tollgate_core::DenyReason::BalanceExhausted)
    ));
    charge(&handle, 11, 2, 2);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 2);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(5));
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_account_withdraws_a_fixed_instance_but_not_a_discovering_one() {
    for all in [true, false] {
        let store = store();
        account(&store, 1, 1_000);
        account(&store, 2, 0);
        publish(&store, 11, 1, 1, EnforcementMode::Strict);
        publish(&store, 22, 2, 1, EnforcementMode::Strict);
        let mut cfg = config();
        if !all {
            cfg.snapshots.principals = TrackedPrincipals::Fixed(vec![Principal(11), Principal(22)]);
        }
        let (runtime, handle) = start(&store, cfg);
        wait(|| {
            handle.report().managed_accounts == 2 && handle.report().refill.unwrap().acquired == 1
        })
        .await;
        let ready = handle.readiness(t(100));
        assert_eq!(ready.is_ready(), all);
        assert_eq!(ready.unfundable_accounts, 1);
        store.remove_snapshot(Principal(11));
        wait(|| handle.readiness(t(100)).eligible_accounts == 1).await;
        assert!(
            !handle.readiness(t(100)).is_ready(),
            "the remaining account cannot fund any work"
        );
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_committed_usage_before_returning_the_grant() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let context = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap();
    let pending = context
        .admit(&[(Op, 9)], handle.recorder().try_reserve().unwrap(), t(100))
        .unwrap();
    let committed = pending
        .acquire_capacity(&NoGate)
        .unwrap()
        .commit(RequestId(1), t(100))
        .unwrap();
    let shutdown = tokio::spawn(runtime.shutdown());
    wait(|| handle.recorder().is_closed()).await;
    assert!(!shutdown.is_finished());
    assert_eq!(store.balance(AccountId(1)), CostUnits(500));
    assert!(handle.recorder().try_reserve().is_err());
    drop(committed);
    let report = shutdown.await.unwrap().unwrap();
    assert_eq!(report.usage.unwrap().accepted, 1);
    assert!(report.unfinished_accounts.is_empty());
    assert_eq!(store.balance(AccountId(1)), CostUnits(991));
}

struct ReleaseBlock {
    started: AtomicBool,
    resume: tokio::sync::Notify,
}

/// The injections the allocator fixtures vary, shared with the test.
#[derive(Default)]
struct Scripted {
    panic: AtomicBool,
    invalid_release: bool,
    release_block: Option<Arc<ReleaseBlock>>,
}

impl Scripted {
    /// The handle and the store to hand [`InstanceRuntime::spawn`].
    fn spawn(self, store: &Arc<MemoryStore>) -> (Arc<Self>, Arc<DelegatingStore<MemoryStore>>) {
        let scripted = Arc::new(self);
        (Arc::clone(&scripted), scripted_allocator(store, &scripted))
    }

    fn panicking() -> Self {
        Self {
            panic: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn refusing_release() -> Self {
        Self {
            invalid_release: true,
            ..Self::default()
        }
    }

    fn blocking(release_block: Option<Arc<ReleaseBlock>>) -> Self {
        Self {
            release_block,
            ..Self::default()
        }
    }
}

fn scripted_allocator(
    store: &Arc<MemoryStore>,
    scripted: &Arc<Scripted>,
) -> Arc<DelegatingStore<MemoryStore>> {
    let (acquire, release, consolidate) = (
        Arc::clone(scripted),
        Arc::clone(scripted),
        Arc::clone(scripted),
    );
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store))
            .on_acquire(move |inner, account, units, ttl, now| {
                let scripted = Arc::clone(&acquire);
                async move {
                    assert!(
                        !scripted.panic.swap(false, Ordering::SeqCst),
                        "injected allocator panic"
                    );
                    inner.acquire(account, units, ttl, now).await
                }
            })
            .on_release(move |inner, lease, fence, remaining, now| {
                let scripted = Arc::clone(&release);
                async move {
                    if let Some(block) = &scripted.release_block {
                        block.started.store(true, Ordering::Release);
                        block.resume.notified().await;
                    }
                    if scripted.invalid_release {
                        return Err(AllocateError::InvalidRelease);
                    }
                    inner.release(lease, fence, remaining, now).await
                }
            })
            // Subject to the same injected `invalid_release`: a consolidation
            // is a release and an acquire, so a fixture that refuses one half
            // must refuse it here too.
            .on_consolidate(move |inner, lease, fence, unspent, requested, ttl, now| {
                let scripted = Arc::clone(&consolidate);
                async move {
                    if scripted.invalid_release {
                        return Err(AllocateError::InvalidRelease);
                    }
                    inner
                        .consolidate(lease, fence, unspent, requested, ttl, now)
                        .await
                }
            }),
    )
}

#[tokio::test(start_paused = true)]
async fn a_dead_manager_is_joined_then_restarted_with_backoff() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (_scripted, allocator) = Scripted::panicking().spawn(&store);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator,
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.report().restarting_accounts == 1).await;
    assert!(!handle.readiness(t(100)).is_ready());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    assert_eq!(handle.report().manager_restarts, 1);
    assert_eq!(handle.report().uncertain_acquires, 1);
    charge(&handle, 11, 1, 4);
    runtime.shutdown().await.unwrap();
    assert_eq!(store.balance(AccountId(1)), CostUnits(996));
}

#[tokio::test(start_paused = true)]
async fn shutdown_with_an_unresolved_permit_still_obeys_the_total_deadline() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let cfg = config();
    let budget = cfg.shutdown_deadline;
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let permit = handle.recorder().try_reserve().unwrap();
    let began = tokio::time::Instant::now();
    let report = runtime.shutdown().await.unwrap();
    assert!(began.elapsed() <= budget);
    assert_eq!(report.usage.unwrap().unresolved, 1);
    drop(permit);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
}

#[test]
fn a_shutdown_deadline_shorter_than_its_phases_is_rejected() {
    let mut cfg = config();
    cfg.shutdown_deadline = Duration::from_millis(199);
    assert!(cfg.validate().is_err());
    cfg.shutdown_deadline = Duration::from_millis(200);
    assert!(cfg.validate().is_ok());
    cfg.manager_restart_backoff = Duration::ZERO;
    assert!(cfg.validate().is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn arbitrary_catalogue_churn_retires_each_owner_once(
        events in proptest::collection::vec((0usize..8, proptest::bool::ANY), 1..65)
    ) {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
            tokio::time::pause();
            let store = store();
            for id in 1..=4 { account(&store, id, 1_000); }
            let (runtime, handle) = start(&store, config());
            let mut live = [false; 8];
            for (index, (principal, present)) in events.into_iter().enumerate() {
                live[principal] = present;
                if present {
                    publish(&store, principal as u128 + 10, principal as u128 / 2 + 1,
                        index as u64 * 2 + 100, EnforcementMode::Strict);
                } else {
                    store.remove_snapshot(Principal(principal as u128 + 10));
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
                let expected = live.chunks(2).filter(|pair| pair.iter().any(|&v| v)).count();
                let report = handle.report();
                assert_eq!(report.managed_accounts, expected);
                assert_eq!(report.retiring_accounts, 0);
                let counters = report.refill.unwrap();
                assert_eq!(counters.acquired - counters.released, expected as u64);
                assert_eq!(counters.abandoned, 0);
                assert_eq!(report.manager_restarts, 0);
            }
            let report = runtime.shutdown().await.unwrap();
            assert!(report.unfinished_accounts.is_empty());
            for id in 1..=4 { assert_eq!(store.balance(AccountId(id)), CostUnits(1_000)); }
            let counters = handle.report().refill.unwrap();
            assert_eq!(counters.acquired, counters.released);
        });
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_checks_freshness_at_the_callers_time_before_a_background_wakeup() {
    for mode in [
        EnforcementMode::Strict,
        EnforcementMode::Elastic {
            overage_cap: CostUnits(100),
        },
    ] {
        let store = store();
        account(&store, 1, 1_000);
        publish(&store, 11, 1, 1, mode);
        let (runtime, handle) = start(&store, config());
        wait(|| handle.readiness(t(100)).is_ready()).await;
        let expired = handle.readiness(t(10_000));
        assert!(!expired.is_ready());
        assert!(!expired.snapshots_ready);
        assert_eq!(expired.eligible_accounts, 0);
        assert_eq!(expired.unresolved_principals, 1);
        assert!(!handle.account_reports(t(10_000))[0].eligible);
        assert!(!handle.account_reports(t(10_000))[0].fundable);
        assert_eq!(handle.funding(t(10_000)).total_overage_cap, None);
        assert_eq!(handle.funding(t(10_000)).earliest_lease_usable_until, None);
        runtime.shutdown().await.unwrap();
    }
}

/// A source whose pulls can be made to fail. `subscribe` and `principals`
/// need no mention: delegation supplies both, and `principals` in particular
/// must not fall back to the trait default (#83).
fn failing_snapshots(
    store: &Arc<MemoryStore>,
    fail: &Arc<AtomicBool>,
) -> Arc<DelegatingStore<MemoryStore>> {
    let fail = Arc::clone(fail);
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store)).on_snapshot(move |inner, principal| {
            let fail = Arc::clone(&fail);
            async move {
                if fail.load(Ordering::Acquire) {
                    return Err(tollgate_store::StoreError("test outage".into()));
                }
                tollgate_store::SnapshotSource::snapshot(&*inner, principal).await
            }
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn a_refresh_outage_preserves_a_fresh_accounts_manager() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let fail = Arc::new(AtomicBool::new(false));
    let source = failing_snapshots(&store, &fail);
    let (runtime, handle) = InstanceRuntime::spawn(
        source.clone(),
        store.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    fail.store(true, Ordering::Release);
    wait(|| handle.report().snapshots.refresh_failures > 0).await;
    assert!(handle.readiness(t(100)).is_ready());
    assert_eq!(handle.report().managed_accounts, 1);
    assert_eq!(handle.report().manager_restarts, 0);
    charge(&handle, 11, 1, 1);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
}

/// A source with a controllable catalogue and, deliberately, no pushes: the
/// dead receiver is what isolates the enumeration path this test measures, so
/// `subscribe` is overridden rather than delegated.
fn catalogue_source(
    store: &Arc<MemoryStore>,
    visible: &Arc<AtomicBool>,
) -> Arc<DelegatingStore<MemoryStore>> {
    let visible = Arc::clone(visible);
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store))
            .on_subscribe(|_| {
                let (_, rx) = tokio::sync::broadcast::channel(1);
                rx
            })
            .on_principals(move |_| {
                let visible = Arc::clone(&visible);
                async move {
                    Ok(Some(if visible.load(Ordering::Acquire) {
                        vec![Principal(11)]
                    } else {
                        vec![]
                    }))
                }
            }),
    )
}

#[tokio::test(start_paused = true)]
async fn catalogue_removal_withdraws_a_still_fresh_map_entry_and_can_restore_it() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let visible = Arc::new(AtomicBool::new(true));
    let source = catalogue_source(&store, &visible);
    let (runtime, handle) = InstanceRuntime::spawn(
        source.clone(),
        store.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready() && handle.report().managed_accounts == 1).await;
    visible.store(false, Ordering::Release);
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    assert!(
        handle
            .begin(Principal(11), PermissionBits::bit(0), t(100))
            .is_err()
    );
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    visible.store(true, Ordering::Release);
    wait(|| handle.report().managed_accounts == 1 && handle.readiness(t(100)).is_ready()).await;
    charge(&handle, 11, 1, 3);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
}

#[tokio::test(start_paused = true)]
async fn retiring_an_idle_account_does_not_withdraw_another_funded_account() {
    let store = store();
    for id in 1..=2 {
        account(&store, id, 1_000);
        publish(&store, id * 11, id, 1, EnforcementMode::Strict);
    }
    let (runtime, handle) = start(&store, config());
    wait(|| {
        handle.report().managed_accounts == 2
            && handle.account_reports(t(100)).iter().all(|a| a.fundable)
    })
    .await;
    let pending = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap()
        .admit(&[(Op, 1)], handle.recorder().try_reserve().unwrap(), t(100))
        .unwrap();
    store.remove_snapshot(Principal(11));
    wait(|| handle.report().retiring_accounts == 1).await;
    assert!(handle.readiness(t(100)).is_ready());
    drop(pending);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn suspension_retires_funding_and_reinstatement_starts_it_again() {
    use tollgate_store::AdminStore;
    let store = store();
    account(&store, 1, 1_000);
    publish(
        &store,
        11,
        1,
        1,
        EnforcementMode::Elastic {
            overage_cap: CostUnits(100),
        },
    );
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    store
        .set_account_status(AccountId(1), AccountStatus::Suspended)
        .await
        .unwrap();
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Lingering).await;
    assert!(handle.funding(t(100)).total_lease_remaining.is_some());
    assert_eq!(handle.funding(t(100)).total_overage_cap, None);
    assert_eq!(handle.funding(t(100)).earliest_lease_usable_until, None);
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    assert!(!handle.readiness(t(100)).is_ready());
    assert!(
        handle
            .begin(Principal(11), PermissionBits::bit(0), t(100))
            .is_err()
    );
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    store
        .set_account_status(AccountId(1), AccountStatus::Active)
        .await
        .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    charge(&handle, 11, 1, 3);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_racing_onboarding_joins_every_started_manager() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let publishing = {
        let store = store.clone();
        tokio::spawn(async move {
            for id in 2..=32 {
                account(&store, id, 1_000);
                publish(&store, id + 10, id, 1, EnforcementMode::Strict);
                tokio::task::yield_now().await;
            }
        })
    };
    let report = runtime.shutdown().await.unwrap();
    publishing.await.unwrap();
    assert!(report.unfinished_accounts.is_empty());
    assert!(
        report
            .accounts
            .values()
            .all(|report| !report.task_died && report.abandoned == 0)
    );
    assert!(handle.recorder().is_closed());
    for id in 1..=32 {
        assert_eq!(store.balance(AccountId(id)), CostUnits(1_000));
    }
    assert_eq!(handle.report().managed_accounts, 0);
}

#[tokio::test(start_paused = true)]
async fn an_accounting_integrity_fault_stops_the_instance_without_restart() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (_scripted, allocator) = Scripted::refusing_release().spawn(&store);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator,
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    for id in 1..=5 {
        charge(&handle, 11, id, 99);
    }
    wait(|| handle.recorder().is_closed()).await;
    assert!(!handle.readiness(t(100)).is_ready());
    assert_eq!(handle.report().manager_restarts, 0);
    assert!(runtime.shutdown().await.unwrap().background_failed);
    assert!(!handle.readiness(t(100)).background_healthy);
}

#[tokio::test(start_paused = true)]
async fn dropping_the_runtime_aborts_its_task_tree_while_handles_remain() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    drop(runtime);
    wait(|| handle.recorder().is_closed() && !handle.account_reports(t(100))[0].task_healthy).await;
    assert!(!handle.readiness(t(100)).is_ready());
    assert!(!handle.readiness(t(100)).background_healthy);
    assert_eq!(
        store.balance(AccountId(1)),
        CostUnits(500),
        "aborted grants wait for reclaim"
    );
}

#[tokio::test]
async fn invalid_component_settings_are_rejected_before_spawning_the_runtime() {
    let store = store();
    let mut snapshots = config();
    snapshots.snapshots.max_concurrent_fetches = 0;
    let mut leases = config();
    leases.leases.low_water = leases.leases.target_grant;
    let mut usage = config();
    usage.usage.queue_capacity = 0;
    let mut clock_domain = config();
    clock_domain.idle_account_linger = Duration::MAX;
    let mut phase_overflow = config();
    phase_overflow.usage.shutdown_drain_deadline = Duration::MAX;
    for config in [snapshots, leases, usage, clock_domain, phase_overflow] {
        assert!(
            InstanceRuntime::spawn(
                store.clone(),
                store.clone(),
                store.clone(),
                Arc::new(ManualClock::new(t(100))),
                config
            )
            .is_err()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_pauses_refills_while_previously_issued_permits_drain() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let mut cfg = config();
    cfg.leases.target_grant = CostUnits(100);
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let context = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap();
    let permit = handle.recorder().try_reserve().unwrap();
    let outstanding = handle.recorder().try_reserve().unwrap();
    let shutdown = tokio::spawn(runtime.shutdown());
    wait(|| handle.recorder().is_closed()).await;
    drop(
        context
            .admit(&[(Op, 90)], permit, t(100))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(1), t(100))
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    drop(outstanding);
    let report = shutdown.await.unwrap().unwrap();
    assert_eq!(report.usage.unwrap().accepted, 1);
    assert_eq!(store.balance(AccountId(1)), CostUnits(910));
}

#[tokio::test(start_paused = true)]
async fn repeated_large_grants_report_counter_overflow_without_wrapping() {
    let store = store();
    account(&store, 1, u64::MAX);
    let mut cfg = config();
    cfg.leases.target_grant = CostUnits(u64::MAX / 2);
    let (runtime, handle) = start(&store, cfg);
    for generation in [1, 3, 5] {
        publish(&store, 11, 1, generation, EnforcementMode::Strict);
        wait(|| handle.report().managed_accounts == 1 && handle.readiness(t(100)).is_ready()).await;
        store.remove_snapshot(Principal(11));
        wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
        assert_eq!(store.balance(AccountId(1)), CostUnits(u64::MAX));
    }
    assert!(handle.report().counter_overflow);
    assert!(handle.report().refill.is_none());
    assert!(handle.account_reports(t(100))[0].refill.is_none());
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_in_flight_release_cannot_start_a_refill_after_shutdown_pauses_it() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let block = Arc::new(ReleaseBlock {
        started: AtomicBool::new(false),
        resume: tokio::sync::Notify::new(),
    });
    let (_scripted, allocator) = Scripted::blocking(Some(block.clone())).spawn(&store);
    let clock = Arc::new(ManualClock::new(t(100)));
    let mut cfg = config();
    cfg.leases.store_call_timeout = Duration::from_secs(5);
    let (runtime, handle) =
        InstanceRuntime::spawn(store.clone(), allocator, store.clone(), clock.clone(), cfg)
            .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let permit = handle.recorder().try_reserve().unwrap();
    clock.set(t(399));
    wait(|| block.started.load(Ordering::Acquire)).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    let shutdown = tokio::spawn(runtime.shutdown());
    wait(|| handle.recorder().is_closed()).await;
    block.resume.notify_one();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    drop(permit);
    shutdown.await.unwrap().unwrap();
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
}

/// A source that pushes but cannot enumerate.
///
/// The `Ok(None)` is stated rather than inherited. This double previously got
/// it by *omitting* `principals`, which is the same silence that made other
/// wrappers lie about a catalogue they did have — here it happened to be the
/// intent. Saying it out loud is the difference between the two (#83).
fn push_only_source(store: &Arc<MemoryStore>) -> Arc<DelegatingStore<MemoryStore>> {
    Arc::new(DelegatingStore::wrapping(Arc::clone(store)).on_principals(|_| async { Ok(None) }))
}

#[tokio::test(start_paused = true)]
async fn a_push_only_source_reports_unresolved_principals_without_enumeration() {
    let store = store();
    let (runtime, handle) = InstanceRuntime::spawn(
        push_only_source(&store),
        store.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    wait(|| handle.report().managed_accounts == 1 && handle.readiness(t(100)).is_ready()).await;
    assert_eq!(handle.readiness(t(10_000)).unresolved_principals, 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn repeated_shutdown_requests_share_the_first_deadline() {
    let store = store();
    let (runtime, handle) = start(&store, config());
    let first = handle.request_shutdown();
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(handle.clone().request_shutdown(), first);
    runtime.shutdown().await.unwrap();
    assert_eq!(handle.request_shutdown(), first);
}

#[tokio::test(start_paused = true)]
async fn freshness_expiry_retires_a_manager_without_another_snapshot_notification() {
    let store = store();
    account(&store, 1, 1_000);
    publish_until(&store, 11, 1, 1, EnforcementMode::Strict, t(101));
    let clock = Arc::new(ManualClock::new(t(100)));
    let mut cfg = config();
    cfg.snapshots.refresh_interval = Duration::from_secs(3_600);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        clock.clone(),
        cfg,
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    clock.set(t(101));
    tokio::time::advance(Duration::from_secs(1)).await;
    wait(|| handle.account_reports(t(101))[0].phase == AccountPhase::Dormant).await;
    assert_eq!(handle.report().refill.unwrap().released, 1);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reassignment_moves_membership_and_rejected_generations_cannot_move_it_back() {
    let store = store();
    account(&store, 1, 1_000);
    account(&store, 2, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    publish(&store, 11, 2, 2, EnforcementMode::Strict);
    wait(|| {
        handle
            .account_reports(t(100))
            .iter()
            .any(|a| a.account == AccountId(2) && a.fundable)
    })
    .await;
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(handle.report().managed_accounts, 1);
    charge(&handle, 11, 1, 7);
    runtime.shutdown().await.unwrap();
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    assert_eq!(store.balance(AccountId(2)), CostUnits(993));
}

#[tokio::test(start_paused = true)]
async fn reactivation_during_release_waits_for_the_old_manager_before_reporting_managed() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let pending = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap()
        .admit(&[(Op, 7)], handle.recorder().try_reserve().unwrap(), t(100))
        .unwrap();
    store.remove_snapshot(Principal(11));
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Retiring).await;
    publish(&store, 11, 1, 3, EnforcementMode::Strict);
    wait(|| handle.account_reports(t(100))[0].eligible).await;
    let readiness = handle.readiness(t(100));
    assert_eq!(readiness.unmanaged_accounts, 1);
    assert!(!readiness.background_healthy);
    assert!(!readiness.is_ready());
    assert_eq!(handle.report().refill.unwrap().acquired, 1);
    drop(pending);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    assert_eq!(handle.report().refill.unwrap().acquired, 2);
    assert_eq!(handle.report().manager_restarts, 0);
    runtime.shutdown().await.unwrap();
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
}

#[tokio::test(start_paused = true)]
async fn readiness_withdraws_at_queue_capacity_and_recovers_when_a_permit_returns() {
    let store = store();
    let mut cfg = config();
    cfg.usage.queue_capacity = 2;
    cfg.usage.max_batch = 2;
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let first = handle.recorder().try_reserve().unwrap();
    assert!(handle.readiness(t(100)).accounting_healthy);
    let second = handle.recorder().try_reserve().unwrap();
    assert!(!handle.readiness(t(100)).accounting_healthy);
    assert!(!handle.readiness(t(100)).is_ready());
    drop(second);
    assert!(handle.readiness(t(100)).is_ready());
    drop(first);
    runtime.shutdown().await.unwrap();
    assert!(!handle.readiness(t(100)).accounting_healthy);
}

/// A sink that refuses, permanently or by rejecting every event. It has no
/// inner store, so it rejects everything it is not given rather than
/// delegating — there is nothing behind it to delegate to.
fn rejecting_sink(permanent: bool) -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("a usage-sink fixture answers ingest and nothing else").on_ingest(
            move |_, events, _now| async move {
                if permanent {
                    Err(tollgate_store::IngestError::Refused(
                        tollgate_store::StoreError("injected refusal".into()),
                    ))
                } else {
                    Ok(tollgate_store::IngestReport {
                        unattributed: None,
                        accepted: 0,
                        duplicate: 0,
                        rejected: events.len() as u64,
                    })
                }
            },
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn rejected_and_permanently_lost_usage_withdraw_accounting_readiness() {
    for permanent in [false, true] {
        let store = store();
        account(&store, 1, 1_000);
        publish(&store, 11, 1, 1, EnforcementMode::Strict);
        let (runtime, handle) = InstanceRuntime::spawn(
            store.clone(),
            store.clone(),
            rejecting_sink(permanent),
            Arc::new(ManualClock::new(t(100))),
            config(),
        )
        .unwrap();
        wait(|| handle.readiness(t(100)).is_ready()).await;
        charge(&handle, 11, 1, 7);
        wait(|| {
            let stats = handle.report().accounting.stats;
            stats.rejected + stats.lost == 1
        })
        .await;
        assert!(!handle.readiness(t(100)).accounting_healthy);
        assert!(!handle.readiness(t(100)).is_ready());
        assert_eq!(handle.report().accounting.unaccounted, 0);
        assert!(!handle.recorder().is_closed());
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn a_second_crash_reports_parked_grants_after_releasing_an_inherited_capability() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (scripted, allocator) = Scripted::default().spawn(&store);
    let mut cfg = config();
    cfg.leases.target_grant = CostUnits(100);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        cfg,
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let hold = || {
        handle
            .begin(Principal(11), PermissionBits::bit(0), t(100))
            .unwrap()
            .admit(
                &[(Op, 99)],
                handle.recorder().try_reserve().unwrap(),
                t(100),
            )
            .unwrap()
    };
    scripted.panic.store(true, Ordering::Release);
    let inherited = hold();
    wait(|| handle.report().restarting_accounts == 1).await;
    assert_eq!(handle.report().unrecovered_grants, 0);
    wait(|| handle.report().refill.unwrap().acquired == 2).await;
    drop(inherited);
    wait(|| handle.report().refill.unwrap().released == 1).await;
    let parked = hold();
    wait(|| handle.report().refill.unwrap().acquired == 3).await;
    scripted.panic.store(true, Ordering::Release);
    let current = hold();
    wait(|| handle.report().restarting_accounts == 1).await;
    assert_eq!(handle.report().unrecovered_grants, 1);
    assert_eq!(handle.report().uncertain_acquires, 2);
    drop(parked);
    drop(current);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    runtime.shutdown().await.unwrap();
    assert_eq!(
        store.balance(AccountId(1)),
        CostUnits(900),
        "the parked grant requires TTL reclaim"
    );
}

#[tokio::test(start_paused = true)]
async fn repeated_inactive_publications_do_not_extend_the_initial_linger() {
    use tollgate_store::AdminStore;
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (runtime, handle) = start(&store, config());
    wait(|| handle.readiness(t(100)).is_ready()).await;
    store
        .set_account_status(AccountId(1), AccountStatus::Suspended)
        .await
        .unwrap();
    wait(|| handle.report().lingering_accounts == 1).await;
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        store
            .set_account_status(AccountId(1), AccountStatus::Suspended)
            .await
            .unwrap();
    }
    assert_eq!(handle.report().managed_accounts, 0);
    assert_eq!(handle.report().refill.unwrap().released, 1);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_held_reservation_exhausts_and_reports_the_shared_shutdown_deadline() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let mut cfg = config();
    cfg.shutdown_deadline = Duration::from_millis(200);
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let pending = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap()
        .admit(
            &[(Op, 99)],
            handle.recorder().try_reserve().unwrap(),
            t(100),
        )
        .unwrap();
    let began = tokio::time::Instant::now();
    let report = runtime.shutdown().await.unwrap();
    assert_eq!(began.elapsed(), Duration::from_millis(200));
    assert!(report.deadline_expired);
    assert_eq!(report.usage.unwrap().unresolved, 1);
    assert_eq!(
        report.accounts.values().map(|a| a.abandoned).sum::<u64>()
            + report.unfinished_accounts.len() as u64,
        1
    );
    drop(pending);
    assert_eq!(store.balance(AccountId(1)), CostUnits(500));
}

#[tokio::test(start_paused = true)]
async fn a_fixed_instance_with_only_inactive_accounts_does_not_advertise_funding() {
    use tollgate_store::AdminStore;
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let mut cfg = config();
    cfg.snapshots.principals = TrackedPrincipals::Fixed(vec![Principal(11)]);
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    store
        .set_account_status(AccountId(1), AccountStatus::Suspended)
        .await
        .unwrap();
    wait(|| {
        let ready = handle.readiness(t(100));
        ready.eligible_accounts == 0 && ready.snapshots_ready && ready.background_healthy
    })
    .await;
    let ready = handle.readiness(t(100));
    assert!(ready.accounting_healthy);
    assert!(
        !ready.is_ready(),
        "a resolved but inactive account cannot serve work"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn removing_the_last_principal_cancels_a_pending_manager_restart() {
    let store = store();
    account(&store, 1, 1_000);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (_scripted, allocator) = Scripted::panicking().spawn(&store);
    let mut cfg = config();
    cfg.manager_restart_backoff = Duration::from_secs(1);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator,
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        cfg,
    )
    .unwrap();
    wait(|| handle.report().restarting_accounts == 1).await;
    store.remove_snapshot(Principal(11));
    wait(|| handle.account_reports(t(100))[0].phase == AccountPhase::Dormant).await;
    assert_eq!(handle.report().restarting_accounts, 0);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(handle.report().manager_restarts, 0);
    assert_eq!(handle.report().managed_accounts, 0);
    assert_eq!(handle.report().refill.unwrap().acquired, 0);
    runtime.shutdown().await.unwrap();
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
}

#[tokio::test(start_paused = true)]
async fn integrity_faults_in_idle_and_final_release_are_reported_as_terminal() {
    for (idle, reactivate) in [(true, false), (true, true), (false, false)] {
        let store = store();
        account(&store, 1, 1_000);
        publish(&store, 11, 1, 1, EnforcementMode::Strict);
        let block = reactivate.then(|| {
            Arc::new(ReleaseBlock {
                started: AtomicBool::new(false),
                resume: tokio::sync::Notify::new(),
            })
        });
        let (_scripted, allocator) = Scripted {
            invalid_release: true,
            release_block: block.clone(),
            ..Scripted::default()
        }
        .spawn(&store);
        let (runtime, handle) = InstanceRuntime::spawn(
            store.clone(),
            allocator,
            store.clone(),
            Arc::new(ManualClock::new(t(100))),
            config(),
        )
        .unwrap();
        wait(|| handle.readiness(t(100)).is_ready()).await;
        if idle {
            store.remove_snapshot(Principal(11));
            if let Some(block) = block {
                wait(|| block.started.load(Ordering::Acquire)).await;
                publish(&store, 11, 1, 3, EnforcementMode::Strict);
                wait(|| handle.account_reports(t(100))[0].eligible).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                block.resume.notify_one();
            }
            wait(|| handle.recorder().is_closed()).await;
        }
        let report = runtime.shutdown().await.unwrap();
        assert!(report.background_failed);
        let account = &handle.account_reports(t(100))[0];
        assert_eq!(account.phase, AccountPhase::Faulted);
        assert!(!account.task_healthy);
        assert_eq!(handle.report().manager_restarts, 0);
        assert_eq!(handle.report().restarting_accounts, 0);
        assert!(!handle.readiness(t(100)).background_healthy);
        assert_eq!(handle.report().refill.unwrap().abandoned, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_consolidated_predecessor_is_not_reported_as_crash_exposure() {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        ..GrantPolicy::default()
    })
    .unwrap();
    account(&store, 1, 160);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let (scripted, allocator) = Scripted::default().spawn(&store);
    let mut cfg = config();
    cfg.leases.target_grant = CostUnits(100);
    cfg.leases.low_water = CostUnits(50);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        cfg,
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    for id in 1..=2 {
        charge(&handle, 11, id, 51);
        wait(|| handle.report().refill.unwrap().acquired == id as u64 + 1).await;
    }
    let request = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap();
    assert!(matches!(
        request.admit(
            &[(Op, 51)],
            handle.recorder().try_reserve().unwrap(),
            t(100)
        ),
        Err(tollgate_core::DenyReason::LeaseExhausted { .. })
    ));
    wait(|| handle.report().refill.unwrap().consolidated == 1).await;
    scripted.panic.store(true, Ordering::SeqCst);
    charge(&handle, 11, 3, 51);
    wait(|| handle.report().restarting_accounts == 1).await;
    assert_eq!(handle.report().unrecovered_grants, 0);
    assert_eq!(handle.report().uncertain_acquires, 1);
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let stopped = runtime.shutdown().await.unwrap();
    assert!(!stopped.deadline_expired);
    let counters = handle.report().refill.unwrap();
    assert_eq!(counters.acquired, counters.released);
    assert_eq!(store.balance(AccountId(1)), CostUnits(7));
}

/// Commits the exchange and then loses its reply, as a transport can.
fn unanswered_consolidator(
    store: &Arc<MemoryStore>,
    committed: &Arc<AtomicBool>,
    reply_error: bool,
) -> Arc<DelegatingStore<MemoryStore>> {
    let committed = Arc::clone(committed);
    Arc::new(DelegatingStore::wrapping(Arc::clone(store)).on_consolidate(
        move |inner, lease, fence, unspent, requested, ttl, now| {
            let committed = Arc::clone(&committed);
            async move {
                let _fresh = inner
                    .consolidate(lease, fence, unspent, requested, ttl, now)
                    .await?;
                committed.store(true, Ordering::SeqCst);
                if reply_error {
                    return Err(AllocateError::Storage(tollgate_store::StoreError(
                        "reply lost after commit".into(),
                    )));
                }
                std::future::pending().await
            }
        },
    ))
}

async fn unanswered_consolidation(
    reply_error: bool,
) -> (InstanceRuntime, RuntimeHandle, Arc<MemoryStore>) {
    let store = store();
    account(&store, 1, 100);
    publish(&store, 11, 1, 1, EnforcementMode::Strict);
    let committed = Arc::new(AtomicBool::new(false));
    let allocator = unanswered_consolidator(&store, &committed, reply_error);
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        allocator.clone(),
        store.clone(),
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    wait(|| handle.readiness(t(100)).is_ready()).await;
    let request = handle
        .begin(Principal(11), PermissionBits::bit(0), t(100))
        .unwrap();
    assert!(
        request
            .admit(
                &[(Op, 60)],
                handle.recorder().try_reserve().unwrap(),
                t(100)
            )
            .is_err()
    );
    wait(|| committed.load(Ordering::SeqCst)).await;
    (runtime, handle, store)
}

#[tokio::test(start_paused = true)]
async fn shutdown_reports_an_unanswered_consolidation_grant() {
    let (runtime, handle, store) = unanswered_consolidation(false).await;
    let stopped = runtime.shutdown().await.unwrap();
    assert!(!stopped.deadline_expired);
    assert_eq!(
        store.balance(AccountId(1)),
        CostUnits(50),
        "the unanswered replacement still holds 50 units"
    );
    assert_eq!(
        handle.report().uncertain_acquires,
        1,
        "shutdown must expose the lost replacement capability"
    );
    assert_eq!(
        handle.report().unrecovered_grants,
        0,
        "the replacement size was never confirmed"
    );
    let reclaimed = store.reclaim_expired(t(10_000)).await.unwrap();
    assert_eq!(
        reclaimed.len(),
        1,
        "only the unanswered replacement remains active"
    );
    assert_eq!(store.balance(AccountId(1)), CostUnits(100));
}

#[tokio::test(start_paused = true)]
async fn ambiguous_consolidations_remain_visible_after_a_clean_shutdown() {
    for reply_error in [false, true] {
        let (runtime, handle, store) = unanswered_consolidation(reply_error).await;
        wait(|| handle.report().uncertain_acquires == 1).await;
        assert_eq!(handle.account_reports(t(100))[0].uncertain_acquires, 1);
        assert_eq!(
            handle.report().refill.unwrap().acquire_timeouts,
            u64::from(!reply_error)
        );
        let stopped = runtime.shutdown().await.unwrap();
        assert!(!stopped.deadline_expired);
        assert_eq!(
            handle.report().uncertain_acquires,
            1,
            "retirement counts each ambiguous outcome once"
        );
        assert_eq!(handle.account_reports(t(100))[0].uncertain_acquires, 1);
        assert_eq!(store.balance(AccountId(1)), CostUnits(50));
        let reclaimed = store.reclaim_expired(t(10_000)).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(store.balance(AccountId(1)), CostUnits(100));
    }
}

/// An instance reports the layout it is running and whether that layout still
/// buys anything (#124).
///
/// Sharding is worth its memory only while a request-serving thread writes to
/// lines no peer writes, and that holds only while the affinities handed out do
/// not outnumber the shards. When it stops holding, the instance degrades
/// toward the unsharded cost while looking exactly like the contention the
/// layout was enabled to remove — so the report carries it rather than leaving
/// an operator to infer it from latency.
///
/// The reading has to be live, not a copy taken at startup, because crowding
/// arrives *after* startup: threads take their affinity on first use. So this
/// stands one instance up with room to spare, confirms it reports room, then
/// consumes the headroom and confirms the same handle now says so.
#[tokio::test(start_paused = true)]
async fn a_runtime_report_carries_the_effective_shard_layout() {
    if isolated::rerun_in_child() {
        return;
    }
    let store = store();
    // Isolation prevents parallel tests from consuming this headroom while
    // readiness is awaited. This Tokio runtime uses only the current thread.
    assert_eq!(tollgate_core::Locality::assigned(), 0);
    let shards = 8;
    let mut cfg = config();
    cfg.sharding = tollgate_core::LocalSharding::new(std::num::NonZeroUsize::new(shards).unwrap());
    let (runtime, handle) = start(&store, cfg);
    wait(|| handle.readiness(t(100)).is_ready()).await;

    let spread = handle.report().sharding;
    assert_eq!(
        spread.shards, shards,
        "the report names the configured count"
    );
    assert_eq!(
        spread.affinities_assigned,
        tollgate_core::Locality::assigned(),
        "and reads the live counter"
    );
    assert!(
        !spread.is_crowded(),
        "every affinity fits, so every thread can still have its own shard"
    );
    assert_eq!(spread.crowded_shards(), 0);

    // Consume the headroom the way a deployment does — threads taking their
    // number on first use — and the same handle must change its answer.
    for _ in 0..16 {
        std::thread::spawn(|| {
            let _claimed = tollgate_core::Locality::current();
        })
        .join()
        .unwrap();
    }
    let crowded = handle.report().sharding;
    assert_eq!(crowded.shards, shards, "the layout itself did not move");
    assert!(
        crowded.affinities_assigned > spread.affinities_assigned,
        "the affinities did"
    );
    assert!(
        crowded.is_crowded(),
        "more affinities than shards must be reported, not inferred from latency"
    );
    assert_eq!(crowded.affinities_assigned, spread.affinities_assigned + 16);
    assert_eq!(crowded.crowded_shards(), shards);
    drop(runtime.shutdown().await.unwrap());
}
