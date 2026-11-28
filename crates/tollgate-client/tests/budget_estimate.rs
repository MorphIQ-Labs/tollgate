//! Budget observations converge through ordinary refresh, without changing
//! authorization policy (GL-77, INVARIANTS.md GL-28).

#![allow(
    clippy::disallowed_methods,
    reason = "all business time is supplied explicitly; Tokio time is paused"
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use std::sync::Mutex;
use tokio::sync::broadcast;
use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, MokaSnapshotMap, NoGate, SnapshotMap,
};
use tollgate_client::{
    ManualClock, SlotRegistry, SnapshotManager, SnapshotManagerConfig, TrackedPrincipals,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, BudgetView, CapacityClass, CostTable, CostUnits,
    Generation, LocalLease, OpIndex, PermissionBits, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits, UsageEvent, UsageSlot,
};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, MemoryStore, SnapshotPush,
    SnapshotResolution, SnapshotSource, StoreError, UsageSink,
};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(7);
const INITIAL: u64 = 1_000;
const CHARGE: u64 = 6;

#[derive(Clone, Copy)]
struct Price;
impl OpIndex for Price {
    fn index(&self) -> usize {
        0
    }
}

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

fn snapshot(account: AccountId) -> PublishableSnapshot {
    PublishableSnapshot::try_new(Arc::new(
        AccountSnapshot::builder(
            account,
            Generation(1),
            AccountStatus::Active,
            t(100_000),
            PermissionBits::bit(0),
            ResolvedLimits::new(64).with_weighted_rate(100_000, 100_000),
            Arc::new(
                CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                    .weight(&Price, CostUnits(CHARGE))
                    .build(),
            ),
        )
        .build(),
    ))
    .unwrap()
}

struct Instance<M: SnapshotMap + 'static = ArcSwapSnapshotMap> {
    engine: AdmissionEngine<Arc<M>>,
    slots: Arc<SlotRegistry>,
    manager: SnapshotManager,
}

impl Instance {
    async fn start(store: Arc<MemoryStore>) -> Self {
        Instance::start_with(store, ArcSwapSnapshotMap::new(), vec![PRINCIPAL]).await
    }
}

impl<M: SnapshotMap + 'static> Instance<M> {
    async fn start_with(
        source: Arc<dyn SnapshotSource>,
        map: M,
        principals: Vec<Principal>,
    ) -> Self {
        let map = Arc::new(map);
        let slots = SlotRegistry::new();
        let manager = SnapshotManager::spawn(
            source,
            map.clone(),
            slots.clone(),
            Arc::new(ManualClock::new(t(1))),
            SnapshotManagerConfig {
                principals: TrackedPrincipals::Fixed(principals),
                refresh_interval: Duration::from_millis(20),
                unknown_ttl: SignedDuration::from_secs(30),
                revoked_ttl: SignedDuration::from_secs(3_600),
                retry_backoff: Duration::from_millis(5),
                max_concurrent_fetches: 4,
                fetch_timeout: Duration::from_secs(5),
                enumeration_timeout: Duration::from_secs(30),
            },
        )
        .unwrap();
        let mut ready = manager.ready();
        while !*ready.borrow_and_update() {
            ready.changed().await.unwrap();
        }
        Self {
            engine: AdmissionEngine::new(map),
            slots,
            manager,
        }
    }

    fn estimate(&self) -> u64 {
        estimate(&self.engine, PRINCIPAL)
    }

    async fn fund(&self, store: &MemoryStore) {
        let grant = store
            .acquire(ACCOUNT, CostUnits(100), SignedDuration::from_secs(60), t(0))
            .await
            .unwrap()
            .grant;
        drop(
            self.slots
                .slot(ACCOUNT)
                .replace(Arc::new(LocalLease::new(grant, CostUnits::ZERO))),
        );
    }

    async fn spend_and_ingest(&self, store: &MemoryStore) {
        self.fund(store).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let committed = self
            .engine
            .begin(PRINCIPAL, PermissionBits::bit(0), t(1))
            .unwrap()
            .admit(&[(Price, 1)], Capture(tx), t(1))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(1), t(1))
            .unwrap();
        assert_eq!(committed.units(), CostUnits(CHARGE));
        drop(committed);
        let report = store.ingest(&[rx.await.unwrap()], t(1)).await.unwrap();
        assert_eq!(
            (report.accepted, report.duplicate, report.rejected),
            (1, 0, 0)
        );
        assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(CHARGE));
        assert!(store.conservation(ACCOUNT).unwrap().holds());
    }
}

fn estimate<M: SnapshotMap>(engine: &AdmissionEngine<M>, principal: Principal) -> u64 {
    engine
        .begin(principal, PermissionBits::bit(0), t(1))
        .unwrap()
        .estimate_remaining()
        .unwrap()
        .0
}

struct Capture(tokio::sync::oneshot::Sender<UsageEvent>);
impl UsageSlot for Capture {
    fn record(self, event: UsageEvent) {
        self.0.send(event).unwrap();
    }
}

async fn setup() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(300),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(INITIAL),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    AdminStore::publish_snapshot(&*store, PRINCIPAL, snapshot(ACCOUNT))
        .await
        .unwrap();
    store
}

async fn refresh_while_idle<A: SnapshotMap + 'static, B: SnapshotMap + 'static>(
    a: &Instance<A>,
    b: &Instance<B>,
) {
    let before_a = a.manager.counters().snapshot().refresh_attempts;
    let before_b = b.manager.counters().snapshot().refresh_attempts;
    for _ in 0..110 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ca = a.manager.counters().snapshot();
    let cb = b.manager.counters().snapshot();
    assert!(ca.refresh_attempts - before_a >= 100);
    assert!(cb.refresh_attempts - before_b >= 100);
    assert_eq!(ca.refresh_failures + ca.refresh_timeouts, 0);
    assert_eq!(cb.refresh_failures + cb.refresh_timeouts, 0);
}

#[tokio::test(start_paused = true)]
async fn settled_spend_converges_after_refresh_without_policy_republication() {
    let store = setup().await;
    let a = Instance::start(store.clone()).await;
    let b = Instance::start(store.clone()).await;
    a.spend_and_ingest(&store).await;
    refresh_while_idle(&a, &b).await;
    let observed = (a.estimate(), b.estimate());
    a.manager.shutdown().await;
    b.manager.shutdown().await;
    assert_eq!(observed, (INITIAL - CHARGE, INITIAL - CHARGE));
}

#[tokio::test(start_paused = true)]
async fn cancelled_admission_does_not_leave_a_permanent_budget_deduction() {
    let store = setup().await;
    let a = Instance::start(store.clone()).await;
    let b = Instance::start(store.clone()).await;
    a.fund(&store).await;
    let (tx, _rx) = tokio::sync::oneshot::channel();
    a.engine
        .begin(PRINCIPAL, PermissionBits::bit(0), t(1))
        .unwrap()
        .admit(&[(Price, 1)], Capture(tx), t(1))
        .unwrap()
        .cancel();
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
    refresh_while_idle(&a, &b).await;
    let observed = (a.estimate(), b.estimate());
    a.manager.shutdown().await;
    b.manager.shutdown().await;
    assert_eq!(observed, (INITIAL, INITIAL));
}

#[tokio::test(start_paused = true)]
async fn a_late_instance_observes_usage_settled_before_it_started() {
    let store = setup().await;
    let a = Instance::start(store.clone()).await;
    a.spend_and_ingest(&store).await;
    let late = Instance::start(store.clone()).await;
    let observed = late.estimate();
    a.manager.shutdown().await;
    late.manager.shutdown().await;
    assert_eq!(observed, INITIAL - CHARGE);
}

async fn unrelated_accounts_are_isolated<M: SnapshotMap>(map: M) {
    let store = setup().await;
    let engine = AdmissionEngine::new(map);
    let principal_b = Principal(8);
    for (principal, account) in [(PRINCIPAL, ACCOUNT), (principal_b, AccountId(2))] {
        let stamped = snapshot(account).with_budget(Some(BudgetView {
            balance: CostUnits(INITIAL),
            period_end: None,
            consumed: None,
        }));
        let slot = LeaseSlot::for_account(account);
        if account == ACCOUNT {
            let grant = store
                .acquire(ACCOUNT, CostUnits(100), SignedDuration::from_secs(60), t(0))
                .await
                .unwrap()
                .grant;
            drop(slot.replace(Arc::new(LocalLease::new(grant, CostUnits::ZERO))));
        }
        engine
            .map()
            .install(principal, Arc::new(stamped.as_snapshot().clone()), slot)
            .unwrap();
    }
    assert_eq!(estimate(&engine, principal_b), INITIAL);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let committed = engine
        .begin(PRINCIPAL, PermissionBits::bit(0), t(1))
        .unwrap()
        .admit(&[(Price, 1)], Capture(tx), t(1))
        .unwrap()
        .acquire_capacity(&NoGate)
        .unwrap()
        .commit(RequestId(1), t(1))
        .unwrap();
    drop(committed);
    assert_eq!(rx.await.unwrap().account_id, ACCOUNT);
    assert_eq!(
        estimate(&engine, principal_b),
        INITIAL,
        "account A's six-unit admission must not change account B's estimate"
    );
}

#[tokio::test(start_paused = true)]
async fn arc_swap_budget_does_not_subtract_another_accounts_admissions() {
    unrelated_accounts_are_isolated(ArcSwapSnapshotMap::new()).await;
}

#[tokio::test(start_paused = true)]
async fn moka_budget_does_not_subtract_another_accounts_admissions() {
    unrelated_accounts_are_isolated(MokaSnapshotMap::new(16)).await;
}

/// Both principals of one account read one observation, on either map, and the
/// authorization generation never moves.
async fn principals_of_one_account_converge<M: SnapshotMap + 'static>(map: M) {
    let store = setup().await;
    let second = Principal(8);
    AdminStore::publish_snapshot(&*store, second, snapshot(ACCOUNT))
        .await
        .unwrap();
    let a = Instance::start(store.clone()).await;
    let b = Instance::start_with(store.clone(), map, vec![PRINCIPAL, second]).await;
    a.spend_and_ingest(&store).await;
    refresh_while_idle(&a, &b).await;
    for principal in [PRINCIPAL, second] {
        let context = b
            .engine
            .begin(principal, PermissionBits::bit(0), t(1))
            .unwrap();
        assert_eq!(context.generation(), Generation(1));
        assert_eq!(
            context.estimate_remaining(),
            Some(CostUnits(INITIAL - CHARGE))
        );
    }
    assert_eq!(b.manager.counters().snapshot().refused_updates, 0);
    a.manager.shutdown().await;
    b.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn arc_swap_principals_of_one_account_converge() {
    principals_of_one_account_converge(ArcSwapSnapshotMap::new()).await;
}

#[tokio::test(start_paused = true)]
async fn moka_principals_of_one_account_converge() {
    principals_of_one_account_converge(MokaSnapshotMap::new(16)).await;
}

/// An observation fetched for one account is delivered to that account alone:
/// each estimate follows its own ledger through refresh.
#[tokio::test(start_paused = true)]
async fn refresh_delivers_each_account_its_own_observation() {
    let store = setup().await;
    let (other, other_principal) = (AccountId(2), Principal(8));
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: other,
            initial_balance: CostUnits(2 * INITIAL),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    AdminStore::publish_snapshot(&*store, other_principal, snapshot(other))
        .await
        .unwrap();
    let principals = vec![PRINCIPAL, other_principal];
    let a =
        Instance::start_with(store.clone(), ArcSwapSnapshotMap::new(), principals.clone()).await;
    let b = Instance::start_with(store.clone(), MokaSnapshotMap::new(16), principals).await;
    a.spend_and_ingest(&store).await;
    refresh_while_idle(&a, &b).await;
    let observed = [
        (a.estimate(), estimate(&a.engine, other_principal)),
        (
            estimate(&b.engine, PRINCIPAL),
            estimate(&b.engine, other_principal),
        ),
    ];
    a.manager.shutdown().await;
    b.manager.shutdown().await;
    assert_eq!(observed, [(INITIAL - CHARGE, 2 * INITIAL); 2]);
}

/// What the scripted source answers next. It sends no pushes.
#[derive(Debug, Clone, Copy)]
enum Answer {
    Observation { balance: u64, consumed: u64 },
    Failure,
}

struct Scripted {
    answer: Mutex<Answer>,
    push: broadcast::Sender<SnapshotPush>,
}

impl Scripted {
    fn set(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }
}

#[async_trait]
impl SnapshotSource for Scripted {
    async fn snapshot(&self, _: Principal) -> Result<SnapshotResolution, StoreError> {
        let answer = *self.answer.lock().unwrap();
        match answer {
            Answer::Observation { balance, consumed } => Ok(SnapshotResolution::Present(
                snapshot(ACCOUNT).with_budget(Some(BudgetView {
                    balance: CostUnits(balance),
                    period_end: None,
                    consumed: Some(CostUnits(consumed)),
                })),
            )),
            Answer::Failure => Err(StoreError("scripted outage".into())),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

async fn one_sweep() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// A failed refresh keeps the last observation, and an older answer — a lagging
/// replica, or a delayed read — is refused and counted rather than restoring
/// spent units. A newer one is taken again.
#[tokio::test(start_paused = true)]
async fn failed_and_stale_refreshes_keep_the_newest_observation() {
    let source = Arc::new(Scripted {
        answer: Mutex::new(Answer::Observation {
            balance: 900,
            consumed: 100,
        }),
        push: broadcast::channel(1).0,
    });
    let instance =
        Instance::start_with(source.clone(), ArcSwapSnapshotMap::new(), vec![PRINCIPAL]).await;
    let counters = instance.manager.counters();
    assert_eq!(instance.estimate(), 900);

    source.set(Answer::Failure);
    one_sweep().await;
    assert!(counters.snapshot().refresh_failures > 0);
    assert_eq!(instance.estimate(), 900);
    assert_eq!(counters.snapshot().refused_observations, 0);

    source.set(Answer::Observation {
        balance: 1_000,
        consumed: 0,
    });
    one_sweep().await;
    assert!(counters.snapshot().refused_observations > 0);
    assert_eq!(instance.estimate(), 900);

    source.set(Answer::Observation {
        balance: 800,
        consumed: 200,
    });
    one_sweep().await;
    assert_eq!(instance.estimate(), 800);
    instance.manager.shutdown().await;
}
