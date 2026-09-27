//! Identical backend scenarios for the operator account read (GL-121).
//!
//! Shared rather than mirrored, for the reason `account_keys.rs` is: a shared
//! scenario cannot drift, and drift inside mirrored bodies is what GL-85 found.
use jiff::{SignedDuration, Timestamp};
use tollgate_core::{
    AccountId, AccountStatus, BudgetSchedule, CapacityClass, CostUnits, Period, Rollover,
};
use tollgate_store::{AccountConfig, AdminStore, LeaseAllocator};

pub trait Backend: AdminStore + LeaseAllocator {}
impl<T: AdminStore + LeaseAllocator> Backend for T {}

const ACCOUNT: AccountId = AccountId(1);

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

/// An unknown account is `None`, never a zeroed view.
///
/// A default-shaped answer would read to a dashboard as a real account with no
/// funding, which is a different fact from one that does not exist.
pub async fn an_unknown_account_is_absent_not_empty(store: &impl Backend) {
    assert!(
        store
            .account_view(AccountId(404))
            .await
            .expect("the read succeeds")
            .is_none(),
        "no such account is None"
    );
}

/// Status, capacity and schedule come back as set, and the funding equation
/// holds.
pub async fn the_view_reports_what_was_administered(store: &impl Backend) {
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::BestEffort,
        },
    )
    .await
    .expect("account creation succeeds");

    let bare = store
        .account_view(ACCOUNT)
        .await
        .expect("the read succeeds")
        .expect("the account exists");
    assert_eq!(bare.status, AccountStatus::Active);
    assert_eq!(bare.capacity_class, CapacityClass::BestEffort);
    assert_eq!(bare.schedule, None, "no schedule is None, not a zero one");
    assert_eq!(bare.conservation.balance, CostUnits(1_000));
    assert!(bare.conservation.holds(), "the funding equation holds");

    let schedule = BudgetSchedule {
        allowance: CostUnits(500),
        period: Period::UtcCalendarMonth,
        rollover: Rollover::None,
    };
    store
        .set_budget_schedule(ACCOUNT, Some(schedule))
        .await
        .expect("schedule set");
    store
        .set_account_status(ACCOUNT, AccountStatus::Suspended)
        .await
        .expect("status set");

    let after = store
        .account_view(ACCOUNT)
        .await
        .expect("the read succeeds")
        .expect("the account exists");
    assert_eq!(after.schedule, Some(schedule));
    assert_eq!(after.status, AccountStatus::Suspended);
    assert_eq!(
        after.capacity_class,
        CapacityClass::BestEffort,
        "a status change leaves the capacity class alone"
    );
}

/// Units out on an unsettled lease are grants, not usage.
///
/// The distinction GL-121 asks the surface to preserve: a customer reading only
/// a falling balance would call this spend, and it is not — nothing has been
/// billed until the lease settles.
pub async fn funding_out_on_lease_is_not_reported_as_usage(store: &impl Backend) {
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .expect("account creation succeeds");

    let before = store
        .account_view(ACCOUNT)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(before.conservation.active_lease_grants, CostUnits::ZERO);
    assert_eq!(before.conservation.settled_usage, CostUnits::ZERO);

    let grant = store
        .acquire(ACCOUNT, CostUnits(400), SignedDuration::from_secs(60), t(0))
        .await
        .expect("a lease is granted")
        .grant;

    let held = store
        .account_view(ACCOUNT)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(
        held.conservation.active_lease_grants, grant.units,
        "the units are out on lease"
    );
    assert_eq!(
        held.conservation.settled_usage,
        CostUnits::ZERO,
        "and nothing has been billed"
    );
    assert!(
        held.conservation.balance < before.conservation.balance,
        "the balance fell, which is exactly why it must not be read as spend"
    );
    assert!(
        held.conservation.holds(),
        "funding is conserved across the grant"
    );
}

/// Setting a schedule reports what it replaced, and a repeat reports a no-op.
///
/// The receipt convention's whole purpose: an operator surface has to say what
/// a call actually committed. A bare `Ok` cannot distinguish "introduced a
/// schedule" from "replaced a different one" from "changed nothing", and those
/// are three different things to show someone who just clicked a button.
pub async fn setting_a_schedule_reports_what_it_replaced(store: &impl Backend) {
    use tollgate_store::AdminState;

    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(100),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .expect("account creation succeeds");

    let monthly = |units| BudgetSchedule {
        allowance: CostUnits(units),
        period: Period::UtcCalendarMonth,
        rollover: Rollover::None,
    };

    let introduced = store
        .set_budget_schedule(ACCOUNT, Some(monthly(500)))
        .await
        .expect("schedule set");
    assert_eq!(
        introduced.before,
        AdminState::Budget { schedule: None },
        "there was no schedule before"
    );
    assert_eq!(
        introduced.after,
        AdminState::Budget {
            schedule: Some(monthly(500))
        }
    );

    let replaced = store
        .set_budget_schedule(ACCOUNT, Some(monthly(900)))
        .await
        .expect("schedule replaced");
    assert_eq!(
        replaced.before,
        AdminState::Budget {
            schedule: Some(monthly(500))
        },
        "the receipt names the allowance this call displaced"
    );

    let repeated = store
        .set_budget_schedule(ACCOUNT, Some(monthly(900)))
        .await
        .expect("schedule repeated");
    assert_eq!(
        repeated.before, repeated.after,
        "equal states are how the convention says nothing changed"
    );

    let cleared = store
        .set_budget_schedule(ACCOUNT, None)
        .await
        .expect("schedule cleared");
    assert_eq!(
        cleared.after,
        AdminState::Budget { schedule: None },
        "clearing is a state, not an absence of one"
    );
}
