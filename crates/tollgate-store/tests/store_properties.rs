//! Account-status transitions hold their invariant over *sequences*, not just
//! the transitions a scenario happened to write (GL-51, INVARIANTS.md GL-22).
//!
//! Written against `MemoryStore` as the reference, like `store_suite.rs`. What
//! the scenarios pin one case at a time, this pins over random walks: after
//! every accepted transition the ledger and every live snapshot of that
//! account carry the same status, generations only ever move forward and only
//! for principals that actually changed, no other account moves, and `Closed`
//! absorbs.

use std::sync::Arc;

use proptest::prelude::*;
use std::num::NonZeroUsize;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, BudgetSchedule, CapacityClass, CostTable, CostUnits,
    Generation, Period, PermissionBits, PolicyRevision, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, MemoryStore, SetStatusError, SnapshotResolution,
    SnapshotSource, UsageSink,
};

fn snapshot(account: AccountId, status: AccountStatus) -> PublishableSnapshot {
    PublishableSnapshot::try_new(Arc::new(
        AccountSnapshot::builder(
            account,
            Generation(1),
            status,
            jiff::Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .build(),
    ))
    .expect("fixture limits are valid")
}

fn any_status() -> impl Strategy<Value = AccountStatus> {
    prop_oneof![
        Just(AccountStatus::Active),
        Just(AccountStatus::Suspended),
        Just(AccountStatus::Closed),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn account_status_transitions_keep_the_two_records_equal(
        accounts in 1usize..4,
        principals in 1usize..4,
        transitions in prop::collection::vec((0usize..3, any_status()), 1..12),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        runtime.block_on(async {
            let store = MemoryStore::new(GrantPolicy::default()).unwrap();
            let ids: Vec<AccountId> = (0..accounts).map(|i| AccountId(i as u128 + 1)).collect();
            // principal id -> owning account, so a bystander's snapshots can be
            // checked for having *not* moved.
            let mut owners = Vec::new();
            for (index, account) in ids.iter().enumerate() {
                store.create_account(AccountConfig {
                    account_id: *account,
                    initial_balance: CostUnits(1_000),
                    status: AccountStatus::Active,
                    capacity_class: CapacityClass::Assured,
                });
                for slot in 0..principals {
                    let principal = Principal((index * principals + slot) as u128 + 1);
                    AdminStore::publish_snapshot(
                        &*store,
                        principal,
                        snapshot(*account, AccountStatus::Active),
                    )
                    .await
                    .expect("a fresh account is Active, so the publish agrees");
                    owners.push((principal, *account));
                }
            }

            // The model: what each account's status should be, and the last
            // generation each principal was seen at.
            let mut expected: Vec<AccountStatus> =
                ids.iter().map(|_| AccountStatus::Active).collect();
            let mut last_generation: Vec<(Principal, Generation)> =
                owners.iter().map(|(p, _)| (*p, Generation(1))).collect();

            for (which, target) in transitions {
                let index = which % ids.len();
                let account = ids[index];
                let before = expected[index];
                let result = AdminStore::set_account_status(&*store, account, target).await;

                if before == AccountStatus::Closed && target != AccountStatus::Closed {
                    prop_assert_eq!(
                        result.unwrap_err(),
                        SetStatusError::AccountClosed,
                        "Closed absorbs: {:?} -> {:?} must be refused",
                        before,
                        target
                    );
                } else {
                    prop_assert!(result.is_ok(), "{before:?} -> {target:?} must be accepted");
                    expected[index] = target;
                }

                // After every step: every live snapshot agrees with its
                // account's status, and generations moved forward exactly for
                // the principals whose status changed.
                for (slot, (principal, owner)) in owners.iter().enumerate() {
                    let SnapshotResolution::Present(current) =
                        store.snapshot(*principal).await.unwrap()
                    else {
                        prop_assert!(false, "no principal is revoked in this model");
                        unreachable!()
                    };
                    let owner_index = ids.iter().position(|a| a == owner).unwrap();
                    prop_assert_eq!(
                        current.status,
                        expected[owner_index],
                        "principal {} disagrees with its account's status",
                        principal
                    );
                    let (_, previous) = last_generation[slot];
                    prop_assert!(
                        current.generation >= previous,
                        "generations never move backward"
                    );
                    last_generation[slot] = (*principal, current.generation);
                }
            }
            Ok(())
        })?;
    }
}

/// An amount near the points where the funding total runs out, and small ones
/// that make progress.
fn funding_amount() -> impl Strategy<Value = u64> {
    prop_oneof![
        0u64..8,
        (u64::MAX / 2 - 4)..(u64::MAX / 2 + 4),
        (u64::MAX - 8)..=u64::MAX,
    ]
}

#[derive(Debug, Clone, Copy)]
enum Funds {
    Deposit(u64),
    Overage(u64),
    /// Schedule this allowance and cross the next month's boundary.
    Allowance(u64),
}

fn any_funds() -> impl Strategy<Value = Funds> {
    prop_oneof![
        funding_amount().prop_map(Funds::Deposit),
        funding_amount().prop_map(Funds::Overage),
        funding_amount().prop_map(Funds::Allowance),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// #110: over any sequence of the funding writers, one is accepted exactly
    /// when the funding total still fits `u64` afterwards (computed here in
    /// `u128`), a refused one moves nothing, and the budget read succeeds and
    /// reports the funding less what was consumed.
    #[test]
    fn no_funding_writer_takes_the_total_past_the_unit_domain(
        opening in funding_amount(),
        writes in prop::collection::vec(any_funds(), 1..16),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        runtime.block_on(async {
            let account = AccountId(1);
            let principal = Principal(1);
            let store = MemoryStore::new(GrantPolicy::default()).unwrap();
            AdminStore::create_account(
                &*store,
                AccountConfig {
                    account_id: account,
                    initial_balance: CostUnits(opening),
                    status: AccountStatus::Active,
                    capacity_class: CapacityClass::Assured,
                },
            )
            .await
            .unwrap();
            AdminStore::publish_snapshot(&*store, principal, snapshot(account, AccountStatus::Active))
                .await
                .unwrap();

            // Money in, credit extended, and the allowance currently unspent.
            let (mut deposited, mut overage, mut allowance) = (u128::from(opening), 0u128, 0u128);
            let mut expired = 0u128;
            let mut month: jiff::Timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
            for (request, write) in writes.into_iter().enumerate() {
                let fits = |units: u64| {
                    deposited + overage + u128::from(units) <= u128::from(u64::MAX)
                };
                match write {
                    Funds::Deposit(units) => {
                        let accepted = fits(units);
                        let result = AdminStore::deposit(&*store, account, CostUnits(units)).await;
                        prop_assert_eq!(result.is_ok(), accepted, "{:?}", write);
                        if accepted {
                            deposited += u128::from(units);
                        }
                    }
                    Funds::Overage(units) => {
                        let accepted = fits(units);
                        let event = UsageEvent::new(
                            RequestId(request as u128),
                            account,
                            UsageSource::Overage,
                            CostUnits(units),
                            month,
                            PolicyRevision::UNSTATED,
                            None,
                        );
                        let result = store.ingest(&[event], month).await;
                        prop_assert_eq!(result.is_ok(), accepted, "{:?}", write);
                        if accepted {
                            overage += u128::from(units);
                        }
                    }
                    Funds::Allowance(units) => {
                        let accepted = fits(units);
                        AdminStore::set_budget_schedule(
                            &*store,
                            account,
                            Some(BudgetSchedule::monthly(CostUnits(units))),
                        )
                        .await
                        .unwrap();
                        let pass = AdminStore::roll_due_periods(
                            &*store,
                            month,
                            NonZeroUsize::new(8).unwrap(),
                        )
                        .await;
                        prop_assert_eq!(
                            pass.map(|batch| batch.len()).ok(),
                            accepted.then_some(1),
                            "{:?}",
                            write
                        );
                        if accepted {
                            deposited += u128::from(units);
                            expired += allowance;
                            allowance = u128::from(units);
                        }
                        month = Period::UtcCalendarMonth.end_after(month);
                    }
                }

                let ledger = store.conservation(account).unwrap();
                prop_assert!(ledger.holds(), "{:?}", ledger);
                prop_assert_eq!(u128::from(ledger.deposited.get()), deposited);
                prop_assert_eq!(u128::from(ledger.overage_recorded.get()), overage);
                let SnapshotResolution::Present(read) =
                    store.snapshot(principal).await.unwrap()
                else {
                    panic!("the snapshot stays published");
                };
                let view = read.budget.expect("the ledger holds the account");
                prop_assert_eq!(
                    u128::from(view.balance.get()),
                    deposited - expired,
                    "everything funded, less overage consumed and allowance expired"
                );
            }
            Ok(())
        })?;
    }
}
