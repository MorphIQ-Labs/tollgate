//! Account-status transitions hold their invariant over *sequences*, not just
//! the transitions a scenario happened to write (#51, INVARIANTS.md #22).
//!
//! Written against `MemoryStore` as the reference, like `store_suite.rs`. What
//! the scenarios pin one case at a time, this pins over random walks: after
//! every accepted transition the ledger and every live snapshot of that
//! account carry the same status, generations only ever move forward and only
//! for principals that actually changed, no other account moves, and `Closed`
//! absorbs.

use std::sync::Arc;

use proptest::prelude::*;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, Generation, PermissionBits,
    Principal, PublishableSnapshot, ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, MemoryStore, SetStatusError, SnapshotResolution,
    SnapshotSource,
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
