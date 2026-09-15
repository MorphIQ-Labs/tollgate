//! Shared implementation witnesses for administrative predecessor receipts.
use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_core::{
    AccountId, AccountStatus, BudgetSchedule, CapacityClass, CostUnits, KeyId, Period, Principal,
    Rollover,
};
use tollgate_store::{
    AccountConfig, AdminState, AdminStore, KeyDirectory, KeyError, KeyRecord, Revocation,
};

const ACCOUNT: AccountId = AccountId(91);

async fn account(store: &impl AdminStore) {
    store
        .create_account(AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(1000),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        })
        .await
        .unwrap();
}

pub fn schedule(allowance: u64) -> BudgetSchedule {
    BudgetSchedule {
        allowance: CostUnits(allowance),
        period: Period::UtcCalendarMonth,
        rollover: Rollover::None,
    }
}

pub async fn budget_receipts_form_a_serial_history<S: AdminStore + Send + Sync + 'static>(
    store: Arc<S>,
) {
    account(&*store).await;
    let mut tasks = tokio::task::JoinSet::new();
    for allowance in 1..=8 {
        let store = Arc::clone(&store);
        tasks.spawn(async move {
            store
                .set_budget_schedule(ACCOUNT, Some(schedule(allowance)))
                .await
                .unwrap()
        });
    }
    let mut receipts = Vec::new();
    while let Some(result) = tasks.join_next().await {
        receipts.push(result.unwrap());
    }
    let mut previous = AdminState::Budget { schedule: None };
    while !receipts.is_empty() {
        let next = receipts
            .iter()
            .position(|receipt| receipt.before == previous)
            .expect("every successful update joins the actual predecessor");
        previous = receipts.remove(next).after;
    }
    let view = store.account_view(ACCOUNT).await.unwrap().unwrap();
    assert_eq!(
        previous,
        AdminState::Budget {
            schedule: view.schedule
        }
    );
    assert_eq!(view.conservation.balance, CostUnits(1000));
    let cleared = store.set_budget_schedule(ACCOUNT, None).await.unwrap();
    assert_eq!(cleared.before, previous);
    assert_eq!(cleared.after, AdminState::Budget { schedule: None });
    let repeated = store.set_budget_schedule(ACCOUNT, None).await.unwrap();
    assert_eq!(repeated.before, cleared.after);
    assert_eq!(repeated.after, cleared.after);
}

pub async fn credential_receipts_capture_lifecycle<
    S: AdminStore + KeyDirectory + Send + Sync + 'static,
>(
    store: Arc<S>,
) {
    account(&*store).await;
    let now = Timestamp::from_second(100).unwrap();
    let key_id = KeyId(900);
    let record = KeyRecord {
        key_id,
        account_id: ACCOUNT,
        principal: Principal(901),
        digest: [0xab; 32],
        // Expiry and retirement are separate facts, even for a lapsed key.
        not_after: Some(Timestamp::from_second(50).unwrap()),
    };
    let bound = NonZeroUsize::new(1).unwrap();
    let issued = store
        .insert_key_within_audited(record.clone(), bound, now)
        .await
        .unwrap();
    let unrevoked = AdminState::Credential {
        account_id: ACCOUNT,
        key_id,
        revoked: false,
    };
    let revoked = AdminState::Credential {
        account_id: ACCOUNT,
        key_id,
        revoked: true,
    };
    assert_eq!(issued.before, AdminState::Absent);
    assert_eq!(issued.after, unrevoked);
    assert_eq!(
        store
            .insert_key_within_audited(record.clone(), bound, now)
            .await
            .unwrap_err(),
        KeyError::AlreadyExists
    );

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        tasks.spawn(async move { store.revoke_key_audited(key_id, now).await.unwrap() });
    }
    let mut transitions = 0;
    while let Some(result) = tasks.join_next().await {
        let receipt = result.unwrap();
        assert_eq!(receipt.after, revoked);
        match receipt.outcome {
            Revocation::Retired => {
                transitions += 1;
                assert_eq!(receipt.before, unrevoked);
            }
            Revocation::AlreadyRetired => assert_eq!(receipt.before, revoked),
        }
    }
    assert_eq!(
        transitions, 1,
        "exactly one concurrent revocation changes the key"
    );
    let repeated = store.revoke_key_audited(key_id, now).await.unwrap();
    assert_eq!(repeated.before, revoked);
    assert_eq!(repeated.after, revoked);
    assert_eq!(repeated.outcome, Revocation::AlreadyRetired);
    assert_eq!(
        store.revoke_key(key_id, now).await.unwrap(),
        Revocation::AlreadyRetired
    );
    assert_eq!(
        store.insert_key(record).await.unwrap_err(),
        KeyError::AlreadyExists
    );
    assert_eq!(
        store.revoke_key_audited(KeyId(404), now).await.unwrap_err(),
        KeyError::UnknownKey
    );
}
