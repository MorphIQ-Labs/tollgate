//! Identical backend scenarios for the account-scoped credential surface
//! (GL-121): the two harnesses supply isolated stores.
//!
//! Written once and included by both suites rather than mirrored by name. GL-85
//! showed that mirrored tests drift *inside* the body where no name diff can
//! see it; a shared scenario cannot drift at all, which is the stronger
//! guarantee where the whole point is that two backends agree.
use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId, Principal};
use tollgate_store::{AccountConfig, AdminStore, KeyDirectory, KeyError, KeyRecord};

pub trait Backend: KeyDirectory + AdminStore {}
impl<T: KeyDirectory + AdminStore> Backend for T {}

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

fn limit(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a test bound is positive")
}

/// Distinct ids per account so a cross-account leak is visible rather than
/// coincidentally equal.
fn key(account: u128, id: u128, not_after: Option<Timestamp>) -> KeyRecord {
    KeyRecord {
        key_id: KeyId(account * 1_000 + id),
        account_id: AccountId(account),
        principal: Principal(account * 1_000_000 + id),
        digest: [u8::try_from(id % 251).expect("fits"); 32],
        not_after,
    }
}

pub async fn accounts(store: &impl Backend) {
    for account in [1u128, 2] {
        AdminStore::create_account(
            store,
            AccountConfig {
                account_id: AccountId(account),
                initial_balance: CostUnits(1_000),
                status: AccountStatus::Active,
                capacity_class: CapacityClass::Assured,
            },
        )
        .await
        .map(|_| ())
        // Each harness supplies an isolated store, but one of them creates its
        // standard account first. The scenario needs these two accounts to
        // exist, not to be the thing that created them.
        .or_else(|error| match error {
            tollgate_store::CreateAccountError::AlreadyExists => Ok(()),
            other => Err(other),
        })
        .expect("account creation succeeds");
    }
}

/// The listing is account-scoped, ordered, and pageable.
pub async fn listing_is_scoped_ordered_and_paged(store: &impl Backend) {
    accounts(store).await;
    // Inserted out of order: the order the listing reports must come from the
    // stored ids, not from insertion or from a map's iteration order.
    for id in [3u128, 1, 2] {
        store.insert_key(key(1, id, None)).await.expect("insert");
    }
    store.insert_key(key(2, 9, None)).await.expect("insert");

    let all = store
        .account_keys(AccountId(1), None, limit(10))
        .await
        .expect("listing succeeds");
    assert_eq!(
        all.iter().map(|k| k.key_id).collect::<Vec<_>>(),
        vec![KeyId(1_001), KeyId(1_002), KeyId(1_003)],
        "one account's credentials, ascending by id, and no other account's"
    );

    let first = store
        .account_keys(AccountId(1), None, limit(2))
        .await
        .expect("listing succeeds");
    assert_eq!(first.len(), 2, "the page honours its limit");
    let rest = store
        .account_keys(AccountId(1), Some(first[1].key_id), limit(10))
        .await
        .expect("listing succeeds");
    assert_eq!(
        rest.iter().map(|k| k.key_id).collect::<Vec<_>>(),
        vec![KeyId(1_003)],
        "`after` is exclusive, so paging never repeats or skips a credential"
    );

    let other = store
        .account_keys(AccountId(2), None, limit(10))
        .await
        .expect("listing succeeds");
    assert_eq!(
        other.iter().map(|k| k.key_id).collect::<Vec<_>>(),
        vec![KeyId(2_009)],
        "an account sees its own credentials only"
    );
}

/// Revoked and expired credentials are listed, and distinguishable.
pub async fn listing_separates_expiry_from_revocation(store: &impl Backend) {
    accounts(store).await;
    store.insert_key(key(1, 1, None)).await.expect("insert");
    store
        .insert_key(key(1, 2, Some(t(100))))
        .await
        .expect("insert");
    store.insert_key(key(1, 3, None)).await.expect("insert");
    store
        .revoke_key(KeyId(1_003), t(50))
        .await
        .expect("revocation succeeds");

    let listed = store
        .account_keys(AccountId(1), None, limit(10))
        .await
        .expect("listing succeeds");
    assert_eq!(listed.len(), 3, "retired credentials stay visible");

    let after_expiry = t(200);
    let live: Vec<KeyId> = listed
        .iter()
        .filter(|summary| summary.is_live(after_expiry))
        .map(|summary| summary.key_id)
        .collect();
    assert_eq!(
        live,
        vec![KeyId(1_001)],
        "one lapsed on its own and one was withdrawn; only the third still authenticates"
    );

    let expired = listed.iter().find(|s| s.key_id == KeyId(1_002)).unwrap();
    let revoked = listed.iter().find(|s| s.key_id == KeyId(1_003)).unwrap();
    assert_eq!(expired.not_after, Some(t(100)));
    assert_eq!(expired.revoked_at, None, "expiry is not revocation");
    assert_eq!(revoked.revoked_at, Some(t(50)));
    assert_eq!(revoked.not_after, None, "revocation is not expiry");
}

/// The bound refuses the credential that would exceed it, and counts only
/// credentials that can still authenticate.
pub async fn the_active_bound_counts_only_live_credentials(store: &impl Backend) {
    accounts(store).await;
    let now = t(100);

    store
        .insert_key_within(key(1, 1, None), limit(2), now)
        .await
        .expect("the first credential fits");
    store
        .insert_key_within(key(1, 2, None), limit(2), now)
        .await
        .expect("the second fills the bound");
    assert_eq!(
        store
            .insert_key_within(key(1, 3, None), limit(2), now)
            .await
            .expect_err("the third exceeds it"),
        KeyError::ActiveKeyLimit { limit: limit(2) }
    );

    // Retiring one makes room: a bound that counted withdrawn credentials
    // would strand the account behind keys nobody can authenticate with.
    store
        .revoke_key(KeyId(1_001), now)
        .await
        .expect("revocation succeeds");
    store
        .insert_key_within(key(1, 3, None), limit(2), now)
        .await
        .expect("the withdrawn credential no longer occupies the bound");

    // So does letting one lapse. The same rule, reached by the other route.
    store
        .insert_key(key(1, 4, Some(t(150))))
        .await
        .expect("insert");
    assert_eq!(
        store
            .insert_key_within(key(1, 5, None), limit(3), now)
            .await
            .expect_err("three live credentials fill a bound of three"),
        KeyError::ActiveKeyLimit { limit: limit(3) }
    );
    store
        .insert_key_within(key(1, 5, None), limit(3), t(200))
        .await
        .expect("past its expiry the lapsed credential no longer counts");
}

/// The bound is per account, and issuance still refuses what `insert_key`
/// refuses.
pub async fn the_bound_is_per_account_and_preserves_issuance_rules(store: &impl Backend) {
    accounts(store).await;
    let now = t(100);
    store
        .insert_key_within(key(1, 1, None), limit(1), now)
        .await
        .expect("insert");
    store
        .insert_key_within(key(2, 1, None), limit(1), now)
        .await
        .expect("another account's credentials do not fill this one's bound");

    // The retry answer. A caller that lost the response resends the same
    // `key_id` and is told the credential exists — true, and disclosing
    // nothing. Checked *under* a bound that is already full, because the
    // duplicate must be reported as a duplicate rather than as a limit.
    assert_eq!(
        store
            .insert_key_within(key(1, 1, None), limit(1), now)
            .await
            .expect_err("a resent credential is a duplicate"),
        KeyError::AlreadyExists
    );

    // Identity is a disjunction, and each side has to be enough on its own.
    // A retry that re-derived its secret resends the same `key_id` under a new
    // principal; a caller that reused a secret sends a new `key_id` under a
    // principal already in use. Both are duplicates. Both are checked under a
    // bound that is already full, because that is the only state where the
    // two answers differ: conjunction in place of disjunction lets a one-sided
    // collision reach the count and come back as `ActiveKeyLimit`, telling the
    // caller to retire a credential over a name it already owns.
    let mut collides_on_key_id = key(1, 1, None);
    collides_on_key_id.principal = Principal(7_000_001);
    assert_eq!(
        store
            .insert_key_within(collides_on_key_id, limit(1), now)
            .await
            .expect_err("the `key_id` alone makes it a duplicate"),
        KeyError::AlreadyExists
    );
    let mut collides_on_principal = key(1, 7, None);
    collides_on_principal.principal = key(1, 1, None).principal;
    assert_eq!(
        store
            .insert_key_within(collides_on_principal, limit(1), now)
            .await
            .expect_err("the principal alone makes it a duplicate"),
        KeyError::AlreadyExists
    );

    assert_eq!(
        store
            .insert_key_within(key(99, 1, None), limit(5), now)
            .await
            .expect_err("no such account"),
        KeyError::UnknownAccount
    );
}

/// Concurrent issuers do not exceed the bound.
///
/// Eight issuers contend for three places against one account. The assertion
/// is on the *total*, never on which issuers won: which three get in is a
/// race, and races are allowed here. Exceeding the bound is not.
///
/// **What this does not do, stated because the obvious reading is wrong.** It
/// is not evidence that `PostgresStore`'s `FOR UPDATE` is required. Measured:
/// with the row lock removed, this passes -- at eight issuers and at
/// thirty-two, repeatedly. The transactions are short enough that they do not
/// overlap at the moment that matters, so the harness cannot force the
/// interleaving the lock defends against. Forcing it would need a
/// synchronisation point *inside* the store call, which is production
/// instrumentation for a test.
///
/// The argument for the lock is therefore the isolation level, not this test:
/// under READ COMMITTED each statement sees only rows committed before it
/// began, so two transactions that count before either commits both read
/// `max_active - 1` and both insert. This scenario is a regression guard on
/// the observable outcome, and it earns its place by pinning that the two
/// backends answer alike -- not by discriminating between a locked and an
/// unlocked implementation.
pub async fn concurrent_issuers_cannot_exceed_the_bound<S>(store: Arc<S>)
where
    S: Backend + Send + Sync + 'static,
{
    accounts(&*store).await;
    let now = t(100);
    const ISSUERS: u128 = 8;
    const PLACES: usize = 3;

    let mut attempts = tokio::task::JoinSet::new();
    for id in 1..=ISSUERS {
        let store = Arc::clone(&store);
        attempts.spawn(async move {
            store
                .insert_key_within(key(1, id, None), limit(PLACES), now)
                .await
        });
    }

    let mut issued = 0usize;
    while let Some(outcome) = attempts.join_next().await {
        match outcome.expect("no issuer panicked") {
            Ok(()) => issued += 1,
            Err(KeyError::ActiveKeyLimit { limit: refused }) => {
                assert_eq!(refused, limit(PLACES), "the refusal names the bound it hit");
            }
            Err(other) => panic!("unexpected issuance failure: {other}"),
        }
    }
    assert_eq!(
        issued, PLACES,
        "exactly the bound was issued; a count released before the write would admit more"
    );

    let live = store
        .account_keys(AccountId(1), None, limit(32))
        .await
        .expect("listing succeeds")
        .into_iter()
        .filter(|summary| summary.is_live(now))
        .count();
    assert_eq!(
        live, PLACES,
        "and the stored state agrees with the answers given"
    );
}

/// Both APIs contend on each uniqueness constraint and agree on the loser.
pub async fn mixed_issuers_report_duplicates<S>(store: Arc<S>)
where
    S: Backend + Send + Sync + 'static,
{
    accounts(&*store).await;
    for same_key in [true, false] {
        let id = if same_key { 10 } else { 20 };
        let first = key(1, id, None);
        let mut second = key(1, id + 1, None);
        if same_key {
            second.key_id = first.key_id;
        } else {
            second.principal = first.principal;
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let unbounded_store = Arc::clone(&store);
        let unbounded_barrier = Arc::clone(&barrier);
        let unbounded = tokio::spawn(async move {
            unbounded_barrier.wait().await;
            unbounded_store.insert_key(first).await
        });
        let bounded_store = Arc::clone(&store);
        let bounded = tokio::spawn(async move {
            barrier.wait().await;
            bounded_store
                .insert_key_within(second, limit(32), t(100))
                .await
        });
        let outcomes = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            [unbounded.await.unwrap(), bounded.await.unwrap()]
        })
        .await
        .expect("both issuers complete");
        assert!(
            matches!(
                outcomes,
                [Ok(()), Err(KeyError::AlreadyExists)] | [Err(KeyError::AlreadyExists), Ok(())]
            ),
            "one issuer wins and the other reports the duplicate: {outcomes:?}"
        );
    }
    assert_eq!(
        store
            .account_keys(AccountId(1), None, limit(32))
            .await
            .unwrap()
            .len(),
        2
    );
}

/// An absent owner is not an empty page, including with a cursor past all keys.
pub async fn listing_distinguishes_unknown_from_empty(store: &impl Backend) {
    let account = AccountId(41);
    for after in [None, Some(KeyId(u128::MAX))] {
        assert_eq!(
            store
                .account_keys(account, after, limit(1))
                .await
                .unwrap_err(),
            KeyError::UnknownAccount,
        );
    }
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: account,
            initial_balance: CostUnits::ZERO,
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    for after in [None, Some(KeyId(u128::MAX))] {
        assert!(
            store
                .account_keys(account, after, limit(1))
                .await
                .unwrap()
                .is_empty()
        );
    }
    store.insert_key(key(41, 1, None)).await.unwrap();
    assert_eq!(
        store.account_keys(account, None, limit(1)).await.unwrap()[0].key_id,
        KeyId(41_001)
    );
    assert!(
        store
            .account_keys(account, Some(KeyId(41_001)), limit(1))
            .await
            .unwrap()
            .is_empty()
    );
}
