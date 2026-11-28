//! One scenario shared by the reference, durable and HTTP harnesses
//! (INVARIANTS.md 34).
use std::num::NonZeroUsize;

use jiff::Timestamp;
use tollgate_core::{AccountId, KeyId, Principal};
use tollgate_store::{KeyDirectory, KeyPage, KeyRecord, KeySource};

/// A record every backend accepts, whose principal is not its digest's: what a
/// faulty issuer, or a damaged row, looks like to every reader.
fn record(account: AccountId, id: u128, readable: bool, not_after: Option<Timestamp>) -> KeyRecord {
    let mut digest = [0x34; 32];
    digest[..16].copy_from_slice(&id.to_be_bytes());
    KeyRecord {
        key_id: KeyId(id),
        account_id: account,
        principal: Principal(if readable { id } else { id + 1_000 }),
        digest,
        not_after,
    }
}

fn ids(page: &KeyPage) -> (Vec<u128>, Vec<u128>, Option<u128>) {
    (
        page.records().iter().map(|key| key.key_id.0).collect(),
        page.unreadable().key_ids.iter().map(|id| id.0).collect(),
        page.next_after().map(|id| id.0),
    )
}

/// Write through `directory` and read through `source`, which selects active
/// credentials at `now`.
pub async fn unreadable_credentials_are_withheld(
    directory: &impl KeyDirectory,
    source: &impl KeySource,
    account: AccountId,
    now: Timestamp,
) {
    let two = NonZeroUsize::new(2).unwrap();
    for id in 1..=9 {
        let readable = ![2, 3, 5, 9].contains(&id);
        // Number 9 has expired, so it is not part of the feed at all.
        let expiry = (id == 9).then_some(now);
        directory
            .insert_key(record(account, id, readable, expiry))
            .await
            .unwrap();
    }
    let read = async |after: Option<u128>, limit| {
        source
            .active_keys_page(now, after.map(KeyId), limit)
            .await
            .unwrap()
    };

    let first = read(None, two).await;
    assert_eq!(ids(&first), (vec![1, 4], vec![2, 3], Some(4)));
    assert_eq!(first.unreadable().unnamed, 0);
    let second = read(Some(4), two).await;
    assert_eq!(ids(&second), (vec![6, 7], vec![5], Some(7)));
    let last = read(Some(7), two).await;
    assert_eq!(ids(&last), (vec![8], vec![], None));
    assert_eq!(first.revision(), last.revision());

    // A page withholds no more than its limit, and continues past what it
    // withheld.
    let one = NonZeroUsize::new(1).unwrap();
    assert_eq!(ids(&read(Some(1), one).await), (vec![], vec![2], Some(2)));
    assert_eq!(ids(&read(Some(2), one).await), (vec![4], vec![3], Some(4)));

    // A final page can withhold its last row.
    directory
        .insert_key(record(account, 10, false, None))
        .await
        .unwrap();
    let ending = read(Some(7), two).await;
    assert_eq!(ids(&ending), (vec![8], vec![10], None));
    assert!(ending.revision() > last.revision());

    // Retiring a withheld credential needs only its id, and clears the report.
    for id in [2, 3, 5, 10] {
        directory.revoke_key(KeyId(id), now).await.unwrap();
    }
    let repaired = read(None, NonZeroUsize::new(16).unwrap()).await;
    assert_eq!(ids(&repaired), (vec![1, 4, 6, 7, 8], vec![], None));
}
