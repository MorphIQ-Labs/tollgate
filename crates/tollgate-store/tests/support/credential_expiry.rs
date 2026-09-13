//! One scenario shared by the reference and durable backend harnesses.
use std::num::NonZeroUsize;

use jiff::{SignedDuration, Timestamp};
use tollgate_core::{AccountId, KeyId, Principal};
use tollgate_store::{KeyDirectory, KeyRecord};

pub async fn exact_expiry(store: &impl KeyDirectory, account: AccountId) {
    let times = [
        Timestamp::MIN,
        Timestamp::MIN + SignedDuration::from_nanos(1),
        Timestamp::new(-100, -1).unwrap(),
        Timestamp::new(0, -1_001).unwrap(),
        Timestamp::new(0, -1_000).unwrap(),
        Timestamp::new(0, -999).unwrap(),
        Timestamp::new(0, -1).unwrap(),
        Timestamp::UNIX_EPOCH,
        Timestamp::new(0, 1).unwrap(),
        Timestamp::new(0, 999).unwrap(),
        Timestamp::new(0, 1_001).unwrap(),
        Timestamp::new(100, 1).unwrap(),
        Timestamp::MAX,
    ];
    let mut source = vec![(KeyId(0), None)];
    source.extend(
        times
            .iter()
            .enumerate()
            .map(|(i, &expiry)| (KeyId(i as u128 + 1), Some(expiry))),
    );
    for &(key_id, not_after) in &source {
        let mut digest = [0x18; 32];
        digest[..16].copy_from_slice(&key_id.0.to_be_bytes());
        store
            .insert_key(KeyRecord {
                key_id,
                account_id: account,
                principal: Principal(key_id.0),
                digest,
                not_after,
            })
            .await
            .unwrap();
    }
    // A distinct revoked credential with indefinite expiry never leaks into
    // either read, even before the timestamp recorded for its retirement.
    let mut digest = [0x19; 32];
    digest[..16].copy_from_slice(&99_u128.to_be_bytes());
    store
        .insert_key(KeyRecord {
            key_id: KeyId(99),
            account_id: account,
            principal: Principal(99),
            digest,
            not_after: None,
        })
        .await
        .unwrap();
    store.revoke_key(KeyId(99), Timestamp::MAX).await.unwrap();

    let cutoffs = times
        .into_iter()
        .flat_map(|expiry| {
            [
                expiry.checked_sub(SignedDuration::from_nanos(1)).ok(),
                Some(expiry),
            ]
        })
        .flatten();
    let mut revision = None;
    for now in cutoffs {
        let expected: Vec<_> = source
            .iter()
            .copied()
            .filter(|(_, end)| end.is_none_or(|end| now.as_nanosecond() < end.as_nanosecond()))
            .collect();
        let actual: Vec<_> = store
            .active_keys(now)
            .await
            .unwrap()
            .into_iter()
            .map(|key| (key.key_id, key.not_after))
            .collect();
        assert_eq!(actual, expected, "directory at {now}");
        for limit in [1, 3] {
            let mut after = None;
            let mut all = Vec::new();
            loop {
                let page = store
                    .active_keys_page(now, after, NonZeroUsize::new(limit).unwrap())
                    .await
                    .unwrap();
                let expected_revision = *revision.get_or_insert(page.revision());
                assert_eq!(
                    page.revision(),
                    expected_revision,
                    "expiry/read does not mutate revision"
                );
                assert_eq!(page.as_of(), now);
                all.extend(page.records().iter().map(|key| (key.key_id, key.not_after)));
                match page.next_after() {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            assert_eq!(all, expected, "paged projection at {now}, limit {limit}");
        }
    }
}
