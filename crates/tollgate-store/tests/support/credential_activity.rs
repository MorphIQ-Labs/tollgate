//! Identical backend scenarios: the two harnesses supply isolated stores.
use jiff::Timestamp;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    KeyId, PermissionBits, PolicyRevision, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AdminStore, CredentialActivityState as State, KeyDirectory, KeyRecord,
    PublishSnapshotError, UsageSink,
};

pub trait Backend: KeyDirectory + UsageSink + AdminStore + tollgate_store::SnapshotSource {}
impl<T: KeyDirectory + UsageSink + AdminStore + tollgate_store::SnapshotSource> Backend for T {}

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

pub async fn setup(store: &impl Backend) {
    for account in [1, 2] {
        AdminStore::create_account(
            store,
            AccountConfig {
                account_id: AccountId(account),
                initial_balance: CostUnits(1000),
                status: AccountStatus::Active,
                capacity_class: CapacityClass::Assured,
            },
        )
        .await
        .unwrap();
    }
    for (id, account) in [(1, 1), (2, 1), (3, 2)] {
        let principal = Principal(id + 100);
        let mut digest = [0x99; 32];
        digest[..16].copy_from_slice(&principal.0.to_be_bytes());
        store
            .insert_key(KeyRecord {
                key_id: KeyId(id),
                account_id: AccountId(account),
                principal,
                digest,
                not_after: if id == 2 { Some(t(5)) } else { None },
            })
            .await
            .unwrap();
    }
}

pub fn event(id: u128, key: Option<u128>, at: i64) -> UsageEvent {
    UsageEvent::new(
        RequestId(id),
        AccountId(1),
        UsageSource::Overage,
        CostUnits(1),
        t(at),
        PolicyRevision::UNSTATED,
        key.map(KeyId),
    )
}

pub fn snapshot(key: Option<KeyId>, account: AccountId, generation: u64) -> PublishableSnapshot {
    let mut snapshot = AccountSnapshot::builder(
        account,
        Generation(generation),
        AccountStatus::Active,
        t(10000),
        PermissionBits::bit(0),
        ResolvedLimits::new(10),
        Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .build();
    snapshot.key_id = key;
    PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap()
}

async fn state(store: &impl Backend, id: u128) -> State {
    store.credential_activity(&[KeyId(id)]).await.unwrap()[0].state
}

pub async fn committed_usage_attributes_each_event_and_preserves_replay_identity(
    store: &impl Backend,
) {
    assert_eq!(
        store.ingest(&[], t(0)).await.unwrap(),
        tollgate_store::IngestReport {
            accepted: 0,
            duplicate: 0,
            rejected: 0,
            unattributed: Some(0),
        },
        "an empty batch confirms zero attribution loss, not unavailable reporting",
    );
    let original = event(1, Some(1), 20);
    let altered = event(1, Some(2), 90);
    let report = store
        .ingest(&[original, altered, event(2, Some(1), 10)], t(100))
        .await
        .unwrap();
    assert_eq!(
        (
            report.accepted,
            report.duplicate,
            report.rejected,
            report.unattributed
        ),
        (2, 1, 0, Some(0))
    );
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(20)
        }
    );
    assert_eq!(state(store, 2).await, State::Unobserved);
    let report = store
        .ingest(&[altered, event(2, Some(1), 500), original], t(600))
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.unattributed),
        (0, 3, Some(0))
    );
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(20)
        }
    );
    assert_eq!(state(store, 2).await, State::Unobserved);
    // Two attributable events for one key advance zero rows at an older time.
    let report = store
        .ingest(&[event(3, Some(1), 9), event(4, Some(1), 8)], t(600))
        .await
        .unwrap();
    assert_eq!((report.accepted, report.unattributed), (2, Some(0)));
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(20)
        }
    );
    store
        .ingest(&[event(5, Some(1), 30)], t(600))
        .await
        .unwrap();
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(30)
        }
    );
}

pub async fn missing_unknown_and_wrong_account_attribution_preserve_billing(store: &impl Backend) {
    let mut rejected = event(9, Some(1), 900);
    rejected.account_id = AccountId(999);
    let events = [
        event(1, None, 10),
        event(2, Some(404), 20),
        event(3, Some(3), 30),
        event(4, Some(1), 40),
        rejected,
    ];
    let report = store.ingest(&events, t(1000)).await.unwrap();
    assert_eq!(
        (
            report.accepted,
            report.duplicate,
            report.rejected,
            report.unattributed
        ),
        (4, 0, 1, Some(3))
    );
    report.validate(events.len()).unwrap();
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(40)
        }
    );
    assert_eq!(state(store, 3).await, State::Unobserved);
    assert_eq!(state(store, 404).await, State::Unknown);
    let replay = store.ingest(&events, t(2000)).await.unwrap();
    assert_eq!(
        (replay.duplicate, replay.rejected, replay.unattributed),
        (4, 1, Some(0))
    );
}

pub async fn retired_activity_never_changes_the_credential_revision(store: &impl Backend) {
    store.revoke_key(KeyId(1), t(3)).await.unwrap();
    let limit = NonZeroUsize::new(10).unwrap();
    let before = store.active_keys_page(t(10), None, limit).await.unwrap();
    store
        .ingest(&[event(1, Some(1), 2), event(2, Some(2), 4)], t(20))
        .await
        .unwrap();
    let after = store.active_keys_page(t(10), None, limit).await.unwrap();
    assert_eq!(before.revision(), after.revision());
    assert_eq!(before.records(), after.records());
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(2)
        }
    );
    assert_eq!(
        state(store, 2).await,
        State::Committed {
            last_committed_at: t(4)
        }
    );
}

pub async fn activity_reads_preserve_every_requested_key_and_its_state(store: &impl Backend) {
    store.ingest(&[event(1, Some(1), 10)], t(20)).await.unwrap();
    assert!(store.credential_activity(&[]).await.unwrap().is_empty());
    let keys: Vec<_> = [KeyId(404), KeyId(1), KeyId(2), KeyId(1)]
        .into_iter()
        .cycle()
        .take(tollgate_store::MAX_INGEST_BATCH + 5)
        .collect();
    let values = store.credential_activity(&keys).await.unwrap();
    assert_eq!(values.len(), keys.len());
    for (&key_id, value) in keys.iter().zip(values) {
        assert_eq!(value.key_id, key_id);
        assert_eq!(
            value.state,
            match key_id.0 {
                404 => State::Unknown,
                1 => State::Committed {
                    last_committed_at: t(10)
                },
                2 => State::Unobserved,
                _ => unreachable!(),
            }
        );
    }
}

pub async fn publication_checks_the_stated_credential_binding(store: &impl Backend) {
    use tollgate_store::SnapshotResolution;
    AdminStore::publish_snapshot(
        store,
        Principal(101),
        snapshot(Some(KeyId(1)), AccountId(1), 1),
    )
    .await
    .unwrap();
    let mut pushes = store.subscribe();
    for (key, account) in [(2, 1), (3, 1), (1, 2), (404, 1)] {
        let error = AdminStore::publish_snapshot(
            store,
            Principal(101),
            snapshot(Some(KeyId(key)), AccountId(account), 2),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            PublishSnapshotError::CredentialMismatch { key_id: KeyId(key) }
        );
        assert!(pushes.try_recv().is_err());
        let SnapshotResolution::Present(current) = store.snapshot(Principal(101)).await.unwrap()
        else {
            panic!("lost predecessor")
        };
        assert_eq!(current.generation, Generation(1));
    }
    AdminStore::publish_snapshot(store, Principal(404), snapshot(None, AccountId(1), 1))
        .await
        .unwrap();
}

pub async fn concurrent_activity_commits_converge_to_the_maximum(store: &impl Backend) {
    let a = [event(1, Some(1), 30), event(2, Some(2), 40)];
    let b = [event(3, Some(2), 20), event(4, Some(1), 10)];
    let (a, b) = tokio::join!(store.ingest(&a, t(100)), store.ingest(&b, t(100)));
    assert_eq!(a.unwrap().accepted + b.unwrap().accepted, 4);
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(30)
        }
    );
    assert_eq!(
        state(store, 2).await,
        State::Committed {
            last_committed_at: t(40)
        }
    );
}

pub async fn activity_uses_durable_microsecond_precision(store: &impl Backend) {
    for (id, at) in [
        Timestamp::MIN,
        "1969-12-31T23:59:59.999999123Z".parse().unwrap(),
        "2026-09-09T12:00:00.123456789Z".parse().unwrap(),
        Timestamp::MAX,
    ]
    .into_iter()
    .enumerate()
    {
        let mut event = event(id as u128 + 1, Some(1), 0);
        event.occurred_at = at;
        store.ingest(&[event], t(0)).await.unwrap();
        assert_eq!(
            state(store, 1).await,
            State::Committed {
                last_committed_at: Timestamp::new(
                    at.as_second(),
                    at.subsec_nanosecond() / 1_000 * 1_000
                )
                .unwrap(),
            }
        );
    }
}

pub async fn a_failed_batch_preserves_activity_and_canonical_events(
    store: &impl Backend,
    unit_ceiling: u64,
) {
    store.ingest(&[event(1, Some(1), 10)], t(20)).await.unwrap();
    let mut overflow = event(3, Some(1), 30);
    overflow.units = CostUnits(unit_ceiling);
    let error = store
        .ingest(&[event(2, Some(1), 20), overflow], t(40))
        .await
        .unwrap_err();
    assert!(!error.is_retryable());
    assert_eq!(
        state(store, 1).await,
        State::Committed {
            last_committed_at: t(10)
        }
    );
    let retry = store.ingest(&[event(2, Some(1), 20)], t(50)).await.unwrap();
    assert_eq!((retry.accepted, retry.duplicate), (1, 0));
}

fn generation_of(resolution: tollgate_store::SnapshotResolution) -> Option<(u64, bool)> {
    use tollgate_store::SnapshotResolution;
    match resolution {
        SnapshotResolution::Present(snapshot) => Some((snapshot.generation.0, false)),
        SnapshotResolution::Revoked { generation } => Some((generation.0, true)),
        SnapshotResolution::Unknown => None,
    }
}

fn snapshot_state(generation: u64, revoked: bool) -> tollgate_store::AdminState {
    tollgate_store::AdminState::Snapshot {
        generation: Generation(generation),
        revoked,
    }
}

/// An operator names a credential by `(account, key)`; the store resolves
/// its principal and applies every rule the principal route applies (GL-143).
pub async fn key_bound_publication_resolves_the_principal_in_the_store(store: &impl Backend) {
    use tollgate_store::{AdminState, SnapshotResolution};
    let mut pushes = store.subscribe();
    let receipt = store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(1),
            snapshot(Some(KeyId(1)), AccountId(1), 1),
        )
        .await
        .unwrap();
    assert_eq!(
        (receipt.before, receipt.after),
        (AdminState::Absent, snapshot_state(1, false))
    );
    let push = pushes.try_recv().expect("a publication is pushed");
    assert_eq!(
        push.principal,
        Principal(101),
        "bound to the key's own principal"
    );
    assert!(matches!(push.resolution, SnapshotResolution::Present(_)));
    assert_eq!(
        generation_of(store.snapshot(Principal(101)).await.unwrap()),
        Some((1, false))
    );

    // A replay at or below the stored generation is a silent no-op (GL-15).
    let replay = store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(1),
            snapshot(Some(KeyId(1)), AccountId(1), 1),
        )
        .await
        .unwrap();
    assert_eq!(replay.before, replay.after);
    assert!(pushes.try_recv().is_err());

    let removed = store
        .remove_key_snapshot(AccountId(1), KeyId(1))
        .await
        .unwrap();
    assert_eq!(
        (removed.before, removed.after),
        (snapshot_state(1, false), snapshot_state(1, true))
    );
    assert!(matches!(
        pushes
            .try_recv()
            .expect("a withdrawal is pushed")
            .resolution,
        SnapshotResolution::Revoked { .. }
    ));
    let again = store
        .remove_key_snapshot(AccountId(1), KeyId(1))
        .await
        .unwrap();
    assert_eq!(
        (again.before, again.after),
        (snapshot_state(1, true), snapshot_state(1, true))
    );
    assert!(
        pushes.try_recv().is_err(),
        "an unchanged tombstone is not re-announced"
    );

    // The tombstone keeps its generation: only a strictly newer one revives.
    let stale = store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(1),
            snapshot(Some(KeyId(1)), AccountId(1), 1),
        )
        .await
        .unwrap();
    assert_eq!(stale.before, stale.after);
    store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(1),
            snapshot(Some(KeyId(1)), AccountId(1), 2),
        )
        .await
        .unwrap();
    assert_eq!(
        generation_of(store.snapshot(Principal(101)).await.unwrap()),
        Some((2, false))
    );
}

/// A foreign or unknown key, and a snapshot not stating the path's key or
/// account, publish nothing and push nothing.
pub async fn key_bound_publication_is_bound_to_the_account_and_key(store: &impl Backend) {
    use tollgate_store::KeySnapshotError;
    let mut pushes = store.subscribe();
    for (account, key) in [(2, 1), (1, 404), (1, 3)] {
        assert_eq!(
            store
                .publish_key_snapshot(
                    AccountId(account),
                    KeyId(key),
                    snapshot(Some(KeyId(key)), AccountId(account), 1)
                )
                .await
                .unwrap_err(),
            KeySnapshotError::UnknownCredential,
            "account {account}, key {key}"
        );
        assert_eq!(
            store
                .remove_key_snapshot(AccountId(account), KeyId(key))
                .await
                .unwrap_err(),
            KeySnapshotError::UnknownCredential
        );
    }
    for stated in [
        snapshot(None, AccountId(1), 1),
        snapshot(Some(KeyId(2)), AccountId(1), 1),
        snapshot(Some(KeyId(1)), AccountId(2), 1),
    ] {
        assert_eq!(
            store
                .publish_key_snapshot(AccountId(1), KeyId(1), stated)
                .await
                .unwrap_err(),
            KeySnapshotError::Publish(PublishSnapshotError::CredentialMismatch {
                key_id: KeyId(1)
            })
        );
    }
    assert!(pushes.try_recv().is_err());
    for principal in [101, 102, 103] {
        assert_eq!(
            generation_of(store.snapshot(Principal(principal)).await.unwrap()),
            None
        );
    }
}

/// Revocation is terminal (INVARIANTS.md GL-27): a retired credential is never
/// granted a snapshot again, but its snapshot can still be withdrawn. Expiry
/// is not retirement.
pub async fn a_retired_credential_is_never_granted_a_snapshot_but_can_be_withdrawn(
    store: &impl Backend,
) {
    use tollgate_store::KeySnapshotError;
    store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(1),
            snapshot(Some(KeyId(1)), AccountId(1), 1),
        )
        .await
        .unwrap();
    store.revoke_key(KeyId(1), t(1)).await.unwrap();
    assert_eq!(
        store
            .publish_key_snapshot(
                AccountId(1),
                KeyId(1),
                snapshot(Some(KeyId(1)), AccountId(1), 2)
            )
            .await
            .unwrap_err(),
        KeySnapshotError::Retired { key_id: KeyId(1) }
    );
    assert_eq!(
        generation_of(store.snapshot(Principal(101)).await.unwrap()),
        Some((1, false))
    );
    let removed = store
        .remove_key_snapshot(AccountId(1), KeyId(1))
        .await
        .unwrap();
    assert_eq!(removed.after, snapshot_state(1, true));

    // Key 2 expired at t(5) but was never revoked.
    store
        .publish_key_snapshot(
            AccountId(1),
            KeyId(2),
            snapshot(Some(KeyId(2)), AccountId(1), 1),
        )
        .await
        .unwrap();
    assert_eq!(
        generation_of(store.snapshot(Principal(102)).await.unwrap()),
        Some((1, false))
    );
}
