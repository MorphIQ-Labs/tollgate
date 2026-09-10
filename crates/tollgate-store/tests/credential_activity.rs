mod support {
    pub mod credential_activity;
}
use tollgate_store::{GrantPolicy, MemoryStore};

#[tokio::test]
async fn inherent_publication_cannot_bypass_credential_or_account_validation() {
    use tollgate_core::{AccountId, AccountStatus, CapacityClass, KeyId, Principal};
    use tollgate_store::{AdminStore, PublishSnapshotError};
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    assert_eq!(
        store.publish_snapshot(
            Principal(101),
            support::credential_activity::snapshot(Some(KeyId(2)), AccountId(1), 1)
        ),
        Err(PublishSnapshotError::CredentialMismatch { key_id: KeyId(2) })
    );
    AdminStore::set_account_status(&*store, AccountId(1), AccountStatus::Suspended)
        .await
        .unwrap();
    assert!(matches!(
        store.publish_snapshot(
            Principal(101),
            support::credential_activity::snapshot(None, AccountId(1), 1)
        ),
        Err(PublishSnapshotError::StatusMismatch { .. })
    ));
    AdminStore::set_capacity_class(&*store, AccountId(2), CapacityClass::BestEffort)
        .await
        .unwrap();
    assert!(matches!(
        store.publish_snapshot(
            Principal(103),
            support::credential_activity::snapshot(None, AccountId(2), 1)
        ),
        Err(PublishSnapshotError::CapacityClassMismatch { .. })
    ));
}

#[test]
fn malformed_usage_acknowledgements_are_never_complete_evidence() {
    use tollgate_store::IngestReport;
    for report in [
        IngestReport {
            accepted: 1,
            duplicate: 1,
            rejected: 1,
            unattributed: Some(0),
        },
        IngestReport {
            accepted: 0,
            duplicate: 0,
            rejected: 0,
            unattributed: None,
        },
        IngestReport {
            accepted: u64::MAX,
            duplicate: 1,
            rejected: 0,
            unattributed: Some(0),
        },
        IngestReport {
            accepted: 0,
            duplicate: 1,
            rejected: 0,
            unattributed: Some(1),
        },
    ] {
        assert!(report.validate(1).is_err());
    }
    IngestReport {
        accepted: 1,
        duplicate: 1,
        rejected: 1,
        unattributed: Some(1),
    }
    .validate(3)
    .unwrap();
    IngestReport::default().validate(0).unwrap();
}

#[test]
fn durable_timestamp_conversion_covers_the_complete_domain() {
    use jiff::Timestamp;
    use tollgate_store::clock::timestamp_from_micros;
    for value in [
        i64::MIN,
        Timestamp::MIN.as_microsecond() - 1,
        Timestamp::MAX.as_microsecond() + 1,
        i64::MAX,
    ] {
        assert!(timestamp_from_micros(value).is_err());
    }
    assert_eq!(
        timestamp_from_micros(-1).unwrap().to_string(),
        "1969-12-31T23:59:59.999999Z"
    );
}

#[test]
fn credential_record_debug_keeps_identity_but_excludes_the_digest() {
    let record = tollgate_store::KeyRecord {
        key_id: tollgate_core::KeyId(1),
        account_id: tollgate_core::AccountId(2),
        principal: tollgate_core::Principal(3),
        digest: [0x9b; 32],
        not_after: None,
    };
    let rendered = format!("{record:?}");
    assert!(rendered.contains("key_id") && rendered.contains("account_id"));
    assert!(!rendered.contains(&format!("{:?}", record.digest)));
    assert!(!rendered.contains(&"9b".repeat(32)));
}

#[tokio::test]
async fn committed_usage_attributes_each_event_and_preserves_replay_identity() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::committed_usage_attributes_each_event_and_preserves_replay_identity(&*store).await;
}

#[tokio::test]
async fn missing_unknown_and_wrong_account_attribution_preserve_billing() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::missing_unknown_and_wrong_account_attribution_preserve_billing(
        &*store,
    )
    .await;
}

#[tokio::test]
async fn retired_activity_never_changes_the_credential_revision() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::retired_activity_never_changes_the_credential_revision(&*store)
        .await;
}

#[tokio::test]
async fn activity_reads_preserve_every_requested_key_and_its_state() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::activity_reads_preserve_every_requested_key_and_its_state(
        &*store,
    )
    .await;
}

#[tokio::test]
async fn publication_checks_the_stated_credential_binding() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::publication_checks_the_stated_credential_binding(&*store).await;
}

#[tokio::test]
async fn concurrent_activity_commits_converge_to_the_maximum() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::concurrent_activity_commits_converge_to_the_maximum(&*store)
        .await;
}

#[tokio::test]
async fn activity_uses_durable_microsecond_precision() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::activity_uses_durable_microsecond_precision(&*store).await;
}

#[tokio::test]
async fn a_failed_batch_preserves_activity_and_canonical_events() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    support::credential_activity::setup(&*store).await;
    support::credential_activity::a_failed_batch_preserves_activity_and_canonical_events(&*store)
        .await;
}
