//! The declared wire limits, pinned against the types they describe (GL-61).
//!
//! `MAX_INGEST_BODY_BYTES` is derived from `MAX_USAGE_EVENT_BYTES` and
//! `MAX_INGEST_BATCH`. If a field is added to `UsageEvent` and that constant
//! is not revised, a legitimate maximal batch starts being refused by the
//! server as too large — a failure that would appear as a production outage
//! rather than a build error, so it is checked here instead.

use jiff::Timestamp;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    KeyId, LeaseId, PermissionBits, PolicyRevision, RequestId, ResolvedLimits, UsageEvent,
    UsageSource,
};
use tollgate_store::MAX_INGEST_BATCH;
use tollgate_store::wire::{
    MAX_INGEST_BODY_BYTES, MAX_SNAPSHOT_BODY_BYTES, MAX_USAGE_EVENT_BYTES, PublishSnapshotRequest,
};

/// Every identifier at full width, the policy revision at full width, both
/// 64-bit fields at their maximum, and
/// the timestamp at the far end of the representable range. The leased form,
/// which carries a lease id and fencing token the overage form does not.
fn widest_event() -> UsageEvent {
    UsageEvent::new(
        RequestId(u128::MAX),
        AccountId(u128::MAX),
        UsageSource::Leased {
            lease_id: LeaseId(u128::MAX),
            fencing_token: FencingToken(u64::MAX),
        },
        CostUnits(u64::MAX),
        Timestamp::MIN
            .checked_add(jiff::SignedDuration::from_nanos(999_999_999))
            .unwrap(),
        PolicyRevision([0xff; 32]),
        Some(KeyId(u128::MAX)),
    )
}

#[test]
fn the_widest_usage_event_still_fits_its_declared_size() {
    let encoded = serde_json::to_string(&widest_event()).expect("a usage event serialises");
    assert!(
        encoded.len() <= MAX_USAGE_EVENT_BYTES,
        "the widest usage event is {} bytes against a declared {MAX_USAGE_EVENT_BYTES}; \
         the body limit derived from it no longer admits a full batch",
        encoded.len()
    );
}

#[test]
fn a_full_batch_of_the_widest_events_fits_the_declared_body_limit() {
    // What the server must accept: the largest batch a conforming client can
    // send, with every event at its widest. Envelope plus one comma per event
    // after the first.
    let envelope = r#"{"events":[]}"#.len();
    let worst = envelope + MAX_INGEST_BATCH * (MAX_USAGE_EVENT_BYTES + 1);
    assert!(
        worst <= MAX_INGEST_BODY_BYTES,
        "a maximal legitimate batch is {worst} bytes against a body limit of \
         {MAX_INGEST_BODY_BYTES}: the server would refuse a batch the client \
         is entitled to send"
    );
    let events = vec![widest_event(); MAX_INGEST_BATCH];
    let body =
        serde_json::to_vec(&tollgate_store::wire::IngestRequestRef { events: &events }).unwrap();
    assert!(body.len() <= MAX_INGEST_BODY_BYTES);
}

#[test]
fn activity_wire_preserves_identity_and_distinguishes_unknown_reporting() {
    #[derive(serde::Deserialize)]
    struct LegacyUsageEvent {
        request_id: RequestId,
        account_id: AccountId,
        source: UsageSource,
        units: CostUnits,
        occurred_at: Timestamp,
        policy_revision: PolicyRevision,
    }
    let original = widest_event();
    let mut value = serde_json::to_value(original).unwrap();
    let legacy: LegacyUsageEvent = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(legacy.request_id, original.request_id);
    assert_eq!(legacy.account_id, original.account_id);
    assert_eq!(legacy.source, original.source);
    assert_eq!(legacy.units, original.units);
    assert_eq!(legacy.occurred_at, original.occurred_at);
    assert_eq!(legacy.policy_revision, original.policy_revision);
    assert_eq!(value["key_id"], "f".repeat(32));
    assert_eq!(
        serde_json::from_value::<UsageEvent>(value.clone()).unwrap(),
        original
    );
    value.as_object_mut().unwrap().remove("key_id");
    assert_eq!(
        serde_json::from_value::<UsageEvent>(value).unwrap().key_id,
        None
    );

    let legacy = r#"{"accepted":1,"duplicate":0,"rejected":0}"#;
    let report: tollgate_store::IngestReport = serde_json::from_str(legacy).unwrap();
    assert_eq!(report.unattributed, None);
    report.validate(1).unwrap();
    let report: tollgate_store::IngestReport =
        serde_json::from_str(r#"{"accepted":1,"duplicate":0,"rejected":0,"unattributed":0}"#)
            .unwrap();
    assert_eq!(report.unattributed, Some(0));
}

#[test]
fn maximal_usage_acknowledgement_fits_its_declared_limit() {
    let report = tollgate_store::IngestReport {
        accepted: u64::MAX,
        duplicate: u64::MAX,
        rejected: u64::MAX,
        unattributed: Some(u64::MAX),
    };
    // A representation bound, not a claim that this is a valid batch report.
    let encoded = serde_json::to_vec(&report).unwrap();
    assert!(
        encoded.len() <= tollgate_store::wire::MAX_INGEST_REPORT_BYTES,
        "{}",
        encoded.len()
    );
}

/// The snapshot limit admits the catalogue its doc claims it does.
///
/// `MAX_SNAPSHOT_BODY_BYTES` was justified by a sentence, and a sentence is
/// not a limit — the mutation gate found the constant unconstrained by
/// anything, so a build that shrank it fourfold would have shipped.
///
/// A snapshot carries its whole cost table, including per-operation rights.
/// Full-width weights and permissions cost about 32 bytes per class in the
/// parallel arrays. The fixture must also pass publication validation: one
/// maximal quote fits its burst, and every required permission is granted.
#[test]
fn the_snapshot_limit_admits_a_hundred_thousand_class_catalogue() {
    const CLASSES: usize = 100_000;
    let mut builder = CostTable::builder(CostUnits(1), CostUnits(1));
    for class in 0..CLASSES {
        builder = builder.class(
            &Class(class),
            CostUnits(u64::from(u32::MAX - 1)),
            PermissionBits::ALL,
        );
    }
    let snapshot = AccountSnapshot::builder(
        AccountId(u128::MAX),
        Generation(u64::MAX),
        AccountStatus::Active,
        Timestamp::MIN
            .checked_add(jiff::SignedDuration::from_nanos(999_999_999))
            .unwrap(),
        PermissionBits::ALL,
        ResolvedLimits::new(1).with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX)),
        std::sync::Arc::new(builder.build()),
    )
    .key_id(KeyId(u128::MAX))
    .build();

    let snapshot = std::sync::Arc::new(snapshot);
    tollgate_core::PublishableSnapshot::try_new(snapshot.clone()).unwrap();
    let encoded =
        serde_json::to_string(&PublishSnapshotRequest { snapshot }).expect("a snapshot serialises");
    assert!(
        encoded.len() <= MAX_SNAPSHOT_BODY_BYTES,
        "a {CLASSES}-class catalogue is {} bytes against a limit of \
         {MAX_SNAPSHOT_BODY_BYTES}: an operator publishing it would be refused",
        encoded.len()
    );
}

/// One `OpIndex` per class, so the table is built at its full width.
#[derive(Clone, Copy)]
struct Class(usize);

impl tollgate_core::OpIndex for Class {
    fn index(&self) -> usize {
        self.0
    }
}

#[test]
fn maximal_key_pages_fit_the_derived_envelope_and_digests_are_canonical() {
    use tollgate_core::Principal;
    use tollgate_store::wire::{KeysResponse, MAX_KEY_RECORD_BYTES, MAX_KEYS_BODY_BYTES};
    use tollgate_store::{CredentialRecord, MAX_KEY_PAGE_LIMIT, MAX_KEY_REVISION};
    // The negative year and maximum fractional precision are wider than MAX.
    let expiry: Timestamp = "-009998-01-02T01:02:03.123456789Z".parse().unwrap();
    let record = CredentialRecord {
        key_id: KeyId(u128::MAX),
        principal: Principal(u128::MAX),
        digest: [0xff; 32],
        not_after: Some(expiry),
    };
    let encoded = serde_json::to_vec(&record).unwrap();
    assert_eq!(encoded.len(), MAX_KEY_RECORD_BYTES);
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["digest"], "f".repeat(64));
    assert_eq!(
        serde_json::from_slice::<CredentialRecord>(&encoded).unwrap(),
        record
    );
    let page = KeysResponse {
        revision: MAX_KEY_REVISION,
        as_of: expiry,
        keys: vec![record; MAX_KEY_PAGE_LIMIT],
        next_after: Some(KeyId(u128::MAX)),
    };
    let encoded = serde_json::to_vec(&page).unwrap();
    assert_eq!(encoded.len(), MAX_KEYS_BODY_BYTES);
    assert!(encoded.len() > MAX_KEYS_BODY_BYTES / 2);
    for bad in [
        "f".repeat(63),
        "f".repeat(65),
        "F".repeat(64),
        "g".repeat(64),
    ] {
        let mut invalid = value.clone();
        invalid["digest"] = serde_json::json!(bad);
        assert!(serde_json::from_value::<CredentialRecord>(invalid).is_err());
    }
    let mut invalid = value;
    invalid.as_object_mut().unwrap().remove("not_after");
    assert!(serde_json::from_value::<CredentialRecord>(invalid).is_err());
    let mut page = serde_json::to_value(page).unwrap();
    page.as_object_mut().unwrap().remove("next_after");
    assert!(serde_json::from_value::<KeysResponse>(page).is_err());
}
