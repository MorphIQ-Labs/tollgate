//! The declared wire limits, pinned against the types they describe (#61).
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
        Timestamp::from_second(253_402_207_200).unwrap(),
        PolicyRevision([0xff; 32]),
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
}

/// The snapshot limit admits the catalogue its doc claims it does.
///
/// `MAX_SNAPSHOT_BODY_BYTES` was justified by a sentence, and a sentence is
/// not a limit — the mutation gate found the constant unconstrained by
/// anything, so a build that shrank it fourfold would have shipped.
///
/// A snapshot carries its whole cost table, so a catalogue is the input the
/// number has to admit. Measured: a class costs about eleven bytes on the
/// wire, the table serialising as parallel arrays rather than as named
/// objects, so a hundred thousand of them is a little over a megabyte. That
/// is the size this asserts, and it is what makes the assertion bite: at a
/// hundred thousand classes the encoding is large enough that a shrunken
/// limit fails, where the four thousand this first used was not.
#[test]
fn the_snapshot_limit_admits_a_hundred_thousand_class_catalogue() {
    const CLASSES: usize = 100_000;
    let mut builder = CostTable::builder(CostUnits(1), CostUnits(1));
    for class in 0..CLASSES {
        builder = builder.weight(&Class(class), CostUnits(u64::from(u32::MAX)));
    }
    let snapshot = AccountSnapshot::builder(
        AccountId(u128::MAX),
        Generation(u64::MAX),
        AccountStatus::Active,
        Timestamp::from_second(253_402_207_200).unwrap(),
        PermissionBits::ALL,
        ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
        std::sync::Arc::new(builder.build()),
    )
    .key_id(KeyId(u128::MAX))
    .build();

    let encoded = serde_json::to_string(&PublishSnapshotRequest {
        snapshot: std::sync::Arc::new(snapshot),
    })
    .expect("a snapshot serialises");
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
