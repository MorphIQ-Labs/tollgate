//! The HTTP wire contract between `tollgate-server` and `tollgate-client`'s HTTP
//! transport. One place, versioned by [`API_PREFIX`] on the server.
//!
//! Every 128-bit identifier is exactly 32 lowercase hexadecimal characters,
//! without a `0x` prefix, in both JSON strings and URL path segments. JSON
//! numbers are not portable for these values: common consumers round integers
//! above 2^53. Snapshots travel whole, cost table included — the receiving
//! instance validates them and resolves nothing again.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostUnits, FencingToken, Generation,
    LeaseGrant, LeaseId, UsageEvent,
};

/// Current HTTP wire-contract prefix.
///
/// The pre-public V1 contract uses canonical textual identifiers. Keeping the
/// prefix here makes client and server select the same contract without
/// duplicating a magic string.
pub const API_PREFIX: &str = "/v1";

/// The widest a single [`UsageEvent`] serialises to, in bytes.
///
/// Measured, not estimated: every identifier at its full 32-hex width, the
/// policy revision at its full 64-hex width, both 64-bit fields at `u64::MAX`,
/// and an expanded negative year with nine fractional digits — the leased
/// form, which carries a lease id and fencing token the overage form does not.
///
/// ```text
/// {"request_id":"ff…ff","account_id":"ff…ff","source":{"Leased":
///  {"lease_id":"ff…ff","fencing_token":18446744073709551615}},
///  "units":18446744073709551615,"occurred_at":"-009999-01-02T01:59:59.999999999Z",
///  "policy_revision":"ff…ff","key_id":"ff…ff"}
/// ```
///
/// Pinned by `the_widest_usage_event_still_fits_its_declared_size`, so a field
/// added to `UsageEvent` cannot silently push a legitimate batch past the body
/// limit derived from this number.
///
/// #94 raised it from 268: the policy revision is fixed-width, so it costs the
/// same 85 bytes on every event whether stated or unstated. That is the price
/// of carrying it on the wire in its canonical spelling, and it is recorded
/// here rather than discovered when a maximal batch starts being refused.
/// #105 adds the optional key ID and includes expanded negative years and
/// nine fractional digits in the fixture: measured maximum 410 bytes.
pub const MAX_USAGE_EVENT_BYTES: usize = 410;

/// The body limit `/v1/usage/ingest` is served with, in bytes.
///
/// Derived from the two constants above rather than chosen: a full batch of
/// the widest events is `MAX_INGEST_BATCH * (MAX_USAGE_EVENT_BYTES + 1)` — the
/// `+ 1` being each event's separating comma — plus `{"events":[]}`. That is
/// about 1.61 MiB, and 2 MiB still leaves room to
/// spare, so a legitimate maximal batch is never refused for want of a byte.
/// The headroom is checked by
/// `a_full_batch_of_the_widest_events_fits_the_declared_body_limit`, not
/// assumed: the next field to land here may be the one that exhausts it, and
/// this limit must then rise with it.
///
/// Declared rather than inherited. Without it the endpoint ran on axum's
/// implicit 2 MiB default, which no document stated and which the server
/// reported as malformed JSON when it bit (#61).
pub const MAX_INGEST_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Four maximal u64 counters plus field names and framing. Includes a
/// present attribution count; legacy missing/null values are shorter.
/// Pinned by the maximal acknowledgement serialization witness.
pub const MAX_INGEST_REPORT_BYTES: usize = 134;

/// The body limit `PUT /v1/admin/snapshots/{principal}` is served with, in
/// bytes.
///
/// A published snapshot carries its whole cost table, so its worst case grows
/// with the number of priced classes rather than with any batch size. Measured
/// against that: full-width weights and per-operation permissions serialize
/// as parallel arrays at about 32 bytes per class. A hundred-thousand-class
/// catalogue is about 3.1 MiB and fits with room for the snapshot envelope. Pinned
/// by `the_snapshot_limit_admits_a_hundred_thousand_class_catalogue`. It is stated for the same
/// reason the ingest limit is — an operator publishing a large catalogue
/// should be refused by a documented number or not at all, never by an
/// undocumented default reported as bad JSON.
pub const MAX_SNAPSHOT_BODY_BYTES: usize = 4 * 1024 * 1024;

/// One complete active credential projection. Digests are fixed 32-byte arrays;
/// HMAC secrets and raw credentials are never part of this response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysResponse {
    pub revision: u64,
    pub as_of: jiff::Timestamp,
    pub keys: Vec<crate::CredentialRecord>,
    #[serde(deserialize_with = "crate::credentials::required_option")]
    pub next_after: Option<tollgate_core::KeyId>,
}

/// Fixed-width identifiers/digest plus the widest timestamp and JSON framing.
/// `wire_limits` measures a maximal record and page against these bounds.
pub const MAX_KEY_RECORD_BYTES: usize = 216;
/// The maximal envelope is 134 bytes; the first record needs no comma.
pub const MAX_KEYS_BODY_BYTES: usize = crate::MAX_KEY_PAGE_LIMIT * (MAX_KEY_RECORD_BYTES + 1) + 133;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AcquireRequest {
    pub account_id: AccountId,
    pub requested: CostUnits,
    pub ttl_seconds: u32,
}

pub type AcquireResponse = LeaseGrant;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReleaseRequest {
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub unspent: CostUnits,
}

/// A lease returned and re-granted in one server-side transaction.
///
/// Deliberately not an `AcquireRequest` plus a `ReleaseRequest`: the account
/// is the one the released lease names, so there is no field for a client to
/// disagree with the ledger about (see `LeaseAllocator::consolidate`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConsolidateRequest {
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub unspent: CostUnits,
    pub requested: CostUnits,
    pub ttl_seconds: u32,
}

pub type ConsolidateResponse = LeaseGrant;

/// The owned form, which the server needs: axum's `Json<T>` extractor requires
/// `DeserializeOwned`, so the receiving side cannot borrow from the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub events: Vec<UsageEvent>,
}

/// The sending form. `UsageSink::ingest` hands the transport a `&[UsageEvent]`
/// it does not own, and copying that batch into a `Vec` just to reach serde
/// bought nothing — least of all on the one path where it happened repeatedly.
/// A failing sink is retried indefinitely with backoff by design (an outage is
/// a duration, not an event), and every attempt re-copied the same batch (#20).
///
/// This produces byte-identical JSON to [`IngestRequest`], which is the whole
/// reason it is safe to have two types; `borrowed_and_owned_ingest_requests_serialize_identically`
/// is what checks that rather than trusting it.
#[derive(Debug, Serialize)]
pub struct IngestRequestRef<'a> {
    pub events: &'a [UsageEvent],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub account_id: AccountId,
    pub initial_balance: CostUnits,
    pub status: AccountStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DepositRequest {
    pub units: CostUnits,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
/// The one operator action for an account's administrative status (#51).
///
/// An [`AccountStatus`] rather than the `active` bool this carried before:
/// the bool named only the ledger flag, while the request now also republishes
/// every live snapshot of the account. Changing what an existing field means
/// would have been the silent-semantics change the repository guidelines
/// forbid, so the field is renamed and an old body fails loudly.
pub struct SetStatusRequest {
    pub status: AccountStatus,
}

/// The one operator action for an account's execution-capacity class (#99).
///
/// Its own request type rather than an optional field on
/// [`SetStatusRequest`]: the two are different operator decisions about
/// different axes, and a combined body would make "change the status" and
/// "change the class" indistinguishable from "change the status and leave the
/// class alone" without a nested `Option` nobody would enjoy reading.
///
/// The response is [`SetStatusResponse`], reused deliberately: both actions
/// answer the same question — how many live snapshots did this move, and how
/// many rows could not be pushed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SetCapacityClassRequest {
    pub capacity_class: CapacityClass,
}

/// What a status change did, so an operator learns its blast radius at the
/// moment of the call rather than from a later denial (#51).
///
/// `republished: 0` means the account had no live snapshot to change — no
/// credentials, all of them revoked, or the change was a repeat. All three are
/// worth seeing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SetStatusResponse {
    pub republished: usize,
    /// Rows that changed durably but could not be decoded well enough to push,
    /// so those principals converge only at their next refresh. Always zero on
    /// an in-memory backend.
    pub unreadable: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishSnapshotRequest {
    pub snapshot: Arc<AccountSnapshot>,
}

/// The catalogue of principals a control plane knows, revoked ones included
/// (#48). An instance serving any customer needs this to learn the set that
/// already exists; pushes only carry what changes after it subscribes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalsResponse {
    pub principals: Vec<tollgate_core::Principal>,
}

/// RFC-7807-shaped error body with a stable machine `code`, mirrored back
/// into domain errors by the HTTP transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Problem {
    pub status: u16,
    pub code: String,
    pub title: String,
    /// Optional extension used by versioned tombstone responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<Generation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;
    use tollgate_core::PolicyRevision;
    use tollgate_core::RequestId;
    use tollgate_core::UsageSource;

    fn event(seq: u128) -> UsageEvent {
        UsageEvent::new(
            RequestId(seq),
            AccountId(7),
            UsageSource::Leased {
                lease_id: LeaseId(11),
                fencing_token: FencingToken(3),
            },
            CostUnits(64),
            Timestamp::from_second(1_755_600_000).unwrap(),
            PolicyRevision::UNSTATED,
            None,
        )
    }

    /// #20 added a second ingest type so the transport could stop copying the
    /// batch, and the entire argument for that being safe is that the two
    /// produce the same bytes. Checked here rather than trusted, because it is
    /// a claim about what leaves the process.
    #[test]
    fn borrowed_and_owned_ingest_requests_serialize_identically() {
        for count in [0, 1, 256] {
            let events: Vec<UsageEvent> = (0..count).map(event).collect();
            let borrowed = serde_json::to_string(&IngestRequestRef { events: &events }).unwrap();
            let owned = serde_json::to_string(&IngestRequest {
                events: events.clone(),
            })
            .unwrap();
            assert_eq!(
                borrowed, owned,
                "the two ingest forms disagree at {count} events, so the wire \
                 format depends on which one the caller happened to use"
            );
        }
    }

    /// The client→server path in one fast test: the transport serializes the
    /// borrowed form and the server deserializes the owned one. A field added
    /// to one type and not the other fails here in milliseconds instead of
    /// only in the loopback suite.
    #[test]
    fn a_borrowed_request_deserializes_as_the_owned_one() {
        let events: Vec<UsageEvent> = (0..3).map(event).collect();
        let body = serde_json::to_string(&IngestRequestRef { events: &events }).unwrap();
        let received: IngestRequest = serde_json::from_str(&body).unwrap();
        assert_eq!(received.events, events);
    }

    /// An empty flush is legitimate — the writer can wake with nothing queued
    /// — and must not become a malformed body or a missing field.
    #[test]
    fn an_empty_batch_stays_an_empty_list() {
        let body = serde_json::to_string(&IngestRequestRef { events: &[] }).unwrap();
        assert_eq!(body, r#"{"events":[]}"#);
        let received: IngestRequest = serde_json::from_str(&body).unwrap();
        assert!(received.events.is_empty());
    }

    #[test]
    fn high_bit_ids_are_portable_text_in_an_untyped_json_consumer() {
        let high = (1u128 << 127) | 0x2a;
        let mut event = event(high);
        event.account_id = AccountId(high + 1);
        event.source = UsageSource::Leased {
            lease_id: LeaseId(high + 2),
            fencing_token: FencingToken(3),
        };
        let events = [event];
        let value = serde_json::to_value(IngestRequestRef { events: &events }).unwrap();
        let event = &value["events"][0];
        let leased = &event["source"]["Leased"];

        for (field, node, expected) in [
            ("request_id", event, high),
            ("account_id", event, high + 1),
            ("lease_id", leased, high + 2),
        ] {
            let text = node[field]
                .as_str()
                .unwrap_or_else(|| panic!("{field} must be JSON text, got {}", node[field]));
            assert_eq!(text.len(), 32);
            assert_eq!(u128::from_str_radix(text, 16).unwrap(), expected);
        }

        let received: IngestRequest = serde_json::from_value(value).unwrap();
        assert_eq!(received.events, events);
    }
}
