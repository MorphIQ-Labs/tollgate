//! The HTTP wire contract between `tollgate-server` and `tollgate-client`'s HTTP
//! transport. One place, versioned by path prefix (`/v1/...`) on the server.
//!
//! Ids serialize as JSON numbers (`serde_json` handles u128 natively);
//! snapshots travel whole, cost table included — the receiving instance
//! installs them as-is, resolving nothing.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, Generation, LeaseGrant, LeaseId,
    UsageEvent,
};

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
    pub active: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DepositRequest {
    pub units: CostUnits,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SetStatusRequest {
    pub active: bool,
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
    use tollgate_core::RequestId;

    fn event(seq: u128) -> UsageEvent {
        UsageEvent {
            request_id: RequestId(seq),
            account_id: AccountId(7),
            lease_id: LeaseId(11),
            fencing_token: FencingToken(3),
            units: CostUnits(64),
            occurred_at: Timestamp::from_second(1_755_600_000).unwrap(),
        }
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
}
