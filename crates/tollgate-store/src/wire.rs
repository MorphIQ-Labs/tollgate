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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub events: Vec<UsageEvent>,
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
