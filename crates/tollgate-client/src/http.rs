//! HTTP implementations of the store traits against a `tollgate-server`.
//!
//! This is the "via-server" topology: the same `LeaseManager`/`UsageWriter`
//! code runs unchanged over [`HttpStore`] instead of a direct backend — the
//! pluggability claim, made executable. Server `Problem.code` strings map
//! back to [`AllocateError`] variants, so fencing and settlement semantics
//! survive the transport.
//!
//! `SnapshotSource::subscribe` returns a channel that never fires: push
//! distribution over HTTP (SSE/long-poll) is a documented seam in
//! `docs/DESIGN.md`, and the pull path (`snapshot()`) is sufficient for the
//! PoC's cold-fetch flow.

use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, LeaseGrant, LeaseId, Principal, UsageEvent,
};
use tollgate_store::wire::{AcquireRequest, IngestRequest, Problem, ReleaseRequest};
use tollgate_store::{
    AllocateError, IngestReport, LeaseAllocator, ReclaimedLease, SnapshotPush, SnapshotSource,
    StoreError, UsageSink,
};

pub struct HttpStore {
    base: String,
    client: reqwest::Client,
    /// Kept alive so `subscribe` can hand out receivers; nothing sends yet.
    push: broadcast::Sender<SnapshotPush>,
}

impl HttpStore {
    /// Connect with default deadlines (2s connect, 10s per request). A hung
    /// control-plane request must never stall refill, billing, or shutdown
    /// indefinitely — background tasks rely on these bounds.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Arc<Self> {
        Self::with_timeouts(
            base_url,
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(10),
        )
    }

    #[must_use]
    pub fn with_timeouts(
        base_url: impl Into<String>,
        connect_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
    ) -> Arc<Self> {
        let (push, _) = broadcast::channel(16);
        Arc::new(HttpStore {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .connect_timeout(connect_timeout)
                .timeout(request_timeout)
                .build()
                .expect("static client configuration"),
            push,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

fn transport_error(e: reqwest::Error) -> AllocateError {
    AllocateError::Storage(StoreError(format!("http: {e}")))
}

/// Map a server problem back into the domain vocabulary. Unknown codes are
/// storage errors: fail toward "retryable", never toward a fabricated domain
/// refusal.
fn problem_to_allocate(problem: Problem) -> AllocateError {
    match problem.code.as_str() {
        "unknown-account" => AllocateError::UnknownAccount,
        "account-inactive" => AllocateError::AccountInactive,
        "insufficient-balance" => AllocateError::InsufficientBalance,
        "unknown-lease" => AllocateError::UnknownLease,
        "fenced" => AllocateError::Fenced,
        "lease-not-active" => AllocateError::LeaseNotActive,
        "invalid-release" => AllocateError::InvalidRelease,
        _ => AllocateError::Storage(StoreError(format!(
            "server {}: {}",
            problem.status, problem.title
        ))),
    }
}

async fn read_problem(response: reqwest::Response) -> AllocateError {
    let status = response.status().as_u16();
    match response.json::<Problem>().await {
        Ok(problem) => problem_to_allocate(problem),
        Err(_) => AllocateError::Storage(StoreError(format!("server returned {status}"))),
    }
}

#[async_trait]
impl LeaseAllocator for HttpStore {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError> {
        // The server stamps its own clock; `now` stays local-only.
        let ttl_seconds = u32::try_from(ttl.as_secs().max(0)).unwrap_or(u32::MAX);
        let response = self
            .client
            .post(self.url("/v1/leases/acquire"))
            .json(&AcquireRequest {
                account_id: account,
                requested,
                ttl_seconds,
            })
            .send()
            .await
            .map_err(transport_error)?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        response.json().await.map_err(transport_error)
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), AllocateError> {
        let response = self
            .client
            .post(self.url("/v1/leases/release"))
            .json(&ReleaseRequest {
                lease_id,
                fencing_token,
                unspent,
            })
            .send()
            .await
            .map_err(transport_error)?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        Ok(())
    }

    async fn reclaim_expired(&self, _now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        // Reclaim is the server's own maintenance loop; the HTTP transport
        // can trigger it but instances never need to.
        Err(StoreError(
            "reclaim is server-side; not exposed through the instance transport".into(),
        ))
    }
}

#[async_trait]
impl SnapshotSource for HttpStore {
    async fn snapshot(
        &self,
        principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        let response = self
            .client
            .get(self.url(&format!("/v1/snapshots/{}", principal.0)))
            .send()
            .await
            .map_err(|e| StoreError(format!("http: {e}")))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None); // confirmed unknown → negative-cacheable
        }
        if !response.status().is_success() {
            return Err(StoreError(format!("server returned {}", response.status())));
        }
        let snapshot: AccountSnapshot = response
            .json()
            .await
            .map_err(|e| StoreError(format!("http: {e}")))?;
        Ok(Some(Arc::new(snapshot)))
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

#[async_trait]
impl UsageSink for HttpStore {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        let response = self
            .client
            .post(self.url("/v1/usage/ingest"))
            .json(&IngestRequest {
                events: events.to_vec(),
            })
            .send()
            .await
            .map_err(|e| StoreError(format!("http: {e}")))?;
        if !response.status().is_success() {
            return Err(StoreError(format!("server returned {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|e| StoreError(format!("http: {e}")))
    }
}
