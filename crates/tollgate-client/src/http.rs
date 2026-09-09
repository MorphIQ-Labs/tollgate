//! HTTP implementations of the store traits against a `tollgate-server`.
//!
//! This is the "via-server" topology: the same `LeaseManager`/`UsageWriter`
//! code runs unchanged over [`HttpStore`] instead of a direct backend — the
//! pluggability claim, made executable. Server `Problem.code` strings map
//! back to [`AllocateError`] variants, so fencing and settlement semantics
//! survive the transport.
//!
//! `SnapshotSource::subscribe` returns a closed channel: push distribution
//! over HTTP (SSE/long-poll) is a documented seam in `docs/DESIGN.md`.
//! [`SnapshotManager`](crate::SnapshotManager) detects the closure and uses
//! periodic plus negative-TTL pulls for freshness.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, LeaseGrant, LeaseId, Principal,
    PublishableSnapshot, UsageEvent,
};
use tollgate_store::wire::{
    API_PREFIX, AcquireRequest, ConsolidateRequest, IngestRequestRef, PrincipalsResponse, Problem,
    ReleaseRequest,
};
use tollgate_store::{
    AllocateError, IngestError, IngestReport, LeaseAllocator, ReclaimBatch, SnapshotPush,
    SnapshotResolution, SnapshotSource, StoreError, UsageSink,
};

pub struct HttpStore {
    base: String,
    transport: arc_swap::ArcSwap<HttpTransport>,
}

struct HttpTransport {
    client: reqwest::Client,
    bearer: Option<Arc<dyn crate::http_security::BearerProvider>>,
    request_timeout: std::time::Duration,
}

impl HttpStore {
    /// Connect with default deadlines. Unauthenticated calls are refused by
    /// tollgate-server; use `with_config` to supply credentials or mTLS.
    pub fn new(base_url: impl Into<String>) -> Result<Arc<Self>, StoreError> {
        Self::with_config(base_url, crate::http_security::HttpStoreConfig::default())
    }

    pub fn with_timeouts(
        base_url: impl Into<String>,
        connect_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
    ) -> Result<Arc<Self>, StoreError> {
        Self::with_config(
            base_url,
            crate::http_security::HttpStoreConfig {
                connect_timeout,
                request_timeout,
                ..Default::default()
            },
        )
    }

    pub fn with_config(
        base_url: impl Into<String>,
        config: crate::http_security::HttpStoreConfig,
    ) -> Result<Arc<Self>, StoreError> {
        let (base, client) = crate::http_security::client(&base_url.into(), &config)?;
        Ok(Arc::new(Self {
            base,
            transport: arc_swap::ArcSwap::from_pointee(HttpTransport {
                client,
                bearer: config.bearer,
                request_timeout: config.request_timeout,
            }),
        }))
    }

    /// Rotate TLS roots, the client certificate and/or credential provider as
    /// one generation. In-flight calls retain their original generation; future
    /// calls use the new one through the same Arc held by background managers.
    pub fn reconfigure(
        &self,
        config: crate::http_security::HttpStoreConfig,
    ) -> Result<(), StoreError> {
        let (_, client) = crate::http_security::client(&self.base, &config)?;
        self.transport.store(Arc::new(HttpTransport {
            client,
            bearer: config.bearer,
            request_timeout: config.request_timeout,
        }));
        Ok(())
    }

    fn request(&self, method: reqwest::Method, path: &str) -> HttpRequest {
        let transport = self.transport.load_full();
        let request = transport
            .client
            .request(method, format!("{}{API_PREFIX}{path}", self.base));
        HttpRequest { transport, request }
    }
}

// The builder owns the same generation that supplies its credentials and
// deadline. Call sites cannot combine a new bearer with an old mTLS identity.
struct HttpRequest {
    transport: Arc<HttpTransport>,
    request: reqwest::RequestBuilder,
}

impl HttpRequest {
    fn json<T: serde::Serialize + ?Sized>(mut self, body: &T) -> Self {
        self.request = self.request.json(body);
        self
    }

    async fn send(self) -> Result<reqwest::Response, StoreError> {
        let mut request = self.request;
        let deadline = tokio::time::Instant::now() + self.transport.request_timeout;
        if let Some(provider) = &self.transport.bearer {
            let token = tokio::time::timeout_at(deadline, provider.token())
                .await
                .map_err(|_| StoreError("control-plane credential deadline expired".into()))??;
            request = request.header(reqwest::header::AUTHORIZATION, token.header());
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(StoreError("control-plane request deadline expired".into()));
        }
        // Reqwest's deadline includes reading the response body. Reduce it by
        // credential time so a slow identity provider cannot double the budget.
        request
            .timeout(remaining)
            .send()
            .await
            .map_err(|e| StoreError(format!("http: {}", e.without_url())))
    }
}

fn transport_error(e: reqwest::Error) -> AllocateError {
    AllocateError::Storage(StoreError(format!("http: {}", e.without_url())))
}

/// Map a server problem back into the domain vocabulary. Unknown codes are
/// storage errors: fail toward "retryable", never toward a fabricated domain
/// refusal.
fn problem_to_allocate(problem: Problem) -> AllocateError {
    match problem.code.as_str() {
        "unknown-account" => AllocateError::UnknownAccount,
        "account-inactive" => AllocateError::AccountInactive,
        "insufficient-balance" => AllocateError::InsufficientBalance,
        "invalid-ttl" => AllocateError::InvalidTtl,
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

/// The server's own words for a refusal, for an error a human will read.
///
/// Falls back to the status when the body is not a `Problem`: a proxy or a
/// load balancer can refuse before the handler is reached, and "server
/// returned 502" is still more use than an empty string.
async fn problem_detail(response: reqwest::Response) -> String {
    let status = response.status().as_u16();
    match response.json::<Problem>().await {
        Ok(problem) => format!("{status} {}: {}", problem.code, problem.title),
        Err(_) => format!("server returned {status}"),
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
            .request(reqwest::Method::POST, "/leases/acquire")
            .json(&AcquireRequest {
                account_id: account,
                requested,
                ttl_seconds,
            })
            .send()
            .await?;
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
            .request(reqwest::Method::POST, "/leases/release")
            .json(&ReleaseRequest {
                lease_id,
                fencing_token,
                unspent,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        Ok(())
    }

    async fn consolidate(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        requested: CostUnits,
        ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError> {
        // One request, because the guarantee is transactional: two calls over
        // this transport would reintroduce exactly the gap the operation
        // exists to close. The server stamps its own clock, as it does for
        // acquire and release.
        let ttl_seconds = u32::try_from(ttl.as_secs().max(0)).unwrap_or(u32::MAX);
        let response = self
            .request(reqwest::Method::POST, "/leases/consolidate")
            .json(&ConsolidateRequest {
                lease_id,
                fencing_token,
                unspent,
                requested,
                ttl_seconds,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        response.json().await.map_err(transport_error)
    }

    async fn reclaim_expired_batch(
        &self,
        _now: Timestamp,
        _limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        // Reclaim is the server's own maintenance loop; the HTTP transport
        // can trigger it but instances never need to.
        Err(StoreError(
            "reclaim is server-side; not exposed through the instance transport".into(),
        ))
    }
}

#[async_trait]
impl SnapshotSource for HttpStore {
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        let response = self
            .request(reqwest::Method::GET, &format!("/snapshots/{principal}"))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let problem: Problem = response.json().await.map_err(|e| {
                StoreError(format!(
                    "snapshot endpoint returned an unstructured 404, not a confirmed unknown principal: {}", e.without_url()
                ))
            })?;
            if problem.code == "unknown-principal" {
                return Ok(SnapshotResolution::Unknown);
            }
            return Err(StoreError(format!(
                "snapshot endpoint returned 404 with code {} instead of unknown-principal",
                problem.code
            )));
        }
        if response.status() == reqwest::StatusCode::GONE {
            let problem: Problem = response
                .json()
                .await
                .map_err(|e| StoreError(format!("http: {}", e.without_url())))?;
            if problem.code != "revoked-principal" {
                return Err(StoreError(format!(
                    "server {}: {}",
                    problem.status, problem.title
                )));
            }
            let generation = problem.generation.ok_or_else(|| {
                StoreError("revoked-principal response omitted generation".into())
            })?;
            return Ok(SnapshotResolution::Revoked { generation });
        }
        if !response.status().is_success() {
            return Err(StoreError(problem_detail(response).await));
        }
        let snapshot: AccountSnapshot = response
            .json()
            .await
            .map_err(|e| StoreError(format!("http: {}", e.without_url())))?;
        let snapshot = PublishableSnapshot::try_new(Arc::new(snapshot))
            .map_err(|error| StoreError(format!("invalid snapshot from server: {error}")))?;
        Ok(SnapshotResolution::Present(snapshot))
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = broadcast::channel(1);
        drop(sender);
        receiver
    }

    /// Over HTTP this is the *only* way an instance learns the principal set:
    /// `subscribe` above is a closed channel, so there are no deltas to
    /// accumulate and the periodic refresh carries everything (#48).
    ///
    /// A server whose backend cannot enumerate answers 501, which maps back
    /// to `None` — "stay on your configured set" — rather than to an empty
    /// catalogue, which would mean "forget everyone".
    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        let response = self
            .request(reqwest::Method::GET, "/snapshots")
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_IMPLEMENTED {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(StoreError(problem_detail(response).await));
        }
        let body: PrincipalsResponse = response
            .json()
            .await
            .map_err(|e| StoreError(format!("http: {}", e.without_url())))?;
        Ok(Some(body.principals))
    }
}

#[async_trait]
impl UsageSink for HttpStore {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        let response = self
            .request(reqwest::Method::POST, "/usage/ingest")
            .json(&IngestRequestRef { events })
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let detail = problem_detail(response).await;
            // A 4xx is the server's judgement about *this batch*: too large,
            // undecodable, a contract it does not implement. Replaying it
            // unchanged earns the same answer forever, so it is refused rather
            // than retried — the wedge #61 describes is a writer looping on
            // exactly this. The exceptions are the 4xx statuses that
            // describe the moment rather than the payload: 408 and 429 are
            // invitations to try again. 401/403 describe credentials, which
            // can rotate without changing or discarding the batch.
            let terminal = status.is_client_error()
                && status != reqwest::StatusCode::REQUEST_TIMEOUT
                && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                && status != reqwest::StatusCode::UNAUTHORIZED
                && status != reqwest::StatusCode::FORBIDDEN;
            let error = StoreError(detail);
            return Err(if terminal {
                IngestError::Refused(error)
            } else {
                IngestError::Unavailable(error)
            });
        }
        response
            .json()
            .await
            .map_err(|e| StoreError(format!("http: {}", e.without_url())).into())
    }
}
