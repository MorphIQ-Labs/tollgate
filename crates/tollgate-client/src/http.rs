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
    AccountId, AccountSnapshot, CostUnits, FencingToken, Generation, LeaseId, Principal,
    PublishableSnapshot, UsageEvent,
};
use tollgate_store::wire::{
    API_PREFIX, AcquireRequest, ConsolidateRequest, IngestRequestRef, LeaseTtl, PrincipalsResponse,
    Problem, ReleaseRequest,
};
use tollgate_store::{
    AllocateError, Allocation, IngestError, IngestReport, LeaseAllocator, ReclaimBatch,
    SnapshotPush, SnapshotResolution, SnapshotSource, StoreError, UsageSink,
};

/// A `tollgate-server` client implementing [`LeaseAllocator`],
/// [`SnapshotSource`], [`UsageSink`] and `KeySource` over HTTP(S).
///
/// Every call carries the configured bearer credential, if any, and runs
/// under one request deadline that also covers obtaining that credential.
/// Server problem codes map back to the domain errors a direct backend
/// returns. Snapshot pushes are not supported, so the snapshot manager relies
/// on periodic refresh. Shared as an `Arc` by every background component;
/// [`reconfigure`](Self::reconfigure) rotates credentials and TLS material in
/// place.
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

    /// Connect with the given deadlines and otherwise default settings, so
    /// no credentials: tollgate-server refuses unauthenticated calls. See
    /// [`HttpStoreConfig`](crate::HttpStoreConfig) for what each deadline bounds.
    ///
    /// # Errors
    ///
    /// [`StoreError`] for an unusable URL or deadline, as
    /// [`with_config`](Self::with_config).
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

    /// Connect to `base_url` with `config`'s deadlines, TLS material and
    /// credential provider. No request is made here.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the URL is not HTTP(S), carries userinfo, a query
    /// or a fragment, or uses plaintext `http` to anything but a loopback
    /// address or with TLS material configured; when a deadline is zero or
    /// unrepresentable; or when the CA or identity PEM is invalid.
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
        // The code alone is a complete, unattested refusal. Evidence rides
        // beside it and is discarded, not escalated, when it cannot be valid:
        // zero remaining must arrive as `balance-exhausted`.
        "insufficient-balance" => match problem.balance_shortfall {
            Some(evidence) if problem.status == 409 && !evidence.remaining.is_zero() => {
                AllocateError::BalanceInsufficient(evidence)
            }
            _ => AllocateError::InsufficientBalance,
        },
        "balance-exhausted" => match problem.balance_exhaustion {
            Some(evidence) if problem.status == 409 => AllocateError::BalanceExhausted(evidence),
            _ => AllocateError::Storage(StoreError(
                "exhaustion response lacks valid evidence".into(),
            )),
        },
        "balance-overflow" => AllocateError::BalanceOverflow,
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

/// A grant's evidence counts the grant's own units, so a smaller remaining
/// cannot describe the ledger that granted it. Discard such evidence rather
/// than let it refuse quotes the account can fund; the grant itself stands.
async fn read_allocation(response: reqwest::Response) -> Result<Allocation, AllocateError> {
    require_complete(response)?
        .json()
        .await
        .map(credible_evidence)
        .map_err(transport_error)
}

fn credible_evidence(mut allocation: Allocation) -> Allocation {
    if allocation
        .funding
        .is_some_and(|funding| funding.remaining < allocation.grant.units)
    {
        allocation.funding = None;
    }
    allocation
}

#[async_trait]
impl LeaseAllocator for HttpStore {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        // The server stamps its own clock; `now` stays local-only.
        let ttl = LeaseTtl::try_from(ttl)?;
        let response = self
            .request(reqwest::Method::POST, "/leases/acquire")
            .json(&AcquireRequest {
                account_id: account,
                requested,
                ttl,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        read_allocation(response).await
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
        needed: CostUnits,
        ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        // One request, because the guarantee is transactional: two calls over
        // this transport would reintroduce exactly the gap the operation
        // exists to close. The server stamps its own clock, as it does for
        // acquire and release.
        let ttl = LeaseTtl::try_from(ttl)?;
        let response = self
            .request(reqwest::Method::POST, "/leases/consolidate")
            .json(&ConsolidateRequest {
                lease_id,
                fencing_token,
                unspent,
                requested,
                needed,
                ttl,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(read_problem(response).await);
        }
        read_allocation(response).await
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
                // A generation says the row is there and cannot be decoded,
                // which orders it against what this instance holds.
                return Ok(match problem.generation {
                    Some(generation) => SnapshotResolution::Unreadable { generation },
                    None => SnapshotResolution::Unknown,
                });
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
        let body = require_complete(response)?
            .bytes()
            .await
            .map_err(|e| StoreError(format!("http: {}", e.without_url())))?;
        decode_snapshot(principal, &body)
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = broadcast::channel(1);
        drop(sender);
        receiver
    }

    /// Over HTTP this is the *only* way an instance learns the principal set:
    /// `subscribe` above is a closed channel, so there are no deltas to
    /// accumulate and the periodic refresh carries everything (GL-48).
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
        let body: PrincipalsResponse = require_complete(response)?
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
            // than retried — the wedge GL-61 describes is a writer looping on
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
        let bytes = bounded_body(
            require_complete(response)?,
            tollgate_store::wire::MAX_INGEST_REPORT_BYTES,
        )
        .await?;
        let report: IngestReport = serde_json::from_slice(&bytes)
            .map_err(|_| StoreError("invalid usage acknowledgement JSON".into()))?;
        report.validate(events.len())?;
        Ok(report)
    }
}

/// Resolve a complete, successful snapshot body.
///
/// A body that states a generation and yet is no snapshot this instance can
/// enforce is the control plane's answer, not a failure to get one: typically a
/// newer server publishing a value this build does not know. Reporting it as an
/// error would keep the older snapshot serving, whatever the newer one says, so
/// it resolves as unreadable (INVARIANTS.md GL-22). A body without a readable
/// generation answers nothing and is an error.
fn decode_snapshot(principal: Principal, body: &[u8]) -> Result<SnapshotResolution, StoreError> {
    #[derive(serde::Deserialize)]
    struct Stamped {
        generation: Generation,
    }

    let failure = match serde_json::from_slice::<AccountSnapshot>(body) {
        Ok(snapshot) => match PublishableSnapshot::try_new(Arc::new(snapshot)) {
            Ok(snapshot) => return Ok(SnapshotResolution::Present(snapshot)),
            Err(error) => format!("invalid snapshot from server: {error}"),
        },
        Err(error) => format!("snapshot decode: {error}"),
    };
    let Ok(Stamped { generation }) = serde_json::from_slice(body) else {
        return Err(StoreError(failure));
    };
    tracing::error!(
        %principal,
        generation = generation.0,
        error = %failure,
        "served snapshot does not decode; the principal is withdrawn"
    );
    Ok(SnapshotResolution::Unreadable { generation })
}

// Successful partial responses must never become a whole snapshot or catalogue.
fn require_complete(response: reqwest::Response) -> Result<reqwest::Response, StoreError> {
    if response.status() != reqwest::StatusCode::OK
        || response
            .headers()
            .contains_key(reqwest::header::CONTENT_RANGE)
    {
        return Err(StoreError(
            "control-plane response requires a complete HTTP 200 response".into(),
        ));
    }
    Ok(response)
}

async fn bounded_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, StoreError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(StoreError(
            "control-plane body exceeds its wire limit".into(),
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| StoreError(format!("control-plane transport: {}", e.without_url())))?
    {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(StoreError(
                "control-plane body exceeds its wire limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[async_trait]
impl tollgate_store::KeySource for HttpStore {
    async fn active_keys_page(
        &self,
        _now: Timestamp,
        after: Option<tollgate_core::KeyId>,
        limit: NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        tollgate_store::validate_key_page_limit(limit)?;
        let mut request = self.request(reqwest::Method::GET, "/keys");
        request.request = request.request.query(&[("limit", limit.get().to_string())]);
        if let Some(after) = after {
            request.request = request.request.query(&[("after", after.to_string())]);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let bytes = bounded_body(response, tollgate_store::wire::MAX_KEYS_BODY_BYTES).await?;
            let code = serde_json::from_slice::<Problem>(&bytes)
                .ok()
                .map(|p| p.code);
            let safe_code = match code.as_deref() {
                Some("authentication-required") => "authentication-required",
                Some("scope-forbidden") => "scope-forbidden",
                Some("invalid-query") => "invalid-query",
                Some("invalid-limit") => "invalid-limit",
                _ => "credential-source-unavailable",
            };
            return Err(StoreError(format!(
                "credential read refused: {status} {safe_code}"
            )));
        }
        let bytes = bounded_body(
            require_complete(response)?,
            tollgate_store::wire::MAX_KEYS_BODY_BYTES,
        )
        .await?;
        credential_page(&bytes, after, limit)
    }
}

/// [`KeysResponse`](tollgate_store::wire::KeysResponse), with each record kept
/// apart so that one this build cannot decode does not lose the others.
#[derive(serde::Deserialize)]
struct KeysBody<'a> {
    revision: u64,
    as_of: Timestamp,
    #[serde(borrow)]
    keys: Vec<&'a serde_json::value::RawValue>,
    #[serde(deserialize_with = "Option::deserialize")]
    next_after: Option<tollgate_core::KeyId>,
    #[serde(default)]
    unreadable: Vec<tollgate_core::KeyId>,
    #[serde(default)]
    unnamed_unreadable: u64,
}

/// The identity of a record that does not otherwise decode.
#[derive(serde::Deserialize)]
struct KeyIdentity {
    key_id: tollgate_core::KeyId,
}

/// Decode one complete credential page. A record that states a key id and
/// either does not decode or does not identify its principal is withheld, as a
/// direct store withholds such a row. A body or a record without that much
/// remains a failed read.
fn credential_page(
    bytes: &[u8],
    after: Option<tollgate_core::KeyId>,
    limit: NonZeroUsize,
) -> Result<tollgate_store::KeyPage, StoreError> {
    let invalid = |_| StoreError("invalid credential page body".into());
    let body: KeysBody<'_> = serde_json::from_slice(bytes).map_err(invalid)?;
    let mut keys = Vec::with_capacity(body.keys.len());
    let mut unreadable = body.unreadable;
    // The order of what arrived is checked before anything is set aside, so
    // withholding cannot repair a malformed page.
    let mut previous = after;
    for raw in body.keys {
        let decoded = serde_json::from_str::<tollgate_store::CredentialRecord>(raw.get())
            .ok()
            .filter(|record| record.validate().is_ok());
        let key_id = match &decoded {
            Some(record) => record.key_id,
            None => {
                serde_json::from_str::<KeyIdentity>(raw.get())
                    .map_err(invalid)?
                    .key_id
            }
        };
        if previous.is_some_and(|previous| key_id <= previous) {
            return Err(StoreError(
                "credential page is unordered or repeated".into(),
            ));
        }
        previous = Some(key_id);
        match decoded {
            Some(record) => keys.push(record),
            None => {
                // The decode error is not logged: it can quote the record.
                tracing::error!(
                    %key_id,
                    "served credential does not decode; it is withheld from the projection"
                );
                unreadable.push(key_id);
            }
        }
    }
    unreadable.sort_unstable();
    tollgate_store::KeyPage::try_with_unreadable(
        body.revision,
        body.as_of,
        after,
        limit,
        keys,
        body.next_after,
        tollgate_store::UnreadableCredentials {
            key_ids: unreadable,
            unnamed: body.unnamed_unreadable,
        },
    )
}

#[cfg(test)]
mod credential_page_tests {
    use super::*;
    use serde_json::{Value, json};
    use tollgate_core::KeyId;

    fn key_id(id: u128) -> Value {
        json!(KeyId(id))
    }

    fn record(id: u128) -> Value {
        let mut digest = [0; 32];
        digest[..16].copy_from_slice(&id.to_be_bytes());
        serde_json::to_value(tollgate_store::CredentialRecord {
            key_id: KeyId(id),
            principal: tollgate_core::Principal(id),
            digest,
            not_after: None,
        })
        .unwrap()
    }

    fn with(mut record: Value, field: &str, value: Value) -> Value {
        record[field] = value;
        record
    }

    fn page(body: Value) -> Result<tollgate_store::KeyPage, StoreError> {
        credential_page(
            &serde_json::to_vec(&body).unwrap(),
            None,
            NonZeroUsize::new(2).unwrap(),
        )
    }

    fn body(keys: Vec<Value>, next_after: Value) -> Value {
        json!({"revision": 1, "as_of": "2026-01-01T00:00:00Z", "keys": keys, "next_after": next_after})
    }

    fn ids(page: &tollgate_store::KeyPage) -> (Vec<u128>, Vec<u128>) {
        (
            page.records().iter().map(|key| key.key_id.0).collect(),
            page.unreadable().key_ids.iter().map(|id| id.0).collect(),
        )
    }

    /// A server that predates withheld ids sends neither field.
    #[test]
    fn a_body_without_the_withheld_fields_reads() {
        let read = page(body(vec![record(1), record(2)], key_id(2))).unwrap();
        assert_eq!(ids(&read), (vec![1, 2], vec![]));
        assert_eq!(read.unreadable().unnamed, 0);
        assert_eq!(read.next_after(), Some(KeyId(2)));
    }

    #[test]
    fn withheld_ids_and_counts_cross_the_wire() {
        let mut served = body(vec![record(2)], Value::Null);
        served["unreadable"] = json!([key_id(1)]);
        served["unnamed_unreadable"] = json!(3);
        let read = page(served).unwrap();
        assert_eq!(ids(&read), (vec![2], vec![1]));
        assert_eq!(read.unreadable().unnamed, 3);
    }

    /// A record this build cannot decode, or whose identity does not hold, is
    /// withheld as a direct store withholds the row, wherever it falls in a
    /// full page, and merged in order with the ids the server withheld.
    #[test]
    fn a_served_record_that_does_not_decode_is_withheld() {
        for damaged in [
            with(record(2), "digest", json!("from-a-newer-scheme")),
            with(record(2), "principal", json!(key_id(9))),
            with(record(2), "not_after", json!(true)),
            json!({"key_id": key_id(2)}),
        ] {
            let mut served = body(vec![record(1), damaged.clone()], key_id(2));
            served["unreadable"] = json!([key_id(0)]);
            let read = credential_page(
                &serde_json::to_vec(&served).unwrap(),
                None,
                NonZeroUsize::new(2).unwrap(),
            )
            .unwrap();
            assert_eq!(ids(&read), (vec![1], vec![0, 2]), "{damaged}");
            assert_eq!(read.next_after(), Some(KeyId(2)));
        }
        let first = with(record(1), "digest", json!(""));
        let read = page(body(vec![first, record(2)], key_id(2))).unwrap();
        assert_eq!(ids(&read), (vec![2], vec![1]));
    }

    /// What cannot be named cannot be withheld, and withholding never repairs
    /// a page that was malformed as served.
    #[test]
    fn an_unusable_credential_body_stays_a_failed_read() {
        let undecodable = |id| with(record(id), "digest", json!("x"));
        for served in [
            body(vec![record(1), json!({"digest": "x"})], Value::Null),
            body(vec![record(1), json!({"key_id": 7})], Value::Null),
            body(vec![record(2), undecodable(1)], Value::Null),
            body(vec![record(1), undecodable(1)], Value::Null),
            body(vec![record(1), record(2), undecodable(3)], key_id(2)),
            json!({"revision": 1, "as_of": "2026-01-01T00:00:00Z", "keys": []}),
            json!({"revision": 1, "keys": [], "next_after": null}),
        ] {
            assert!(page(served.clone()).is_err(), "{served}");
        }
        assert!(
            credential_page(
                &serde_json::to_vec(&body(vec![undecodable(1)], Value::Null)).unwrap(),
                Some(KeyId(1)),
                NonZeroUsize::new(2).unwrap(),
            )
            .is_err(),
            "a withheld record must still advance the cursor"
        );
    }
}

#[cfg(test)]
mod exhaustion_tests {
    use super::*;
    use tollgate_core::LeaseGrant;

    #[test]
    fn incomplete_exhaustion_responses_never_become_authoritative() {
        for body in [
            r#"{"status":409,"code":"balance-exhausted","title":"empty"}"#,
            r#"{"status":409,"code":"balance-exhausted","title":"empty","balance_exhaustion":{}}"#,
            r#"{"status":503,"code":"balance-exhausted","title":"empty","balance_exhaustion":{"period_end":null}}"#,
        ] {
            if let Ok(problem) = serde_json::from_str::<Problem>(body) {
                assert!(matches!(
                    problem_to_allocate(problem),
                    AllocateError::Storage(_)
                ));
            }
        }
        let old = serde_json::from_str::<Problem>(
            r#"{"status":409,"code":"insufficient-balance","title":"empty"}"#,
        )
        .unwrap();
        assert_eq!(problem_to_allocate(old), AllocateError::InsufficientBalance);
    }

    /// Every plain domain code maps back to its refusal. A code that fell
    /// through to `Storage` would turn a definite answer into an ambiguous one.
    #[test]
    fn plain_refusal_codes_round_trip() {
        for (code, expected) in [
            ("unknown-account", AllocateError::UnknownAccount),
            ("account-inactive", AllocateError::AccountInactive),
            ("insufficient-balance", AllocateError::InsufficientBalance),
            ("balance-overflow", AllocateError::BalanceOverflow),
            ("invalid-ttl", AllocateError::InvalidTtl),
            ("unknown-lease", AllocateError::UnknownLease),
            ("fenced", AllocateError::Fenced),
            ("lease-not-active", AllocateError::LeaseNotActive),
            ("invalid-release", AllocateError::InvalidRelease),
        ] {
            let problem = Problem {
                status: 409,
                code: code.into(),
                title: code.into(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            };
            assert_eq!(problem_to_allocate(problem), expected, "{code}");
        }
    }

    #[test]
    fn incomplete_shortfall_responses_never_become_authoritative() {
        for body in [
            // Zero remaining is exhaustion and must arrive under its own code.
            r#"{"status":409,"code":"insufficient-balance","title":"short","balance_shortfall":{"remaining":0,"period_end":null}}"#,
            r#"{"status":503,"code":"insufficient-balance","title":"short","balance_shortfall":{"remaining":5,"period_end":null}}"#,
        ] {
            let problem = serde_json::from_str::<Problem>(body).unwrap();
            assert_eq!(
                problem_to_allocate(problem),
                AllocateError::InsufficientBalance,
                "{body}"
            );
        }
        // A period end is required, never defaulted to "unscheduled".
        assert!(
            serde_json::from_str::<Problem>(
                r#"{"status":409,"code":"insufficient-balance","title":"short","balance_shortfall":{"remaining":5}}"#
            )
            .is_err()
        );
        let attested = serde_json::from_str::<Problem>(
            r#"{"status":409,"code":"insufficient-balance","title":"short","balance_shortfall":{"remaining":5,"period_end":null}}"#,
        )
        .unwrap();
        assert_eq!(
            problem_to_allocate(attested),
            AllocateError::BalanceInsufficient(tollgate_core::BalanceShortfall {
                remaining: CostUnits(5),
                period_end: None,
            })
        );
    }

    /// A server that predates evidence sends a bare grant, which reads as a
    /// grant with nothing attested.
    #[test]
    fn a_grant_without_evidence_parses_as_unattested() {
        let grant = LeaseGrant {
            lease_id: LeaseId(1),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(10),
            expires_at: jiff::Timestamp::UNIX_EPOCH,
        };
        let old = serde_json::to_string(&grant).unwrap();
        let parsed: Allocation = serde_json::from_str(&old).unwrap();
        assert_eq!(
            parsed,
            Allocation {
                grant,
                funding: None
            }
        );
    }

    #[test]
    fn grant_evidence_below_the_grant_itself_is_discarded() {
        let grant = LeaseGrant {
            lease_id: LeaseId(1),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(10),
            expires_at: jiff::Timestamp::UNIX_EPOCH,
        };
        let with = |remaining| Allocation {
            grant,
            funding: Some(tollgate_core::BalanceShortfall {
                remaining: CostUnits(remaining),
                period_end: None,
            }),
        };
        assert_eq!(credible_evidence(with(9)).funding, None);
        assert_eq!(credible_evidence(with(10)), with(10));
    }
}
