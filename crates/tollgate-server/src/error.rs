//! RFC-7807 `application/problem+json` errors with stable machine codes.
//!
//! The `code` strings are wire contract: `tollgate-client`'s HTTP transport maps
//! them back to `AllocateError` variants. Change one and the loopback
//! correctness suite fails.

use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{FromRequest, FromRequestParts, Json, MatchedPath, Path, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;

use tollgate_core::{Generation, SnapshotValidationError};
use tollgate_store::wire::Problem;
use tollgate_store::{
    AllocateError, CreateAccountError, PublishSnapshotError, SetStatusError, StoreError,
};
use tollgate_store::{IngestError, MAX_INGEST_BATCH};

/// A refused control-plane request, rendered as an RFC-7807
/// `application/problem+json` body in the [`Problem`] shape.
///
/// A `401` also carries a `Bearer` challenge. A 5xx or `usage-refused`
/// response adds an optional `error_id` that matches a warning on the
/// `tollgate::diagnostics` target. Converting a [`StoreError`] never exposes
/// its text, in the body or in `Debug` output (INVARIANTS.md 37).
#[derive(Debug)]
pub struct ApiError {
    /// The HTTP status, also sent as the body's `status`.
    pub status: StatusCode,
    /// The stable machine code clients classify the refusal by. Part of the
    /// wire contract.
    pub code: &'static str,
    /// A short public description. Not a contract; classify by `status` and
    /// `code`.
    pub title: String,
    /// The tombstone's generation on a `revoked-principal` refusal.
    pub generation: Option<Generation>,
    /// The allocator's evidence on a `balance-exhausted` refusal.
    pub balance_exhaustion: Option<tollgate_core::BalanceExhaustion>,
    /// The remaining funding on an `insufficient-balance` refusal, when the
    /// allocator attested it.
    pub balance_shortfall: Option<tollgate_core::BalanceShortfall>,
}

/// JSON input whose extractor failures stay inside the RFC-7807 contract.
///
/// Axum's default rejection is plain text. Keeping the wrapper at the service
/// boundary makes malformed identifiers and every same-pattern body failure
/// structured without relying on each handler to remember an error mapping.
pub(crate) struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(ApiError::from)
    }
}

/// Path input whose parse failures stay distinct from a legitimate 404.
pub(crate) struct ApiPath<T>(pub T);

/// Query decoding is subject to the same structured external-input contract.
pub(crate) struct ApiQuery<T>(pub T);
impl<T, S> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(value)| Self(value))
            .map_err(|_| {
                ApiError::bad_request(
                    "invalid-query",
                    "query parameters are malformed or unsupported",
                )
            })
    }
}

impl<T, S> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(ApiError::from)
    }
}

impl ApiError {
    /// `401 authentication-required`: credentials are missing, invalid,
    /// expired or conflicting. The response carries a `Bearer` challenge.
    pub fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "authentication-required",
            title: "valid control-plane credentials required".into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }

    /// `403 scope-forbidden`: the credential is valid, but its identity lacks
    /// the role this route requires.
    pub fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "scope-forbidden",
            title: "credential does not authorize this control-plane operation".into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
    /// `404` with the given machine code and title.
    pub fn not_found(code: &'static str, title: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::NOT_FOUND,
            code,
            title: title.into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }

    /// `410 revoked-principal`: the principal's snapshot is a tombstone.
    /// Carries the tombstone's `generation` so a client can order it against
    /// the positive snapshots it holds (INVARIANTS.md 15).
    pub fn revoked(generation: Generation) -> Self {
        ApiError {
            status: StatusCode::GONE,
            code: "revoked-principal",
            title: "snapshot revoked".to_string(),
            generation: Some(generation),
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }

    /// `400` with the given machine code and title.
    pub fn bad_request(code: &'static str, title: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            code,
            title: title.into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }

    /// The backend cannot answer this at all, as opposed to answering
    /// "nothing" — a distinction a caller must be able to act on differently
    /// (GL-48).
    pub fn not_implemented(code: &'static str, title: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::NOT_IMPLEMENTED,
            code,
            title: title.into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(error: JsonRejection) -> Self {
        let status = error.into_response().status();
        // A body over the endpoint's limit is not malformed JSON, and saying
        // so sent a client looking for a syntax error in a payload it had
        // serialised correctly. The two are told apart by status rather than
        // by matching axum's rejection variants, because the nesting that
        // produces a 413 is an internal detail of the extractor and the status
        // is the part of that behaviour axum documents (GL-61).
        //
        // Distinct codes matter beyond the message: a client can retry a
        // transient failure, and must never retry this one unchanged — an
        // oversized batch is refused identically forever.
        if status == StatusCode::PAYLOAD_TOO_LARGE {
            return ApiError {
                status,
                code: "batch-too-large",
                title: format!(
                    "request body exceeds this endpoint's limit; \
                     usage batches are capped at {MAX_INGEST_BATCH} events"
                ),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            };
        }
        ApiError {
            status,
            code: "invalid-json",
            title: "request body is not valid JSON for this endpoint".to_string(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<PathRejection> for ApiError {
    fn from(error: PathRejection) -> Self {
        let status = error.into_response().status();
        ApiError {
            status,
            code: "invalid-id",
            title: "path identifier must be exactly 32 lowercase hexadecimal digits".to_string(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<AllocateError> for ApiError {
    fn from(e: AllocateError) -> Self {
        let balance_exhaustion = match &e {
            AllocateError::BalanceExhausted(evidence) => Some(*evidence),
            _ => None,
        };
        let balance_shortfall = match &e {
            AllocateError::BalanceInsufficient(evidence) => Some(*evidence),
            _ => None,
        };
        let (status, code) = match e {
            AllocateError::UnknownAccount => (StatusCode::NOT_FOUND, "unknown-account"),
            AllocateError::AccountInactive => (StatusCode::CONFLICT, "account-inactive"),
            // Attested and unattested shortfalls share the code, so a client
            // that predates the extension reads the refusal it always did.
            AllocateError::InsufficientBalance | AllocateError::BalanceInsufficient(_) => {
                (StatusCode::CONFLICT, "insufficient-balance")
            }
            AllocateError::BalanceExhausted(_) => (StatusCode::CONFLICT, "balance-exhausted"),
            AllocateError::InvalidTtl => (StatusCode::UNPROCESSABLE_ENTITY, "invalid-ttl"),
            AllocateError::UnknownLease => (StatusCode::NOT_FOUND, "unknown-lease"),
            AllocateError::Fenced => (StatusCode::CONFLICT, "fenced"),
            AllocateError::LeaseNotActive => (StatusCode::CONFLICT, "lease-not-active"),
            AllocateError::InvalidRelease => (StatusCode::UNPROCESSABLE_ENTITY, "invalid-release"),
            AllocateError::Storage(inner) => return ApiError::from(inner),
        };
        ApiError {
            status,
            code,
            title: e.to_string(),
            generation: None,
            balance_exhaustion,
            balance_shortfall,
        }
    }
}

impl From<CreateAccountError> for ApiError {
    fn from(e: CreateAccountError) -> Self {
        match e {
            CreateAccountError::AlreadyExists => ApiError {
                status: StatusCode::CONFLICT,
                code: "account-exists",
                title: "account already exists".to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            CreateAccountError::Storage(inner) => ApiError::from(inner),
        }
    }
}

impl From<tollgate_store::KeyError> for ApiError {
    fn from(e: tollgate_store::KeyError) -> Self {
        use tollgate_store::KeyError;
        let (status, code) = match &e {
            KeyError::UnknownAccount => (StatusCode::NOT_FOUND, "unknown-account"),
            KeyError::UnknownKey => (StatusCode::NOT_FOUND, "unknown-credential"),
            // 409, not 422: the request is well-formed and the caller is not
            // at fault for asking. It is also the retry answer — a caller that
            // lost the response and resent the same `key_id` is being told its
            // first call worked, which is the truth and discloses nothing.
            KeyError::AlreadyExists => (StatusCode::CONFLICT, "credential-exists"),
            // 409 for the same reason: nothing about the request is malformed.
            // The account is at the bound it was asked to respect, and the
            // remedy is to revoke a credential, not to rephrase the call.
            KeyError::ActiveKeyLimit { .. } => (StatusCode::CONFLICT, "active-key-limit"),
            KeyError::Storage(inner) => return inner.clone().into(),
        };
        ApiError {
            status,
            code,
            title: e.to_string(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<tollgate_store::KeySnapshotError> for ApiError {
    fn from(e: tollgate_store::KeySnapshotError) -> Self {
        use tollgate_store::KeySnapshotError;
        let (status, code) = match &e {
            // The answer revocation gives for a foreign or unknown key.
            KeySnapshotError::UnknownCredential => (StatusCode::NOT_FOUND, "unknown-credential"),
            // 409: well-formed, but the credential is terminally retired and is
            // never granted positive authorization again (INVARIANTS.md GL-27).
            KeySnapshotError::Retired { .. } => (StatusCode::CONFLICT, "credential-retired"),
            KeySnapshotError::Publish(inner) => return inner.clone().into(),
            KeySnapshotError::Storage(inner) => return inner.clone().into(),
        };
        ApiError {
            status,
            code,
            title: e.to_string(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<tollgate_store::BudgetError> for ApiError {
    fn from(e: tollgate_store::BudgetError) -> Self {
        match e {
            tollgate_store::BudgetError::UnknownAccount => ApiError {
                status: StatusCode::NOT_FOUND,
                code: "unknown-account",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            tollgate_store::BudgetError::Storage(inner) => inner.into(),
        }
    }
}

impl From<SetStatusError> for ApiError {
    fn from(e: SetStatusError) -> Self {
        match e {
            SetStatusError::UnknownAccount => ApiError {
                status: StatusCode::NOT_FOUND,
                code: "unknown-account",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            // 409, not 422: the request is well-formed and the operator is
            // not at fault for asking. The account is simply in a state no
            // transition leaves (INVARIANTS.md GL-22).
            SetStatusError::AccountClosed => ApiError {
                status: StatusCode::CONFLICT,
                code: "account-closed",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            SetStatusError::Storage(inner) => ApiError::from(inner),
        }
    }
}

impl From<PublishSnapshotError> for ApiError {
    fn from(e: PublishSnapshotError) -> Self {
        match e {
            PublishSnapshotError::CredentialMismatch { .. } => ApiError {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "invalid-credential-binding",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            PublishSnapshotError::StatusMismatch { .. } => ApiError {
                status: StatusCode::CONFLICT,
                code: "snapshot-status-mismatch",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            // 409 for the reason the status mismatch is: the request is
            // well-formed and the operator is not at fault — the account
            // simply owns this fact, and it is changed through its own
            // endpoint (GL-99).
            PublishSnapshotError::CapacityClassMismatch { .. } => ApiError {
                status: StatusCode::CONFLICT,
                code: "snapshot-capacity-class-mismatch",
                title: e.to_string(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
            PublishSnapshotError::Storage(inner) => ApiError::from(inner),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(_: StoreError) -> Self {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "storage",
            // A backend owns arbitrary text, which may include credentials or
            // private row values. Never format it into a public diagnostic.
            title: "backend unavailable".into(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl From<IngestError> for ApiError {
    fn from(error: IngestError) -> Self {
        match error {
            // The store could not answer. A client should retry, and 503 is
            // the status that says so.
            IngestError::Unavailable(e) => ApiError::from(e),
            // The store examined this batch and refused it: an accounting
            // total that cannot absorb these units will not absorb them on a
            // replay either. 422 rather than 503, so a client can tell a
            // refusal it must not repeat from an outage it should wait out —
            // which is the distinction GL-61 is about, made at both ends of the
            // wire rather than only at the transport.
            IngestError::Refused(_) => ApiError {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "usage-refused",
                title: "usage batch refused".into(),
                generation: None,
                balance_exhaustion: None,
                balance_shortfall: None,
            },
        }
    }
}

impl From<SnapshotValidationError> for ApiError {
    fn from(error: SnapshotValidationError) -> Self {
        ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "invalid-snapshot-limits",
            title: error.to_string(),
            generation: None,
            balance_exhaustion: None,
            balance_shortfall: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        self.render(|| diagnostic_id(getrandom::fill))
    }
}

/// Only generated identifiers and static machine codes cross into diagnostics.
/// The middleware receives this marker, never a backend error or response text.
#[derive(Clone)]
struct HttpFailure {
    code: &'static str,
    error_id: Option<String>,
}

fn diagnostic_id(fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>) -> Option<String> {
    let mut bytes = [0u8; 16];
    fill(&mut bytes).ok()?;
    Some(tollgate_core::RequestId(u128::from_be_bytes(bytes)).to_string())
}

impl ApiError {
    fn render(self, new_id: impl FnOnce() -> Option<String>) -> Response {
        let unauthorized = self.status == StatusCode::UNAUTHORIZED;
        let failure =
            (self.status.is_server_error() || self.code == "usage-refused").then(|| HttpFailure {
                code: self.code,
                error_id: new_id(),
            });
        let problem = Problem {
            status: self.status.as_u16(),
            code: self.code.to_string(),
            title: self.title,
            generation: self.generation,
            balance_exhaustion: self.balance_exhaustion,
            balance_shortfall: self.balance_shortfall,
        };
        // Keep the public Problem Rust shape intact. This optional JSON
        // extension is ignored by existing clients and carries no authority.
        #[derive(serde::Serialize)]
        struct DiagnosticProblem {
            #[serde(flatten)]
            problem: Problem,
            #[serde(skip_serializing_if = "Option::is_none")]
            error_id: Option<String>,
        }
        let body = DiagnosticProblem {
            problem,
            error_id: failure
                .as_ref()
                .and_then(|failure| failure.error_id.clone()),
        };
        let mut response = (self.status, Json(body)).into_response();
        if let Some(failure) = failure {
            response.extensions_mut().insert(failure);
        }
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/problem+json"),
        );
        if unauthorized {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer realm=\"tollgate-control\""),
            );
        }
        response
    }
}

/// The router owns failure reporting, including static route context. Raw
/// paths, queries, headers, request bodies and backend text are never logged.
pub(crate) async fn report_http_failure(request: Request, next: Next) -> Response {
    let route = request.extensions().get::<MatchedPath>().cloned();
    let response = next.run(request).await;
    if let Some(failure) = response.extensions().get::<HttpFailure>() {
        tracing::warn!(
            target: "tollgate::diagnostics",
            route = route.as_ref().map(MatchedPath::as_str).unwrap_or("unmatched"),
            code = failure.code,
            status = response.status().as_u16(),
            error_id = failure.error_id.as_deref(),
            error_id_unavailable = failure.error_id.is_none(),
            "control-plane operation failed; consult backend health and retained operational records"
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_identifiers_use_all_entropy_and_surface_entropy_failure() {
        let id = diagnostic_id(|bytes| {
            bytes.copy_from_slice(&0x00112233445566778899aabbccddeeff_u128.to_be_bytes());
            Ok(())
        });
        assert_eq!(id.as_deref(), Some("00112233445566778899aabbccddeeff"));
        assert!(diagnostic_id(|_| Err(getrandom::Error::UNSUPPORTED)).is_none());
    }

    #[tokio::test]
    async fn entropy_failure_preserves_the_error_without_inventing_an_identifier() {
        use http_body_util::BodyExt;
        let response = ApiError::from(StoreError("fixture-secret-70".into())).render(|| None);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let failure = response.extensions().get::<HttpFailure>().unwrap();
        assert_eq!(failure.code, "storage");
        assert!(failure.error_id.is_none());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["title"], "backend unavailable");
        assert!(json.get("error_id").is_none());
    }
}
