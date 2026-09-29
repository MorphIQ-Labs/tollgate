use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tollgate_core::{CommitError, CostUnits, DenyReason, PolicyRevision, RequestId};

use crate::Rejection;

/// Authoritative metadata copied from the committed guard, never re-quoted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChargeMetadata {
    /// Accounting identity shared with the usage event.
    pub request_id: RequestId,
    /// Full committed charge, even when business work returns an error.
    pub units_charged: CostUnits,
    /// Application policy identity carried in the usage event.
    pub policy_revision: PolicyRevision,
}

/// Failure constructing an owned, non-streaming response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseError {
    /// Informational/upgrade responses do not fit a completed-work lifetime.
    InformationalStatus,
    /// The application's JSON serializer failed after execution started.
    Serialization,
}

/// Completed response bytes. No stream, upgrade callback or body factory can
/// outlive the charge guard through this type.
///
/// Application work, including serialization, must finish before returning.
/// Detached tasks and blocking work surviving cancellation require the explicit
/// low-level guard API instead. Buffered network transmission is not execution.
#[derive(Debug)]
pub struct BufferedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl BufferedResponse {
    /// Complete a response with owned bytes and a non-informational status.
    pub fn bytes(status: StatusCode, body: impl Into<Bytes>) -> Result<Self, ResponseError> {
        if status.is_informational() {
            return Err(ResponseError::InformationalStatus);
        }
        Ok(Self {
            status,
            headers: HeaderMap::new(),
            body: body.into(),
        })
    }

    /// Serialize before returning, while the wrapper still owns the guard.
    pub fn json<T: Serialize>(status: StatusCode, value: &T) -> Result<Self, ResponseError> {
        let body = serde_json::to_vec(value).map_err(|_| ResponseError::Serialization)?;
        Ok(Self::bytes(status, body)?.with_header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ))
    }

    /// Add a header without exposing response extensions or streaming bodies.
    #[must_use]
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }
}

impl IntoResponse for BufferedResponse {
    fn into_response(self) -> Response {
        (self.status, self.headers, self.body).into_response()
    }
}

fn domain(reason: DenyReason) -> (StatusCode, &'static str) {
    match reason {
        DenyReason::UnknownPrincipal => (StatusCode::UNAUTHORIZED, "unknown-principal"),
        DenyReason::AccountSuspended
        | DenyReason::AccountClosed
        | DenyReason::MissingPermission => (StatusCode::FORBIDDEN, "forbidden"),
        DenyReason::SnapshotExpired => (StatusCode::SERVICE_UNAVAILABLE, "policy-stale"),
        DenyReason::RequestTooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, "batch-too-large"),
        DenyReason::UnpricedOperation | DenyReason::CostOverflow => {
            (StatusCode::UNPROCESSABLE_ENTITY, "unpriceable")
        }
        DenyReason::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate-limited"),
        DenyReason::RequestRateLimited => (StatusCode::TOO_MANY_REQUESTS, "request-rate-limited"),
        DenyReason::ConcurrencyLimited => (StatusCode::TOO_MANY_REQUESTS, "concurrency-limited"),
        DenyReason::UnpriceableUnderLimits { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, "unpriceable-under-limits")
        }
        DenyReason::LeaseUnavailable | DenyReason::LeaseExpired => {
            (StatusCode::SERVICE_UNAVAILABLE, "quota-unavailable")
        }
        DenyReason::BalanceExhausted => (StatusCode::PAYMENT_REQUIRED, "balance-exhausted"),
        DenyReason::BalanceInsufficient { .. } => {
            (StatusCode::PAYMENT_REQUIRED, "balance-insufficient")
        }
        DenyReason::LeaseExhausted { .. } => (StatusCode::TOO_MANY_REQUESTS, "quota-exhausted"),
        DenyReason::OverageCapExhausted { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "overage-cap-exhausted")
        }
        DenyReason::OverageCapTemporarilyExhausted { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "overage-cap-temporarily-exhausted",
        ),
        DenyReason::OverageCommitInProgress { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "overage-commit-in-progress",
        ),
        DenyReason::AccountingBackpressure => (StatusCode::SERVICE_UNAVAILABLE, "accounting-busy"),
        DenyReason::EmptyWorkload => (StatusCode::UNPROCESSABLE_ENTITY, "empty-workload"),
        DenyReason::FundingExpiredAtStart => {
            (StatusCode::SERVICE_UNAVAILABLE, "funding-expired-at-start")
        }
        DenyReason::CapacityUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "capacity-unavailable")
        }
    }
}

/// Safe default problem response. Backend messages and input contents are never
/// echoed. A custom local renderer can preserve an application's wire contract.
///
/// `charge` is present after commit, including serialization failure. An
/// impossible AlreadyCommitted core error reports unknown units rather than
/// pretending that an existing charge was refunded.
#[must_use]
pub fn render_rejection(error: &Rejection, charge: Option<ChargeMetadata>) -> BufferedResponse {
    let (status, code) = match error {
        Rejection::Denied(reason) | Rejection::Commit(CommitError::Denied(reason)) => {
            domain(*reason)
        }
        Rejection::MissingConnection => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "missing-connection-state",
        ),
        Rejection::Json(error) => (
            error.status(),
            match error.status() {
                StatusCode::UNSUPPORTED_MEDIA_TYPE => "unsupported-media-type",
                StatusCode::PAYLOAD_TOO_LARGE => "body-too-large",
                _ => "malformed-body",
            },
        ),
        Rejection::BodyTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "body-too-large"),
        Rejection::BodyTimeout => (StatusCode::REQUEST_TIMEOUT, "body-timeout"),
        Rejection::InvalidInput(_) => (StatusCode::UNPROCESSABLE_ENTITY, "invalid-input"),
        Rejection::UnexpectedBody => (StatusCode::BAD_REQUEST, "unexpected-body"),
        Rejection::RequestIdUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "request-id-unavailable")
        }
        Rejection::Commit(CommitError::Cancelled | CommitError::AlreadyReleased) => {
            (StatusCode::SERVICE_UNAVAILABLE, "execution-cancelled")
        }
        Rejection::Commit(CommitError::AlreadyCommitted) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "charge-state-invalid")
        }
        Rejection::Response(_) => (StatusCode::INTERNAL_SERVER_ERROR, "response-failed"),
    };
    let units = match (error, charge) {
        (_, Some(charge)) => Some(charge.units_charged.get()),
        (Rejection::Commit(CommitError::AlreadyCommitted), None) => None,
        _ => Some(0),
    };
    // These fields are primitives with infallible JSON representations. No
    // application Serialize implementation participates in this error path.
    let body = serde_json::to_vec(&serde_json::json!({
        "status": status.as_u16(), "code": code, "title": code, "units_charged": units,
    }))
    .expect("primitive problem fields serialize");
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    BufferedResponse {
        status,
        headers,
        body: body.into(),
    }
}
