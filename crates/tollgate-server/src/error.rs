//! RFC-7807 `application/problem+json` errors with stable machine codes.
//!
//! The `code` strings are wire contract: `tollgate-client`'s HTTP transport maps
//! them back to `AllocateError` variants. Change one and the loopback
//! correctness suite fails.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use tollgate_core::Generation;
use tollgate_store::wire::Problem;
use tollgate_store::{AllocateError, CreateAccountError, StoreError};

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub title: String,
    pub generation: Option<Generation>,
}

impl ApiError {
    pub fn not_found(code: &'static str, title: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::NOT_FOUND,
            code,
            title: title.into(),
            generation: None,
        }
    }

    pub fn revoked(generation: Generation) -> Self {
        ApiError {
            status: StatusCode::GONE,
            code: "revoked-principal",
            title: "snapshot revoked".to_string(),
            generation: Some(generation),
        }
    }

    pub fn bad_request(code: &'static str, title: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            code,
            title: title.into(),
            generation: None,
        }
    }
}

impl From<AllocateError> for ApiError {
    fn from(e: AllocateError) -> Self {
        let (status, code) = match &e {
            AllocateError::UnknownAccount => (StatusCode::NOT_FOUND, "unknown-account"),
            AllocateError::AccountInactive => (StatusCode::CONFLICT, "account-inactive"),
            AllocateError::InsufficientBalance => (StatusCode::CONFLICT, "insufficient-balance"),
            AllocateError::InvalidTtl => (StatusCode::UNPROCESSABLE_ENTITY, "invalid-ttl"),
            AllocateError::UnknownLease => (StatusCode::NOT_FOUND, "unknown-lease"),
            AllocateError::Fenced => (StatusCode::CONFLICT, "fenced"),
            AllocateError::LeaseNotActive => (StatusCode::CONFLICT, "lease-not-active"),
            AllocateError::InvalidRelease => (StatusCode::UNPROCESSABLE_ENTITY, "invalid-release"),
            AllocateError::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage"),
        };
        ApiError {
            status,
            code,
            title: e.to_string(),
            generation: None,
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
            },
            CreateAccountError::Storage(inner) => ApiError::from(inner),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "storage",
            title: e.to_string(),
            generation: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let problem = Problem {
            status: self.status.as_u16(),
            code: self.code.to_string(),
            title: self.title,
            generation: self.generation,
        };
        let mut response = (self.status, Json(problem)).into_response();
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}
