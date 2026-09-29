use axum::{
    body::to_bytes,
    http::{HeaderName, HeaderValue, StatusCode},
    response::IntoResponse,
};
use tollgate_axum::{
    BufferedResponse, ChargeMetadata, InputError, Rejection, ResponseError, render_rejection,
};
use tollgate_core::{CommitError, CostUnits, DenyReason, PolicyRevision, RequestId};

async fn check(error: Rejection, status: u16, code: &str) {
    for charge in [
        None,
        Some(ChargeMetadata {
            request_id: RequestId(7),
            units_charged: CostUnits(13),
            policy_revision: PolicyRevision::UNSTATED,
        }),
    ] {
        let response = render_rejection(&error, charge).into_response();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(
            response.headers()["content-type"],
            "application/problem+json"
        );
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], status);
        assert_eq!(body["code"], code);
        assert_eq!(body["title"], code);
        let units = if charge.is_some() {
            serde_json::json!(13)
        } else if matches!(error, Rejection::Commit(CommitError::AlreadyCommitted)) {
            serde_json::Value::Null
        } else {
            serde_json::json!(0)
        };
        assert_eq!(body["units_charged"], units);
    }
}

#[tokio::test]
async fn every_domain_reason_has_a_stable_http_response() {
    use DenyReason::*;
    let cases = [
        (UnknownPrincipal, 401, "unknown-principal"),
        (AccountSuspended, 403, "forbidden"),
        (AccountClosed, 403, "forbidden"),
        (MissingPermission, 403, "forbidden"),
        (SnapshotExpired, 503, "policy-stale"),
        (RequestTooLarge { max_items: 2 }, 413, "batch-too-large"),
        (UnpricedOperation, 422, "unpriceable"),
        (CostOverflow, 422, "unpriceable"),
        (RateLimited, 429, "rate-limited"),
        (RequestRateLimited, 429, "request-rate-limited"),
        (ConcurrencyLimited, 429, "concurrency-limited"),
        (
            UnpriceableUnderLimits {
                weight: CostUnits(2),
                burst_units: CostUnits(1),
            },
            422,
            "unpriceable-under-limits",
        ),
        (LeaseUnavailable, 503, "quota-unavailable"),
        (LeaseExpired, 503, "quota-unavailable"),
        (BalanceExhausted, 402, "balance-exhausted"),
        (
            BalanceInsufficient {
                remaining: CostUnits(1),
            },
            402,
            "balance-insufficient",
        ),
        (
            LeaseExhausted {
                remaining: CostUnits(1),
            },
            429,
            "quota-exhausted",
        ),
        (
            OverageCapExhausted {
                spent: CostUnits(1),
                overage_cap: CostUnits(1),
            },
            503,
            "overage-cap-exhausted",
        ),
        (
            OverageCapTemporarilyExhausted {
                spent: CostUnits(1),
                overage_cap: CostUnits(1),
            },
            503,
            "overage-cap-temporarily-exhausted",
        ),
        (
            OverageCommitInProgress {
                spent: CostUnits(1),
                overage_cap: CostUnits(1),
            },
            503,
            "overage-commit-in-progress",
        ),
        (AccountingBackpressure, 503, "accounting-busy"),
        (EmptyWorkload, 422, "empty-workload"),
        (FundingExpiredAtStart, 503, "funding-expired-at-start"),
        (CapacityUnavailable, 503, "capacity-unavailable"),
    ];
    assert_eq!(cases.len(), DenyReason::COUNT);
    for (reason, status, code) in cases {
        check(Rejection::Denied(reason), status, code).await;
        check(Rejection::Commit(CommitError::Denied(reason)), status, code).await;
    }
}

#[tokio::test]
async fn transport_errors_preserve_charge_truth_without_echoing_input() {
    for (error, status, code) in [
        (
            Rejection::MissingConnection,
            500,
            "missing-connection-state",
        ),
        (Rejection::BodyTooLarge, 413, "body-too-large"),
        (Rejection::BodyTimeout, 408, "body-timeout"),
        (
            Rejection::InvalidInput(InputError("private application detail")),
            422,
            "invalid-input",
        ),
        (Rejection::UnexpectedBody, 400, "unexpected-body"),
        (
            Rejection::RequestIdUnavailable,
            503,
            "request-id-unavailable",
        ),
        (
            Rejection::Commit(CommitError::Cancelled),
            503,
            "execution-cancelled",
        ),
        (
            Rejection::Commit(CommitError::AlreadyReleased),
            503,
            "execution-cancelled",
        ),
        (
            Rejection::Commit(CommitError::AlreadyCommitted),
            500,
            "charge-state-invalid",
        ),
        (
            Rejection::Response(ResponseError::Serialization),
            500,
            "response-failed",
        ),
        (
            Rejection::Response(ResponseError::InformationalStatus),
            500,
            "response-failed",
        ),
    ] {
        check(error, status, code).await;
    }
}

#[tokio::test]
async fn buffered_responses_preserve_headers_and_bytes_but_refuse_upgrades() {
    for status in 100..200 {
        assert_eq!(
            BufferedResponse::bytes(StatusCode::from_u16(status).unwrap(), "").unwrap_err(),
            ResponseError::InformationalStatus
        );
    }
    let response = BufferedResponse::bytes(StatusCode::CREATED, "owned")
        .unwrap()
        .with_header(
            HeaderName::from_static("x-result"),
            HeaderValue::from_static("complete"),
        )
        .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-result"], "complete");
    assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), "owned");
    let response = BufferedResponse::json(StatusCode::OK, &vec![1, 2, 3])
        .unwrap()
        .into_response();
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(
        to_bytes(response.into_body(), 100).await.unwrap(),
        "[1,2,3]"
    );
}
