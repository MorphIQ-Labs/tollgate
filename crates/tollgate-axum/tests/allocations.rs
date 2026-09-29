mod support;

use axum::{Router, body::Body, http::StatusCode};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};
use support::*;
use tollgate_admission::NoGate;
use tollgate_alloc_count::AllocScope;
use tollgate_axum::{BufferedResponse, Validated};
use tollgate_core::{CapacityClass, PermissionBits, RequestId};
use tower::ServiceExt;

tollgate_alloc_count::install!();

fn complete<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => {
            panic!("the buffered local path must complete without waiting for a store")
        }
    }
}

#[tokio::test(start_paused = true)]
async fn metered_wrapper_adds_no_allocations_to_equivalent_manual_http_glue() {
    let (adapter, runtime, _, _) = fixture_with(NoGate, 64, CapacityClass::Assured).await;
    let wrapped = Router::new().route(
        "/",
        adapter.post_json(
            Op,
            PermissionBits::bit(0),
            limits(100),
            |quantity: u64| Ok(Validated::new((), quantity)),
            |(), _| async { BufferedResponse::bytes(StatusCode::OK, "ok") },
        ),
    );
    let manual_adapter = adapter.clone();
    let manual = Router::new().route(
        "/",
        axum::routing::post(move |req| {
            let adapter = manual_adapter.clone();
            async move {
                let prepared = adapter
                    .prepare_json::<u64>(req, PermissionBits::bit(0), limits(100))
                    .await
                    .unwrap();
                let (quantity, context, permit) = prepared.into_parts();
                let committed = context
                    .admit(&[(Op, quantity)], permit, time(100))
                    .unwrap()
                    .acquire_capacity(&NoGate)
                    .unwrap()
                    .commit(RequestId(1000 + quantity as u128), time(100))
                    .unwrap();
                let response = BufferedResponse::bytes(StatusCode::OK, "ok").unwrap();
                drop(committed);
                response
            }
        }),
    );
    // Warm framework/TLS paths before comparing the same parse/work/output.
    for router in [manual.clone(), wrapped.clone()] {
        assert_eq!(
            complete(router.oneshot(request(Body::from("1"))))
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    for quantity in 2..8 {
        let request_manual = request(Body::from(quantity.to_string()));
        let request_wrapped = request(Body::from(quantity.to_string()));
        let manual = manual.clone();
        let wrapped = wrapped.clone();
        let (response, baseline) =
            AllocScope::measure(|| complete(manual.oneshot(request_manual)).unwrap());
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        let (response, actual) =
            AllocScope::measure(|| complete(wrapped.oneshot(request_wrapped)).unwrap());
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        tollgate_alloc_count::record_if_requested!("axum/manual_http", "caller", baseline).unwrap();
        tollgate_alloc_count::record_if_requested!("axum/wrapped_http", "caller", actual).unwrap();
        assert_eq!(actual.alloc_calls, baseline.alloc_calls);
        assert_eq!(actual.alloc_zeroed_calls, baseline.alloc_zeroed_calls);
        assert_eq!(actual.realloc_calls, baseline.realloc_calls);
    }
    // Transport has already decoded. The adapter's existing core protocol is
    // separately held to zero, so HTTP allocation attribution cannot hide it.
    let prepared = adapter
        .prepare_json::<u64>(
            request(Body::from("1")),
            PermissionBits::bit(0),
            limits(100),
        )
        .await
        .unwrap();
    let (quantity, context, permit) = prepared.into_parts();
    let ((), allocations) = AllocScope::measure(|| {
        let committed = context
            .admit(&[(Op, quantity)], permit, time(100))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(2000), time(100))
            .unwrap();
        drop(committed);
    });
    tollgate_alloc_count::record_if_requested!(
        "axum/prepared_admission_through_record",
        "tollgate",
        allocations
    )
    .unwrap();
    assert!(allocations.is_allocation_free(), "{allocations:?}");
    assert_eq!(
        runtime.shutdown().await.unwrap().usage.unwrap().accepted,
        15
    );
}
