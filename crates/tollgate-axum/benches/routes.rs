//! Compare orchestration with identical JSON/authentication/core work. Request
//! construction and writer draining occur outside the timed interval.
#[path = "../tests/support/mod.rs"]
mod support;

use axum::{Router, body::Body, http::StatusCode};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use support::*;
use tollgate_admission::NoGate;
use tollgate_axum::{BufferedResponse, Validated};
use tollgate_client::Clock;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    PermissionBits, Principal, PublishableSnapshot, RequestId, ResolvedLimits,
};
use tower::ServiceExt;

fn complete<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("buffered admission unexpectedly waited"),
    }
}
fn routes(c: &mut Criterion) {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (adapter, runtime, store, clock) =
        executor.block_on(fixture_with(NoGate, 64, CapacityClass::Assured));
    // A priceable zero-cost operation exercises the same lifecycle without
    // benchmark duration depending on how fast a finite fixture deposit drains.
    let snapshot = AccountSnapshot::builder(
        AccountId(1),
        Generation(2),
        AccountStatus::Active,
        time(1000),
        PermissionBits::bit(0),
        ResolvedLimits::new(100),
        Arc::new(
            CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                .weight(&Op, CostUnits::ZERO)
                .build(),
        ),
    )
    .build();
    store
        .publish_snapshot(
            Principal(1),
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
        )
        .unwrap();
    executor.block_on(async {
        while adapter
            .runtime()
            .begin(Principal(1), PermissionBits::bit(0), time(100))
            .unwrap()
            .generation()
            != Generation(2)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let bounds = limits(100);
    let wrapped = Router::new().route(
        "/",
        adapter.post_json(
            Op,
            PermissionBits::bit(0),
            bounds,
            |quantity: u64| Ok(Validated::new((), quantity)),
            |(), _| async { BufferedResponse::bytes(StatusCode::OK, "ok") },
        ),
    );
    let manual_state = Arc::new((
        adapter.clone(),
        clock,
        std::sync::atomic::AtomicU64::new(1_000_000_000_000),
    ));
    let manual = Router::new().route(
        "/",
        axum::routing::post(move |req| {
            let state = manual_state.clone();
            async move {
                let (adapter, clock, ids) = state.as_ref();
                let (quantity, context, permit) = adapter
                    .prepare_json::<u64>(req, PermissionBits::bit(0), bounds)
                    .await
                    .unwrap()
                    .into_parts();
                let committed = context
                    .admit(&[(Op, quantity)], permit, clock.now())
                    .unwrap()
                    .acquire_capacity(&NoGate)
                    .unwrap()
                    .commit(
                        RequestId(u128::from(
                            ids.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        )),
                        clock.now(),
                    )
                    .unwrap();
                let response = BufferedResponse::bytes(StatusCode::OK, "ok").unwrap();
                drop(committed);
                response
            }
        }),
    );
    let mut group = c.benchmark_group("axum_orchestration");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    for (name, router) in [("manual", manual), ("wrapped", wrapped)] {
        group.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    executor.block_on(async {
                        while adapter.runtime().recorder().try_reserve().is_err() {
                            tokio::task::yield_now().await;
                        }
                    });
                    let router = router.clone();
                    let req = request(Body::from("1"));
                    let _entered = executor.enter();
                    let start = Instant::now();
                    let response = complete(router.oneshot(req)).unwrap();
                    elapsed += start.elapsed();
                    assert_eq!(response.status(), StatusCode::OK);
                    std::hint::black_box(response);
                }
                elapsed
            })
        });
    }
    group.finish();
    executor.block_on(runtime.shutdown()).unwrap();
}
criterion_group!(benches, routes);
criterion_main!(benches);
