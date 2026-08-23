//! Readiness tells the truth about the store behind it (INVARIANTS.md #10).
//!
//! A server whose source of truth is unreachable must not attract traffic,
//! and — since #36 — must also say *why* rather than only answering 503.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, CostUnits, Principal, PublishableSnapshot};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, CreateAccountError, IngestReport, LeaseAllocator,
    ReclaimBatch, SnapshotPush, SnapshotResolution, SnapshotSource, StoreError, StoreHealth,
    SystemClock, UsageSink,
};

use tollgate_server::{ServerState, router};

/// A backend that answers only `ping`, and answers it however the test says.
/// Everything else is unreachable: a readiness probe must not depend on it.
struct PingOnlyStore {
    healthy: bool,
}

#[async_trait]
impl StoreHealth for PingOnlyStore {
    async fn ping(&self) -> Result<(), StoreError> {
        if self.healthy {
            Ok(())
        } else {
            Err(StoreError("store unreachable".into()))
        }
    }
}

#[async_trait]
impl LeaseAllocator for PingOnlyStore {
    async fn acquire(
        &self,
        _account: AccountId,
        _requested: CostUnits,
        _ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, AllocateError> {
        unreachable!("readiness never acquires")
    }

    async fn release(
        &self,
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), AllocateError> {
        unreachable!("readiness never releases")
    }

    async fn reclaim_expired_batch(
        &self,
        _now: Timestamp,
        _limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        unreachable!("readiness never reclaims")
    }
}

#[async_trait]
impl SnapshotSource for PingOnlyStore {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        unreachable!("readiness never fetches snapshots")
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        unreachable!("readiness never subscribes")
    }
}

#[async_trait]
impl UsageSink for PingOnlyStore {
    async fn ingest(
        &self,
        _events: &[tollgate_core::UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        unreachable!("readiness never ingests")
    }
}

#[async_trait]
impl AdminStore for PingOnlyStore {
    async fn create_account(&self, _config: AccountConfig) -> Result<(), CreateAccountError> {
        unreachable!("readiness never administers")
    }

    async fn deposit(&self, _account: AccountId, _units: CostUnits) -> Result<(), AllocateError> {
        unreachable!("readiness never administers")
    }

    async fn set_active(&self, _account: AccountId, _active: bool) -> Result<(), AllocateError> {
        unreachable!("readiness never administers")
    }

    async fn publish_snapshot(
        &self,
        _principal: Principal,
        _snapshot: PublishableSnapshot,
    ) -> Result<(), StoreError> {
        unreachable!("readiness never administers")
    }

    async fn remove_snapshot(&self, _principal: Principal) -> Result<(), StoreError> {
        unreachable!("readiness never administers")
    }
}

async fn readyz_status(healthy: bool) -> axum::http::StatusCode {
    use tower::ServiceExt as _;

    let app = router(ServerState {
        store: Arc::new(PingOnlyStore { healthy }),
        clock: Arc::new(SystemClock),
    });
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

/// The failing half: a store that cannot answer must not read as ready.
/// Fail-closed correctness must not masquerade as availability.
#[tokio::test]
async fn readyz_is_503_when_the_store_cannot_answer() {
    assert_eq!(
        readyz_status(false).await,
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}

/// And the converse, so "always 503" would fail too: a healthy store is ready.
#[tokio::test]
async fn readyz_is_200_when_the_store_answers() {
    assert_eq!(readyz_status(true).await, axum::http::StatusCode::OK);
}
