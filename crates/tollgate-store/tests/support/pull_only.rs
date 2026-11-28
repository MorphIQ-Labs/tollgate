//! A snapshot source that can only be pulled, for witnesses of refresh.
//!
//! A test that claims a change arrives by a pull must not be satisfiable by a
//! push: an in-process store publishes both ways, so a witness over it stays
//! green with the pull switched off. Wrapping the source here closes the push
//! stream whatever the wrapped source does, as it is closed over HTTP, and
//! offers no way to open it again.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use tollgate_core::Principal;
use tollgate_store::{SnapshotPush, SnapshotResolution, SnapshotSource, StoreError};

/// Forwards every pull to the wrapped source and pushes nothing.
pub struct PullOnly<S>(Arc<S>);

/// `source` with its push stream closed.
pub fn pull_only<S: SnapshotSource>(source: Arc<S>) -> Arc<PullOnly<S>> {
    Arc::new(PullOnly(source))
}

#[deny(
    clippy::missing_trait_methods,
    reason = "a defaulted method inherited here would hide the wrapped source's answer"
)]
#[async_trait]
impl<S: SnapshotSource> SnapshotSource for PullOnly<S> {
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        self.0.snapshot(principal).await
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        // The sender is dropped here, so the receiver reports `Closed`.
        broadcast::channel(1).1
    }

    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        self.0.principals().await
    }
}
