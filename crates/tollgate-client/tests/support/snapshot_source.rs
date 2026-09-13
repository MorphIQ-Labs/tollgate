use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tollgate_core::Principal;
use tollgate_store::{SnapshotPush, SnapshotResolution, SnapshotSource, StoreError};

/// A source whose pulls are fixed and whose pushes the test drives directly.
///
/// The #53 regression tests all use `MutableNoPushSource`, so they exercise the
/// *sweep*. Pushes are the primary propagation path for an in-process store,
/// with the sweep as fallback — and the manager applies the same generation
/// rule at both. This source is what lets the push half be pinned.
pub(crate) struct DrivenPushSource {
    pull: Mutex<SnapshotResolution>,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

impl DrivenPushSource {
    pub(crate) fn new(pull: SnapshotResolution) -> Arc<Self> {
        let (push, _) = tokio::sync::broadcast::channel(8);
        Arc::new(Self {
            pull: Mutex::new(pull),
            push,
        })
    }

    pub(crate) fn set_pull(&self, resolution: SnapshotResolution) {
        *self.pull.lock().expect("pull mode poisoned") = resolution;
    }

    pub(crate) fn send(&self, principal: Principal, resolution: SnapshotResolution) {
        // Zero receivers is not a failure here either -- the manager may not
        // have subscribed yet -- but the count is the difference between a
        // push landing and the test silently retesting the sweep, so it is
        // asserted rather than discarded.
        let delivered = self
            .push
            .send(SnapshotPush {
                principal,
                resolution,
            })
            .expect("the manager must be subscribed");
        assert_eq!(delivered, 1, "exactly one subscriber must receive the push");
    }
}

#[async_trait]
impl SnapshotSource for DrivenPushSource {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        Ok(self.pull.lock().expect("pull mode poisoned").clone())
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}
