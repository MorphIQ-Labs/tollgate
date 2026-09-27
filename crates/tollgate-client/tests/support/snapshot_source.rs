use std::sync::{Arc, Mutex};
use tollgate_core::Principal;
use tollgate_store::{SnapshotPush, SnapshotResolution};

// The delegating double is a sibling module in whichever test binary includes
// this file, so it is referred to rather than included again: loading the same
// file as a module twice in one binary is `clippy::duplicate_mod`.
use crate::delegating::rejecting;
pub(crate) use crate::delegating::{DelegatingStore, RejectingStore};

/// A source whose pulls are fixed and whose pushes the test drives directly.
///
/// The GL-53 regression tests all use `MutableNoPushSource`, so they exercise the
/// *sweep*. Pushes are the primary propagation path for an in-process store,
/// with the sweep as fallback — and the manager applies the same generation
/// rule at both. This source is what lets the push half be pinned.
pub(crate) struct DrivenPushSource {
    pull: Mutex<SnapshotResolution>,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

impl DrivenPushSource {
    /// The handle and the source to hand `SnapshotManager::spawn`.
    pub(crate) fn new(
        pull: SnapshotResolution,
    ) -> (Arc<Self>, Arc<DelegatingStore<RejectingStore>>) {
        let (push, _) = tokio::sync::broadcast::channel(8);
        let source = Arc::new(Self {
            pull: Mutex::new(pull),
            push,
        });
        let (pulls, subs) = (Arc::clone(&source), Arc::clone(&source));
        (
            source,
            Arc::new(
                rejecting("a driven-push fixture answers snapshots and nothing else")
                    .on_snapshot(move |_, _principal| {
                        let source = Arc::clone(&pulls);
                        async move { Ok(source.pull.lock().expect("pull mode poisoned").clone()) }
                    })
                    .on_subscribe(move |_| subs.push.subscribe())
                    // Stated, not inherited: this source has no catalogue, and
                    // saying so is what keeps it distinguishable from a wrapper
                    // that forgot to forward one it did have (GL-83).
                    .on_principals(|_| async { Ok(None) }),
            ),
        )
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
