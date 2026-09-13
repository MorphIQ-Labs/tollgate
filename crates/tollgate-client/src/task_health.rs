//! A task owns the last value observers retain after its health channel closes.
use tokio::sync::watch;

/// Construct before spawning, and move into the future. Unlike a statement
/// after `run().await`, Drop also runs on unwind, cancellation and destruction
/// before the first poll. There is one publisher; callers only borrow it.
pub(crate) struct TaskHealth(watch::Sender<bool>);

impl TaskHealth {
    pub(crate) fn channel(initial: bool) -> (Self, watch::Receiver<bool>) {
        let (sender, receiver) = watch::channel(initial);
        (Self(sender), receiver)
    }

    pub(crate) fn sender(&self) -> &watch::Sender<bool> {
        &self.0
    }
}

impl Drop for TaskHealth {
    fn drop(&mut self) {
        // Retain false even if no receivers currently remain. Existing
        // receivers observe this value before closure, including via borrow.
        self.0.send_replace(false);
    }
}
