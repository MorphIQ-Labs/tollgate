//! The account's budget estimate, for responses and for reads that admit
//! nothing.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::request::Parts;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, get};
use tollgate_client::Clock;
use tollgate_core::{BudgetView, PermissionBits};

use crate::{BufferedResponse, Rejection, RequestAuthenticator, ResponseError, Tollgate};

/// Whether a route reads the estimate for its callbacks. Chosen when the route
/// is built, so a route that reports nothing performs no read.
pub(crate) trait Estimate: Send + Sync + 'static {
    /// What the route's protocol carries to its callbacks. The default is the
    /// value for a request refused before it was authorized.
    type Value: Copy + Default + Send + Sync + 'static;

    fn read(observe: impl FnOnce() -> Option<BudgetView>) -> Self::Value;
}

/// The route's callbacks take no estimate.
pub(crate) struct Unread;

impl Estimate for Unread {
    type Value = ();

    fn read(_: impl FnOnce() -> Option<BudgetView>) {}
}

/// The route's callbacks take the estimate.
pub(crate) struct Reported;

impl Estimate for Reported {
    type Value = Option<BudgetView>;

    fn read(observe: impl FnOnce() -> Option<BudgetView>) -> Option<BudgetView> {
        observe()
    }
}

impl<A, C, I, G> Tollgate<A, C, I, G>
where
    A: RequestAuthenticator,
    C: Clock + ?Sized,
{
    /// Authenticate a request and read its account's budget estimate without
    /// admitting anything, for a handler this crate does not wrap.
    ///
    /// The request is verified by the adapter's authenticator at the adapter's
    /// clock and authorized as a metered request is: an active account, a
    /// fresh snapshot and the `required` permission. It creates no admission
    /// context, reserves no accounting slot, takes no rate token, concurrency
    /// slot or execution capacity, charges nothing, and **counts nothing** on
    /// the instance, whether it answers or refuses (INVARIANTS.md 20). A
    /// refusal the application wants tallied is the application's to tally, and
    /// so is any rate limit on this read.
    ///
    /// `Ok(None)` means the control plane has reported no budget. Otherwise
    /// the balance and period end come from one observation. It is an estimate
    /// that lags the ledger and never decides admission (INVARIANTS.md 28).
    ///
    /// # Errors
    ///
    /// The authenticator's [`Rejection`], or [`Rejection::Denied`] with the
    /// reason authorization refused.
    pub fn read_budget(
        &self,
        parts: &Parts,
        required: PermissionBits,
    ) -> Result<Option<BudgetView>, Rejection> {
        let now = self.config.clock.now();
        let principal = self.config.authenticator.authenticate(parts, now)?;
        self.config
            .runtime
            .read_budget(principal, required, now)
            .map_err(Rejection::Denied)
    }

    /// An unmetered GET that answers with the caller's budget estimate:
    /// [`Self::read_budget`] followed by `respond`.
    ///
    /// Both callbacks are synchronous and local. `respond` receives `None`
    /// when the control plane has reported no budget; the application chooses
    /// the wire shape. `error_handler` receives authentication and
    /// authorization refusals, and a `respond` failure as
    /// [`Rejection::Response`]; pass
    /// `|error| render_rejection(error, None)` for the default problem
    /// response. No request body is read.
    pub fn get_budget<Respond, E>(
        &self,
        required: PermissionBits,
        respond: Respond,
        error_handler: E,
    ) -> MethodRouter
    where
        A: 'static,
        C: 'static,
        I: Send + Sync + 'static,
        G: Send + Sync + 'static,
        Respond: Fn(Option<BudgetView>) -> Result<BufferedResponse, ResponseError>
            + Send
            + Sync
            + 'static,
        E: Fn(&Rejection) -> BufferedResponse + Send + Sync + 'static,
    {
        let shared = Arc::new((self.clone(), respond, error_handler));
        get(move |request: Request| {
            let shared = Arc::clone(&shared);
            async move {
                let (adapter, respond, error_handler) = shared.as_ref();
                let (parts, _unread) = request.into_parts();
                adapter
                    .read_budget(&parts, required)
                    .and_then(|view| respond(view).map_err(Rejection::Response))
                    .unwrap_or_else(|error| error_handler(&error))
                    .into_response()
            }
        })
    }
}
