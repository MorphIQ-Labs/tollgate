//! Axum's transport boundary for Tollgate admission.
//!
//! An application owns one [`tollgate_client::InstanceRuntime`] and shares its
//! handle with [`Tollgate`]. Authentication, permission checks and accounting
//! backpressure precede body decoding. Preparing input never charges it.
//! The runtime owner remains responsible for readiness and graceful shutdown.

#![deny(missing_docs)]

mod auth;
mod input;

use std::sync::Arc;

use axum::http::request::Parts;
use tollgate_admission::{CapacityGate, RequestContext};
use tollgate_client::{Clock, RuntimeHandle, UsagePermit};
use tollgate_core::{DenyReason, PermissionBits, RequestId};

pub use auth::{BearerAuth, RequestAuthenticator, TollgateConnection};
pub use input::{InputLimits, InputLimitsError, Prepared};

/// A request-local failure before business execution starts. Always uncharged.
#[derive(Debug)]
pub enum Rejection {
    /// A domain refusal, already classified by the admission engine.
    Denied(DenyReason),
    /// The bearer authenticator requires the installed connection context.
    MissingConnection,
    /// Axum refused the bounded JSON input.
    Json(axum::extract::rejection::JsonRejection),
    /// A configured or outer body byte limit was exceeded.
    BodyTooLarge,
    /// The configured body-read deadline elapsed.
    BodyTimeout,
}

/// Generates accounting IDs unique across service instances and restarts.
///
/// Implementations run before commit and must be local and nonblocking. Do not
/// use a process-local counter in production: IDs deduplicate usage globally.
pub trait RequestIdSource: Send + Sync + 'static {
    /// Obtain the next ID, refusing safely if the source is unavailable.
    fn next_id(&self) -> Result<RequestId, RequestIdUnavailable>;
}

/// A request-ID source could not safely produce an ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestIdUnavailable;

/// Shared application configuration. Construction starts no tasks.
///
/// All operational choices are explicit. The runtime and capacity gate have
/// already validated their configurations in their owning libraries.
pub struct AdapterConfig<A, C: ?Sized, I, G> {
    /// Existing runtime's request-side handle, independent of store topology.
    pub runtime: RuntimeHandle,
    /// Local principal verification; never trust identity from a raw header.
    pub authenticator: A,
    /// Clock shared with the application/runtime for policy instants.
    pub clock: Arc<C>,
    /// Cross-instance accounting IDs, obtained before commit.
    pub request_ids: I,
    /// Startup-selected capacity gate, including zero-sized NoGate.
    pub capacity: G,
}

/// Cloneable Axum adapter sharing one configuration and runtime handle.
pub struct Tollgate<A, C: ?Sized, I, G> {
    pub(crate) config: Arc<AdapterConfig<A, C, I, G>>,
}

impl<A, C: ?Sized, I, G> Clone for Tollgate<A, C, I, G> {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
        }
    }
}

impl<A, C, I, G> Tollgate<A, C, I, G>
where
    A: RequestAuthenticator,
    C: Clock + ?Sized,
    I: RequestIdSource,
    G: CapacityGate,
{
    /// Share validated runtime/gate configuration without starting workers.
    #[must_use]
    pub fn new(config: AdapterConfig<A, C, I, G>) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Borrow the existing runtime for readiness, metrics and shutdown control.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeHandle {
        &self.config.runtime
    }

    pub(crate) fn stage(
        &self,
        parts: &Parts,
        required: PermissionBits,
    ) -> Result<(RequestContext, UsagePermit), Rejection> {
        let now = self.config.clock.now();
        let principal = self
            .config
            .authenticator
            .authenticate(parts, now)
            .inspect_err(|rejection| {
                if let Rejection::Denied(reason) = rejection {
                    self.config.runtime.counters().record_deny(reason);
                }
            })?;
        let context = self
            .config
            .runtime
            .begin(principal, required, now)
            .map_err(Rejection::Denied)?;
        let permit = self
            .config
            .runtime
            .recorder()
            .try_reserve()
            .map_err(|reason| {
                self.config.runtime.counters().record_deny(&reason);
                Rejection::Denied(reason)
            })?;
        Ok((context, permit))
    }
}
