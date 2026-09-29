use std::num::NonZeroUsize;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use axum::{Json, RequestExt};
use tollgate_admission::{CapacityGate, RequestContext};
use tollgate_client::{Clock, UsagePermit};
use tollgate_core::PermissionBits;

use crate::{Rejection, RequestAuthenticator, RequestIdSource, Tollgate};

/// Explicit upper bounds on one route's buffered input.
#[derive(Clone, Copy, Debug)]
pub struct InputLimits {
    bytes: NonZeroUsize,
    timeout: Duration,
}

/// Invalid route limits; no value is silently clamped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputLimitsError {
    /// A route must permit a positive number of bytes.
    ZeroBytes,
    /// A route must allow a positive body-read interval.
    ZeroTimeout,
    /// The duration cannot be represented as a monotonic deadline.
    TimeoutOverflow,
}

impl InputLimits {
    /// Validate limits at route construction, before accepting requests.
    pub fn new(bytes: usize, timeout: Duration) -> Result<Self, InputLimitsError> {
        let bytes = NonZeroUsize::new(bytes).ok_or(InputLimitsError::ZeroBytes)?;
        if timeout.is_zero() {
            return Err(InputLimitsError::ZeroTimeout);
        }
        if std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(InputLimitsError::TimeoutOverflow);
        }
        Ok(Self { bytes, timeout })
    }
}

/// Decoded input with its pinned policy and reserved accounting capacity.
///
/// Dropping this value charges nothing and releases the queue permit. Input
/// decoding is not business validation; validate before calling admission.
pub struct Prepared<T> {
    input: T,
    context: RequestContext,
    permit: UsagePermit,
}

impl<T> Prepared<T> {
    /// Transfer the original evidence into a custom execution boundary.
    #[must_use]
    pub fn into_parts(self) -> (T, RequestContext, UsagePermit) {
        (self.input, self.context, self.permit)
    }
}

impl<A, C, I, G> Tollgate<A, C, I, G>
where
    A: RequestAuthenticator,
    C: Clock + ?Sized,
    I: RequestIdSource,
    G: CapacityGate,
{
    /// Authenticate, begin and reserve recording capacity, then decode once.
    ///
    /// Both the explicit byte limit and Axum's existing DefaultBodyLimit apply;
    /// the smaller wins. No layer installed by this method widens another
    /// layer's limit. The default Axum limit still applies unless the application
    /// explicitly changes it. Deadline expiry and JSON rejection are uncharged.
    pub async fn prepare_json<T: serde::de::DeserializeOwned + Send>(
        &self,
        request: Request,
        required: PermissionBits,
        limits: InputLimits,
    ) -> Result<Prepared<T>, Rejection> {
        let (parts, body) = request.into_parts();
        let (context, permit) = self.stage(&parts, required)?;
        let body = Body::new(http_body_util::Limited::new(body, limits.bytes.get()));
        let request = Request::from_parts(parts, body).with_limited_body();
        let Json(input) =
            tokio::time::timeout(limits.timeout, Json::<T>::from_request(request, &()))
                .await
                .map_err(|_| Rejection::BodyTimeout)?
                .map_err(Rejection::Json)?;
        Ok(Prepared {
            input,
            context,
            permit,
        })
    }
}
