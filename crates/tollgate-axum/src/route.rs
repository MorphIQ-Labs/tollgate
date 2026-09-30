use std::future::Future;
use std::sync::Arc;

use axum::body::HttpBody;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use tollgate_admission::{CapacityGate, RequestContext};
use tollgate_client::{Clock, UsagePermit};
use tollgate_core::{OpIndex, PermissionBits};

use crate::{
    BufferedResponse, ChargeMetadata, InputLimits, Rejection, RequestAuthenticator,
    RequestIdSource, ResponseError, Tollgate, render_rejection,
};

/// Safe application validation failure before execution. Messages should be
/// fixed descriptions; do not put raw request data or credentials in them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputError(pub &'static str);

/// Business input and a checked quantity, produced before admission.
pub struct Validated<T> {
    input: T,
    quantity: u64,
}

impl<T> Validated<T> {
    /// Associate validated input with its operation quantity. Core admission
    /// still checks zero quantities, item limits, permissions and cost overflow.
    #[must_use]
    pub fn new(input: T, quantity: u64) -> Self {
        Self { input, quantity }
    }
}

impl<A, C, I, G> Tollgate<A, C, I, G>
where
    A: RequestAuthenticator,
    C: Clock + ?Sized,
    I: RequestIdSource,
    G: CapacityGate,
{
    /// Meter a POST with bounded JSON and synchronous pre-execution validation.
    ///
    /// The callback factory and its future run after commit. Return only owned
    /// buffered output; detached work, upgrades and automatic retries require
    /// a custom low-level integration. An outer canceling timeout is supported.
    pub fn post_json<T, V, O, Validate, Execute, F>(
        &self,
        operation: O,
        required: PermissionBits,
        limits: InputLimits,
        validate: Validate,
        execute: Execute,
    ) -> MethodRouter
    where
        T: serde::de::DeserializeOwned + Send + 'static,
        V: Send + 'static,
        O: OpIndex + Copy + Send + Sync + 'static,
        Validate: Fn(T) -> Result<Validated<V>, InputError> + Send + Sync + 'static,
        Execute: Fn(V, ChargeMetadata) -> F + Send + Sync + 'static,
        F: Future<Output = Result<BufferedResponse, ResponseError>> + Send + 'static,
    {
        self.post_json_with_error_handler(
            operation,
            required,
            limits,
            validate,
            execute,
            render_rejection,
        )
    }

    /// As [`Self::post_json`], with an application-owned error renderer.
    ///
    /// Error rendering is synchronous/local and receives authoritative charge
    /// metadata for failures after start. It must not retry execution.
    pub fn post_json_with_error_handler<T, V, O, Validate, Execute, F, E>(
        &self,
        operation: O,
        required: PermissionBits,
        limits: InputLimits,
        validate: Validate,
        execute: Execute,
        error_handler: E,
    ) -> MethodRouter
    where
        T: serde::de::DeserializeOwned + Send + 'static,
        V: Send + 'static,
        O: OpIndex + Copy + Send + Sync + 'static,
        Validate: Fn(T) -> Result<Validated<V>, InputError> + Send + Sync + 'static,
        Execute: Fn(V, ChargeMetadata) -> F + Send + Sync + 'static,
        F: Future<Output = Result<BufferedResponse, ResponseError>> + Send + 'static,
        E: Fn(&Rejection, Option<ChargeMetadata>) -> BufferedResponse + Send + Sync + 'static,
    {
        // One route-state Arc clone per invocation. Callbacks are borrowed, not
        // cloned, and no extra boxed callback future is introduced here.
        let shared = Arc::new((self.clone(), validate, execute, error_handler));
        post(move |request: Request| {
            let shared = Arc::clone(&shared);
            async move {
                let (adapter, validate, execute, error_handler) = shared.as_ref();
                let prepared = match adapter.prepare_json::<T>(request, required, limits).await {
                    Ok(prepared) => prepared,
                    Err(error) => return error_handler(&error, None).into_response(),
                };
                let (input, context, permit) = prepared.into_parts();
                let input = match validate(input) {
                    Ok(input) => input,
                    Err(error) => {
                        return error_handler(&Rejection::InvalidInput(error), None)
                            .into_response();
                    }
                };
                adapter
                    .execute((context, permit), operation, input, execute, error_handler)
                    .await
            }
        })
    }

    /// Meter a bodyless POST with a validated, usually fixed quantity.
    ///
    /// No body is read. A body not known to be empty is refused. Business data
    /// must come from the validated callback input, not a deferred body read.
    pub fn post<V, O, Validate, Execute, F>(
        &self,
        operation: O,
        required: PermissionBits,
        validate: Validate,
        execute: Execute,
    ) -> MethodRouter
    where
        V: Send + 'static,
        O: OpIndex + Copy + Send + Sync + 'static,
        Validate: Fn() -> Result<Validated<V>, InputError> + Send + Sync + 'static,
        Execute: Fn(V, ChargeMetadata) -> F + Send + Sync + 'static,
        F: Future<Output = Result<BufferedResponse, ResponseError>> + Send + 'static,
    {
        let shared = Arc::new((self.clone(), validate, execute));
        post(move |request: Request| {
            let shared = Arc::clone(&shared);
            async move {
                let (adapter, validate, execute) = shared.as_ref();
                let (parts, body) = request.into_parts();
                let staged = match adapter.stage(&parts, required) {
                    Ok(staged) => staged,
                    Err(error) => return render_rejection(&error, None).into_response(),
                };
                if !body.is_end_stream() {
                    return render_rejection(&Rejection::UnexpectedBody, None).into_response();
                }
                let input = match validate() {
                    Ok(input) => input,
                    Err(error) => {
                        return render_rejection(&Rejection::InvalidInput(error), None)
                            .into_response();
                    }
                };
                adapter
                    .execute(staged, operation, input, execute, &render_rejection)
                    .await
            }
        })
    }

    async fn execute<V, O, Execute, F, E>(
        &self,
        (context, permit): (RequestContext, UsagePermit),
        operation: O,
        input: Validated<V>,
        execute: &Execute,
        error_handler: &E,
    ) -> Response
    where
        V: Send,
        O: OpIndex,
        Execute: Fn(V, ChargeMetadata) -> F + Sync,
        F: Future<Output = Result<BufferedResponse, ResponseError>> + Send,
        E: Fn(&Rejection, Option<ChargeMetadata>) -> BufferedResponse + Sync,
    {
        let id = match self.config.request_ids.next_id() {
            Ok(id) => id,
            Err(_) => return error_handler(&Rejection::RequestIdUnavailable, None).into_response(),
        };
        let pending = match context.admit(
            &[(operation, input.quantity)],
            permit,
            self.config.clock.now(),
        ) {
            Ok(pending) => pending,
            Err(reason) => return error_handler(&Rejection::Denied(reason), None).into_response(),
        };
        let ready = match pending.acquire_capacity(&self.config.capacity) {
            Ok(ready) => ready,
            Err((reason, _released)) => {
                return error_handler(&Rejection::Denied(reason), None).into_response();
            }
        };
        let committed = match ready.commit(id, self.config.clock.now()) {
            Ok(committed) => committed,
            Err((error, _released)) => {
                return error_handler(&Rejection::Commit(error), None).into_response();
            }
        };
        let charge = ChargeMetadata {
            request_id: committed.request_id(),
            units_charged: committed.units(),
            policy_revision: committed.policy_revision(),
        };
        // Construct the callback future only after commit. Drop owns all exits,
        // including factory panic, async cancellation and serialization errors.
        let response = match execute(input.input, charge).await {
            Ok(response) => response,
            Err(error) => error_handler(&Rejection::Response(error), Some(charge)),
        };
        drop(committed);
        response.into_response()
    }
}
