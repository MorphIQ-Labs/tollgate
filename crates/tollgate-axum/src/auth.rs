use std::sync::Arc;

use axum::extract::{
    ConnectInfo,
    connect_info::{Connected, MockConnectInfo},
};
use axum::http::{header, request::Parts};
use jiff::Timestamp;
use tollgate_auth::{CredentialVerifier, SessionCredential};
use tollgate_core::{DenyReason, Principal};

use crate::Rejection;

/// Locally verifies HTTP identity before any body is consumed.
///
/// This is a trusted application seam: no I/O, blocking lock, or asynchronous
/// credential lookup belongs here. Return verified identity, not authority;
/// Tollgate checks current account policy for every request.
pub trait RequestAuthenticator: Send + Sync + 'static {
    /// Verify the request at the supplied policy instant.
    fn authenticate(&self, parts: &Parts, now: Timestamp) -> Result<Principal, Rejection>;
}

/// Per-connection credential evidence. Install with Axum's ConnectInfo service.
///
/// A fresh accepted connection gets a fresh cache. Clones share only that
/// connection's evidence; never install one global value for all connections.
#[derive(Clone, Default)]
pub struct TollgateConnection {
    session: SessionCredential,
}

impl Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>> for TollgateConnection {
    fn connect_info(_stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        Self::default()
    }
}

/// Bearer credential verification through the existing session cache.
pub struct BearerAuth<V: ?Sized> {
    verifier: Arc<V>,
}

impl<V: CredentialVerifier + Send + Sync + ?Sized + 'static> BearerAuth<V> {
    /// Use a local verifier such as HmacRegistry or the managed KeyVerifier.
    #[must_use]
    pub fn new(verifier: Arc<V>) -> Self {
        Self { verifier }
    }
}

impl<V: CredentialVerifier + Send + Sync + ?Sized + 'static> RequestAuthenticator
    for BearerAuth<V>
{
    fn authenticate(&self, parts: &Parts, now: Timestamp) -> Result<Principal, Rejection> {
        let connection = parts
            .extensions
            .get::<ConnectInfo<TollgateConnection>>()
            .map(|info| &info.0)
            // Match Axum's ConnectInfo extractor: real connection data wins;
            // MockConnectInfo is the supported in-process test fallback.
            .or_else(|| {
                parts
                    .extensions
                    .get::<MockConnectInfo<TollgateConnection>>()
                    .map(|info| &info.0)
            })
            .ok_or(Rejection::MissingConnection)?;
        let credential = parts
            .headers
            .get(header::AUTHORIZATION)
            .map(axum::http::HeaderValue::as_bytes)
            .and_then(|value| value.strip_prefix(b"Bearer "));
        connection
            .session
            .authenticate(credential, self.verifier.as_ref(), now)
            .ok_or(Rejection::Denied(DenyReason::UnknownPrincipal))
    }
}
