//! Control-plane identities and route authorization. No application policy lives here.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::extract::{FromRequestParts, Request, State};
use axum::http::{header, request::Parts};
use axum::middleware::Next;
use axum::response::Response;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tollgate_auth::CredentialVerifier;
use tollgate_core::Principal;
use tollgate_store::Clock;

use crate::error::ApiError;
use crate::transport::{PeerIdentity, TlsConfig};

/// Roles are disjoint. An operator credential cannot fund an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Instance,
    Operator,
}

/// A stable, non-secret audit identity chosen by the deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlIdentity {
    name: String,
    role: Role,
}

impl ControlIdentity {
    pub fn new(name: impl Into<String>, role: Role) -> Result<Self, SecurityError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"@._:/-".contains(&b))
        {
            return Err(SecurityError(
                "identity must be 1..=128 ASCII identifier characters",
            ));
        }
        Ok(Self { name, role })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn role(&self) -> Role {
        self.role
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityError(pub &'static str);

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for SecurityError {}

struct BearerScheme {
    verifier: Arc<dyn CredentialVerifier + Send + Sync>,
    identities: HashMap<Principal, ControlIdentity>,
}

/// An immutable, validated mapping. Publish verification and authorization together;
/// rotating one without the other would temporarily assign old credentials new roles.
#[derive(Default)]
pub struct SecurityPolicy {
    bearers: Vec<BearerScheme>,
    certificates: HashMap<[u8; 32], ControlIdentity>,
}

impl SecurityPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_bearer(
        mut self,
        verifier: Arc<dyn CredentialVerifier + Send + Sync>,
        identities: impl IntoIterator<Item = (Principal, ControlIdentity)>,
    ) -> Result<Self, SecurityError> {
        let mut mapped = HashMap::new();
        for (principal, identity) in identities {
            if mapped.insert(principal, identity).is_some() {
                return Err(SecurityError("duplicate bearer principal"));
            }
        }
        self.bearers.push(BearerScheme {
            verifier,
            identities: mapped,
        });
        Ok(self)
    }

    /// The SHA-256 fingerprint of the leaf DER certificate, verified by TLS.
    pub fn with_certificate(
        mut self,
        fingerprint: [u8; 32],
        identity: ControlIdentity,
    ) -> Result<Self, SecurityError> {
        if self.certificates.insert(fingerprint, identity).is_some() {
            return Err(SecurityError("duplicate client certificate"));
        }
        Ok(self)
    }

    fn bearer(&self, credential: &[u8], now: Timestamp) -> Result<ControlIdentity, ApiError> {
        let mut identity = None;
        for scheme in &self.bearers {
            if let Some(proof) = scheme.verifier.verify(credential) {
                if !proof.is_reusable_at(now) {
                    return Err(ApiError::unauthorized());
                }
                let found = scheme
                    .identities
                    .get(&proof.principal)
                    .ok_or_else(ApiError::forbidden)?;
                if identity.as_ref().is_some_and(|previous| previous != found) {
                    return Err(ApiError::unauthorized());
                }
                identity = Some(found.clone());
            }
        }
        identity.ok_or_else(ApiError::unauthorized)
    }
}

pub(crate) struct SecurityBundle {
    pub policy: SecurityPolicy,
    pub tls: Option<TlsConfig>,
}

/// Shared by the listener and router. A request pins one complete generation.
pub struct ServerSecurity {
    pub(crate) current: ArcSwap<SecurityBundle>,
    encrypted: bool,
}

impl ServerSecurity {
    pub fn new(policy: SecurityPolicy, tls: Option<TlsConfig>) -> Result<Arc<Self>, SecurityError> {
        validate(&policy, tls.as_ref())?;
        Ok(Arc::new(Self {
            encrypted: tls.is_some(),
            current: ArcSwap::from_pointee(SecurityBundle { policy, tls }),
        }))
    }

    /// Existing connections consult the new role map on their next request.
    /// Changing transport mode requires restarting the listener; TLS cannot be
    /// removed by a credential reload on an exposed listener.
    pub fn replace(
        &self,
        policy: SecurityPolicy,
        tls: Option<TlsConfig>,
    ) -> Result<(), SecurityError> {
        validate(&policy, tls.as_ref())?;
        if self.encrypted != tls.is_some() {
            return Err(SecurityError(
                "changing TLS mode requires a listener restart",
            ));
        }
        self.current.store(Arc::new(SecurityBundle { policy, tls }));
        Ok(())
    }

    pub fn encrypted(&self) -> bool {
        self.encrypted
    }
}

fn validate(policy: &SecurityPolicy, tls: Option<&TlsConfig>) -> Result<(), SecurityError> {
    if !policy.certificates.is_empty() && tls.is_none_or(|tls| !tls.verifies_clients()) {
        return Err(SecurityError(
            "certificate identities require TLS with a client CA",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Authorization {
    pub security: Arc<ServerSecurity>,
    pub clock: Arc<dyn Clock>,
    pub role: Role,
}

/// Transport framing is bounded and singular. Never log a rejected header.
fn bearer(request: &Request) -> Result<Option<&[u8]>, ApiError> {
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let Some(header) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::unauthorized());
    }
    let header = header.as_bytes();
    // Google ID tokens and 256-bit static credentials fit comfortably. This is
    // the supported token envelope, enforced before signature work.
    if header.len() > 16 * 1024 {
        return Err(ApiError::unauthorized());
    }
    let Some(separator) = header.iter().position(|b| *b == b' ') else {
        return Err(ApiError::unauthorized());
    };
    let (scheme, token) = (&header[..separator], &header[separator + 1..]);
    if !scheme.eq_ignore_ascii_case(b"Bearer")
        || token.is_empty()
        || !token.iter().all(|b| b.is_ascii_graphic())
    {
        return Err(ApiError::unauthorized());
    }
    Ok(Some(token))
}

pub(crate) async fn authorize(
    State(auth): State<Authorization>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let bundle = auth.security.current.load_full();
    let now = auth.clock.now();
    let bearer = bearer(&request)?;
    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<PeerIdentity>>();
    let certificate = match peer.and_then(|peer| peer.0.certificates.as_deref()) {
        Some(chain) => {
            let tls = bundle.tls.as_ref().ok_or_else(ApiError::unauthorized)?;
            // TLS proved key possession. Recheck current trust and validity so
            // removing a CA or expiring a certificate affects keep-alive too.
            tls.verify(chain, now)
                .map_err(|_| ApiError::unauthorized())?;
            let fingerprint = crate::transport::fingerprint(&chain[0]);
            Some(
                bundle
                    .policy
                    .certificates
                    .get(&fingerprint)
                    .ok_or_else(ApiError::forbidden)?
                    .clone(),
            )
        }
        None => None,
    };
    let token = bearer
        .map(|token| bundle.policy.bearer(token, now))
        .transpose()?;
    let identity = match (token, certificate) {
        (Some(a), Some(b)) if a != b => return Err(ApiError::unauthorized()),
        (Some(identity), _) | (_, Some(identity)) => identity,
        (None, None) => return Err(ApiError::unauthorized()),
    };
    if identity.role != auth.role {
        return Err(ApiError::forbidden());
    }
    tracing::debug!(actor = identity.name(), role = ?identity.role(), "control-plane request authenticated");
    match identity.role {
        Role::Instance => {
            request.extensions_mut().insert(InstanceIdentity);
        }
        Role::Operator => {
            request.extensions_mut().insert(OperatorIdentity(identity));
        }
    }
    Ok(next.run(request).await)
}

// Requiring the proof in handler arguments also fails closed if someone wires
// a handler without its router layer. Constructors stay private to this module.
#[derive(Clone)]
pub(crate) struct InstanceIdentity;
#[derive(Clone)]
pub(crate) struct OperatorIdentity(pub ControlIdentity);

macro_rules! identity_extractor {
    ($name:ident) => {
        impl<S: Send + Sync> FromRequestParts<S> for $name {
            type Rejection = ApiError;
            async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, ApiError> {
                parts
                    .extensions
                    .get::<Self>()
                    .cloned()
                    .ok_or_else(ApiError::unauthorized)
            }
        }
    };
}
identity_extractor!(InstanceIdentity);
identity_extractor!(OperatorIdentity);

#[cfg(test)]
mod framing_tests {
    use super::*;

    #[test]
    fn bearer_framing_checks_each_condition_before_scheme_verification() {
        let request = |value: &[u8]| {
            Request::builder()
                .header(
                    header::AUTHORIZATION,
                    axum::http::HeaderValue::from_bytes(value).unwrap(),
                )
                .body(axum::body::Body::empty())
                .unwrap()
        };
        for value in [
            b"Basic token".as_slice(),
            b"Bearer ",
            b"Bearer one\ttwo",
            b"Bearer  token",
            b"Bearer",
            b"Bearer \x80",
        ] {
            assert!(bearer(&request(value)).is_err());
        }
        for size in [1, 16 * 1024 - 7] {
            let token = vec![b'x'; size];
            let mut header = b"bEaReR ".to_vec();
            header.extend_from_slice(&token);
            let request = request(&header);
            assert_eq!(bearer(&request).unwrap(), Some(token.as_slice()));
        }
        for size in [16 * 1024 + 1, 32 * 1024] {
            let mut value = b"Bearer ".to_vec();
            value.resize(size, b'x');
            assert!(bearer(&request(&value)).is_err());
        }
        let mut duplicate = request(b"Bearer token");
        duplicate
            .headers_mut()
            .append(header::AUTHORIZATION, "Bearer token".parse().unwrap());
        assert!(bearer(&duplicate).is_err());
        assert_eq!(
            bearer(&Request::new(axum::body::Body::empty())).unwrap(),
            None
        );
    }

    #[test]
    fn overlapping_bearer_schemes_must_agree_on_the_verified_identity() {
        use tollgate_auth::{HmacRegistry, Verified};
        let verifier = Arc::new(HmacRegistry::new(b"fixture-scheme-agreement-secret"));
        let principal = verifier.install_credentials([b"shared-credential".as_slice()])[0];
        let now = Timestamp::from_second(100).unwrap();
        let instance = ControlIdentity::new("instance", Role::Instance).unwrap();
        let operator = ControlIdentity::new("operator", Role::Operator).unwrap();
        for (second, agrees) in [(instance.clone(), true), (operator, false)] {
            let policy = SecurityPolicy::new()
                .with_bearer(verifier.clone(), [(principal, instance.clone())])
                .unwrap()
                .with_bearer(verifier.clone(), [(principal, second)])
                .unwrap();
            assert_eq!(policy.bearer(b"shared-credential", now).is_ok(), agrees);
            if agrees {
                assert_eq!(policy.bearer(b"shared-credential", now).unwrap(), instance);
            }
        }
        struct Expired(Principal);
        impl CredentialVerifier for Expired {
            fn verify(&self, _: &[u8]) -> Option<Verified> {
                Some(Verified::until(
                    self.0,
                    Timestamp::from_second(100).unwrap(),
                ))
            }
        }
        let policy = SecurityPolicy::new()
            .with_bearer(Arc::new(Expired(principal)), [(principal, instance)])
            .unwrap();
        assert!(policy.bearer(b"shared-credential", now).is_err());
    }
}

impl OperatorIdentity {
    /// The receipt is the backend's proof. Errors and cancellation never
    /// manufacture a before/after pair or claim that an ambiguous write rolled back.
    pub(crate) async fn run<T, E: Into<ApiError>>(
        &self,
        action: &'static str,
        target: impl std::fmt::Display,
        clock: &dyn Clock,
        operation: impl Future<Output = Result<tollgate_store::AdminReceipt<T>, E>>,
    ) -> Result<T, ApiError> {
        struct Attempt<'a> {
            id: tollgate_core::RequestId,
            identity: &'a ControlIdentity,
            action: &'static str,
            target: String,
            clock: &'a dyn Clock,
            finished: bool,
        }
        impl Drop for Attempt<'_> {
            fn drop(&mut self) {
                if !self.finished {
                    tracing::warn!(target: "tollgate::audit", actor = self.identity.name(), action = self.action,
                        operation_id = %self.id,
                        resource = self.target, at = %self.clock.now(), outcome = "cancelled_unknown",
                        "administrative operation abandoned; commit outcome may be unknown");
                }
            }
        }
        let mut identifier = [0u8; 16];
        getrandom::fill(&mut identifier).map_err(|_| {
            ApiError::from(tollgate_store::StoreError(
                "audit identity entropy unavailable".into(),
            ))
        })?;
        let mut attempt = Attempt {
            id: tollgate_core::RequestId(u128::from_be_bytes(identifier)),
            identity: &self.0,
            action,
            target: target.to_string(),
            clock,
            finished: false,
        };
        tracing::info!(target: "tollgate::audit", actor = self.0.name(), action,
            operation_id = %attempt.id,
            resource = attempt.target, at = %clock.now(), outcome = "started", "administrative operation started");
        let result = operation.await;
        attempt.finished = true;
        match result {
            Ok(receipt) => {
                tracing::info!(target: "tollgate::audit", actor = self.0.name(), action,
                    operation_id = %attempt.id,
                    resource = attempt.target, at = %clock.now(), outcome = "confirmed",
                    before = ?receipt.before, after = ?receipt.after, "administrative operation completed");
                Ok(receipt.outcome)
            }
            Err(error) => {
                let error = error.into();
                tracing::warn!(target: "tollgate::audit", actor = self.0.name(), action,
                    operation_id = %attempt.id,
                    resource = attempt.target, at = %clock.now(), outcome = "failed",
                    code = error.code, status = error.status.as_u16(), "administrative operation failed; storage errors may conceal a commit");
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_admin_operations_report_an_unknown_commit_without_a_receipt() {
        use tracing::instrument::WithSubscriber;
        #[derive(Clone, Default)]
        struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let capture = Capture::default();
        let output = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || output.clone())
            .finish();
        let identity =
            OperatorIdentity(ControlIdentity::new("fixture-operator", Role::Operator).unwrap());
        async {
            let operation = identity.run(
                "deposit",
                "fixture-account",
                &tollgate_store::SystemClock,
                std::future::pending::<
                    Result<tollgate_store::AdminReceipt<()>, tollgate_store::StoreError>,
                >(),
            );
            tokio::pin!(operation);
            tokio::select! {
                biased;
                _ = &mut operation => panic!("the backend remains pending"),
                _ = tokio::task::yield_now() => {},
            }
            // Dropping the pending operation must report an ambiguous outcome.
        }
        .with_subscriber(subscriber)
        .await;
        let bytes = capture.0.lock().unwrap();
        let log = std::str::from_utf8(&bytes).unwrap();
        assert!(
            log.contains("started") && log.contains("cancelled_unknown"),
            "{log}"
        );
        assert!(
            log.contains("fixture-operator")
                && log.contains("fixture-account")
                && log.contains("operation_id=")
        );
        assert!(!log.contains("confirmed") && !log.contains("before=") && !log.contains("after="));
    }
}
