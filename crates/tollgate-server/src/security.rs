//! Control-plane identities and route authorization. No application policy lives here.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::extract::{FromRequestParts, MatchedPath, OriginalUri, Request, State};
use axum::http::{header, request::Parts};
use axum::middleware::Next;
use axum::response::Response;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tollgate_auth::CredentialVerifier;
use tollgate_core::{CostUnits, Principal};
use tollgate_store::Clock;

use crate::error::ApiError;
use crate::transport::{PeerIdentity, TlsConfig};

/// Roles are disjoint. An operator credential cannot fund an instance, and a
/// provisioner reaches only the self-service subset of the admin API (#39).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// A service instance: lease lifecycle, snapshot and principal reads,
    /// the active-credential projection (`GET /v1/keys`) and usage ingestion.
    Instance,
    /// An operator: every route under `/v1/admin`, including account
    /// funding, status, budgets, credentials and snapshot publication.
    Operator,
    /// A self-service account service (#39): creates unfunded, suspended,
    /// best-effort accounts, activates them, sets budgets within its
    /// ceiling, and manages their credentials, on accounts a provisioner
    /// created only. It can never fund, suspend, close, grant `Assured`, or
    /// publish principal snapshots.
    Provisioner,
}

impl Role {
    /// The configuration spelling, which is also the audit spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Instance => "instance",
            Role::Operator => "operator",
            Role::Provisioner => "provisioner",
        }
    }
}

/// What a provisioner identity may grant beyond its fixed route and argument
/// scope (#39).
///
/// Required rather than optional: a budget allowance funds admission, so an
/// unbounded one would be a deposit by another name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvisionerLimits {
    max_budget_allowance: CostUnits,
}

impl ProvisionerLimits {
    /// `max_budget_allowance` is the largest periodic allowance this
    /// identity may set on an account, inclusive.
    pub fn new(max_budget_allowance: CostUnits) -> Self {
        Self {
            max_budget_allowance,
        }
    }

    /// The largest periodic allowance this identity may set, inclusive.
    pub fn max_budget_allowance(&self) -> CostUnits {
        self.max_budget_allowance
    }
}

/// A role with whatever it carries. Limits exist exactly for a provisioner,
/// so an operator with a ceiling or a provisioner without one is
/// unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grant {
    Instance,
    Operator,
    Provisioner(ProvisionerLimits),
}

/// A stable, non-secret audit identity chosen by the deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlIdentity {
    name: String,
    grant: Grant,
}

impl ControlIdentity {
    /// An instance or operator identity named `name`. A provisioner needs its
    /// limits, so it is built with [`ControlIdentity::provisioner`].
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] for [`Role::Provisioner`], or unless
    /// `name` is 1 to 128 ASCII characters, each alphanumeric or one of
    /// `@ . _ : / -`.
    pub fn new(name: impl Into<String>, role: Role) -> Result<Self, SecurityError> {
        let grant = match role {
            Role::Instance => Grant::Instance,
            Role::Operator => Grant::Operator,
            Role::Provisioner => {
                return Err(SecurityError(
                    "a provisioner identity requires max_budget_allowance",
                ));
            }
        };
        Self::with_grant(name.into(), grant)
    }

    /// A provisioner identity named `name`, bounded by `limits` (#39).
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] for a `name` [`ControlIdentity::new`]
    /// would refuse.
    pub fn provisioner(
        name: impl Into<String>,
        limits: ProvisionerLimits,
    ) -> Result<Self, SecurityError> {
        Self::with_grant(name.into(), Grant::Provisioner(limits))
    }

    fn with_grant(name: String, grant: Grant) -> Result<Self, SecurityError> {
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
        Ok(Self { name, grant })
    }

    /// The audit name, recorded as the actor on authentication and
    /// administrative audit events.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// The one role this identity holds.
    pub fn role(&self) -> Role {
        match self.grant {
            Grant::Instance => Role::Instance,
            Grant::Operator => Role::Operator,
            Grant::Provisioner(_) => Role::Provisioner,
        }
    }
    /// The provisioner's limits; `None` for every other role.
    pub fn provisioner_limits(&self) -> Option<ProvisionerLimits> {
        match self.grant {
            Grant::Provisioner(limits) => Some(limits),
            Grant::Instance | Grant::Operator => None,
        }
    }
}

/// A refused security configuration, credential file, TLS material or key
/// set.
///
/// The message is a static description of which check failed. It never
/// contains credential, key or token material, so it is safe to log.
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
    /// A policy with no credentials. It denies every protected operation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a bearer scheme: `verifier` authenticates the presented token,
    /// and `identities` maps each principal it verifies as to an identity.
    ///
    /// A request's bearer token is offered to every scheme. It is refused
    /// with `401` if no scheme verifies it, if a verifying scheme's evidence
    /// is no longer reusable at the server's current time, or if two schemes
    /// resolve it to different identities. A verified principal with no
    /// mapping in its scheme is refused with `403` (INVARIANTS.md 32).
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] if `identities` names a principal twice.
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
    /// Publishes the first generation and fixes the transport mode: TLS if
    /// `tls` is present, plaintext otherwise. [`replace`](Self::replace)
    /// cannot change that mode later.
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] if `policy` maps client certificates but
    /// `tls` is absent or has no client CA.
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

    /// Whether this server was constructed with TLS. Fixed for its lifetime;
    /// [`serve`](crate::serve) refuses a non-loopback listener when it is
    /// `false`.
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
    /// The roles this router admits. Each route sits under exactly one
    /// router, so each route admits exactly one fixed set.
    pub roles: &'static [Role],
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
    if !auth.roles.contains(&identity.role()) {
        // Audited, because a refused role is the signature of a credential
        // used outside its purpose: a provisioner reaching for a deposit is
        // what a compromised signup service looks like (#39). The route
        // template names the action and the path is the resource; the
        // credential itself is never logged.
        let action = request
            .extensions()
            .get::<MatchedPath>()
            .map_or("unmatched", MatchedPath::as_str);
        tracing::warn!(target: "tollgate::audit", actor = identity.name(),
            role = identity.role().as_str(), action = %format_args!("{} {action}", request.method()),
            resource = request.extensions().get::<OriginalUri>().map_or_else(|| request.uri().path(), |uri| uri.path()),
            at = %now, outcome = "refused",
            code = "scope-forbidden", "control-plane request refused for its role");
        return Err(ApiError::forbidden());
    }
    tracing::debug!(
        actor = identity.name(),
        role = identity.role().as_str(),
        "control-plane request authenticated"
    );
    match identity.grant {
        Grant::Instance => {
            request.extensions_mut().insert(InstanceIdentity);
        }
        Grant::Operator => {
            request
                .extensions_mut()
                .insert(AdminIdentity::Operator(identity.clone()));
            request.extensions_mut().insert(OperatorIdentity(identity));
        }
        Grant::Provisioner(limits) => {
            request
                .extensions_mut()
                .insert(AdminIdentity::Provisioner(identity, limits));
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
/// Evidence for a route operators and provisioners share. The router decided
/// only that this identity may ask; the handler decides what a provisioner
/// may send (#39).
#[derive(Clone)]
pub(crate) enum AdminIdentity {
    Operator(ControlIdentity),
    Provisioner(ControlIdentity, ProvisionerLimits),
}

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
identity_extractor!(AdminIdentity);

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
    /// See [`audited`].
    pub(crate) async fn run<T, E: Into<ApiError>>(
        &self,
        action: &'static str,
        target: impl std::fmt::Display,
        clock: &dyn Clock,
        operation: impl Future<Output = Result<tollgate_store::AdminReceipt<T>, E>>,
    ) -> Result<T, ApiError> {
        audited(&self.0, action, target, clock, operation).await
    }
}

impl AdminIdentity {
    pub(crate) fn identity(&self) -> &ControlIdentity {
        match self {
            AdminIdentity::Operator(identity) | AdminIdentity::Provisioner(identity, _) => identity,
        }
    }

    /// See [`audited`].
    pub(crate) async fn run<T, E: Into<ApiError>>(
        &self,
        action: &'static str,
        target: impl std::fmt::Display,
        clock: &dyn Clock,
        operation: impl Future<Output = Result<tollgate_store::AdminReceipt<T>, E>>,
    ) -> Result<T, ApiError> {
        audited(self.identity(), action, target, clock, operation).await
    }

    /// Refuse a request outside this identity's scope before any store call,
    /// and audit the refusal: an attempt a role may not make is evidence,
    /// not noise (#39).
    pub(crate) fn refuse(
        &self,
        action: &'static str,
        target: impl std::fmt::Display,
        clock: &dyn Clock,
        code: &'static str,
        title: &'static str,
    ) -> ApiError {
        let identity = self.identity();
        tracing::warn!(target: "tollgate::audit", actor = identity.name(),
            role = identity.role().as_str(), action, resource = %target, at = %clock.now(),
            outcome = "refused", code, reason = title, "administrative operation refused for its scope");
        ApiError::refused_scope(code, title)
    }

    /// A provisioner may act only on an account a provisioner created (#39);
    /// an operator passes unconditionally.
    ///
    /// A read separate from the operation it guards is sound here, where it
    /// would not be for status: `origin` is written once at creation and
    /// never changes, so no write can slip between this check and the
    /// operation. The mutable fact, who set the status, is checked inside
    /// the store's own transaction instead.
    pub(crate) async fn check_account<S: tollgate_store::AdminStore + ?Sized>(
        &self,
        store: &S,
        account: tollgate_core::AccountId,
        action: &'static str,
        target: impl std::fmt::Display,
        clock: &dyn Clock,
    ) -> Result<(), ApiError> {
        if let AdminIdentity::Operator(_) = self {
            return Ok(());
        }
        match store.account_view(account).await? {
            None => Err(tollgate_store::SetStatusError::UnknownAccount.into()),
            Some(view) if view.origin == tollgate_store::AdminAuthority::Provisioner => Ok(()),
            Some(_) => Err(self.refuse(
                action,
                target,
                clock,
                "account-not-provisioned",
                "account was not created by a provisioner",
            )),
        }
    }
}

/// The receipt is the backend's proof. Errors and cancellation never
/// manufacture a before/after pair or claim that an ambiguous write rolled back.
async fn audited<T, E: Into<ApiError>>(
    identity: &ControlIdentity,
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
                tracing::warn!(target: "tollgate::audit", actor = self.identity.name(),
                        role = self.identity.role().as_str(), action = self.action,
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
        identity,
        action,
        target: target.to_string(),
        clock,
        finished: false,
    };
    tracing::info!(target: "tollgate::audit", actor = identity.name(),
            role = identity.role().as_str(), action,
            operation_id = %attempt.id,
            resource = attempt.target, at = %clock.now(), outcome = "started", "administrative operation started");
    let result = operation.await;
    attempt.finished = true;
    match result {
        Ok(receipt) => {
            tracing::info!(target: "tollgate::audit", actor = identity.name(),
            role = identity.role().as_str(), action,
                    operation_id = %attempt.id,
                    resource = attempt.target, at = %clock.now(), outcome = "confirmed",
                    before = ?receipt.before, after = ?receipt.after, "administrative operation completed");
            Ok(receipt.outcome)
        }
        Err(error) => {
            let error = error.into();
            tracing::warn!(target: "tollgate::audit", actor = identity.name(),
            role = identity.role().as_str(), action,
                    operation_id = %attempt.id,
                    resource = attempt.target, at = %clock.now(), outcome = "failed",
                    code = error.code, status = error.status.as_u16(), "administrative operation failed; storage errors may conceal a commit");
            Err(error)
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
