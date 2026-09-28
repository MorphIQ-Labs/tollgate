#![allow(
    dead_code,
    reason = "shared fixture module; each test binary uses a subset"
)]
// Public test fixtures only. These credentials are never deployment defaults.
use std::sync::Arc;
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_client::{BearerToken, HttpStore, HttpStoreConfig, StaticBearer};
use tollgate_server::security::{
    ControlIdentity, ProvisionerLimits, Role, SecurityPolicy, ServerSecurity,
};

pub const INSTANCE: &str = "fixture-instance-credential-98-only";
pub const OPERATOR: &str = "fixture-operator-credential-98-only";
pub const PROVISIONER: &str = "fixture-provisioner-credential-39-only";
/// The fixture provisioner's budget ceiling.
pub const PROVISIONER_MAX_BUDGET: u64 = 1_000;

pub fn policy() -> SecurityPolicy {
    let verifier = Arc::new(HmacRegistry::new(b"fixture-server-secret"));
    verifier.install_credentials([
        INSTANCE.as_bytes(),
        OPERATOR.as_bytes(),
        PROVISIONER.as_bytes(),
    ]);
    let identities = [
        (
            verifier.verify(INSTANCE.as_bytes()).unwrap().principal,
            ControlIdentity::new("test-instance", Role::Instance).unwrap(),
        ),
        (
            verifier.verify(OPERATOR.as_bytes()).unwrap().principal,
            ControlIdentity::new("test-operator", Role::Operator).unwrap(),
        ),
        (
            verifier.verify(PROVISIONER.as_bytes()).unwrap().principal,
            ControlIdentity::provisioner(
                "test-provisioner",
                ProvisionerLimits::new(tollgate_core::CostUnits(PROVISIONER_MAX_BUDGET)),
            )
            .unwrap(),
        ),
    ];
    SecurityPolicy::new()
        .with_bearer(verifier, identities)
        .unwrap()
}

pub fn security() -> Arc<ServerSecurity> {
    ServerSecurity::new(policy(), None).unwrap()
}

pub fn http(base: impl Into<String>) -> Arc<HttpStore> {
    HttpStore::with_config(
        base,
        HttpStoreConfig {
            bearer: Some(StaticBearer::new(BearerToken::new(INSTANCE).unwrap())),
            ..Default::default()
        },
    )
    .unwrap()
}

#[derive(Clone, Debug)]
pub struct CapturedEvent {
    pub target: String,
    pub level: tracing::Level,
    pub fields: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Default)]
pub struct EventCapture(Arc<std::sync::Mutex<Vec<CapturedEvent>>>);

impl EventCapture {
    pub fn events(&self) -> Vec<CapturedEvent> {
        self.0.lock().unwrap().clone()
    }

    /// Capture every event `work` emits, and nothing emitted outside it.
    pub async fn during<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::layer::SubscriberExt;
        arm();
        work.with_subscriber(tracing_subscriber::registry().with(self.clone()))
            .await
    }

    /// Capture this thread's events until the guard drops.
    ///
    /// The sibling of [`during`](Self::during) for a fixture whose emissions
    /// do not all happen inside one future — a task spawned onto the test's
    /// current-thread runtime runs on this thread and so shares this
    /// dispatcher, which a future-scoped subscriber would not reach.
    #[must_use]
    pub fn on_this_thread(&self) -> tracing::subscriber::DefaultGuard {
        use tracing_subscriber::layer::SubscriberExt;
        arm();
        tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
    }
}

/// Install a process-global dispatcher, once per test binary, so that no
/// callsite is ever cached as disabled.
///
/// `tracing` keeps each callsite's `Interest` in a process-global slot and
/// computes it the first time *any* thread reaches that callsite, against
/// that thread's current dispatcher. A thread-local or future-scoped
/// subscriber therefore does not make the decision local: one test reaching a
/// callsite while no dispatcher is installed caches `Interest::never()` for
/// the whole process, and every other test's capture of that callsite
/// silently returns nothing.
///
/// The failure is partial and order-dependent, which is what makes it worth
/// owning here rather than leaving to each test. `api.rs`'s credential-audit
/// witness failed three runs in fifteen locally and once in CI, capturing the
/// two `revoke_key` pairs while both `issue_key` events — same callsite,
/// emitted earlier in the same scope — were dropped, because another test's
/// dispatcher rebuilt the cache partway through. The dangerous direction is
/// the quiet one: `dropping_an_unpolled_reloader_is_an_expected_stop` asserts
/// that *no* event was emitted, and a callsite cached as `never` makes that
/// pass for the wrong reason.
///
/// A global dispatcher makes it unrepresentable: every thread always has a
/// real subscriber, so interest is never `never`, whoever reaches a callsite
/// first. Per-test isolation stays with the scoped subscribers above, which
/// `tracing` consults ahead of this one.
///
/// Arming is part of subscribing rather than a setup call each test makes
/// first, because a convention upheld at every capturing call site is exactly
/// what drifted here: `tests/sweep.rs` and `tollgate-client/tests/events.rs`
/// already carry this lesson in their own `Router`, and five capture sites in
/// four other binaries did not.
fn arm() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        // A binary that installs its own global (see `tests/sweep.rs`) has
        // already satisfied the requirement, so losing this race is success
        // and the error is dropped deliberately rather than ignored.
        drop(tracing::subscriber::set_global_default(
            tracing_subscriber::registry(),
        ));
    });
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        #[derive(Default)]
        struct Fields(std::collections::BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().into(), value.into());
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().unwrap().push(CapturedEvent {
            target: event.metadata().target().into(),
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

pub struct Certificates {
    pub ca: String,
    pub server: String,
    pub server_key: String,
    pub client: String,
    pub client_key: String,
    pub other_client: String,
    pub other_client_key: String,
}

pub fn certificates() -> Certificates {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap().pem();
    let issuer = Issuer::new(params, key);
    let mut params = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let server = params.signed_by(&key, &issuer).unwrap().pem();
    let server_key = key.serialize_pem();
    let mut params = CertificateParams::new(vec!["fixture-instance".into()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().unwrap();
    let client = params.signed_by(&key, &issuer).unwrap().pem();
    let client_key = key.serialize_pem();
    let mut params = CertificateParams::new(vec!["fixture-other-instance".into()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let other_key = KeyPair::generate().unwrap();
    let other_client = params.signed_by(&other_key, &issuer).unwrap().pem();
    Certificates {
        ca,
        server,
        server_key,
        client,
        client_key,
        other_client,
        other_client_key: other_key.serialize_pem(),
    }
}

#[derive(Clone, Copy)]
pub enum TransportMode {
    LoopbackBearer,
    TlsBearer,
    Mtls,
}

pub fn transport(
    mode: TransportMode,
    address: std::net::SocketAddr,
) -> (Arc<ServerSecurity>, Arc<HttpStore>) {
    use tollgate_server::transport::{TlsConfig, certificate_fingerprint};
    if matches!(mode, TransportMode::LoopbackBearer) {
        return (security(), http(format!("http://{address}")));
    }
    let certificates = certificates();
    let tls = TlsConfig::from_pem(
        certificates.server.as_bytes(),
        certificates.server_key.as_bytes(),
        Some(certificates.ca.as_bytes()),
    )
    .unwrap();
    let policy = policy()
        .with_certificate(
            certificate_fingerprint(certificates.client.as_bytes()).unwrap(),
            ControlIdentity::new("test-mtls-instance", Role::Instance).unwrap(),
        )
        .unwrap();
    let security = ServerSecurity::new(policy, Some(tls)).unwrap();
    let mut config = HttpStoreConfig {
        root_ca_pem: Some(certificates.ca.into_bytes()),
        ..Default::default()
    };
    match mode {
        TransportMode::TlsBearer => {
            config.bearer = Some(StaticBearer::new(BearerToken::new(INSTANCE).unwrap()))
        }
        TransportMode::Mtls => {
            config.identity_pem = Some(
                format!("{}{}", certificates.client, certificates.client_key)
                    .into_bytes()
                    .into(),
            )
        }
        TransportMode::LoopbackBearer => unreachable!(),
    }
    (
        security,
        HttpStore::with_config(format!("https://{address}"), config).unwrap(),
    )
}

/// Send a request to an in-process router and decode its JSON reply.
///
/// The `oneshot` / status / collect / `from_slice` tail this replaces is
/// written out thirteen times across this crate's test binaries. An empty body
/// decodes as `Value::Null` rather than a parse error, because several routes
/// answer 204.
pub async fn send(
    router: &axum::Router,
    request: axum::http::Request<axum::body::Body>,
) -> (axum::http::StatusCode, serde_json::Value) {
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}
