#![allow(dead_code)]
// Public test fixtures only. These credentials are never deployment defaults.
use std::sync::Arc;
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_client::{BearerToken, HttpStore, HttpStoreConfig, StaticBearer};
use tollgate_server::security::{ControlIdentity, Role, SecurityPolicy, ServerSecurity};

pub const INSTANCE: &str = "fixture-instance-credential-98-only";
pub const OPERATOR: &str = "fixture-operator-credential-98-only";

pub fn policy() -> SecurityPolicy {
    let verifier = Arc::new(HmacRegistry::new(b"fixture-server-secret"));
    verifier.install_credentials([INSTANCE.as_bytes(), OPERATOR.as_bytes()]);
    let identities = [
        (
            verifier.verify(INSTANCE.as_bytes()).unwrap().principal,
            ControlIdentity::new("test-instance", Role::Instance).unwrap(),
        ),
        (
            verifier.verify(OPERATOR.as_bytes()).unwrap().principal,
            ControlIdentity::new("test-operator", Role::Operator).unwrap(),
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
