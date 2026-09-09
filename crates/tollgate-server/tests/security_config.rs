mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use std::sync::Arc;
use tollgate_server::config::SecurityLoader;
use tollgate_server::security::{ControlIdentity, Role, SecurityPolicy, ServerSecurity};
use tollgate_server::transport::{TlsConfig, certificate_fingerprint, is_loopback};
use tollgate_server::{ServerState, router};
use tollgate_store::{Clock, GrantPolicy, MemoryStore, SystemClock};
use tower::ServiceExt;

#[tokio::test]
async fn security_reload_stages_validates_and_only_then_replaces() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    let token_path = directory.path().join("instance.token");
    std::fs::write(&token_path, common::INSTANCE).unwrap();
    let manifest = json!({ "bearers": [{"identity": "instance", "role": "instance", "token_file": "instance.token"}] });
    std::fs::write(&path, manifest.to_string()).unwrap();
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let security = loader.start(loaded).unwrap();
    let app = router(ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(SystemClock),
        security: Arc::clone(&security),
    });
    assert!(
        loader.load(SystemClock.now()).await.unwrap().is_none(),
        "unchanged input does not rotate"
    );
    std::fs::write(&path, "{broken manifest").unwrap();
    assert!(loader.load(SystemClock.now()).await.is_err());
    let call = |token: &str| {
        app.clone().oneshot(
            Request::builder()
                .uri("/v1/snapshots")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
    };
    assert_eq!(
        call(common::INSTANCE).await.unwrap().status(),
        StatusCode::OK
    );
    std::fs::write(&path, manifest.to_string()).unwrap();
    const REPLACEMENT: &str = "replacement-instance-credential-fixture";
    std::fs::write(&token_path, REPLACEMENT).unwrap();
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    // Loading alone does not make a credential authoritative.
    assert_eq!(
        call(REPLACEMENT).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    loader.install(loaded, &security).unwrap();
    assert_eq!(call(REPLACEMENT).await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        call(common::INSTANCE).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    std::fs::write(&token_path, "invalid").unwrap();
    assert!(loader.load(SystemClock.now()).await.is_err());
    assert_eq!(call(REPLACEMENT).await.unwrap().status(), StatusCode::OK);
    std::fs::write(&path, "{}").unwrap();
    let withdrawn = loader.load(SystemClock.now()).await.unwrap().unwrap();
    loader.install(withdrawn, &security).unwrap();
    assert_eq!(
        call(REPLACEMENT).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_failed_install_is_retried_and_cannot_remove_tls() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(directory.path().join("instance.token"), common::INSTANCE).unwrap();
    std::fs::write(&path, json!({ "bearers": [{"identity": "instance", "role": "instance", "token_file": "instance.token"}] }).to_string()).unwrap();
    let certificates = common::certificates();
    let tls = TlsConfig::from_pem(
        certificates.server.as_bytes(),
        certificates.server_key.as_bytes(),
        None,
    )
    .unwrap();
    let security = ServerSecurity::new(common::policy(), Some(tls)).unwrap();
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    assert!(loader.install(loaded, &security).is_err());
    assert!(security.encrypted());
    assert!(loader.load(SystemClock.now()).await.unwrap().is_some());
}

#[tokio::test]
async fn file_boundaries_are_part_of_the_rotation_fingerprint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(directory.path().join("a"), "a".repeat(64)).unwrap();
    std::fs::write(directory.path().join("b"), "b".repeat(64)).unwrap();
    std::fs::write(
        &path,
        json!({ "bearers": [
        {"identity": "a", "role": "instance", "token_file": "a"},
        {"identity": "b", "role": "operator", "token_file": "b"}
    ] })
        .to_string(),
    )
    .unwrap();
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let _security = loader.start(loaded).unwrap();
    // Concatenated bytes remain equal, but both credentials have changed.
    std::fs::write(directory.path().join("a"), "a".repeat(64) + "b").unwrap();
    std::fs::write(directory.path().join("b"), "b".repeat(63)).unwrap();
    assert!(loader.load(SystemClock.now()).await.unwrap().is_some());
}

#[tokio::test]
async fn malformed_or_ambiguous_credentials_cannot_become_authoritative() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(directory.path().join("token"), common::INSTANCE).unwrap();
    for manifest in [
        json!({"unexpected": true}),
        json!({"bearers": [{"identity": "actor", "role": "superuser", "token_file": "token"}]}),
        json!({"bearers": [{"identity": "actor\ninjected", "role": "instance", "token_file": "token"}]}),
        json!({"bearers": [{"identity": "actor", "role": "instance", "token_file": "missing"}]}),
        json!({"bearers": [
            {"identity": "actor", "role": "instance", "token_file": "token"},
            {"identity": "actor", "role": "operator", "token_file": "token"}]}),
        json!({"bearers": [{"identity": "actor", "role": "instance", "token_file": "token", "unexpected": true}]}),
    ] {
        std::fs::write(&path, manifest.to_string()).unwrap();
        assert!(
            SecurityLoader::new(&path)
                .load(SystemClock.now())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn static_credential_files_enforce_both_length_bounds_and_visible_framing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    let token = directory.path().join("instance.token");
    std::fs::write(&path, json!({"bearers": [{"identity": "instance", "role": "instance", "token_file": "instance.token"}]}).to_string()).unwrap();
    for (bytes, valid) in [
        (vec![], false),
        (vec![b'a'; 31], false),
        (vec![b'a'; 32], true),
        (vec![b'a'; 16377], true),
        (vec![b'a'; 16378], false),
        (vec![b' '; 32], false),
        (vec![b'\t'; 32], false),
        (vec![0xff; 32], false),
        ([vec![b'a'; 32], b"\r\n".to_vec()].concat(), true),
    ] {
        std::fs::write(&token, bytes).unwrap();
        assert_eq!(
            SecurityLoader::new(&path)
                .load(SystemClock.now())
                .await
                .is_ok(),
            valid
        );
    }
}

#[test]
fn certificate_configuration_checks_trust_key_pairs_and_handshake_bounds() {
    let a = common::certificates();
    let b = common::certificates();
    for (cert, key, ca) in [
        ("", a.server_key.as_str(), None),
        (a.server.as_str(), "", None),
        (a.server.as_str(), b.server_key.as_str(), None),
        (a.server.as_str(), a.server_key.as_str(), Some("broken CA")),
    ] {
        assert!(
            TlsConfig::from_pem(cert.as_bytes(), key.as_bytes(), ca.map(str::as_bytes)).is_err()
        );
    }
    let identity = ControlIdentity::new("client", Role::Instance).unwrap();
    let fingerprint = certificate_fingerprint(a.client.as_bytes()).unwrap();
    assert_ne!(
        fingerprint,
        certificate_fingerprint(a.other_client.as_bytes()).unwrap()
    );
    assert!(certificate_fingerprint(format!("{}{}", a.client, a.ca).as_bytes()).is_err());
    assert!(
        ServerSecurity::new(
            SecurityPolicy::new()
                .with_certificate(fingerprint, identity.clone())
                .unwrap(),
            None
        )
        .is_err()
    );
    assert!(
        SecurityPolicy::new()
            .with_certificate(fingerprint, identity.clone())
            .unwrap()
            .with_certificate(fingerprint, identity)
            .is_err()
    );
    let tls = TlsConfig::from_pem(a.server.as_bytes(), a.server_key.as_bytes(), None).unwrap();
    let certificate_policy = SecurityPolicy::new()
        .with_certificate(
            fingerprint,
            ControlIdentity::new("client", Role::Instance).unwrap(),
        )
        .unwrap();
    assert!(
        ServerSecurity::new(certificate_policy, Some(tls.clone())).is_err(),
        "a TLS listener without a client CA cannot map certificate identities"
    );
    for duration in [std::time::Duration::ZERO, std::time::Duration::MAX] {
        assert!(
            tls.clone()
                .with_handshake_limits(duration, std::num::NonZeroUsize::new(1).unwrap())
                .is_err()
        );
    }
    for address in ["127.0.0.1", "::1", "::ffff:127.0.0.1"] {
        assert!(is_loopback(address.parse().unwrap()));
    }
    for address in ["0.0.0.0", "::", "::ffff:10.0.0.1", "192.168.1.1"] {
        assert!(!is_loopback(address.parse().unwrap()));
    }
}
