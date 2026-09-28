mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use std::sync::Arc;
use tollgate_auth::{CredentialVerifier, HmacRegistry};
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
        issuer: None,
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

/// A provisioner entry needs its budget ceiling, and no other role may carry
/// one, whichever credential source names it (#39). Google subjects are
/// covered beside the loader's key fetch in `config.rs`.
#[tokio::test]
async fn a_provisioner_entry_requires_its_ceiling_and_no_other_role_takes_one() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(directory.path().join("token"), common::PROVISIONER).unwrap();
    let certificates = common::certificates();
    std::fs::write(directory.path().join("client.pem"), &certificates.client).unwrap();
    for (entry, valid) in [
        (
            json!({"bearers": [{"identity": "signup", "role": "provisioner", "token_file": "token", "max_budget_allowance": 1000}]}),
            true,
        ),
        (
            json!({"bearers": [{"identity": "signup", "role": "provisioner", "token_file": "token"}]}),
            false,
        ),
        (
            json!({"bearers": [{"identity": "ops", "role": "operator", "token_file": "token", "max_budget_allowance": 1000}]}),
            false,
        ),
        (
            json!({"bearers": [{"identity": "svc", "role": "instance", "token_file": "token", "max_budget_allowance": 1000}]}),
            false,
        ),
        (
            json!({"certificates": [{"identity": "signup", "role": "provisioner", "certificate": "client.pem", "max_budget_allowance": 1000}]}),
            true,
        ),
        (
            json!({"certificates": [{"identity": "signup", "role": "provisioner", "certificate": "client.pem"}]}),
            false,
        ),
        (
            json!({"certificates": [{"identity": "ops", "role": "operator", "certificate": "client.pem", "max_budget_allowance": 1000}]}),
            false,
        ),
    ] {
        std::fs::write(&path, entry.to_string()).unwrap();
        assert_eq!(
            SecurityLoader::new(&path)
                .load(SystemClock.now())
                .await
                .is_ok(),
            valid,
            "{entry}"
        );
    }

    // A loaded provisioner bearer reaches the shared admin routes and no other.
    std::fs::write(
        &path,
        json!({"bearers": [{"identity": "signup", "role": "provisioner", "token_file": "token", "max_budget_allowance": 1000}]})
            .to_string(),
    )
    .unwrap();
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let app = router(ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(SystemClock),
        security: loader.start(loaded).unwrap(),
        issuer: None,
    });
    for (path, expected) in [
        ("/v1/admin/accounts", StatusCode::CREATED),
        (
            "/v1/admin/accounts/00000000000000000000000000000001/deposit",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {}", common::PROVISIONER))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"account_id": "00000000000000000000000000000001", "initial_balance": 0, "status": "Suspended", "units": 1})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{path}");
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

const ISSUER_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// A manifest with one instance bearer and, optionally, an issuer file.
fn write_issuer_manifest(directory: &std::path::Path, issuer: Option<&[u8]>) -> std::path::PathBuf {
    let path = directory.join("security.json");
    std::fs::write(directory.join("instance.token"), common::INSTANCE).unwrap();
    let mut manifest = json!({ "bearers": [{"identity": "instance", "role": "instance", "token_file": "instance.token"}] });
    if let Some(secret) = issuer {
        std::fs::write(directory.join("issuer.secret"), secret).unwrap();
        manifest["issuer"] = json!({"secret_file": "issuer.secret"});
    }
    std::fs::write(&path, manifest.to_string()).unwrap();
    path
}

/// The format is exact so that one stored value keys issuer and verifiers
/// identically: an uppercase spelling would be a different HMAC key.
#[tokio::test]
async fn issuer_secret_files_must_be_exactly_64_lowercase_hex_characters() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(directory.path(), Some(ISSUER_SECRET.as_bytes()));
    let secret = directory.path().join("issuer.secret");
    for (bytes, valid) in [
        (ISSUER_SECRET.as_bytes().to_vec(), true),
        (format!("{ISSUER_SECRET}\n").into_bytes(), true),
        (format!("{ISSUER_SECRET}\r\n").into_bytes(), true),
        (format!("{ISSUER_SECRET}\n\n").into_bytes(), false),
        (vec![], false),
        (ISSUER_SECRET.as_bytes()[..63].to_vec(), false),
        (format!("{ISSUER_SECRET}0").into_bytes(), false),
        (ISSUER_SECRET.to_uppercase().into_bytes(), false),
        (format!("{}g", &ISSUER_SECRET[..63]).into_bytes(), false),
        (format!(" {}", &ISSUER_SECRET[..63]).into_bytes(), false),
    ] {
        std::fs::write(&secret, &bytes).unwrap();
        assert_eq!(
            SecurityLoader::new(&path)
                .load(SystemClock.now())
                .await
                .is_ok(),
            valid,
            "{:?}",
            String::from_utf8_lossy(&bytes)
        );
    }
    std::fs::remove_file(&secret).unwrap();
    assert!(
        SecurityLoader::new(&path)
            .load(SystemClock.now())
            .await
            .is_err(),
        "a named issuer file that is missing refuses the configuration"
    );
}

/// Operator and issuer authority cannot collapse into one secret.
#[tokio::test]
async fn an_issuer_secret_equal_to_a_bearer_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(directory.path(), Some(ISSUER_SECRET.as_bytes()));
    std::fs::write(
        directory.path().join("instance.token"),
        format!("{ISSUER_SECRET}\n"),
    )
    .unwrap();
    let Err(error) = SecurityLoader::new(&path).load(SystemClock.now()).await else {
        panic!("an issuer secret shared with a bearer must be refused");
    };
    assert_eq!(
        error.to_string(),
        "issuer secret must differ from every bearer credential"
    );
}

/// The HMAC key is the file's 64 characters as bytes — what every verifier
/// handed the same value as text already uses — not their decoded value.
#[tokio::test]
async fn the_manifest_issuer_keys_on_the_file_text() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(
        directory.path(),
        Some(format!("{ISSUER_SECRET}\n").as_bytes()),
    );
    let mut loader = SecurityLoader::new(&path);
    assert!(loader.issuer().is_none(), "no issuer before start");
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let _security = loader.start(loaded).unwrap();
    let minted = loader
        .issuer()
        .expect("the manifest configures an issuer")
        .mint(tollgate_core::KeyId(7))
        .unwrap();
    let verifier = |key: &[u8]| {
        let registry = HmacRegistry::new(key);
        registry.install([(minted.principal, minted.digest, None)]);
        registry
            .verify(&minted.secret)
            .map(|verified| verified.principal)
    };
    assert_eq!(verifier(ISSUER_SECRET.as_bytes()), Some(minted.principal));
    let decoded: Vec<u8> = (0..32)
        .map(|i| u8::from_str_radix(&ISSUER_SECRET[2 * i..2 * i + 2], 16).unwrap())
        .collect();
    assert_eq!(
        verifier(&decoded),
        None,
        "the decoded bytes are not the key"
    );
}

#[tokio::test]
async fn a_manifest_without_an_issuer_configures_none() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(directory.path(), None);
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let _security = loader.start(loaded).unwrap();
    assert!(loader.issuer().is_none());
}

/// A stray edit or half-finished rotation of the issuer file cannot freeze
/// certificate or bearer rotation: the rest of the manifest installs, the
/// live issuer keeps minting, and the pending change is reported once.
#[tokio::test]
async fn an_issuer_change_is_deferred_without_blocking_other_rotation() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(directory.path(), Some(ISSUER_SECRET.as_bytes()));
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let security = loader.start(loaded).unwrap();
    let live = loader.issuer().unwrap();
    let app = router(ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(SystemClock),
        security: Arc::clone(&security),
        issuer: None,
    });
    let call = |token: &str| {
        app.clone().oneshot(
            Request::builder()
                .uri("/v1/snapshots")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
    };

    const REPLACEMENT: &str = "replacement-instance-credential-fixture";
    const OTHER: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    let capture = common::EventCapture::default();
    let _guard = capture.on_this_thread();
    std::fs::write(directory.path().join("instance.token"), REPLACEMENT).unwrap();
    std::fs::write(directory.path().join("issuer.secret"), OTHER).unwrap();
    let staged = loader.load(SystemClock.now()).await.unwrap().unwrap();
    loader.install(staged, &security).unwrap();
    assert_eq!(call(REPLACEMENT).await.unwrap().status(), StatusCode::OK);
    assert!(loader.issuer_change_pending());
    assert!(
        Arc::ptr_eq(&live, &loader.issuer().unwrap()),
        "only a restart replaces the issuer"
    );
    assert!(
        loader.load(SystemClock.now()).await.unwrap().is_none(),
        "the installed manifest is not retried every five seconds"
    );

    // Withdrawing the issuer entirely is a change too.
    let manifest = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, json!({ "bearers": [{"identity": "instance", "role": "instance", "token_file": "instance.token"}] }).to_string()).unwrap();
    let staged = loader.load(SystemClock.now()).await.unwrap().unwrap();
    loader.install(staged, &security).unwrap();
    assert!(loader.issuer_change_pending());
    assert!(loader.issuer().is_some());

    // Restoring the live secret clears the pending state.
    std::fs::write(&path, manifest).unwrap();
    std::fs::write(directory.path().join("issuer.secret"), ISSUER_SECRET).unwrap();
    let staged = loader.load(SystemClock.now()).await.unwrap().unwrap();
    loader.install(staged, &security).unwrap();
    assert!(!loader.issuer_change_pending());

    let events = capture.events();
    let pending: Vec<_> = events
        .iter()
        .filter(|event| {
            event.fields.get("reason").map(String::as_str) == Some("issuer-change-requires-restart")
        })
        .collect();
    assert_eq!(
        pending.len(),
        2,
        "once per distinct staged issuer: {events:?}"
    );
    assert!(
        pending
            .iter()
            .all(|event| event.level == tracing::Level::WARN)
    );
    let rendered = format!("{events:?}");
    for secret in [ISSUER_SECRET, OTHER] {
        assert!(!rendered.contains(secret), "{rendered}");
    }
}

/// An invalid issuer is a failed load like any other malformed file: the
/// whole reload is refused and the live generation keeps serving.
#[tokio::test]
async fn an_invalid_issuer_on_reload_keeps_the_live_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_issuer_manifest(directory.path(), Some(ISSUER_SECRET.as_bytes()));
    let mut loader = SecurityLoader::new(&path);
    let loaded = loader.load(SystemClock.now()).await.unwrap().unwrap();
    let security = loader.start(loaded).unwrap();
    let app = router(ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(SystemClock),
        security: Arc::clone(&security),
        issuer: None,
    });
    for (token, issuer) in [
        (common::INSTANCE.to_owned(), "too-short".to_owned()),
        (ISSUER_SECRET.to_owned(), ISSUER_SECRET.to_owned()),
    ] {
        std::fs::write(directory.path().join("instance.token"), &token).unwrap();
        std::fs::write(directory.path().join("issuer.secret"), &issuer).unwrap();
        assert!(loader.load(SystemClock.now()).await.is_err());
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/snapshots")
                    .header("authorization", format!("Bearer {}", common::INSTANCE))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::OK, "the live generation still serves");
        assert!(!loader.issuer_change_pending());
    }
}
