//! TLS to a real PostgreSQL (INVARIANTS.md 42): what each `sslmode` accepts and
//! refuses, observed on the connections the store itself opens.
//!
//! Env-gated. `./scripts/with_tls_postgres.sh` starts a server with a
//! certificate from a throwaway authority, naming 127.0.0.1 and `localhost`,
//! and a server without TLS. Without it every test skips, unless
//! `TOLLGATE_REQUIRE_PG_TLS` is set.

use std::sync::Arc;
use std::time::Duration;

use sqlx::Row;
use tokio::sync::Mutex;
use tollgate_store::{GrantPolicy, StoreError};
use tollgate_store_postgres::{PoolConfig, PostgresStore};

struct Servers {
    tls: String,
    ca: String,
    wrong_ca: String,
    plaintext: String,
}

fn servers() -> Option<Servers> {
    let read = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let servers = (|| {
        Some(Servers {
            tls: read("TOLLGATE_PG_TLS_URL")?,
            ca: read("TOLLGATE_PG_TLS_CA")?,
            wrong_ca: read("TOLLGATE_PG_TLS_WRONG_CA")?,
            plaintext: read("TOLLGATE_PG_PLAINTEXT_URL")?,
        })
    })();
    if servers.is_none() {
        assert!(
            read("TOLLGATE_REQUIRE_PG_TLS").is_none(),
            "TOLLGATE_REQUIRE_PG_TLS is set, but the TLS environment is incomplete; \
             run under ./scripts/with_tls_postgres.sh"
        );
        eprintln!("SKIPPED: run under ./scripts/with_tls_postgres.sh");
    }
    servers
}

/// Connect, running migrations. Both fixtures' URLs carry no parameters.
async fn connect(url: &str, parameters: &str) -> Result<Arc<PostgresStore>, StoreError> {
    // One at a time: concurrent migrators of a new database deadlock between
    // the migration lock and a concurrent index build.
    static CONNECTING: Mutex<()> = Mutex::const_new(());
    let _one_at_a_time = CONNECTING.lock().await;
    PostgresStore::connect_with(
        &format!("{url}?{parameters}"),
        GrantPolicy::default(),
        PoolConfig {
            acquire_timeout: Duration::from_secs(30),
            ..PoolConfig::default()
        },
    )
    .await
}

/// Whether PostgreSQL itself reports a store connection as encrypted.
async fn encrypted(store: &PostgresStore) -> bool {
    sqlx::query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(&tollgate_store_postgres::test_support::pool(store))
        .await
        .expect("the session describes itself")
        .get(0)
}

/// The refusal, which must be a TLS failure and must not quote the password.
async fn refused(url: &str, parameters: &str) -> String {
    let Err(error) = connect(url, parameters).await else {
        panic!("{parameters} must be refused");
    };
    assert!(
        !error.0.contains("tollgate-tls-fixture-password"),
        "a refusal quoted the password"
    );
    assert!(error.0.starts_with("postgres: TLS: "), "{}", error.0);
    error.0
}

#[tokio::test]
async fn verify_full_connects_over_tls_to_the_authenticated_server() {
    let Some(servers) = servers() else {
        return;
    };
    let ca = &servers.ca;
    for name in ["sslrootcert", "ssl-root-cert", "ssl-ca"] {
        let store = connect(&servers.tls, &format!("sslmode=verify-full&{name}={ca}"))
            .await
            .expect("the certificate names this server and the authority signed it");
        assert!(encrypted(&store).await);
    }
    // The name in the certificate is checked against `host`, while `hostaddr`
    // says where to connect.
    let by_name = format!("sslmode=verify-full&sslrootcert={ca}&host=localhost&hostaddr=127.0.0.1");
    assert!(encrypted(&connect(&servers.tls, &by_name).await.unwrap()).await);
}

#[tokio::test]
async fn an_authority_that_did_not_sign_the_certificate_is_refused() {
    let Some(servers) = servers() else {
        return;
    };
    for mode in ["verify-ca", "verify-full"] {
        let wrong = format!("sslmode={mode}&sslrootcert={}", servers.wrong_ca);
        let cause = refused(&servers.tls, &wrong).await;
        assert!(cause.contains("UnknownIssuer"), "{mode}: {cause}");
        // With no root certificate only the bundled public authorities are
        // trusted, and none of them signed it.
        let cause = refused(&servers.tls, &format!("sslmode={mode}")).await;
        assert!(cause.contains("UnknownIssuer"), "{mode}: {cause}");
    }
}

#[tokio::test]
async fn only_verify_full_checks_the_server_name() {
    let Some(servers) = servers() else {
        return;
    };
    let other_name = |mode: &str| {
        format!(
            "sslmode={mode}&sslrootcert={}&host=another-server.invalid&hostaddr=127.0.0.1",
            servers.ca
        )
    };
    let cause = refused(&servers.tls, &other_name("verify-full")).await;
    assert!(cause.contains("not valid for"), "{cause}");
    let store = connect(&servers.tls, &other_name("verify-ca"))
        .await
        .expect("verify-ca accepts any name the authority signed for");
    assert!(encrypted(&store).await);
}

/// `require` and `prefer` encrypt without authenticating: even a root
/// certificate that did not sign the server's is not consulted.
#[tokio::test]
async fn require_and_prefer_encrypt_without_authenticating() {
    let Some(servers) = servers() else {
        return;
    };
    for parameters in [
        "sslmode=require".to_owned(),
        format!("sslmode=require&sslrootcert={}", servers.wrong_ca),
        "sslmode=prefer".to_owned(),
        // The default.
        "application_name=tollgate-tls-test".to_owned(),
    ] {
        let store = connect(&servers.tls, &parameters).await.unwrap();
        assert!(encrypted(&store).await, "{parameters}");
    }
}

#[tokio::test]
async fn disable_and_allow_stay_plaintext_beside_a_tls_server() {
    let Some(servers) = servers() else {
        return;
    };
    for mode in ["disable", "allow"] {
        let store = connect(&servers.tls, &format!("sslmode={mode}"))
            .await
            .unwrap();
        assert!(!encrypted(&store).await, "{mode}");
    }
}

#[tokio::test]
async fn a_server_without_tls_is_refused_by_every_mode_that_requires_it() {
    let Some(servers) = servers() else {
        return;
    };
    for mode in ["require", "verify-ca", "verify-full"] {
        let parameters = format!("sslmode={mode}&sslrootcert={}", servers.ca);
        let cause = refused(&servers.plaintext, &parameters).await;
        assert!(
            cause.contains("server does not support TLS"),
            "{mode}: {cause}"
        );
    }
    // The default still connects, in plaintext: its meaning is unchanged.
    let store = connect(&servers.plaintext, "sslmode=prefer").await.unwrap();
    assert!(!encrypted(&store).await);
}
