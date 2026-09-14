//! Exact source expiry reaches HTTP projection, verification and warm sessions.
mod common;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use tollgate_auth::{CredentialVerifier, HmacRegistry, SessionCredential};
use tollgate_client::{KeyManager, KeyManagerConfig};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId};
use tollgate_server::{Backend, ServerState, serve};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, KeyDirectory, KeyRecord, KeySource, ManualClock,
    MemoryStore,
};

const SECRET: &[u8] = b"fixture-exact-credential-expiry-secret-118";

async fn exact_projection<S: Backend + KeyDirectory>(store: Arc<S>) {
    let account = AccountId(118);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: account,
            initial_balance: CostUnits(100),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    let clock = Arc::new(ManualClock::new(Timestamp::UNIX_EPOCH));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = common::http(format!("http://{}", listener.local_addr().unwrap()));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            store: store.clone(),
            clock: clock.clone(),
        },
        Duration::from_secs(60),
        async {
            stopped.await.unwrap_or_default();
        },
    ));
    for (index, expiry) in [
        Timestamp::new(0, -1).unwrap(),
        Timestamp::new(0, 1).unwrap(),
        Timestamp::new(100, 1).unwrap(),
        Timestamp::MAX,
    ]
    .into_iter()
    .enumerate()
    {
        // Keep freshness later than individual expiry so that it cannot mask
        // a lost credential deadline. MAX is the only endpoint with no later
        // representable freshness bound.
        let lead_seconds = if expiry == Timestamp::MAX { 20 } else { 10 };
        let now = expiry - SignedDuration::from_secs(lead_seconds);
        clock.set(now);
        let minted = HmacRegistry::new(SECRET)
            .mint(KeyId(index as u128 + 1))
            .unwrap();
        store
            .insert_key(KeyRecord {
                key_id: minted.key_id,
                account_id: account,
                principal: minted.principal,
                digest: minted.digest,
                not_after: Some(expiry),
            })
            .await
            .unwrap();
        let manager = KeyManager::spawn(
            http.clone(),
            SECRET,
            clock.clone(),
            KeyManagerConfig {
                refresh_interval: Duration::from_secs(5),
                fetch_timeout: Duration::from_secs(2),
                pass_timeout: Duration::from_secs(5),
                max_age: Duration::from_secs(20),
                page_limit: NonZeroUsize::new(1).unwrap(),
                ..KeyManagerConfig::default()
            },
        )
        .unwrap();
        let mut monitor = manager.monitor();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !monitor.report(now).ready {
                monitor.changed().await.unwrap();
            }
        })
        .await
        .expect("a complete exact projection becomes ready");
        let verifier = manager.verifier();
        let evidence = verifier.verify(&minted.secret).unwrap();
        assert_eq!(evidence.reusable_until, Some(expiry));
        let session = SessionCredential::new();
        assert_eq!(
            session.authenticate(Some(&minted.secret), &verifier, now),
            Some(minted.principal)
        );
        assert_eq!(
            session.authenticate(
                Some(&minted.secret),
                &verifier,
                expiry - SignedDuration::from_nanos(1)
            ),
            Some(minted.principal)
        );
        assert!(
            session
                .authenticate(Some(&minted.secret), &verifier, expiry)
                .is_none()
        );
        assert!(!session.is_authenticated());
        assert!(
            SessionCredential::new()
                .authenticate(Some(&minted.secret), &verifier, expiry)
                .is_none()
        );
        clock.set(expiry);
        let page = http
            .active_keys_page(expiry, None, NonZeroUsize::new(16).unwrap())
            .await
            .unwrap();
        assert_eq!(page.as_of(), expiry);
        assert!(page.records().iter().all(|key| key.key_id != minted.key_id));
        assert!(!manager.shutdown().await.task_failed);
    }
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn memory_expiry_reaches_http_projection_and_session_exactly() {
    exact_projection(MemoryStore::new(GrantPolicy::default()).unwrap()).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_expiry_reaches_http_projection_and_session_exactly() {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        assert!(
            std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
            "PostgreSQL is required"
        );
        eprintln!("SKIPPED: TOLLGATE_PG_URL not set");
        return;
    };
    let store = tollgate_store_postgres::PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    tollgate_store_postgres::test_support::truncate_all(&store)
        .await
        .unwrap();
    exact_projection(store).await;
}
