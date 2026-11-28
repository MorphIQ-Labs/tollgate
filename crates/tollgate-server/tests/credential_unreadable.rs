//! A credential row that cannot be projected crosses HTTP as a withheld id, and
//! delays nothing else (INVARIANTS.md 34).
mod common;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use tollgate_auth::{CredentialVerifier, HmacRegistry, MintedKey};
use tollgate_client::{KeyManager, KeyManagerConfig};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId, Principal};
use tollgate_server::{Backend, ServerState, serve};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, KeyDirectory, KeyPage, KeyRecord, KeySource,
    ManualClock, MemoryStore,
};

#[path = "../../tollgate-store/tests/support/credential_unreadable.rs"]
mod credential_unreadable;

const SECRET: &[u8] = b"fixture-unreadable-credential-secret-107";
const ACCOUNT: AccountId = AccountId(107);

fn t(second: i64) -> Timestamp {
    Timestamp::from_second(second).unwrap()
}

async fn account(store: &impl AdminStore) {
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(100),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
}

fn record(key: &MintedKey) -> KeyRecord {
    KeyRecord {
        key_id: key.key_id,
        account_id: ACCOUNT,
        principal: key.principal,
        digest: key.digest,
        not_after: None,
    }
}

/// The shared scenario read over HTTP, then one instance's key manager running
/// against the same server.
async fn over_http<S: Backend + KeyDirectory>(store: Arc<S>) {
    account(&*store).await;
    let clock = Arc::new(ManualClock::new(t(100)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = common::http(format!("http://{}", listener.local_addr().unwrap()));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            issuer: None,
            store: store.clone(),
            clock: clock.clone(),
        },
        Duration::from_secs(60),
        async {
            stopped.await.unwrap_or_default();
        },
    ));
    credential_unreadable::unreadable_credentials_are_withheld(&*store, &*http, ACCOUNT, t(100))
        .await;

    let issuer = HmacRegistry::new(SECRET);
    let mint = |id| issuer.mint(KeyId(id)).unwrap();
    let (retired, broken, kept) = (mint(20), mint(21), mint(22));
    store.insert_key(record(&retired)).await.unwrap();
    store.insert_key(record(&kept)).await.unwrap();
    let manager = KeyManager::spawn(
        http.clone(),
        SECRET,
        clock.clone(),
        KeyManagerConfig {
            refresh_interval: Duration::from_millis(20),
            fetch_timeout: Duration::from_secs(5),
            pass_timeout: Duration::from_secs(10),
            max_age: Duration::from_secs(60),
            page_limit: NonZeroUsize::new(1).unwrap(),
            ..KeyManagerConfig::default()
        },
    )
    .unwrap();
    let monitor = manager.monitor();
    let verifier = manager.verifier();
    let until = async |condition: &dyn Fn() -> bool| {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the running manager must converge");
    };
    until(&|| verifier.verify(&retired.secret).is_some()).await;

    store
        .insert_key(KeyRecord {
            principal: Principal(broken.principal.0 ^ 1),
            ..record(&broken)
        })
        .await
        .unwrap();
    store.revoke_key(KeyId(20), t(100)).await.unwrap();
    until(&|| verifier.verify(&retired.secret).is_none()).await;
    until(&|| monitor.report(t(100)).stats.unreadable_credentials > 0).await;
    assert!(verifier.verify(&kept.secret).is_some());
    assert!(verifier.verify(&broken.secret).is_none());
    assert!(monitor.report(t(100)).ready);

    // An instance that predates the withheld ids validates a page by its
    // records alone. A full page stays valid to it.
    let page = http
        .active_keys_page(t(100), None, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert!(page.next_after().is_some());
    assert!(
        KeyPage::try_new(
            page.revision(),
            page.as_of(),
            None,
            NonZeroUsize::new(1).unwrap(),
            page.records().to_vec(),
            page.next_after(),
        )
        .is_ok()
    );

    assert!(!manager.shutdown().await.task_failed);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn memory_unreadable_credentials_cross_http_without_stalling_the_projection() {
    over_http(MemoryStore::new(GrantPolicy::default()).unwrap()).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_unreadable_credentials_cross_http_without_stalling_the_projection() {
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
    over_http(store).await;
}
