//! A store call dropped mid-flight, as a server handler is when its client
//! disconnects, leaves the pooled connection outside any transaction
//! (INVARIANTS.md 18, GH-101).
//!
//! The pool holds one connection, so the next call takes the one the cancelled
//! call used. A TCP hop between the store and PostgreSQL holds the server's
//! replies once a chosen statement has been sent, which fixes where the call is
//! when it is dropped without relying on timing.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::{Connection, PgConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits};
use tollgate_store::{AccountConfig, AdminStore, GrantPolicy, KeySource};
use tollgate_store_postgres::{PoolConfig, PostgresStore, test_support};

static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A bound on every wait, so a defect fails the test instead of hanging it.
const DEADLINE: Duration = Duration::from_secs(10);

const EXISTING: AccountId = AccountId(0x101);
const LATER: AccountId = AccountId(0x102);

async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"))
}

/// Forwards to PostgreSQL. [`Hop::hold_after`] makes it withhold every reply
/// from the moment the store sends the named bytes.
struct Hop {
    address: String,
    trigger: Arc<Mutex<Option<&'static [u8]>>>,
    holding: watch::Sender<bool>,
}

impl Hop {
    async fn to(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let trigger = Arc::new(Mutex::new(None));
        let (holding, _) = watch::channel(false);
        let (armed, gate) = (Arc::clone(&trigger), holding.clone());
        // Ends with the test's runtime.
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(forward(
                    client,
                    upstream.clone(),
                    Arc::clone(&armed),
                    gate.clone(),
                ));
            }
        });
        Hop {
            address,
            trigger,
            holding,
        }
    }

    fn hold_after(&self, sent: &'static [u8]) {
        *self.trigger.lock().unwrap() = Some(sent);
    }

    async fn holding(&self) {
        let mut holding = self.holding.subscribe();
        holding.wait_for(|holding| *holding).await.unwrap();
    }

    fn release(&self) {
        self.holding.send_replace(false);
    }
}

async fn forward(
    client: TcpStream,
    upstream: String,
    trigger: Arc<Mutex<Option<&'static [u8]>>>,
    holding: watch::Sender<bool>,
) {
    let server = TcpStream::connect(upstream).await.unwrap();
    let (mut from_client, mut to_client) = client.into_split();
    let (mut from_server, mut to_server) = server.into_split();
    let mut gate = holding.subscribe();
    let requests = async move {
        let mut buffer = [0_u8; 16 * 1024];
        // Kept across reads, so a statement split between two is still seen.
        let mut seen = Vec::new();
        while let Ok(read @ 1..) = from_client.read(&mut buffer).await {
            {
                let mut trigger = trigger.lock().unwrap();
                if let Some(sent) = *trigger {
                    seen.extend_from_slice(&buffer[..read]);
                    if seen.windows(sent.len()).any(|window| window == sent) {
                        *trigger = None;
                        // Before the statement is forwarded, so its reply
                        // cannot overtake the hold.
                        holding.send_replace(true);
                    }
                } else {
                    seen.clear();
                }
            }
            if to_server.write_all(&buffer[..read]).await.is_err() {
                return;
            }
        }
    };
    let replies = async move {
        let mut buffer = [0_u8; 16 * 1024];
        while let Ok(read @ 1..) = from_server.read(&mut buffer).await {
            if gate.wait_for(|holding| !*holding).await.is_err()
                || to_client.write_all(&buffer[..read]).await.is_err()
            {
                return;
            }
        }
    };
    tokio::select! {
        () = requests => {}
        () = replies => {}
    }
}

/// `url` with its `host:port` replaced, and that address.
fn through(url: &str, hop: &str) -> String {
    url.replacen(address_of(url), hop, 1)
}

/// The `host:port` of a `scheme://user:password@host:port/database` URL.
fn address_of(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split(['/', '?']).next().unwrap();
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, address)| address)
}

struct Fixture {
    hop: Hop,
    store: Arc<PostgresStore>,
    /// A session of its own, past the hop: what everyone else can see.
    observer: PgConnection,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
            assert!(
                std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
                "PostgreSQL is required"
            );
            eprintln!("SKIPPED: TOLLGATE_PG_URL not set");
            return None;
        };
        let direct = PostgresStore::connect(&url, GrantPolicy::default())
            .await
            .unwrap();
        test_support::truncate_all(&direct).await.unwrap();
        create(&direct, EXISTING).await;
        let hop = Hop::to(address_of(&url).to_owned()).await;
        let store = PostgresStore::connect_with(
            &through(&url, &hop.address),
            GrantPolicy::default(),
            PoolConfig {
                max_connections: 1,
                acquire_timeout: DEADLINE,
            },
        )
        .await
        .unwrap();
        let observer = PgConnection::connect(&url).await.unwrap();
        Some(Fixture {
            hop,
            store,
            observer,
        })
    }

    /// Start changing the existing account's class, and drop the call once
    /// `sent` has left the store and before any reply to it has arrived.
    async fn cancel_a_change_after(&self, sent: &'static [u8]) {
        self.hop.hold_after(sent);
        let change = self
            .store
            .set_capacity_class(EXISTING, CapacityClass::BestEffort);
        bounded("the statement to be sent", async {
            tokio::select! {
                result = change => panic!("answered while its replies were held: {result:?}"),
                () = self.hop.holding() => {}
            }
        })
        .await;
        self.hop.release();
    }

    /// The next write is visible to another session, and the next transaction,
    /// which must be the connection's outermost to set its isolation, runs.
    async fn the_connection_is_clean(&mut self) {
        bounded("the next write", create(&self.store, LATER)).await;
        let visible: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tollgate_accounts WHERE account_id = $1")
                .bind(LATER.0.to_be_bytes().to_vec())
                .fetch_one(&mut self.observer)
                .await
                .unwrap();
        assert_eq!(visible, 1, "a write reported as done was never committed");
        bounded(
            "the next transaction",
            self.store
                .active_keys_page(jiff::Timestamp::UNIX_EPOCH, None, NonZeroUsize::MIN),
        )
        .await
        .expect("the next transaction starts cleanly");
    }

    async fn stored_class(&mut self) -> String {
        sqlx::query_scalar("SELECT capacity_class FROM tollgate_accounts WHERE account_id = $1")
            .bind(EXISTING.0.to_be_bytes().to_vec())
            .fetch_one(&mut self.observer)
            .await
            .unwrap()
    }
}

async fn create(store: &PostgresStore, account_id: AccountId) {
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id,
            initial_balance: CostUnits(1),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_call_cancelled_during_begin_leaves_no_open_transaction() {
    let _guard = DB_LOCK.lock().await;
    let Some(mut fixture) = Fixture::new().await else {
        return;
    };
    fixture.cancel_a_change_after(b"BEGIN\0").await;
    fixture.the_connection_is_clean().await;
    assert_eq!(
        fixture.stored_class().await,
        CapacityClass::Assured.as_str()
    );
}

#[tokio::test]
async fn a_call_cancelled_inside_its_transaction_is_rolled_back() {
    let _guard = DB_LOCK.lock().await;
    let Some(mut fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .cancel_a_change_after(b"UPDATE tollgate_accounts SET capacity_class")
        .await;
    fixture.the_connection_is_clean().await;
    assert_eq!(
        fixture.stored_class().await,
        CapacityClass::Assured.as_str()
    );
}

/// The caller of a call dropped here learns nothing, and the change is made:
/// PostgreSQL had the commit.
#[tokio::test]
async fn a_call_cancelled_during_commit_commits_and_leaves_no_open_transaction() {
    let _guard = DB_LOCK.lock().await;
    let Some(mut fixture) = Fixture::new().await else {
        return;
    };
    fixture.cancel_a_change_after(b"COMMIT\0").await;
    fixture.the_connection_is_clean().await;
    assert_eq!(
        fixture.stored_class().await,
        CapacityClass::BestEffort.as_str()
    );
}
