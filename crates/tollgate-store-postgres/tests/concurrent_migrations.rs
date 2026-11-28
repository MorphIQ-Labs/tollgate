//! Instances starting together against one database (GH-112, INVARIANTS.md 42).
//!
//! Every test creates a database of its own, so the migration lock, which is
//! per database, and the snapshots an index build waits for are this test's
//! alone. The role in `TOLLGATE_PG_URL` therefore needs `CREATEDB`.
//!
//! Interleavings are placed by observing `pg_stat_activity` and by holding
//! locks, never by timing. The pauses below only space out those observations,
//! and the attempt limit turns a hang into a failure.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use sqlx::migrate::{Migrate, Migrator};
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tollgate_store::{GrantPolicy, StoreError};
use tollgate_store_postgres::PostgresStore;

type Startup = tokio::task::JoinHandle<Result<Arc<PostgresStore>, StoreError>>;

struct Database {
    server: String,
    name: String,
    url: String,
}

impl Database {
    async fn create() -> Option<Self> {
        let Ok(server) = std::env::var("TOLLGATE_PG_URL") else {
            assert!(
                std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
                "PostgreSQL is required"
            );
            eprintln!("SKIPPED: TOLLGATE_PG_URL not set");
            return None;
        };
        let name = format!("tollgate_migrate_{}", uuid::Uuid::new_v4().simple());
        let mut connection = PgConnection::connect(&server).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let (address, query) = server.split_once('?').unwrap_or((&server, ""));
        let (authority, _) = address
            .rsplit_once('/')
            .expect("the URL names a database after the host");
        let url = format!("{authority}/{name}?{query}");
        Some(Database { server, name, url })
    }

    async fn connection(&self) -> PgConnection {
        PgConnection::connect(&self.url).await.unwrap()
    }

    fn startup(&self) -> Startup {
        self.startup_at(self.url.clone())
    }

    /// Named, so that [`WANTS_THE_LOCK`] sees a starting instance and never a
    /// session of the test's own.
    fn startup_at(&self, url: String) -> Startup {
        let url = format!("{url}&application_name=tollgate_starting");
        tokio::spawn(async move { PostgresStore::connect(&url, GrantPolicy::default()).await })
    }

    /// The same database with one more session setting.
    fn url_with(&self, setting: &str) -> String {
        let separator = if self.url.ends_with('?') { "" } else { "&" };
        format!("{}{separator}options[{setting}", self.url)
    }

    async fn drop_database(self) {
        let mut connection = PgConnection::connect(&self.server).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.name
        )))
        .execute(&mut connection)
        .await
        .unwrap();
        connection.close().await.unwrap();
    }
}

fn catalogue(through: i64) -> Migrator {
    let mut migrations = sqlx::migrate!("./migrations");
    migrations.migrations = Cow::Owned(
        migrations
            .iter()
            .filter(|m| m.version <= through)
            .cloned()
            .collect(),
    );
    migrations
}

fn head() -> i64 {
    sqlx::migrate!("./migrations")
        .iter()
        .map(|migration| migration.version)
        .max()
        .expect("the migrator carries migrations")
}

/// Wait until another session of this database matches `predicate` over
/// `pg_stat_activity`.
async fn wait_for_session(observer: &mut PgConnection, predicate: &'static str) {
    wait_until_sessions(observer, predicate, true).await;
}

/// Wait until whether such a session exists equals `present`.
async fn wait_until_sessions(observer: &mut PgConnection, predicate: &'static str, present: bool) {
    let statement = format!(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() AND ({predicate}))"
    );
    for _ in 0..3_000 {
        let found: bool = sqlx::query_scalar(AssertSqlSafe(statement.clone()))
            .fetch_one(&mut *observer)
            .await
            .unwrap();
        if found == present {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("sessions matching {predicate} never became present = {present}");
}

/// A concurrent index build held up by an open write. The statement text is
/// no use here: the migration's header fills what the view keeps of it.
const BUILD_IS_WAITING: &str = "wait_event_type = 'Lock' AND pid IN \
     (SELECT pid FROM pg_stat_progress_create_index)";

/// A startup asking for the migration lock, however it asks.
const WANTS_THE_LOCK: &str =
    "application_name = 'tollgate_starting' AND query ILIKE '%advisory_lock(%'";

/// Any session of a starting instance.
const STARTING_SESSION: &str = "application_name = 'tollgate_starting'";

async fn assert_at_head(connection: &mut PgConnection) {
    let (applied, newest): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE success), MAX(version) FROM _sqlx_migrations",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert_eq!(newest, head());
    assert_eq!(
        applied,
        i64::try_from(sqlx::migrate!("./migrations").iter().count()).unwrap()
    );
    let invalid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_index WHERE NOT indisvalid")
        .fetch_one(connection)
        .await
        .unwrap();
    assert_eq!(invalid, 0, "an interrupted concurrent build leaves one");
}

/// Wait until no session holds an advisory lock on this database, then take the
/// migration lock to show it can be taken. The wait only reads `pg_locks`:
/// waiting inside `pg_advisory_lock` would hold a snapshot, and deadlock with an
/// index build an abandoned startup is still running.
async fn assert_migration_lock_is_released(database: &Database) {
    let mut connection = database.connection().await;
    wait_until_sessions(
        &mut connection,
        "pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted)",
        false,
    )
    .await;
    connection.lock().await.expect("the lock was released");
    connection.close().await.unwrap();
}

#[tokio::test]
async fn servers_starting_together_on_a_new_database_all_start() {
    let Some(database) = Database::create().await else {
        return;
    };
    let startups: Vec<_> = (0..8).map(|_| database.startup()).collect();
    for startup in startups {
        startup.await.unwrap().expect("every instance starts");
    }
    assert_at_head(&mut database.connection().await).await;
    database.drop_database().await;
}

/// The cycle itself, placed rather than raced. An open write holds the first
/// instance inside migration 5's concurrent index build, and the second asks
/// for the migration lock before the build reaches its wait for old snapshots.
#[tokio::test]
async fn a_startup_waiting_to_migrate_does_not_deadlock_a_concurrent_index_build() {
    let Some(database) = Database::create().await else {
        return;
    };
    let mut setup = database.connection().await;
    catalogue(4).run(&mut setup).await.unwrap();

    let mut writer = database.connection().await;
    let mut write = writer.begin().await.unwrap();
    sqlx::query("LOCK TABLE tollgate_leases IN ROW EXCLUSIVE MODE")
        .execute(&mut *write)
        .await
        .unwrap();

    let mut observer = database.connection().await;
    let first = database.startup();
    wait_for_session(&mut observer, BUILD_IS_WAITING).await;
    let second = database.startup();
    wait_for_session(&mut observer, WANTS_THE_LOCK).await;
    write.commit().await.unwrap();

    first.await.unwrap().expect("the migrating instance starts");
    second.await.unwrap().expect("the waiting instance starts");
    assert_at_head(&mut observer).await;
    database.drop_database().await;
}

/// Earlier releases and the `sqlx` command line take the lock through sqlx, so
/// this build must stay exclusive with them.
#[tokio::test]
async fn startup_waits_for_a_session_holding_sqlx_s_migration_lock() {
    let Some(database) = Database::create().await else {
        return;
    };
    let mut holder = database.connection().await;
    holder.lock().await.unwrap();
    let mut observer = database.connection().await;

    let startup = database.startup();
    wait_for_session(&mut observer, WANTS_THE_LOCK).await;
    let migrated: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert!(
        !migrated,
        "nothing runs while another migrator holds the lock"
    );
    assert!(!startup.is_finished());

    holder.unlock().await.unwrap();
    startup
        .await
        .unwrap()
        .expect("starts once the lock is free");
    assert_at_head(&mut observer).await;
    database.drop_database().await;
}

#[tokio::test]
async fn a_configured_timeout_ends_the_wait_for_the_migration_lock() {
    let Some(database) = Database::create().await else {
        return;
    };
    let mut holder = database.connection().await;
    holder.lock().await.unwrap();

    // Each setting on its own, then both, where the shorter one decides.
    for (settings, named) in [
        (&["lock_timeout]=200ms"][..], "lock_timeout = 200ms"),
        (&["statement_timeout]=250ms"], "statement_timeout = 250ms"),
        (
            &["lock_timeout]=1h", "statement_timeout]=300ms"],
            "statement_timeout = 300ms",
        ),
        (
            &["lock_timeout]=350ms", "statement_timeout]=1h"],
            "lock_timeout = 350ms",
        ),
    ] {
        let mut url = database.url.clone();
        for setting in settings {
            url = Database {
                server: database.server.clone(),
                name: database.name.clone(),
                url,
            }
            .url_with(setting);
        }
        let error = database
            .startup_at(url)
            .await
            .unwrap()
            .err()
            .expect("the lock is held");
        assert!(error.0.starts_with("migrate: "), "{error}");
        assert!(error.0.contains("migration lock"), "{error}");
        assert!(error.0.contains(named), "{error}");
    }

    // A bound that has not run out does not refuse a lock that is released.
    // The refused startups' sessions must be gone first, or one of them would
    // be taken for the waiting instance.
    let mut observer = database.connection().await;
    wait_until_sessions(&mut observer, STARTING_SESSION, false).await;
    let startup = database.startup_at(database.url_with("lock_timeout]=1h"));
    wait_for_session(&mut observer, WANTS_THE_LOCK).await;
    holder.unlock().await.unwrap();
    startup.await.unwrap().expect("starts inside its bound");
    database.drop_database().await;
}

#[tokio::test]
async fn a_failed_startup_releases_the_migration_lock() {
    let Some(database) = Database::create().await else {
        return;
    };
    database.startup().await.unwrap().unwrap();
    let mut connection = database.connection().await;
    sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, 'fixture unknown migration', true, $2, 0)")
        .bind(i64::MAX)
        .bind(vec![0u8; 48])
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(database.startup().await.unwrap().is_err());
    assert_migration_lock_is_released(&database).await;
    database.drop_database().await;
}

#[tokio::test]
async fn a_dropped_startup_releases_the_migration_lock() {
    let Some(database) = Database::create().await else {
        return;
    };
    let mut setup = database.connection().await;
    catalogue(4).run(&mut setup).await.unwrap();
    let mut writer = database.connection().await;
    let mut write = writer.begin().await.unwrap();
    sqlx::query("LOCK TABLE tollgate_leases IN ROW EXCLUSIVE MODE")
        .execute(&mut *write)
        .await
        .unwrap();

    let mut observer = database.connection().await;
    let startup = database.startup();
    wait_for_session(&mut observer, BUILD_IS_WAITING).await;
    startup.abort();
    assert!(startup.await.err().expect("aborted").is_cancelled());
    // The statement already sent still finishes; the session then ends.
    write.commit().await.unwrap();
    assert_migration_lock_is_released(&database).await;
    database.drop_database().await;
}
