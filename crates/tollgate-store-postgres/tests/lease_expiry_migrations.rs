//! Upgrade, retained-connection compatibility, and durable timing witnesses.
use std::borrow::Cow;

use jiff::{SignedDuration, Timestamp};
use sqlx::{Connection, Executor, PgConnection, Row, migrate::Migrator};
use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseId};
use tollgate_store::{GrantPolicy, LeaseAllocator};
use tollgate_store_postgres::PostgresStore;

type StoredLease = (Vec<u8>, i64, i64, i64, i64, i16);

// Old catalogues include CREATE INDEX CONCURRENTLY. PostgreSQL's database-
// wide migration advisory lock and its snapshot waits need serialization,
// even though each fixture owns a distinct schema.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn legacy_fixture() -> Option<(PgConnection, String, String)> {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        assert!(
            std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
            "PostgreSQL is required"
        );
        eprintln!("SKIPPED: TOLLGATE_PG_URL not set");
        return None;
    };
    let mut connection = PgConnection::connect(&url)
        .await
        .expect("connect to test database");
    let schema = format!("tollgate_expiry_upgrade_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    catalogue(16).run(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO tollgate_accounts (account_id, balance, deposited, status, next_fence, usage_recorded, settlement_loss) VALUES ($1, 80, 100, 'Active', 3, 0, 0)")
        .bind(1_u128.to_be_bytes().to_vec()).execute(&mut connection).await.unwrap();
    for id in [1_u128, 2] {
        sqlx::query("INSERT INTO tollgate_leases (lease_id, account_id, fencing_token, granted, used, credited, expires_at_us, state) VALUES ($1, $2, $3, 10, 0, 0, 100000000, 0)")
            .bind(id.to_be_bytes().to_vec()).bind(1_u128.to_be_bytes().to_vec())
            .bind(id as i64).execute(&mut connection).await.unwrap();
    }
    // Keep the original connection (and its statement cache) across upgrade.
    connection
        .prepare("SELECT expires_at_us FROM tollgate_leases")
        .await
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let schema_url = format!("{url}{separator}options=-csearch_path%3D{schema}");
    Some((connection, schema, schema_url))
}

async fn cleanup(mut connection: PgConnection, schema: &str) {
    sqlx::raw_sql(&format!(
        "SET search_path TO public; DROP SCHEMA {schema} CASCADE"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}

#[tokio::test]
async fn expiry_upgrade_preserves_accounting_and_fences_old_lease_queries() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema, url)) = legacy_fixture().await else {
        return;
    };
    let before: Vec<StoredLease> = sqlx::query_as(
        "SELECT lease_id, fencing_token, granted, used, credited, state FROM tollgate_leases ORDER BY lease_id")
        .fetch_all(&mut connection).await.unwrap();
    catalogue(17).run(&mut connection).await.unwrap();
    let after: Vec<StoredLease> = sqlx::query_as(
        "SELECT lease_id, fencing_token, granted, used, credited, state FROM tollgate_leases ORDER BY lease_id")
        .fetch_all(&mut connection).await.unwrap();
    assert_eq!(before, after);
    for query in [
        "SELECT expires_at_us FROM tollgate_leases",
        "SELECT account_id, expires_at_us FROM tollgate_leases FOR UPDATE",
        "SELECT lease_id FROM tollgate_leases WHERE state = 0 AND expires_at_us <= 130000000 FOR UPDATE SKIP LOCKED",
        "INSERT INTO tollgate_leases (lease_id, account_id, fencing_token, granted, used, credited, expires_at_us, state) VALUES (decode('03','hex'), decode('03','hex'), 3, 10, 0, 0, 140000000, 0)",
    ] {
        // Also prove that an old acquire's account debit cannot survive its
        // refused INSERT. The transaction rollback restores the ledger.
        let mut tx = connection.begin().await.unwrap();
        sqlx::query("UPDATE tollgate_accounts SET balance = balance - 10")
            .execute(&mut *tx)
            .await
            .unwrap();
        let error = sqlx::query(query).execute(&mut *tx).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42703")
        );
        tx.rollback().await.unwrap();
        let balance: i64 = sqlx::query_scalar("SELECT balance FROM tollgate_accounts")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(balance, 80);
    }
    let old_startup = catalogue(16).run(&mut connection).await;
    assert!(matches!(
        old_startup,
        Err(sqlx::migrate::MigrateError::VersionMissing(17))
    ));
    // A failed old migrator can retain its advisory lock on this connection.
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut connection)
        .await
        .unwrap();
    let policy = GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        ..GrantPolicy::default()
    };
    let store = PostgresStore::connect(&url, policy).await.unwrap();
    let boundary = Timestamp::new(130, 999).unwrap();
    assert!(
        store
            .reclaim_expired(boundary - SignedDuration::from_nanos(1))
            .await
            .unwrap()
            .is_empty()
    );
    store
        .release(
            LeaseId(1),
            FencingToken(1),
            CostUnits(10),
            boundary - SignedDuration::from_nanos(1),
        )
        .await
        .unwrap();
    let reclaimed = store.reclaim_expired(boundary).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].lease_id, LeaseId(2));
    assert_eq!(reclaimed[0].reclaimed, CostUnits(10));
    let c = store.conservation(AccountId(1)).await.unwrap().unwrap();
    assert!(c.holds());
    assert_eq!(c.balance, CostUnits(100));
    assert_eq!(c.active_lease_grants, CostUnits::ZERO);
    assert_eq!(c.settlement_loss, CostUnits::ZERO);
    let legacy: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tollgate_leases WHERE expiry_is_upper_bound")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(
        legacy, 2,
        "settled history retains its uncertainty provenance"
    );

    let lease = store
        .acquire(
            AccountId(1),
            CostUnits(10),
            SignedDuration::from_nanos(1),
            Timestamp::new(-1, 0).unwrap(),
        )
        .await
        .unwrap();
    let row = sqlx::query("SELECT expires_at_floor_us, expires_at_submicro_ns, expiry_is_upper_bound FROM tollgate_leases WHERE lease_id = $1")
        .bind(lease.lease_id.0.to_be_bytes().to_vec()).fetch_one(&mut connection).await.unwrap();
    assert_eq!(row.get::<i64, _>(0), -1_000_000);
    assert_eq!(row.get::<i16, _>(1), 1);
    assert!(!row.get::<bool, _>(2));
    drop(store);
    let restarted = PostgresStore::connect(&url, policy).await.unwrap();
    let deadline = lease.expires_at + policy.reclaim_grace;
    assert!(
        restarted
            .reclaim_expired(deadline - SignedDuration::from_nanos(1))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        restarted.reclaim_expired(deadline).await.unwrap()[0].lease_id,
        lease.lease_id
    );
    drop(restarted);
    cleanup(connection, &schema).await;
}

#[tokio::test]
async fn legacy_expiry_bounds_cover_both_sides_of_the_epoch() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema, _)) = legacy_fixture().await else {
        return;
    };
    sqlx::query("DELETE FROM tollgate_leases")
        .execute(&mut connection)
        .await
        .unwrap();
    for (i, nanos) in [-1_001_i64, -1_000, -999, -1, 0, 1, 999, 1_000, 1_001]
        .into_iter()
        .enumerate()
    {
        sqlx::query("INSERT INTO tollgate_leases (lease_id, account_id, fencing_token, granted, used, credited, expires_at_us, state) VALUES ($1, $2, 1, 0, 0, 0, $3, 0)")
            .bind((i as u128).to_be_bytes().to_vec()).bind(1_u128.to_be_bytes().to_vec())
            .bind(nanos / 1_000).execute(&mut connection).await.unwrap();
    }
    catalogue(17).run(&mut connection).await.unwrap();
    let rows = sqlx::query("SELECT expires_at_floor_us, expires_at_submicro_ns, expiry_is_upper_bound FROM tollgate_leases ORDER BY lease_id")
        .fetch_all(&mut connection).await.unwrap();
    for (row, original) in rows
        .iter()
        .zip([-1_001_i64, -1_000, -999, -1, 0, 1, 999, 1_000, 1_001])
    {
        let bound = row.get::<i64, _>(0) * 1_000 + i64::from(row.get::<i16, _>(1));
        assert!(bound >= original);
        // The zero microsecond bucket straddles the epoch: -999..=999.
        assert!(bound - original <= 1_998);
        assert!(row.get::<bool, _>(2));
    }
    cleanup(connection, &schema).await;
}

#[tokio::test]
async fn invalid_legacy_expiry_rolls_back_upgrade_and_exact_rows_enforce_the_domain() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema, _)) = legacy_fixture().await else {
        return;
    };
    sqlx::query("UPDATE tollgate_leases SET expires_at_us = $1")
        .bind(i64::MAX)
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(catalogue(17).run(&mut connection).await.is_err());
    let old: Vec<i64> = sqlx::query_scalar("SELECT expires_at_us FROM tollgate_leases")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    assert_eq!(old, vec![i64::MAX; 2]);
    let applied: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(applied, 16);
    // Fixture repair uses known original evidence, never blind clamping.
    sqlx::query("UPDATE tollgate_leases SET expires_at_us = 100000000")
        .execute(&mut connection)
        .await
        .unwrap();
    catalogue(17).run(&mut connection).await.unwrap();
    for (micros, nanos) in [(i64::MIN, 0_i16), (i64::MAX, 0), (0, -1), (0, 1_000)] {
        let error = sqlx::query(
            "UPDATE tollgate_leases SET expires_at_floor_us = $1, expires_at_submicro_ns = $2",
        )
        .bind(micros)
        .bind(nanos)
        .execute(&mut connection)
        .await
        .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().constraint(),
            Some("tollgate_leases_expiry_domain")
        );
    }
    cleanup(connection, &schema).await;
}
