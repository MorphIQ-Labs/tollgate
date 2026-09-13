//! Finite expiry migration, old-writer fencing, and corruption behavior.
use std::borrow::Cow;
use std::num::NonZeroUsize;

use jiff::Timestamp;
use sqlx::{Connection, Executor, PgConnection, Row, migrate::Migrator};
use tollgate_core::{AccountId, KeyId, Principal};
use tollgate_store::{GrantPolicy, KeyDirectory, KeyRecord, KeySource};
use tollgate_store_postgres::PostgresStore;

// Old migrations build indexes concurrently: their database-wide advisory
// lock and snapshot waits require serialization even for separate schemas.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
type Identity = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Option<i64>);

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

async fn fixture(expiries: &[Option<i64>]) -> Option<(PgConnection, String, String)> {
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
    let schema = format!(
        "tollgate_credential_expiry_{}",
        uuid::Uuid::new_v4().simple()
    );
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    catalogue(17).run(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO tollgate_accounts (account_id, balance, deposited, status, next_fence, usage_recorded, settlement_loss) VALUES ($1, 0, 0, 'Active', 1, 0, 0)")
        .bind(1_u128.to_be_bytes().to_vec()).execute(&mut connection).await.unwrap();
    for (id, &expiry) in expiries.iter().enumerate() {
        let id = (id as u128 + 1).to_be_bytes();
        let mut digest = [0x18; 32];
        digest[..16].copy_from_slice(&id);
        sqlx::query("INSERT INTO tollgate_credential_keys (key_id, account_id, principal, digest, not_after_us) VALUES ($1,$2,$1,$3,$4)")
            .bind(id.to_vec()).bind(1_u128.to_be_bytes().to_vec()).bind(digest.to_vec()).bind(expiry)
            .execute(&mut connection).await.unwrap();
    }
    connection
        .prepare("SELECT not_after_us FROM tollgate_credential_keys")
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

async fn revision(connection: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT revision FROM tollgate_credential_revision")
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn credential_expiry_upgrade_bounds_legacy_authority_and_fences_old_queries() {
    let _guard = DB_LOCK.lock().await;
    let legacy = [
        None,
        Some(Timestamp::MIN.as_microsecond()),
        Some(-1),
        Some(0),
        Some(1),
        Some(100_000_000),
        Some(Timestamp::MAX.as_microsecond()),
    ];
    let Some((mut connection, schema, url)) = fixture(&legacy).await else {
        return;
    };
    // Preserve an actual retirement value across migration as well as NULLs.
    sqlx::query("UPDATE tollgate_credential_keys SET revoked_at_us = -1 WHERE key_id = $1")
        .bind(7_u128.to_be_bytes().to_vec())
        .execute(&mut connection)
        .await
        .unwrap();
    let original: Vec<Identity> = sqlx::query_as("SELECT key_id, account_id, principal, digest, revoked_at_us FROM tollgate_credential_keys ORDER BY key_id")
        .fetch_all(&mut connection).await.unwrap();
    let before_revision = revision(&mut connection).await;
    catalogue(18).run(&mut connection).await.unwrap();
    assert_eq!(revision(&mut connection).await, before_revision + 1);
    let retained: Vec<Identity> = sqlx::query_as("SELECT key_id, account_id, principal, digest, revoked_at_us FROM tollgate_credential_keys ORDER BY key_id")
        .fetch_all(&mut connection).await.unwrap();
    assert_eq!(original, retained);
    let bounds = sqlx::query("SELECT not_after_floor_us, not_after_submicro_ns, not_after_is_lower_bound FROM tollgate_credential_keys ORDER BY key_id")
        .fetch_all(&mut connection).await.unwrap();
    let mut expected = Vec::new();
    for (row, old) in bounds.iter().zip(legacy) {
        let actual = row
            .get::<Option<i64>, _>(0)
            .map(|u| i128::from(u) * 1000 + i128::from(row.get::<i16, _>(1)));
        let lower = old.map(|u| {
            (i128::from(u) * 1000 - if u <= 0 { 999 } else { 0 })
                .max(Timestamp::MIN.as_nanosecond())
        });
        assert_eq!(actual, lower);
        assert_eq!(row.get::<bool, _>(2), old.is_some());
        if old.is_none() {
            assert_eq!(row.get::<Option<i16>, _>(1), None);
        }
        expected.push(lower);
    }
    for query in [
        "SELECT not_after_us FROM tollgate_credential_keys",
        "SELECT key_id FROM tollgate_credential_keys WHERE not_after_us IS NULL OR not_after_us > 0 ORDER BY key_id LIMIT 2",
        "UPDATE tollgate_credential_keys SET not_after_us = 1",
        "INSERT INTO tollgate_credential_keys (key_id, account_id, principal, digest, not_after_us) VALUES (decode('ff','hex'),decode('ff','hex'),decode('ff','hex'),decode('ff','hex'),1)",
    ] {
        let error = sqlx::query(query)
            .execute(&mut connection)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42703")
        );
        assert_eq!(revision(&mut connection).await, before_revision + 1);
    }
    let mut digest = [0x21; 32];
    digest[..16].copy_from_slice(&200_u128.to_be_bytes());
    let old_indefinite_insert = sqlx::query("INSERT INTO tollgate_credential_keys (key_id, account_id, principal, digest) VALUES ($1,$2,$1,$3)")
        .bind(200_u128.to_be_bytes().to_vec()).bind(1_u128.to_be_bytes().to_vec()).bind(digest.to_vec())
        .execute(&mut connection).await.unwrap_err();
    assert_eq!(
        old_indefinite_insert
            .as_database_error()
            .unwrap()
            .code()
            .as_deref(),
        Some("23502")
    );
    assert_eq!(revision(&mut connection).await, before_revision + 1);
    assert!(matches!(
        catalogue(17).run(&mut connection).await,
        Err(sqlx::migrate::MigrateError::VersionMissing(18))
    ));
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut connection)
        .await
        .unwrap();

    for _ in 0..2 {
        // Restart repeats the same durable evidence without renewing it.
        let store = PostgresStore::connect(&url, GrantPolicy::default())
            .await
            .unwrap();
        let now = Timestamp::MIN;
        let expected: Vec<_> = expected
            .iter()
            .enumerate()
            .filter(|(i, end)| *i != 6 && end.is_none_or(|end| now.as_nanosecond() < end))
            .map(|(i, end)| (KeyId(i as u128 + 1), *end))
            .collect();
        let keys: Vec<_> = store
            .active_keys(now)
            .await
            .unwrap()
            .iter()
            .map(|key| (key.key_id, key.not_after.map(Timestamp::as_nanosecond)))
            .collect();
        assert_eq!(keys, expected);
        let mut after = None;
        let mut keys = Vec::new();
        loop {
            let page = store
                .active_keys_page(now, after, NonZeroUsize::new(1).unwrap())
                .await
                .unwrap();
            assert_eq!(page.revision(), (before_revision + 1) as u64);
            keys.extend(
                page.records()
                    .iter()
                    .map(|key| (key.key_id, key.not_after.map(Timestamp::as_nanosecond))),
            );
            match page.next_after() {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(keys, expected);
        drop(store);
    }
    let store = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    for (id, expiry) in [(100_u128, Some(Timestamp::MAX)), (101, None)] {
        let mut digest = [0x20; 32];
        digest[..16].copy_from_slice(&id.to_be_bytes());
        store
            .insert_key(KeyRecord {
                key_id: KeyId(id),
                account_id: AccountId(1),
                principal: Principal(id),
                digest,
                not_after: expiry,
            })
            .await
            .unwrap();
        let row = sqlx::query("SELECT not_after_floor_us, not_after_submicro_ns, not_after_is_lower_bound FROM tollgate_credential_keys WHERE key_id=$1")
            .bind(id.to_be_bytes().to_vec()).fetch_one(&mut connection).await.unwrap();
        assert!(!row.get::<bool, _>(2), "new evidence is exact");
        let actual = row
            .get::<Option<i64>, _>(0)
            .map(|u| i128::from(u) * 1000 + i128::from(row.get::<i16, _>(1)));
        assert_eq!(actual, expiry.map(Timestamp::as_nanosecond));
    }
    drop(store);
    cleanup(connection, &schema).await;
}

#[tokio::test]
async fn invalid_credential_history_or_revision_overflow_preserves_the_old_schema() {
    let _guard = DB_LOCK.lock().await;
    for invalid_time in [true, false] {
        let Some((mut connection, schema, _)) = fixture(&[Some(100_000_000)]).await else {
            return;
        };
        let before_revision = revision(&mut connection).await;
        if invalid_time {
            sqlx::query("UPDATE tollgate_credential_keys SET not_after_us = $1")
                .bind(i64::MAX)
                .execute(&mut connection)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE tollgate_credential_revision SET revision = $1")
                .bind(i64::MAX)
                .execute(&mut connection)
                .await
                .unwrap();
        }
        let staged_revision = revision(&mut connection).await;
        assert!(catalogue(18).run(&mut connection).await.is_err());
        assert_eq!(revision(&mut connection).await, staged_revision);
        let value: i64 = sqlx::query_scalar("SELECT not_after_us FROM tollgate_credential_keys")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(value, if invalid_time { i64::MAX } else { 100_000_000 });
        let version: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(version, 17);
        // Restore only known fixture values; production revision reset or
        // expiry clamping without authoritative evidence is not recovery.
        sqlx::query("UPDATE tollgate_credential_revision SET revision = $1")
            .bind(before_revision)
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("UPDATE tollgate_credential_keys SET not_after_us = 100000000")
            .execute(&mut connection)
            .await
            .unwrap();
        catalogue(18).run(&mut connection).await.unwrap();
        cleanup(connection, &schema).await;
    }
}

#[tokio::test]
async fn credential_expiry_constraints_and_readers_refuse_incomplete_evidence() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema, url)) = fixture(&[Some(100_000_000)]).await else {
        return;
    };
    catalogue(18).run(&mut connection).await.unwrap();
    for (micros, nanos, lower) in [
        (Some(i64::MIN), Some(0_i16), false),
        (Some(i64::MAX), Some(0), false),
        (Some(0), Some(-1), false),
        (Some(0), Some(1000), false),
        (None, Some(0), false),
        (Some(0), None, false),
        (None, None, true),
    ] {
        let error = sqlx::query("UPDATE tollgate_credential_keys SET not_after_floor_us=$1, not_after_submicro_ns=$2, not_after_is_lower_bound=$3")
            .bind(micros).bind(nanos).bind(lower).execute(&mut connection).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().constraint(),
            Some("tollgate_credential_keys_expiry_domain")
        );
    }
    // Corrupt storage is outside the schema contract, but readers must still
    // report a missing counterpart rather than omit it or invent infinity.
    sqlx::query("ALTER TABLE tollgate_credential_keys DROP CONSTRAINT tollgate_credential_keys_expiry_domain")
        .execute(&mut connection).await.unwrap();
    let store = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    for (micros, nanos) in [
        (None, Some(0_i16)),
        (Some(100_000_000_i64), None),
        (None, None),
    ] {
        sqlx::query(
            "UPDATE tollgate_credential_keys SET not_after_floor_us=$1, not_after_submicro_ns=$2",
        )
        .bind(micros)
        .bind(nanos)
        .execute(&mut connection)
        .await
        .unwrap();
        assert!(store.active_keys(Timestamp::UNIX_EPOCH).await.is_err());
        for after in [None, Some(KeyId(0))] {
            assert!(
                store
                    .active_keys_page(Timestamp::UNIX_EPOCH, after, NonZeroUsize::new(1).unwrap())
                    .await
                    .is_err()
            );
        }
    }
    drop(store);
    cleanup(connection, &schema).await;
}

#[tokio::test]
async fn an_indefinite_catalogue_migrates_without_inventing_expiry_or_revision() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema, url)) = fixture(&[None]).await else {
        return;
    };
    sqlx::query("UPDATE tollgate_credential_revision SET revision = $1")
        .bind(i64::MAX)
        .execute(&mut connection)
        .await
        .unwrap();
    catalogue(18).run(&mut connection).await.unwrap();
    assert_eq!(revision(&mut connection).await, i64::MAX);
    let store = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    let keys = store.active_keys(Timestamp::MAX).await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].not_after, None);
    drop(store);
    cleanup(connection, &schema).await;
}
