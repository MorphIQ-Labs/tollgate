//! Upgrade/recovery against isolated schemas, never the shared scenario tables.
use std::borrow::Cow;

use sqlx::{Connection, PgConnection, migrate::Migrator};

// SQLx's advisory lock and CREATE INDEX CONCURRENTLY's snapshot waits are
// database-wide even though the fixture tables live in distinct schemas.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type StoredUsage = (Vec<u8>, Option<Vec<u8>>, Option<i64>, i64);

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

async fn legacy_fixture() -> Option<(PgConnection, String)> {
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
    let schema = format!("tollgate_usage_upgrade_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    catalogue(14).run(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO tollgate_accounts (account_id, balance, deposited, status, next_fence, usage_recorded, settlement_loss) VALUES ($1, 90, 100, 'Active', 2, 5, 0)")
        .bind(vec![1u8; 16]).execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO tollgate_leases (lease_id, account_id, fencing_token, granted, used, credited, expires_at_us, state) VALUES ($1, $1, 1, 10, 5, 0, 60000000, 0)")
        .bind(vec![1u8; 16]).execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO tollgate_usage_events (request_id, account_id, lease_id, fencing_token, units, occurred_at_us) VALUES ($1, $1, $1, 1, 5, 0), ($2, $1, NULL, NULL, 0, 0)")
        .bind(vec![1u8; 16]).bind(vec![2u8; 16]).execute(&mut connection).await.unwrap();
    Some((connection, schema))
}

async fn cleanup(mut connection: PgConnection, schema: &str) {
    sqlx::raw_sql(&format!(
        "SET search_path TO public; DROP SCHEMA {schema} CASCADE"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    // A failed SQLx startup may retain its advisory lock until disconnection.
    connection.close().await.unwrap();
}

#[tokio::test]
async fn usage_guard_upgrade_preserves_legacy_rows_and_refuses_an_old_catalogue() {
    let _guard = DB_LOCK.lock().await;
    let Some((mut connection, schema)) = legacy_fixture().await else {
        return;
    };
    let before: Vec<StoredUsage> = sqlx::query_as(
        "SELECT request_id, lease_id, fencing_token, units FROM tollgate_usage_events ORDER BY request_id"
    ).fetch_all(&mut connection).await.unwrap();
    sqlx::migrate!("./migrations")
        .run(&mut connection)
        .await
        .unwrap();
    let after: Vec<StoredUsage> = sqlx::query_as(
        "SELECT request_id, lease_id, fencing_token, units FROM tollgate_usage_events ORDER BY request_id"
    ).fetch_all(&mut connection).await.unwrap();
    assert_eq!(before, after);
    let validated: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint WHERE connamespace = current_schema()::regnamespace AND convalidated AND conname IN ('tollgate_usage_events_units_nonneg', 'tollgate_usage_events_fencing_token_positive', 'tollgate_accounts_next_fence_positive', 'tollgate_leases_fencing_token_positive')"
    ).fetch_one(&mut connection).await.unwrap();
    assert_eq!(validated, 4);
    // The old INSERT shape continues to accept zero-unit overage without the
    // newer optional attribution fields after the guards are installed.
    sqlx::query("INSERT INTO tollgate_usage_events (request_id, account_id, lease_id, fencing_token, units, occurred_at_us) VALUES ($1, $2, NULL, NULL, 0, 0)")
        .bind(vec![3u8; 16]).bind(vec![1u8; 16]).execute(&mut connection).await.unwrap();
    let old_startup = catalogue(14).run(&mut connection).await;
    assert!(matches!(
        old_startup,
        Err(sqlx::migrate::MigrateError::VersionMissing(15))
    ));
    cleanup(connection, &schema).await;
}

#[tokio::test]
async fn invalid_history_blocks_validation_but_leaves_write_guards_and_can_be_repaired() {
    let _guard = DB_LOCK.lock().await;
    for (table, column, invalid, repaired, predicate) in [
        (
            "tollgate_usage_events",
            "units",
            -1_i64,
            5_i64,
            "lease_id IS NOT NULL",
        ),
        (
            "tollgate_usage_events",
            "fencing_token",
            -1,
            1,
            "lease_id IS NOT NULL",
        ),
        (
            "tollgate_usage_events",
            "fencing_token",
            0,
            1,
            "lease_id IS NOT NULL",
        ),
        ("tollgate_accounts", "next_fence", 0, 2, "TRUE"),
        ("tollgate_leases", "fencing_token", 0, 1, "TRUE"),
    ] {
        let Some((mut connection, schema)) = legacy_fixture().await else {
            return;
        };
        sqlx::query(&format!(
            "UPDATE {table} SET {column} = $1 WHERE {predicate}"
        ))
        .bind(invalid)
        .execute(&mut connection)
        .await
        .unwrap();
        let error = sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .unwrap_err();
        let suffix = if column == "units" {
            "nonneg"
        } else {
            "positive"
        };
        assert!(
            error
                .to_string()
                .contains(&format!("{table}_{column}_{suffix}")),
            "{error}"
        );
        let applied: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            applied, 15,
            "validation failure preserves the installed guards"
        );
        let old_value: i64 =
            sqlx::query_scalar(&format!("SELECT {column} FROM {table} WHERE {predicate}"))
                .fetch_one(&mut connection)
                .await
                .unwrap();
        assert_eq!(
            old_value, invalid,
            "migration must not rewrite corrupt history"
        );
        let refused =
            sqlx::query("UPDATE tollgate_usage_events SET units = -2 WHERE lease_id IS NULL")
                .execute(&mut connection)
                .await
                .unwrap_err();
        assert_eq!(
            refused.as_database_error().unwrap().constraint(),
            Some("tollgate_usage_events_units_nonneg")
        );
        // Fixture-only repair to the known original value. Operators must use
        // authoritative evidence; blindly normalizing history is not recovery.
        sqlx::query(&format!(
            "UPDATE {table} SET {column} = $1 WHERE {predicate}"
        ))
        .bind(repaired)
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .unwrap();
        let applied: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(applied, 16);
        cleanup(connection, &schema).await;
    }
}
