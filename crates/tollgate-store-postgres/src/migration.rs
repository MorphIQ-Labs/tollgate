//! Running the embedded migrations when several instances start together.
//!
//! sqlx takes its migration lock with `pg_advisory_lock`, a statement that
//! waits while holding a snapshot. `CREATE INDEX CONCURRENTLY`, run by the
//! instance that holds the lock, waits for that snapshot to be released, so
//! each waits on the other and PostgreSQL aborts one of them (GH-112). This
//! module takes the same lock by asking for it instead: each attempt is a
//! statement that returns at once, and between attempts the session holds no
//! snapshot for an index build to wait on.

use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use tokio::time::Instant;
use tollgate_store::StoreError;

/// The pause between two attempts to take a held migration lock.
const LOCK_RETRY: Duration = Duration::from_millis(100);

/// The tightest wait the session's own settings allow, in milliseconds: the
/// two that would have ended a statement blocked on the lock. No row when both
/// are disabled.
const LOCK_WAIT_BOUND_SQL: &str = "SELECT name, setting::bigint FROM pg_settings \
     WHERE name IN ('lock_timeout', 'statement_timeout') AND setting::bigint > 0 \
     ORDER BY setting::bigint, name LIMIT 1";

/// Apply every pending migration on a connection of its own.
///
/// The connection is detached from the pool, so the session-level lock cannot
/// outlive this call on a pooled connection: success, failure and a dropped
/// future all end the session, and PostgreSQL releases the lock with it.
pub(crate) async fn run(pool: &PgPool) -> Result<(), StoreError> {
    let mut connection = pool.acquire().await.map_err(failed)?.detach();
    lock(&mut connection).await?;
    let mut migrator = sqlx::migrate!("./migrations");
    // The lock above is the migrator's; its own acquisition would wait.
    migrator.set_locking(false);
    // `run_direct` is `Migrator::run` over a connection already in hand: the
    // generic signature leaves the connect future unable to be spawned
    // ("implementation of `Acquire` is not general enough").
    migrator
        .run_direct(None, &mut connection, false)
        .await
        .map_err(failed)?;
    // The migrations are committed. A failed goodbye leaves nothing to undo:
    // dropping the socket ends the session just the same.
    if let Err(error) = sqlx::Connection::close(connection).await {
        tracing::warn!(%error, "closing the migration connection failed");
    }
    Ok(())
}

/// Take sqlx's migration lock for this database without ever waiting inside a
/// statement.
///
/// The wait is bounded exactly where a blocked statement's was: by the
/// session's `lock_timeout` or `statement_timeout`, whichever is shorter, and
/// not at all when neither is set.
async fn lock(connection: &mut PgConnection) -> Result<(), StoreError> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *connection)
        .await
        .map_err(failed)?;
    let key = lock_key(&database);
    let bound: Option<(String, i64)> = sqlx::query_as(LOCK_WAIT_BOUND_SQL)
        .fetch_optional(&mut *connection)
        .await
        .map_err(failed)?;
    let started = Instant::now();
    if attempt_lock(connection, key).await? {
        return Ok(());
    }
    tracing::info!(
        advisory_key = key,
        "waiting for another session to finish migrating this database"
    );
    loop {
        if let Some((setting, millis)) = &bound
            && started.elapsed() >= Duration::from_millis(millis.unsigned_abs())
        {
            return Err(failed(format!(
                "another session holds the migration lock (advisory key {key}); \
                 gave up after {setting} = {millis}ms"
            )));
        }
        tokio::time::sleep(LOCK_RETRY).await;
        if attempt_lock(connection, key).await? {
            return Ok(());
        }
    }
}

/// One attempt, which returns at once whether or not it gets the lock.
async fn attempt_lock(connection: &mut PgConnection, key: i64) -> Result<bool, StoreError> {
    sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(connection)
        .await
        .map_err(failed)
}

/// The advisory key sqlx's PostgreSQL migrator locks for a database
/// (`sqlx-postgres`, `migrate::generate_lock_id`). Using the same one keeps
/// this build exclusive with instances still taking the lock through sqlx, and
/// with the `sqlx` command line.
fn lock_key(database: &str) -> i64 {
    const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    0x3d32_ad9e * i64::from(CRC.checksum(database.as_bytes()))
}

fn failed(error: impl std::fmt::Display) -> StoreError {
    StoreError(format!("migrate: {error}"))
}
