//! Opt-in PostgreSQL fixture operations (`test-support` feature).
//!
//! These helpers mutate a database: reset destroys ledger data, and plan
//! inspection runs ANALYZE. Use only an isolated disposable test database.
//! Normal consumers do not enable this feature; in-repository integration
//! tests opt in through dev-dependencies.

use sqlx::Row;
use tollgate_core::AccountId;
use tollgate_store::StoreError;

use crate::{
    ACTIVE_LEASE_SUM_SQL, DUE_PERIODS_SQL, PostgresStore, RECLAIM_DUE_LEASES_SQL, id_bytes,
    instant::StoredInstant, storage,
};

/// The plan PostgreSQL chooses for the active-lease sum inside
/// [`PostgresStore::conservation`] — the query migration 0005's partial index
/// exists to serve (#12).
///
/// Whether an index is *used* is a claim only the planner can settle, and
/// this explains the same query string `conservation` runs rather than one
/// a test wrote: a hand-copied query would keep reporting an index scan
/// long after the real predicate had drifted away from the index.
///
/// Statistics are refreshed first. `TRUNCATE` resets `reltuples` to zero
/// and autoanalyze runs on its own schedule, so a plan chosen immediately
/// after a load would describe an empty table rather than a real one.
pub async fn explain_active_lease_sum(
    store: &PostgresStore,
    account: AccountId,
) -> Result<String, StoreError> {
    sqlx::raw_sql("ANALYZE tollgate_leases")
        .execute(&store.pool)
        .await
        .map_err(storage)?;
    let rows = sqlx::query(&format!("EXPLAIN {ACTIVE_LEASE_SUM_SQL}"))
        .bind(id_bytes(account.0))
        .fetch_all(&store.pool)
        .await
        .map_err(storage)?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The plan PostgreSQL chooses for the expiry sweep's selection inside
/// `reclaim_expired_batch` (#65).
///
/// The property worth pinning is not "an index is used" — the predicate always
/// matched `tollgate_leases_expiry`. It is that the `LIMIT` can *stop* the
/// walk: under an `ORDER BY` the index cannot answer, the planner reads every
/// expired row and sorts it before taking a page, so each bounded batch costs
/// the whole backlog. A `Sort` node above the scan is that defect, which is
/// why the caller asserts on its absence.
///
/// Statistics are refreshed first, for the reason
/// [`explain_active_lease_sum`] gives.
pub async fn explain_reclaim_due_leases(
    store: &PostgresStore,
    cutoff: jiff::Timestamp,
    limit: i64,
) -> Result<String, StoreError> {
    let cutoff = StoredInstant::from(cutoff);
    sqlx::raw_sql("ANALYZE tollgate_leases")
        .execute(&store.pool)
        .await
        .map_err(storage)?;
    let rows = sqlx::query(&format!("EXPLAIN {RECLAIM_DUE_LEASES_SQL}"))
        .bind(cutoff.micros)
        .bind(cutoff.submicro_nanos)
        .bind(limit)
        .fetch_all(&store.pool)
        .await
        .map_err(storage)?;
    Ok(plan_text(&rows))
}

/// The plan PostgreSQL chooses for the rollover sweep's selection inside
/// `roll_due_periods` — #65's sibling, and the same property: the bounded page
/// must be an index-range stop rather than a sort of every account whose
/// boundary has passed.
pub async fn explain_due_periods(
    store: &PostgresStore,
    period: &str,
    boundary_us: i64,
    limit: i64,
) -> Result<String, StoreError> {
    sqlx::raw_sql("ANALYZE tollgate_accounts")
        .execute(&store.pool)
        .await
        .map_err(storage)?;
    let rows = sqlx::query(&format!("EXPLAIN {DUE_PERIODS_SQL}"))
        .bind(period)
        .bind(boundary_us)
        .bind(limit)
        .fetch_all(&store.pool)
        .await
        .map_err(storage)?;
    Ok(plan_text(&rows))
}

fn plan_text(rows: &[sqlx::postgres::PgRow]) -> String {
    rows.iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Delete all account, lease, snapshot, credential and usage rows, including
/// dependent rows removed by CASCADE. Preserve the schema and migration history.
///
/// Destructive: use only on an isolated disposable test database. Never call
/// against a production store or a database another test process is using.
pub async fn truncate_all(store: &PostgresStore) -> Result<(), StoreError> {
    sqlx::raw_sql(
        "TRUNCATE tollgate_credential_keys, tollgate_usage_events, tollgate_leases, \
         tollgate_snapshots, tollgate_accounts CASCADE",
    )
    .execute(&store.pool)
    .await
    .map_err(storage)?;
    Ok(())
}
