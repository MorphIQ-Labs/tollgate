//! PostgreSQL backend.
//!
//! Reproduces `MemoryStore`'s settlement rules exactly — the shared
//! correctness suite (`tests/postgres_suite.rs`, mirroring
//! `tollgate-store/tests/store_suite.rs` by name) is the proof. Concurrency
//! control is row-level: `SELECT ... FOR UPDATE` on the account funds
//! acquire; on the lease it serializes release/ingest/reclaim against each
//! other. Fencing tokens come from the account row's `next_fence` counter,
//! so allocation is strictly monotonic per account across any number of
//! servers sharing the database. Each token remains a capability for its own
//! lease record; the sequence is not an account-wide validity epoch.
//!
//! Representation choices (PoC-pragmatic, documented):
//! - u128 ids as 16-byte `BYTEA` (big-endian);
//! - units as `BIGINT` with checked u64↔i64 conversion in both directions
//!   (a balance beyond i64::MAX is refused, not wrapped; a negative stored
//!   value is refused, not clamped) and schema-level CHECK constraints
//!   keeping every unit column non-negative;
//! - timestamps as `BIGINT` microseconds since the Unix epoch;
//! - snapshots as storage-local `JSONB`: ids in the legacy u64 range remain
//!   numeric for rollback, larger ids use canonical text, and the public
//!   HTTP/Serde contract always uses text.
//!
//! Snapshot pushes broadcast in-process only; cross-process push
//! (LISTEN/NOTIFY or the server's future SSE) is a documented seam in
//! `docs/DESIGN.md`.

use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, EnforcementMode, FencingToken,
    Generation, KeyId, LeaseGrant, LeaseId, PermissionBits, Principal, PublishableSnapshot,
    ResolvedLimits, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, Conservation, CreateAccountError, GrantPolicy,
    IngestReport, KeyDirectory, KeyError, KeyRecord, LeaseAllocator, PUSH_CHANNEL_CAPACITY,
    PublishSnapshotError, ReclaimBatch, ReclaimedLease, Revocation, SetStatusError, SnapshotPush,
    SnapshotResolution, SnapshotSource, StatusChange, StoreError, StoreHealth, UsageSink,
    pushes_exceed_capacity,
};

const STATE_ACTIVE: i16 = 0;
const STATE_RELEASED: i16 = 1;
const STATE_EXPIRED: i16 = 2;

/// One storage-local identifier. Values the previous codec could represent
/// stay numeric for rollback; the rest of the promised u128 domain uses
/// canonical text because serde_json's default number type rejects it.
#[derive(Debug, Clone, Copy)]
struct StoredId(u128);

impl Serialize for StoredId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match u64::try_from(self.0) {
            Ok(value) => serializer.serialize_u64(value),
            Err(_) => serializer.collect_str(&format_args!("{:032x}", self.0)),
        }
    }
}

impl<'de> Deserialize<'de> for StoredId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StoredIdVisitor;

        impl serde::de::Visitor<'_> for StoredIdVisitor {
            type Value = StoredId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a legacy u64 number or a canonical 128-bit identifier string")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(StoredId(u128::from(value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                value
                    .parse::<AccountId>()
                    .map(|id| StoredId(id.0))
                    .map_err(E::custom)
            }
        }

        deserializer.deserialize_any(StoredIdVisitor)
    }
}

/// A digest column that is not exactly 32 bytes is corruption, never
/// something to pad or truncate into shape: the comparison it feeds decides
/// authentication, and a silently reshaped digest would either refuse a
/// legitimate credential forever or, worse, shorten what is compared.
fn digest_from(bytes: &[u8], key_id: KeyId) -> Result<[u8; 32], StoreError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| {
        StoreError(format!(
            "credential {key_id} has a {}-byte digest in storage, expected 32",
            bytes.len()
        ))
    })
}

#[async_trait]
impl KeyDirectory for PostgresStore {
    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError> {
        // The account reference is checked by the foreign key rather than by a
        // prior SELECT: a check-then-insert would admit a credential against
        // an account deleted between the two, and this backend refuses to
        // hold a credential no account owns.
        let result = sqlx::query(
            "INSERT INTO tollgate_credential_keys
             (key_id, account_id, principal, digest, not_after_us, revoked_at_us)
             VALUES ($1, $2, $3, $4, $5, NULL)
             ON CONFLICT (key_id) DO NOTHING",
        )
        .bind(id_bytes(record.key_id.0))
        .bind(id_bytes(record.account_id.0))
        .bind(id_bytes(record.principal.0))
        .bind(record.digest.to_vec())
        .bind(record.not_after.map(ts_micros))
        .execute(&self.pool)
        .await;
        match result {
            Ok(done) if done.rows_affected() == 0 => Err(KeyError::AlreadyExists),
            Ok(_) => Ok(()),
            // A violated foreign key is the account not existing; a violated
            // unique index on `principal` is two credentials colliding on the
            // identity admission decides with, which at 128 bits of HMAC
            // output means secret reuse or corruption rather than chance.
            Err(sqlx::Error::Database(e)) if e.is_foreign_key_violation() => {
                Err(KeyError::UnknownAccount)
            }
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                Err(KeyError::AlreadyExists)
            }
            Err(e) => Err(KeyError::Storage(storage(e))),
        }
    }

    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError> {
        // One statement decides all three answers, so "did this retire
        // anything" cannot race a concurrent revocation between a read and a
        // write: the UPDATE matches only live rows, and the RETURNING tells us
        // whether it matched. A follow-up existence check separates "no such
        // key" from "already retired".
        let retired = sqlx::query(
            "UPDATE tollgate_credential_keys SET revoked_at_us = $2
             WHERE key_id = $1 AND revoked_at_us IS NULL
             RETURNING key_id",
        )
        .bind(id_bytes(key_id.0))
        .bind(ts_micros(now))
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| KeyError::Storage(storage(e)))?;
        if retired.is_some() {
            return Ok(Revocation::Retired);
        }
        let exists = sqlx::query("SELECT 1 FROM tollgate_credential_keys WHERE key_id = $1")
            .bind(id_bytes(key_id.0))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| KeyError::Storage(storage(e)))?;
        if exists.is_some() {
            Ok(Revocation::AlreadyRetired)
        } else {
            Err(KeyError::UnknownKey)
        }
    }

    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        // Expiry is applied here, beside revocation, so this backend answers
        // "active" exactly as `MemoryStore` does and a projection built from
        // either sees the same live set. Ordering is explicit for the same
        // reason: two instances must not build tables that differ by row
        // order alone.
        let rows = sqlx::query(
            "SELECT key_id, account_id, principal, digest, not_after_us
             FROM tollgate_credential_keys
             WHERE revoked_at_us IS NULL
               AND (not_after_us IS NULL OR not_after_us > $1)
             ORDER BY key_id",
        )
        .bind(ts_micros(now))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;

        rows.into_iter()
            .map(|row| {
                let key_id = KeyId(id_from(row.get::<Vec<u8>, _>(0).as_slice()));
                Ok(KeyRecord {
                    key_id,
                    account_id: AccountId(id_from(row.get::<Vec<u8>, _>(1).as_slice())),
                    principal: Principal(id_from(row.get::<Vec<u8>, _>(2).as_slice())),
                    digest: digest_from(row.get::<Vec<u8>, _>(3).as_slice(), key_id)?,
                    not_after: row
                        .get::<Option<i64>, _>(4)
                        .map(|us| {
                            Timestamp::from_microsecond(us).map_err(|_| {
                                StoreError(format!(
                                    "credential {key_id} has an undecodable not_after: {us}"
                                ))
                            })
                        })
                        .transpose()?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod stored_id_tests {
    use super::StoredId;

    #[test]
    fn malformed_storage_id_explains_both_accepted_representations() {
        let error = serde_json::from_value::<StoredId>(serde_json::Value::Bool(true)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("a legacy u64 number or a canonical 128-bit identifier string"),
            "unexpected diagnostic: {error}"
        );
    }
}

/// PostgreSQL's snapshot JSON predates the portable HTTP identifier contract.
/// Keep this boundary explicit so existing rows remain readable and ordinary
/// legacy-range writes remain rollback-safe while high-bit ids finally work.
///
/// **Two facts in this table follow opposite rules, deliberately.** The
/// generation is *not* here — it lives only in the `generation` column, because
/// that column is what the `ON CONFLICT ... WHERE` monotonicity guard compares
/// and what a tombstone reports after its snapshot is gone (#54). `account_id`
/// *is* here and must stay: its column is `GENERATED ALWAYS AS` a function of
/// this JSON (migration 0006), so deleting it here would silently NULL that
/// column, unmatch the partial index, and make an account-wide status change
/// republish nothing.
///
/// Both rules say "one writer per fact". They differ on which copy survives
/// because the generation needs a BIGINT comparison the JSON cannot give.
#[derive(Serialize)]
struct StoredSnapshotRef<'a> {
    account_id: StoredId,
    key_id: Option<StoredId>,
    status: &'a AccountStatus,
    enforcement_mode: &'a EnforcementMode,
    valid_until: &'a Timestamp,
    permissions: &'a PermissionBits,
    limits: &'a ResolvedLimits,
    cost_table: &'a Arc<CostTable>,
}

impl<'a> From<&'a AccountSnapshot> for StoredSnapshotRef<'a> {
    fn from(snapshot: &'a AccountSnapshot) -> Self {
        StoredSnapshotRef {
            account_id: StoredId(snapshot.account_id.0),
            key_id: snapshot.key_id.map(|id| StoredId(id.0)),
            status: &snapshot.status,
            enforcement_mode: &snapshot.enforcement_mode,
            valid_until: &snapshot.valid_until,
            permissions: &snapshot.permissions,
            limits: &snapshot.limits,
            cost_table: &snapshot.cost_table,
        }
    }
}

/// Rows written before #54 still carry a `generation` key. Serde ignores
/// unknown fields, so those rows decode unchanged and the vestigial key is
/// simply not read — which is why this needed no backfill.
#[derive(Deserialize)]
struct StoredSnapshot {
    account_id: StoredId,
    key_id: Option<StoredId>,
    status: AccountStatus,
    /// Absent from every row written before elastic mode existed, and those
    /// rows are the overwhelming majority the first time this ships.
    ///
    /// `default` rather than a required field, because the alternative is not
    /// a loud failure but a silent outage: a required field makes every
    /// pre-existing row fail to decode, `SnapshotSource::snapshot` returns a
    /// store error for each, and the request path denies every principal until
    /// someone republishes the whole catalogue. Defaulting to
    /// [`EnforcementMode::Strict`] is also the safe direction — an
    /// undecided account enforces, it does not extend credit.
    #[serde(default)]
    enforcement_mode: EnforcementMode,
    valid_until: Timestamp,
    permissions: PermissionBits,
    limits: ResolvedLimits,
    cost_table: Arc<CostTable>,
}

impl StoredSnapshot {
    /// Rebuild the snapshot around a generation the JSON no longer carries.
    ///
    /// Deliberately not a `From` impl: the generation has to come from the
    /// row's column, and a conversion that could be written without it would
    /// be a conversion someone writes without it.
    fn into_snapshot(self, generation: Generation) -> AccountSnapshot {
        let builder = AccountSnapshot::builder(
            AccountId(self.account_id.0),
            generation,
            self.status,
            self.valid_until,
            self.permissions,
            self.limits,
            self.cost_table,
        )
        .enforcement_mode(self.enforcement_mode);
        match self.key_id {
            Some(key_id) => builder.key_id(tollgate_core::KeyId(key_id.0)).build(),
            None => builder.build(),
        }
    }
}

/// The reconciliation query's active-lease sum, named because two callers must
/// agree on it: `conservation` runs it, and `explain_active_lease_sum` asks the
/// planner what it does with it. A test that copied the text would keep
/// reporting an index scan after the real predicate had drifted away from
/// migration 0005's index (#12).
///
/// `state = 0` is spelled out rather than bound, so the predicate is a literal
/// the partial index can match.
const ACTIVE_LEASE_SUM_SQL: &str =
    "SELECT COALESCE(SUM(granted), 0)::BIGINT, COALESCE(SUM(used), 0)::BIGINT
     FROM tollgate_leases WHERE account_id = $1 AND state = 0";

fn id_bytes(id: u128) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn id_from(bytes: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    u128::from_be_bytes(buf)
}

fn to_i64(units: CostUnits, what: &str) -> Result<i64, StoreError> {
    i64::try_from(units.get()).map_err(|_| StoreError(format!("{what} exceeds i64 range")))
}

/// A negative unit column is ledger corruption, never a value to normalise:
/// clamping it to zero would let the conservation equation pass over exactly
/// the discrepancy it exists to expose.
fn to_units(value: i64, what: &str) -> Result<CostUnits, StoreError> {
    u64::try_from(value)
        .map(CostUnits)
        .map_err(|_| StoreError(format!("{what} is negative in storage: {value}")))
}

fn ts_micros(ts: Timestamp) -> i64 {
    ts.as_microsecond()
}

fn storage(e: sqlx::Error) -> StoreError {
    StoreError(format!("postgres: {e}"))
}

fn alloc_storage(e: sqlx::Error) -> AllocateError {
    AllocateError::Storage(storage(e))
}

/// Finish a transaction before making its result observable.
///
/// `sqlx::Transaction` only queues a rollback when it is dropped. A caller can
/// therefore start a competing transaction after this future returns but
/// before PostgreSQL has released the first transaction's row locks. That is
/// especially visible to reclaim's `SKIP LOCKED` query, which may otherwise
/// miss a lease owned by an operation that has already reported failure.
///
/// Generic over the error type rather than duplicated per error: this crate
/// now finishes transactions that fail with `StoreError`, `AllocateError` and
/// `SetStatusError`, and three copies of the rollback-and-report logic would
/// be three places for it to drift.
async fn finish_transaction<T, E>(
    tx: Transaction<'_, Postgres>,
    result: Result<T, E>,
) -> Result<T, E>
where
    E: From<StoreError> + std::fmt::Display,
{
    match result {
        Ok(value) => {
            tx.commit().await.map_err(|e| E::from(storage(e)))?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = tx.rollback().await {
                return Err(E::from(StoreError(format!(
                    "operation failed ({error}); transaction rollback failed ({})",
                    storage(rollback_error)
                ))));
            }
            Err(error)
        }
    }
}

pub struct PostgresStore {
    pool: PgPool,
    policy: GrantPolicy,
    reclaim_grace_us: i64,
    push: broadcast::Sender<SnapshotPush>,
}

/// Connection-pool bounds. Callers that hold background tasks open against
/// this store rely on `acquire_timeout`: when every connection is checked out
/// by a stalled query, it is the only thing that turns "wait forever" into an
/// error the caller can report (INVARIANTS.md #18).
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    pub max_connections: u32,
    /// How long a caller may wait for a free connection before the call
    /// fails. Must be positive.
    pub acquire_timeout: std::time::Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_connections: 16,
            acquire_timeout: std::time::Duration::from_secs(5),
        }
    }
}

impl PoolConfig {
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.max_connections == 0 {
            return Err(StoreError("max_connections must be positive".into()));
        }
        if self.acquire_timeout.is_zero() {
            return Err(StoreError("acquire_timeout must be positive".into()));
        }
        Ok(())
    }
}

impl PostgresStore {
    /// Connect with default pool bounds and run pending migrations.
    pub async fn connect(url: &str, policy: GrantPolicy) -> Result<Arc<Self>, StoreError> {
        Self::connect_with(url, policy, PoolConfig::default()).await
    }

    /// Connect and run pending migrations (versioned under ./migrations,
    /// tracked by sqlx's _sqlx_migrations table — review finding #11).
    ///
    /// Note the limits of what a pool bound can promise: `acquire_timeout`
    /// covers waiting for a connection, including establishing one, but a
    /// query already in flight on a healthy connection is bounded only by a
    /// server-side `statement_timeout`. Callers must still bound their own
    /// calls (INVARIANTS.md #18).
    pub async fn connect_with(
        url: &str,
        policy: GrantPolicy,
        pool_config: PoolConfig,
    ) -> Result<Arc<Self>, StoreError> {
        policy
            .validate()
            .map_err(|e| StoreError(format!("invalid grant policy: {e}")))?;
        pool_config.validate()?;
        let reclaim_grace_us = i64::try_from(policy.reclaim_grace.as_micros())
            .map_err(|_| StoreError("reclaim_grace exceeds PostgreSQL timestamp range".into()))?;
        let pool = PgPoolOptions::new()
            .max_connections(pool_config.max_connections)
            .acquire_timeout(pool_config.acquire_timeout)
            .connect(url)
            .await
            .map_err(storage)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| StoreError(format!("migrate: {e}")))?;
        let (push, _) = broadcast::channel(PUSH_CHANNEL_CAPACITY);
        Ok(Arc::new(PostgresStore {
            pool,
            policy,
            reclaim_grace_us,
            push,
        }))
    }

    /// Test/reset helper: drop all quota rows (not the schema).
    /// Broadcast a control-plane change to in-process subscribers. No
    /// receivers is not a failure — a pull still observes the change — but
    /// how many instances the push reached is the difference between
    /// propagating in milliseconds and propagating at the next refresh, so
    /// it is reported rather than discarded.
    fn push_to_subscribers(&self, push: SnapshotPush) {
        let principal = push.principal;
        let subscribers = self.push.send(push).unwrap_or(0);
        tracing::debug!(
            %principal,
            subscribers,
            "snapshot pushed to subscribers"
        );
    }

    /// The plan PostgreSQL chooses for the active-lease sum inside
    /// [`Self::conservation`] — the query migration 0005's partial index
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
    pub async fn explain_active_lease_sum(&self, account: AccountId) -> Result<String, StoreError> {
        sqlx::raw_sql("ANALYZE tollgate_leases")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        let rows = sqlx::query(&format!("EXPLAIN {ACTIVE_LEASE_SUM_SQL}"))
            .bind(id_bytes(account.0))
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect::<Vec<_>>()
            .join("\n"))
    }

    pub async fn truncate_all(&self) -> Result<(), StoreError> {
        sqlx::raw_sql(
            "TRUNCATE tollgate_credential_keys, tollgate_usage_events, tollgate_leases, \
             tollgate_snapshots, tollgate_accounts CASCADE",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    // ---- reconciliation / test surface (mirrors MemoryStore) ---------

    pub async fn balance(&self, account: AccountId) -> Result<CostUnits, StoreError> {
        let row = sqlx::query("SELECT balance FROM tollgate_accounts WHERE account_id = $1")
            .bind(id_bytes(account.0))
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        row.map(|r| to_units(r.get::<i64, _>(0), "account balance"))
            .transpose()
            .map(|units| units.unwrap_or(CostUnits::ZERO))
    }

    pub async fn usage_recorded(&self, account: AccountId) -> Result<CostUnits, StoreError> {
        let row = sqlx::query("SELECT usage_recorded FROM tollgate_accounts WHERE account_id = $1")
            .bind(id_bytes(account.0))
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        row.map(|r| to_units(r.get::<i64, _>(0), "account usage_recorded"))
            .transpose()
            .map(|units| units.unwrap_or(CostUnits::ZERO))
    }

    pub async fn conservation(
        &self,
        account: AccountId,
    ) -> Result<Option<Conservation>, StoreError> {
        let Some(row) = sqlx::query(
            "SELECT deposited, balance, usage_recorded, settlement_loss, overage_recorded
             FROM tollgate_accounts WHERE account_id = $1",
        )
        .bind(id_bytes(account.0))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?
        else {
            return Ok(None);
        };
        let lease_row = sqlx::query(ACTIVE_LEASE_SUM_SQL)
            .bind(id_bytes(account.0))
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        let active_grants = to_units(lease_row.get::<i64, _>(0), "active lease grants")?;
        let active_used = to_units(lease_row.get::<i64, _>(1), "active lease usage")?;
        Ok(Some(Conservation {
            deposited: to_units(row.get::<i64, _>(0), "deposited")?,
            overage_recorded: to_units(row.get::<i64, _>(4), "overage_recorded")?,
            balance: to_units(row.get::<i64, _>(1), "balance")?,
            active_lease_grants: active_grants,
            settled_usage: to_units(row.get::<i64, _>(2), "usage_recorded")?
                .checked_sub(active_used)
                .expect("active usage never exceeds recorded usage"),
            settlement_loss: to_units(row.get::<i64, _>(3), "settlement_loss")?,
        }))
    }
}

/// One locked lease row, named so its accounting fields cannot be confused at
/// the call site and tooling does not manufacture a Cartesian product of
/// arbitrary replacements for an anonymous seven-field tuple.
struct LockedLeaseRow {
    account_id: Vec<u8>,
    fencing_token: i64,
    granted: i64,
    used: i64,
    credited: i64,
    expires_at_us: i64,
    state: i16,
}

/// Lock one lease row for a settlement transition.
async fn lock_lease(
    tx: &mut Transaction<'_, Postgres>,
    lease_id: LeaseId,
) -> Result<Option<LockedLeaseRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT account_id, fencing_token, granted, used, credited, expires_at_us, state
         FROM tollgate_leases WHERE lease_id = $1 FOR UPDATE",
    )
    .bind(id_bytes(lease_id.0))
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|row| LockedLeaseRow {
        account_id: row.get(0),
        fencing_token: row.get(1),
        granted: row.get(2),
        used: row.get(3),
        credited: row.get(4),
        expires_at_us: row.get(5),
        state: row.get(6),
    }))
}

#[async_trait]
impl LeaseAllocator for PostgresStore {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError> {
        if ttl <= SignedDuration::ZERO {
            return Err(AllocateError::InvalidTtl);
        }
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let result = async {
            let row = sqlx::query(
                "SELECT balance, status, next_fence FROM tollgate_accounts
                 WHERE account_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(alloc_storage)?
            .ok_or(AllocateError::UnknownAccount)?;

            // Suspended and Closed both refuse, under one deny reason: no
            // client acts on the distinction, and splitting it would widen
            // `AllocateError`'s per-reason tally for nothing (#51).
            if decode_status(row.get::<String, _>(1)).map_err(AllocateError::Storage)?
                != AccountStatus::Active
            {
                return Err(AllocateError::AccountInactive);
            }
            let balance = to_units(row.get::<i64, _>(0), "account balance")
                .map_err(AllocateError::Storage)?;
            let granted = self
                .policy
                .grant(requested, balance)
                .ok_or(AllocateError::InsufficientBalance)?;
            let fence = row.get::<i64, _>(2);
            // Fence counters are seeded at 1 and only incremented; a negative
            // stored value is corruption, never a token to alias to 0.
            let fence_token = u64::try_from(fence).map(FencingToken).map_err(|_| {
                AllocateError::Storage(StoreError(format!(
                    "stored fencing token is negative: {fence}"
                )))
            })?;

            let ttl = if ttl > self.policy.max_ttl {
                self.policy.max_ttl
            } else {
                ttl
            };
            let expires_at = now.checked_add(ttl).map_err(|e| {
                AllocateError::Storage(StoreError(format!("ttl overflow: {e}")))
            })?;
            let lease_id = LeaseId(uuid::Uuid::new_v4().as_u128());

            sqlx::query(
                "UPDATE tollgate_accounts SET balance = balance - $2, next_fence = next_fence + 1
                 WHERE account_id = $1",
            )
            .bind(id_bytes(account.0))
            .bind(to_i64(granted, "grant").map_err(AllocateError::Storage)?)
            .execute(&mut *tx)
            .await
            .map_err(alloc_storage)?;
            sqlx::query(
                "INSERT INTO tollgate_leases
                 (lease_id, account_id, fencing_token, granted, used, credited, expires_at_us, state)
                 VALUES ($1, $2, $3, $4, 0, 0, $5, 0)",
            )
            .bind(id_bytes(lease_id.0))
            .bind(id_bytes(account.0))
            .bind(fence)
            .bind(to_i64(granted, "grant").map_err(AllocateError::Storage)?)
            .bind(ts_micros(expires_at))
            .execute(&mut *tx)
            .await
            .map_err(alloc_storage)?;

            Ok(LeaseGrant {
                lease_id,
                account_id: account,
                fencing_token: fence_token,
                units: granted,
                expires_at,
            })
        }
        .await;
        finish_transaction(tx, result).await
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError> {
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let result = async {
            let LockedLeaseRow {
                account_id,
                fencing_token: fence,
                granted,
                used,
                credited: _credited,
                expires_at_us,
                state,
            } = lock_lease(&mut tx, lease_id)
                .await
                .map_err(alloc_storage)?
                .ok_or(AllocateError::UnknownLease)?;

            let stored_fence = u64::try_from(fence).map_err(|_| {
                AllocateError::Storage(StoreError(format!(
                    "stored fencing token is negative: {fence}"
                )))
            })?;
            if stored_fence != fencing_token.0 {
                return Err(AllocateError::Fenced);
            }
            // Releases are accepted through the grace window (see GrantPolicy::
            // reclaim_grace) — only a settled or grace-exhausted lease refuses.
            let release_deadline_us = expires_at_us.saturating_add(self.reclaim_grace_us);
            if state != STATE_ACTIVE || ts_micros(now) >= release_deadline_us {
                return Err(AllocateError::LeaseNotActive);
            }
            let unspent_i = to_i64(unspent, "unspent").map_err(AllocateError::Storage)?;
            let loss = granted
                .checked_sub(
                    used.checked_add(unspent_i)
                        .ok_or(AllocateError::InvalidRelease)?,
                )
                .filter(|l| *l >= 0)
                .ok_or(AllocateError::InvalidRelease)?;

            sqlx::query("UPDATE tollgate_leases SET state = $2, credited = $3 WHERE lease_id = $1")
                .bind(id_bytes(lease_id.0))
                .bind(STATE_RELEASED)
                .bind(unspent_i)
                .execute(&mut *tx)
                .await
                .map_err(alloc_storage)?;
            sqlx::query(
                "UPDATE tollgate_accounts
                 SET balance = balance + $2, settlement_loss = settlement_loss + $3
                 WHERE account_id = $1",
            )
            .bind(account_id)
            .bind(unspent_i)
            .bind(loss)
            .execute(&mut *tx)
            .await
            .map_err(alloc_storage)?;
            Ok(())
        }
        .await;
        finish_transaction(tx, result).await
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        let limit_i = i64::try_from(limit.get())
            .map_err(|_| StoreError(format!("reclaim batch limit exceeds i64 range: {limit}")))?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // Reclaim only once the grace window past expiry has fully lapsed:
            // expires_at + grace <= now  ⟺  expires_at_us <= now_us - grace_us.
            let threshold_us = ts_micros(now).saturating_sub(self.reclaim_grace_us);
            // SKIP LOCKED lets concurrent sweepers cooperate; the limit keeps
            // both the lease locks and the transaction's row work bounded.
            let rows = sqlx::query(
                "SELECT lease_id, account_id, granted, used FROM tollgate_leases
                 WHERE state = 0 AND expires_at_us <= $1
                 ORDER BY account_id, lease_id LIMIT $2 FOR UPDATE SKIP LOCKED",
            )
            .bind(threshold_us)
            .bind(limit_i)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;

            let mut reclaimed = Vec::with_capacity(rows.len());
            let mut lease_ids = Vec::with_capacity(rows.len());
            let mut lease_credits = Vec::with_capacity(rows.len());
            let mut account_credits: std::collections::BTreeMap<Vec<u8>, i64> =
                std::collections::BTreeMap::new();
            for row in rows {
                let lease_bytes: Vec<u8> = row.get(0);
                let account_bytes: Vec<u8> = row.get(1);
                let granted = row.get::<i64, _>(2);
                let used = row.get::<i64, _>(3);
                let credit = granted.checked_sub(used).ok_or_else(|| {
                    StoreError(format!(
                        "reclaim credit overflow: granted {granted}, used {used}"
                    ))
                })?;
                // Validate the whole batch before either set-wise UPDATE: a
                // negative credit is corruption, and paying it would debit the
                // account rather than returning quota.
                let credit_units = to_units(credit, "reclaim credit")?;
                let account_credit = account_credits.entry(account_bytes.clone()).or_default();
                *account_credit = account_credit.checked_add(credit).ok_or_else(|| {
                    StoreError(format!(
                        "reclaim credit sum overflow for account {:#034x}",
                        id_from(&account_bytes)
                    ))
                })?;
                lease_ids.push(lease_bytes.clone());
                lease_credits.push(credit);
                reclaimed.push(ReclaimedLease {
                    lease_id: LeaseId(id_from(&lease_bytes)),
                    account_id: AccountId(id_from(&account_bytes)),
                    reclaimed: credit_units,
                });
            }

            let batch = ReclaimBatch::try_new(reclaimed, limit)?;
            if batch.is_empty() {
                return Ok(batch);
            }

            let (account_ids, account_credit_deltas): (Vec<_>, Vec<_>) =
                account_credits.into_iter().unzip();
            let expected_lease_rows = u64::try_from(lease_ids.len())
                .map_err(|_| StoreError("reclaim lease row count exceeds u64 range".into()))?;
            let expected_account_rows = u64::try_from(account_ids.len())
                .map_err(|_| StoreError("reclaim account row count exceeds u64 range".into()))?;

            // A set-wise UPDATE does not promise row-lock order. Lock every
            // affected account explicitly in byte-sorted order first, matching
            // release and ingest's lease-then-account order and preventing
            // concurrent multi-account sweeps from forming a deadlock cycle.
            let locked_accounts = sqlx::query(
                "SELECT account_id FROM tollgate_accounts
                 WHERE account_id = ANY($1) ORDER BY account_id FOR UPDATE",
            )
            .bind(&account_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            if locked_accounts.len() != account_ids.len() {
                return Err(StoreError(format!(
                    "reclaim locked {} of {} referenced account rows",
                    locked_accounts.len(),
                    account_ids.len()
                )));
            }

            let updated_leases = sqlx::query(
                "UPDATE tollgate_leases AS lease
                 SET state = $3, credited = delta.credit
                 FROM UNNEST($1::bytea[], $2::bigint[]) AS delta(lease_id, credit)
                 WHERE lease.lease_id = delta.lease_id AND lease.state = $4",
            )
            .bind(&lease_ids)
            .bind(&lease_credits)
            .bind(STATE_EXPIRED)
            .bind(STATE_ACTIVE)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            if updated_leases.rows_affected() != expected_lease_rows {
                return Err(StoreError(format!(
                    "reclaim updated {} of {} locked lease rows",
                    updated_leases.rows_affected(),
                    lease_ids.len()
                )));
            }

            let updated_accounts = sqlx::query(
                "UPDATE tollgate_accounts AS account
                 SET balance = account.balance + delta.credit
                 FROM UNNEST($1::bytea[], $2::bigint[]) AS delta(account_id, credit)
                 WHERE account.account_id = delta.account_id",
            )
            .bind(&account_ids)
            .bind(&account_credit_deltas)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            if updated_accounts.rows_affected() != expected_account_rows {
                return Err(StoreError(format!(
                    "reclaim updated {} of {} locked account rows",
                    updated_accounts.rows_affected(),
                    account_ids.len()
                )));
            }

            Ok(batch)
        }
        .await;
        finish_transaction(tx, result).await
    }
}

#[async_trait]
impl UsageSink for PostgresStore {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        // One transaction per *batch* (review finding #8): leases are locked
        // in a single sorted ANY() query (sorted to keep concurrent batches
        // deadlock-free), duplicates are detected with one lookup, events are
        // classified in memory against the locked rows, and the accepted set
        // lands via one bulk insert plus set-wise per-lease/per-account
        // updates. Classification in application code preserves the partial
        // acceptance contract without savepoints.
        let mut report = IngestReport::default();
        if events.is_empty() {
            return Ok(report);
        }

        // Encode each ID and timestamp exactly once before opening the
        // transaction. The prepared values are reused by the lock, dedup,
        // classification, insert, and aggregate-update phases. Units are
        // checked once later, after duplicate and capability classification, to
        // preserve the partial-acceptance ordering.
        struct PreparedEvent<'a> {
            event: &'a UsageEvent,
            request_id: Vec<u8>,
            account_id: Vec<u8>,
            /// `None` for overage, which names no lease. Every phase below
            /// keys off this rather than re-matching on the source, so a lease
            /// id can never be conjured for an event that has none.
            lease_id: Option<Vec<u8>>,
            occurred_at_us: i64,
        }
        let prepared: Vec<PreparedEvent<'_>> = events
            .iter()
            .map(|event| {
                Ok(PreparedEvent {
                    event,
                    request_id: id_bytes(event.request_id.0),
                    account_id: id_bytes(event.account_id.0),
                    lease_id: event.source.lease_id().map(|id| id_bytes(id.0)),
                    occurred_at_us: ts_micros(event.occurred_at),
                })
            })
            .collect::<Result<_, StoreError>>()?;

        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // Lock every referenced lease in the same global account/lease
            // order as reclaim. Release touches one lease, so every
            // lease-writing transaction now agrees on this order.
            let lease_ids: Vec<Vec<u8>> = prepared
                .iter()
                .filter_map(|event| event.lease_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            struct LeaseRow {
                account_id: Vec<u8>,
                fence: i64,
                granted: i64,
                used: i64,
                used_delta: Option<NonZeroI64>,
                credited: i64,
                settled: bool,
            }
            let rows = sqlx::query(
                "SELECT lease_id, account_id, fencing_token, granted, used, credited, state
                 FROM tollgate_leases
                 WHERE lease_id = ANY($1)
                 ORDER BY account_id, lease_id FOR UPDATE",
            )
            .bind(&lease_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            let mut leases: std::collections::BTreeMap<Vec<u8>, LeaseRow> =
                std::collections::BTreeMap::new();
            for row in rows {
                let lease_id: Vec<u8> = row.get(0);
                let fence: i64 = row.get(2);
                let granted: i64 = row.get(3);
                let used: i64 = row.get(4);
                let credited: i64 = row.get(5);
                leases.insert(
                    lease_id,
                    LeaseRow {
                        account_id: row.get(1),
                        fence,
                        granted,
                        used,
                        used_delta: None,
                        credited,
                        settled: row.get::<i16, _>(6) != STATE_ACTIVE,
                    },
                );
            }

            // Existing request ids in one lookup.
            let request_ids: Vec<Vec<u8>> = prepared
                .iter()
                .map(|event| event.request_id.clone())
                .collect();
            let mut seen: std::collections::HashSet<Vec<u8>> = sqlx::query(
                "SELECT request_id FROM tollgate_usage_events WHERE request_id = ANY($1)",
            )
            .bind(&request_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?
            .into_iter()
            .map(|row| row.get::<Vec<u8>, _>(0))
            .collect();

            // Which accounts referenced by *overage* events actually exist.
            //
            // A leased event proves its account exists by resolving a lease
            // row, whose `account_id` is a foreign key. An overage event has
            // no such proof, and the memory backend rejects one naming an
            // unknown account -- so this backend must too, or the two
            // classify the same batch differently.
            //
            // Read without `FOR UPDATE`, deliberately. The accounts these
            // events touch are locked in account order further down, after
            // the leases, and taking that lock here instead would invert the
            // lease-then-account order every writing transaction agrees on.
            // A row that vanished between this probe and that lock would fail
            // the lock's own count check and roll the batch back, which is the
            // fail-closed outcome; nothing in this store deletes accounts.
            let overage_account_ids: Vec<Vec<u8>> = prepared
                .iter()
                .filter(|event| event.lease_id.is_none())
                .map(|event| event.account_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let known_overage_accounts: std::collections::HashSet<Vec<u8>> =
                if overage_account_ids.is_empty() {
                    std::collections::HashSet::new()
                } else {
                    sqlx::query(
                        "SELECT account_id FROM tollgate_accounts WHERE account_id = ANY($1)",
                    )
                    .bind(&overage_account_ids)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(storage)?
                    .into_iter()
                    .map(|row| row.get::<Vec<u8>, _>(0))
                    .collect()
                };

            // Classify in memory against the locked rows (identical rules to
            // MemoryStore: capability triple, then the conservation fit that
            // also converts a released lease's provisional loss into billed usage).
            struct Accepted {
                event_index: usize,
                settled: bool,
                /// The lease's stored fence, already validated non-negative;
                /// acceptance required the event's token to equal it, so this
                /// is the event's fence in storage form with no reconversion.
                /// `None` for overage, whose row carries no capability.
                fence: Option<i64>,
                /// True when these units were extended as unfunded credit, so
                /// they fund the ledger as well as bill it.
                overage: bool,
                /// Checked once during classification and reused by every
                /// aggregate and insert array.
                units: i64,
            }
            let mut accepted: Vec<Accepted> = Vec::with_capacity(prepared.len());
            for (event_index, event) in prepared.iter().enumerate() {
                if seen.contains(event.request_id.as_slice()) {
                    report.duplicate += 1;
                    continue;
                }
                let Some(lease_key) = event.lease_id.as_deref() else {
                    // Overage: no capability to verify and no lease capacity
                    // to fit inside, so the only question is whether the
                    // account exists. It is accepted regardless of the
                    // account's current enforcement mode -- the ledger does
                    // not record which mode a request was admitted under, and
                    // discarding a charge because the account was switched
                    // back to `Strict` after the work ran would be fail-open
                    // on accounting.
                    if !known_overage_accounts.contains(event.account_id.as_slice()) {
                        report.rejected += 1;
                        continue;
                    }
                    let units = to_i64(event.event.units, "units")?;
                    seen.insert(event.request_id.clone());
                    accepted.push(Accepted {
                        event_index,
                        // Overage belongs to no lease, so there is no
                        // provisional settlement loss for it to convert and
                        // it is settled the moment it is recorded.
                        settled: false,
                        fence: None,
                        overage: true,
                        units,
                    });
                    report.accepted += 1;
                    continue;
                };
                let Some(lease) = leases.get_mut(lease_key) else {
                    report.rejected += 1;
                    continue;
                };
                let stored_fence = u64::try_from(lease.fence).map_err(|_| {
                    StoreError(format!(
                        "stored fencing token is negative: {}",
                        lease.fence
                    ))
                })?;
                if Some(stored_fence) != event.event.source.fencing_token().map(|token| token.0)
                    || lease.account_id.as_slice() != event.account_id.as_slice()
                {
                    report.rejected += 1;
                    continue;
                }
                let units = to_i64(event.event.units, "units")?;
                to_units(lease.granted, "lease granted")?;
                to_units(lease.used, "lease used")?;
                to_units(lease.credited, "lease credited")?;
                let committed = lease
                    .used
                    .checked_add(lease.used_delta.map_or(0, NonZeroI64::get))
                    .and_then(|used| used.checked_add(lease.credited))
                    .ok_or_else(|| {
                        StoreError(format!(
                            "lease accounting overflow for {:#034x}",
                            id_from(lease_key)
                        ))
                    })?;
                let remaining = lease.granted.checked_sub(committed).ok_or_else(|| {
                    StoreError(format!(
                        "lease accounting exceeds grant for {:#034x}: granted {}, committed {committed}",
                        id_from(lease_key), lease.granted
                    ))
                })?;
                if units > remaining {
                    report.rejected += 1;
                    continue;
                }
                let used_delta = lease
                    .used_delta
                    .map_or(0, NonZeroI64::get)
                    .checked_add(units)
                    .ok_or_else(|| {
                        StoreError(format!(
                            "lease usage delta overflow for {:#034x}",
                            id_from(lease_key)
                        ))
                    })?;
                lease.used_delta = NonZeroI64::new(used_delta);
                seen.insert(event.request_id.clone());
                accepted.push(Accepted {
                    event_index,
                    settled: lease.settled,
                    fence: Some(lease.fence),
                    overage: false,
                    units,
                });
                report.accepted += 1;
            }

            if accepted.is_empty() {
                return Ok(report);
            }

            #[derive(Default)]
            struct AccountDelta {
                usage: i64,
                loss: i64,
                /// Units that fund themselves: overage bills and funds in the
                /// same transaction, so the equation closes by construction
                /// rather than by a later reconciliation step.
                overage: i64,
            }

            // Bulk insert the accepted events.
            let (mut rid, mut acct, mut lease, mut fence, mut units, mut at) = (
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
            );
            let mut account_deltas: std::collections::BTreeMap<Vec<u8>, AccountDelta> =
                std::collections::BTreeMap::new();
            for accepted_event in &accepted {
                let event = &prepared[accepted_event.event_index];
                rid.push(event.request_id.clone());
                acct.push(event.account_id.clone());
                lease.push(event.lease_id.clone());
                fence.push(accepted_event.fence);
                units.push(accepted_event.units);
                at.push(event.occurred_at_us);
                if accepted_event.overage {
                    debug_assert!(
                        event.lease_id.is_none() && accepted_event.fence.is_none(),
                        "an overage row must carry neither half of a capability"
                    );
                }

                let entry = account_deltas.entry(event.account_id.clone()).or_default();
                entry.usage = entry.usage.checked_add(accepted_event.units).ok_or_else(|| {
                    StoreError(format!(
                        "usage delta overflow for account {:#034x}",
                        event.event.account_id.0
                    ))
                })?;
                if accepted_event.settled {
                    entry.loss = entry.loss.checked_add(accepted_event.units).ok_or_else(|| {
                        StoreError(format!(
                            "settlement loss delta overflow for account {:#034x}",
                            event.event.account_id.0
                        ))
                    })?;
                }
                if accepted_event.overage {
                    entry.overage =
                        entry.overage.checked_add(accepted_event.units).ok_or_else(|| {
                            StoreError(format!(
                                "overage delta overflow for account {:#034x}",
                                event.event.account_id.0
                            ))
                        })?;
                }
            }
            let inserted = sqlx::query(
                "INSERT INTO tollgate_usage_events
                 (request_id, account_id, lease_id, fencing_token, units, occurred_at_us)
                 SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::bytea[], $4::bigint[], $5::bigint[], $6::bigint[])",
            )
            .bind(&rid)
            .bind(&acct)
            .bind(&lease)
            .bind(&fence)
            .bind(&units)
            .bind(&at)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            let expected_event_rows = u64::try_from(accepted.len())
                .map_err(|_| StoreError("accepted event count exceeds u64 range".into()))?;
            if inserted.rows_affected() != expected_event_rows {
                return Err(StoreError(format!(
                    "ingest inserted {} of {} accepted usage rows",
                    inserted.rows_affected(),
                    accepted.len()
                )));
            }

            // Every lease row was already locked by the sorted SELECT above,
            // so one set-wise statement can apply the grouped usage deltas
            // without adding a round trip per distinct lease.
            let (lease_update_ids, lease_used_deltas): (Vec<Vec<u8>>, Vec<i64>) = leases
                .iter()
                .filter_map(|(lease_id, row)| {
                    row.used_delta
                        .map(|used_delta| (lease_id.clone(), used_delta.get()))
                })
                .unzip();
            if !lease_update_ids.is_empty() {
                let updated_leases = sqlx::query(
                    "UPDATE tollgate_leases AS lease
                     SET used = lease.used + delta.used
                     FROM UNNEST($1::bytea[], $2::bigint[]) AS delta(lease_id, used)
                     WHERE lease.lease_id = delta.lease_id",
                )
                .bind(&lease_update_ids)
                .bind(&lease_used_deltas)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                let expected_lease_rows = u64::try_from(lease_update_ids.len())
                    .map_err(|_| StoreError("ingest lease row count exceeds u64 range".into()))?;
                if updated_leases.rows_affected() != expected_lease_rows {
                    return Err(StoreError(format!(
                        "ingest updated {} of {} locked lease rows",
                        updated_leases.rows_affected(),
                        lease_update_ids.len()
                    )));
                }
            }

            let mut account_ids = Vec::with_capacity(account_deltas.len());
            let mut account_usage_deltas = Vec::with_capacity(account_deltas.len());
            let mut account_loss_deltas = Vec::with_capacity(account_deltas.len());
            let mut account_overage_deltas = Vec::with_capacity(account_deltas.len());
            for (account_id, delta) in &account_deltas {
                account_ids.push(account_id.clone());
                account_usage_deltas.push(delta.usage);
                account_loss_deltas.push(delta.loss);
                account_overage_deltas.push(delta.overage);
            }

            // A set-wise UPDATE does not promise row-lock order. Lock every
            // affected account explicitly in byte order first. BTreeMap made
            // account_ids sorted, preserving ingest's lease-then-account
            // order and preventing concurrent multi-account batches from
            // forming a deadlock cycle.
            let locked_accounts = sqlx::query(
                "SELECT account_id, usage_recorded, settlement_loss, overage_recorded
                 FROM tollgate_accounts
                 WHERE account_id = ANY($1)
                 ORDER BY account_id FOR UPDATE",
            )
            .bind(&account_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            if locked_accounts.len() != account_ids.len() {
                return Err(StoreError(format!(
                    "ingest locked {} of {} referenced account rows",
                    locked_accounts.len(),
                    account_ids.len()
                )));
            }

            // Validate every account before the set-wise mutation. The
            // per-lease fit check bounds each straggler by the provisional
            // loss its own release recorded, so a short account loss means
            // ledger corruption and the entire transaction must roll back.
            for row in locked_accounts {
                let account_id: Vec<u8> = row.get(0);
                let usage_recorded: i64 = row.get(1);
                let settlement_loss: i64 = row.get(2);
                let overage_recorded: i64 = row.get(3);
                to_units(usage_recorded, "account usage_recorded")?;
                to_units(settlement_loss, "account settlement_loss")?;
                to_units(overage_recorded, "account overage_recorded")?;
                let delta = account_deltas.get(&account_id).ok_or_else(|| {
                    StoreError(format!(
                        "ingest locked unexpected account {:#034x}",
                        id_from(&account_id)
                    ))
                })?;
                usage_recorded.checked_add(delta.usage).ok_or_else(|| {
                    StoreError(format!(
                        "usage_recorded overflow for account {:#034x}",
                        id_from(&account_id)
                    ))
                })?;
                // The funding half of the same units. Both terms must be
                // representable or neither may move, or the batch would bill
                // overage it did not fund and leave the equation open.
                overage_recorded.checked_add(delta.overage).ok_or_else(|| {
                    StoreError(format!(
                        "overage_recorded overflow for account {:#034x}",
                        id_from(&account_id)
                    ))
                })?;
                if settlement_loss < delta.loss {
                    return Err(StoreError(format!(
                        "settlement_loss underflow for account {:#034x}: settled straggler \
                         usage {} exceeds recorded loss",
                        id_from(&account_id),
                        delta.loss
                    )));
                }
            }

            let updated_accounts = sqlx::query(
                "UPDATE tollgate_accounts AS account
                 SET usage_recorded = account.usage_recorded + delta.usage,
                     settlement_loss = account.settlement_loss - delta.loss,
                     overage_recorded = account.overage_recorded + delta.overage
                 FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[])
                      AS delta(account_id, usage, loss, overage)
                 WHERE account.account_id = delta.account_id
                   AND account.settlement_loss >= delta.loss",
            )
            .bind(&account_ids)
            .bind(&account_usage_deltas)
            .bind(&account_loss_deltas)
            .bind(&account_overage_deltas)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            let expected_account_rows = u64::try_from(account_ids.len())
                .map_err(|_| StoreError("ingest account row count exceeds u64 range".into()))?;
            if updated_accounts.rows_affected() != expected_account_rows {
                return Err(StoreError(format!(
                    "ingest updated {} of {} locked account rows",
                    updated_accounts.rows_affected(),
                    account_ids.len()
                )));
            }

            Ok(report)
        }
        .await;
        finish_transaction(tx, result).await
    }
}

/// Decode a stored status column into the enum, refusing anything the
/// vocabulary does not contain.
///
/// Never defaults to `Active`. A value the `CHECK` constraint should have made
/// impossible means the row was written outside this code, and admitting it as
/// "active" would turn corruption into service (the rule INVARIANTS.md #11
/// applies to the ledger's numbers, applied to its status).
fn decode_status(stored: String) -> Result<AccountStatus, StoreError> {
    match stored.as_str() {
        s if s == AccountStatus::Active.as_str() => Ok(AccountStatus::Active),
        s if s == AccountStatus::Suspended.as_str() => Ok(AccountStatus::Suspended),
        s if s == AccountStatus::Closed.as_str() => Ok(AccountStatus::Closed),
        other => Err(StoreError(format!("unrecognized account status {other:?}"))),
    }
}

/// Decode one stored snapshot row into the publication proof.
///
/// Shared by `SnapshotSource::snapshot` and the status republish so the latter
/// reads back what it wrote through exactly the reader's path -- a row this
/// refuses is one the request path would refuse too, and finding that out at
/// write time is the point.
///
/// Since #54 that is literally true of the generation as well: both callers
/// hand this the row's `generation` column, so a live read and a republish
/// resolve it identically. Before, the JSON carried a second copy that only the
/// live path consulted.
fn decode_publishable(
    principal: Principal,
    generation: i64,
    value: serde_json::Value,
) -> Result<PublishableSnapshot, StoreError> {
    let generation = generation_from(generation)?;
    let snapshot: StoredSnapshot =
        serde_json::from_value(value).map_err(|e| StoreError(format!("snapshot decode: {e}")))?;
    PublishableSnapshot::try_new(Arc::new(snapshot.into_snapshot(generation))).map_err(|error| {
        StoreError(format!(
            "invalid stored snapshot for principal {:#034x}: {error}",
            principal.0
        ))
    })
}

/// Read a stored generation column.
///
/// Takes the raw `i64` and converts here rather than at each call site, so
/// every caller shares one refusal. That matters for the republish
/// specifically: it must not fail an account-wide status change over one
/// unreadable row, and a caller-side conversion is one `?` away from doing
/// exactly that.
///
/// A negative value is corruption to surface, never to clamp (INVARIANTS #11).
/// Migration 0007's CHECK is what keeps it from being written in the first
/// place; this is the read-side backstop.
fn generation_from(column: i64) -> Result<Generation, StoreError> {
    u64::try_from(column)
        .map(Generation)
        .map_err(|_| StoreError("stored snapshot generation is negative".into()))
}

#[async_trait]
impl SnapshotSource for PostgresStore {
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        let row = sqlx::query(
            "SELECT generation, snapshot, deleted FROM tollgate_snapshots WHERE principal = $1",
        )
        .bind(id_bytes(principal.0))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        match row {
            // Both branches now resolve the generation from the same column.
            // They did not before: the tombstone read it here while a live read
            // decoded a second copy out of the JSON, so one row could answer
            // two different generations depending on which branch you reached
            // (#54).
            Some(row) if row.get::<bool, _>(2) => Ok(SnapshotResolution::Revoked {
                generation: generation_from(row.get::<i64, _>(0))?,
            }),
            Some(row) => Ok(SnapshotResolution::Present(decode_publishable(
                principal,
                row.get::<i64, _>(0),
                row.get(1),
            )?)),
            None => Ok(SnapshotResolution::Unknown),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }

    /// Tombstones included, for the reason `MemoryStore::principals` gives:
    /// forgetting a revoked principal is how one gets resurrected.
    ///
    /// A primary-key scan, so no new index — the table holds one row per
    /// principal, not per request, and `ORDER BY` makes the result stable so
    /// a caller diffing two enumerations sees real changes rather than
    /// storage order.
    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        let rows = sqlx::query("SELECT principal FROM tollgate_snapshots ORDER BY principal")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        Ok(Some(
            rows.iter()
                .map(|row| Principal(id_from(row.get::<Vec<u8>, _>(0).as_slice())))
                .collect(),
        ))
    }
}

#[async_trait]
impl StoreHealth for PostgresStore {
    async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }
}

#[async_trait]
impl AdminStore for PostgresStore {
    async fn create_account(&self, config: AccountConfig) -> Result<(), CreateAccountError> {
        let result = sqlx::query(
            "INSERT INTO tollgate_accounts
             (account_id, balance, deposited, status, next_fence, usage_recorded,
              settlement_loss, overage_recorded)
             VALUES ($1, $2, $2, $3, 1, 0, 0, 0)
             ON CONFLICT (account_id) DO NOTHING",
        )
        .bind(id_bytes(config.account_id.0))
        .bind(to_i64(config.initial_balance, "balance").map_err(CreateAccountError::Storage)?)
        .bind(config.status.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| CreateAccountError::Storage(storage(e)))?;
        if result.rows_affected() == 0 {
            return Err(CreateAccountError::AlreadyExists);
        }
        Ok(())
    }

    async fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError> {
        let result = sqlx::query(
            "UPDATE tollgate_accounts
             SET balance = balance + $2, deposited = deposited + $2
             WHERE account_id = $1",
        )
        .bind(id_bytes(account.0))
        .bind(to_i64(units, "deposit").map_err(AllocateError::Storage)?)
        .execute(&self.pool)
        .await
        .map_err(alloc_storage)?;
        if result.rows_affected() == 0 {
            return Err(AllocateError::UnknownAccount);
        }
        Ok(())
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<StatusChange, SetStatusError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // The account row first, and its lock is the serialization point:
            // two concurrent status changes cannot interleave their snapshot
            // updates, so "ledger says Active, snapshots say Suspended" is
            // unrepresentable rather than merely unlikely.
            let row = sqlx::query(
                "SELECT status FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(SetStatusError::UnknownAccount)?;

            if decode_status(row.get::<String, _>(0))? == AccountStatus::Closed
                && status != AccountStatus::Closed
            {
                // Terminal, and the refusal changes nothing: no ledger write,
                // no generation bump. Rolling back here is what makes that so.
                return Err(SetStatusError::AccountClosed);
            }

            sqlx::query("UPDATE tollgate_accounts SET status = $2 WHERE account_id = $1")
                .bind(id_bytes(account.0))
                .bind(status.as_str())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;

            // `jsonb_set` rather than read-modify-write in Rust, for three
            // reasons any one of which decides it:
            //
            // 1. RMW reintroduces this issue's own bug. A concurrent
            //    `publish_snapshot` landing between the read and the write
            //    makes `generation + 1` no longer greater than stored, and the
            //    monotonic guard then *silently drops the status change* for
            //    that principal.
            // 2. RMW loses fields. `StoredSnapshot` has no `flatten`, so
            //    decoding and re-serialising a row written by a newer binary
            //    discards what this one does not know about.
            // 3. RMW fails whole on one bad row. A safety operation must not
            //    be blockable by one unrelated corrupt credential.
            //
            // Only `status` is patched into the JSON. The generation lives in
            // the column alone (#54), so this no longer has to keep a second
            // copy in step -- and `RETURNING generation` is what carries the
            // new value out to the push.
            //
            // `deleted = FALSE` leaves tombstones alone: republishing one
            // would resurrect a revoked principal (INVARIANTS.md #15), and
            // revocation stays its own per-credential mechanism.
            // `IS DISTINCT FROM` makes a repeat converge, bumping nothing.
            let rows = sqlx::query(
                "UPDATE tollgate_snapshots
                    SET generation = generation + 1,
                        snapshot   = jsonb_set(snapshot, '{status}', to_jsonb($2::text))
                  WHERE account_id = $1
                    AND deleted = FALSE
                    AND snapshot ->> 'status' IS DISTINCT FROM $2::text
                RETURNING principal, generation, snapshot",
            )
            .bind(id_bytes(account.0))
            .bind(status.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;

            let mut republished = Vec::with_capacity(rows.len());
            let mut unreadable = 0usize;
            for row in rows {
                let principal = Principal(id_from(row.get::<Vec<u8>, _>(0).as_slice()));
                match decode_publishable(
                    principal,
                    row.get::<i64, _>(1),
                    row.get::<serde_json::Value, _>(2),
                ) {
                    Ok(snapshot) => republished.push((principal, snapshot)),
                    // Skipped for push, not fatal: this row was already
                    // unreadable before the status change touched it, and
                    // refusing to suspend an account because one of its
                    // credentials is corrupt is the worse outcome. Reported,
                    // never silent (INVARIANTS.md #19).
                    // Counted as well as logged: the row changed durably but
                    // will not be pushed, so those principals converge only at
                    // their next refresh. Reported to the caller rather than
                    // absorbed into a log line nobody is reading.
                    Err(error) => {
                        unreadable += 1;
                        tracing::warn!(
                            %principal,
                            %error,
                            "restamped snapshot could not be decoded for push"
                        );
                    }
                }
            }
            // Ordered, so both backends emit the same sequence and a mirrored
            // test need not assert on incidental ordering.
            republished.sort_unstable_by_key(|(principal, _)| *principal);
            Ok((republished, unreadable))
        }
        .await;

        let (republished, unreadable) = finish_transaction(tx, result).await?;
        // After commit. `publish_snapshot` gets away with pushing before only
        // because it is a single autocommit statement.
        if pushes_exceed_capacity(republished.len()) {
            tracing::warn!(
                %account,
                principals = republished.len(),
                capacity = PUSH_CHANNEL_CAPACITY,
                "status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(StatusChange {
            // Rows that changed durably: the ones pushed, plus any that could
            // not be decoded to push. The caller is told both numbers.
            republished: count + unreadable,
            unreadable,
        })
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<(), PublishSnapshotError> {
        let generation = i64::try_from(snapshot.generation.0).map_err(|_| {
            StoreError("snapshot generation exceeds PostgreSQL BIGINT range".into())
        })?;
        let value = serde_json::to_value(StoredSnapshotRef::from(snapshot.as_snapshot()))
            .map_err(|e| StoreError(format!("snapshot encode: {e}")))?;

        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // The ledger decides an account's status; a publish may carry it
            // but not change it, or the two records `set_account_status`
            // unified could be pulled apart again one principal at a time
            // (#51). FOR SHARE, not FOR UPDATE: this only has to hold the
            // status still, and a status change takes FOR UPDATE on the same
            // row, so the two serialize without publishes blocking each other.
            let ledger =
                sqlx::query("SELECT status FROM tollgate_accounts WHERE account_id = $1 FOR SHARE")
                    .bind(id_bytes(snapshot.account_id.0))
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            // An account the ledger does not hold publishes unchanged: this
            // adds no account-existence requirement to publication.
            if let Some(row) = ledger {
                let ledger = decode_status(row.get::<String, _>(0))?;
                if ledger != snapshot.status {
                    return Err(PublishSnapshotError::StatusMismatch {
                        ledger,
                        submitted: snapshot.status,
                    });
                }
            }

            let result = sqlx::query(
                "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
                 VALUES ($1, $2, $3, FALSE)
                 ON CONFLICT (principal) DO UPDATE
                 SET generation = EXCLUDED.generation, snapshot = EXCLUDED.snapshot,
                     deleted = FALSE
                 WHERE tollgate_snapshots.generation < EXCLUDED.generation",
            )
            .bind(id_bytes(principal.0))
            .bind(generation)
            .bind(value)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            Ok(result.rows_affected() > 0)
        }
        .await;

        if finish_transaction(tx, result).await? {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(())
    }

    async fn remove_snapshot(&self, principal: Principal) -> Result<(), StoreError> {
        let row = sqlx::query(
            "UPDATE tollgate_snapshots SET deleted = TRUE
             WHERE principal = $1 AND deleted = FALSE RETURNING generation",
        )
        .bind(id_bytes(principal.0))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        if let Some(row) = row {
            let generation = generation_from(row.get::<i64, _>(0))?;
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Revoked { generation },
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A readiness probe is evidence that PostgreSQL answered, not merely that
    /// a store object exists (INVARIANTS.md #19).
    #[tokio::test]
    async fn ping_surfaces_a_closed_pool() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/tollgate")
            .unwrap();
        pool.close().await;
        let (push, _) = broadcast::channel(1);
        let store = PostgresStore {
            pool,
            policy: GrantPolicy::default(),
            reclaim_grace_us: 0,
            push,
        };

        assert!(store.ping().await.is_err());
    }
}
