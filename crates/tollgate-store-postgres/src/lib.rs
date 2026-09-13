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
//! - lease expiry as floor `BIGINT` microseconds plus a `SMALLINT` nanosecond
//!   remainder; other timestamps as `BIGINT` microseconds since the Unix epoch;
//! - snapshots as storage-local `JSONB`: ids in the legacy u64 range remain
//!   numeric for rollback, larger ids use canonical text, and the public
//!   HTTP/Serde contract always uses text.
//!
//! Snapshot pushes broadcast in-process only; cross-process push
//! (LISTEN/NOTIFY or the server's future SSE) is a documented seam in
//! `docs/DESIGN.md`.

mod lease_time;

use lease_time::LeaseInstant;
use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, BudgetSchedule, BudgetView, CapacityClass,
    CostTable, CostUnits, EnforcementMode, FencingToken, Generation, KeyId, LeaseGrant, LeaseId,
    Period, PermissionBits, PolicyRevision, Principal, PublishableSnapshot, ResolvedLimits,
    Rollover, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AdminReceipt, AdminState, AdminStore, AllocateError, BudgetError, Conservation,
    CreateAccountError, GrantPolicy, IngestError, IngestReport, KeyDirectory, KeyError, KeyRecord,
    LeaseAllocator, PUSH_CHANNEL_CAPACITY, PublishSnapshotError, ReclaimBatch, ReclaimedLease,
    Revocation, RolledAccount, RolloverBatch, SetStatusError, SnapshotPush, SnapshotResolution,
    SnapshotSource, StatusChange, StoreError, StoreHealth, UsageSink, pushes_exceed_capacity,
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
    async fn credential_activity(
        &self,
        keys: &[KeyId],
    ) -> Result<Vec<tollgate_store::CredentialActivity>, StoreError> {
        use tollgate_store::{CredentialActivity, CredentialActivityState};
        let mut result = Vec::with_capacity(keys.len());
        // Reuse the bulk-work budget, not a total operator-list cap. Each
        // input has one row, including duplicates and missing credentials.
        for chunk in keys.chunks(tollgate_store::MAX_INGEST_BATCH) {
            let ids: Vec<_> = chunk.iter().map(|id| id_bytes(id.0)).collect();
            let rows = sqlx::query(
                // Each lateral lookup is bounded by the credential PK. The
                // explicit one-row bound prevents a bulk join from choosing
                // a catalogue-wide hash scan for a moderately sized directory.
                "SELECT evidence.key_id IS NOT NULL, evidence.last_committed_at_us
                 FROM UNNEST($1::bytea[]) WITH ORDINALITY AS requested(key_id, ordinal)
                 LEFT JOIN LATERAL (
                    SELECT k.key_id, a.last_committed_at_us
                    FROM tollgate_credential_keys k
                    LEFT JOIN tollgate_credential_activity a ON a.key_id = k.key_id
                    WHERE k.key_id = requested.key_id LIMIT 1
                 ) AS evidence ON true
                 ORDER BY requested.ordinal",
            )
            .bind(&ids)
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
            if rows.len() != chunk.len() {
                return Err(StoreError("incomplete credential activity read".into()));
            }
            for (&key_id, row) in chunk.iter().zip(rows) {
                let state = if !row.get::<bool, _>(0) {
                    CredentialActivityState::Unknown
                } else if let Some(at) = row.get::<Option<i64>, _>(1) {
                    CredentialActivityState::Committed {
                        last_committed_at: micros_ts(at, "credential activity")?,
                    }
                } else {
                    CredentialActivityState::Unobserved
                };
                result.push(CredentialActivity { key_id, state });
            }
        }
        Ok(result)
    }

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

        rows.into_iter().map(credential_from_row).collect()
    }
}

fn credential_from_row(row: sqlx::postgres::PgRow) -> Result<KeyRecord, StoreError> {
    let id = |index| -> Result<u128, StoreError> {
        let bytes: Vec<u8> = row.get(index);
        let fixed: [u8; 16] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| StoreError("credential identifier is not 16 bytes".into()))?;
        Ok(u128::from_be_bytes(fixed))
    };
    let key_id = KeyId(id(0)?);
    Ok(KeyRecord {
        key_id,
        account_id: AccountId(id(1)?),
        principal: Principal(id(2)?),
        digest: digest_from(row.get::<Vec<u8>, _>(3).as_slice(), key_id)?,
        not_after: row
            .get::<Option<i64>, _>(4)
            .map(|us| micros_ts(us, "credential expiry"))
            .transpose()?,
    })
}

#[async_trait]
impl tollgate_store::KeySource for PostgresStore {
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        tollgate_store::validate_key_page_limit(limit)?;
        // One short snapshot per page, never a transaction held across HTTP
        // requests. Revision and records therefore cannot describe two commits.
        let mut tx = self.pool.begin().await.map_err(storage)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM tollgate_credential_revision WHERE singleton")
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        let revision = u64::try_from(revision)
            .map_err(|_| StoreError("credential revision is negative".into()))?;
        // Separate SQL shapes preserve an indexable range in prepared plans.
        let sql = if after.is_some() {
            "SELECT key_id, account_id, principal, digest, not_after_us
             FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
             AND (not_after_us IS NULL OR not_after_us > $1) AND key_id > $2
             ORDER BY key_id LIMIT $3"
        } else {
            "SELECT key_id, account_id, principal, digest, not_after_us
             FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
             AND (not_after_us IS NULL OR not_after_us > $1) AND key_id >= $2
             ORDER BY key_id LIMIT $3"
        };
        let rows = sqlx::query(sql)
            .bind(ts_micros(now))
            .bind(id_bytes(after.unwrap_or(KeyId(0)).0))
            .bind((limit.get() + 1) as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        // Decode lookahead too: corrupt data cannot masquerade as a normal
        // continuation and silently defer a failure to another request.
        let mut records = rows
            .into_iter()
            .map(credential_from_row)
            .map(|row| row.map(tollgate_store::CredentialRecord::from))
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = if records.len() > limit.get() {
            records.pop();
            records.last().map(|record| record.key_id)
        } else {
            None
        };
        tollgate_store::KeyPage::try_new(revision, now, after, limit, records, next_after)
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
    /// The account-owned execution-capacity class (#99). Rides the JSONB
    /// document; omitting it here would drop it on every publish.
    capacity_class: &'a CapacityClass,
    enforcement_mode: &'a EnforcementMode,
    valid_until: &'a Timestamp,
    permissions: &'a PermissionBits,
    limits: &'a ResolvedLimits,
    cost_table: &'a Arc<CostTable>,
    /// Stored so a *pulled* snapshot carries what a pushed one does. Without
    /// it an instance that refreshed instead of receiving a push would report
    /// no budget at all, and the two would disagree about the same account.
    budget: Option<&'a BudgetView>,
    /// The consuming application's policy identity (#94). Rides the JSONB
    /// column, so it needs no schema change of its own — but it does need to
    /// be here: this DTO is the whole of what storage writes, and a field
    /// omitted from it is dropped on every publish without a word.
    policy_revision: &'a PolicyRevision,
}

impl<'a> From<&'a AccountSnapshot> for StoredSnapshotRef<'a> {
    fn from(snapshot: &'a AccountSnapshot) -> Self {
        StoredSnapshotRef {
            account_id: StoredId(snapshot.account_id.0),
            key_id: snapshot.key_id.map(|id| StoredId(id.0)),
            status: &snapshot.status,
            capacity_class: &snapshot.capacity_class,
            enforcement_mode: &snapshot.enforcement_mode,
            valid_until: &snapshot.valid_until,
            permissions: &snapshot.permissions,
            limits: &snapshot.limits,
            cost_table: &snapshot.cost_table,
            budget: snapshot.budget.as_ref(),
            policy_revision: &snapshot.policy_revision,
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
    /// Absent from every row written before #99, and `default` for the reason
    /// the fields below are: a required field would make every pre-existing
    /// row fail to decode and deny every principal until the whole catalogue
    /// was republished. `Assured` is the safe default — the availability every
    /// account already has.
    #[serde(default)]
    capacity_class: CapacityClass,
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
    /// Absent from every row written before periodic budgets, and `default`
    /// for the reason `enforcement_mode` is: a required field would make every
    /// pre-existing row fail to decode and deny every principal until the
    /// whole catalogue was republished. `None` is "the control plane said
    /// nothing", which readers report as such rather than as a zero balance.
    #[serde(default)]
    budget: Option<BudgetView>,
    /// Absent from every row written before #94, and `default` for the reason
    /// the two fields above are. The unstated revision is a value rather than
    /// an absence, so a pre-existing row decodes to "this account's publisher
    /// stated no policy identity" — which is true, and is what a consumer
    /// reading it back should be told.
    #[serde(default)]
    policy_revision: PolicyRevision,
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
        .enforcement_mode(self.enforcement_mode)
        .capacity_class(self.capacity_class)
        .policy_revision(self.policy_revision);
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
///
/// Deliberately still its own statement. Folding it into the account read as a
/// lateral subquery would make the reconciliation read atomic without a
/// transaction, but it also destabilises the plan: measured over twelve runs
/// of `the_account_filter_is_answered_by_an_index_not_by_discarding_rows`, the
/// correlated form kept the index 4 times in 5 and the uncorrelated form 10
/// times in 12, against 12 in 12 for this shape. Atomicity is bought with a
/// transaction instead (#56), which leaves this predicate — and the plan #12
/// pinned — untouched.
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

/// Persisted capabilities are minted from counters seeded at one. Invalid
/// storage must not become a caller mismatch or mint a zero capability.
fn stored_fence(value: i64) -> Result<FencingToken, StoreError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .map(FencingToken)
        .ok_or_else(|| StoreError(format!("stored fencing token is not positive: {value}")))
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

/// The inverse of [`ts_micros`]. A stored value outside the representable
/// range is corruption to report, never an instant to clamp to.
fn micros_ts(value: i64, what: &str) -> Result<Timestamp, StoreError> {
    tollgate_store::clock::timestamp_from_micros(value).map_err(|e| {
        StoreError(format!(
            "{what} is not a representable instant: {value} ({e})"
        ))
    })
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
        Ok(Arc::new(PostgresStore { pool, policy, push }))
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
        // One snapshot for both halves. The account's stored totals and the
        // sums over its live leases move together — `ingest` raises
        // `usage_recorded` and the lease's `used` in one transaction — so
        // reading them on two pooled connections let a commit land between
        // them: reconciliation then paired a pre-write total with a post-write
        // sum and reported corruption on a correct ledger, or underflowed the
        // subtraction outright (#56).
        //
        // `READ COMMITTED` is not enough, because it takes a fresh snapshot
        // per statement; `REPEATABLE READ` takes one at the first read and
        // holds it for the rest of the transaction. Read-only, so it cannot
        // hit the serialization failures that make `SERIALIZABLE` a retry
        // contract rather than a snapshot.
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            let account_row = sqlx::query(
                "SELECT deposited, balance, usage_recorded, settlement_loss, overage_recorded,
                        expired
                 FROM tollgate_accounts WHERE account_id = $1",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            let lease_row = sqlx::query(ACTIVE_LEASE_SUM_SQL)
                .bind(id_bytes(account.0))
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
            Ok::<_, StoreError>((account_row, lease_row))
        }
        .await;
        let (account_row, lease_row) = finish_transaction(tx, result).await?;

        let Some(row) = account_row else {
            return Ok(None);
        };
        let active_grants = to_units(lease_row.get::<i64, _>(0), "active lease grants")?;
        let active_used = to_units(lease_row.get::<i64, _>(1), "active lease usage")?;
        let recorded = to_units(row.get::<i64, _>(2), "usage_recorded")?;
        Ok(Some(Conservation {
            deposited: to_units(row.get::<i64, _>(0), "deposited")?,
            overage_recorded: to_units(row.get::<i64, _>(4), "overage_recorded")?,
            balance: to_units(row.get::<i64, _>(1), "balance")?,
            active_lease_grants: active_grants,
            // Surfaced, never panicked on. These are two stored columns read
            // from a database this process does not exclusively own, so
            // `recorded < active_used` is corruption to report — the same
            // class as a negative unit column (INVARIANTS.md #11) — not an
            // internal invariant a caller could not violate. `MemoryStore`
            // keeps its `expect` because there the counters are maintained by
            // one process under one lock and the state is unrepresentable.
            settled_usage: recorded.checked_sub(active_used).ok_or_else(|| {
                StoreError(format!(
                    "active lease usage {} exceeds recorded usage {} for account {account}",
                    active_used.get(),
                    recorded.get()
                ))
            })?,
            settlement_loss: to_units(row.get::<i64, _>(3), "settlement_loss")?,
            expired: to_units(row.get::<i64, _>(5), "expired")?,
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
    expires_at: LeaseInstant,
    state: i16,
    /// The half of `granted` drawn from the account's periodic allowance, and
    /// the period that funded it. Settlement needs both: the split says which
    /// bucket each unspent unit belongs to, the period says whether the
    /// allowance half still exists (#97).
    from_allowance: i64,
    period_start_us: i64,
}

/// One expired lease's settlement, before the account's current period is
/// known.
///
/// Named rather than a tuple for the reason [`LockedLeaseRow`] is: the sweep
/// carries four same-typed numbers per lease, and a positional tuple would let
/// any two of them swap without a compiler error.
struct LeaseSettlement {
    account: Vec<u8>,
    /// Unspent units returning to the top-up bucket, which no boundary
    /// touches.
    to_topup: i64,
    /// The whole unspent credit. What is not `to_topup` is the allowance half,
    /// and the account's period decides where that lands.
    credit: i64,
    period_start_us: i64,
}

/// One account's share of a sweep, split the way the ledger columns are.
#[derive(Default)]
struct AccountCredit {
    /// Added to `balance`: the top-up half always, plus the allowance half of
    /// any lease still inside the account's current period.
    spendable: i64,
    /// The part of `spendable` that is allowance, so `allowance_balance`
    /// tracks the same units `balance` just gained.
    to_allowance: i64,
    /// The allowance half of leases funded by a period that has since closed.
    expiring: i64,
}

/// Settlement evidence from the account update inside the current transaction.
/// Consolidation uses the credit that survived the period boundary as its floor.
struct ReleasedCredit {
    account: AccountId,
    restored: CostUnits,
}

impl AccountCredit {
    /// Fold one lease's settlement in, deciding the allowance half against the
    /// account's current period.
    ///
    /// Checked, and refused rather than wrapped: these sums are paid into a
    /// ledger, so an overflow is corruption to report, not a number to
    /// truncate (INVARIANTS.md #11).
    fn add(
        &mut self,
        settlement: &LeaseSettlement,
        account_period_us: i64,
    ) -> Result<(), StoreError> {
        let overflow = || StoreError("reclaim credit sum overflow".into());
        let to_allowance = settlement
            .credit
            .checked_sub(settlement.to_topup)
            .ok_or_else(overflow)?;
        self.spendable = self
            .spendable
            .checked_add(settlement.to_topup)
            .ok_or_else(overflow)?;
        if settlement.period_start_us < account_period_us {
            self.expiring = self
                .expiring
                .checked_add(to_allowance)
                .ok_or_else(overflow)?;
        } else {
            self.spendable = self
                .spendable
                .checked_add(to_allowance)
                .ok_or_else(overflow)?;
            self.to_allowance = self
                .to_allowance
                .checked_add(to_allowance)
                .ok_or_else(overflow)?;
        }
        Ok(())
    }
}

/// Lock one lease row for a settlement transition.
async fn lock_lease(
    tx: &mut Transaction<'_, Postgres>,
    lease_id: LeaseId,
) -> Result<Option<LockedLeaseRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT account_id, fencing_token, granted, used, credited, expires_at_floor_us, state,
                from_allowance, period_start_us, expires_at_submicro_ns
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
        expires_at: LeaseInstant {
            micros: row.get(5),
            submicro_nanos: row.get(9),
        },
        state: row.get(6),
        from_allowance: row.get(7),
        period_start_us: row.get(8),
    }))
}

impl PostgresStore {
    /// One grant, inside a caller-owned transaction.
    ///
    /// `floor` is the units the caller is returning to this same account in
    /// this same transaction, which the grant policy's shrink cap may not size
    /// the result below (see [`LeaseAllocator::consolidate`]). A plain acquire
    /// returns nothing and passes zero, leaving the policy's answer untouched.
    async fn acquire_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        account: AccountId,
        requested: CostUnits,
        expires_at: Timestamp,
        floor: CostUnits,
    ) -> Result<LeaseGrant, AllocateError> {
        let row = sqlx::query(
            "SELECT balance, status, next_fence, allowance_balance, period_start_us
             FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
        )
        .bind(id_bytes(account.0))
        .fetch_optional(&mut **tx)
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
        let balance =
            to_units(row.get::<i64, _>(0), "account balance").map_err(AllocateError::Storage)?;
        // The floor is applied to the policy's answer, not to the
        // balance test: the units the caller returned rejoined `balance`
        // earlier in this same transaction, so a floor can only re-select
        // capacity the account demonstrably has, and an account with
        // nothing left still refuses.
        let granted = self
            .policy
            .grant(requested, balance)
            .ok_or(AllocateError::InsufficientBalance)?
            .max(floor.min(balance));
        // Allowance first: the units with an expiry date are spent
        // before the manual credits sitting beside them, and the lease
        // remembers the split so settlement can return each half to where
        // it came from (#97).
        let granted_i = to_i64(granted, "grant").map_err(AllocateError::Storage)?;
        let allowance_balance = row.get::<i64, _>(3);
        let from_allowance = granted_i.min(allowance_balance);
        let period_start_us = row.get::<i64, _>(4);
        let fence = row.get::<i64, _>(2);
        let fence_token = stored_fence(fence).map_err(AllocateError::Storage)?;

        let lease_id = LeaseId(uuid::Uuid::new_v4().as_u128());

        sqlx::query(
            "UPDATE tollgate_accounts
             SET balance = balance - $2,
                 allowance_balance = allowance_balance - $3,
                 next_fence = next_fence + 1
             WHERE account_id = $1",
        )
        .bind(id_bytes(account.0))
        .bind(granted_i)
        .bind(from_allowance)
        .execute(&mut **tx)
        .await
        .map_err(alloc_storage)?;
        sqlx::query(
            "INSERT INTO tollgate_leases
             (lease_id, account_id, fencing_token, granted, used, credited, expires_at_floor_us,
              state, from_allowance, period_start_us, expires_at_submicro_ns, expiry_is_upper_bound)
             VALUES ($1, $2, $3, $4, 0, 0, $5, 0, $6, $7, $8, FALSE)",
        )
        .bind(id_bytes(lease_id.0))
        .bind(id_bytes(account.0))
        .bind(fence)
        .bind(granted_i)
        .bind(LeaseInstant::from(expires_at).micros)
        .bind(from_allowance)
        .bind(period_start_us)
        .bind(LeaseInstant::from(expires_at).submicro_nanos)
        .execute(&mut **tx)
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

    /// One release, returning its account and the credit the account update
    /// actually restored, inside the caller-owned transaction.
    async fn release_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<ReleasedCredit, AllocateError> {
        let LockedLeaseRow {
            account_id,
            fencing_token: fence,
            granted,
            used,
            credited: _credited,
            expires_at,
            state,
            from_allowance,
            period_start_us,
        } = lock_lease(tx, lease_id)
            .await
            .map_err(alloc_storage)?
            .ok_or(AllocateError::UnknownLease)?;

        if stored_fence(fence).map_err(AllocateError::Storage)? != fencing_token {
            return Err(AllocateError::Fenced);
        }
        // Releases are accepted through the grace window (see GrantPolicy::
        // reclaim_grace) — only a settled or grace-exhausted lease refuses.
        let expires_at = expires_at.timestamp().map_err(AllocateError::Storage)?;
        if state != STATE_ACTIVE
            || self
                .policy
                .reclaim_cutoff(now)
                .is_some_and(|cutoff| expires_at <= cutoff)
        {
            return Err(AllocateError::LeaseNotActive);
        }
        // Decoded before the byte form is consumed by the credit below, and
        // returned so a consolidation draws its replacement grant from the
        // account this lease named rather than one a caller supplied.
        let account = AccountId(id_from(&account_id));
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
            .execute(&mut **tx)
            .await
            .map_err(alloc_storage)?;
        // The lease's usage is charged in the order the account spends,
        // allowance first, so the top-up half is what survives a partly
        // spent lease. Crediting the allowance half back first would close
        // the equation just as well while moving durable credits into the
        // bucket that expires at the next boundary (#97).
        let from_topup = granted
            .checked_sub(from_allowance)
            .filter(|t| *t >= 0)
            .ok_or_else(|| {
                AllocateError::Storage(StoreError(format!(
                    "lease allowance funding {from_allowance} exceeds its grant {granted}"
                )))
            })?;
        let to_topup = from_topup.min(unspent_i);
        let to_allowance = unspent_i - to_topup;

        // One statement, and the `CASE` is the whole boundary rule: if the
        // account has moved on to a later period, the allowance half is
        // expired instead of returned. Deciding it in SQL against the
        // row's own `period_start_us` keeps the read and the write in one
        // atomic step, so a rollover committing between them cannot make
        // this credit an allowance the account no longer has.
        let restored: i64 = sqlx::query_scalar(
            "UPDATE tollgate_accounts SET
                 balance = balance + $2
                     + CASE WHEN period_start_us > $5 THEN 0 ELSE $3 END,
                 allowance_balance = allowance_balance
                     + CASE WHEN period_start_us > $5 THEN 0 ELSE $3 END,
                 expired = expired + CASE WHEN period_start_us > $5 THEN $3 ELSE 0 END,
                 settlement_loss = settlement_loss + $4
             WHERE account_id = $1
             RETURNING $2 + CASE WHEN period_start_us > $5 THEN 0 ELSE $3 END",
        )
        .bind(account_id)
        .bind(to_topup)
        .bind(to_allowance)
        .bind(loss)
        .bind(period_start_us)
        .fetch_one(&mut **tx)
        .await
        .map_err(alloc_storage)?;
        Ok(ReleasedCredit {
            account,
            restored: to_units(restored, "restored release credit")
                .map_err(AllocateError::Storage)?,
        })
    }

    /// The expiry a grant issued now would carry, clamping to the policy's
    /// `max_ttl`. Fallible before any transaction opens, so a bad TTL never
    /// settles a lease it cannot replace.
    fn grant_expiry(
        &self,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Timestamp, AllocateError> {
        if ttl <= SignedDuration::ZERO {
            return Err(AllocateError::InvalidTtl);
        }
        now.checked_add(ttl.min(self.policy.max_ttl))
            .map_err(|e| AllocateError::Storage(StoreError(format!("ttl overflow: {e}"))))
    }
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
        let expires_at = self.grant_expiry(ttl, now)?;
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let result = self
            .acquire_in_tx(&mut tx, account, requested, expires_at, CostUnits::ZERO)
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
        let result = self
            .release_in_tx(&mut tx, lease_id, fencing_token, unspent, now)
            .await
            .map(|_| ());
        finish_transaction(tx, result).await
    }

    async fn consolidate(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError> {
        let expires_at = self.grant_expiry(ttl, now)?;
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        // One transaction for both halves is the whole point. Lease row first
        // and account row second, which is the order `release` already takes,
        // so a consolidation cannot invert the lock order against a concurrent
        // release of a sibling lease on the same account.
        //
        // The new grant is drawn from the account the released lease named,
        // never one the caller supplied, so the two halves cannot disagree
        // about whose balance moved.
        let result = async {
            let released = self
                .release_in_tx(&mut tx, lease_id, fencing_token, unspent, now)
                .await?;
            self.acquire_in_tx(
                &mut tx,
                released.account,
                requested,
                expires_at,
                released.restored,
            )
            .await
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
        let Some(cutoff) = self.policy.reclaim_cutoff(now) else {
            return ReclaimBatch::try_new(Vec::new(), limit);
        };
        let cutoff = LeaseInstant::from(cutoff);
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // Reclaim only once the grace window past expiry has fully lapsed:
            // expires_at + grace <= now  iff  expires_at <= now - grace.
            // Both tuple components preserve the exact timestamp ordering.
            // SKIP LOCKED lets concurrent sweepers cooperate; the limit keeps
            // both the lease locks and the transaction's row work bounded.
            let rows = sqlx::query(
                "SELECT lease_id, account_id, granted, used, from_allowance, period_start_us
                 FROM tollgate_leases
                 WHERE state = 0 AND (expires_at_floor_us, expires_at_submicro_ns) <= ($1, $2)
                 ORDER BY account_id, lease_id LIMIT $3 FOR UPDATE SKIP LOCKED",
            )
            .bind(cutoff.micros)
            .bind(cutoff.submicro_nanos)
            .bind(limit_i)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;

            let mut reclaimed = Vec::with_capacity(rows.len());
            let mut lease_ids = Vec::with_capacity(rows.len());
            let mut lease_credits = Vec::with_capacity(rows.len());
            let mut settlements = Vec::with_capacity(rows.len());
            for row in rows {
                let lease_bytes: Vec<u8> = row.get(0);
                let account_bytes: Vec<u8> = row.get(1);
                let granted = row.get::<i64, _>(2);
                let used = row.get::<i64, _>(3);
                let from_allowance = row.get::<i64, _>(4);
                let credit = granted.checked_sub(used).ok_or_else(|| {
                    StoreError(format!(
                        "reclaim credit overflow: granted {granted}, used {used}"
                    ))
                })?;
                // Validate the whole batch before either set-wise UPDATE: a
                // negative credit is corruption, and paying it would debit the
                // account rather than returning quota.
                let credit_units = to_units(credit, "reclaim credit")?;
                let from_topup = granted
                    .checked_sub(from_allowance)
                    .filter(|t| *t >= 0)
                    .ok_or_else(|| {
                        StoreError(format!(
                            "lease allowance funding {from_allowance} exceeds its grant {granted}"
                        ))
                    })?;
                settlements.push(LeaseSettlement {
                    account: account_bytes.clone(),
                    // Allowance first, exactly as `release` charges it.
                    to_topup: from_topup.min(credit),
                    credit,
                    period_start_us: row.get::<i64, _>(5),
                });
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

            let mut account_ids: Vec<Vec<u8>> = settlements
                .iter()
                .map(|settlement| settlement.account.clone())
                .collect();
            account_ids.sort_unstable();
            account_ids.dedup();
            let expected_lease_rows = u64::try_from(lease_ids.len())
                .map_err(|_| StoreError("reclaim lease row count exceeds u64 range".into()))?;
            let expected_account_rows = u64::try_from(account_ids.len())
                .map_err(|_| StoreError("reclaim account row count exceeds u64 range".into()))?;

            // A set-wise UPDATE does not promise row-lock order. Lock every
            // affected account explicitly in byte-sorted order first, matching
            // release and ingest's lease-then-account order and preventing
            // concurrent multi-account sweeps from forming a deadlock cycle.
            //
            // The lock is also what makes `period_start_us` safe to read here
            // and aggregate against: a rollover cannot commit between this
            // read and the credit below, so a lease is expired or returned
            // against the period the account is actually in (#97).
            let locked_accounts = sqlx::query(
                "SELECT account_id, period_start_us FROM tollgate_accounts
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
            let periods: std::collections::BTreeMap<Vec<u8>, i64> = locked_accounts
                .iter()
                .map(|row| (row.get::<Vec<u8>, _>(0), row.get::<i64, _>(1)))
                .collect();

            let mut credits: std::collections::BTreeMap<Vec<u8>, AccountCredit> =
                std::collections::BTreeMap::new();
            for settlement in settlements {
                let account_period = *periods.get(&settlement.account).ok_or_else(|| {
                    StoreError(format!(
                        "reclaim locked no account row for {:#034x}",
                        id_from(&settlement.account)
                    ))
                })?;
                let credit = credits.entry(settlement.account.clone()).or_default();
                credit.add(&settlement, account_period)?;
            }

            let (account_ids, account_credits): (Vec<_>, Vec<_>) = credits.into_iter().unzip();
            let (spendable, expiring): (Vec<_>, Vec<_>) = account_credits
                .iter()
                .map(|credit| (credit.spendable, credit.expiring))
                .unzip();
            let to_allowance: Vec<_> = account_credits
                .iter()
                .map(|credit| credit.to_allowance)
                .collect();

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
                 SET balance = account.balance + delta.spendable,
                     allowance_balance = account.allowance_balance + delta.to_allowance,
                     expired = account.expired + delta.expiring
                 FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[])
                     AS delta(account_id, spendable, to_allowance, expiring)
                 WHERE account.account_id = delta.account_id",
            )
            .bind(&account_ids)
            .bind(&spendable)
            .bind(&to_allowance)
            .bind(&expiring)
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
    ) -> Result<IngestReport, IngestError> {
        // One transaction per *batch* (review finding #8): leases are locked
        // in a single sorted ANY() query (sorted to keep concurrent batches
        // deadlock-free), duplicates are detected with one lookup, events are
        // classified in memory against the locked rows, and the accepted set
        // lands via one bulk insert plus set-wise per-lease/per-account
        // updates. Classification in application code preserves the partial
        // acceptance contract without savepoints.
        let mut report = IngestReport {
            unattributed: Some(0),
            ..IngestReport::default()
        };
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
            key_id: Option<Vec<u8>>,
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
                    key_id: event.key_id.map(|id| id_bytes(id.0)),
                    lease_id: event.source.lease_id().map(|id| id_bytes(id.0)),
                    occurred_at_us: ts_micros(event.occurred_at),
                })
            })
            .collect::<Result<_, StoreError>>()?;

        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result: Result<_, IngestError> = async {
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
                /// The lease's stored fence, already validated positive;
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
                    let Ok(units) = i64::try_from(event.event.units.get()) else {
                        // Caller data outside this backend's storage domain
                        // rejects this event, not the valid neighboring work.
                        report.rejected += 1;
                        continue;
                    };
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
                if Some(stored_fence(lease.fence)?) != event.event.source.fencing_token()
                    || lease.account_id.as_slice() != event.account_id.as_slice()
                {
                    report.rejected += 1;
                    continue;
                }
                to_units(lease.granted, "lease granted")?;
                to_units(lease.used, "lease used")?;
                to_units(lease.credited, "lease credited")?;
                let Ok(units) = i64::try_from(event.event.units.get()) else {
                    report.rejected += 1;
                    continue;
                };
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
            let (mut rid, mut acct, mut lease, mut fence, mut units, mut at, mut revision, mut keys) = (
                Vec::with_capacity(accepted.len()),
                Vec::with_capacity(accepted.len()),
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
                keys.push(event.key_id.clone());
                lease.push(event.lease_id.clone());
                fence.push(accepted_event.fence);
                units.push(accepted_event.units);
                at.push(event.occurred_at_us);
                // Carried verbatim: 32 bytes in, 32 bytes out, and the
                // schema's length CHECK says so. Unlike the identifiers
                // above this needs no width conversion — the Rust type is
                // already the stored representation.
                revision.push(event.event.policy_revision.as_bytes().to_vec());
                if accepted_event.overage {
                    debug_assert!(
                        event.lease_id.is_none() && accepted_event.fence.is_none(),
                        "an overage row must carry neither half of a capability"
                    );
                }

                let entry = account_deltas.entry(event.account_id.clone()).or_default();
                entry.usage = entry.usage.checked_add(accepted_event.units).ok_or_else(|| {
                    IngestError::Refused(StoreError(format!(
                        "usage delta overflow for account {:#034x}",
                        event.event.account_id.0
                    )))
                })?;
                if accepted_event.settled {
                    entry.loss = entry.loss.checked_add(accepted_event.units).ok_or_else(|| {
                        IngestError::Refused(StoreError(format!(
                            "settlement loss delta overflow for account {:#034x}",
                            event.event.account_id.0
                        )))
                    })?;
                }
                if accepted_event.overage {
                    entry.overage =
                        entry.overage.checked_add(accepted_event.units).ok_or_else(|| {
                            IngestError::Refused(StoreError(format!(
                                "overage delta overflow for account {:#034x}",
                                event.event.account_id.0
                            )))
                        })?;
                }
            }
            let inserted = sqlx::query(
                "INSERT INTO tollgate_usage_events
                 (request_id, account_id, lease_id, fencing_token, units, occurred_at_us, policy_revision, key_id)
                 SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::bytea[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bytea[], $8::bytea[])",
            )
            .bind(&rid)
            .bind(&acct)
            .bind(&lease)
            .bind(&fence)
            .bind(&units)
            .bind(&at)
            .bind(&revision)
            .bind(&keys)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            let expected_event_rows = u64::try_from(accepted.len())
                .map_err(|_| StoreError("accepted event count exceeds u64 range".into()))?;
            if inserted.rows_affected() != expected_event_rows {
                return Err(IngestError::Unavailable(StoreError(format!(
                    "ingest inserted {} of {} accepted usage rows",
                    inserted.rows_affected(),
                    accepted.len()
                ))));
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
                    return Err(IngestError::Unavailable(StoreError(format!(
                        "ingest updated {} of {} locked lease rows",
                        updated_leases.rows_affected(),
                        lease_update_ids.len()
                    ))));
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
                return Err(IngestError::Unavailable(StoreError(format!(
                    "ingest locked {} of {} referenced account rows",
                    locked_accounts.len(),
                    account_ids.len()
                ))));
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
                    IngestError::Refused(StoreError(format!(
                        "usage_recorded overflow for account {:#034x}",
                        id_from(&account_id)
                    )))
                })?;
                // The funding half of the same units. Both terms must be
                // representable or neither may move, or the batch would bill
                // overage it did not fund and leave the equation open.
                overage_recorded.checked_add(delta.overage).ok_or_else(|| {
                    IngestError::Refused(StoreError(format!(
                        "overage_recorded overflow for account {:#034x}",
                        id_from(&account_id)
                    )))
                })?;
                if settlement_loss < delta.loss {
                    return Err(IngestError::Unavailable(StoreError(format!(
                        "settlement_loss underflow for account {:#034x}: settled straggler \
                         usage {} exceeds recorded loss",
                        id_from(&account_id),
                        delta.loss
                    ))));
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
                return Err(IngestError::Unavailable(StoreError(format!(
                    "ingest updated {} of {} locked account rows",
                    updated_accounts.rows_affected(),
                    account_ids.len()
                ))));
            }

            // Legacy/unscoped batches cannot name activity. Their complete
            // attribution result is already known, so avoid an empty database
            // round trip while the ledger transaction still commits normally.
            if keys.iter().all(Option::is_none) {
                report.unattributed = Some(expected_event_rows);
                return Ok(report);
            }

            // Only newly accepted rows supply evidence. Count events before
            // grouping: two events for one key are both attributable, even
            // if neither advances an already-newer maximum. Sort writes to
            // share one activity-lock order across concurrent transactions.
            let attributed: i64 = sqlx::query_scalar(
                "WITH matched AS MATERIALIZED (
                    SELECT k.key_id, b.occurred_at_us
                    FROM UNNEST($1::bytea[], $2::bytea[], $3::bigint[])
                        AS b(key_id, account_id, occurred_at_us)
                    JOIN LATERAL (
                        SELECT key_id FROM tollgate_credential_keys
                        WHERE key_id = b.key_id AND account_id = b.account_id LIMIT 1
                    ) k ON true
                 ), updated AS (
                    INSERT INTO tollgate_credential_activity AS activity (key_id, last_committed_at_us)
                    SELECT key_id, MAX(occurred_at_us) FROM matched GROUP BY key_id ORDER BY key_id
                    ON CONFLICT (key_id) DO UPDATE
                    SET last_committed_at_us = EXCLUDED.last_committed_at_us
                    WHERE activity.last_committed_at_us < EXCLUDED.last_committed_at_us
                    RETURNING key_id
                 ) SELECT COUNT(*) FROM matched"
            ).bind(&keys).bind(&acct).bind(&at).fetch_one(&mut *tx).await.map_err(storage)?;
            report.unattributed = Some(expected_event_rows.checked_sub(
                u64::try_from(attributed).map_err(|_| StoreError("negative attribution count".into()))?
            ).ok_or_else(|| StoreError("attribution count exceeds accepted events".into()))?);

            Ok(report)
        }
        .await;
        // Monotonic accounting overflow is a permanent batch refusal. Database
        // and stored-corruption errors remain retryable, and rollback must
        // complete before either outcome becomes observable.
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

/// The ledger's execution-capacity class, or a refusal for a spelling the
/// vocabulary does not contain (#99).
///
/// Never defaults to `Assured`, for the reason [`decode_status`] never defaults
/// to `Active`. A value the `CHECK` constraint should have made impossible
/// means the row was written outside this code, and admitting it as `Assured`
/// would turn corruption into unconditional capacity — the permissive answer,
/// arrived at by accident.
fn decode_capacity_class(stored: String) -> Result<CapacityClass, StoreError> {
    match stored.as_str() {
        s if s == CapacityClass::Assured.as_str() => Ok(CapacityClass::Assured),
        s if s == CapacityClass::BestEffort.as_str() => Ok(CapacityClass::BestEffort),
        other => Err(StoreError(format!("unrecognized capacity class {other:?}"))),
    }
}

/// Rebuild an account's schedule from its three stored columns.
///
/// `None` is "no schedule", and it is only reachable when all three are NULL:
/// the row's all-or-nothing CHECK makes half a schedule unstorable, so a
/// half-populated row read here is corruption to report rather than a shape to
/// interpret. Unknown names are refused for the same reason `decode_status`
/// refuses them — a period this binary cannot evaluate must not be presented
/// as if it were monthly.
fn decode_schedule(
    allowance: Option<i64>,
    period: Option<String>,
    rollover: Option<String>,
) -> Result<Option<BudgetSchedule>, StoreError> {
    let populated = [allowance.is_some(), period.is_some(), rollover.is_some()];
    let (Some(allowance), Some(period), Some(rollover)) = (allowance, period, rollover) else {
        if populated.iter().any(|present| *present) {
            return Err(StoreError(
                "stored budget schedule is partially populated".into(),
            ));
        }
        return Ok(None);
    };
    let period = match period.as_str() {
        s if s == Period::UtcCalendarMonth.as_str() => Period::UtcCalendarMonth,
        other => return Err(StoreError(format!("unrecognized budget period {other:?}"))),
    };
    let rollover = match rollover.as_str() {
        s if s == Rollover::None.as_str() => Rollover::None,
        other => {
            return Err(StoreError(format!(
                "unrecognized budget rollover {other:?}"
            )));
        }
    };
    Ok(Some(BudgetSchedule {
        allowance: to_units(allowance, "budget allowance")?,
        period,
        rollover,
    }))
}

/// What the account could still spend, for a snapshot's budget view (#97).
///
/// Balance *plus* the unspent remainder of every active lease, because units
/// out on lease are still the account's. Derived from the account row alone:
/// the conservation equation makes `balance + active grants` equal to what the
/// account was funded with minus what it consumed, so no join over
/// `tollgate_leases` is needed at every publication to answer the same
/// number.
///
/// Reads the columns of the `FOR SHARE` row in `publish_snapshot`, positions 1
/// through 9. Corruption is reported rather than saturated: unlike
/// `MemoryStore`, this backend does not exclusively own the ledger it reads,
/// so an underflow here is the same class of event as a negative unit column
/// (INVARIANTS.md #11) — see the note in `PostgresStore::conservation`.
fn budget_view(row: &sqlx::postgres::PgRow) -> Result<BudgetView, StoreError> {
    let deposited = to_units(row.get::<i64, _>(1), "deposited")?;
    let overage = to_units(row.get::<i64, _>(2), "overage_recorded")?;
    let usage = to_units(row.get::<i64, _>(3), "usage_recorded")?;
    let loss = to_units(row.get::<i64, _>(4), "settlement_loss")?;
    let expired = to_units(row.get::<i64, _>(5), "expired")?;
    let schedule = decode_schedule(row.get(6), row.get(7), row.get(8))?;
    let period_start = micros_ts(row.get::<i64, _>(9), "period_start_us")?;

    let funded = deposited
        .checked_add(overage)
        .ok_or_else(|| StoreError("account funding total overflows".into()))?;
    let consumed = usage
        .checked_add(loss)
        .and_then(|spent| spent.checked_add(expired))
        .ok_or_else(|| StoreError("account consumption total overflows".into()))?;
    Ok(BudgetView {
        balance_at_publish: funded.checked_sub(consumed).ok_or_else(|| {
            StoreError(format!(
                "consumption {} exceeds funding {}",
                consumed.get(),
                funded.get()
            ))
        })?,
        // The period the account is *in*, which is what it can spend against.
        // An account whose schedule was set but whose first rollover has not
        // run yet is still in its previous period, and says so, until the next
        // sweep tick moves it.
        period_end: schedule.map(|schedule| schedule.period.end_after(period_start)),
    })
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
    // Carried across the rebuild rather than through the builder: the builder
    // has no setter for a budget on purpose, so that a publisher cannot supply
    // one. Re-attaching what this store itself wrote is the store writing it
    // again, which is the same rule and not an exception to it.
    let budget = snapshot.budget;
    let publishable = PublishableSnapshot::try_new(Arc::new(snapshot.into_snapshot(generation)))
        .map_err(|error| {
            StoreError(format!(
                "invalid stored snapshot for principal {:#034x}: {error}",
                principal.0
            ))
        })?;
    Ok(match budget {
        Some(budget) => publishable.with_budget(Some(budget)),
        None => publishable,
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

/// Re-stamp every live snapshot of `account`, patching one JSON key, and
/// report which principals to push and how many rows could not be decoded.
///
/// Shared by the two account-owned facts that republish — status (#51) and
/// execution-capacity class (#99). They differ in their precondition and their
/// ledger column; everything below is identical, and it is the part where the
/// subtlety lives, so it is written once.
///
/// `jsonb_set` rather than read-modify-write in Rust, for three reasons any
/// one of which decides it:
///
/// 1. RMW reintroduces #51's own bug. A concurrent `publish_snapshot` landing
///    between the read and the write makes `generation + 1` no longer greater
///    than stored, and the monotonic guard then *silently drops the change*
///    for that principal.
/// 2. RMW loses fields. `StoredSnapshot` has no `flatten`, so decoding and
///    re-serialising a row written by a newer binary discards what this one
///    does not know about.
/// 3. RMW fails whole on one bad row. A safety operation must not be blockable
///    by one unrelated corrupt credential.
///
/// Only the named key is patched. The generation lives in the column alone
/// (#54), and `RETURNING generation` carries the new value out to the push.
///
/// `deleted = FALSE` leaves tombstones alone: republishing one would resurrect
/// a revoked principal (INVARIANTS.md #15), and revocation stays its own
/// per-credential mechanism. `IS DISTINCT FROM` makes a repeat converge,
/// bumping nothing — and note that a document predating the key has SQL NULL
/// there, so the first change of a newly added fact rewrites every row once.
async fn republish_patched_snapshots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: AccountId,
    json_path: &'static str,
    value: &str,
) -> Result<(Vec<(Principal, PublishableSnapshot)>, usize), SetStatusError> {
    let rows = sqlx::query(
        "UPDATE tollgate_snapshots
            SET generation = generation + 1,
                snapshot   = jsonb_set(snapshot, $3::text[], to_jsonb($2::text))
          WHERE account_id = $1
            AND deleted = FALSE
            AND snapshot #>> $3::text[] IS DISTINCT FROM $2::text
        RETURNING principal, generation, snapshot",
    )
    .bind(id_bytes(account.0))
    .bind(value)
    .bind(json_path)
    .fetch_all(&mut **tx)
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
            // Skipped for push, not fatal: this row was already unreadable
            // before the change touched it, and refusing to suspend or
            // reclassify an account because one of its credentials is corrupt
            // is the worse outcome. Reported, never silent (INVARIANTS.md
            // #19). Counted as well as logged: the row changed durably but
            // will not be pushed, so those principals converge only at their
            // next refresh.
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
    // Ordered, so both backends emit the same sequence and a mirrored test
    // need not assert on incidental ordering.
    republished.sort_unstable_by_key(|(principal, _)| *principal);
    Ok((republished, unreadable))
}
#[async_trait]
impl AdminStore for PostgresStore {
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<tollgate_store::AdminReceipt<()>, CreateAccountError> {
        let result = sqlx::query(
            "INSERT INTO tollgate_accounts
             (account_id, balance, deposited, status, capacity_class, next_fence,
              usage_recorded, settlement_loss, overage_recorded)
             VALUES ($1, $2, $2, $3, $4, 1, 0, 0, 0)
             ON CONFLICT (account_id) DO NOTHING",
        )
        .bind(id_bytes(config.account_id.0))
        .bind(to_i64(config.initial_balance, "balance").map_err(CreateAccountError::Storage)?)
        .bind(config.status.as_str())
        .bind(config.capacity_class.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| CreateAccountError::Storage(storage(e)))?;
        if result.rows_affected() == 0 {
            return Err(CreateAccountError::AlreadyExists);
        }
        Ok(AdminReceipt::new(
            (),
            AdminState::Absent,
            AdminState::AccountCreated {
                initial_balance: config.initial_balance,
                status: config.status,
                capacity_class: config.capacity_class,
            },
        ))
    }

    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<AdminReceipt<()>, AllocateError> {
        let row = sqlx::query(
            "UPDATE tollgate_accounts
             SET balance = balance + $2, deposited = deposited + $2
             WHERE account_id = $1
             RETURNING balance - $2 AS old_topup, deposited - $2 AS old_deposited,
                       balance AS new_topup, deposited AS new_deposited",
        )
        .bind(id_bytes(account.0))
        .bind(to_i64(units, "deposit").map_err(AllocateError::Storage)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(alloc_storage)?
        .ok_or(AllocateError::UnknownAccount)?;
        let state = |topup: &str, deposited: &str| -> Result<AdminState, StoreError> {
            Ok(AdminState::Funding {
                topup: to_units(row.get(topup), "audit topup")?,
                deposited: to_units(row.get(deposited), "audit deposited")?,
            })
        };
        Ok(AdminReceipt::new(
            (),
            state("old_topup", "old_deposited")?,
            state("new_topup", "new_deposited")?,
        ))
    }

    async fn set_budget_schedule(
        &self,
        account: AccountId,
        schedule: Option<BudgetSchedule>,
    ) -> Result<(), BudgetError> {
        // No deposit here. Were setting a schedule also a funding operation,
        // an operator correcting a mistyped allowance would fund the account
        // twice, and there would be no way to describe next month's budget
        // without paying it today. The first allowance arrives at the first
        // `roll_period` after this lands.
        //
        // All three columns are written together, NULL together, which is what
        // the row's all-or-nothing CHECK enforces: half a schedule satisfies
        // neither branch of the rollover.
        let allowance = schedule
            .map(|s| to_i64(s.allowance, "budget allowance"))
            .transpose()
            .map_err(BudgetError::Storage)?;
        let result = sqlx::query(
            "UPDATE tollgate_accounts
             SET budget_allowance = $2, budget_period = $3, budget_rollover = $4
             WHERE account_id = $1",
        )
        .bind(id_bytes(account.0))
        .bind(allowance)
        .bind(schedule.map(|s| s.period.as_str()))
        .bind(schedule.map(|s| s.rollover.as_str()))
        .execute(&self.pool)
        .await
        .map_err(|e| BudgetError::Storage(storage(e)))?;
        if result.rows_affected() == 0 {
            return Err(BudgetError::UnknownAccount);
        }
        Ok(())
    }

    async fn roll_due_periods(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<RolloverBatch, StoreError> {
        let limit_i = i64::try_from(limit.get())
            .map_err(|_| StoreError(format!("rollover batch limit exceeds i64 range: {limit}")))?;
        let mut rolled = Vec::new();
        // One statement per period kind, because the boundary is a property of
        // the period: a weekly schedule and a monthly one are due at different
        // instants, and a single comparison could only be right for one of
        // them. The exhaustive match in `every_period_is_swept` is what makes
        // a new variant a compile error here rather than a silently unswept
        // schedule.
        for period in Period::ALL {
            let boundary_us = ts_micros(period.start_of(now));
            let remaining = limit_i
                - i64::try_from(rolled.len())
                    .map_err(|_| StoreError("rollover batch row count exceeds i64 range".into()))?;
            if remaining <= 0 {
                break;
            }
            // One statement, so the selection and the crossing cannot be
            // separated. `FOR UPDATE SKIP LOCKED` is what lets replicas
            // cooperate: a concurrent pass skips the rows this one holds, and
            // once this commits their `period_start_us < boundary` test is
            // false — so a boundary is crossed exactly once however many
            // passes race it.
            //
            // The prior `allowance_balance` is read in the CTE because the
            // UPDATE cannot return it: PostgreSQL's RETURNING sees the new row
            // only, and `expired` has to be reported as the delta it is.
            let rows = sqlx::query(
                "WITH due AS (
                     SELECT account_id, allowance_balance AS prior, budget_allowance AS allowance
                     FROM tollgate_accounts
                     WHERE budget_period = $1 AND period_start_us < $2
                     ORDER BY account_id LIMIT $3 FOR UPDATE SKIP LOCKED
                 )
                 UPDATE tollgate_accounts AS account SET
                     deposited = account.deposited + due.allowance,
                     expired = account.expired + due.prior,
                     balance = account.balance - due.prior + due.allowance,
                     allowance_balance = due.allowance,
                     period_start_us = $2
                 FROM due
                 WHERE account.account_id = due.account_id
                 RETURNING due.account_id, due.allowance, due.prior",
            )
            .bind(period.as_str())
            .bind(boundary_us)
            .bind(remaining)
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;

            for row in rows {
                rolled.push(RolledAccount {
                    account_id: AccountId(id_from(&row.get::<Vec<u8>, _>(0))),
                    deposited: to_units(row.get::<i64, _>(1), "budget allowance")?,
                    expired: to_units(row.get::<i64, _>(2), "expiring allowance")?,
                });
            }
        }
        RolloverBatch::try_new(rolled, limit)
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<tollgate_store::AdminReceipt<StatusChange>, SetStatusError> {
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

            let before = decode_status(row.get::<String, _>(0))?;
            if before == AccountStatus::Closed && status != AccountStatus::Closed {
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

            let (republished, unreadable) =
                republish_patched_snapshots(&mut tx, account, "{status}", status.as_str()).await?;
            Ok((before, republished, unreadable))
        }
        .await;

        let (before, republished, unreadable) = finish_transaction(tx, result).await?;
        // Publish only after commit, as with snapshot publication itself.
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
        Ok(AdminReceipt::new(
            StatusChange {
                // Rows that changed durably: the ones pushed, plus any that could
                // not be decoded to push. The caller is told both numbers.
                republished: count + unreadable,
                unreadable,
            },
            AdminState::Status { status: before },
            AdminState::Status { status },
        ))
    }

    async fn set_capacity_class(
        &self,
        account: AccountId,
        class: CapacityClass,
    ) -> Result<tollgate_store::AdminReceipt<StatusChange>, SetStatusError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // `FOR UPDATE` for the reason `set_account_status` takes it: the
            // account row's lock is the serialization point, so two concurrent
            // class changes cannot interleave their snapshot updates and leave
            // the ledger saying one thing while some snapshots say another.
            // It also serialises against a concurrent status change, which is
            // what keeps the two account-owned facts from racing each other's
            // republications.
            let row = sqlx::query(
                "SELECT status, capacity_class FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(SetStatusError::UnknownAccount)?;

            let before = decode_capacity_class(row.get::<String, _>(1))?;
            // Closed is terminal, so reclassifying is meaningless. Unlike a
            // status change there is no "already at the target" escape: every
            // class is equally meaningless on a closed account. Rolling back
            // here is what makes the refusal change nothing.
            if decode_status(row.get::<String, _>(0))? == AccountStatus::Closed {
                return Err(SetStatusError::AccountClosed);
            }

            sqlx::query("UPDATE tollgate_accounts SET capacity_class = $2 WHERE account_id = $1")
                .bind(id_bytes(account.0))
                .bind(class.as_str())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;

            let (republished, unreadable) =
                republish_patched_snapshots(&mut tx, account, "{capacity_class}", class.as_str()).await?;
            Ok((before, republished, unreadable))
        }
        .await;

        let (before, republished, unreadable) = finish_transaction(tx, result).await?;
        if pushes_exceed_capacity(republished.len()) {
            tracing::warn!(
                %account,
                principals = republished.len(),
                capacity = PUSH_CHANNEL_CAPACITY,
                "capacity class change emitted more pushes than the channel holds; \
                 subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(AdminReceipt::new(
            StatusChange {
                // Rows that changed durably, whether or not they could be decoded
                // for a push — the same accounting `set_account_status` reports.
                republished: count + unreadable,
                unreadable,
            },
            AdminState::CapacityClass {
                capacity_class: before,
            },
            AdminState::CapacityClass {
                capacity_class: class,
            },
        ))
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<tollgate_store::AdminReceipt<()>, PublishSnapshotError> {
        let generation = i64::try_from(snapshot.generation.0).map_err(|_| {
            StoreError("snapshot generation exceeds PostgreSQL BIGINT range".into())
        })?;

        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            if let Some(key_id) = snapshot.key_id {
                let matches: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM tollgate_credential_keys
                     WHERE key_id = $1 AND principal = $2 AND account_id = $3)",
                )
                .bind(id_bytes(key_id.0))
                .bind(id_bytes(principal.0))
                .bind(id_bytes(snapshot.account_id.0))
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
                if !matches {
                    return Err(PublishSnapshotError::CredentialMismatch { key_id });
                }
            }
            // The ledger decides an account's status; a publish may carry it
            // but not change it, or the two records `set_account_status`
            // unified could be pulled apart again one principal at a time
            // (#51). FOR SHARE, not FOR UPDATE: this only has to hold the
            // status still, and a status change takes FOR UPDATE on the same
            // row, so the two serialize without publishes blocking each other.
            //
            // The budget columns ride along on the read that was already being
            // taken, under the same lock, so the view stamped below is the
            // ledger as of this publication rather than a second read that
            // could straddle a lease or a rollover (#97).
            let ledger = sqlx::query(
                "SELECT status, deposited, overage_recorded, usage_recorded, settlement_loss,
                        expired, budget_allowance, budget_period, budget_rollover, period_start_us,
                        capacity_class
                 FROM tollgate_accounts WHERE account_id = $1 FOR SHARE",
            )
            .bind(id_bytes(snapshot.account_id.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;

            // An account the ledger does not hold publishes unchanged: this
            // adds no account-existence requirement to publication, and an
            // unstamped snapshot reports no budget rather than a zero.
            // Unconditional, including the `None` arm: a snapshot arrives here
            // having crossed a wire, where nothing stops a publisher putting a
            // balance in the JSON. Overwriting always is what makes the store
            // the field's only writer, rather than only usually.
            let view = match &ledger {
                Some(row) => {
                    let ledger = decode_status(row.get::<String, _>(0))?;
                    if ledger != snapshot.status {
                        return Err(PublishSnapshotError::StatusMismatch {
                            ledger,
                            submitted: snapshot.status,
                        });
                    }
                    // The capacity class is the same kind of fact and gets the
                    // same guard (#99): the ledger owns it, a publish may
                    // carry it, and only `set_capacity_class` may change it.
                    let ledger_class = decode_capacity_class(row.get::<String, _>(10))?;
                    if ledger_class != snapshot.capacity_class {
                        return Err(PublishSnapshotError::CapacityClassMismatch {
                            ledger: ledger_class,
                            submitted: snapshot.capacity_class,
                        });
                    }
                    Some(budget_view(row)?)
                }
                None => None,
            };
            let published = snapshot.with_budget(view);
            let value = serde_json::to_value(StoredSnapshotRef::from(published.as_snapshot()))
                .map_err(|e| StoreError(format!("snapshot encode: {e}")))?;

            let (written, before, after) =
                write_snapshot_audited(&mut tx, principal, generation, value).await?;
            Ok((written, published, before, after))
        }
        .await;

        let (written, published, before, after) = finish_transaction(tx, result).await?;
        if written {
            // The stamped snapshot, not the submitted one: a subscriber must
            // receive exactly what was stored, or a pushed instance and a
            // pulling one would disagree about the account's balance.
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(published),
            });
        }
        Ok(AdminReceipt::new((), before, after))
    }

    async fn remove_snapshot(&self, principal: Principal) -> Result<AdminReceipt<()>, StoreError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            let before = snapshot_audit_row(&mut tx, principal).await?;
            let after = match before {
                AdminState::Snapshot {
                    generation,
                    revoked: false,
                } => {
                    sqlx::query(
                        "UPDATE tollgate_snapshots SET deleted = TRUE WHERE principal = $1",
                    )
                    .bind(id_bytes(principal.0))
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                    AdminState::Snapshot {
                        generation,
                        revoked: true,
                    }
                }
                state => state,
            };
            Ok(AdminReceipt::new((), before, after))
        }
        .await;
        let receipt = finish_transaction(tx, result).await?;
        if receipt.before != receipt.after
            && let AdminState::Snapshot { generation, .. } = receipt.after
        {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Revoked { generation },
            });
        }
        Ok(receipt)
    }
}

/// Holding the snapshot row while both observing and replacing it makes the
/// receipt exact even when another server publishes or revokes concurrently.
async fn snapshot_audit_row(
    tx: &mut Transaction<'_, Postgres>,
    principal: Principal,
) -> Result<AdminState, StoreError> {
    let row = sqlx::query(
        "SELECT generation, deleted FROM tollgate_snapshots WHERE principal = $1 FOR UPDATE",
    )
    .bind(id_bytes(principal.0))
    .fetch_optional(&mut **tx)
    .await
    .map_err(storage)?;
    row.map(|row| {
        Ok(AdminState::Snapshot {
            generation: generation_from(row.get(0))?,
            revoked: row.get(1),
        })
    })
    .unwrap_or(Ok(AdminState::Absent))
}

async fn write_snapshot_audited(
    tx: &mut Transaction<'_, Postgres>,
    principal: Principal,
    generation: i64,
    value: serde_json::Value,
) -> Result<(bool, AdminState, AdminState), StoreError> {
    let mut before = snapshot_audit_row(tx, principal).await?;
    let after = AdminState::Snapshot {
        generation: generation_from(generation)?,
        revoked: false,
    };
    if before == AdminState::Absent {
        let inserted = sqlx::query(
            "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
            VALUES ($1, $2, $3, FALSE) ON CONFLICT (principal) DO NOTHING",
        )
        .bind(id_bytes(principal.0))
        .bind(generation)
        .bind(&value)
        .execute(&mut **tx)
        .await
        .map_err(storage)?;
        if inserted.rows_affected() == 1 {
            return Ok((true, before, after));
        }
        // Another creator won the unique constraint. READ COMMITTED sees its
        // row here; lock and observe that actual predecessor before replacing.
        before = snapshot_audit_row(tx, principal).await?;
    }
    let AdminState::Snapshot {
        generation: previous,
        ..
    } = before
    else {
        return Err(StoreError("snapshot disappeared during publication".into()));
    };
    if previous >= generation_from(generation)? {
        return Ok((false, before, before));
    }
    sqlx::query("UPDATE tollgate_snapshots SET generation = $2, snapshot = $3, deleted = FALSE WHERE principal = $1")
        .bind(id_bytes(principal.0)).bind(generation).bind(value)
        .execute(&mut **tx).await.map_err(storage)?;
    Ok((true, before, after))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_fences_use_the_exact_positive_bigint_domain() {
        for invalid in [i64::MIN, -1, 0] {
            assert!(stored_fence(invalid).is_err());
        }
        for valid in [1, 2, i64::MAX] {
            assert_eq!(stored_fence(valid).unwrap(), FencingToken(valid as u64));
        }
    }

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
            push,
        };

        assert!(store.ping().await.is_err());
    }
}
