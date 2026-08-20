//! PostgreSQL backend.
//!
//! Reproduces `MemoryStore`'s settlement rules exactly — the shared
//! correctness suite (`tests/postgres_suite.rs`, mirroring
//! `tollgate-store/tests/store_suite.rs` by name) is the proof. Concurrency
//! control is row-level: `SELECT ... FOR UPDATE` on the account funds
//! acquire; on the lease it serializes release/ingest/reclaim against each
//! other. Fencing tokens come from the account row's `next_fence` counter,
//! so they are strictly monotonic per account across any number of servers
//! sharing the database.
//!
//! Representation choices (PoC-pragmatic, documented):
//! - u128 ids as 16-byte `BYTEA` (big-endian);
//! - units as `BIGINT` with checked u64↔i64 conversion (a balance beyond
//!   i64::MAX is refused, not wrapped);
//! - timestamps as `BIGINT` microseconds since the Unix epoch;
//! - snapshots as `JSONB` of the wire serialization.
//!
//! Snapshot pushes broadcast in-process only; cross-process push
//! (LISTEN/NOTIFY or the server's future SSE) is a documented seam in
//! `docs/DESIGN.md`.

use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, LeaseGrant, LeaseId, Principal, UsageEvent,
};
use tollgate_store::memory::Conservation;
use tollgate_store::{
    AccountConfig, AdminStore, AllocateError, CreateAccountError, GrantPolicy, IngestReport,
    LeaseAllocator, ReclaimedLease, SnapshotPush, SnapshotSource, StoreError, StoreHealth,
    UsageSink,
};

const STATE_ACTIVE: i16 = 0;
const STATE_RELEASED: i16 = 1;
const STATE_EXPIRED: i16 = 2;

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

fn to_units(value: i64) -> CostUnits {
    CostUnits(u64::try_from(value).unwrap_or(0))
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

pub struct PostgresStore {
    pool: PgPool,
    policy: GrantPolicy,
    push: broadcast::Sender<SnapshotPush>,
}

impl PostgresStore {
    /// Connect and run pending migrations (versioned under ./migrations,
    /// tracked by sqlx's _sqlx_migrations table — review finding #11).
    pub async fn connect(url: &str, policy: GrantPolicy) -> Result<Arc<Self>, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await
            .map_err(storage)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| StoreError(format!("migrate: {e}")))?;
        let (push, _) = broadcast::channel(256);
        Ok(Arc::new(PostgresStore { pool, policy, push }))
    }

    /// Test/reset helper: drop all quota rows (not the schema).
    pub async fn truncate_all(&self) -> Result<(), StoreError> {
        sqlx::raw_sql(
            "TRUNCATE tollgate_usage_events, tollgate_leases, tollgate_snapshots, tollgate_accounts CASCADE",
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
        Ok(row
            .map(|r| to_units(r.get::<i64, _>(0)))
            .unwrap_or(CostUnits::ZERO))
    }

    pub async fn usage_recorded(&self, account: AccountId) -> Result<CostUnits, StoreError> {
        let row = sqlx::query("SELECT usage_recorded FROM tollgate_accounts WHERE account_id = $1")
            .bind(id_bytes(account.0))
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        Ok(row
            .map(|r| to_units(r.get::<i64, _>(0)))
            .unwrap_or(CostUnits::ZERO))
    }

    pub async fn conservation(
        &self,
        account: AccountId,
    ) -> Result<Option<Conservation>, StoreError> {
        let Some(row) = sqlx::query(
            "SELECT deposited, balance, usage_recorded, settlement_loss
             FROM tollgate_accounts WHERE account_id = $1",
        )
        .bind(id_bytes(account.0))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?
        else {
            return Ok(None);
        };
        let lease_row = sqlx::query(
            "SELECT COALESCE(SUM(granted), 0)::BIGINT, COALESCE(SUM(used), 0)::BIGINT
             FROM tollgate_leases WHERE account_id = $1 AND state = 0",
        )
        .bind(id_bytes(account.0))
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        let active_grants = to_units(lease_row.get::<i64, _>(0));
        let active_used = to_units(lease_row.get::<i64, _>(1));
        Ok(Some(Conservation {
            deposited: to_units(row.get::<i64, _>(0)),
            balance: to_units(row.get::<i64, _>(1)),
            active_lease_grants: active_grants,
            settled_usage: to_units(row.get::<i64, _>(2))
                .checked_sub(active_used)
                .expect("active usage never exceeds recorded usage"),
            settlement_loss: to_units(row.get::<i64, _>(3)),
        }))
    }
}

/// Lock one lease row. Returns (account_id, fencing_token, granted, used,
/// credited, expires_at_us, state).
async fn lock_lease(
    tx: &mut Transaction<'_, Postgres>,
    lease_id: LeaseId,
) -> Result<Option<(Vec<u8>, i64, i64, i64, i64, i64, i16)>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT account_id, fencing_token, granted, used, credited, expires_at_us, state
         FROM tollgate_leases WHERE lease_id = $1 FOR UPDATE",
    )
    .bind(id_bytes(lease_id.0))
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|r| {
        (
            r.get(0),
            r.get(1),
            r.get(2),
            r.get(3),
            r.get(4),
            r.get(5),
            r.get(6),
        )
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
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let row = sqlx::query(
            "SELECT balance, active, next_fence FROM tollgate_accounts
             WHERE account_id = $1 FOR UPDATE",
        )
        .bind(id_bytes(account.0))
        .fetch_optional(&mut *tx)
        .await
        .map_err(alloc_storage)?
        .ok_or(AllocateError::UnknownAccount)?;

        if !row.get::<bool, _>(1) {
            return Err(AllocateError::AccountInactive);
        }
        let balance = to_units(row.get::<i64, _>(0));
        let granted = self
            .policy
            .grant(requested, balance)
            .ok_or(AllocateError::InsufficientBalance)?;
        let fence = row.get::<i64, _>(2);

        let ttl = if ttl > self.policy.max_ttl {
            self.policy.max_ttl
        } else {
            ttl
        };
        let expires_at = now
            .checked_add(ttl)
            .map_err(|e| AllocateError::Storage(StoreError(format!("ttl overflow: {e}"))))?;
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
        tx.commit().await.map_err(alloc_storage)?;

        Ok(LeaseGrant {
            lease_id,
            account_id: account,
            fencing_token: FencingToken(u64::try_from(fence).unwrap_or(0)),
            units: granted,
            expires_at,
        })
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError> {
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let (account_id, fence, granted, used, _credited, expires_at_us, state) =
            lock_lease(&mut tx, lease_id)
                .await
                .map_err(alloc_storage)?
                .ok_or(AllocateError::UnknownLease)?;

        if u64::try_from(fence).unwrap_or(0) != fencing_token.0 {
            return Err(AllocateError::Fenced);
        }
        // Releases are accepted through the grace window (see GrantPolicy::
        // reclaim_grace) — only a settled or grace-exhausted lease refuses.
        let release_deadline_us =
            expires_at_us.saturating_add(self.policy.reclaim_grace.as_micros() as i64);
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
        tx.commit().await.map_err(alloc_storage)
    }

    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        // Reclaim only once the grace window past expiry has fully lapsed:
        // expires_at + grace <= now  ⟺  expires_at_us <= now_us - grace_us.
        let threshold_us =
            ts_micros(now).saturating_sub(self.policy.reclaim_grace.as_micros() as i64);
        // SKIP LOCKED: concurrent sweeps cooperate instead of deadlocking.
        let rows = sqlx::query(
            "SELECT lease_id, account_id, granted, used FROM tollgate_leases
             WHERE state = 0 AND expires_at_us <= $1
             FOR UPDATE SKIP LOCKED",
        )
        .bind(threshold_us)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage)?;

        let mut reclaimed = Vec::with_capacity(rows.len());
        for row in rows {
            let lease_bytes: Vec<u8> = row.get(0);
            let account_bytes: Vec<u8> = row.get(1);
            let credit: i64 = row.get::<i64, _>(2) - row.get::<i64, _>(3);
            sqlx::query("UPDATE tollgate_leases SET state = $2, credited = $3 WHERE lease_id = $1")
                .bind(&lease_bytes)
                .bind(STATE_EXPIRED)
                .bind(credit)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            sqlx::query(
                "UPDATE tollgate_accounts SET balance = balance + $2 WHERE account_id = $1",
            )
            .bind(&account_bytes)
            .bind(credit)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            reclaimed.push(ReclaimedLease {
                lease_id: LeaseId(id_from(&lease_bytes)),
                account_id: AccountId(id_from(&account_bytes)),
                reclaimed: to_units(credit),
            });
        }
        tx.commit().await.map_err(storage)?;
        Ok(reclaimed)
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
        // lands via one bulk insert plus grouped per-lease/per-account
        // updates. Classification in application code preserves the partial
        // acceptance contract without savepoints.
        let mut report = IngestReport::default();
        if events.is_empty() {
            return Ok(report);
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;

        // Lock every referenced lease, in stable order.
        let mut lease_ids: Vec<Vec<u8>> = events
            .iter()
            .map(|e| id_bytes(e.lease_id.0))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        lease_ids.sort();
        struct LeaseRow {
            account_id: Vec<u8>,
            fence: i64,
            granted: i64,
            used: i64,
            used_delta: i64,
            credited: i64,
            settled: bool,
        }
        let rows = sqlx::query(
            "SELECT lease_id, account_id, fencing_token, granted, used, credited, state
             FROM tollgate_leases WHERE lease_id = ANY($1) ORDER BY lease_id FOR UPDATE",
        )
        .bind(&lease_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage)?;
        let mut leases: std::collections::HashMap<Vec<u8>, LeaseRow> = rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<Vec<u8>, _>(0),
                    LeaseRow {
                        account_id: r.get(1),
                        fence: r.get(2),
                        granted: r.get(3),
                        used: r.get(4),
                        used_delta: 0,
                        credited: r.get(5),
                        settled: r.get::<i16, _>(6) != STATE_ACTIVE,
                    },
                )
            })
            .collect();

        // Existing request ids in one lookup.
        let request_ids: Vec<Vec<u8>> = events.iter().map(|e| id_bytes(e.request_id.0)).collect();
        let mut seen: std::collections::HashSet<Vec<u8>> =
            sqlx::query("SELECT request_id FROM tollgate_usage_events WHERE request_id = ANY($1)")
                .bind(&request_ids)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage)?
                .into_iter()
                .map(|r| r.get::<Vec<u8>, _>(0))
                .collect();

        // Classify in memory against the locked rows (identical rules to
        // MemoryStore: fencing triple, then the conservation fit that also
        // converts a released lease's provisional loss into billed usage).
        struct Accepted<'a> {
            event: &'a UsageEvent,
            settled: bool,
        }
        let mut accepted: Vec<Accepted<'_>> = Vec::with_capacity(events.len());
        for event in events {
            let rid = id_bytes(event.request_id.0);
            if seen.contains(&rid) {
                report.duplicate += 1;
                continue;
            }
            let Some(lease) = leases.get_mut(&id_bytes(event.lease_id.0)) else {
                report.rejected += 1;
                continue;
            };
            if u64::try_from(lease.fence).unwrap_or(0) != event.fencing_token.0
                || id_from(&lease.account_id) != event.account_id.0
            {
                report.rejected += 1;
                continue;
            }
            let units = to_i64(event.units, "units")?;
            if units > lease.granted - lease.used - lease.used_delta - lease.credited {
                report.rejected += 1;
                continue;
            }
            lease.used_delta += units;
            seen.insert(rid);
            accepted.push(Accepted {
                event,
                settled: lease.settled,
            });
            report.accepted += 1;
        }

        if !accepted.is_empty() {
            // Bulk insert the accepted events.
            let (mut rid, mut acct, mut lease, mut fence, mut units, mut at) = (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            for a in &accepted {
                rid.push(id_bytes(a.event.request_id.0));
                acct.push(id_bytes(a.event.account_id.0));
                lease.push(id_bytes(a.event.lease_id.0));
                fence.push(i64::try_from(a.event.fencing_token.0).unwrap_or(0));
                units.push(to_i64(a.event.units, "units")?);
                at.push(ts_micros(a.event.occurred_at));
            }
            sqlx::query(
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

            // Grouped per-lease and per-account aggregate updates.
            let mut account_deltas: std::collections::HashMap<Vec<u8>, (i64, i64)> =
                std::collections::HashMap::new();
            for a in &accepted {
                let units = to_i64(a.event.units, "units")?;
                let entry = account_deltas
                    .entry(id_bytes(a.event.account_id.0))
                    .or_insert((0, 0));
                entry.0 += units;
                if a.settled {
                    entry.1 += units;
                }
            }
            for (lease_id, row) in leases.iter().filter(|(_, r)| r.used_delta > 0) {
                sqlx::query("UPDATE tollgate_leases SET used = used + $2 WHERE lease_id = $1")
                    .bind(lease_id)
                    .bind(row.used_delta)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            for (account_id, (usage_delta, loss_delta)) in &account_deltas {
                sqlx::query(
                    "UPDATE tollgate_accounts
                     SET usage_recorded = usage_recorded + $2,
                         settlement_loss = settlement_loss - $3
                     WHERE account_id = $1",
                )
                .bind(account_id)
                .bind(usage_delta)
                .bind(loss_delta)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(report)
    }
}

#[async_trait]
impl SnapshotSource for PostgresStore {
    async fn snapshot(
        &self,
        principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        let row = sqlx::query("SELECT snapshot FROM tollgate_snapshots WHERE principal = $1")
            .bind(id_bytes(principal.0))
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        match row {
            Some(row) => {
                let value: serde_json::Value = row.get(0);
                let snapshot: AccountSnapshot = serde_json::from_value(value)
                    .map_err(|e| StoreError(format!("snapshot decode: {e}")))?;
                Ok(Some(Arc::new(snapshot)))
            }
            None => Ok(None),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
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
             (account_id, balance, deposited, active, next_fence, usage_recorded, settlement_loss)
             VALUES ($1, $2, $2, $3, 1, 0, 0)
             ON CONFLICT (account_id) DO NOTHING",
        )
        .bind(id_bytes(config.account_id.0))
        .bind(to_i64(config.initial_balance, "balance").map_err(CreateAccountError::Storage)?)
        .bind(config.active)
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

    async fn set_active(&self, account: AccountId, active: bool) -> Result<(), AllocateError> {
        let result = sqlx::query("UPDATE tollgate_accounts SET active = $2 WHERE account_id = $1")
            .bind(id_bytes(account.0))
            .bind(active)
            .execute(&self.pool)
            .await
            .map_err(alloc_storage)?;
        if result.rows_affected() == 0 {
            return Err(AllocateError::UnknownAccount);
        }
        Ok(())
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
    ) -> Result<(), StoreError> {
        let value = serde_json::to_value(&*snapshot)
            .map_err(|e| StoreError(format!("snapshot encode: {e}")))?;
        sqlx::query(
            "INSERT INTO tollgate_snapshots (principal, generation, snapshot)
             VALUES ($1, $2, $3)
             ON CONFLICT (principal) DO UPDATE
             SET generation = EXCLUDED.generation, snapshot = EXCLUDED.snapshot
             WHERE tollgate_snapshots.generation < EXCLUDED.generation",
        )
        .bind(id_bytes(principal.0))
        .bind(i64::try_from(snapshot.generation.0).unwrap_or(i64::MAX))
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        let _ = self.push.send(SnapshotPush {
            principal,
            snapshot,
        });
        Ok(())
    }

    async fn remove_snapshot(&self, principal: Principal) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM tollgate_snapshots WHERE principal = $1")
            .bind(id_bytes(principal.0))
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }
}
