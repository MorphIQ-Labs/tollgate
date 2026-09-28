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
//! - lease and credential expiry as floor `BIGINT` microseconds plus a
//!   `SMALLINT` nanosecond remainder; informational timestamps as `BIGINT`
//!   microseconds since the Unix epoch;
//! - snapshots as storage-local `JSONB`: ids in the legacy u64 range remain
//!   numeric for rollback, larger ids use canonical text, and the public
//!   HTTP/Serde contract always uses text.
//!
//! Snapshot pushes broadcast in-process only; cross-process push
//! (LISTEN/NOTIFY or the server's future SSE) is a documented seam in
//! `docs/DESIGN.md`.

#![deny(missing_docs)]

pub(crate) mod instant;

#[cfg(feature = "test-support")]
pub mod test_support;

use instant::StoredInstant;
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
    AccountConfig, AccountView, AdminAuthority, AdminReceipt, AdminState, AdminStore,
    AllocateError, Allocation, BudgetError, Conservation, CreateAccountError, GrantPolicy,
    IngestError, IngestReport, KeyDirectory, KeyError, KeyRecord, KeySnapshotError, KeySummary,
    LeaseAllocator, PUSH_CHANNEL_CAPACITY, PublishSnapshotError, ReclaimBatch, ReclaimedLease,
    Revocation, RolledAccount, RolloverBatch, SetStatusError, SnapshotPush, SnapshotResolution,
    SnapshotSource, StatusChange, StoreError, StoreHealth, UsageSink, pushes_exceed_capacity,
    validate_key_page_limit,
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
        self.insert_credential(record, None)
            .await
            .map(|receipt| receipt.outcome)
    }

    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError> {
        self.revoke_key_audited(key_id, now)
            .await
            .map(|receipt| receipt.outcome)
    }

    async fn revoke_key_audited(
        &self,
        key_id: KeyId,
        now: Timestamp,
    ) -> Result<AdminReceipt<Revocation>, KeyError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // The row lock captures the actual predecessor, including a prior
            // retirement committed while this call waited. No account lock is
            // acquired after the credential lock.
            let row = sqlx::query(
                "SELECT account_id, revoked_at_us IS NOT NULL
                 FROM tollgate_credential_keys WHERE key_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(key_id.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(KeyError::UnknownKey)?;
            let account_bytes: Vec<u8> = row.get(0);
            let account_bytes: [u8; 16] = account_bytes
                .try_into()
                .map_err(|_| StoreError("credential account identifier is not 16 bytes".into()))?;
            let account_id = AccountId(u128::from_be_bytes(account_bytes));
            let revoked: bool = row.get(1);
            let before = AdminState::Credential {
                account_id,
                key_id,
                revoked,
            };
            let outcome = if revoked {
                Revocation::AlreadyRetired
            } else {
                sqlx::query(
                    "UPDATE tollgate_credential_keys SET revoked_at_us = $2 WHERE key_id = $1",
                )
                .bind(id_bytes(key_id.0))
                .bind(ts_micros(now))
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                Revocation::Retired
            };
            Ok(AdminReceipt::new(
                outcome,
                before,
                AdminState::Credential {
                    account_id,
                    key_id,
                    revoked: true,
                },
            ))
        }
        .await;
        finish_transaction(tx, result).await
    }

    async fn publish_key_snapshot(
        &self,
        account: AccountId,
        key: KeyId,
        snapshot: PublishableSnapshot,
    ) -> Result<AdminReceipt<()>, KeySnapshotError> {
        self.publish_key_snapshot_with_generation(account, key, snapshot, false)
            .await
    }

    async fn publish_key_snapshot_next(
        &self,
        account: AccountId,
        key: KeyId,
        snapshot: PublishableSnapshot,
    ) -> Result<AdminReceipt<()>, KeySnapshotError> {
        self.publish_key_snapshot_with_generation(account, key, snapshot, true)
            .await
    }

    async fn remove_key_snapshot(
        &self,
        account: AccountId,
        key: KeyId,
    ) -> Result<AdminReceipt<()>, KeySnapshotError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            let (principal, _revoked) = lock_account_key(&mut tx, account, key).await?;
            Ok::<_, KeySnapshotError>((principal, remove_in_tx(&mut tx, principal).await?))
        }
        .await;
        let (principal, receipt) = finish_transaction(tx, result).await?;
        self.announce_removal(principal, &receipt);
        Ok(receipt)
    }

    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        let cutoff = StoredInstant::from(now);
        // Expiry is applied here, beside revocation, so this backend answers
        // "active" exactly as `MemoryStore` does and a projection built from
        // either sees the same live set. Ordering is explicit for the same
        // reason: two instances must not build tables that differ by row
        // order alone.
        let rows = sqlx::query(
            "SELECT key_id, account_id, principal, digest, not_after_floor_us, not_after_submicro_ns, not_after_is_lower_bound
             FROM tollgate_credential_keys
             WHERE revoked_at_us IS NULL
               AND (not_after_floor_us IS NULL OR not_after_submicro_ns IS NULL
                    OR (not_after_floor_us, not_after_submicro_ns) > ($1, $2))
             ORDER BY key_id",
        )
        .bind(cutoff.micros)
        .bind(cutoff.submicro_nanos)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;

        rows.into_iter().map(credential_from_row).collect()
    }

    async fn account_keys(
        &self,
        account: AccountId,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<KeySummary>, KeyError> {
        validate_key_page_limit(limit)?;
        // The account anchors the result in one statement snapshot: no rows
        // means no account, while a NULL key marks an existing, empty page.
        // LATERAL keeps the bounded credential scan on the account/key index;
        // separate cursor shapes preserve an indexable range in prepared plans.
        let sql = if after.is_some() {
            "SELECT page.* FROM tollgate_accounts AS account
             LEFT JOIN LATERAL (
                 SELECT key_id, not_after_floor_us, not_after_submicro_ns,
                        not_after_is_lower_bound, revoked_at_us
                 FROM tollgate_credential_keys
                 WHERE account_id = account.account_id AND key_id > $3
                 ORDER BY key_id LIMIT $2
             ) AS page ON TRUE
             WHERE account.account_id = $1 ORDER BY page.key_id"
        } else {
            "SELECT page.* FROM tollgate_accounts AS account
             LEFT JOIN LATERAL (
                 SELECT key_id, not_after_floor_us, not_after_submicro_ns,
                        not_after_is_lower_bound, revoked_at_us
                 FROM tollgate_credential_keys
                 WHERE account_id = account.account_id
                 ORDER BY key_id LIMIT $2
             ) AS page ON TRUE
             WHERE account.account_id = $1 ORDER BY page.key_id"
        };
        let mut query = sqlx::query(sql)
            .bind(id_bytes(account.0))
            .bind(i64::try_from(limit.get()).unwrap_or(i64::MAX));
        if let Some(cursor) = after {
            query = query.bind(id_bytes(cursor.0));
        }
        let rows = query.fetch_all(&self.pool).await.map_err(storage)?;
        if rows.is_empty() {
            return Err(KeyError::UnknownAccount);
        }
        rows.into_iter()
            .filter(|row| row.get::<Option<&[u8]>, _>(0).is_some())
            .map(|row| summary_from_row(row).map_err(KeyError::Storage))
            .collect()
    }

    async fn insert_key_within(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<(), KeyError> {
        self.insert_key_within_audited(record, max_active, now)
            .await
            .map(|receipt| receipt.outcome)
    }

    async fn insert_key_within_audited(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<AdminReceipt<()>, KeyError> {
        self.insert_credential(record, Some((max_active, now)))
            .await
    }
}

impl PostgresStore {
    async fn publish_key_snapshot_with_generation(
        &self,
        account: AccountId,
        key: KeyId,
        snapshot: PublishableSnapshot,
        allocate_generation: bool,
    ) -> Result<AdminReceipt<()>, KeySnapshotError> {
        let generation = if allocate_generation {
            None
        } else {
            Some(i64::try_from(snapshot.generation.0).map_err(|_| {
                StoreError("snapshot generation exceeds PostgreSQL BIGINT range".into())
            })?)
        };
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            let (principal, revoked) = lock_account_key(&mut tx, account, key).await?;
            if revoked {
                return Err(KeySnapshotError::Retired { key_id: key });
            }
            if snapshot.key_id != Some(key) {
                return Err(PublishSnapshotError::CredentialMismatch { key_id: key }.into());
            }
            let published = publish_in_tx(&mut tx, principal, generation, snapshot).await?;
            Ok((principal, published))
        }
        .await;
        let (principal, (written, published, before, after)) =
            finish_transaction(tx, result).await?;
        if written {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(published),
            });
        }
        Ok(AdminReceipt::new((), before, after))
    }

    /// Account creation for either authority (#39). `origin` is written once
    /// here, as both the creator and the author of the opening status.
    async fn create_account_as(
        &self,
        config: AccountConfig,
        origin: AdminAuthority,
    ) -> Result<tollgate_store::AdminReceipt<()>, CreateAccountError> {
        let result = sqlx::query(
            "INSERT INTO tollgate_accounts
             (account_id, balance, deposited, status, capacity_class, next_fence,
              usage_recorded, settlement_loss, overage_recorded, origin, status_set_by)
             VALUES ($1, $2, $2, $3, $4, 1, 0, 0, 0, $5, $5)
             ON CONFLICT (account_id) DO NOTHING",
        )
        .bind(id_bytes(config.account_id.0))
        .bind(to_i64(config.initial_balance, "balance").map_err(CreateAccountError::Storage)?)
        .bind(config.status.as_str())
        .bind(config.capacity_class.as_str())
        .bind(origin.as_str())
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
                origin,
            },
        ))
    }

    /// The one status transition, for either authority (#39). The hold and the
    /// write share the account row's `FOR UPDATE` lock, so an operator
    /// suspension racing a provisioner activation is ordered, never lost.
    async fn set_status_as(
        &self,
        account: AccountId,
        status: AccountStatus,
        authority: AdminAuthority,
    ) -> Result<tollgate_store::AdminReceipt<StatusChange>, SetStatusError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // The account row first, and its lock is the serialization point:
            // two concurrent status changes cannot interleave their snapshot
            // updates, so "ledger says Active, snapshots say Suspended" is
            // unrepresentable rather than merely unlikely.
            let row = sqlx::query(
                "SELECT status, origin, status_set_by
                 FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(SetStatusError::UnknownAccount)?;

            let before = decode_status(row.get::<String, _>(0))?;
            let origin = decode_authority(row.get::<String, _>(1))?;
            let before_set_by = decode_authority(row.get::<String, _>(2))?;
            if authority == AdminAuthority::Provisioner && origin != AdminAuthority::Provisioner {
                return Err(SetStatusError::NotProvisioned);
            }
            if before == AccountStatus::Closed && status != AccountStatus::Closed {
                // Terminal, and the refusal changes nothing: no ledger write,
                // no generation bump. Rolling back here is what makes that so.
                return Err(SetStatusError::AccountClosed);
            }
            if authority == AdminAuthority::Provisioner
                && before != status
                && before_set_by == AdminAuthority::Operator
            {
                return Err(SetStatusError::OperatorHold);
            }
            // A provisioner repeating an activation changes nothing, not even
            // the author: an operator who reactivated keeps that authorship.
            let set_by = if authority == AdminAuthority::Provisioner && before == status {
                before_set_by
            } else {
                authority
            };

            sqlx::query(
                "UPDATE tollgate_accounts SET status = $2, status_set_by = $3 WHERE account_id = $1",
            )
                .bind(id_bytes(account.0))
                .bind(status.as_str())
                .bind(set_by.as_str())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;

            let (republished, unreadable) =
                republish_patched_snapshots(&mut tx, account, "{status}", status.as_str()).await?;
            Ok(((before, before_set_by, set_by), republished, unreadable))
        }
        .await;

        let ((before, before_set_by, set_by), republished, unreadable) =
            finish_transaction(tx, result).await?;
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
            AdminState::Status {
                status: before,
                set_by: before_set_by,
            },
            AdminState::Status { status, set_by },
        ))
    }

    /// Both issuance APIs enter the same account-first transaction. The lock
    /// covers the optional bound check, unique-index insertion, foreign-key
    /// check and revision trigger through commit. Acquiring it after inserting
    /// would invert the bounded issuer's order and permit a deadlock.
    async fn insert_credential(
        &self,
        record: KeyRecord,
        bound: Option<(NonZeroUsize, Timestamp)>,
    ) -> Result<AdminReceipt<()>, KeyError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| KeyError::Storage(storage(e)))?;
        // Take the account row first. Counting and inserting without it is the
        // race this method exists to prevent: under READ COMMITTED neither
        // transaction sees the other's uncommitted credential, so both count
        // `max_active - 1`, both insert, and the account ends up over the
        // bound with no error raised anywhere. This is the same row
        // `set_account_status` and `acquire` serialise on, so an issuance in
        // flight also orders against a suspension.
        let account = sqlx::query(ACCOUNT_LOCK_SQL)
            .bind(id_bytes(record.account_id.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| KeyError::Storage(storage(e)))?;
        if account.is_none() {
            return Err(KeyError::UnknownAccount);
        }

        if let Some((max_active, now)) = bound {
            // Identity before the bound, and the order is load-bearing. A caller
            // that lost the response resends the same `key_id`; by then its own
            // successful write may have filled the bound, and answering
            // `ActiveKeyLimit` would tell it to retire a credential when in fact
            // its first call worked. `AlreadyExists` is both true and what makes
            // the retry safe (GL-121).
            let existing: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM tollgate_credential_keys WHERE key_id = $1 OR principal = $2",
            )
            .bind(id_bytes(record.key_id.0))
            .bind(id_bytes(record.principal.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| KeyError::Storage(storage(e)))?;
            if existing.is_some() {
                return Err(KeyError::AlreadyExists);
            }

            let cutoff = StoredInstant::from(now);
            let live: i64 = sqlx::query_scalar(LIVE_KEY_COUNT_SQL)
                .bind(id_bytes(record.account_id.0))
                .bind(cutoff.micros)
                .bind(cutoff.submicro_nanos)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| KeyError::Storage(storage(e)))?;
            if u128::from(live.max(0).unsigned_abs())
                >= u128::try_from(max_active.get()).unwrap_or(u128::MAX)
            {
                return Err(KeyError::ActiveKeyLimit { limit: max_active });
            }
        }

        let expiry = record.not_after.map(StoredInstant::from);
        let result = sqlx::query(
            "INSERT INTO tollgate_credential_keys
             (key_id, account_id, principal, digest, not_after_floor_us,
              not_after_submicro_ns, not_after_is_lower_bound, revoked_at_us)
             VALUES ($1, $2, $3, $4, $5, $6, FALSE, NULL)
             ON CONFLICT (key_id) DO NOTHING",
        )
        .bind(id_bytes(record.key_id.0))
        .bind(id_bytes(record.account_id.0))
        .bind(id_bytes(record.principal.0))
        .bind(record.digest.to_vec())
        .bind(expiry.map(|expiry| expiry.micros))
        .bind(expiry.map(|expiry| expiry.submicro_nanos))
        .execute(&mut *tx)
        .await;
        let outcome = match result {
            Ok(done) if done.rows_affected() == 0 => Err(KeyError::AlreadyExists),
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                Err(KeyError::AlreadyExists)
            }
            // No arm for the foreign key, deliberately. The account row was
            // taken `FOR UPDATE` above and its absence already answered
            // `UnknownAccount`; nothing in this crate deletes an account, so a
            // violation here cannot mean "no such account". It would mean the
            // row vanished under a held lock, and reporting that as a caller
            // error would absorb storage corruption as a routine refusal --
            // the caller would retire a credential over a broken database.
            // `Storage` is the truthful answer, and the mutation gate is what
            // noticed the old arm could not be reached to be tested.
            Err(e) => Err(KeyError::Storage(storage(e))),
        };
        outcome?;
        tx.commit()
            .await
            .map_err(|e| KeyError::Storage(storage(e)))?;
        Ok(AdminReceipt::new(
            (),
            AdminState::Absent,
            AdminState::Credential {
                account_id: record.account_id,
                key_id: record.key_id,
                revoked: false,
            },
        ))
    }
}

fn summary_from_row(row: sqlx::postgres::PgRow) -> Result<KeySummary, StoreError> {
    let bytes: Vec<u8> = row.get(0);
    let fixed: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| StoreError("credential identifier is not 16 bytes".into()))?;
    let not_after = match (row.get::<Option<i64>, _>(1), row.get::<Option<i16>, _>(2)) {
        (None, None) if !row.get::<bool, _>(3) => None,
        (Some(micros), Some(submicro_nanos)) => Some(
            StoredInstant {
                micros,
                submicro_nanos,
            }
            .timestamp()?,
        ),
        _ => return Err(StoreError("incomplete stored credential expiry".into())),
    };
    Ok(KeySummary {
        key_id: KeyId(u128::from_be_bytes(fixed)),
        not_after,
        revoked_at: row
            .get::<Option<i64>, _>(4)
            .map(|micros| {
                StoredInstant {
                    micros,
                    submicro_nanos: 0,
                }
                .timestamp()
            })
            .transpose()?,
    })
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
    let not_after = match (row.get::<Option<i64>, _>(4), row.get::<Option<i16>, _>(5)) {
        (None, None) if !row.get::<bool, _>(6) => None,
        (Some(micros), Some(submicro_nanos)) => Some(
            StoredInstant {
                micros,
                submicro_nanos,
            }
            .timestamp()?,
        ),
        _ => return Err(StoreError("incomplete stored credential expiry".into())),
    };
    Ok(KeyRecord {
        key_id,
        account_id: AccountId(id(1)?),
        principal: Principal(id(2)?),
        digest: digest_from(row.get::<Vec<u8>, _>(3).as_slice(), key_id)?,
        not_after,
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
        validate_key_page_limit(limit)?;
        let cutoff = StoredInstant::from(now);
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
            "SELECT key_id, account_id, principal, digest, not_after_floor_us, not_after_submicro_ns, not_after_is_lower_bound
             FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
             AND (not_after_floor_us IS NULL OR not_after_submicro_ns IS NULL
                  OR (not_after_floor_us, not_after_submicro_ns) > ($1, $2)) AND key_id > $3
             ORDER BY key_id LIMIT $4"
        } else {
            "SELECT key_id, account_id, principal, digest, not_after_floor_us, not_after_submicro_ns, not_after_is_lower_bound
             FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
             AND (not_after_floor_us IS NULL OR not_after_submicro_ns IS NULL
                  OR (not_after_floor_us, not_after_submicro_ns) > ($1, $2)) AND key_id >= $3
             ORDER BY key_id LIMIT $4"
        };
        let rows = sqlx::query(sql)
            .bind(cutoff.micros)
            .bind(cutoff.submicro_nanos)
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
/// and what a tombstone reports after its snapshot is gone (GL-54). `account_id`
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
    /// The account-owned execution-capacity class (GL-99). Rides the JSONB
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
    /// The consuming application's policy identity (GL-94). Rides the JSONB
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

/// Rows written before GL-54 still carry a `generation` key. Serde ignores
/// unknown fields, so those rows decode unchanged and the vestigial key is
/// simply not read — which is why this needed no backfill.
#[derive(Deserialize)]
struct StoredSnapshot {
    account_id: StoredId,
    key_id: Option<StoredId>,
    status: AccountStatus,
    /// Absent from every row written before GL-99, and `default` for the reason
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
    /// Absent from every row written before GL-94, and `default` for the reason
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
/// migration 0005's index (GL-12).
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
/// transaction instead (GL-56), which leaves this predicate — and the plan GL-12
/// pinned — untouched.
const ACTIVE_LEASE_SUM_SQL: &str =
    "SELECT COALESCE(SUM(granted), 0)::BIGINT, COALESCE(SUM(used), 0)::BIGINT
     FROM tollgate_leases WHERE account_id = $1 AND state = 0";

/// The expiry sweep's selection, named for the same reason as the sum above:
/// `reclaim_expired_batch` runs it and `explain_reclaim_due_leases` asks the
/// planner what it does with it. A test that copied the text would keep
/// reporting an index-ordered walk after the real `ORDER BY` had drifted away
/// from `tollgate_leases_expiry` — which is exactly the drift GL-65 found.
///
/// The `ORDER BY` is the index's own column order, and that is the whole
/// point. It was `(account_id, lease_id)`, which no index answers, so the
/// `LIMIT` could not stop an index walk: every batch read and sorted the
/// entire remaining backlog to return 256 rows, making a drain quadratic in
/// the backlog it exists to clear (GL-65). Ordering by the expiry pair makes
/// the `LIMIT` a range stop, and settles oldest-due-first like the reference
/// backend does.
/// Takes the account row before an issuance counts against its bound.
///
/// Hoisted so the test that demonstrates the race can drive the *same*
/// statements this method does, rather than a copy that could drift from them
/// — the convention `RECLAIM_DUE_LEASES_SQL` established.
pub(crate) const ACCOUNT_LOCK_SQL: &str =
    "SELECT 1 FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE";

/// Credentials that can still authenticate at the given instant: not revoked,
/// and not past `not_after`. The predicate the bound counts with.
pub(crate) const LIVE_KEY_COUNT_SQL: &str = "SELECT count(*) FROM tollgate_credential_keys
     WHERE account_id = $1
       AND revoked_at_us IS NULL
       AND (not_after_floor_us IS NULL OR not_after_submicro_ns IS NULL
            OR (not_after_floor_us, not_after_submicro_ns) > ($2, $3))";

const RECLAIM_DUE_LEASES_SQL: &str = "SELECT lease_id, account_id, granted, used
     FROM tollgate_leases
     WHERE state = 0 AND (expires_at_floor_us, expires_at_submicro_ns) <= ($1, $2)
     ORDER BY expires_at_floor_us, expires_at_submicro_ns
     LIMIT $3 FOR UPDATE SKIP LOCKED";

/// The rollover sweep's selection, hoisted and fixed for the same reason
/// (GL-65's sibling) — but it needed two changes, not one.
///
/// `ORDER BY account_id` sorted every due account to return one bounded page,
/// and served the lowest ids rather than the most overdue boundaries.
/// `budget_period = $1` is an equality, so within that prefix of
/// `tollgate_accounts_due_rollover (budget_period, period_start_us)` the scan
/// is already ordered by `period_start_us`: ordering by it is index-native and
/// crosses the oldest boundary first.
///
/// That alone changed nothing, because the index is *partial* on
/// `budget_allowance IS NOT NULL` and this query never said so — the planner
/// cannot apply a partial index it cannot prove applies, so the sweep had
/// never once used the index added for it. Restating the predicate is free and
/// selects exactly the same rows: `tollgate_accounts_budget_all_or_nothing` already
/// requires the allowance, period and rollover columns to be all null or all
/// non-null, and `budget_period = $1` has ruled out all-null. Measured on 400
/// accounts with five due: seq scan and sort at cost 25.02, versus an index
/// scan with no sort at 5.92.
/// The crossed-from boundary is returned so the caller can order the batch.
///
/// `RETURNING` has no defined row order: PostgreSQL emits rows in whatever
/// order the update's join produced, which is physical order, not the boundary
/// order the CTE selected by. Ordering it *here* would mean a trailing
/// `ORDER BY` and a `Sort` node, which is the cost
/// `the_rollover_sweep_reaches_its_index_instead_of_sorting_the_due_set`
/// forbids (GL-65) — so the page is ordered in Rust instead, where it is bounded
/// by the batch limit rather than by how many accounts are due.
const DUE_PERIODS_SQL: &str = "WITH due AS (
         SELECT account_id, allowance_balance AS prior, budget_allowance AS allowance,
                period_start_us AS crossed_from
         FROM tollgate_accounts
         WHERE budget_allowance IS NOT NULL
           AND budget_period = $1 AND period_start_us < $2
         ORDER BY period_start_us LIMIT $3 FOR UPDATE SKIP LOCKED
     )
     UPDATE tollgate_accounts AS account SET
         deposited = account.deposited + due.allowance,
         expired = account.expired + due.prior,
         balance = account.balance - due.prior + due.allowance,
         allowance_balance = due.allowance,
         period_start_us = $2
     FROM due
     WHERE account.account_id = due.account_id
     RETURNING due.account_id, due.allowance, due.prior, due.crossed_from";

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

/// Fixture reset is available only in the opt-in `test_support` module,
/// never as a method on a normal store handle.
///
/// ```compile_fail,E0599
/// # use tollgate_store_postgres::PostgresStore;
/// fn reset(store: &PostgresStore) {
///     let _ = store.truncate_all();
/// }
/// ```
///
/// Query-plan inspection, which refreshes database statistics, is likewise
/// absent from the operational handle:
///
/// ```compile_fail,E0599
/// # use tollgate_store_postgres::PostgresStore;
/// # use tollgate_core::AccountId;
/// fn explain(store: &PostgresStore) {
///     let _ = store.explain_active_lease_sum(AccountId(1));
/// }
/// ```
///
/// The same holds for the two sweep plans (GL-65):
///
/// ```compile_fail,E0599
/// # use tollgate_store_postgres::PostgresStore;
/// fn explain_sweep(store: &PostgresStore) {
///     let _ = store.explain_reclaim_due_leases(jiff::Timestamp::UNIX_EPOCH, 256);
/// }
/// ```
///
/// ```compile_fail,E0599
/// # use tollgate_store_postgres::PostgresStore;
/// fn explain_rollover(store: &PostgresStore) {
///     let _ = store.explain_due_periods("daily", 0, 256);
/// }
/// ```
pub struct PostgresStore {
    pool: PgPool,
    policy: GrantPolicy,
    push: broadcast::Sender<SnapshotPush>,
}

/// Connection-pool bounds. Callers that hold background tasks open against
/// this store rely on `acquire_timeout`: when every connection is checked out
/// by a stalled query, it is the only thing that turns "wait forever" into an
/// error the caller can report (INVARIANTS.md GL-18).
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// The most connections the pool opens at once. Must be positive;
    /// defaults to 16.
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
    /// Rejects a zero `max_connections` or a zero `acquire_timeout`.
    ///
    /// [`PostgresStore::connect_with`] calls this before it opens any
    /// connection, so an unsafe bound never reaches the database
    /// (INVARIANTS.md 16).
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
    /// tracked by sqlx's _sqlx_migrations table — review finding GL-11).
    ///
    /// Note the limits of what a pool bound can promise: `acquire_timeout`
    /// covers waiting for a connection, including establishing one, but a
    /// query already in flight on a healthy connection is bounded only by a
    /// server-side `statement_timeout`. Callers must still bound their own
    /// calls (INVARIANTS.md GL-18).
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

    /// Push a tombstone only when a committed removal changed something.
    fn announce_removal(&self, principal: Principal, receipt: &AdminReceipt<()>) {
        if receipt.before != receipt.after
            && let AdminState::Snapshot { generation, .. } = receipt.after
        {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Revoked { generation },
            });
        }
    }

    // ---- reconciliation / test surface (mirrors MemoryStore) ---------

    /// The account's unspent balance, read in one statement outside any
    /// transaction. An unknown account reads as zero, as in `MemoryStore`.
    ///
    /// A negative stored value is reported as a [`StoreError`], never
    /// clamped (INVARIANTS.md 11). For figures that must agree with each
    /// other, use [`conservation`](Self::conservation), which reads them
    /// from one snapshot.
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

    /// Total usage accepted into the account's billing ledger, including
    /// overage usage, read in one statement outside any transaction. An
    /// unknown account reads as zero, as in `MemoryStore`.
    ///
    /// A negative stored value is reported as a [`StoreError`], never
    /// clamped (INVARIANTS.md 11).
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

    /// The terms of the account's ledger equation, for reconciliation
    /// against the ledger contract in `INVARIANTS.md`. `None` when the account
    /// does not exist.
    ///
    /// The account totals and the sums over its active leases are read in one
    /// `REPEATABLE READ, READ ONLY` transaction, so both come from the same
    /// snapshot and a concurrent commit cannot land between them. The
    /// transaction writes nothing.
    ///
    /// Stored state that cannot be a valid ledger is reported as a
    /// [`StoreError`], never clamped or panicked on: a negative unit column
    /// (INVARIANTS.md 11), or active-lease usage exceeding recorded usage.
    /// Whether the returned terms balance is for the caller to check with
    /// [`Conservation::holds`](tollgate_store::Conservation::holds).
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
        // subtraction outright (GL-56).
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
            // class as a negative unit column (INVARIANTS.md GL-11) — not an
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
    expires_at: StoredInstant,
    state: i16,
    /// The half of `granted` drawn from the account's periodic allowance, and
    /// the period that funded it. Settlement needs both: the split says which
    /// bucket each unspent unit belongs to, the period says whether the
    /// allowance half still exists (GL-97).
    from_allowance: i64,
    period_start_us: i64,
}

/// Settlement evidence from the account update inside the current transaction.
/// Consolidation uses the credit that survived the period boundary as its floor.
struct ReleasedCredit {
    account: AccountId,
    restored: CostUnits,
    preserves_funding: bool,
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
        expires_at: StoredInstant {
            micros: row.get(5),
            submicro_nanos: row.get(9),
        },
        state: row.get(6),
        from_allowance: row.get(7),
        period_start_us: row.get(8),
    }))
}

/// What a consolidation's release half hands the grant half of the same
/// transaction. A plain acquire returns nothing: [`Exchange::ACQUIRE`].
struct Exchange {
    /// The credit this transaction restored to the account, which the grant
    /// policy's shrink cap may not size the result below (see
    /// [`LeaseAllocator::consolidate`]).
    floor: CostUnits,
    /// The largest quote the returned lease refused, which the grant may grow
    /// to when the restored balance funds it (GL-131).
    needed: CostUnits,
    /// Whether the settlement left total funding unchanged, so a refusal may
    /// attest the ledger it reads (GL-130).
    preserves_funding: bool,
}

impl Exchange {
    const ACQUIRE: Exchange = Exchange {
        floor: CostUnits::ZERO,
        needed: CostUnits::ZERO,
        preserves_funding: true,
    };
}

impl PostgresStore {
    /// One grant, inside a caller-owned transaction.
    async fn acquire_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        account: AccountId,
        requested: CostUnits,
        expires_at: Timestamp,
        exchange: Exchange,
    ) -> Result<Allocation, AllocateError> {
        let Exchange {
            floor,
            needed,
            preserves_funding: settlement_preserves_funding,
        } = exchange;
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
        // `AllocateError`'s per-reason tally for nothing (GL-51).
        if decode_status(row.get::<String, _>(1)).map_err(AllocateError::Storage)?
            != AccountStatus::Active
        {
            return Err(AllocateError::AccountInactive);
        }
        let balance =
            to_units(row.get::<i64, _>(0), "account balance").map_err(AllocateError::Storage)?;
        // The floor and demand are applied to the policy's answer, not to
        // the balance test: the units the caller returned rejoined `balance`
        // earlier in this same transaction, and both are capped by it, so
        // they can only re-select capacity the account demonstrably has, and
        // an account with nothing left still refuses.
        let granted = match self
            .policy
            .consolidation_grant(requested, balance, floor, needed)
        {
            Some(granted) => granted,
            None => {
                // A refused consolidation rolls its settlement back. Only
                // attest when that settlement did not remove funding itself.
                if settlement_preserves_funding && !requested.is_zero() {
                    let budget = sqlx::query(BUDGET_VIEW_SQL)
                        .bind(id_bytes(account.0))
                        .fetch_one(&mut **tx)
                        .await
                        .map_err(alloc_storage)?;
                    let evidence = budget_view(&budget)
                        .map_err(AllocateError::Storage)?
                        .shortfall();
                    return Err(match evidence.exhaustion() {
                        Some(exhausted) => AllocateError::BalanceExhausted(exhausted),
                        None => AllocateError::BalanceInsufficient(evidence),
                    });
                }
                return Err(AllocateError::InsufficientBalance);
            }
        };
        // Allowance first: the units with an expiry date are spent
        // before the manual credits sitting beside them, and the lease
        // remembers the split so settlement can return each half to where
        // it came from (GL-97).
        let granted_i = to_i64(granted, "grant").map_err(AllocateError::Storage)?;
        let allowance_balance = row.get::<i64, _>(3);
        let from_allowance = granted_i.min(allowance_balance);
        let period_start_us = row.get::<i64, _>(4);
        let fence = row.get::<i64, _>(2);
        let fence_token = stored_fence(fence).map_err(AllocateError::Storage)?;

        let lease_id = LeaseId(uuid::Uuid::new_v4().as_u128());

        // The grant commits with this transaction, and a consolidation's
        // settlement has already applied inside it, so the row this returns is
        // the ledger the grant lands in: loss and expiry included.
        let budget = sqlx::query(GRANT_DEBIT_SQL)
            .bind(id_bytes(account.0))
            .bind(granted_i)
            .bind(from_allowance)
            .fetch_one(&mut **tx)
            .await
            .map_err(alloc_storage)?;
        let funding = budget_view(&budget)
            .map_err(AllocateError::Storage)?
            .shortfall();
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
        .bind(StoredInstant::from(expires_at).micros)
        .bind(from_allowance)
        .bind(period_start_us)
        .bind(StoredInstant::from(expires_at).submicro_nanos)
        .execute(&mut **tx)
        .await
        .map_err(alloc_storage)?;

        Ok(Allocation {
            grant: LeaseGrant {
                lease_id,
                account_id: account,
                fencing_token: fence_token,
                units: granted,
                expires_at,
            },
            funding: Some(funding),
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
        // bucket that expires at the next boundary (GL-97).
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
            preserves_funding: loss == 0 && restored == unspent_i,
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
    ) -> Result<Allocation, AllocateError> {
        let expires_at = self.grant_expiry(ttl, now)?;
        let mut tx = self.pool.begin().await.map_err(alloc_storage)?;
        let result = self
            .acquire_in_tx(&mut tx, account, requested, expires_at, Exchange::ACQUIRE)
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
        needed: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
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
                Exchange {
                    floor: released.restored,
                    needed,
                    preserves_funding: released.preserves_funding,
                },
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
        let cutoff = StoredInstant::from(cutoff);
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // Reclaim only once the grace window past expiry has fully lapsed:
            // expires_at + grace <= now  iff  expires_at <= now - grace.
            // Both tuple components preserve the exact timestamp ordering.
            // SKIP LOCKED lets concurrent sweepers cooperate; the limit keeps
            // both the lease locks and the transaction's row work bounded.
            let rows = sqlx::query(RECLAIM_DUE_LEASES_SQL)
                .bind(cutoff.micros)
                .bind(cutoff.submicro_nanos)
                .bind(limit_i)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage)?;

            // A holder that never released cannot prove any unit unspent, so
            // nothing is credited: each remainder becomes provisional
            // settlement loss, which later usage for the lease converts into
            // billed usage (GL-136). `credited` stays zero so that usage fits.
            let mut reclaimed = Vec::with_capacity(rows.len());
            let mut lease_ids = Vec::with_capacity(rows.len());
            let mut forfeits: std::collections::BTreeMap<Vec<u8>, i64> =
                std::collections::BTreeMap::new();
            for row in rows {
                let lease_bytes: Vec<u8> = row.get(0);
                let account_bytes: Vec<u8> = row.get(1);
                let granted = row.get::<i64, _>(2);
                let used = row.get::<i64, _>(3);
                // Validate the whole batch before either set-wise UPDATE: a
                // negative remainder is corruption, and recording it would
                // shrink the account's loss rather than account for a lease.
                let forfeited = granted.checked_sub(used).ok_or_else(|| {
                    StoreError(format!(
                        "reclaim remainder overflow: granted {granted}, used {used}"
                    ))
                })?;
                let forfeited_units = to_units(forfeited, "reclaim remainder")?;
                let total = forfeits.entry(account_bytes.clone()).or_default();
                *total = total
                    .checked_add(forfeited)
                    .ok_or_else(|| StoreError("reclaim loss sum overflow".into()))?;
                lease_ids.push(lease_bytes.clone());
                reclaimed.push(ReclaimedLease {
                    lease_id: LeaseId(id_from(&lease_bytes)),
                    account_id: AccountId(id_from(&account_bytes)),
                    forfeited: forfeited_units,
                });
            }

            let batch = ReclaimBatch::try_new(reclaimed, limit)?;
            if batch.is_empty() {
                return Ok(batch);
            }

            let (account_ids, account_forfeits): (Vec<_>, Vec<_>) = forfeits.into_iter().unzip();
            let expected_lease_rows = u64::try_from(lease_ids.len())
                .map_err(|_| StoreError("reclaim lease row count exceeds u64 range".into()))?;
            let expected_account_rows = u64::try_from(account_ids.len())
                .map_err(|_| StoreError("reclaim account row count exceeds u64 range".into()))?;

            // A set-wise UPDATE does not promise row-lock order. Lock every
            // affected account explicitly in byte-sorted order first (the
            // BTreeMap above), matching release and ingest's lease-then-account
            // order and preventing concurrent multi-account sweeps from
            // forming a deadlock cycle.
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
                "UPDATE tollgate_leases
                 SET state = $2, credited = 0
                 WHERE lease_id = ANY($1) AND state = $3",
            )
            .bind(&lease_ids)
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
                 SET settlement_loss = account.settlement_loss + delta.forfeited
                 FROM UNNEST($1::bytea[], $2::bigint[]) AS delta(account_id, forfeited)
                 WHERE account.account_id = delta.account_id",
            )
            .bind(&account_ids)
            .bind(&account_forfeits)
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
        // One transaction per *batch* (review finding GL-8): leases are locked
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
            // Lock every referenced lease in one global account/lease order.
            // A cycle needs two waiters, and this is the path that waits:
            // release touches a single lease, and reclaim takes its leases
            // with SKIP LOCKED, so it abandons a contended row instead of
            // queueing behind it. Concurrent ingests are therefore what this
            // order is for -- reclaim selects in expiry order (GL-65) and is
            // still safe, because the lease-then-account phase order below is
            // what keeps the two from crossing.
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
/// "active" would turn corruption into service (the rule INVARIANTS.md GL-11
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
/// vocabulary does not contain (GL-99).
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

/// The authority that created an account or set its status (#39), or a
/// refusal for a spelling the vocabulary does not contain.
///
/// Never defaults to `Provisioner`, and never to `Operator` either: an unknown
/// value is a row written outside this code, and reading it as either would
/// decide who may administer the account by accident.
fn decode_authority(stored: String) -> Result<AdminAuthority, StoreError> {
    match stored.as_str() {
        s if s == AdminAuthority::Operator.as_str() => Ok(AdminAuthority::Operator),
        s if s == AdminAuthority::Provisioner.as_str() => Ok(AdminAuthority::Provisioner),
        other => Err(StoreError(format!(
            "unrecognized admin authority {other:?}"
        ))),
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

/// The ledger row [`budget_view`] decodes, for allocator evidence read inside
/// the grant's own transaction.
const BUDGET_VIEW_SQL: &str = "SELECT account_id, deposited, overage_recorded, usage_recorded,
            settlement_loss, expired, budget_allowance, budget_period,
            budget_rollover, period_start_us
     FROM tollgate_accounts WHERE account_id = $1";

/// Debit a grant and return the committed-to-be row [`budget_view`] decodes,
/// in the same column order as [`BUDGET_VIEW_SQL`].
const GRANT_DEBIT_SQL: &str = "UPDATE tollgate_accounts
     SET balance = balance - $2,
         allowance_balance = allowance_balance - $3,
         next_fence = next_fence + 1
     WHERE account_id = $1
     RETURNING account_id, deposited, overage_recorded, usage_recorded,
               settlement_loss, expired, budget_allowance, budget_period,
               budget_rollover, period_start_us";

/// What the account could still spend, for a snapshot's budget view (GL-97).
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
/// (INVARIANTS.md GL-11) — see the note in `PostgresStore::conservation`.
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
/// Since GL-54 that is literally true of the generation as well: both callers
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
/// A negative value is corruption to surface, never to clamp (INVARIANTS GL-11).
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
            // (GL-54).
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
/// Shared by the two account-owned facts that republish — status (GL-51) and
/// execution-capacity class (GL-99). They differ in their precondition and their
/// ledger column; everything below is identical, and it is the part where the
/// subtlety lives, so it is written once.
///
/// `jsonb_set` rather than read-modify-write in Rust, for three reasons any
/// one of which decides it:
///
/// 1. RMW reintroduces GL-51's own bug. A concurrent `publish_snapshot` landing
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
/// (GL-54), and `RETURNING generation` carries the new value out to the push.
///
/// `deleted = FALSE` leaves tombstones alone: republishing one would resurrect
/// a revoked principal (INVARIANTS.md GL-15), and revocation stays its own
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
            // GL-19). Counted as well as logged: the row changed durably but
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
        self.create_account_as(config, AdminAuthority::Operator)
            .await
    }

    async fn create_provisioned_account(
        &self,
        account: AccountId,
    ) -> Result<tollgate_store::AdminReceipt<()>, CreateAccountError> {
        self.create_account_as(
            AccountConfig {
                account_id: account,
                initial_balance: CostUnits::ZERO,
                status: AccountStatus::Suspended,
                capacity_class: CapacityClass::BestEffort,
            },
            AdminAuthority::Provisioner,
        )
        .await
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
        .bind(i64::try_from(units.get()).map_err(|_| AllocateError::BalanceOverflow)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            // Only this deposit's arithmetic refusal is permanent. Connection,
            // constraint and other database failures retain Storage semantics.
            // The single UPDATE rolls back both counters on numeric overflow.
            if error
                .as_database_error()
                .and_then(|db| db.code())
                .as_deref()
                == Some("22003")
            {
                AllocateError::BalanceOverflow
            } else {
                alloc_storage(error)
            }
        })?
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
    ) -> Result<AdminReceipt<()>, BudgetError> {
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
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            // A self-join's `previous` alias retains its statement snapshot
            // after waiting for a writer. Lock first so the receipt observes
            // the committed predecessor, then update under that same lock.
            let row = sqlx::query(
                "SELECT budget_allowance, budget_period, budget_rollover
                 FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
            )
            .bind(id_bytes(account.0))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(BudgetError::UnknownAccount)?;
            let before = decode_schedule(row.get(0), row.get(1), row.get(2))?;
            sqlx::query(
                "UPDATE tollgate_accounts
                 SET budget_allowance = $2, budget_period = $3, budget_rollover = $4
                 WHERE account_id = $1",
            )
            .bind(id_bytes(account.0))
            .bind(allowance)
            .bind(schedule.map(|s| s.period.as_str()))
            .bind(schedule.map(|s| s.rollover.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            Ok(AdminReceipt::new(
                (),
                AdminState::Budget { schedule: before },
                AdminState::Budget { schedule },
            ))
        }
        .await;
        finish_transaction(tx, result).await
    }

    async fn account_view(&self, account: AccountId) -> Result<Option<AccountView>, StoreError> {
        // One `REPEATABLE READ, READ ONLY` snapshot over the account row and
        // its live leases, for the reason `conservation` takes one (GL-56): the
        // stored totals and the sums over active leases move together in a
        // single `ingest` transaction, so reading them under separate
        // snapshots can pair a pre-write total with a post-write sum and
        // report corruption on a correct ledger. Status and schedule join that
        // same snapshot here, so a view cannot straddle a suspension or a
        // rollover and describe a state the account was never in.
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let result = async {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            let account_row = sqlx::query(
                "SELECT deposited, balance, usage_recorded, settlement_loss, overage_recorded,
                        expired, status, capacity_class, budget_allowance, budget_period,
                        budget_rollover, period_start_us, origin, status_set_by
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
        Ok(Some(AccountView {
            account_id: account,
            status: decode_status(row.get::<String, _>(6))?,
            capacity_class: decode_capacity_class(row.get::<String, _>(7))?,
            origin: decode_authority(row.get::<String, _>(12))?,
            status_set_by: decode_authority(row.get::<String, _>(13))?,
            schedule: decode_schedule(
                row.get::<Option<i64>, _>(8),
                row.get::<Option<String>, _>(9),
                row.get::<Option<String>, _>(10),
            )?,
            period_start: StoredInstant {
                micros: row.get::<i64, _>(11),
                submicro_nanos: 0,
            }
            .timestamp()?,
            conservation: Conservation {
                deposited: to_units(row.get::<i64, _>(0), "deposited")?,
                overage_recorded: to_units(row.get::<i64, _>(4), "overage_recorded")?,
                balance: to_units(row.get::<i64, _>(1), "balance")?,
                active_lease_grants: active_grants,
                // Surfaced rather than panicked on, as `conservation` does:
                // two stored columns from a database this process does not
                // exclusively own, so this is corruption to report.
                settled_usage: recorded.checked_sub(active_used).ok_or_else(|| {
                    StoreError(format!(
                        "active lease usage {} exceeds recorded usage {} for account {account}",
                        active_used.get(),
                        recorded.get()
                    ))
                })?,
                settlement_loss: to_units(row.get::<i64, _>(3), "settlement_loss")?,
                expired: to_units(row.get::<i64, _>(5), "expired")?,
            },
        }))
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
            let rows = sqlx::query(DUE_PERIODS_SQL)
                .bind(period.as_str())
                .bind(boundary_us)
                .bind(remaining)
                .fetch_all(&self.pool)
                .await
                .map_err(storage)?;

            for row in rows {
                rolled.push((
                    row.get::<i64, _>(3),
                    RolledAccount {
                        account_id: AccountId(id_from(&row.get::<Vec<u8>, _>(0))),
                        deposited: to_units(row.get::<i64, _>(1), "budget allowance")?,
                        expired: to_units(row.get::<i64, _>(2), "expiring allowance")?,
                    },
                ));
            }
        }
        // Oldest boundary first, account id breaking ties, which is the order
        // `MemoryStore` reports and therefore the one this backend owes. The
        // rows arrive unordered from `RETURNING` and, across more than one
        // period kind, in per-period groups; sorting the assembled page is what
        // makes the report a function of the stored state rather than of the
        // planner. Bounded by `limit`, not by how many accounts are due.
        rolled
            .sort_unstable_by_key(|(crossed_from, account)| (*crossed_from, account.account_id.0));
        RolloverBatch::try_new(
            rolled.into_iter().map(|(_, account)| account).collect(),
            limit,
        )
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<tollgate_store::AdminReceipt<StatusChange>, SetStatusError> {
        self.set_status_as(account, status, AdminAuthority::Operator)
            .await
    }

    async fn activate_provisioned(
        &self,
        account: AccountId,
    ) -> Result<tollgate_store::AdminReceipt<StatusChange>, SetStatusError> {
        self.set_status_as(account, AccountStatus::Active, AdminAuthority::Provisioner)
            .await
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
        let result = publish_in_tx(&mut tx, principal, Some(generation), snapshot).await;

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
        let result = remove_in_tx(&mut tx, principal).await;
        let receipt = finish_transaction(tx, result).await?;
        self.announce_removal(principal, &receipt);
        Ok(receipt)
    }
}

/// Resolve `account`'s credential `key` to its principal and retirement,
/// holding the credential row `FOR SHARE` until the transaction ends (GL-143).
///
/// `FOR SHARE` conflicts with `revoke_key_audited`'s `FOR UPDATE`, so a
/// key-bound publication and a revocation serialize: the publish either sees
/// the retirement and is refused, or commits first.
///
/// **Lock order: credential, then account, then snapshot.** A cycle needs a
/// path that holds an account lock and then waits on a credential row lock.
/// None does:
/// - `revoke_key_audited` locks only the credential row, never an account.
/// - `insert_credential` takes the account `FOR UPDATE`, then inserts a *new*
///   credential row; it never locks an existing one.
/// - `ingest` locks accounts `FOR UPDATE`, then reads credentials with a plain
///   `SELECT` and writes activity rows whose foreign key takes only
///   `FOR KEY SHARE`, which `FOR SHARE` does not block.
/// - Every other account-locking path — `set_account_status`,
///   `set_capacity_class`, `set_budget_schedule`, `deposit`, rollover,
///   reclaim and lease acquisition — touches account, lease, usage and
///   snapshot rows only; `tollgate_credential_keys` appears in none of them.
///
/// After the credential row, this path takes the account and snapshot rows in
/// the same order `publish_snapshot` and `set_account_status` do.
async fn lock_account_key(
    tx: &mut Transaction<'_, Postgres>,
    account: AccountId,
    key: KeyId,
) -> Result<(Principal, bool), KeySnapshotError> {
    let row = sqlx::query(
        "SELECT principal, revoked_at_us IS NOT NULL
         FROM tollgate_credential_keys WHERE key_id = $1 AND account_id = $2 FOR SHARE",
    )
    .bind(id_bytes(key.0))
    .bind(id_bytes(account.0))
    .fetch_optional(&mut **tx)
    .await
    .map_err(storage)?
    .ok_or(KeySnapshotError::UnknownCredential)?;
    let principal: Vec<u8> = row.get(0);
    let principal: [u8; 16] = principal
        .try_into()
        .map_err(|_| StoreError("credential principal is not 16 bytes".into()))?;
    Ok((Principal(u128::from_be_bytes(principal)), row.get(1)))
}

/// A publication's checks and write inside the caller's transaction: the
/// stated credential binding (GL-35), the ledger status and capacity-class
/// guards (GL-51, GL-99), the budget stamp (GL-97), and the generation-ordered
/// write. Returns whether a row was written, the stamped snapshot to push
/// after commit, and the audited predecessor and successor. `None` requests
/// generation allocation under the snapshot write lock; `Some` retains the
/// operator's generation-ordered publication contract.
async fn publish_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    principal: Principal,
    generation: Option<i64>,
    snapshot: PublishableSnapshot,
) -> Result<(bool, PublishableSnapshot, AdminState, AdminState), PublishSnapshotError> {
    if let Some(key_id) = snapshot.key_id {
        let matches: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tollgate_credential_keys
                 WHERE key_id = $1 AND principal = $2 AND account_id = $3)",
        )
        .bind(id_bytes(key_id.0))
        .bind(id_bytes(principal.0))
        .bind(id_bytes(snapshot.account_id.0))
        .fetch_one(&mut **tx)
        .await
        .map_err(storage)?;
        if !matches {
            return Err(PublishSnapshotError::CredentialMismatch { key_id });
        }
    }
    // The ledger decides an account's status; a publish may carry it
    // but not change it, or the two records `set_account_status`
    // unified could be pulled apart again one principal at a time
    // (GL-51). FOR SHARE, not FOR UPDATE: this only has to hold the
    // status still, and a status change takes FOR UPDATE on the same
    // row, so the two serialize without publishes blocking each other.
    //
    // The budget columns ride along on the read that was already being
    // taken, under the same lock, so the view stamped below is the
    // ledger as of this publication rather than a second read that
    // could straddle a lease or a rollover (GL-97).
    let ledger = sqlx::query(
        "SELECT status, deposited, overage_recorded, usage_recorded, settlement_loss,
                    expired, budget_allowance, budget_period, budget_rollover, period_start_us,
                    capacity_class
             FROM tollgate_accounts WHERE account_id = $1 FOR SHARE",
    )
    .bind(id_bytes(snapshot.account_id.0))
    .fetch_optional(&mut **tx)
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
            // same guard (GL-99): the ledger owns it, a publish may
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

    let (written, before, after) = write_snapshot_audited(tx, principal, generation, value).await?;
    let published = if generation.is_none() {
        let AdminState::Snapshot { generation, .. } = after else {
            return Err(StoreError("published snapshot has no generation".into()).into());
        };
        published.restamped(published.status, generation)
    } else {
        published
    };
    Ok((written, published, before, after))
}

/// Tombstone a live snapshot inside the caller's transaction, keeping its
/// generation as the watermark.
async fn remove_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    principal: Principal,
) -> Result<AdminReceipt<()>, StoreError> {
    let before = snapshot_audit_row(tx, principal).await?;
    let after = match before {
        AdminState::Snapshot {
            generation,
            revoked: false,
        } => {
            sqlx::query("UPDATE tollgate_snapshots SET deleted = TRUE WHERE principal = $1")
                .bind(id_bytes(principal.0))
                .execute(&mut **tx)
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
    generation: Option<i64>,
    value: serde_json::Value,
) -> Result<(bool, AdminState, AdminState), StoreError> {
    let mut before = snapshot_audit_row(tx, principal).await?;
    if before == AdminState::Absent {
        let initial = generation.unwrap_or(1);
        let after = AdminState::Snapshot {
            generation: generation_from(initial)?,
            revoked: false,
        };
        let inserted = sqlx::query(
            "INSERT INTO tollgate_snapshots (principal, generation, snapshot, deleted)
            VALUES ($1, $2, $3, FALSE) ON CONFLICT (principal) DO NOTHING",
        )
        .bind(id_bytes(principal.0))
        .bind(initial)
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
    // Recompute after an insert conflict as well: the locked predecessor,
    // including a tombstone, is the only authority for the next generation.
    let generation = match generation {
        Some(stated) => stated,
        None => i64::try_from(previous.0)
            .ok()
            .and_then(|previous| previous.checked_add(1))
            .ok_or_else(|| StoreError("snapshot generation overflow".into()))?,
    };
    let after = AdminState::Snapshot {
        generation: generation_from(generation)?,
        revoked: false,
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
    fn stored_authorities_reject_unknown_vocabulary() {
        assert_eq!(
            decode_authority("Operator".into()).unwrap(),
            AdminAuthority::Operator
        );
        assert_eq!(
            decode_authority("Provisioner".into()).unwrap(),
            AdminAuthority::Provisioner
        );
        for invalid in [
            "",
            "operator",
            "provisioner",
            "Administrator",
            "Provisioner ",
        ] {
            assert!(decode_authority(invalid.into()).is_err(), "{invalid:?}");
        }
    }

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
    /// a store object exists (INVARIANTS.md GL-19).
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
        assert!(matches!(
            AdminStore::deposit(&store, AccountId(1), CostUnits(1)).await,
            Err(AllocateError::Storage(_))
        ));
    }
}
