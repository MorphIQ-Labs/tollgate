# Usage ingestion and database guards

`UsageSink::ingest` classifies each input as accepted, duplicate or rejected.
Only accepted events affect billing, lease usage and credential activity.
Duplicate detection precedes payload validation, including when a replay changes
its units or funding source. A rejected input does not claim its request ID;
a corrected event with that ID can still be accepted.

MemoryStore's unit domain is `0..=u64::MAX`. PostgreSQL uses nonnegative
`BIGINT`, `0..=i64::MAX`. Both admit zero-unit events. PostgreSQL rejects an
individual event outside its domain while retaining valid neighbors. Memory's
larger range remains supported; moving workloads between backends requires
values that fit the destination's existing domain.

A batch of individually representable events can still overflow an aggregate
or a monotonic accounting counter. Both backends then return
`IngestError::Refused` and apply none of the batch, including request IDs and
credential activity. This is terminal for the unchanged batch: the usage writer
accounts for undelivered events through its existing loss reporting instead of
retrying forever. Operational errors and stored corruption remain retryable;
corruption needs explicit repair before retries can succeed.

The request path, wire DTOs and accepted-event ledger equations are unchanged.
PostgreSQL's oversized-event outcome changes from a failed batch to an individual
rejection. Its accounting-overflow outcome becomes a permanent refusal.
MemoryStore's leased-usage overflow becomes a refusal instead of a panic.

## Migration and rollout

0015 installs checks for nonnegative `tollgate_usage_events.units` and positive
fences in usage events, leases and the account's `next_fence` counter. Overage
retains a null lease and fence; the existing all-or-nothing capability check
still applies. All legitimate prior writers already use positive fences.

0015 uses `NOT VALID` to install guards without scanning usage history under an
exclusive lock. It still needs brief exclusive locks for the catalogue changes;
plan deployment around long-running transactions that could delay them. 0016
validates the guards in a separate transaction with `SHARE UPDATE EXCLUSIVE`,
which permits ordinary row reads and writes during the scan. Validation time and
I/O scale with existing table contents. These lock modes follow
[PostgreSQL's constraint validation rules](https://www.postgresql.org/docs/16/sql-altertable.html).

Before deployment, inspect existing data without changing it:

```sql
SELECT request_id, units, fencing_token FROM tollgate_usage_events
WHERE units < 0 OR fencing_token <= 0;
SELECT lease_id, fencing_token FROM tollgate_leases WHERE fencing_token <= 0;
SELECT account_id, next_fence FROM tollgate_accounts WHERE next_fence <= 0;
```

Prepare migration-aware deployment and rollback binaries with both unchanged
migration files. `PostgresStore::connect` runs SQLx migration validation before
serving. A binary whose embedded catalogue ends at 0014 refuses a cold start
after 0015 is applied. Already-running old binaries retain compatible SQL write
shapes, but keep the old batch-failure behavior until upgraded. Prevent old
binaries from restarting or scaling out during the transition.

If 0016 finds invalid history, startup fails and 0015 remains committed. Its
guards continue checking new writes; the invalid historical rows are preserved.
Reconcile affected rows from authoritative usage and lease evidence, preserving
the audit trail, then rerun startup to validate. Do not clamp negative units,
invent replacement fences, drop guards or delete migration history to make
startup pass. In particular, repairing a counter requires establishing the
account's allocation sequence, not merely replacing zero with one.

Rollback retains the schema and migration history and uses a rollback build
that knows both migrations. An unmodified older binary is not a cold-rollback
artifact. Any later schema change requires a new forward migration.

## Evidence

The mirrored store suites test mixed-batch classification, duplicate precedence,
zero and maximum representable units, and atomic refusal in both event orders.
PostgreSQL cases additionally exercise out-of-range inputs, negative and zero
stored fences, and schema write rejection. Isolated-schema migration tests
upgrade populated 0014 tables, preserve leased/overage rows, reject an old startup
catalogue, and exercise validation failure followed by evidence-based fixture
repair without removing migration history.

The natural-number conservation proofs in `formal/lean/Tollgate/Conservation.lean`
continue to describe accepted transitions. These changes do not add a ledger
term or change those equations. The finite-width limits, rejection partition and
transaction rollback are implementation obligations covered by these tests and
the mutation gate; the exact model does not prove SQL locking or overflow behavior.
