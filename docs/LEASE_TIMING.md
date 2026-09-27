# Lease timing and PostgreSQL upgrades

Both allocators preserve the nanosecond precision of the caller's `Timestamp`
and positive `SignedDuration`. The policy clamps a requested TTL only to
`max_ttl`. Acquire and consolidation return the exact resulting expiry; an
unrepresentable expiry returns a structured storage error before any balance
debit or settlement. Grace remains a nonnegative duration, including zero and
values too large for a timestamp deadline.

A lease with expiry `e` is reclaimable at supplied time `n` exactly when
`e + reclaim_grace <= n`. Release is accepted before that boundary, subject
to its normal capability, state and accounting checks. `GrantPolicy::reclaim_cutoff`
computes `n - reclaim_grace` with checked timestamp arithmetic. Underflow
means no representable expiry is due. A deadline beyond `Timestamp::MAX` has
not elapsed even at `Timestamp::MAX`; it is never saturated to an earlier time.
These rules apply equally to MemoryStore and PostgresStore. Grace configuration
must remain consistent across backends sharing the same grants.

PostgreSQL stores new lease instants in two columns:

| Column | Contract |
| --- | --- |
| `expires_at_floor_us` | Signed microseconds rounded down toward negative infinity |
| `expires_at_submicro_ns` | Remaining nanoseconds, 0 through 999 |
| `expiry_is_upper_bound` | False for exact grants; true for migrated legacy history |

Together the first two fields represent `1000 * floor_us + submicro_ns` exact
nanoseconds since the Unix epoch. Database constraints enforce the complete
Jiff timestamp domain. Tuple comparison has the same ordering as the original
instant, including before the epoch. The active expiry index covers both
components. Release decodes that same stored instant; reclaim compares it to
the same exact policy cutoff. No grace duration is converted to microseconds.

## Migration 0017

This is a coordinated backend upgrade with a database maintenance window.
Public allocator signatures, HTTP DTOs, error codes and valid configuration
retain their contracts. Old PostgreSQL lease statements are incompatible with
the migrated schema. The schema change does not enter the request path.

1. Preserve a recoverable database backup and the running policy configuration.
   Quiesce incoming work, drain usage and shut down old store/server processes,
   including reclaim workers and direct-store clients. Keep the same reclaim
   grace throughout the upgrade. Active grants do not need to be discarded.
2. Start a backend carrying migration 0017. The migration takes a table lock,
   renames the expiry column, adds its remainder and provenance, backfills
   legacy bounds, validates every row and rebuilds the partial expiry index in
   one transaction. Allow for table-size-dependent backfill/index work and
   lock acquisition. It changes no unit, fencing, state or accounting field.
3. Start only compatible backends. Check storage and maintenance readiness,
   normal accounting conservation and the count of active legacy bounds below;
   then resume traffic. Existing HTTP clients and retained lease capabilities
   continue to work with the upgraded service.

The column rename is a database-enforced compatibility fence. An old acquire,
release, consolidation or reclaim query fails with an undefined-column error,
including on a connection opened before migration. The normal transaction
rollback preserves any preceding account debit. An old process starting after
the migration refuses the unknown migration version. A mixed fleet therefore
loses availability at old lease endpoints rather than accepting an early
reclamation. Do not bypass either fence. Already-running old transactions hold
table locks that the migration must wait for; the new format is not visible
until the migration commits.

Legacy writers truncated fractional microseconds toward zero. The original
expiry cannot be reconstructed from those rows. Migration records the latest
possible expiry consistent with the stored value: add 999 ns for nonnegative
microseconds, and retain the stored instant for negative microseconds. This
can delay settlement by at most 999 ns, except the zero-microsecond bucket
spans -999 through +999 ns and can delay it by 1,998 ns. It never makes a
previously advertised expiry earlier. The true marker makes this uncertainty
inspectable; it remains on settled history. Ordinary release, consolidation or
reclaim drains active legacy grants, and every new grant is marked exact.

```sql
SELECT state, expiry_is_upper_bound, count(*)
FROM tollgate_leases
GROUP BY state, expiry_is_upper_bound;
```

There is no automatic schema downgrade. An older binary cannot run its lease
operations against migration 0017. Recovery after a successful upgrade is a
compatible corrected binary or a separately designed, reconciled restore;
dropping the remainder would lose live timing evidence. A failed migration
rolls back its schema and data changes and leaves version 0016 authoritative.
For out-of-domain historical timestamps, inspect the offending records and
repair only from authoritative evidence before retrying. Blind clamping would
invent an expiry. The migration does not reconstruct loss from grants already
settled by older software.

## Evidence and limits

`Tollgate.LeaseTiming` proves integer-pair encoding/order, exact cutoff
equivalence, underflow/overflow safety and the conservative legacy bounds.
It assumes integer nanoseconds and valid timing inputs; it does not prove
driver behavior, PostgreSQL isolation, clock accuracy or a Rust refinement.
Property tests compare finite timestamp arithmetic with an independent i128
oracle. Mirrored backend tests cover fractional TTL/grace, policy clamping,
negative timestamps, both domain endpoints, consolidation, release and
idempotent reclamation with exact ledger checks. Migration tests cover
retained old connections, old startup refusal, accounting preservation,
restart durability, legacy bounds, invalid history and schema constraints.
Mutation and formal gates run in CI.

No request-path code, measured benchmark path, timing threshold or baseline
changes. The control plane retains its existing transaction count and bounded
reclaim batches. Migration backfill and index creation scale with stored rows;
the account-order reclaim sorting concern was fixed separately, in GL-65.
