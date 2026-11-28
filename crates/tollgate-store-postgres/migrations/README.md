# Migrations

Forward-only, applied by `sqlx::migrate!("./migrations")` at
`PostgresStore::connect`, tracked in `_sqlx_migrations`.

## Starting several instances at once

Instances may start together. One takes sqlx's per-database advisory lock and
migrates; the others ask for it with `pg_try_advisory_lock` until it is free,
holding no snapshot meanwhile, then find nothing left to apply. Waiting inside
`pg_advisory_lock`, as sqlx's migrator does by itself, deadlocks against a
`CREATE INDEX CONCURRENTLY` run by the holder (INVARIANTS.md 42), so do not run
this catalogue through a migrator that takes the lock that way while another
instance may be migrating, and expect the `sqlx` command line to be subject to
the same deadlock when run beside a starting instance on a database that still
needs a `no-transaction` migration.

A startup waiting its turn logs `waiting for another session to finish
migrating this database` with the advisory key, which finds the holder:

```sql
SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
  AND ((classid::bigint << 32) | objid::bigint) = <advisory_key>;
```

A session `lock_timeout` or `statement_timeout` ends the wait with a startup
error naming the setting; with neither, the startup waits as long as the holder
keeps the lock.

## An applied migration is immutable, comments included

sqlx checksums each file. Change one byte of a migration some deployment has
already run and that deployment stops starting:

```
migrate: migration 5 was previously applied but has been modified
```

That is the intended behaviour, not an obstacle to work around: startup fails
closed on any divergence between this directory and the recorded history, which
is what `postgres_startup_never_ignores_an_unknown_applied_migration` and
`older_migration_catalogues_refuse_restart_without_erasing_history` exist to
enforce. Never edit an applied file, and never delete rows from
`_sqlx_migrations` to make one apply again.

The consequence worth stating: **a comment in an applied migration is frozen at
whatever was believed when it was written.** Some of them have since turned out
to be wrong. Correct them here rather than in place.

## Corrections to historical migration comments

### 0005 — "The sweep path was already fine"

`0005_leases_account_active_index.sql` says:

> The sweep path was already fine — `tollgate_leases_expiry` covers it.

It covered the `WHERE`. It did not cover the `ORDER BY`, and that is what
decided the plan. Until GL-65 the expiry sweep ordered by `(account_id,
lease_id)`, which no index answers, so PostgreSQL read and sorted every expired
row to return one bounded page — the `LIMIT` could not stop an index walk, and a
drain's read work was quadratic in the backlog it existed to clear. The index
named in that sentence was right all along; the query was not.

Fixed in GL-65 by ordering the sweep on `(expires_at_floor_us,
expires_at_submicro_ns)` — the index's own columns — with
`the_expiry_sweep_stops_at_its_batch_instead_of_sorting_the_backlog` pinning the
plan. See `docs/DESIGN.md`, "The sweep's ORDER BY defeated its own index (GL-65)".
