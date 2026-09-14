# Lease ownership and PostgreSQL test support

`LeaseSlot::replace` publishes a grant and returns the previous handle, if any.
`LeaseSlot::take` empties the slot and returns its handle. Both results are
`must_use`: removing a local handle does not return its unspent quota to the
allocator. Retain it until all reservations and local views have quiesced, then
release its exact remainder. `LeaseManager` handles this for supported runtimes,
including a grant displaced when a consolidation call finishes.

The single-view slot swaps atomically; sharded publications serialize mutators
while requests continue to load without a blocking lock. Each returned handle
names the displaced grant's shared state. Never release it just because it was
removed from the slot: an in-flight request may still debit or refund it.

## Migrating lease callers

`LeaseSlot::install` and `LeaseSlot::clear` have been removed. This is a Rust API
break; the service wire protocol and store schema are unchanged.

- Replace `install(fresh)` with `replace(fresh)` and retain any displaced grant
  for quiesced release.
- Replace `clear()` with `take()` and handle the returned grant. If its state is
  already settled, or deliberately abandoned, dispose of the handle explicitly.
- `drop(slot.replace(fresh))` and `drop(slot.take())` deliberately abandon the
  old handle. An unsettled grant then waits for TTL reclamation. In-repository
  tests and benchmarks use this spelling for synthetic grants with no allocator
  to settle; production refill paths retain the grant.

`replace` and `take` otherwise retain their signatures and publication behavior.
No request-path algorithm, benchmark workload or performance threshold changes.

## PostgreSQL fixture operations

`PostgresStore` does not expose `truncate_all` or `explain_active_lease_sum` methods.
Tests that need them enable the backend's non-default `test-support` feature and
call the functions in `tollgate_store_postgres::test_support`:

```rust,ignore
use tollgate_store_postgres::test_support;

test_support::truncate_all(&store).await?;
let plan = test_support::explain_active_lease_sum(&store, account).await?;
```

Use only an isolated disposable database. Reset deletes account, credential,
lease, snapshot and usage rows, including dependent rows through `CASCADE`;
schema and migration history remain. Plan inspection runs `ANALYZE` before
`EXPLAIN`, so it also mutates database statistics. Neither belongs in a serving
application's operational API.

Ordinary consumers do not enable `test-support`. In-repository backend and server
tests opt in through test-only self dependencies, preserving the existing test
commands without enabling the helpers in normal builds. The server forwards the
feature only when its optional PostgreSQL dependency is already enabled; its
`--no-default-features` build and tests remain independent of PostgreSQL.
External test harnesses enable the feature on their backend dev-dependency.
Cargo features are additive: a production build that explicitly requests all
features also opts into this module, so deployments should select their required
features rather than treating all features as a production profile.

The helper SQL, connection usage and database migration catalogue are unchanged.
Public read operations (`balance`, `usage_recorded`, `conservation`) remain on
the normal store handle.
