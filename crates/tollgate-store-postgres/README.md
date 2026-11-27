# tollgate-store-postgres

The PostgreSQL backend of [Tollgate](https://github.com/MorphIQ-Labs/tollgate):
transactional fenced lease allocation, idempotent usage ingest, snapshot and
credential storage, and bounded expiry reclaim, implementing the
[`tollgate-store`](https://crates.io/crates/tollgate-store) traits.

It reproduces `MemoryStore`'s settlement rules exactly. A mirrored test suite,
matched to the in-memory backend's scenario by scenario, is the proof, and
repository checks keep the two suites driving one contract.

- **Concurrency** is row-level: `SELECT … FOR UPDATE` on the account row for
  acquisition, and on the lease row to serialise release, ingest, and reclaim.
- **Fencing tokens** come from a per-account counter, so allocation is
  strictly monotonic per account across any number of servers sharing the
  database.
- **Arithmetic** is checked in both directions: units are `BIGINT` with
  checked conversion, a balance beyond `i64::MAX` is refused rather than
  wrapped, and schema constraints keep every unit column non-negative.
- **Usage ingest** is set-wise; **expiry reclaim** runs in bounded,
  set-wise `SKIP LOCKED` batches, oldest due first, in index order.
- **Budget rollover** crosses the free accounts of a bounded page set-wise,
  then queues for each held one within a few seconds per call, so an account in
  constant use is still crossed and one held for good is reported, not waited
  on.
- **Migrations** are embedded and run on connect.

The conservation equation holds on this backend as on every other, and a
reconciliation query checks it on a live database.

## Connecting

`PostgresStore::connect` takes a PostgreSQL URL, which
[sqlx](https://docs.rs/sqlx/0.9/sqlx/postgres/struct.PgConnectOptions.html)
reads together with the `PG*` environment variables and the password file.

- The crate enables no TLS backend for sqlx. Unless another dependency of the
  same build enables one, connections are plaintext: `sslmode=prefer`, the
  default, falls back to plaintext, and `require`, `verify-ca` and
  `verify-full` are refused. Keep the database on a trusted network or put a
  TLS-terminating proxy beside the application.
- `hostaddr` and `PGHOSTADDR` give the address to connect to without replacing
  the host name. `PGHOSTADDR` applies even when the URL names a host.
- A password or user name that SASLprep prohibits, such as one containing a
  control character, is refused under SCRAM authentication.
- Values given as `options[name]=value` are escaped for the server, so write
  them unescaped. A raw `options=` value is passed through.

## Features

- `test-support`: destructive fixture reset and query-plan inspection, for
  tests. Not for production builds, and not covered by semantic versioning: it
  may change in any release.

## Documentation

[`docs/USAGE_ACCOUNTING.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/USAGE_ACCOUNTING.md)
covers batch rejection semantics, PostgreSQL numeric limits, and
database-guard migration and recovery;
[`docs/LEASE_OWNERSHIP.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/LEASE_OWNERSHIP.md)
covers grant retirement. The contract is
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md).

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
