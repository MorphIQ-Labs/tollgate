# tollgate-server

The control-plane HTTP service of
[Tollgate](https://github.com/MorphIQ-Labs/tollgate): fenced lease allocation,
snapshot distribution, idempotent usage ingest, credential issuance, and
account administration, over any `tollgate-store` backend.

It is latency-tolerant by design. Its contract is correctness (no
double-spend, fencing, idempotent ingest), enforced by the backend it is built
over; anything implementing the `Backend` seam serves identically.

## Surface

Under `/v1`: lease acquire, release, consolidate and reclaim; revisioned
credential pages; the snapshot catalogue and snapshot fetch; usage ingest; and
account, key, and snapshot administration. `/livez` and `/readyz` are the
probes.

Every protected handler requires verified instance or operator evidence, with
disjoint roles. The server owns TLS and refuses exposed plaintext; mutual TLS,
rotating bearer credentials, and Google service identity are supported, and
rotate without a restart. Administrative actions emit audit events carrying
receipts the backend captured at mutation.

## Running it

The `tollgate-server` binary reads:

- `TOLLGATE_SECURITY_CONFIG`: the required JSON security manifest
  (identities, TLS, and optionally an issuer secret).
- `TOLLGATE_BIND`: the listen address, default `127.0.0.1:8080`.
- `TOLLGATE_STORE`: `memory` (the default; ephemeral, for development) or
  `postgres`.
- `TOLLGATE_PG_URL`: the PostgreSQL connection URL, for the `postgres` store.
- `TOLLGATE_RECLAIM_INTERVAL_SECS`: the expiry sweep interval, default 5.

[`docs/CONTROL_PLANE_SECURITY.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/CONTROL_PLANE_SECURITY.md)
covers deployment, credential rotation, and audit collection;
[`docs/ACCOUNT_ADMINISTRATION.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/ACCOUNT_ADMINISTRATION.md)
covers operator actions.

## Features

- `postgres` (default): the PostgreSQL backend, via
  [`tollgate-store-postgres`](https://crates.io/crates/tollgate-store-postgres).
  Without it, the server builds with no database dependency.
- `test-support`: test-only APIs of the PostgreSQL backend (destructive
  fixture reset, query-plan inspection). Not for production builds, and not
  covered by semantic versioning: it may change in any release.

## Contract

[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md)
specifies the ledger, fencing, and ingest rules the server enforces through its
backend.

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
