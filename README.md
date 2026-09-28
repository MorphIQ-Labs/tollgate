# Tollgate

[![crates.io](https://img.shields.io/crates/v/tollgate-core.svg)](https://crates.io/crates/tollgate-core)
[![docs.rs](https://img.shields.io/docsrs/tollgate-core)](https://docs.rs/tollgate-core)
[![CI](https://github.com/MorphIQ-Labs/tollgate/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MorphIQ-Labs/tollgate/actions/workflows/ci.yml)
[![MSRV 1.89](https://img.shields.io/badge/MSRV-1.89-blue.svg)](#status)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Quota admission and usage accounting for latency-critical services, with no
I/O on the request path.**

A metered API has to answer "may this caller do this, and what does it cost?"
on every request, and record the answer for billing. The usual answers put a
database or a shared counter on the request path, which costs orders of
magnitude more than the work it guards when that work takes nanoseconds or
microseconds. Rate limiters remove the round trip but don't bill anything.

Tollgate splits the problem into two planes:

- **The request path** admits against an immutable compiled snapshot of the
  account and debits a *fenced quota lease* through local atomic counters. It
  performs no I/O, takes no blocking locks, and reads no clock for a policy
  decision.
- **The control plane** does everything slow in the background: it allocates
  leases from a central balance, distributes snapshots and revocations, and
  ingests idempotent, batched usage events as the billing record.

The core is domain-agnostic. It knows cost units, operations by index and
permission bits, never plan names, product currencies or SQL.

## What you get

- **Admission in one call chain:** snapshot lookup, status and permission
  checks, a direct-indexed cost quote, weighted rate limiting, concurrency
  limits, and a lease debit that opens a pending charge.
- **Charging at execution start:** success, failure and timeout are all
  charged, and cancelling before execution releases the debit for zero units.
  A committed charge is recorded even across a panic.
- **Strict or elastic enforcement:** a strict account never spends past its
  allocation. An elastic account may run past an unfunded lease, and every
  unit it does is recorded as overage.
- **Budgets and periods:** allowances roll per period, with expired units
  accounted for rather than lost.
- **Credentials:** HMAC API keys with digests at rest, session-scoped caching,
  and bounded revocation windows.
- **Backends:** an in-memory store that is the executable specification, and
  a PostgreSQL store that passes the same scenario suite by name.
- **A control-plane server** over rustls with mTLS, rotating bearer tokens or
  Google service-account identity, disjoint instance and operator roles, and
  an audit trail for every administrative change.

## Guarantees, and how they're checked

Tollgate is built so that its claims are checked, not asserted:

- **[40 numbered invariants](INVARIANTS.md)** form the testable contract.
  Each one names the tests that enforce it, and CI fails if a named witness
  stops existing.
- **Fail closed:** unknown, stale, exhausted or backpressured states deny with
  zero units charged. There is no slower fallback path to fail open into.
- **Exact conservation:** every backend's suite asserts, per account,
  `deposited + overage_recorded == balance + active grants + settled usage +
  settlement loss + expired`.
- **Machine-checked proofs:** [21 Lean 4 modules](formal/lean/README.md)
  with 273 theorems, and no `sorry` or axioms, model a request's charge
  lifecycle, lease timing and fencing, idempotent ingest, sharded counters,
  snapshot revocation, conservation and more. CI checks them on every pull
  request.
- **Mutation testing:** every pull request is mutation-tested, so a test that
  doesn't bite fails the build.
- **Measured performance:** hot-path benchmarks and allocation counts are
  gated. In September 2026, full admission measured about 134 ns on an Apple
  M1 Pro, cached credential authentication about 38 ns, and HMAC verification
  on a cache miss about 800 ns. Those are host-specific microbenchmarks, not
  end-to-end HTTP latency; [the performance workflow](docs/PERFORMANCE.md)
  records the workloads and their limits.

## Install

The library crates release together at one version. Depend on the ones you
need, at the same version:

```toml
[dependencies]
tollgate-core = "0.30"
tollgate-admission = "0.30"
# The managed runtime: snapshot distribution, lease renewal, usage batching.
tollgate-client = "0.30"
```

| Crate | Role |
|---|---|
| [`tollgate-core`](https://docs.rs/tollgate-core) | Zero-I/O, clock-free types: cost units and tables, account snapshots, fenced leases, reservations |
| [`tollgate-admission`](https://docs.rs/tollgate-admission) | The per-request admission pipeline |
| [`tollgate-auth`](https://docs.rs/tollgate-auth) | Credential verification: the HMAC registry and session credentials |
| [`tollgate-store`](https://docs.rs/tollgate-store) | Backend traits, the in-memory reference store, wire types |
| [`tollgate-store-postgres`](https://docs.rs/tollgate-store-postgres) | The transactional PostgreSQL backend |
| [`tollgate-client`](https://docs.rs/tollgate-client) | `InstanceRuntime`, the HTTP store client, key and period management |
| [`tollgate-server`](https://docs.rs/tollgate-server) | The authenticated control-plane server |

## A request, end to end

`begin` looks the caller up and pins its snapshot, `admit` quotes the
operation and debits the lease, `commit` charges at execution start, and
dropping the committed guard records the billing event. In a service, the
snapshot and lease below arrive from the control plane.

```rust
use std::sync::{Arc, Mutex};

use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, NoGate, Principal, SnapshotMap,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, PublishableSnapshot, RequestId,
    ResolvedLimits, UsageEvent, UsageSlot,
};

/// The one operation this API meters.
struct Price;

impl OpIndex for Price {
    fn index(&self) -> usize {
        0
    }
}

/// Where committed charges go. In a service, a permit from the usage writer.
struct Billing(Arc<Mutex<Vec<UsageEvent>>>);

impl UsageSlot for Billing {
    fn record(self, event: UsageEvent) {
        self.0.lock().unwrap().push(event);
    }
}

let now = Timestamp::from_second(1_755_600_000).unwrap();
let expires = Timestamp::from_second(1_755_600_060).unwrap();

// From the control plane: the account's compiled snapshot (1 unit per
// request plus 2 per priced item) and a 1,000-unit lease.
let costs = CostTable::builder(CostUnits(1), CostUnits(1))
    .weight(&Price, CostUnits(2))
    .build();
let snapshot = AccountSnapshot::builder(
    AccountId(1),
    Generation(1),
    AccountStatus::Active,
    expires,
    PermissionBits::bit(0),
    ResolvedLimits::new(64),
    Arc::new(costs),
)
.build();
let lease = LocalLease::new(
    LeaseGrant {
        lease_id: LeaseId(1),
        account_id: AccountId(1),
        fencing_token: FencingToken(1),
        units: CostUnits(1_000),
        expires_at: expires,
    },
    CostUnits(100),
);

let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
let slot = LeaseSlot::for_account(AccountId(1));
drop(slot.replace(Arc::new(lease)));
engine
    .map()
    .install_publishable(
        Principal(42),
        PublishableSnapshot::try_new(Arc::new(snapshot)).expect("valid snapshot"),
        slot,
    )
    .expect("installed");

// The request path: no I/O from here on.
let billed = Arc::new(Mutex::new(Vec::new()));
let committed = engine
    .begin(Principal(42), PermissionBits::bit(0), now)
    .expect("known, active, permitted caller")
    .admit(&[(Price, 3)], Billing(Arc::clone(&billed)), now)
    .expect("within limits and funded")
    .acquire_capacity(&NoGate)
    .expect("no capacity gate configured")
    .commit(RequestId(1), now)
    .map_err(|(error, _released)| error)
    .expect("inside the lease window");

// ... do the work ...
drop(committed);

assert_eq!(billed.lock().unwrap()[0].units, CostUnits(7)); // 1 + 2 × 3
```

[Embedding Tollgate](docs/EMBEDDING.md) gives the supported request order,
what you implement, what is sealed, and how to shut down without losing
usage.

## Beyond the request path

- **`InstanceRuntime`** (in `tollgate-client`) supervises accounts
  dynamically: it distributes snapshots, acquires and renews leases, batches
  usage, reports continuous readiness, and shuts down within a bound.
- **HTTP-backed services** authenticate customer keys through a read-only
  `KeySource`, with a `KeyManager` beside the runtime whose verifier bounds
  cached evidence by feed freshness and each key's expiry. See
  [credential projection](docs/CREDENTIAL_PROJECTION.md).
- **Direct-store services with budget schedules** run a `PeriodRoller`, which
  funds schedules at startup and rolls due periods in bounded batches. See
  [its lifecycle example](crates/tollgate-client/src/period_roller.rs). With
  `HttpStore`, the server does period maintenance instead.
- **Batch rejection, PostgreSQL limits and recovery** are covered in
  [usage accounting](docs/USAGE_ACCOUNTING.md), and generation refusals in
  [snapshot operations](docs/SNAPSHOT_OPERATIONS.md).

## Try the example service

[`examples/pricing-api`](examples/pricing-api) is a complete metered API with
HMAC-verified keys, admission, commit at execution start, and billing:

```sh
cargo run -p pricing-api --bin pricing-api
curl -s -H 'Authorization: Bearer demo-key-1' -H 'Content-Type: application/json' \
     -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
     http://127.0.0.1:8081/v1/price
curl -s http://127.0.0.1:8081/metrics   # what it admitted and refused, by reason
```

Set `TOLLGATE_LOCAL_SHARDS=8` only after profiling sustained same-account
contention across cores; [instance-local sharding](docs/LOCAL_SHARDING.md)
explains how to size it. The control-plane server starts with
`TOLLGATE_SECURITY_CONFIG=/path/to/security.json cargo run -p tollgate-server`
once identities and TLS are configured, and refuses remote plaintext and
anonymous calls.

Every binary answers `--help` and `--version` before reading any
configuration. `pricing-api` and `tollgate-server` take no positional
arguments, and exit with status 2 on any they are given.

## Documentation

The documentation site is at **<https://morphiq-labs.github.io/tollgate/>**,
and the API reference is on [docs.rs](https://docs.rs/tollgate-core).

- **Using Tollgate:** [embedding](docs/EMBEDDING.md),
  [account administration](docs/ACCOUNT_ADMINISTRATION.md),
  [usage accounting](docs/USAGE_ACCOUNTING.md),
  [snapshot operations](docs/SNAPSHOT_OPERATIONS.md),
  [lease timing](docs/LEASE_TIMING.md),
  [lease ownership](docs/LEASE_OWNERSHIP.md),
  [local sharding](docs/LOCAL_SHARDING.md), and
  [credential projection](docs/CREDENTIAL_PROJECTION.md).
- **Operating it:** the [control-plane security runbook](docs/CONTROL_PLANE_SECURITY.md)
  covers TLS, service identity, credential rotation and audit collection.
  Signals, what normal looks like, and the ledger reconciliation query are in
  [Observability](docs/DESIGN.md#observability).
- **Why it's built this way:** the [design record](docs/DESIGN.md), and the
  [technique disclosures](docs/DISCLOSURES.md), published as prior art with no
  patent claims.

## Status

Tollgate is pre-1.0. A breaking change moves the minor version, so a caret
requirement (`"0.30"`) never crosses one; the [changelog](CHANGELOG.md) marks
every break. The minimum supported Rust version is 1.89, and CI checks it on
every pull request.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the local gates and the
pull-request process. [AGENTS.md](AGENTS.md) is the full engineering contract.
Report security issues privately; see [SECURITY.md](SECURITY.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Tollgate by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Tollgate is a product of MorphIQ Labs, a trade name of Prophetizo LLC.
`cargo deny --locked check licenses` gates every dependency against
[`deny.toml`](deny.toml), which admits only permissive licenses.
