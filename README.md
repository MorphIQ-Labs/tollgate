# tollgate

Abstract quota admission and usage accounting for latency-critical web
services: centrally allocated, **fenced quota leases** spent by **local atomic
counters**, immutable **compiled account snapshots** for admission, and
**idempotent batched usage events** for billing — so a request path with a
microsecond budget never performs synchronous I/O.

Started as a proof of concept for FerroRisk's admission layer; built as a
domain-agnostic product: the core knows cost units, operations-by-index, and
permission bits — never plan names, FCUs, or SQL.

## Layout

| Crate | Role |
|---|---|
| `crates/tollgate-core` | Zero-I/O, clock-free hot path: `CostUnits` (checked), `CostTable` (direct-indexed), `AccountSnapshot`, `LocalLease` (fenced, CAS), `Reservation` (pending → committed-at-execution-start \| released) |
| `crates/tollgate-admission` | One-call pipeline: snapshot map (arc-swap and moka candidates) → permissions → quote → weighted `governor` rate token → lease reservation |
| `crates/tollgate-auth` | Credential verification: `CredentialVerifier` scheme seam, `HmacRegistry` (digests at rest), `SessionCredential` session cache with a validity bound |
| `crates/tollgate-store` | `LeaseAllocator` / `SnapshotSource` / `KeySource` / `UsageSink` / `AdminStore` traits, `GrantPolicy`, `MemoryStore` reference backend, wire DTOs, `Clock` |
| `crates/tollgate-store-postgres` | Transactional Postgres backend (row-locked acquire, set-wise usage ingest, bounded set-wise SKIP LOCKED reclaim) |
| `crates/tollgate-server` | Authenticated rustls control plane, disjoint instance/operator roles, rotating mTLS/bearer/Google identity, administrative audit |
| `crates/tollgate-client` | `InstanceRuntime` (dynamic account supervision, readiness, bounded shutdown), staged `RuntimeHandle`, credential `KeyManager`, direct-store `PeriodRoller`, lower-level lease/snapshot/usage managers, `HttpStore` with rotatable TLS and service identity |
| `crates/tollgate-perf-gate` | Benchmark threshold checker (criterion estimates vs manifest, staleness-guarded) |
| `examples/pricing-api` | Concrete API embedding the stack: connection-cached HMAC-verified keys, admission, commit-at-execution-start, billing |

Contract: [`INVARIANTS.md`](INVARIANTS.md). Architecture and findings:
[`docs/DESIGN.md`](docs/DESIGN.md).

Direct-store applications using budget schedules run a `PeriodRoller` beside
their admission runtime. It funds schedules at startup, rolls due periods in
bounded batches, and exposes health and shutdown reports. See the
[direct-store lifecycle example](crates/tollgate-client/src/period_roller.rs).
`InstanceRuntime` does not receive administrative authority; applications using
`HttpStore` leave period maintenance to `tollgate-server`.

HTTP-backed applications authenticate customer keys through a read-only
`KeySource` and own a `KeyManager` beside the runtime. Its immutable
verifier bounds cached evidence by feed freshness and each key's expiry. See
[credential projection](docs/CREDENTIAL_PROJECTION.md) for lifecycle composition,
readiness, revocation windows and rollout.

See [usage accounting](docs/USAGE_ACCOUNTING.md) for batch rejection semantics,
PostgreSQL numeric limits, and database-guard migration and recovery.
See [snapshot operations](docs/SNAPSHOT_OPERATIONS.md) for generation-refusal
diagnostics and retained readiness after task exit.
See [lease ownership and test support](docs/LEASE_OWNERSHIP.md) for explicit
grant retirement and PostgreSQL fixture API migration.

## Quickstart

```sh
git config core.hooksPath .githooks         # once per clone: rustfmt check on commit

cargo test --workspace                     # correctness (Postgres DB cases are env-gated)
./scripts/check_advisories.sh              # RustSec + yanked/informational dependency gate
./scripts/check_formal.sh                  # Lean lease/snapshot proofs
./scripts/check_perf_thresholds.sh         # hot-path microbench gate
./scripts/check_load_thresholds.sh         # local ratios + controlled-host absolutes

# Postgres correctness suite:
docker compose up -d
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate cargo test -p tollgate-store-postgres
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate ./scripts/check_mutations.sh --diff main

# One crate's whole surface, rather than only what a branch changed:
./scripts/check_mutations.sh --package tollgate-core

# Run the example service:
cargo run -p pricing-api --bin pricing-api -- --help
cargo run -p pricing-api --bin pricing-api
# Opt in only after profiling sustained same-account cross-core contention:
TOLLGATE_LOCAL_SHARDS=8 cargo run -p pricing-api --bin pricing-api
curl -s -H 'Authorization: Bearer demo-key-1' -H 'Content-Type: application/json' \
     -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
     http://127.0.0.1:8081/v1/price

# What that instance admitted and refused, by reason:
curl -s http://127.0.0.1:8081/metrics

# Configure control-plane identities/TLS first; see docs/CONTROL_PLANE_SECURITY.md:
TOLLGATE_SECURITY_CONFIG=/path/to/security.json cargo run -p tollgate-server
```

The [control-plane security runbook](docs/CONTROL_PLANE_SECURITY.md) covers TLS,
Cloud Run service identity, credential rotation, audit collection, and the Rust
API/configuration rollout. Remote plaintext and anonymous control-plane calls
are refused.

[Instance-local sharding](docs/LOCAL_SHARDING.md) covers what the opt-in above
buys, how to size it against your worker pool, and how to read the occupancy an
instance reports — sharding separates threads only while the affinities issued
do not outnumber the shards, and an instance says when they do.

Every binary accepts `--help`/`-h` and `--version`/`-V` before validating other
arguments or application configuration. The first information flag before `--`
wins; everything after that marker is positional. `pricing-api` and
`tollgate-server` take no positional arguments: no arguments or a bare `--`
starts the service, and other arguments exit with status 2. Information commands
exit successfully without starting the application, binding its listener or
running measurements. The gate tools retain their documented positional inputs.

## Supported Rust toolchains

The workspace declares Rust 1.89 as its minimum supported Rust version
(MSRV). Every merge request checks the locked workspace, including all
features and targets, with Rust 1.89.0 so dependency updates cannot silently
raise that floor.

`rust-toolchain.toml` separately pins Rust 1.97.1 for local development and
the primary CI jobs. That pin provides reproducible formatting, linting,
testing, and release tooling; it does not replace the MSRV contract. Changes
to the declared minimum and the dedicated `msrv` job must land together.

## Performance measurements

Timed Criterion and production-profile loopback load tests run locally.
Performance-sensitive merge requests and releases carry the reports and their
host/revision provenance. CI compiles the benchmarks and checks allocation
counts; remote timing does not decide whether a change can merge. See
[the local performance workflow](docs/PERFORMANCE.md).

The September 9 #105 run on an Apple M1 Pro measured full admission at about
134 ns and the separate owned admission/commit/emission fixtures at 118–126 ns.
Those fixtures have different workloads and their times are not additive.
Cached managed credential authentication measured about 38 ns; HMAC verification
on a cache miss was about 800 ns, with session setup measured separately.
These are host-specific measurements, not end-to-end HTTP latency guarantees.
The [credential activity evidence](testing/credential_activity_evidence.json)
and [projection evidence](docs/CREDENTIAL_PROJECTION_EVIDENCE.md) record the
workloads and limitations. Gate manifests live in `testing/`; changes to their
thresholds require deliberate calibration evidence.

## Design rules

- The request path performs no I/O, takes no blocking locks, and reads no
  wall clock for a policy decision (`now` is an argument). Everything slow is
  a background plane. Moka's cache housekeeping and governor's bucket
  arithmetic do read their own monotonic clocks, and moka's takes a
  non-blocking `try_lock` on roughly every sixty-fourth lookup; those are
  measured mechanism costs, never sources of truth.
- Fail closed: unknown, stale, exhausted, or backpressured states deny with
  zero units charged — there is no slower fallback path.
- Snapshot revocations are durable, generation-ordered tombstones; delayed
  control-plane messages cannot resurrect an older authorization state.
- Readiness is continuous, covering snapshot freshness/task health, lease
  usability, and accounting-writer health rather than only initial loading.
- Leases bound spend; usage events are the billing truth; per-account
  conservation (`deposited + overage_recorded == balance + active grants +
  settled usage + settlement loss + expired`) is asserted exactly in every
  backend's suite. The [ledger contract](INVARIANTS.md) explains overage
  funding and expired allowances.
- Backends are honest trait implementations: the memory store is the
  executable spec, and Postgres passes the same scenario suite by name.
- Every failure here is silent and recoverable by design, so the signals are
  the only way to tell "working" from "broken for twenty minutes": what is
  emitted, what normal looks like, and what to do about each reading are in
  [Observability](docs/DESIGN.md#observability), along with the reconciliation
  query for checking the two ledgers agree on a live system.
