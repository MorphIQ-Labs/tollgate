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
| `crates/tollgate-store` | `LeaseAllocator` / `SnapshotSource` / `UsageSink` / `AdminStore` traits, `GrantPolicy`, `MemoryStore` reference backend, wire DTOs, `Clock` |
| `crates/tollgate-store-postgres` | Transactional Postgres backend (row-locked acquire, SKIP LOCKED reclaim, ON CONFLICT idempotency) |
| `crates/tollgate-server` | Axum control plane over any backend; RFC-7807 errors with stable codes |
| `crates/tollgate-client` | Instance runtime: `LeaseManager` (background refill, quiescence-gated release), `UsageWriter` (permit-based shed-on-overflow batching), `HttpStore` transport |
| `crates/tollgate-perf-gate` | Benchmark threshold checker (criterion estimates vs manifest, staleness-guarded) |
| `examples/pricing-api` | Concrete API embedding the stack: HMAC-verified keys, admission, commit-at-execution-start, billing |

Contract: [`INVARIANTS.md`](INVARIANTS.md). Architecture and findings:
[`docs/DESIGN.md`](docs/DESIGN.md).

## Quickstart

```sh
git config core.hooksPath .githooks         # once per clone: rustfmt check on commit

cargo test --workspace                     # correctness (Postgres DB cases are env-gated)
./scripts/check_formal.sh                  # Lean authorization-state proofs
./scripts/check_perf_thresholds.sh         # hot-path microbench gate
./scripts/check_load_thresholds.sh         # loopback overhead gate (production profile)

# Postgres correctness suite:
docker compose up -d
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate cargo test -p tollgate-store-postgres
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate ./scripts/check_mutations.sh --diff main

# Run the example service:
cargo run -p pricing-api
curl -s -H 'Authorization: Bearer demo-key-1' -H 'Content-Type: application/json' \
     -d '{"contracts":[{"spot":100,"strike":105,"rate":0.05,"vol":0.2,"tte_years":0.25}]}' \
     http://127.0.0.1:8081/v1/price

# What that instance admitted and refused, by reason:
curl -s http://127.0.0.1:8081/metrics

# Run the control plane:
cargo run -p tollgate-server
```

## Supported Rust toolchains

The workspace declares Rust 1.89 as its minimum supported Rust version
(MSRV). Every merge request checks the locked workspace, including all
features and targets, with Rust 1.89.0 so dependency updates cannot silently
raise that floor.

`rust-toolchain.toml` separately pins Rust 1.97.1 for local development and
the primary CI jobs. That pin provides reproducible formatting, linting,
testing, and release tooling; it does not replace the MSRV contract. Changes
to the declared minimum and the dedicated `msrv` job must land together.

## The numbers that matter (laptop, provisional)

Full admission — lookup, status, permissions, quote, rate token, lease
debit — costs **~112 ns** uncontended; end-to-end loopback overhead of the
whole stack (HMAC verification included) is **×1.05** over a no-admission
baseline. One lease acquire funds thousands of requests; two instances
draining one account over HTTP finish with **zero drift** between admission's
committed units and the billing ledger. Gate manifests live in `testing/`;
recalibrate on a controlled host before treating thresholds as the contract.

## Design rules

- The request path performs no I/O, takes no locks, reads no clock (`now` is
  an argument). Everything slow is a background plane.
- Fail closed: unknown, stale, exhausted, or backpressured states deny with
  zero units charged — there is no slower fallback path.
- Snapshot revocations are durable, generation-ordered tombstones; delayed
  control-plane messages cannot resurrect an older authorization state.
- Readiness is continuous, covering snapshot freshness/task health, lease
  usability, and accounting-writer health rather than only initial loading.
- Leases bound spend; usage events are the billing truth; per-account
  conservation (`deposited == balance + active grants + settled usage +
  loss`) is asserted exactly in every backend's suite.
- Backends are honest trait implementations: the memory store is the
  executable spec, and Postgres passes the same scenario suite by name.
- Every failure here is silent and recoverable by design, so the signals are
  the only way to tell "working" from "broken for twenty minutes": what is
  emitted, what normal looks like, and what to do about each reading are in
  [Observability](docs/DESIGN.md#observability), along with the reconciliation
  query for checking the two ledgers agree on a live system.
