# Repository Guidelines

## Purpose and Architecture

Tollgate provides quota admission and usage accounting for latency-critical services. Centrally allocated, fenced quota leases are spent through local atomic counters; immutable account snapshots drive admission; idempotent, batched usage events drive billing.

The system has two strictly separated planes:

- The request path (`tollgate-core` and `tollgate-admission`) performs no I/O, takes no locks, and reads no clock. Callers pass `jiff` timestamps explicitly. Its pipeline is `SnapshotMap` lookup (arc-swap or moka), `AccountSnapshot` status/permissions, direct-indexed `CostTable` quote, weighted `governor` token, lease debit, then `Reservation`.
- The control plane (`tollgate-client`, `tollgate-server`, and a store backend) handles leases, snapshot distribution, usage ingestion, and expiry reclamation in background tasks.

`INVARIANTS.md` is the testable contract; `docs/DESIGN.md` records architecture and rationale. Read both before behavioral changes. Violating an invariant is a defect even if tests pass, and a new invariant must name its enforcing test.

## Project Structure

Workspace crates live in `crates/`. `tollgate-store` defines `LeaseAllocator`, `SnapshotSource`, `UsageSink`, and `AdminStore`. Its `MemoryStore` is the executable reference implementation; `PostgresStore` and client-side `HttpStore` must preserve those semantics. `LeaseManager` and `UsageWriter` run over direct or HTTP backends. `examples/pricing-api` embeds the full stack. Unit tests stay beside source, integration tests in each crate's `tests/`, Criterion benchmarks in `benches/`, PostgreSQL migrations in `crates/tollgate-store-postgres/migrations/`, and performance manifests/reports in `testing/` and `reports/`.

## Build, Test, and Development Commands

```sh
git config core.hooksPath .githooks
cargo fmt --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features

cargo test -p tollgate-client
cargo test -p tollgate-core reservation::tests::commit_cancel_race_one_winner

docker compose up -d
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate \
  cargo test -p tollgate-store-postgres

./scripts/check_perf_thresholds.sh
./scripts/check_load_thresholds.sh
cargo run -p pricing-api
cargo run -p tollgate-server
```

CI runs format checks, Clippy with warnings denied, all-feature workspace tests, the PostgreSQL suite, and benchmark compilation. Absolute-latency thresholds are calibrated only on a controlled host; overhead ratios are portable. The `production` profile (fat LTO, `panic=abort`) is for deployment and the load gate; Criterion retains the default release profile. Keep `rust-toolchain.toml` and the GitLab CI Rust image in lockstep.

## Design Constraints

- Fail closed with zero charge for unknown, stale, exhausted, overflowed, or backpressured states. Never add a synchronous store fallback to the request path.
- Use checked arithmetic for cost and lease math. Authorization generations never move backward; revocations use durable, generation-ordered tombstones.
- Admission creates a pending debit. Commit at execution start charges success, failure, and timeout; cancellation before execution releases it. Preserve the single compare-exchange commit/cancel race and `ChargeGuard`'s pre-reserved queue permit, which guarantees usage emission across panic or abort.
- Leases bound spend; usage events are billing truth. Backends must enforce `deposited == balance + active grants + settled usage + loss`.
- Leases are usable only through `expires_at - safety_margin`; allocation reclaims them only after `expires_at + grace`. Validate timing configuration before starting tasks or debiting balances. Readiness remains continuous across snapshot freshness, usable leases, and background-task health.
- Keep standards documents current-state only; put discovery history in `docs/DESIGN.md`. Crates remain unpublished and are distributed deliberately by Git tag.

## Engineering and Review Principles

- Treat technical debt as a code-review concern. Address it early and continuously when the change is safe and scoped; when the blast radius is too large for the current work, create a clearly scoped follow-up issue rather than leaving an undocumented TODO.
- For defects caused by regressions, perform a root-cause analysis. Identify how the regression entered, why existing safeguards missed it, and what test, invariant, tooling, or process change will prevent recurrence. Use each regression to strengthen the system.
- Fix problems at the layer where their contract is first violated. Do not mask a downstream defect with an upstream workaround or symptom-specific patch.
- Optimize for simplicity and maintainability. Prefer clear designs over incidental compatibility with awkward internals, and refactor when doing so removes complexity or restores sound boundaries.
- Surface invariants explicitly and keep them current in `INVARIANTS.md`. Encode every new or changed invariant in focused unit tests and mutation tests, adding integration or property tests where the behavior crosses boundaries.

## Numerical and Formal Assurance

- Treat numerical algorithms and critical accounting or concurrency state machines as proof obligations. Define the domain, units, preconditions, postconditions, invariants, and failure semantics before implementation. Prove conservation, bounds, monotonicity, idempotency, and state-transition safety as applicable; use machine-checkable specifications, model checking, SMT, or proof-assistant artifacts for critical properties and keep those artifacts versioned and reproducible.
- Separate exact-model proofs from implementation evidence. Validate finite-precision behavior, overflow boundaries, tolerances, and exceptional inputs independently with canonical references or independent oracles plus property, adversarial, and mutation tests. State assumptions and error bounds explicitly. Tests and numerical agreement support a proof-to-code argument but are not themselves proof; never claim more assurance than the checked artifacts establish.
- Keep assurance requirements executable. Pin mutation and formal-verification configuration, provide documented local commands, run required gates in CI, and retain inspectable reports or proof artifacts. Adding a requirement without its runnable tooling and enforcement is incomplete work; if the initial tooling has a large blast radius, track it in a linked, scoped issue and do not overstate current assurance.

## Security, Safety, and Dependencies

- Never commit or log real credentials, authorization headers, HMAC secrets, private keys, or credential-bearing database URLs. Load production secrets from environment variables or an approved secret store. Demo credentials belong only in clearly marked examples and fixtures; sanitize diagnostic output before sharing it.
- Avoid `unsafe`. Introducing it requires prior design justification, a minimal isolated boundary, documented `SAFETY` invariants, focused tests, and explicit review. Prefer denying unsafe code at crate boundaries where no audited exception is needed.
- Add dependencies only when existing workspace or standard-library facilities are insufficient. Document the maintenance, security, licensing, compile-time, and hot-path cost; declare shared versions in the workspace manifest and keep `Cargo.lock` changes intentional.

## Compatibility, Migrations, and Operations

- Treat public Rust APIs, wire DTOs, RFC-7807 error codes, configuration, and database schemas as contracts. State whether a change is backward compatible and provide a rollout or migration plan for intentional breaks.
- Add forward SQL migrations; never rewrite a migration that may have been applied. Test upgrades and mixed-version behavior when rollout safety depends on them, and document recovery or rollback constraints.
- Return structured errors for external input, dependency, and operational failures. Reserve panics and `expect` for documented internal invariants that callers cannot violate; never convert a recoverable failure into a crash or silent fallback.
- Keep logs structured, contextual, and actionable without exposing sensitive data. Operational changes must consider readiness, shutdown, backpressure, retry bounds, and failure visibility.

## Coding Style and Testing

Use rustfmt defaults and idiomatic Rust naming: `snake_case` for modules, functions, and tests; `UpperCamelCase` for types; `SCREAMING_SNAKE_CASE` for constants. Use standard and Tokio tests, `proptest` for properties, and Criterion for hot-path benchmarks. Name tests for observable behavior. A backend behavior change must update both memory and PostgreSQL implementations and their mirrored scenario tests.

## Commits and Merge Requests

Use concise, imperative subjects with an optional scope, for example `admission: install_many bulk write`; `fix:`, `docs:`, and `chore:` are established prefixes. Keep commits focused. Merge request titles must carry the issue number (`#43: summary`); `Closes #N` in the body is insufficient. Describe behavioral impact, link the issue, list validation performed, and call out invariant, migration, API, or threshold changes.

## Definition of Done

A change is complete only when the root cause is addressed; relevant format, lint, unit, integration, property, mutation, benchmark, and formal gates pass in proportion to risk; memory/PostgreSQL parity is preserved; and affected invariants, APIs, migrations, configuration, and documentation are updated together. The merge request must assess correctness, security, compatibility, operational, and hot-path performance impact, including benchmark evidence for performance-sensitive changes. Record any deliberately deferred work in linked issues with scope and risk; untracked debt or a green test suite alone does not make the change complete.
