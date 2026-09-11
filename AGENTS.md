# Repository Guidelines

**Correctness, performance, and clean architecture are first-class features of every MorphIQ Labs project**, not qualities traded away for delivery speed or deferred to a follow-up. Each is held to evidence, and this document is how: correctness is *proven* — by the invariants, their enforcement ladder, and the assurance gates, never asserted; performance is *designed* — the complexity class, the data structure, the allocation, and any vectorization chosen deliberately at design time, then measured wherever measurement applies; architecture is *enforced* — boundaries checked mechanically, and complexity removed rather than accumulated. What each demands concretely differs by project; that all three stay top of mind does not. A change that erodes any of the three is incomplete however quickly it ships, and "it works" is not evidence for any of them.

## Purpose and Architecture

Tollgate provides credential verification, quota admission, and usage accounting for latency-critical services. Centrally allocated, fenced quota leases are spent through local atomic counters; immutable account snapshots drive admission; idempotent, batched usage events drive billing.

The system has two strictly separated planes:

- The request path (`tollgate-core` and `tollgate-admission`) performs no I/O, takes no blocking locks, and reads no wall/business clock for policy decisions. Callers pass `jiff` timestamps explicitly. Moka maintenance and governor's bucket arithmetic read their own monotonic clocks; those are measured mechanism costs, never sources of snapshot or lease truth. Its pipeline is `SnapshotMap` lookup (arc-swap or moka), `AccountSnapshot` status/permissions, direct-indexed `CostTable` quote, weighted `governor` token, lease debit, then `Reservation`.
- The control plane (`tollgate-client`, `tollgate-server`, and a store backend) handles leases, snapshot distribution, usage ingestion, and expiry reclamation in background tasks.

`INVARIANTS.md` is the testable contract; `docs/DESIGN.md` records architecture and rationale. Read both before behavioral changes. Violating an invariant is a defect even if tests pass, and a new invariant must name its enforcing test.

## Project Structure

Workspace crates live in `crates/`. `tollgate-auth` owns the credential step: `CredentialVerifier` is the pluggable scheme seam, `HmacRegistry` is the implementation in the box, and `SessionCredential` is the session-scoped cache that keeps a digest off the per-request path. It is in the library rather than in an embedder because the credential check is the *largest* cost in front of admission — roughly 800 ns against admission's ~107 ns — and because getting its invalidation ordering right is a security convention that drifts when every embedder writes it again. `tollgate-store` defines `LeaseAllocator`, `SnapshotSource`, `UsageSink`, and `AdminStore`. Its `MemoryStore` is the executable reference implementation; `PostgresStore` and client-side `HttpStore` must preserve those semantics. `LeaseManager` and `UsageWriter` run over direct or HTTP backends. `examples/pricing-api` embeds the full stack. Unit tests stay beside source, integration tests in each crate's `tests/`, Criterion benchmarks in `benches/`, PostgreSQL migrations in `crates/tollgate-store-postgres/migrations/`, and performance manifests/reports in `testing/` and `reports/`.

## Build, Test, and Development Commands

```sh
git config core.hooksPath .githooks
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
./scripts/check_advisories.sh

cargo test -p tollgate-client
cargo test -p tollgate-core reservation::tests::commit_cancel_race_one_winner

docker compose up -d
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate \
  cargo test -p tollgate-store-postgres

./scripts/check_perf_thresholds.sh
./scripts/check_load_thresholds.sh
./scripts/check_allocations.sh
./scripts/check_ci_rules.sh
cargo run -p pricing-api
cargo run -p tollgate-server
```

CI uses `check`, `test`, `assurance`, and `release`: format, Clippy, title convention, repository hygiene, all-feature workspace tests, the PostgreSQL suite, benchmark compilation, and deterministic allocation assertions run on every merge request. The blocking dependency-advisory scan runs on merge requests and the default branch. Mutation and formal assurance run on **every** merge request, whatever it targets (#106). Timed Criterion and loopback load tests run **locally**, including for release validation; remote runners do not execute them or decide performance acceptance. `./scripts/check_ci_rules.sh` enforces the remaining assurance gates and this local-only measurement policy. Performance-sensitive merge requests include local reports with the tested revision, host, toolchain, profile and verdict; unreadable evidence is reported explicitly and is not a successful performance validation. See `docs/PERFORMANCE.md` for commands and review requirements. Release preparation and tagging remain automatic after merge. The `production` profile (fat LTO, `panic=abort`) is for deployment and the local load gate; Criterion retains the default release profile. Keep `rust-toolchain.toml` and the GitLab CI Rust image in lockstep.

## Design Constraints

- Fail closed with zero charge for unknown, stale, overflowed, or backpressured states, under every enforcement mode. A lease that cannot fund the quote — absent, expired, or exhausted — also fails closed under `EnforcementMode::Strict`; it is the single condition `Elastic` may admit past, because it is the only one that speaks to *funding* rather than validity, and every unit it admits is recorded as overage. Never add a synchronous store fallback to the request path.
- Use checked arithmetic for cost and lease math. Authorization generations never move backward; revocations use durable, generation-ordered tombstones.
- Admission creates a pending debit. Commit at execution start charges success, failure, and timeout; cancellation before execution releases it. Preserve the single compare-exchange commit/cancel race and the pre-reserved queue permit that `RequestContext::admit` binds as the request's `UsageSlot`, which `Committed`'s drop guarantees is emitted across panic or abort.
- Leases bound spend; usage events are billing truth. Backends must enforce `deposited + overage recorded == balance + active grants + settled usage + loss`. Overage is a funding term, not a bucket: an elastic admission moves it and settled usage together in one transaction, or the equation reports corruption on a correct ledger.
- Leases are usable only through `expires_at - safety_margin`; allocation reclaims them only after `expires_at + grace`. Validate timing configuration before starting tasks or debiting balances. Readiness remains continuous across snapshot freshness, usable leases, and background-task health.
- Keep standards documents current-state only; put discovery history in `docs/DESIGN.md`. Crates remain unpublished and are distributed deliberately by Git tag.
- **Performance**: the request path is the hot path, and its budget is structural — no I/O, no blocking locks, and no wall/business-clock reads for policy decisions. Moka and governor's internal monotonic reads are explicit measured costs, not an excuse to read time for admission truth. That is what makes the rest defensible: `SnapshotMap` lookup through arc-swap/moka, direct-indexed `CostTable` quoting that must stay O(1) in the cost table's size, a weighted token, a lease debit, a `Reservation`. A scan where an index belongs, or an allocation per admission, is a defect rather than a slow path. `./scripts/check_perf_thresholds.sh`, `./scripts/check_load_thresholds.sh`, and `./scripts/check_allocations.sh` are the gates, and the `production` profile (fat LTO, `panic=abort`) is what the load gate measures. Absolute-latency thresholds are calibrated on a controlled host and meaningful only there; overhead ratios require comparable measurement conditions, so run them locally with the same workload and record host conditions. A threshold change lands with measured evidence and a deliberate manifest update. `testing/perf_baseline.json` is generated whole by `./scripts/check_perf_thresholds.sh --record` and never hand-edited, so a change that adds work to a benchmarked path recalibrates every row in the same change; a full run that could not enforce that baseline exits `UNENFORCED` rather than passing, because a report that checked nothing host-specific is not performance evidence.

## Compatibility, Migrations, and Operations

- Treat public Rust APIs, wire DTOs, RFC-7807 error codes, configuration, and database schemas as contracts. State whether a change is backward compatible and provide a rollout or migration plan for intentional breaks.
- Add forward SQL migrations; never rewrite a migration that may have been applied. Test upgrades and mixed-version behavior when rollout safety depends on them, and document recovery or rollback constraints.
- Return structured errors for external input, dependency, and operational failures. Reserve panics and `expect` for documented internal invariants that callers cannot violate; never convert a recoverable failure into a crash or silent fallback.
- Keep logs structured, contextual, and actionable without exposing sensitive data. Operational changes must consider readiness, shutdown, backpressure, retry bounds, and failure visibility.

## Invariants and Trust Boundaries

Invariants live in `INVARIANTS.md`, the testable contract, with rationale in `docs/DESIGN.md`; violating one is a defect even if tests pass. Every new or changed invariant names its enforcement, chosen from this ladder: (1) unrepresentable by construction (types, guards), (2) enforced inside the owning component, (3) only then a tested convention hardened by mutation gates — mutation testing proves a test bites, but cannot stop an invariant from being convention, and conventions upheld by caller discipline at multiple call sites drift. It also names its test witnesses: focused unit and mutation tests, plus property or integration tests where behavior crosses a boundary. Tests verify enforcement; they do not replace ownership by types or by the component where the contract first applies. The rules below are paid-for lessons; treat them as review gates.

- **Never assert source text, byte offsets, or statement order in a test.** If ordering matters, make the wrong order unrepresentable (e.g. the dependent action is a method on the guard the prerequisite returns).
- **Trust boundaries exchange verified evidence, not re-derivation.** When one plane has verified data, hand the next a typed, validated proof (a `Reservation`, a snapshot generation, a lease) instead of having it re-check. Adding an expensive re-verification pass to the request path must be justified against an explicit budget.
- **Partial data is surfaced, never silently absorbed.** A stale snapshot, a truncated batch, or a missing counterpart is reported out of band; silence lets damage masquerade as a clean result or as a downstream bug.
- **Records with more than one writer need an explicit concurrency story**: exclusive creation, atomic publication, and defined semantics for divergence — the single compare-exchange commit/cancel race is the model.
- **Escape hatches ship complete**: any flag that bypasses an invariant lands in the same change as its durable audit trail, its remediation tooling, and its operator-doc entry. A permanent mark with no exit is an operational trap.
- **Config values are contracts**: never silently change the runtime semantics of an existing value. If semantics must change, warn loudly at startup or rename the value.
- **Limits admit worst-case legitimate data**: every quota, batch, and rate cap is justified against the largest legitimate input the system can produce, not a comfortable constant.
- **Operational tooling validates before it replaces**: anything overwriting last-known-good data (snapshots, promoted artifacts, state files) stages, validates, then promotes atomically, so failure preserves the previous good copy. Correctness logic belongs in Rust with integration tests; shell stays a thin wrapper.
- **Every binary handles `--help`, `--version`, and `--` before argument validation.**

## Engineering and Review Principles

- Treat technical debt as a code-review concern, and finish the job in the change that surfaces it: a fix targets the defect pattern, not the instance, so same-pattern siblings in the crate or module you are already changing are in scope — not material for follow-on issues or MRs. Search that scope for siblings and say what the search found, including when it found none: a reviewer cannot otherwise tell an absence from an omission. Defer only when the issue proved materially misdiagnosed, when the remaining work needs its own design and rollout story, when completing the pattern would make the change too large to review (an unreviewable diff is its own risk), or when a sibling sits in code an in-flight change is already rewriting. Then enumerate the remaining sites in a clearly scoped follow-up issue — never a vague someday ticket, and never an undocumented TODO.
- For defects caused by regressions, perform a root-cause analysis. Identify how the regression entered, why existing safeguards missed it, and what test, invariant, tooling, or process change will prevent recurrence. Use each regression to strengthen the system.
- Fix problems at the layer where their contract is first violated. Do not mask a downstream defect with an upstream workaround or symptom-specific patch.
- Optimize for simplicity and maintainability. Prefer clear designs over incidental compatibility with awkward internals, and refactor when doing so removes complexity or restores sound boundaries.
- Documentation and design artifacts ship in the same change that invalidates them: `INVARIANTS.md`, `docs/DESIGN.md`, operator-facing docs, and embedded diagrams. A doc that no longer describes the current system is a defect, not deferred polish. Keep standards documents current-state only; discovery history belongs in `docs/DESIGN.md`.

## Assurance

- Treat numerical algorithms and critical accounting or concurrency state machines as proof obligations. Define the domain, units, preconditions, postconditions, invariants, and failure semantics before implementation. Prove conservation, bounds, monotonicity, idempotency, and state-transition safety as applicable; use machine-checkable specifications, model checking, SMT, or proof-assistant artifacts for critical properties and keep those artifacts versioned and reproducible.
- Separate exact-model proofs from implementation evidence. Validate finite-precision behavior, overflow boundaries, tolerances, and exceptional inputs independently with canonical references or independent oracles plus property, adversarial, and mutation tests. State assumptions and error bounds explicitly. Tests and numerical agreement support a proof-to-code argument but are not themselves proof; never claim more assurance than the checked artifacts establish.
- Keep assurance requirements executable. Pin mutation and formal-verification configuration, provide documented local commands, run required gates in CI, and retain inspectable reports or proof artifacts. Adding a requirement without its runnable tooling and enforcement is incomplete work; if the initial tooling has a large blast radius, track it in a linked, scoped issue and do not overstate current assurance.

## Security, Safety, and Dependencies

- Never commit or log real credentials, authorization headers, HMAC secrets, private keys, or credential-bearing database URLs. Load production secrets from environment variables or an approved secret store. Demo credentials belong only in clearly marked examples and fixtures; sanitize diagnostic output before sharing it.
- Avoid `unsafe`. Introducing it requires prior design justification, a minimal isolated boundary, documented `SAFETY` invariants, focused tests, and explicit review. Prefer denying unsafe code at crate boundaries where no audited exception is needed.
- Add dependencies only when existing workspace or standard-library facilities are insufficient. Document the maintenance, security, licensing, compile-time, and hot-path cost; declare shared versions in the workspace manifest and keep `Cargo.lock` changes intentional.

## Coding Style and Testing

Use rustfmt defaults and idiomatic Rust naming: `snake_case` for modules, functions, and tests; `UpperCamelCase` for types; `SCREAMING_SNAKE_CASE` for constants. Use standard and Tokio tests, `proptest` for properties, and Criterion for hot-path benchmarks. Name tests for observable behavior. A backend behavior change must update both memory and PostgreSQL implementations and their mirrored scenario tests.

## Merge Requests and Releases

`main` accepts no direct pushes; every change lands through a merge request with all discussions resolved. One MR at a time: do not widen scope unasked, and discuss large architectural changes before implementing them. Completing the defect pattern an issue names is finishing the job, not widening scope.

Branch and title share the conventional-commit vocabulary, because release automation reads it: branches are `<type>/<slug>` (`fix/lease-reclaim-grace`), and the MR title is a conventional commit with an optional scope (`feat(admission): install_many bulk write`), enforced by the `commit-convention` job. The MR is squash-merged, so that title becomes the single commit subject on `main` and is what the release notes are computed from.

An MR describes behavioral impact, lists the validation performed, and calls out invariant, migration, API, or threshold changes, with benchmark evidence for performance-sensitive work. It **must close the issues it resolves**: put `Closes #N` (or `Closes #N, #M`) in the description so merging closes them automatically, and confirm they closed. A resolved issue left open is incomplete work; if the change only partially addresses one, say what remains instead of closing it.

Merging to `main` must leave the repository releasable. Releasing is two jobs, and both are automatic. After every merge, `prepare-release` derives the next version from the conventional-commit subjects since the last release, bumps the workspace version, writes the `CHANGELOG.md` section, and opens — or refreshes — the `chore/release` merge request; the scheduled lane is recovery-only. When that merge request lands, `tag-release` cuts the `v{version}` tag and GitLab release at that exact merge, while preparation recognizes the release merge as a no-op. Both jobs are idempotent and safe to retry.

Review the release merge request like any other — the version and the notes are derived, not authoritative. Correct a wrong bump by fixing the commit subjects it came from, or by editing that merge request before it lands. Never hand-cut a tag or hand-edit a released changelog section.

The split matters, because losing half of it is silent. `tag-release` only publishes the version the manifest already declares; when release-plz was removed and nothing took over the bump, `main` carried six commits past `v0.5.0` — one of them breaking — while the release job reported `already exists; nothing to do` on every push and the pipeline stayed green.

**This project does not use release-plz.** It did, and it worked here — but it cannot work in the other MorphIQ Labs workspaces, because `git_only` still runs `cargo package` per crate, which requires every dependency to carry a version requirement *and* resolve on crates.io. Internal unpublished crates cannot satisfy that. One release model across the group beats a working exception, and packaging cost 9.5 minutes a pipeline to produce a tag this job produces in seconds. See `templates/gitlab-ci.reference.yml` in `the template repository`.

The registry guard stays split the way #46 established: manifests carry `publish = false`, and must never carry a registry list (`publish = ["name"]`), which is what made `cargo package --registry <name>` fail. `repository-hygiene` rejects the list form on every merge request. Crates remain unpublished and are distributed deliberately by git tag.

## Repository and Branch Settings

Trunk-based, one long-lived branch. The workflow above is prose until these settings enforce it, and settings drift without ever failing a build — so they are stated here and audited, not assumed. Every MorphIQ Labs project carries the same configuration:

| Setting | Value | Why |
| --- | --- | --- |
| Default branch | `main` | There is no `develop` and no release train. A second long-lived branch would divert merge requests away from `main`; since #106 that no longer disables the blocking assurance gates, which run on every merge request, but it still splits release preparation. |
| `main` protection | push: **No one**; merge: **Maintainers**; force push: **off** | "Accepts no direct pushes" is a setting, not a convention. |
| Squash commits | **Always** | The MR title becomes the single commit subject on the default branch. |
| Merge method | Merge commit | Group convention, for one review model everywhere. |
| Delete source branch | on | |
| All discussions resolved | required | |
| Pipelines must succeed | required — **only where a pipeline exists** | GitLab treats "no pipeline" as "not successful", so enabling this on a repository without `.gitlab-ci.yml` blocks every merge. Turn it on with the pipeline, not before. |

Check them with `glab api "projects/<encoded-path>"` and `glab api "projects/<encoded-path>/protected_branches"` rather than assuming.

## Definition of Done

A change is complete only when the root cause is addressed; relevant format, lint, unit, integration, property, mutation, benchmark, and formal gates pass in proportion to risk; memory/PostgreSQL parity is preserved; affected invariants, APIs, migrations, configuration, and documentation are updated together; and the issues it resolves are closed by the merge. The merge request must assess correctness, security, compatibility, operational, and hot-path performance impact, including benchmark evidence for performance-sensitive changes. Record any work deferred on the narrow grounds above in linked issues that enumerate what remains, with scope and risk; untracked debt, a deferral that had no grounds, or a green test suite alone does not make the change complete.
