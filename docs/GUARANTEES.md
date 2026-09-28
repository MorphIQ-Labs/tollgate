# Guarantees

Tollgate makes specific claims: a strict account never spends past its
allocation, a refusal never charges, every committed charge is billed once,
and the ledger always balances. This page explains how each kind of claim is
checked, maps the whole contract to its proofs, and states what is *not*
established.

The contract itself is [the invariants](../INVARIANTS.md): 41 numbered
statements, each naming how it is enforced and the tests that witness it.
Violating one is a defect even if every test passes.

## How an invariant is enforced

Every invariant states its enforcement, chosen from a ladder, highest first:

1. **Unrepresentable by construction.** Types and guards make the wrong state
   impossible to write. A `Reservation` can be committed or cancelled, not
   both, because each consumes it.
2. **Enforced inside the owning component.** The store, the runtime or the
   admission map refuses the violation, whatever its caller does.
3. **A tested convention, hardened by mutation testing.** Used only when
   neither of the above is possible, because a rule that relies on every
   caller's discipline drifts.

Tests verify enforcement; they don't replace it.

## The evidence

| Evidence | What it establishes | Where it runs |
|---|---|---|
| Invariant witnesses | Every test, declaration and theorem an invariant cites exists. A renamed or deleted witness fails the build. It checks citations, not that the cited test enforces the claim. | Every pull request |
| Unit, property and integration tests | The behavior. Property tests (`proptest`) cover the interleavings fixed tests miss. | Every pull request |
| Backend parity | `MemoryStore` is the executable specification. `PostgresStore` passes the same scenario suite, test by test and name by name, and a check fails if a mirrored test drives a different contract on each side. | Every pull request |
| Mutation testing | Code a pull request changes is mutated with `cargo-mutants`. A mutant that no test catches fails the build, so a test that doesn't bite can't pass as coverage. | Every pull request |
| Machine-checked proofs | 24 Lean 4 modules with 364 theorems prove exact models of the critical accounting and concurrency state machines. The gate rejects any `sorry` or `admit`. | Every pull request |
| Proof mutation testing | Every transition in the Lean models is mutated, one change at a time: a flipped guard, a dropped update, a loosened bound. Some theorem must fail for each, so a proof cannot pass against a model it doesn't pin down. An equivalent mutant is excused only by name, with a written reason. | Every pull request |
| Allocation assertions | The steady-state admission path allocates nothing it owns; counted deterministically. | Every pull request |
| Performance gates | Hot-path benchmarks and loopback load tests against calibrated thresholds, with the host and revision recorded. See [performance](PERFORMANCE.md). | Locally, on a controlled host |
| Supply chain | RustSec advisories, permissive licenses only, a secret scan, and the declared MSRV. | Every pull request |
| Documentation | Every link and anchor resolves, every tutorial excerpt matches its compiled program, and the map below matches the contract. | Every pull request |

Timed performance runs locally rather than in CI on purpose: shared runners
can't produce comparable timings, and a gate that fails at random teaches
people to ignore it. CI still compiles every benchmark and enforces the
allocation counts.

## The invariant map

Each invariant, and the Lean modules whose theorems it cites. 27 of the 41
rest partly on a machine-checked proof. The others are enforced by types, by
their owning component, or by tested convention, as each one states. A dash
means no proof is claimed, not that the invariant is unchecked.

This table is checked: a test fails if it stops matching `INVARIANTS.md`
row by row, including which theorems each invariant cites, or if a module,
theorem or invariant count stated on this page or in the README goes stale.

<!-- invariant-map:start -->
| # | Invariant | Proofs |
|---|---|---|
| 1 | Bounded spend. | [BalanceExhaustion](../formal/lean/Tollgate/BalanceExhaustion.lean), [Conservation](../formal/lean/Tollgate/Conservation.lean), [LeaseShards](../formal/lean/Tollgate/LeaseShards.lean), [OveragePublication](../formal/lean/Tollgate/OveragePublication.lean) |
| 2 | Zero charge before execution. | [ChargeLifecycle](../formal/lean/Tollgate/ChargeLifecycle.lean) |
| 3 | Atomic commit-vs-cancel. | [CommitFallback](../formal/lean/Tollgate/CommitFallback.lean), [OveragePublication](../formal/lean/Tollgate/OveragePublication.lean) |
| 4 | Lease capabilities are exact and lease-scoped. | [LeaseFencing](../formal/lean/Tollgate/LeaseFencing.lean) |
| 5 | Fail closed, zero I/O. | [RatePublication](../formal/lean/Tollgate/RatePublication.lean) |
| 6 | Foreground isolation. | [AccountLifecycle](../formal/lean/Tollgate/AccountLifecycle.lean) |
| 7 | Idempotent partial accounting. | [IdempotentIngest](../formal/lean/Tollgate/IdempotentIngest.lean) |
| 8 | Accounting backpressure sheds. | [ChargeLifecycle](../formal/lean/Tollgate/ChargeLifecycle.lean) |
| 9 | A crashed holder can never over-spend. | [Conservation](../formal/lean/Tollgate/Conservation.lean), [LeaseTiming](../formal/lean/Tollgate/LeaseTiming.lean) |
| 10 | Ready means currently admissible. | — |
| 11 | Checked arithmetic only. | — |
| 12 | No commit outside the usability window. | [CommitFallback](../formal/lean/Tollgate/CommitFallback.lean) |
| 13 | A committed charge is always emitted. | [ChargeLifecycle](../formal/lean/Tollgate/ChargeLifecycle.lean) |
| 14 | Account creation is never destructive. | — |
| 15 | Authorization generations never move backward. | [SnapshotCache](../formal/lean/Tollgate/SnapshotCache.lean), [SnapshotHistory](../formal/lean/Tollgate/SnapshotHistory.lean) |
| 16 | Unsafe configuration never becomes authoritative. | [SnapshotLimits](../formal/lean/Tollgate/SnapshotLimits.lean) |
| 17 | Negative caching is bounded and self-healing. | [NegativeCache](../formal/lean/Tollgate/NegativeCache.lean) |
| 18 | Background store calls are wall-clock bounded, and so is every pass over them. | — |
| 19 | A control-plane failure is never silent. | — |
| 20 | Every admission outcome is counted, exactly once, under its own reason. | — |
| 21 | Every opaque identifier has one portable wire spelling. | — |
| 22 | An account's status has one writer and one propagation path. | [StatusPropagation](../formal/lean/Tollgate/StatusPropagation.lean) |
| 23 | A cached credential proves identity, never authorization, and never outlives its own validity. | [SessionCredential](../formal/lean/Tollgate/SessionCredential.lean) |
| 24 | Steady-state embedding admission allocates nothing it owns and performs exactly one snapshot lookup. | — |
| 25 | Concurrency ceilings are exact, per instance, and released once. | [ConcurrencyGauge](../formal/lean/Tollgate/ConcurrencyGauge.lean) |
| 26 | A staged request retains its principal generation and reads one current account authority at admission. | [Conservation](../formal/lean/Tollgate/Conservation.lean) |
| 27 | A credential is durable before it is disclosed, and its digest table is a projection. | [CredentialProjection](../formal/lean/Tollgate/CredentialProjection.lean) |
| 28 | A budget period is crossed exactly once, and only its allowance expires. | [Conservation](../formal/lean/Tollgate/Conservation.lean), [PeriodRoller](../formal/lean/Tollgate/PeriodRoller.lean) |
| 29 | The application's policy identity is carried, never interpreted. | — |
| 30 | Execution capacity is conserved, and the reserve is reachable. | [ExecutionCapacity](../formal/lean/Tollgate/ExecutionCapacity.lean) |
| 31 | An account has at most one owned refill manager, including retirement. | [AccountLifecycle](../formal/lean/Tollgate/AccountLifecycle.lean) |
| 32 | Control-plane authority is verified before decoding or mutation. | [ControlPlane](../formal/lean/Tollgate/ControlPlane.lean) |
| 33 | An administrative audit receipt describes its own serialized mutation. | [ControlPlane](../formal/lean/Tollgate/ControlPlane.lean) |
| 34 | A credential projection cannot renew stale identity evidence. | [CredentialProjection](../formal/lean/Tollgate/CredentialProjection.lean) |
| 35 | Credential activity derives from canonical accepted commitments. | [CredentialActivity](../formal/lean/Tollgate/CredentialActivity.lean) |
| 36 | Calibration counts comparable benchmark runs, and replacement preserves the previous contract on refusal. | — |
| 37 | Backend error text is private across the HTTP and diagnostic boundaries. | — |
| 38 | CLI information requests precede application startup and validation. | — |
| 39 | Load execution failures remain reportable under the deployment panic policy. | — |
| 40 | Instance-local sharding separates request-serving threads only while affinities outnumber neither the shards nor their holders. | — |
| 41 | A provisioner can neither fund, close, grant `Assured`, exceed its budget ceiling, extend credit, reach an operator's account, nor undo an operator's status or exhaust snapshot generations through caller-selected jumps. | [ControlPlane](../formal/lean/Tollgate/ControlPlane.lean) |
<!-- invariant-map:end -->

## What the proofs cover, and what they don't

A proof is only as strong as what it states. So the models are themselves
mutation-tested: `check_lean_mutants` changes one operator in one transition
at a time, checks the mutated model with Lean, and requires a theorem to
fail. Only definition bodies are mutated; signatures are types, and
specifications (definitions of type `Prop`) are what the theorems claim, so
weakening one would prove nothing. This pushes the proofs to pin down exact
behaviour: not only that a limit refuses what exceeds it, but that it accepts
exactly what fits. Every mutant that typechecks fails a theorem, apart from one that is
equivalent by construction and is listed in
[`formal/lean/mutants-allowed.txt`](../formal/lean/mutants-allowed.txt)
with its reason.

The Lean models use exact integer arithmetic and atomic transitions. A green
proof says that a modeled transition preserves its property. It does not say
that the Rust or SQL implements that transition faithfully; the Rust
property, backend-parity and mutation tests make that argument, and each
invariant says which ones.

The models deliberately leave out scheduling, data-structure internals,
finite-width overflow, SQL and the network. Those are separate Rust
obligations, and the [formal models README](../formal/lean/README.md) lists them module
by module.

## What is not claimed

- **No end-to-end refinement proof.** There is no machine-checked link from
  the Lean models to the Rust code. The connection is tests, and tests support
  a proof-to-code argument; they are not proof.
- **Performance numbers are host-specific.** They are microbenchmarks on a
  controlled host, not HTTP latency guarantees on yours. Ratios travel better
  than absolutes; [performance](PERFORMANCE.md) records the conditions.
- **Losing the process loses unflushed usage.** A committed charge is emitted
  on every exit path Rust controls, including a panic that unwinds. A killed
  process, or a `panic = "abort"` build that aborts, loses what it had not
  flushed. Its leases are then forfeited as settlement loss, so the account is
  never credited for work that may have run. Invariants 9 and 13 state this
  boundary.
- **Trusted inputs stay trusted.** The proofs and tests assume what their
  invariants name: an atomic store transaction, accurate clocks, verified
  credentials, and correct cryptographic primitives.

## Checking it yourself

```sh
cargo test --workspace --all-features         # tests, witnesses, parity, docs checks
./scripts/check_formal.sh                     # the Lean proofs
./scripts/check_formal_mutants.sh             # mutation testing of the Lean models
./scripts/check_mutations.sh --diff main      # mutation testing on a branch
./scripts/check_perf_thresholds.sh            # timed gates, on your own host
```

[Contributing](../CONTRIBUTING.md) lists every gate.
