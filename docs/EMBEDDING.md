# Embedding Tollgate on a request path

For a service putting Tollgate's staged admission pipeline in front of its own
work. It states the contract: the request order, what you implement, what you
cannot, and how to shut down without losing usage.

It is not the rationale. `docs/DESIGN.md` § "Staged admission interface (#96)"
says *why* the seam has this shape and is the document an interface change
amends first. It is also not the provisioning API — creating accounts, setting
budgets and issuing credentials is `docs/ACCOUNT_ADMINISTRATION.md`.

`examples/pricing-api` is the worked example. Read it alongside this; the last
section says which of its choices are the contract and which are its own.

## Depending on Tollgate

Crates are unpublished and distributed by git tag. Depend on a tag, never a
branch: the workspace releases as one unit, and its internal dependencies are
pinned to exact versions, so a mixed set will not resolve.

```toml
[dependencies]
tollgate-core = { git = "https://github.com/MorphIQ-Labs/tollgate.git", tag = "v0.23.1" }
tollgate-admission = { git = "https://github.com/MorphIQ-Labs/tollgate.git", tag = "v0.23.1" }
# Only if you want the managed runtime — see "Two ways in" below.
tollgate-client = { git = "https://github.com/MorphIQ-Labs/tollgate.git", tag = "v0.23.1" }
```

Pin the tag your conformance run was recorded against, and move it
deliberately.

## Two ways in

**`tollgate_admission::AdmissionEngine`** is the request path itself. You own
snapshot publication, lease funding and usage export.

**`tollgate_client::InstanceRuntime`** owns those for you — snapshot
distribution, lease acquisition and renewal, usage batching, readiness and
shutdown — and hands back a `RuntimeHandle` whose `begin` delegates straight to
the engine. This is what the example uses and what most services want.

Everything below is identical either way: `RuntimeHandle::begin` and
`AdmissionEngine::begin` have the same signature and return the same
`RequestContext`.

## The supported request order

This is the contract, from `docs/DESIGN.md`:

> The supported request order is authenticate, `begin`, reserve the usage slot,
> read/decode under `ctx.limits()`, then call `ctx.admit`.

Each position earns its place, and reordering them loses something specific:

1. **Authenticate** from bounded transport metadata — a header, not a body.
2. **`begin`** resolves the account snapshot once and checks the route
   permission. Everything downstream reads that one pinned generation, so a
   snapshot refresh mid-request cannot change the answer under you.
3. **Reserve the usage slot.** After `begin`, so an unknown or unauthorized
   credential never occupies the usage queue. Before the body, so backpressure
   sheds before expensive input work.
4. **Read and decode** under `ctx.limits()`. A body-read failure drops the
   context and the slot and creates no funding reservation.
5. **`admit`** with the compiled workload. This is the stage that can create
   pending funding — everything before it is free to fail.

Then `acquire_capacity`, `commit` at the moment execution actually starts, and
drop the `Committed` guard when the work is done.

## The stages

```rust
// 2. begin — one snapshot lookup plus the route permission.
pub fn begin(&self, principal: Principal, required: PermissionBits, now: Timestamp)
    -> Result<RequestContext, DenyReason>;

// 5. admit — shape, rate, concurrency and funding.
pub fn admit<O: OpIndex, S: UsageSlot>(self, workload: &[(O, u64)], slot: S, now: Timestamp)
    -> Result<Pending<S>, DenyReason>;

// 6. capacity — a startup-selected policy, not caller input.
pub fn acquire_capacity<G: CapacityGate>(self, gate: &G)
    -> Result<ReadyToStart<S, G::Permit>, (DenyReason, Released)>;

// 7. commit — at actual execution start, not at admission.
pub fn commit(self, request_id: RequestId, now: Timestamp)
    -> Result<Committed<S, P>, (CommitError, Released)>;
```

`RequestContext` is `Send + Sync + 'static` and cheap to hold, so it crosses the
body-read `await`. It carries `snapshot()`, `limits()`, `generation()` and
`policy_revision()` — read your effective limits from it rather than from
anywhere else, because it is the generation this request was authorized under.

**Commit can fail.** `CommitError` at execution start means funding lapsed
between reserving and starting. The `Result` is what forces the executor to
check before running the kernel; the `#[must_use]` on `commit` is deliberate.

**The workload is borrowed.** `&[(O, u64)]` — a stack array or your own
fixed-capacity buffer. Tollgate owns no `Vec`, box, hash table, string or
product identifier, and quoting is O(distinct classes), not O(policy records).

**Dropping `Committed` is what bills.** The usage event is emitted from `Drop`,
so the guard must live exactly as long as the work. `cancel()` before commit
releases everything and charges nothing.

## What you implement

**`OpIndex`** — dense indices for your operation classes.

```rust
impl OpIndex for Op {
    fn index(&self) -> usize { *self as usize }
}
```

**`UsageSlot`** — pre-reserved capacity for exactly one event.

```rust
pub trait UsageSlot: Send + 'static {
    fn record(self, event: UsageEvent);
}
```

`record` must not perform fallible I/O. Obtaining the slot is the backpressure
decision; consuming it is the committed-charge path, and it runs from `Drop`.
Take `tollgate_client::UsagePermit` from the runtime's recorder unless you have
a reason not to.

**Authentication** — verify before the body. `tollgate_auth::CredentialVerifier`
is the scheme seam and `HmacRegistry` the implementation in the box. Strip any
transport prefix before the library sees the bytes, so what you cache and what
you verify are the same bytes by construction.

**A panic boundary around your kernel.** Tollgate's guard is drop-safe and
allocation-free, but a panic that escapes a worker thread can leak the guard
rather than drop it, and a leaked guard emits nothing. `catch_unwind` (or your
executor's equivalent) is yours to place. Under `panic = "abort"` this does not
apply, and INVARIANTS #13 states the process-loss boundary instead.

**Shutdown ordering** — see below.

## What you cannot implement

`CapacityGate` and `CapacityPermit` are sealed. An application's own compute
permit is not a Tollgate capacity decision, and a type outside the crate
satisfying the permit would be a way to start work the gate refused. Choose a
policy at startup: `NoGate` (zero-sized, disabled) or `ExecutionCapacityGate`
with `Uniform` or `Reserved`.

Your own compute admission stays yours and stays separate. Weights you use to
schedule work are not cost units.

## Adopting in stages

You do not have to wire everything at once. Two zero-cost stand-ins exist for
exactly this, and both are honest about being stand-ins:

- **`NoGate`** — admission without execution-capacity limiting.
- **`DiscardedUsageSlot`** — admission without usage export. It *counts* what it
  discarded, deliberately: a silently dropping slot makes "usage is not wired
  up" indistinguishable from "no usage happened", and one of those means you are
  admitting billable work and losing the record. Not for production billing.

## Shutting down

One safe order, because usage events must land while their lease is still live:

1. Stop admitting — stop your listener first.
2. Quiesce request tasks still holding permits or `Committed` guards, bounded by
   the deadline the runtime returns.
3. Await the usage writer's shutdown; it refuses new reservations, drains
   outstanding permits, and reports anything unresolved.
4. Only then release leases.

`InstanceRuntime` does steps 2–4 under one deadline. Step 1 is yours, and so is
bounding your own tasks by the deadline it gives back.

## What is contract, and what is `pricing-api`'s own choice

Contract:

- the request order, and every signature above;
- reading limits from the pinned context;
- commit at execution start, and checking its `Result`;
- the `Committed` guard living as long as the work;
- the shutdown order.

`pricing-api`'s own choices, which you should not copy without deciding:

- **Doing admission inside an Axum `FromRequest` extractor.** It is a tidy place
  to enforce "before the body" in that framework, and nothing requires it.
- **`MemoryStore` in-process.** Pointing the same stack at a `tollgate-server`
  is a swap to `HttpStore`.
- **`NoGate` by default**, with the capacity gate behind configuration.
- **A per-connection credential cache.** Sound for long-lived connections;
  irrelevant if yours are short.
- **Its error mapping.** RFC-7807 shapes and which `DenyReason` becomes which
  status are product decisions.

## Related

- `docs/DESIGN.md` § "Staged admission interface (#96)" — the rationale, and the
  document an interface change amends first.
- `docs/ACCOUNT_ADMINISTRATION.md` — provisioning accounts, budgets and
  credentials over HTTP, with a conformance list for that surface.
- `docs/CREDENTIAL_PROJECTION.md` — authenticating customer keys in an
  HTTP-backed deployment.
- `docs/USAGE_ACCOUNTING.md` — what happens to the events you emit.
- `docs/DESIGN.md` § "Instance-local admission sharding (#3)" — the opt-in
  layout, once same-account contention warrants it. It is off by default and
  costs nothing until you enable it.
- `INVARIANTS.md` — the testable contract. Three of them are reachable from
  outside, which is why they appear above: #8 *accounting backpressure sheds*
  (why the slot is reserved before the body), #12 *no commit outside the
  usability window* (why leases are released last), and #13 *a committed charge
  is always emitted* (whose process-loss boundary is what your panic boundary
  keeps you inside).

## Funding refusal and retry advice

`DenyReason::BalanceExhausted` means the allocator has confirmed that no account
funding remains, including units held in leases. Its retry class is `Never`
under the current funding: callers need a top-up, a changed funding policy, or
the next budget period. The pricing example returns HTTP 402 with
`balance-exhausted` and zero charge. Admission still tries available local lease
and elastic capacity first.

Ordinary lease refusals remain transient/unknown. They do not prove that the
account is spent, even when the allocator returned `InsufficientBalance`:
funds can be held elsewhere and return through settlement or expiry. Usage is
batched, so confirmed exhaustion can lag actual consumption until billing has
recorded it and the background manager retries. Do not turn an estimated
remaining balance or a snapshot's old budget view into authoritative exhaustion.

Evidence is shared by all principals and localities of an account. A successful
new grant clears it; accepted snapshots clear it when the budget view or
enforcement mode changes. Snapshot maps order generations separately for each
principal: an accepted change clears account evidence even if its generation is
below another principal's. Rejected replays and unchanged funding do not clear it.
A stored period end expires the evidence using caller-supplied admission time,
even if the allocator response arrives after rollover. Unscheduled balances
retain evidence until a funding observation invalidates it. Top-ups propagate
through the normal grant/snapshot loops, not synchronously. Refundable pending
elastic reservations and an in-flight overage commit retain their transient
and `AfterInFlight` advice, because they can recover without new funding.

The allocator now returns `AllocateError::BalanceExhausted(BalanceExhaustion)`;
`InsufficientBalance` keeps its existing, weaker meaning. Exhaustive Rust
matches need the new variants. Dense counters append `balance_exhausted` without
renumbering existing reasons. `Problem` and `ApiError` gain an optional
`balance_exhaustion` field; Rust struct literals need updating. HTTP adds the
`balance-exhausted` code with an evidence object whose required `period_end`
is a timestamp or explicit null. Missing/malformed evidence is an operational
error, never a permanent denial.

Deploy updated HTTP clients before servers. New clients retain transient
behavior with old servers; old clients treat the new code as an unknown storage
error and cannot offer the improved advice. No database migration is needed.
Custom allocators can continue returning `InsufficientBalance` until they can
establish the stronger ledger fact; do not manufacture evidence from their
allocatable balance alone. Local evidence publication uses a mutex only in the
control plane. Admission adds one atomic load on stable funding refusals and
no allocation, lock, I/O, or clock read.
