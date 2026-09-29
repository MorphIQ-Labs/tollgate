# Concepts

Tollgate answers two questions for a metered service: *may this caller do
this, and what does it cost?* on every request, and *what did each account
use?* for billing. This page defines the terms the rest of the documentation
uses. [Getting started](GETTING_STARTED.md) shows them working together.

## Two planes

Everything Tollgate does belongs to one of two planes, and the line between
them is the design.

- **The request path** decides admission. It reads only local, immutable or
  atomic state, so it performs no I/O, takes no blocking locks, and reads no
  clock for a policy decision: the caller passes `now` in. It lives in
  `tollgate-core` and `tollgate-admission`.
- **The control plane** does everything slow in background tasks: it
  distributes account state, allocates quota, and records usage. It is
  `tollgate-client`'s runtime on each instance, plus a store backend and,
  optionally, `tollgate-server`.

The request path never waits for the control plane. When local state is
missing, stale or exhausted, the request is refused, and the control plane
catches up in the background.

## Accounts, principals and permissions

An **account** is the unit that holds a balance and is billed. A
**principal** is a stable identity that a verified credential resolves to;
an account can have several (one per API key, say), and they share its
funding.

**Permissions** are bits. A route declares the bits it requires, a snapshot
carries the bits a principal holds, and `begin` checks one against the other.
What each bit means is your product's decision.

## Snapshots

An **account snapshot** is the compiled, immutable form of everything
admission needs to know about a principal: the account's status, its
permissions, its rate, concurrency and shape limits, its cost table, its
enforcement mode, and how long the snapshot is valid. You compile it once per
policy change, never per request, and the control plane distributes it to
every instance.

Snapshots carry a **generation**. An instance never moves a principal back to
an older generation, and a revocation is a durable, generation-ordered
tombstone, so a delayed message cannot resurrect a revoked credential. A
snapshot past its validity window is refused (`SnapshotExpired`) until a
replacement arrives, so a partitioned instance stops admitting rather than
serving stale policy; staleness never triggers an inline fetch. See
[snapshot operations](SNAPSHOT_OPERATIONS.md).

## Cost units and cost tables

A **cost unit** is Tollgate's only currency. Your product's credits,
requests or compute units map onto it at compile time. Arithmetic on cost
units is always checked; an overflow is a refusal, never a wrap.

A **cost table** prices a request's **workload**, a list of `(operation,
count)` pairs: a fixed charge per request plus a weight per operation. You
index operations densely, so a quote is O(1) in the size of the table and
O(distinct operations) in the request.

## Leases

A **lease** is a block of units that the control plane allocates to one
instance from an account's central balance. The instance spends it locally
with atomic counters, which is why admission needs no round trip. A lease is
the answer to "how can many instances enforce one balance without
coordinating per request?": units leave the balance when the lease is
granted, so the instances together can never spend more than was allocated.

- A lease carries a **fencing token**. The store accepts usage and releases
  only when the token matches the lease record, so a stale holder cannot
  settle against a lease it no longer owns.
- A lease expires. It is usable only until `expires_at` minus a safety margin,
  and the store reclaims it only after `expires_at` plus a grace period, so an
  instance never spends a lease the store has already taken back. See
  [lease timing](LEASE_TIMING.md).
- The runtime refills a lease when it falls below a low-water mark, and
  releases unspent units on graceful shutdown. A lease abandoned by a crash is
  forfeited after its grace period and recorded as settlement loss: executed
  work can never become spendable again.

## A request's life

A request moves through typed stages, and each stage can only be reached
from the one before it:

1. **`begin`** looks the principal up once, pins that snapshot generation for
   the rest of the request, and checks the route's permission.
2. **Reserve a usage slot**, room in the usage queue for this request's
   billing event, before reading the request body. If the queue is full, the
   request is shed before any work is done.
3. **`admit`** quotes the workload, takes rate and concurrency tokens, and
   debits the lease. The result is a **pending** reservation.
4. **`acquire_capacity`** applies the instance's execution-capacity policy,
   if one is configured.
5. **`commit`**, at the moment execution starts, turns the reservation into a
   charge. It can fail if the lease stopped being usable in the meantime.
6. **Dropping the committed guard** records the usage event into the slot
   reserved in step 2, on every exit path, including a panic that unwinds.

Cancelling at any stage before `commit` releases the debit and charges
nothing. After `commit`, success, failure and timeout are all charged. The
exact contract is in [Embedding Tollgate](EMBEDDING.md).

## Fast lane: reserved execution capacity

Tollgate's **fast lane** protects execution capacity for priority customers
when best-effort traffic saturates an instance. Enable
`ExecutionCapacityMode::Reserved` and classify priority accounts as `Assured`.
An operator sets each account's capacity class through
[account administration](ACCOUNT_ADMINISTRATION.md).

- **BestEffort** requests can use only the shared pool.
- **Assured** requests try the shared pool first, then their protected reserve.
  Best-effort traffic cannot consume that reserve, even while it is idle.

For example, an instance with 100 capacity units and a 20-unit reserve allows
best-effort work to occupy at most 80 units. Assured work can use the shared
80 plus the protected 20, subject to available capacity and request size.

The reserve is per instance and does not preempt running work or queue requests.
An assured request is still refused if neither eligible pool has enough room,
and the usual permission, rate, concurrency and funding rules still apply.
The feature protects capacity under contention; it does not guarantee a fixed
latency or that every priority request will succeed. See the
[design record](DESIGN.md) for pool sizing and deployment details.

## Refusals

Every refusal is a `DenyReason` with retry advice, and every refusal charges
zero units. Tollgate **fails closed**: an unknown principal, a stale snapshot,
a missing permission, an exceeded limit, an exhausted lease or a full usage
queue all deny. There is no slower fallback path, because a fallback that
reached the store would put I/O back on the request path.

## Enforcement modes

A snapshot's **enforcement mode** decides the one question a lease can't
answer by itself: what happens when the lease cannot fund the quote?

- **Strict** (the default) denies. An account never spends past what was
  allocated to it.
- **Elastic** admits past the lease, up to a per-instance overage cap, and
  records every unit it admits that way as **overage**, billed like any other
  usage.

Only funding is elastic. An unknown principal, a missing permission or a
stale snapshot is refused under either mode.

## Usage events and the ledger

A **usage event** is the billing record of one committed request: its
request ID, the account, what funded it (a lease, or overage), the units
charged, and when. Events are
**idempotent** by request ID and are written to the store in batches, so a
retried batch never bills twice. Usage events, not leases, are the billing
truth: a lease bounds what an instance may spend, and the events record what
it did. See [usage accounting](USAGE_ACCOUNTING.md).

Every backend keeps a per-account ledger that must satisfy one equation
exactly, at every step:

```text
deposited + overage_recorded
    == balance + active grants + settled usage + settlement loss + expired
```

The in-memory store and the PostgreSQL store assert it in the same test
suite.

## Budgets and periods

An account can have a **budget schedule**: an allowance deposited at each
period boundary, such as a monthly allowance that resets. When a period
closes, unspent allowance is **expired**, not lost: it stays in the ledger as
its own term. A direct-store service runs a `PeriodRoller` to apply schedules;
with `tollgate-server`, the server does it. See
[account administration](ACCOUNT_ADMINISTRATION.md).

## Readiness

An instance is **ready** when it has current snapshots, usable leases for
the accounts it serves, and a healthy usage writer. Readiness is continuous,
not a startup flag: an instance that loses its snapshots, its leases or its
accounting becomes unready, so a load balancer can stop sending it traffic it
would only refuse.

## Topologies

The request path is the same in every deployment. What changes is where the
control plane's store lives:

- **In-process:** `MemoryStore`, the reference implementation. For tests,
  examples and single-process tools.
- **Direct store:** each instance's runtime talks to PostgreSQL through
  `tollgate-store-postgres`.
- **Via server:** instances talk to `tollgate-server` over authenticated TLS
  with `HttpStore`, and only the server holds database credentials. See the
  [control-plane security runbook](CONTROL_PLANE_SECURITY.md).
