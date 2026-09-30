# Getting started

<!-- excerpts-of: crates/tollgate-client/tests/getting_started.rs -->

This tutorial builds a metered service's admission layer end to end, in one
process: it provisions an account, starts the runtime that keeps an instance
supplied, serves a request, watches two refusals, and shuts down with the
ledger balanced.

It runs the control plane in-process with `MemoryStore`, the reference
backend, so there is nothing to deploy. In production the same runtime talks to
PostgreSQL directly or to a `tollgate-server` over TLS; nothing on the request
path changes.

The whole program is
[`crates/tollgate-client/tests/getting_started.rs`](../crates/tollgate-client/tests/getting_started.rs).
Every excerpt below is copied from it, and a repository check fails if they
drift apart. To run it:

```sh
cargo test -p tollgate-client --test getting_started
```

[Concepts](CONCEPTS.md) defines the terms used here. For route-level HTTP
integration, use the [Axum guide](AXUM.md); this tutorial remains the
transport-neutral lifecycle.

## Dependencies

```toml
[dependencies]
tollgate-core = "0.30"
tollgate-admission = "0.30"
tollgate-client = "0.30"
tollgate-store = "0.30"
jiff = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Tollgate takes timestamps as arguments and never reads a clock on the request
path, so `jiff` appears in its API. The runtime's background tasks run on
Tokio.

## 1. Describe what you meter

Operations are dense indices into a cost table, and permissions are bits. The
names are yours; Tollgate only sees the numbers.

```rust
/// The operations the service meters, as dense indices into a cost table.
#[derive(Clone, Copy)]
enum Op {
    Quote,
    Report,
}

impl OpIndex for Op {
    fn index(&self) -> usize {
        *self as usize
    }
}

/// What a caller needs to be allowed to call the service at all.
const CALL: PermissionBits = PermissionBits::bit(0);
/// A permission this caller's plan does not include.
const EXPORT: PermissionBits = PermissionBits::bit(1);
```

## 2. Provision an account

This is control-plane work, done once per account and once per policy change,
never per request. The account gets a deposit of 10,000 units. The plan is
compiled into an immutable snapshot, with its cost table, permissions and
limits, and published under the caller's principal: the stable identity a
verified credential resolves to.

```rust
let store = MemoryStore::new(GrantPolicy::default()).expect("default grant policy is valid");
let clock: Arc<dyn Clock> = Arc::new(SystemClock);

let account = AccountId(1);
store.create_account(AccountConfig {
    account_id: account,
    initial_balance: CostUnits(10_000),
    status: AccountStatus::Active,
    capacity_class: CapacityClass::Assured,
});

// The plan, compiled once per policy change and never per request:
// 5 units per request, plus 1 per quote and 20 per report.
let costs = CostTable::builder(CostUnits(5), CostUnits(5))
    .weight(&Op::Quote, CostUnits(1))
    .weight(&Op::Report, CostUnits(20))
    .build();
let valid_until = clock
    .now()
    .checked_add(SignedDuration::from_hours(1))
    .expect("an hour from now is a valid timestamp");
let snapshot = AccountSnapshot::builder(
    account,
    Generation(1),
    AccountStatus::Active,
    valid_until,
    CALL,
    ResolvedLimits::new(64),
    Arc::new(costs),
)
.build();

// The caller's credential resolves to this principal.
let caller = Principal(42);
store
    .publish_snapshot(
        caller,
        PublishableSnapshot::try_new(Arc::new(snapshot)).expect("a consistent snapshot"),
    )
    .expect("the account exists");
```

## 3. Start the runtime

`InstanceRuntime` runs everything slow in the background: it loads and
refreshes snapshots, keeps a lease of units in hand for each account it
serves, and batches usage events to the store. Every field of its
configuration is a decision about your deployment, so it has no defaults; the
values here are reasonable for a small service.

```rust
let config = InstanceRuntimeConfig {
    snapshots: SnapshotManagerConfig {
        // Serve every principal the store knows about.
        principals: TrackedPrincipals::All { seed: vec![caller] },
        refresh_interval: Duration::from_secs(30),
        unknown_ttl: SignedDuration::from_secs(60),
        revoked_ttl: SignedDuration::from_hours(1),
        retry_backoff: Duration::from_millis(200),
        max_concurrent_fetches: 16,
        fetch_timeout: Duration::from_secs(5),
        enumeration_timeout: Duration::from_secs(30),
    },
    leases: AccountLeaseConfig {
        // Draw 1,000 units at a time; refill below 100.
        target_grant: CostUnits(1_000),
        low_water: CostUnits(100),
        lease_ttl: SignedDuration::from_secs(60),
        expiry_safety_margin: SignedDuration::from_secs(2),
        poll_interval: Duration::from_millis(20),
        store_call_timeout: Duration::from_secs(5),
        shutdown_release_deadline: Duration::from_secs(5),
    },
    usage: UsageWriterConfig {
        queue_capacity: 4_096,
        max_batch: 256,
        flush_interval: Duration::from_millis(25),
        retry_backoff: Duration::from_millis(50),
        shutdown_drain_deadline: Duration::from_secs(5),
        ingest_timeout: Duration::from_secs(5),
    },
    sharding: LocalSharding::SINGLE,
    snapshot_history_capacity:
        tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
    idle_account_linger: Duration::from_secs(1),
    manager_restart_backoff: Duration::from_millis(200),
    shutdown_deadline: Duration::from_secs(15),
};

// One store plays all three control-plane roles here: the snapshot
// source, the lease allocator, and the usage sink.
let (runtime, handle) = InstanceRuntime::spawn(
    store.clone(),
    store.clone(),
    store.clone(),
    Arc::clone(&clock),
    config,
)
.expect("a valid runtime configuration");

// Ready means: snapshots loaded, a lease in hand, and usage accounting up.
while !handle.readiness(clock.now()).is_ready() {
    tokio::time::sleep(Duration::from_millis(10)).await;
}
```

The runtime is ready once it has a snapshot, a usable lease and a healthy
usage writer. Serve readiness from `handle.readiness(now)`, so a load balancer
sends no traffic to an instance that would refuse it.

## 4. Serve a request

This is the request path. It performs no I/O: every call below reads local
state that the runtime keeps current.

```rust
let now = clock.now();
// 1. Authenticate (not shown), then `begin`: one snapshot lookup and the
//    route's permission check. Everything after reads this generation.
let context = handle
    .begin(caller, CALL, now)
    .expect("a known, active caller with the permission");
// 2. Reserve room for the billing event before reading the request body,
//    so a full usage queue sheds load before any work is done.
let slot = handle
    .recorder()
    .try_reserve()
    .expect("the usage queue has room");
// 3. Quote the workload and debit the local lease: three quotes and one
//    report cost 5 + 3 × 1 + 1 × 20 = 28 units.
let pending = context
    .admit(&[(Op::Quote, 3), (Op::Report, 1)], slot, now)
    .expect("within limits and funded");
// 4. Commit when execution actually starts.
let committed = pending
    .acquire_capacity(&NoGate)
    .expect("no capacity gate configured")
    .commit(RequestId(1), clock.now())
    .map_err(|(error, _released)| error)
    .expect("the lease is still usable");

// ... do the work ...

// 5. Dropping the guard records the charge, on every exit path.
drop(committed);
```

The order is the contract; [Embedding Tollgate](EMBEDDING.md) says what each
position buys. Cancelling at any point before `commit` releases the debit and
charges nothing. Once committed, the request is charged whether the work
succeeds, fails or times out, because the resources were spent either way.

## 5. Refusals charge nothing

Tollgate fails closed: an unknown caller, a missing permission, a stale
snapshot, an exhausted lease or a full usage queue all deny, with zero units
charged and a `DenyReason` that says which.

```rust
// A permission the plan does not include: denied, and nothing is charged.
let refused = handle.begin(caller, EXPORT, clock.now());
assert!(matches!(refused, Err(DenyReason::MissingPermission)));

// A caller the control plane never published.
let stranger = handle.begin(Principal(7), CALL, clock.now());
assert!(matches!(stranger, Err(DenyReason::UnknownPrincipal)));
```

Each `DenyReason` carries retry advice, and the example service maps them to
HTTP statuses in [`examples/pricing-api`](../examples/pricing-api).

## 6. Shut down and check the ledger

Shutdown order matters: usage must land while its lease is still live. Stop
your listener first, then let the runtime drain in-flight work, flush usage
and release leases under one deadline.

```rust
// Stop accepting requests first (your listener), then drain: the runtime
// flushes usage while leases are still live, then releases them.
let report = runtime.shutdown().await.expect("the runtime task finished");
assert!(!report.deadline_expired);
assert_eq!(report.usage.expect("usage drained").accepted, 1);

// The ledger: one request billed 28 units, the released lease returned
// the rest, and every unit deposited is accounted for.
assert_eq!(store.usage_recorded(account), CostUnits(28));
assert_eq!(store.balance(account), CostUnits(9_972));
let ledger = store.conservation(account).expect("the account exists");
assert_eq!(ledger.deposited, CostUnits(10_000));
assert_eq!(ledger.settled_usage, CostUnits(28));
assert_eq!(ledger.active_lease_grants, CostUnits::ZERO);
```

The released lease returned its unspent units, so the balance is the deposit
less exactly what was billed. Every backend's test suite asserts this
conservation equation, per account, at every step.

## Where next

- [Concepts](CONCEPTS.md): the model behind each step.
- [Embedding Tollgate](EMBEDDING.md): the full request contract, what you
  implement, adopting in stages, and shutdown.
- [Account administration](ACCOUNT_ADMINISTRATION.md): provisioning accounts,
  budgets and credentials over HTTP.
- [Credential projection](CREDENTIAL_PROJECTION.md): authenticating customer
  keys in front of `begin`.
- [`examples/pricing-api`](../examples/pricing-api): a complete HTTP service
  with authentication, admission, metrics and billing.
