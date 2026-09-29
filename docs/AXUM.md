# Axum integration

<!-- excerpts-of: crates/tollgate-axum/tests/getting_started.rs -->

Use `tollgate-axum` to declare metered routes around ordinary asynchronous
business functions. Initialize the runtime once, validate input before
execution, and let the adapter own admission and the charge guard.

The complete [executable guide](../crates/tollgate-axum/tests/getting_started.rs)
provisions one account, serves fixed and body-derived quantities, leaves health
unmetered, and checks the final ledger. Its excerpts below are checked against
the source in CI. From this workspace:

```sh
cargo test -p tollgate-axum --test getting_started
cargo run -p pricing-api --bin pricing-api
```

The adapter is new on the integration branch and is not part of the existing
0.32.1 registry release. Use this workspace until the release publishes it;
then add `tollgate-axum` at the same version as the other Tollgate libraries.
The adapter targets Axum 0.8. An attribute macro is not provided.

## Set up the account and runtime once

The guide's two dense operations are `Quote` (one unit each) and `Report`
(20 units), plus a five-unit request floor/overhead. `CALL` is the route
permission. This setup runs at startup or in the control plane, never per
HTTP request. The fixed credentials below are demonstration values.

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
// Demonstration credentials only; production uses KeyManager's verifier.
let verifier = Arc::new(HmacRegistry::new(b"axum-guide-demo-secret"));
verifier.install_credentials([b"axum-guide-demo-key".as_slice()]);
let caller = verifier.verify(b"axum-guide-demo-key").unwrap().principal;
store
    .publish_snapshot(
        caller,
        PublishableSnapshot::try_new(Arc::new(snapshot)).expect("a consistent snapshot"),
    )
    .expect("the account exists");

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

The application owns `runtime` and calls its bounded `shutdown` after stopping
new HTTP admission. Share `handle` with every route and use its `readiness`
for a readiness endpoint. Cloning `Tollgate` starts no workers.

## Declare metered routes

`Validated<T>` carries validated business input and its quantity. It accepts
neither a caller-supplied price nor a capacity class. The pinned account policy
prices the request. The execution callback receives the committed request ID,
units and policy revision; response metadata uses those values without another
lookup or quote.

```rust
let capacity = ExecutionCapacityGate::new(
    ExecutionCapacityMode::Reserved {
        total: std::num::NonZeroU32::new(8).unwrap(),
        assured_reserve: std::num::NonZeroU32::new(2).unwrap(),
    },
    LocalSharding::SINGLE,
)
.unwrap()
.unwrap();
let tollgate = Tollgate::new(AdapterConfig {
    runtime: handle,
    authenticator: BearerAuth::new(verifier),
    clock,
    request_ids: || Ok(RequestId(uuid::Uuid::new_v4().as_u128())),
    capacity,
});

let app = Router::new()
    .route("/health", axum::routing::get(|| async { "ok" }))
    .route(
        "/report",
        tollgate.post(
            Op::Report,
            CALL,
            || Ok(Validated::new((), 1)),
            |(), charge| async move {
                BufferedResponse::json(
                    StatusCode::OK,
                    &serde_json::json!({
                        "report": "complete", "units_charged": charge.units_charged.get(),
                    }),
                )
            },
        ),
    )
    .route(
        "/quote",
        tollgate.post_json(
            Op::Quote,
            CALL,
            InputLimits::new(4096, Duration::from_secs(5)).unwrap(),
            |input: QuoteInput| {
                if input.items.iter().any(String::is_empty) {
                    return Err(InputError("item names must be nonempty"));
                }
                let quantity = u64::try_from(input.items.len())
                    .map_err(|_| InputError("too many items"))?;
                Ok(Validated::new(input, quantity))
            },
            |input, charge| async move {
                BufferedResponse::json(
                    StatusCode::OK,
                    &serde_json::json!({
                        "items": input.items,
                        "request_id": charge.request_id.to_string(),
                        "units_charged": charge.units_charged.get(),
                        "policy_revision": charge.policy_revision.to_string(),
                    }),
                )
            },
        ),
    );
```

The report costs 25 units and the three-item quote costs eight. Invalid names
are refused before a charge. An empty workload is refused by core admission.
`NoGate` can replace the configured capacity gate without adding a disabled
pool. Account policy selects Assured or BestEffort; request headers cannot
claim reserve access.

For a listener, install one connection cache per accepted connection with
`app.into_make_service_with_connect_info::<TollgateConnection>()`. `BearerAuth`
requires that state and fails closed when it is missing. Never install one
shared global connection cache. In production, use the verifier from
[KeyManager](CREDENTIAL_PROJECTION.md) so credential expiry and revocation
track the control plane; the guide's static HMAC registry is a demonstration.
Its UUID v4 request IDs use a userspace CSPRNG (`uuid`'s `fast-rng` feature),
not a process counter. A custom `RequestIdSource` must be local, nonblocking
and unique across instances and restarts; return an error when unavailable.

## Billing and middleware

The adapter authenticates and checks permission, reserves accounting capacity,
then reads bounded input and validates it. It calls the existing admission
and capacity APIs and commits immediately before constructing the execution
future. Unknown/stale identities, invalid input, unavailable accounting or
funding, and capacity refusal all prevent business execution and charge zero.
A snapshot stays pinned during the body read; a later policy/revocation update
governs the next request. Snapshot expiry and commit-time funding validity are
still checked by the existing core transitions.

After commit, handler errors, timeouts and task cancellation are billable.
Panic unwinding drops the guard and records once; process abort/crash is not
unwinding and retains Tollgate's existing crash/accounting guarantees.
Serialize output through `BufferedResponse` before returning it. This keeps
serialization inside the charge lifetime. Sending completed response bytes
across the network is outside business execution.

Install canceling timeout middleware outside the route wrapper. Cancellation
before start is uncharged; after start it records the committed amount. Outer
body limits remain effective: the smaller limit wins. Route byte limits and
body-read deadlines are explicit. Unmetered routes, unknown paths and wrong
HTTP methods stay outside admission. Do not place a retry layer around the
wrapper: a retry is a new request/charge, not an idempotent replay of business
work. Queueing/backpressure outside it may delay admission without charging.

`post_json_with_error_handler` receives a typed `Rejection` plus optional
committed metadata. Before execution the metadata is absent; serialization
failure carries the original charge. The default problem response distinguishes
capacity exhaustion (`503 capacity-unavailable`) from rate limits (`429`) and
keeps backend messages and input data out of error bodies. The pricing example
uses the customization seam to preserve its existing wire errors and metrics.
Custom renderers must remain local and must not retry work.

## Backends and distributed services

The adapter receives a `RuntimeHandle`, not a database connection. The same
runtime can use `MemoryStore`, a direct `PostgresStore`, or `HttpStore` connected
to a secured `tollgate-server`. Pass the selected backend to the runtime's
allocator, snapshot-source and usage-sink arguments at startup. For PostgreSQL
setup and schema migration, follow the
[embedding guide](EMBEDDING.md); for remote service identities, bearer/TLS
configuration and credential rotation, follow
[control-plane security](CONTROL_PLANE_SECURITY.md).

Instances share an account budget through centrally allocated, fenced leases;
there is no per-request network hop. Fast-lane execution capacity is local to
each instance. Elastic overage caps are also per instance, not a globally
shared allowance. Configure these separately when scaling out.

## Migration and lower-level integrations

The [pricing example](../examples/pricing-api/src/lib.rs) keeps its operation
vocabulary, pricing kernel, response schema, readiness and shutdown. It replaces
`PriceInput`, `Staged`, manual acquisition/commit, and local connection glue
with this adapter; `PricingConnection` remains an alias for listener compatibility.
The admission-disabled load baseline still contains only transport and kernel.

The convenience API supports one fixed operation per route, a bodyless POST
or bounded JSON POST, synchronous validation, and an owned buffered response.
It deliberately does not accept arbitrary extractor combinations or streaming
bodies, upgrades, detached jobs, and blocking tasks that survive cancellation.
For those cases use `prepare_json().into_parts()` or the
[transport-neutral admission API](EMBEDDING.md), and move the owned committed
guard into the work whose lifetime actually defines billing. Do not return a
stream while dropping its guard at response-header creation.
