# tollgate-admission

The per-request admission pipeline of
[Tollgate](https://github.com/MorphIQ-Labs/tollgate), with zero inline I/O:
snapshot lookup, status, staleness and permission checks, a direct-indexed cost
quote, weighted local rate limiting, concurrency limits, and a lease debit that
opens a pending charge.

Nothing here performs I/O, takes a blocking lock on the request path, or reads
a wall or business clock for a policy decision: `now` is an argument, and a
miss denies (resolving it is the background plane's job). Two dependencies
keep their own bookkeeping and the budget counts it: `governor` reads its own
monotonic clock for bucket arithmetic, and the Moka snapshot map runs amortised
housekeeping. Neither is a source of snapshot or lease truth, and neither can
block a request.

## A request, end to end

`begin` looks the principal up once and pins the snapshot generation;
`admit` quotes and debits; the request commits at execution start; and when the
committed guard drops, on any exit path including panic unwind, it records the
billing event into the usage slot reserved at admission.

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

struct Price;

impl OpIndex for Price {
    fn index(&self) -> usize {
        0
    }
}

// Where a committed charge is recorded. In a service this is a permit from
// tollgate-client's usage writer, reserved before the work is accepted.
struct Billing(Arc<Mutex<Vec<UsageEvent>>>);

impl UsageSlot for Billing {
    fn record(self, event: UsageEvent) {
        self.0.lock().unwrap().push(event);
    }
}

let now = Timestamp::from_second(1_755_600_000).unwrap();
let expires = Timestamp::from_second(1_755_600_060).unwrap();

// Published by the control plane: the account's compiled snapshot, and a lease
// its instance spends locally.
let table = Arc::new(
    CostTable::builder(CostUnits(1), CostUnits(1))
        .weight(&Price, CostUnits(2))
        .build(),
);
let snapshot = AccountSnapshot::builder(
    AccountId(1),
    Generation(1),
    AccountStatus::Active,
    expires,
    PermissionBits::bit(0),
    ResolvedLimits::new(64),
    table,
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

// The request path.
let billed = Arc::new(Mutex::new(Vec::new()));
let ready = engine
    .begin(Principal(42), PermissionBits::bit(0), now)
    .expect("known, active, permitted principal")
    .admit(&[(Price, 3)], Billing(Arc::clone(&billed)), now)
    .expect("within limits and funded")
    .acquire_capacity(&NoGate)
    .expect("no capacity gate configured");
let committed = ready
    .commit(RequestId(1), now)
    .map_err(|(error, _released)| error)
    .expect("inside the lease window");

// ... execute the work; success, failure and timeout are all charged ...
drop(committed);

let billed = billed.lock().unwrap();
assert_eq!(billed.len(), 1);
assert_eq!(billed[0].units, CostUnits(7)); // 1 fixed + 2 per item × 3
```

Cancelling at any stage before `commit` releases the debit for zero charge.

## Snapshot maps

`ArcSwapSnapshotMap` and `MokaSnapshotMap` sit behind one `SnapshotMap` trait,
because the cache choice is measured by the performance gate, not assumed. See
[`docs/PERFORMANCE.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/PERFORMANCE.md).

## Embedding

[`docs/EMBEDDING.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/EMBEDDING.md)
gives the supported request order, what an embedder implements, and how to
shut down without losing usage; `examples/pricing-api` in the repository is a
complete service. Contract:
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md).

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
