# tollgate-core

The zero-I/O domain layer of [Tollgate](https://github.com/MorphIQ-Labs/tollgate):
cost tables, compiled account snapshots, fenced local leases, and the
reservation state machine that decides what a request is charged.

Every operation is a function of its arguments. There is no I/O, no clock read
(callers pass `now`), no blocking lock, and cost arithmetic is checked, never
wrapping. Unknown, expired, exhausted, or overflowing states deny; there is no
slower path to fall back to. That is what lets the layer sit inside a request
whose whole budget is a few microseconds.

## Where it sits

Tollgate has two planes. The request path is this crate and
[`tollgate-admission`](https://crates.io/crates/tollgate-admission); the
control plane is [`tollgate-client`](https://crates.io/crates/tollgate-client),
[`tollgate-server`](https://crates.io/crates/tollgate-server), and a
[`tollgate-store`](https://crates.io/crates/tollgate-store) backend. Most
services embed Tollgate through `tollgate-admission` and `tollgate-client`;
this crate is the vocabulary they share.

## Charging a request

A snapshot admits the account, a cost table quotes the work, a lease reserves
the units, and the reservation either commits at execution start, charging the
full quote whatever the outcome, or is cancelled before execution for zero.

```rust
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CancelOutcome, CommitFunding, CostTable,
    CostUnits, FencingToken, Generation, LeaseGrant, LeaseId, LocalLease, OpIndex,
    PermissionBits, Reservation, ResolvedLimits,
};

// The embedder's operations, as dense indexes into the cost table.
enum Op {
    Price,
    Greeks,
}

impl OpIndex for Op {
    fn index(&self) -> usize {
        match self {
            Op::Price => 0,
            Op::Greeks => 1,
        }
    }
}

let now = Timestamp::from_second(1_755_600_000).unwrap();
let expires = Timestamp::from_second(1_755_600_060).unwrap();

// One unit fixed per request, plus a per-item weight per operation.
let table = Arc::new(
    CostTable::builder(CostUnits(1), CostUnits(1))
        .weight(&Op::Price, CostUnits(1))
        .weight(&Op::Greeks, CostUnits(5))
        .build(),
);
let snapshot = AccountSnapshot::builder(
    AccountId(1),
    Generation(1),
    AccountStatus::Active,
    expires,
    PermissionBits::bit(0),
    ResolvedLimits::new(1_024),
    Arc::clone(&table),
)
.build();

// A lease the control plane granted: 100 units, spent locally.
let lease = Arc::new(LocalLease::new(
    LeaseGrant {
        lease_id: LeaseId(1),
        account_id: AccountId(1),
        fencing_token: FencingToken(1),
        units: CostUnits(100),
        expires_at: expires,
    },
    CostUnits(10),
));

snapshot.admit(now, PermissionBits::bit(0)).expect("active, fresh, permitted");
let quote = table.quote(&Op::Greeks, 4).expect("known operation");
assert_eq!(quote.total, CostUnits(21)); // 1 fixed + 5 per item × 4

// Admission debits the lease, but the charge is only pending.
let reservation = Reservation::reserve(&lease, quote.total, now).expect("funded");
assert_eq!(lease.remaining(), CostUnits(79));

// Execution start commits the full quote, for success, failure or timeout.
let charged = reservation
    .commit_at_execution_start(now, CommitFunding::LeaseOnly)
    .expect("inside the lease window");
assert_eq!(charged, CostUnits(21));

// A request that ends before execution is released for zero instead.
let early = Reservation::reserve(&lease, CostUnits(5), now).expect("funded");
assert!(matches!(early.cancel(), CancelOutcome::ZeroCharged));
assert_eq!(lease.remaining(), CostUnits(79));
```

Commit and cancel race on one atomic transition, so exactly one wins.

## Features

- `serde`: `Serialize`/`Deserialize` for the wire-visible types: snapshots
  and cost tables, lease grants, usage events, deny reasons, budgets,
  identifiers, and units. Off by default.

## Contract

The behaviour above is specified in
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md),
where each invariant names the tests that enforce it, and parts are modelled in
Lean under [`formal/lean`](https://github.com/MorphIQ-Labs/tollgate/tree/main/formal/lean).
Rationale and history are in
[`docs/DESIGN.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/DESIGN.md).

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
