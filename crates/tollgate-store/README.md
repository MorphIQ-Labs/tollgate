# tollgate-store

The storage contract of [Tollgate](https://github.com/MorphIQ-Labs/tollgate),
and `MemoryStore`, the reference implementation of it.

Narrow traits separate the data plane from lifecycle authority:

- `LeaseAllocator` atomically debits an account's balance into fenced,
  TTL-bounded leases, and settles them by release or expiry reclaim.
- `SnapshotSource` fetches compiled account snapshots and subscribes to pushes.
- `UsageSink` ingests usage events in idempotent, fencing-checked batches.
- `KeySource` serves validated, revisioned pages of active credential digests.
- `AdminStore` and `KeyDirectory` hold administrative and credential-lifecycle
  authority, which instances do not need.

Every method takes `now` as an argument: a store never reads a clock, so
backends are deterministic under test and the clock decision lives in one
place.

## The reference implementation

`MemoryStore` proves the traits are not shaped like any particular database,
runs the correctness suite without infrastructure, and documents the
settlement rules a real backend must reproduce.
[`tollgate-store-postgres`](https://crates.io/crates/tollgate-store-postgres)
reproduces them exactly, and a mirrored test suite holds the two to one
contract.

```rust
use jiff::{SignedDuration, Timestamp};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits};
use tollgate_store::{AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, MemoryStore};

# #[tokio::main(flavor = "current_thread")]
# async fn main() {
let store = MemoryStore::new(GrantPolicy::default()).expect("valid policy");
AdminStore::create_account(
    &*store,
    AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    },
)
.await
.expect("new account");

// A fenced lease for an instance to spend locally for thirty seconds.
let now = Timestamp::from_second(1_755_600_000).unwrap();
let allocation = store
    .acquire(AccountId(1), CostUnits(100), SignedDuration::from_secs(30), now)
    .await
    .expect("funded");
assert_eq!(allocation.grant.units, CostUnits(100));

// Every unit is accounted for: deposited = balance + active grants + settled
// usage + settlement loss + expired (plus recorded overage on the left).
let ledger = store.conservation(AccountId(1)).expect("known account");
assert_eq!(ledger.balance, CostUnits(900));
assert!(ledger.holds());
# }
```

## Features

- `wire`: the HTTP wire contract shared by `tollgate-server` and
  `tollgate-client`'s HTTP transport. Off by default, so store-only consumers
  skip `serde`.

## Contract

The settlement rules and the conservation equation are specified in
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md);
[`docs/USAGE_ACCOUNTING.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/USAGE_ACCOUNTING.md)
covers batch rejection semantics, and
[`docs/LEASE_OWNERSHIP.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/LEASE_OWNERSHIP.md)
lease ownership.

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
