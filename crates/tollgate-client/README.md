# tollgate-client

The instance-side runtime of [Tollgate](https://github.com/MorphIQ-Labs/tollgate):
the background tasks that keep a service's admission layer supplied and its
usage accounted, all off the request path.

- `InstanceRuntime` owns account discovery, lease slots, supervised lease
  managers, and the usage writer. Its cloneable `RuntimeHandle` provides staged
  admission, readiness, and reports.
- `LeaseManager` keeps an account's lease slot stocked from a
  `LeaseAllocator`, refilling at a low-water mark and consolidating a drained
  tail lease into its replacement.
- `UsageWriter` drains a bounded channel of usage events into a `UsageSink`
  in idempotent batches. When the channel is full, admission sheds before work
  is accepted, so committed work always has somewhere to be billed.
- `KeyManager` refreshes customer credentials from a read-only `KeySource`,
  bounding cached evidence by feed freshness and key expiry.
- `PeriodRoller` funds budget schedules and rolls due periods, for
  applications that talk to a store directly.

The request path only loads published state and reserves channel permits.
Timestamps come from a `Clock`, so every behaviour is testable with a manual
clock.

## Topologies

- **Direct store:** the runtime talks to a `tollgate-store` backend, such as
  [`tollgate-store-postgres`](https://crates.io/crates/tollgate-store-postgres).
- **Via server:** with the `http` feature, `HttpStore` implements the store
  traits against a [`tollgate-server`](https://crates.io/crates/tollgate-server)
  over authenticated TLS.

## Shutdown

The runtime enforces one total shutdown deadline: it closes accounting
admission, pauses refills, drains issued permits and guards, and releases
account leases. Stop admitting, quiesce in-flight requests, then await the
writer, so no committed usage is lost; undelivered events are counted rather
than dropped silently.

## Features

- `http`: the `HttpStore` transport to a `tollgate-server`. Off by default;
  the direct-store topology needs none of it.

## Getting started

[`docs/EMBEDDING.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/EMBEDDING.md)
gives the supported request order and shutdown sequence, and
`examples/pricing-api` in the repository is a complete embedding. Credential
distribution is in
[`docs/CREDENTIAL_PROJECTION.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/CREDENTIAL_PROJECTION.md);
the contract is
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md).

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
