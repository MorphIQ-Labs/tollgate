# Error reference

Two vocabularies of refusal. The control plane answers HTTP requests with RFC
7807 problem bodies, each carrying a stable machine `code`. The request path
refuses admission with a `DenyReason`, which never involves I/O and always
charges zero units; how a service turns one into an HTTP response is its own
choice.

Codes and reasons are contracts: `tollgate-client`'s HTTP transport maps problem
codes back to domain errors, and deny-reason labels name exported counters. An
embedder can match on them.

## The problem body

Every `tollgate-server` error is `application/problem+json`, built in
[`crates/tollgate-server/src/error.rs`](../crates/tollgate-server/src/error.rs)
from the `Problem` type in
[`crates/tollgate-store/src/wire.rs`](../crates/tollgate-store/src/wire.rs):

| field | present | meaning |
| --- | --- | --- |
| `status` | always | the HTTP status, repeated |
| `code` | always | the stable machine code below; classify by this and `status` |
| `title` | always | a human-readable summary. Never backend text: storage failures always read `backend unavailable` |
| `generation` | `revoked-principal` only | the tombstone's generation |
| `balance_exhaustion` | `balance-exhausted`, when the allocator attests it | `{"period_end": …}`: when the period that could refund the account ends, or `null` without a schedule |
| `balance_shortfall` | `insufficient-balance`, when the allocator attests it | `{"remaining": …, "period_end": …}`: funding still held in other leases |
| `error_id` | every 5xx, and `usage-refused` | 32 lowercase hex digits naming the matching `tollgate::diagnostics` warning; see [backend failures and diagnostics](CONTROL_PLANE_SECURITY.md#backend-failures-and-diagnostics) |

A `401` also carries `WWW-Authenticate: Bearer realm="tollgate-control"`.

There is no `type` member: the `code` is the problem type. Requests to a path
or method the router does not serve get axum's plain `404` or `405` with an
empty body, not a problem.

## Problem codes

Every code the server can return. "Retry" means resending the *same* request.
The [HTTP API reference](HTTP_API.md) lists which routes return which codes.

### Authentication and request shape

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `authentication-required` | 401 | no credential, a malformed or duplicate `Authorization` header, a credential that does not verify or has expired, a client certificate no longer trusted, or a certificate and bearer naming different identities | after the credential is fixed or rotated |
| `scope-forbidden` | 403 | the credential verifies but maps to no identity, or to the other role | no — change the role map or the credential |
| `invalid-id` | 400 | a path identifier is not exactly 32 lowercase hexadecimal digits | no — fix the path |
| `invalid-json` | 400, 415 or 422 | the body is not JSON (400), is not sent as `application/json` (415), or does not match the endpoint's type, including a missing required field or an unknown field where refused (422). The status is axum's rejection status | no — fix the body |
| `invalid-query` | 400 | query parameters are malformed or unknown | no — fix the query |
| `invalid-limit` | 422 | a credential page `limit` outside 1–4096 | no — fix the limit |
| `batch-too-large` | 413 | the body exceeds the endpoint's limit: 2 MiB for usage ingest and other routes, 4 MiB for snapshot publication. Title: "request body exceeds this endpoint's limit"; only usage ingest appends the usage-batch event cap | never unchanged — reduce the body or split a usage batch |

### Leases

`tollgate-client` maps each of these to an `AllocateError` variant of the same
name. See [`LeaseAllocator`](../crates/tollgate-store/src/traits.rs).

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `unknown-account` | 404 | no such account | no — fix the id |
| `account-inactive` | 409 | the account exists but is not in a state that may spend | after its status changes |
| `insufficient-balance` | 409 | no grant is possible now. With `balance_shortfall`, the ledger attests how much funding remains, all of it held in other leases | yes, polling: settlement, lease release or a top-up can restore balance |
| `balance-exhausted` | 409 | the ledger confirms no funding remains, including in leases | after a deposit, or after `balance_exhaustion.period_end` for a scheduled account |
| `invalid-ttl` | 422 | the lease TTL is not one unambiguous positive duration | no — fix the TTL |
| `unknown-lease` | 404 | no such lease | no — acquire a new lease |
| `fenced` | 409 | the fencing token does not match the lease | no — the capability is not this caller's |
| `lease-not-active` | 409 | the lease was already released, expired or reclaimed | no — acquire a new lease |
| `invalid-release` | 422 | the release claims more unspent units than the lease can still hold: a client accounting fault | no — investigate the caller |

### Snapshots and principals

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `unknown-principal` | 404 | no snapshot was ever published for this principal | after one is published |
| `revoked-principal` | 410 | the principal's snapshot was withdrawn; `generation` is the tombstone's | only after a republish at a higher generation |
| `enumeration-unsupported` | 501 | the backend cannot list principals. Not an empty catalogue | no — track a configured principal set |
| `invalid-snapshot-limits` | 422 | the snapshot's limits cannot be enforced as published | no — fix the snapshot |
| `invalid-credential-binding` | 422 | the snapshot's `key_id` names a different credential, principal or account than the one being published | no — fix the snapshot |
| `snapshot-status-mismatch` | 409 | the snapshot's status contradicts the account ledger | no — change status through the status route |
| `snapshot-capacity-class-mismatch` | 409 | the snapshot's capacity class contradicts the account ledger | no — change it through the capacity-class route |

### Accounts and credentials

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `account-exists` | 409 | the account is already created | no — creation already succeeded |
| `account-closed` | 409 | the account is `Closed`, which no status or class change leaves | no |
| `zero-deposit` | 400 | a deposit of zero units | no — deposit a positive amount |
| `unknown-credential` | 404 | no such credential for this account, including another account's key | no |
| `credential-exists` | 409 | this `key_id` is already recorded | no — the first issuance succeeded; its secret is not disclosed again |
| `active-key-limit` | 409 | the account already holds `max_active_keys` live credentials | after revoking one |
| `credential-retired` | 409 | the credential is revoked and can never be bound to a policy again | no — issue a new credential |
| `issuance-unsupported` | 501 | this server has no credential issuer configured | no — configure one; see [credential issuer](CONTROL_PLANE_SECURITY.md#credential-issuer) |
| `issuer-misconfigured` | 500 | the configured issuer minted a secret that is not presentable text; nothing was stored | no — fix the issuer |
| `entropy-unavailable` | 503 | the issuer could not obtain entropy | yes, with backoff |

### Usage and availability

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `usage-refused` | 422 | the store examined the usage batch and will refuse it again unchanged, for example an accounting total that cannot absorb its units | never unchanged |
| `credential-source-unavailable` | 503 | the credential feed could not produce a valid page | yes, with backoff |
| `storage` | 503 | the backend could not answer, or refused an operation for a reason it does not classify (a deposit that would overflow the balance is one). A mutation's outcome is unknown | yes, with backoff; reconcile a mutation against its audit receipt |

`tollgate-client` treats an ingest answer of `401`, `403`, `408`, `429` or any
5xx as retryable, and every other 4xx as a refusal it must not replay; see
[usage accounting](USAGE_ACCOUNTING.md).

## Deny reasons

`DenyReason`, in
[`crates/tollgate-core/src/deny.rs`](../crates/tollgate-core/src/deny.rs), is
every reason admission can refuse a request. Each carries a `Retry`
classification from `DenyReason::retry`, so every embedder gives the same
advice for the same refusal:

- **`Transient`** — the same request can become admissible when capacity or
  freshness recovers, without a funding change.
- **`AfterInFlight`** — retry once a concurrent admission decision finishes
  publishing. The retry may then report transient capacity.
- **`Never`** — the same request cannot become admissible under the current
  policy and funding. New funding, a new budget period or a policy change can
  change that.

`label` is `DenyReason::name`, the metric label the reason is counted under
(see [the metrics reference](METRICS.md#deny-reason-labels)).

The last column is the status and problem `code` that
[`examples/pricing-api`](../examples/pricing-api/src/lib.rs) answers with, in
its `deny_response`. **That mapping is the example's own choice**, not part of
Tollgate's contract; another embedder may choose differently. It is recorded
here because it is a worked answer to the question each reason poses.

| reason | label | meaning | `Retry` | pricing-api |
| --- | --- | --- | --- | --- |
| `UnknownPrincipal` | `unknown_principal` | no snapshot is installed for the principal, including a principal recently confirmed unknown | `Never` | 401 `unknown-principal` |
| `AccountSuspended` | `account_suspended` | the account is administratively suspended | `Never` | 403 `forbidden` |
| `AccountClosed` | `account_closed` | the account is closed; terminal | `Never` | 403 `forbidden` |
| `SnapshotExpired` | `snapshot_expired` | the installed snapshot's validity window lapsed and no replacement arrived | `Transient` | 503 `policy-stale` |
| `MissingPermission` | `missing_permission` | the snapshot does not grant the operation's permission bits | `Never` | 403 `forbidden` |
| `RequestTooLarge { max_items }` | `request_too_large` | the item count exceeds the account's batch cap | `Never` | 413 `batch-too-large` |
| `UnpricedOperation` | `unpriced_operation` | the operation has no price in the account's cost table | `Never` | 422 `unpriceable` |
| `RateLimited` | `rate_limited` | the account's weighted rate limiter has no capacity for this request's weight now | `Transient` | 429 `rate-limited` |
| `RequestRateLimited` | `request_rate_limited` | the account's request-count bucket has no token now | `Transient` | 429 `request-rate-limited` |
| `ConcurrencyLimited` | `concurrency_limited` | an account or principal in-flight ceiling is saturated | `Transient` | 429 `concurrency-limited` |
| `UnpriceableUnderLimits { weight, burst_units }` | `unpriceable_under_limits` | the quote exceeds the account's whole burst capacity, so no wait can admit it: a misconfigured schedule | `Never` | 422 `unpriceable-under-limits` |
| `LeaseUnavailable` | `lease_unavailable` | no lease is installed for the account: cold start, or lost | `Transient` | 503 `quota-unavailable` |
| `LeaseExpired` | `lease_expired` | the local lease's validity lapsed and refill has not replaced it | `Transient` | 503 `quota-unavailable` |
| `LeaseExhausted { remaining }` | `lease_exhausted` | the local lease cannot cover the quote | `Transient` | 429 `quota-exhausted` |
| `OverageCapExhausted { spent, overage_cap }` | `overage_cap_exhausted` | an elastic request cannot fit inside the per-instance overage cap even if every refundable reservation releases. Does not prove central exhaustion | `Transient` | 503 `overage-cap-exhausted` |
| `CostOverflow` | `cost_overflow` | cost arithmetic overflowed; the quote is refused rather than wrapped | `Never` | 422 `unpriceable` |
| `AccountingBackpressure` | `accounting_backpressure` | the usage queue is full; admitting would drop billing events or block | `Transient` | 503 `accounting-busy` |
| `OverageCapTemporarilyExhausted { spent, overage_cap }` | `overage_cap_temporarily_exhausted` | pending overage reservations occupy the cap, and can return it without a funding change | `Transient` | 503 `overage-cap-temporarily-exhausted` |
| `OverageCommitInProgress { spent, overage_cap }` | `overage_commit_in_progress` | a reservation is publishing its move from pending to committed overage | `AfterInFlight` | 503 `overage-commit-in-progress` |
| `EmptyWorkload` | `empty_workload` | the staged request carried no priceable work | `Never` | 422 `empty-workload` |
| `FundingExpiredAtStart` | `funding_expired_at_start` | the funding reserved at admission expired before execution started; staged lifecycle only | `Transient` | 503 `funding-expired-at-start` |
| `CapacityUnavailable` | `capacity_unavailable` | this instance has no execution capacity to start the request; says nothing about the account | `Transient` | 503 `capacity-unavailable` |
| `BalanceExhausted` | `balance_exhausted` | the allocator confirmed the account's funding is exhausted | `Never` | 402 `balance-exhausted` |
| `BalanceInsufficient { remaining }` | `balance_insufficient` | the allocator confirmed the account's remaining funding, counting units in leases, is below this quote. Smaller quotes are unaffected | `Never` | 402 `balance-insufficient` |

The payload fields in braces are for the caller's response; they are not part
of the label, so a label never mints a time series per value. The example
renders `DenyReason`'s `Display` text as the problem `title` and sends no
`Retry-After` header: no reason carries a retry instant.

The [concepts page](CONCEPTS.md#refusals) introduces refusals, and
[embedding Tollgate](EMBEDDING.md) states which parts of `pricing-api` are
contract.
