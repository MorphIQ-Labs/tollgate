# HTTP API reference

The control-plane HTTP API that `tollgate-server` serves: every route, the role
that may call it, its request and response bodies, and the problem codes it can
return. Instances reach it through `HttpStore` in `tollgate-client`; operators
and application backends call the administrative routes directly.

The router is `router_with_maintenance` in
[`crates/tollgate-server/src/lib.rs`](../crates/tollgate-server/src/lib.rs).
The request and response types are the wire DTOs in
[`crates/tollgate-store/src/wire.rs`](../crates/tollgate-store/src/wire.rs),
plus the store and core types they embed. Every problem code is described in
the [error reference](ERRORS.md).

## Transport and authentication

The [control-plane security runbook](CONTROL_PLANE_SECURITY.md) is the
authority for deployment; this is a summary.

- **TLS off loopback.** The server refuses to start a plaintext listener on a
  non-loopback address. TLS is terminated by the server itself.
- **Three kinds of evidence.** A request authenticates with a client
  certificate verified by mutual TLS, a static bearer token
  (`Authorization: Bearer <token>`), or a Google service-account ID token
  presented as a bearer. The [security manifest](CONTROL_PLANE_SECURITY.md#security-manifest)
  maps each certificate fingerprint, token file or token subject to a named
  identity with one role. A request that presents both a certificate and a
  bearer must resolve both to the same identity.
- **Three disjoint roles.** `instance` calls the lease, snapshot,
  credential-feed and usage routes. `operator` calls everything under
  `/v1/admin`. `provisioner` calls the self-service subset of `/v1/admin` —
  every account route except `deposit`, and no principal-snapshot route — and
  only with the arguments and on the accounts
  [its scope](#the-provisioner-scope) allows. A credential on a route its role
  does not reach answers `403 scope-forbidden`, and the refusal is audited.
- **Probes are open.** `/livez` and `/readyz` take no credential.

Missing, malformed, unverifiable or expired evidence answers
`401 authentication-required` with
`WWW-Authenticate: Bearer realm="tollgate-control"`. Evidence that verifies
but maps to no identity, or to an identity whose role the route does not admit,
answers `403 scope-forbidden`. Authorization runs before the body or path is decoded,
so an unauthenticated request never reaches validation.

## Conventions

- **Prefix.** Every route except the probes is under `/v1`
  (`API_PREFIX` in `wire.rs`).
- **Identifiers.** Every 128-bit identifier — `account_id`, `lease_id`,
  `key_id`, `request_id`, a principal — is exactly 32 lowercase hexadecimal
  characters, in JSON strings and in path segments. A path segment in any
  other form answers `400 invalid-id`.
- **Units.** `CostUnits` and fencing tokens are JSON integers. Timestamps are
  RFC 3339 strings.
- **Bodies.** Requests are `application/json`. Every error is an RFC 7807
  `application/problem+json` body with a stable machine `code`:

  ```json
  {"status": 409, "code": "insufficient-balance",
   "title": "insufficient balance (5 units remain, all held in leases)",
   "balance_shortfall": {"remaining": 5, "period_end": null}}
  ```

  `status`, `code` and `title` are always present. `generation`,
  `balance_exhaustion`, `balance_shortfall` and `error_id` appear only where the
  [error reference](ERRORS.md#the-problem-body) says. Classify by `status` and
  `code`; the `title` is for people.
- **Body limits.** `POST /v1/usage/ingest` accepts bodies up to 2 MiB
  (`MAX_INGEST_BODY_BYTES`), both snapshot `PUT` routes up to 4 MiB
  (`MAX_SNAPSHOT_BODY_BYTES`), and every other route axum's 2 MiB default. A
  larger body answers `413 batch-too-large`.
- **Common errors.** Every protected route can answer
  `401 authentication-required`, `403 scope-forbidden` and `503 storage`. Every
  route with a JSON body can answer `invalid-json` and `413 batch-too-large`;
  every route with a path identifier can answer `400 invalid-id`. The per-route
  lists below name only the codes particular to each route.
- **Unmatched requests.** A path the router does not serve answers `404`, and a
  method it does not serve on a known path answers `405`, both with an empty
  body rather than a problem.

## Routes

The table is checked against the router by `tollgate-repo-check`
([`src/http_routes.rs`](../crates/tollgate-repo-check/src/http_routes.rs)):
a route added, removed or re-roled without updating it fails the build.

A provisioner's argument and account limits are listed under
[the provisioner scope](#the-provisioner-scope).

<!-- http-routes:start -->
| Method | Path | Role |
|---|---|---|
| GET | /livez | none |
| GET | /readyz | none |
| POST | /v1/leases/acquire | instance |
| POST | /v1/leases/release | instance |
| POST | /v1/leases/consolidate | instance |
| POST | /v1/leases/reclaim | instance |
| GET | /v1/snapshots | instance |
| GET | /v1/snapshots/{principal} | instance |
| GET | /v1/keys | instance |
| POST | /v1/usage/ingest | instance |
| POST | /v1/admin/accounts | operator, provisioner |
| GET | /v1/admin/accounts/{account} | operator, provisioner |
| PUT | /v1/admin/accounts/{account}/budget | operator, provisioner |
| POST | /v1/admin/accounts/{account}/deposit | operator |
| POST | /v1/admin/accounts/{account}/status | operator, provisioner |
| POST | /v1/admin/accounts/{account}/capacity-class | operator, provisioner |
| POST | /v1/admin/accounts/{account}/keys | operator, provisioner |
| GET | /v1/admin/accounts/{account}/keys | operator, provisioner |
| DELETE | /v1/admin/accounts/{account}/keys/{key} | operator, provisioner |
| PUT | /v1/admin/accounts/{account}/keys/{key}/snapshot | operator, provisioner |
| DELETE | /v1/admin/accounts/{account}/keys/{key}/snapshot | operator, provisioner |
| PUT | /v1/admin/snapshots/{principal} | operator |
| DELETE | /v1/admin/snapshots/{principal} | operator |
<!-- http-routes:end -->

## Probes

### `GET /livez`

`200` with an empty body while the process can serve HTTP. No role.

### `GET /readyz`

`200` with an empty body when the store answers `ping` and the server's
maintenance task is healthy: both the reclaim and the budget-rollover passes
have completed and their latest outcomes succeeded. Otherwise `503` with an
empty body. No role. A router built with `tollgate_server::router` rather than
`serve` has no maintenance task and always answers `503`. See
[maintenance readiness and recovery](CONTROL_PLANE_SECURITY.md#maintenance-readiness-and-recovery).

## Leases

The lease lifecycle. Semantics are `LeaseAllocator`'s in
[`crates/tollgate-store/src/traits.rs`](../crates/tollgate-store/src/traits.rs);
the [concepts page](CONCEPTS.md#leases) introduces them.

### `POST /v1/leases/acquire`

Role `instance`. Debit a grant from the account's balance.

Request `AcquireRequest`. `requested` is a ceiling: the backend's grant policy
may grant less. The TTL is `ttl_seconds` (whole seconds), or `ttl_seconds: 0`
with an exact duration string in `ttl`; see
[lease TTL compatibility](CONTROL_PLANE_SECURITY.md#lease-ttl-compatibility).

```json
{"account_id": "…32 hex…", "requested": 1000, "ttl_seconds": 60}
```

`200` with `AcquireResponse` (an `Allocation`): the `LeaseGrant` fields, and
`funding`, the ledger's remaining funding after the grant, when the backend
attests it.

```json
{"lease_id": "…", "account_id": "…", "fencing_token": 7, "units": 1000,
 "expires_at": "2026-09-28T12:01:00Z",
 "funding": {"remaining": 4000, "period_end": null}}
```

Errors: `404 unknown-account`, `409 account-inactive`,
`409 insufficient-balance`, `409 balance-exhausted`, `422 invalid-ttl`.

### `POST /v1/leases/release`

Role `instance`. Return a lease's unspent units and close it.

Request `ReleaseRequest`:

```json
{"lease_id": "…", "fencing_token": 7, "unspent": 120}
```

`204` with no body.

Errors: `404 unknown-lease`, `409 fenced`, `409 lease-not-active`,
`422 invalid-release`.

### `POST /v1/leases/consolidate`

Role `instance`. Return an active lease's unspent units and re-grant against
the restored balance, in one backend transaction. The new grant is for the
account the returned lease names.

Request `ConsolidateRequest`: the release half (`lease_id`, `fencing_token`,
`unspent`), the grant half (`requested`, and the TTL fields as for acquire),
and `needed`, the largest quote the returned lease refused. `needed` is omitted
when zero, and reads as zero when absent.

```json
{"lease_id": "…", "fencing_token": 7, "unspent": 30, "requested": 1000,
 "needed": 51, "ttl_seconds": 60}
```

`200` with `ConsolidateResponse` (an `Allocation`, as for acquire).

Errors: every acquire and release code above. A domain refusal leaves the
original lease unchanged; `503 storage` leaves the outcome unknown, and the
caller must not resume spending from the old lease.

### `POST /v1/leases/reclaim`

Role `instance`. Settle every lease whose TTL and reclaim grace have lapsed,
exactly as the server's own maintenance sweep does. No request body.

`200` with a JSON array of `ReclaimedLease`. `forfeited` is recorded as
settlement loss, not credited back.

```json
[{"lease_id": "…", "account_id": "…", "forfeited": 880}]
```

Errors: only the common ones. A `503 storage` after some batches committed
leaves those batches committed.

## Snapshots

### `GET /v1/snapshots`

Role `instance`. The principal catalogue, revoked principals included.

`200` with `PrincipalsResponse`:

```json
{"principals": ["…32 hex…", "…32 hex…"]}
```

Errors: `501 enumeration-unsupported` when the backend cannot enumerate
principals. That is not an empty catalogue.

### `GET /v1/snapshots/{principal}`

Role `instance`. One principal's compiled snapshot.

`200` with the whole `AccountSnapshot`, cost table included. Its fields are
defined in
[`crates/tollgate-core/src/snapshot.rs`](../crates/tollgate-core/src/snapshot.rs).

Errors: `404 unknown-principal` when no snapshot was ever published;
`410 revoked-principal`, with the tombstone's `generation`, when it was
withdrawn.

## Credential feed

### `GET /v1/keys`

Role `instance`. One page of active customer credential digests, for instance
verifiers. Query: `after`, an exclusive key-ID cursor, and `limit`, 1–4096
(default 256). The response carries `Cache-Control: no-store`.

`200` with `KeysResponse`:

```json
{"revision": 12, "as_of": "2026-09-09T12:00:00Z",
 "keys": [{"key_id": "…", "principal": "…", "digest": "…64 hex…",
           "not_after": null}],
 "next_after": null}
```

Errors: `400 invalid-query`, `422 invalid-limit`,
`503 credential-source-unavailable` in place of `storage`. The
[credential projection](CREDENTIAL_PROJECTION.md) page states the paging and
revision contract.

## Usage

### `POST /v1/usage/ingest`

Role `instance`. Record a batch of committed usage events, idempotently by
`request_id`. A batch holds at most 4096 events (`MAX_INGEST_BATCH`), within a
2 MiB body.

Request `IngestRequest`:

```json
{"events": [{"request_id": "…", "account_id": "…",
             "source": {"Leased": {"lease_id": "…", "fencing_token": 7}},
             "units": 64, "occurred_at": "2026-09-28T12:00:00Z",
             "policy_revision": "…64 hex…", "key_id": "…"}]}
```

`source` is `{"Leased": {…}}` for units spent from a lease, or `"Overage"` for
units admitted under elastic enforcement without one. `policy_revision` and
`key_id` may be omitted.

`200` with an `IngestReport`. `accepted + duplicate + rejected` equals the
number of events sent. `unattributed` counts newly accepted events whose key
attribution was absent, unknown or for another account; it is `null` from a
sink that does not report attribution.

```json
{"accepted": 63, "duplicate": 1, "rejected": 0, "unattributed": 0}
```

Errors: `422 usage-refused` when the store examined the batch and will refuse
it again unchanged; `413 batch-too-large`. See
[usage accounting](USAGE_ACCOUNTING.md) for what `rejected` means for billing.

## Account administration

Every route in this section admits `operator`; all but `deposit` also admit
`provisioner`, within [its scope](#the-provisioner-scope). Mutations are
audited with a receipt captured by the backend; see
[administrative audit](CONTROL_PLANE_SECURITY.md#administrative-audit). The
[account administration](ACCOUNT_ADMINISTRATION.md) page is the provisioning
guide these routes serve.

### The provisioner scope

A `provisioner` credential is what an internet-facing signup service holds, so
compromising it must not fund, close, grant `Assured`, or reach an account an
operator made. Its limits are enforced before any write, and every refusal is
written to the audit log with the actor, the role and the action.

| Route | A provisioner may | Refused with `403 scope-forbidden` |
|---|---|---|
| `POST /accounts` | `initial_balance: 0` and `status: "Suspended"`; the account is created `BestEffort` | any other balance or status |
| `POST …/status` | `Active` | `Suspended`, `Closed` |
| `POST …/capacity-class` | `BestEffort` | `Assured` |
| `PUT …/budget` | an allowance up to its `max_budget_allowance`, or `null` | a larger allowance |
| `PUT …/keys/{key}/snapshot` | a `Strict` snapshot with a store-allocated generation | `Elastic`, which extends unfunded credit |
| `POST …/deposit`, `/snapshots/{principal}` | nothing | every call |

Every route that names an account also requires that a provisioner created it.
An account an operator created — every account that predates the role
included — answers `403 account-not-provisioned`, and one that does not exist
answers `404 unknown-account`. Activation additionally answers
`403 operator-hold` when an operator set the account's current status: an
operator's suspension holds until an operator lifts it. The account's
`origin` and `status_set_by` report both facts.

A provisioner's snapshot still carries its own cost table, limits and
permissions; the enforcement mode is constrained and the store owns generations
([#43](https://github.com/MorphIQ-Labs/tollgate/issues/43) tracks
operator-approved policy templates). Deploy a provisioner whose policy source
you trust.

### `POST /v1/admin/accounts`

Create an account. An operator's is always `Assured`; a provisioner's is
always `BestEffort`, unfunded and suspended.

Request `CreateAccountRequest`:

```json
{"account_id": "…", "initial_balance": 0, "status": "Suspended"}
```

`201` with no body. Errors: `409 account-exists`, and for a provisioner
`403 scope-forbidden`.

### `GET /v1/admin/accounts/{account}`

One account's administrative state and funding position, read from one
consistent backend snapshot as of `as_of`.

`200` with `AccountResponse`:

```json
{"account_id": "…", "as_of": "…", "status": "Active",
 "capacity_class": "Assured", "origin": "Operator", "status_set_by": "Operator",
 "budget": null, "period_start": "…",
 "balance": 600, "outstanding_lease_grants": 400, "settled_usage": 0,
 "expired_allowance": 0, "settlement_loss": 0, "deposited": 1000,
 "overage_recorded": 0}
```

`origin` is the authority that created the account, `Operator` or
`Provisioner`, and never changes. `status_set_by` is the authority that set
the current status. A client reading a server that predates them sees
`Operator` for both.

Errors: `404 unknown-account`; for a provisioner, `403 account-not-provisioned`.

### `PUT /v1/admin/accounts/{account}/budget`

Set or clear the periodic allowance. The `budget` field is required;
`null` clears the schedule.

Request `SetBudgetRequest`:

```json
{"budget": {"allowance": 500, "period": "UtcCalendarMonth", "rollover": "None"}}
```

`200` with `SetBudgetResponse`, the schedule the call replaced and the one now
in force; equal values mean the call changed nothing.

```json
{"previous": null, "current": {"allowance": 500, "period": "UtcCalendarMonth", "rollover": "None"}}
```

Errors: `404 unknown-account`; for a provisioner, `403 scope-forbidden` above
its ceiling and `403 account-not-provisioned`. An absent `budget` field or an
unknown field is `422 invalid-json`.

### `POST /v1/admin/accounts/{account}/deposit`

Role `operator` only. Add units to the account's balance once, as a top-up that survives period
boundaries.

Request `DepositRequest`: `{"units": 1000}`. `204` with no body.

Errors: `400 zero-deposit`, `404 unknown-account`. A deposit that would
overflow the account's balance or deposited total is refused as
`422 balance-overflow` and changes nothing. An amount outside the backend's
unit domain is refused the same way (`u64` for memory, nonnegative `BIGINT`
through `i64::MAX` for PostgreSQL). Never retry the unchanged request.
Backend outages remain `503 storage`.

### `POST /v1/admin/accounts/{account}/status`

Set the account's status and republish every live snapshot of the account with
it.

Request `SetStatusRequest`: `{"status": "Active"}` — `Active`, `Suspended` or
`Closed`.

`200` with `SetStatusResponse`: `republished` is the number of live snapshots
rewritten; `unreadable` is the number of stored rows that changed but could not
be decoded to push, which converge at their next refresh.

```json
{"republished": 2, "unreadable": 0}
```

An operator's status is recorded as an operator hold: a provisioner cannot
change it.

Errors: `404 unknown-account`, `409 account-closed`; for a provisioner,
`403 scope-forbidden` for anything but `Active`, `403 account-not-provisioned`
and `403 operator-hold`.

### `POST /v1/admin/accounts/{account}/capacity-class`

Set the account's execution-capacity class and republish its live snapshots.

Request `SetCapacityClassRequest`: `{"capacity_class": "BestEffort"}` —
`Assured` or `BestEffort`. `200` with `SetStatusResponse`, as for status.

Errors: `404 unknown-account`, `409 account-closed`; for a provisioner,
`403 scope-forbidden` for `Assured` and `403 account-not-provisioned`.

## Credential administration

### `POST /v1/admin/accounts/{account}/keys`

Roles `operator` and `provisioner`. Issue a credential. The response is the only time its secret
is disclosed.

Request `IssueKeyRequest`. The caller chooses `key_id`; `max_active_keys` is
the caller's bound on the account's live credentials; `not_after` is optional.

```json
{"key_id": "…", "max_active_keys": 3, "not_after": "2027-01-01T00:00:00Z"}
```

`201` with `IssuedKeyResponse`:

```json
{"key_id": "…", "secret": "…64 hex…", "not_after": "2027-01-01T00:00:00Z"}
```

Errors: `404 unknown-account`, `409 credential-exists`,
`409 active-key-limit`, `501 issuance-unsupported` when the server has no
credential issuer, `503 entropy-unavailable`, `500 issuer-misconfigured`; for
a provisioner, `404 unknown-account` and `403 account-not-provisioned`, checked
before anything is minted.

### `GET /v1/admin/accounts/{account}/keys`

Roles `operator` and `provisioner`. One page of the account's credentials,
revoked and expired ones included: metadata only, never a secret, digest or
principal. Query: `after` and `limit`, as for `GET /v1/keys`. An account with
no credentials answers an empty page, and so does one that does not exist when
an operator asks; a provisioner gets `404 unknown-account`, because its scope
check reads the account first.

`200` with `AccountKeysResponse`, whose entries are `AccountKeyResponse`.
`next_after` is `null` on the last page.

```json
{"as_of": "…", "keys": [{"key_id": "…", "not_after": null,
  "revoked_at": null, "live": true}], "next_after": null}
```

Errors: `400 invalid-query`, `422 invalid-limit`; for a provisioner,
`404 unknown-account` and `403 account-not-provisioned`.

### `DELETE /v1/admin/accounts/{account}/keys/{key}`

Roles `operator` and `provisioner`. Revoke one of the account's credentials. Revocation leaves its
bound snapshot in place; withdraw that with the `DELETE` below.

`200` with `RevokeKeyResponse`. `retired: false` means it was already revoked.

```json
{"key_id": "…", "retired": true}
```

Errors: `404 unknown-credential`, including for another account's key; for a
provisioner, `404 unknown-account` and `403 account-not-provisioned`.

### `PUT /v1/admin/accounts/{account}/keys/{key}/snapshot`

Roles `operator` and `provisioner`. Publish the snapshot one credential
authorizes against. The
server resolves the principal from its own key record, and fills in the
snapshot's `key_id` when it is unset.

Request `PublishSnapshotRequest`: `{"snapshot": { … }}`, an `AccountSnapshot`.
`204` with no body. For an operator, a generation at or below the stored one
changes nothing and still answers `204`.

For a provisioner, the submitted `generation` is ignored. The store assigns 1
on first publication, then the stored live snapshot or tombstone's generation
plus one, atomically with publication. Repeats receive a fresh generation;
concurrent writes are ordered by the store, so a retry can replace a newer
policy. Serialize policy updates per key when their order matters. The audit
receipt records the allocated generation. Arbitrary caller-selected jumps
cannot exhaust the counter and obstruct operator suspension, closure or
capacity-class changes. Exhaustion of the stored counter returns `503 storage`
without a publication; the generation never wraps or resets.

Errors: `404 unknown-credential`, `409 credential-retired`,
`422 invalid-credential-binding`, `422 invalid-snapshot-limits`,
`409 snapshot-status-mismatch`, `409 snapshot-capacity-class-mismatch`; for a
provisioner, `403 scope-forbidden` for an `Elastic` snapshot,
`404 unknown-account` and `403 account-not-provisioned`.

### `DELETE /v1/admin/accounts/{account}/keys/{key}/snapshot`

Roles `operator` and `provisioner`. Withdraw the snapshot bound to one
credential, revoked or not. `204` with no body.

Errors: `404 unknown-credential`; for a provisioner, `404 unknown-account` and
`403 account-not-provisioned`.

## Principal snapshots

For embedders that derive principals themselves. The key-bound routes above are
the supported path for credentials this server issues.

### `PUT /v1/admin/snapshots/{principal}`

Role `operator` only. Publish a principal's snapshot, generation-monotonically.

Request `PublishSnapshotRequest`. `204` with no body.

Errors: `422 invalid-snapshot-limits`, `422 invalid-credential-binding` when the
snapshot names a `key_id` that does not bind this principal and account,
`409 snapshot-status-mismatch`, `409 snapshot-capacity-class-mismatch`.

### `DELETE /v1/admin/snapshots/{principal}`

Role `operator`. Withdraw a principal's snapshot, replacing it with a tombstone
at the same generation, which instances observe as `410 revoked-principal`.
`204` with no body. A principal with no live snapshot is left unchanged and
also answers `204`.

Errors: only the common ones.
