# Account, budget and credential administration

An application backend provisions and inspects one customer account over HTTP:
its status and capacity class, its periodic allowance, its funding position,
and the credentials it authenticates with. Tollgate owns generic accounts,
credential lifecycle and quota accounting. The application owns customer
login, consent, plan names, prices and billing, and supplies resolved generic
policy values — an allowance in `CostUnits`, a credential limit — rather than
its own vocabulary.

This is the supported alternative to reaching into Tollgate's tables or
re-implementing credential and budget authority in a second service (#121).

## Authority and transport

Every route below requires the **operator** role from the
[control-plane security runbook](CONTROL_PLANE_SECURITY.md). An instance
credential authenticates the request path and cannot administer accounts: it
must not reach a customer's funding position or credential list. Anonymous
callers are refused. TLS is required outside loopback.

Identifiers — `account_id`, `key_id` — are 32 lowercase hexadecimal
characters, as elsewhere in this API.

## The provisioning sequence

Each step is safe to repeat. A caller that loses a response resends the same
request; none of these steps creates a second account, funds a period twice, or
discloses a secret again.

**1. Create the account.** `POST /v1/admin/accounts`

```json
{ "account_id": "…32 hex…", "initial_balance": 0, "status": "Suspended" }
```

`201` on creation, `409 account-exists` if it already exists — which is the
retry answer, not a failure: the account is there and creation is never
destructive. Creation always assigns `CapacityClass::Assured`; step 2 changes
it if the plan calls for something else.

Create suspended and activate in step 4, so an account cannot lease before its
budget and credentials exist.

**2. Set the capacity class**, if not `Assured`.
`POST /v1/admin/accounts/{account}/capacity-class` with
`{"capacity_class": "BestEffort"}`. Idempotent; the response reports how many
live snapshots the change republished.

**3. Set the periodic allowance.**
`PUT /v1/admin/accounts/{account}/budget`

```json
{ "budget": { "allowance": 500, "period": "UtcCalendarMonth", "rollover": "None" } }
```

The response reports both states, so a repeat is visibly a no-op:

```json
{ "previous": null, "current": { "allowance": 500, … } }
```

`"budget": null` clears the schedule. That is **not** the same as an allowance
of zero: `null` means the balance does not expire, zero means the account is
funded nothing each period.

**A one-off deposit is not a substitute for an allowance.**
`POST /v1/admin/accounts/{account}/deposit` adds units once; a schedule funds
the account at every period boundary and expires what the previous period did
not spend. Setting a schedule deposits nothing by itself — the first funding
arrives at the next boundary — so an account that must be usable immediately
needs both.

**4. Activate.** `POST /v1/admin/accounts/{account}/status` with
`{"status": "Active"}`. Idempotent. `Closed` is terminal: no transition leaves
it, and the request is refused with `409 account-closed`.

**5. Issue a credential.** See below. Do this last: a credential that exists
before the account is active authenticates into denials.

## Issuing a credential

`POST /v1/admin/accounts/{account}/keys`

```json
{ "key_id": "…32 hex, chosen by you…", "max_active_keys": 3,
  "not_after": "2027-01-01T00:00:00Z" }
```

`201` with the secret, **disclosed exactly once**:

```json
{ "key_id": "…", "secret": "…hex…", "not_after": "2027-01-01T00:00:00Z" }
```

**The caller chooses `key_id`, and that choice is the retry contract.** Generate
an unguessable one — a v4 UUID — and keep it until the call is acknowledged.
Resending the same request answers `409 credential-exists`, which is true and
discloses nothing: your first call succeeded.

**Nothing can return that secret again.** What Tollgate stores is an HMAC of
it; the secret is not recoverable from anything retained, by anyone, including
an operator with database access. If you lose it, revoke the credential and
issue a new one under a new `key_id`. There is no reissue, and no endpoint that
re-reveals.

Order inside the server is mint, store, return. A crash between minting and
storing loses a secret nobody has ever seen. The reverse would hand out a
credential the server has no record of, which no reconciliation could repair.

`max_active_keys` is your policy, enforced atomically against concurrent
issuers — two requests racing for the last place do not both win.
`409 active-key-limit` means the account already holds that many *live*
credentials; revoked and expired ones do not count, so an account cannot be
stranded behind credentials nobody can authenticate with.

### Listing

`GET /v1/admin/accounts/{account}/keys?after=…&limit=…`

```json
{ "as_of": "…", "keys": [ { "key_id": "…", "not_after": null,
  "revoked_at": null, "live": true } ], "next_after": null }
```

Metadata only. **No secret, no verifier digest, and no principal** — the
principal is the digest's leading 128 bits, so returning it would disclose half
of what the verifier compares against. `key_id` is the non-secret handle, and
the only one you need: it is what revocation names and what an audit shows.

Revoked and expired credentials are listed, and distinguishable: `revoked_at`
says an operator withdrew it, `not_after` says it lapsed on its own. `live` is
derived against `as_of` so you need not re-implement the rule. Page with
`next_after`; `null` means this page is the last.

### Revoking

`DELETE /v1/admin/accounts/{account}/keys/{key}` → `{"key_id": "…",
"retired": true}`.

`retired: false` means it was already revoked. That is a success, not an error —
your intent is satisfied — but the distinction is what an audit needs.

**Revocation is bound to the account in the path.** A `key_id` belonging to
another account answers `404 unknown-credential` and revokes nothing. You
cannot retire another customer's credential by guessing or mistyping an id.

Revocation is terminal and generation-ordered. It is **not** instant across a
fleet: serving instances hold a projection and converge at their next refresh.
See the [credential projection](CREDENTIAL_PROJECTION.md) runbook for that
window. Treat revocation as "no new sessions promptly", not "every in-flight
request stops now".

## Reading an account

`GET /v1/admin/accounts/{account}`

```json
{ "account_id": "…", "as_of": "…", "status": "Active",
  "capacity_class": "Assured", "budget": { … }, "period_start": "…",
  "balance": 600, "outstanding_lease_grants": 400, "settled_usage": 0,
  "expired_allowance": 0, "settlement_loss": 0,
  "deposited": 1000, "overage_recorded": 0 }
```

Authoritative as of `as_of`, from one consistent backend snapshot, so the
figures agree with each other. It is **not a live feed**: an admission
committed a millisecond later is not in it, and two reads are two instants.
`404 unknown-account` for an account that does not exist — never a zeroed body,
because "does not exist" and "exists with no funding" are different answers.

All counts are `CostUnits`: whole units, never fractional, never a currency.
Converting to money is your job, with your prices.

### Balance is not a bill

A falling `balance` is not spend. It falls for three unrelated reasons, and
only one of them is billable:

| field | meaning | billable |
| --- | --- | --- |
| `outstanding_lease_grants` | capacity out on a lease that has not settled | no |
| `settled_usage` | actually consumed | **yes** |
| `expired_allowance` | taken back when a budget period closed | no |
| `settlement_loss` | granted but unaccounted for at settlement | no |

A dashboard that shows depletion as usage will overstate what a customer owes,
by exactly the capacity they are currently holding. `settled_usage` is the
usage number.

`deposited` and `overage_recorded` are the left side of the funding equation
whose right side is `balance + outstanding_lease_grants + settled_usage +
settlement_loss + expired_allowance`. It holds exactly; a caller may check it.

**Unobserved activity.** Work admitted against a live lease is inside
`outstanding_lease_grants` until that lease settles — it is neither in
`settled_usage` nor lost. An account with outstanding grants has usage not yet
attributable, and a bill drawn before those leases settle is provisional.

## Errors

Every failure is RFC 7807 with a stable `code`. Retry behaviour by class:

| code | status | meaning | retry |
| --- | --- | --- | --- |
| `unknown-account` | 404 | no such account | no — fix the id |
| `account-exists` | 409 | already created | no — you are done |
| `account-closed` | 409 | terminal status | no |
| `credential-exists` | 409 | this `key_id` is recorded | no — your call worked |
| `active-key-limit` | 409 | at the supplied bound | no — revoke first |
| `unknown-credential` | 404 | not this account's credential | no |
| `issuance-unsupported` | 501 | this deployment does not issue | no — see below |
| `entropy-unavailable` | 503 | no credential entropy | yes, with backoff |
| `storage` | 503 | backend unavailable | yes, with backoff |

409 rather than 422 throughout: those requests are well-formed and the caller
is not at fault for asking. Secrets and digest material never appear in an
error body or a log line.

## Deployment

**Issuance requires a configured credential issuer.** The `tollgate-server`
binary ships without one and answers `501 issuance-unsupported`. This is
deliberate rather than unfinished: an instance that only verifies should not
hold the capability to create credentials, and the registry the server builds
for control-plane bearer tokens uses a secret regenerated at every start — the
wrong authority and the wrong lifetime for a credential a customer keeps. A
deployment that administers accounts supplies a durable issuer through
`ServerState::issuer`. **Every other route on this page works without one.**

**Migration `0019`** adds `(account_id, key_id)` to the credential table for
the account-scoped listing. Additive and forward-only: no data change, safe to
apply before the code that uses it, and harmless if the deployment is rolled
back.

**`Backend` now requires `KeyDirectory`.** A server administers credentials as
well as projecting them. `HttpStore` implements `KeySource` but not
`KeyDirectory` — correctly, since it is a client of a server rather than the
authority behind one — so it cannot back a server. No in-tree backend is
affected.

## Conformance

A consumer adapter should pin an exact Tollgate release tag and verify, against
that tag, that:

1. repeating each provisioning step changes nothing and reports the repeat;
2. a lost issuance response resent with the same `key_id` answers `409` and
   discloses no secret;
3. a listing contains no `secret`, `digest` or `principal` field;
4. revoking another account's `key_id` answers `404` and leaves it live;
5. `settled_usage` stays zero while a lease is outstanding, and the funding
   equation holds across the grant.

`crates/tollgate-server/tests/api.rs` exercises 1–5 against the in-process
router and is the executable reference for the expected status codes and
bodies.
