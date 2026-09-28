# Account, budget and credential administration

An application backend provisions and inspects one customer account over HTTP:
its status and capacity class, its periodic allowance, its funding position,
and the credentials it authenticates with. Tollgate owns generic accounts,
credential lifecycle and quota accounting. The application owns customer
login, consent, plan names, prices and billing, and supplies resolved generic
policy values — an allowance in `CostUnits`, a credential limit — rather than
its own vocabulary.

This is the supported alternative to reaching into Tollgate's tables or
re-implementing credential and budget authority in a second service (GL-121).

## Authority and transport

Every route below admits the **operator** role from the
[control-plane security runbook](CONTROL_PLANE_SECURITY.md), and every route
but `deposit` also admits the **provisioner** role. An instance credential
authenticates the request path and cannot administer accounts: it must not
reach a customer's funding position or credential list. Anonymous callers are
refused. TLS is required outside loopback.

A self-service signup service should hold a **provisioner**, not an operator.
It can run this whole sequence except deposits, and nothing else: it cannot
fund an account, suspend or close one, grant `Assured`, set a budget above its
`max_budget_allowance`, publish an `Elastic` snapshot, or touch an account an
operator created. An operator's suspension holds against it — a customer cannot
undo an abuse suspension by retrying signup. The
[provisioner scope](HTTP_API.md#the-provisioner-scope) lists every limit;
each refusal is `403` and is audited.

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
destructive. An operator's creation assigns `CapacityClass::Assured`; step 2
changes it if the plan calls for something else. A provisioner must send
exactly this body — zero balance, `Suspended` — and its account is created
`BestEffort`, so step 2 is already done.

Create suspended and activate in step 4, so an account cannot lease before its
budget and credentials exist.

**2. Set the capacity class**, if not `Assured`. A provisioner may only set
`BestEffort`.
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
funded nothing each period. The `budget` field must be present:
`{}` is rejected as `422 invalid-json` and preserves the existing schedule.
Concurrent updates report the predecessor captured under the account lock.

**A one-off deposit is not a substitute for an allowance.**
`POST /v1/admin/accounts/{account}/deposit` adds units once; a schedule funds
the account at every period boundary and expires what the previous period did
not spend. Setting a schedule deposits nothing by itself — the first funding
arrives at the next boundary — so an account that must be usable immediately
needs both.

**4. Activate.** `POST /v1/admin/accounts/{account}/status` with
`{"status": "Active"}`. Idempotent. `Closed` is terminal: no transition leaves
it, and the request is refused with `409 account-closed`. A provisioner's
activation is refused with `403 operator-hold` when an operator set the
account's current status — typically an abuse suspension — and only an
operator can lift it.

**5. Issue a credential.** See below. Do this last: a credential that exists
before the account is active authenticates into denials.

**6. Bind its policy.** `PUT /v1/admin/accounts/{account}/keys/{key}/snapshot`
publishes the snapshot the credential authorizes against. See
[binding a policy](#binding-a-policy). Until this step the credential
authenticates, but no snapshot authorizes it.

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

**`secret` is the credential exactly as it is presented**: 64 lowercase
hexadecimal characters, which the owner sends verbatim — for example as
`Authorization: Bearer <secret>`. Do not decode it. The stored digest covers
these characters, and a verifier is handed the same bytes.

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
`limit` must be between 1 and 4096. Malformed cursors or query values return
`400 invalid-query`; zero or oversized limits return `422 invalid-limit`.
These are client errors and do not trigger a store read or backend diagnostic.

### Revoking

`DELETE /v1/admin/accounts/{account}/keys/{key}` → `{"key_id": "…",
"retired": true}`.

`retired: false` means it was already revoked. That is a success, not an error —
your intent is satisfied — but the distinction is what an audit needs.
Audit resources identify `{account}/keys/{key}`. Confirmed issuance records
`Absent` to `Credential { account_id, key_id, revoked: false }`; first retirement
changes `revoked` to `true`, and repeated retirement records equal states. These
receipts come from the store's mutation lock, not a separate HTTP read. An
unrevoked credential may still be expired. No secret, digest or principal is
included in the audit receipt.

**Revocation is bound to the account in the path.** A `key_id` belonging to
another account answers `404 unknown-credential` and revokes nothing. You
cannot retire another customer's credential by guessing or mistyping an id.

**Revoking a key is two calls.** Revocation retires the credential but leaves
its bound snapshot in place. That is safe for authorization, because a revoked
credential leaves the key projection and fails authentication. It is still a stale
positive grant, so follow every revocation with
`DELETE /v1/admin/accounts/{account}/keys/{key}/snapshot`.

Revocation is terminal and generation-ordered. It is **not** instant across a
fleet: serving instances hold a projection and converge at their next refresh.
See the [credential projection](CREDENTIAL_PROJECTION.md) runbook for that
window. Treat revocation as "no new sessions promptly", not "every in-flight
request stops now".

### Binding a policy

`PUT /v1/admin/accounts/{account}/keys/{key}/snapshot` with
`{"snapshot": { … }}` → `204`. `DELETE` on the same path withdraws it → `204`.

You name the credential by the handles you already hold. The server resolves
its principal from its own key record, and the principal never appears in a
request, response or audit. The principal routes
(`PUT`/`DELETE /v1/admin/snapshots/{principal}`) remain for embedders that
derive principals themselves.

- **Bound to the key in the path.** Leave the snapshot's `key_id` unset and the
  server fills it in, or state the path's key. A snapshot that names a different
  key, or another account, answers `422 invalid-credential-binding` and
  publishes nothing.
- **Bound to the account in the path.** Another account's `key_id` answers
  `404 unknown-credential`, as revocation does.
- **The ledger owns status and capacity class.** A snapshot must carry the
  account's current values (`409 snapshot-status-mismatch`,
  `409 snapshot-capacity-class-mismatch`). The server stamps the budget.
- **Retired credentials are never granted a policy again.** `PUT` on a revoked
  key answers `409 credential-retired`. `DELETE` still works, and is how you
  finish revoking it. An expired but unrevoked key may still be published.
- **Generations only move forward.** A publish at or below the stored generation
  answers `204` and changes nothing: the audit receipt records equal
  `before` and `after`. After a withdrawal, only a strictly higher generation
  republishes. Increase the generation on every policy change.

Audit resources identify `{account}/keys/{key}/snapshot`, with actions
`publish_key_snapshot` and `remove_key_snapshot`. Receipts record the snapshot's
generation and whether it is withdrawn, and never a principal.

## Reading an account

`GET /v1/admin/accounts/{account}`

```json
{ "account_id": "…", "as_of": "…", "status": "Active",
  "capacity_class": "Assured", "origin": "Operator", "status_set_by": "Operator",
  "budget": { … }, "period_start": "…",
  "balance": 600, "outstanding_lease_grants": 400, "settled_usage": 0,
  "expired_allowance": 0, "settlement_loss": 0,
  "deposited": 1000, "overage_recorded": 0 }
```

`origin` names the authority that created the account and `status_set_by` the
one that set its current status, each `Operator` or `Provisioner`. They are
what a provisioner's scope and an operator hold are decided from.

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
| `invalid-json` | 422 | malformed body or missing required budget field | no — fix the body |
| `invalid-query` | 400 | malformed or unsupported query | no — fix the query |
| `invalid-limit` | 422 | credential page limit outside 1–4096 | no — fix the limit |
| `unknown-account` | 404 | no such account | no — fix the id |
| `account-exists` | 409 | already created | no — you are done |
| `account-closed` | 409 | terminal status | no |
| `credential-exists` | 409 | this `key_id` is recorded | no — your call worked |
| `active-key-limit` | 409 | at the supplied bound | no — revoke first |
| `unknown-credential` | 404 | not this account's credential | no |
| `credential-retired` | 409 | a revoked credential cannot be bound to a policy | no — issue a new one |
| `invalid-credential-binding` | 422 | the snapshot names another key or account | no — fix the body |
| `invalid-snapshot-limits` | 422 | the snapshot's limits cannot be enforced | no — fix the body |
| `snapshot-status-mismatch` | 409 | the snapshot's status disagrees with the ledger | no — use the status route |
| `snapshot-capacity-class-mismatch` | 409 | the snapshot's class disagrees with the ledger | no — use the class route |
| `issuance-unsupported` | 501 | this deployment does not issue | no — see below |
| `entropy-unavailable` | 503 | no credential entropy | yes, with backoff |
| `issuer-misconfigured` | 500 | an embedder's issuer minted a secret that is not presentable text; nothing was stored | no — fix the issuer |
| `storage` | 503 | backend unavailable | yes, with backoff |

State conflicts return 409: those requests are well-formed. Invalid bodies and
limits return 422; malformed queries return 400. Secrets and digest material never appear in an
error body or a log line.

## Deployment

**Issuance requires a configured credential issuer.** The `tollgate-server`
binary issues only when its security manifest names one:
`"issuer": {"secret_file": "issuer.secret"}`. See
[credential issuer](CONTROL_PLANE_SECURITY.md#credential-issuer) for the format,
validation, reload and rotation rules. Without it, issuance answers
`501 issuance-unsupported`. That default is deliberate: an instance that only
verifies should not hold the capability to create credentials. The registry the
server builds for control-plane bearers is never used for issuance, because its
secret is regenerated at every start — the wrong authority and the wrong
lifetime for a credential a customer keeps. Embedders that build their own
server still pass any `CredentialIssuer` through `ServerState::issuer`, for
example an HSM- or KMS-backed one. **Every other route on this page works
without an issuer.**

**Credentials are digested as presented.** Releases before this one disclosed
the secret as hex but digested the 32 bytes it encodes, so a credential they
issued verified only for a caller that hex-decoded the presented value. A
verifier that forwards the presented text, like the reference embedder, refused
every such credential. Credentials issued now verify as presented. Re-issue any
credential minted by an earlier `tollgate-server` and revoke the old one.

**Migration `0019`** adds `(account_id, key_id)` to the credential table for
the account-scoped listing. Additive and forward-only: no data change, safe to
apply before the code that uses it, and harmless if the deployment is rolled
back.

**`Backend` now requires `KeyDirectory`.** A server administers credentials as
well as projecting them. `HttpStore` implements `KeySource` but not
`KeyDirectory` — correctly, since it is a client of a server rather than the
authority behind one — so it cannot back a server. No in-tree backend is
affected.

**`KeyDirectory` requires `publish_key_snapshot` and `remove_key_snapshot`.**
Key-bound binding must resolve the principal, check retirement and publish in
one indivisible step, so a backend implements it rather than a caller composing
reads. This is breaking for implementations outside this repository;
`MemoryStore` and `PostgresStore` implement both. No migration: the lookup uses
the `(account_id, key_id)` index from `0019`.

## Conformance

A consumer adapter should pin an exact Tollgate release tag and verify, against
that tag, that:

1. repeating each provisioning step changes nothing and reports the repeat;
2. a lost issuance response resent with the same `key_id` answers `409` and
   discloses no secret;
3. a listing contains no `secret`, `digest` or `principal` field;
4. revoking another account's `key_id` answers `404` and leaves it live;
5. `settled_usage` stays zero while a lease is outstanding, and the funding
   equation holds across the grant;
6. a policy binds by `(account_id, key_id)` without the caller handling a
   principal; another account's key answers `404`; a revoked key answers
   `409 credential-retired`, and revoking a key is followed by withdrawing its
   policy.

`crates/tollgate-server/tests/api.rs` exercises 1–6 against the in-process
router, with the issuer built through the same manifest path the binary uses.
It is the executable reference for the expected status codes and bodies.
`crates/tollgate-server/tests/backend_features.rs` runs issuance, verification,
binding and revocation against the stock binary.
