# Customer credential projection

An HTTP-backed instance obtains active customer credential digests through
`KeySource`, implemented by `HttpStore`. It needs neither a database
connection nor `KeyDirectory` or `AdminStore` mutation authority. Direct memory
and PostgreSQL backends implement the same read trait through `KeyDirectory`.
Account policy and authorization remain in the separate snapshot feed.

## Endpoint and deployment

`GET /v1/keys` requires the **instance** role described in the
[control-plane security runbook](CONTROL_PLANE_SECURITY.md). Operator authority
does not grant this read. TLS is required outside loopback. The server selects
active, non-revoked credentials using its own clock on every page. The only
query fields are `after` (exclusive, 32 lowercase hexadecimal key ID) and
`limit` (1–4096, default 256). Malformed or unknown fields return
`400 invalid-query`; zero or excessive limits return `422 invalid-limit`.
Clients cannot choose a historical activity time.

Each HTTP 200 response is a page:

```json
{"revision":12,"as_of":"2026-09-09T12:00:00Z","keys":[],"next_after":null}
```

Records contain `key_id`, `principal` (32 lowercase hexadecimal characters),
`digest` (64 lowercase hexadecimal characters), and required `not_after`
(timestamp or explicit null). Account policy, customer secrets, and the HMAC
secret are absent. `next_after` is required even when null. A continuation is
the final returned key ID, and exists only if lookahead found another active
record. An exactly full final page is terminal. Success carries
`Cache-Control: no-store`; clients refuse partial statuses and `Content-Range`.
The derived body limit admits every maximal legitimate page, including widest
IDs and timestamps. Do not log the digest catalogue.

Records and revision come from one committed backend read. Mutations advance
the revision; a manager that observes different revisions discards its
candidate and restarts from the beginning. Expiry can narrow later pages
without changing revision, so the manager also filters accumulated records at
the later of its own clock and the latest page's server time. A revocation
after the final page's read is subject to the refresh window: this protocol
provides a coherent committed revision, not a transaction extending through
publication on the instance.

The PostgreSQL schema includes a revision table, a transactional mutation
trigger and a partial `key_id` index. Direct writers do not manage revisions;
the trigger advances them atomically, including rollback and overflow checks.
Removing the trigger or resetting the revision while readers run is unsupported.
Migration **0018** preserves exact credential expiry and requires the coordinated
upgrade below. Old expiry readers and issuers are incompatible with that schema.

`KeyDirectory` has the `KeySource` supertrait, and `Backend` requires its read
bound; custom backends implement the paged contract. `KeyDirectory::active_keys`
remains an unbounded operator read. Allocator/authentication APIs, HTTP page
fields and error codes retain their contracts in this precision correction.
Pin server/client consumers to the same tested Git tag.

## Expiry precision and upgrade

New credentials retain the complete nanosecond `not_after` timestamp through
insertion, directory reads, every page, HTTP decoding and verifier/session
evidence. The expiry is exclusive: an unchanged credential works one nanosecond
before it and refuses at the instant itself. `None` continues to mean no
individual expiry; managed projections still impose their freshness bound.
All representable timestamps, including pre-epoch values and both domain
endpoints, remain valid. Revocation's timestamp is informational: its presence
retires the credential immediately, independently of the supplied read time.
Credential activity remains a separate microsecond reporting contract.

PostgreSQL stores finite expiry as `not_after_floor_us` plus
`not_after_submicro_ns` (0–999). Both fields must be present or both absent.
Their integer pair preserves exact timestamp ordering. The existing key-ID
indexes and page bounds remain; there is no request-path change. Constraints
refuse partial or out-of-domain rows, and readers report incomplete evidence
rather than interpret it as indefinite validity.

Migration 0018 cannot reconstruct fractions discarded by legacy writers. It
uses the earliest possible expiry consistent with truncation toward zero:
positive microseconds start at their stored instant; negative and zero values
start 999 ns before it, bounded by Timestamp::MIN. This can end authority up
to 999 ns early, or 1,998 ns for the zero bucket, but cannot extend it. Finite
legacy rows carry `not_after_is_lower_bound = true`; indefinite rows remain
exact nulls, and every new insertion declares exact evidence. This marker is
durable operator evidence and is not added to the wire DTO.

This upgrade requires a maintenance window across credential sources and
consuming instances, not only the database:

1. Preserve a recoverable backup and the running configuration. Stop ingress,
   drain admitted work and usage, and stop old issuers, credential servers,
   projection managers and direct-store consumers. Follow the lease shutdown
   order for instances that also hold quota.
2. Clear all old registry projections and `SessionCredential` proofs. Restarting
   every process that owns them provides this boundary. A refresh alone is
   insufficient: an existing session retains its originally issued deadline
   even after a corrected table replaces it. An embedder doing an in-process
   upgrade must demonstrably clear every such session before resuming ingress.
3. Start a backend with migration 0018. The transaction locks the credential
   table, renames the old expiry column, backfills bounds and validates all
   rows. The existing trigger advances revision once if finite rows change;
   revision overflow or invalid history rolls back schema and data together.
   Allow for table-size-dependent backfill and lock acquisition.
4. Start compatible issuers/readers, then fresh instance projections and
   sessions. Require key-manager and runtime readiness before resuming ingress.
   Inspect the uncertainty counts below; rotate affected credentials through
   the normal durable issuance/revocation lifecycle when exact replacement
   evidence is needed. Do not clear a marker or extend a bound by guessing.

```sql
SELECT revoked_at_us IS NOT NULL AS revoked,
       not_after_is_lower_bound, count(*)
FROM tollgate_credential_keys
GROUP BY revoked, not_after_is_lower_bound;
```

The renamed column makes old expiry reads/writes fail, including on connections
opened before migration. Old insertion shapes that omit the new evidence
declaration also fail. Old revocation-only statements remain valid because
they only remove authority. Old startup refuses the unknown migration version.
These database fences cannot retroactively invalidate a proof already cached
in another process; the session reset above is required. The migration does
not alter key IDs, accounts, principals, digests, retirement or billing history.

There is no automatic downgrade to the old expiry schema. Recovery after
commit uses a compatible corrected binary or a separately reconciled restore;
dropping nanoseconds would reintroduce the defect. A failed migration retains
the old schema and history. Repair invalid timestamps only from authoritative
evidence; revision exhaustion requires a deliberate source-generation recovery
plan, never lowering the revision under live readers. Finite legacy uncertainty
remains on retired history; normal rotation replaces active credentials with
new exact records without erasing that history.

The Lean projection model proves that conservative source bounds cannot extend
authority and that reset sessions inherit them. Integer-pair order uses the
existing exact timing model. Shared backend scenarios, migration/rollback tests
and real memory/PostgreSQL HTTP-to-session tests are separate finite arithmetic,
driver, transaction and rollout witnesses; these do not prove cryptography or
fleet-wide operator execution.

## Instance lifecycle

Own a `KeyManager` beside `InstanceRuntime`. Pass the same `Arc<HttpStore>` as
the credential source and the snapshot/lease/usage backend, with a common
business clock. Set `KeyManagerConfig.refresh_interval` no longer than the snapshot refresh
interval; include the pass duration in the propagation budget. Each key attempt starts immediately at boot, then waits that interval
after the previous attempt completes. The independent tasks have no shared
publication barrier: onboarding becomes usable after **both** projections are
available and the account is funded.

Supply the HMAC secret used by the customer key issuer from the deployment's
secret store. It must contain at least 32 bytes. This secret is separate from
the instance's control-plane bearer identity and is never fetched through this
endpoint. The manager exposes only `KeyVerifier`, which implements
`CredentialVerifier`; it cannot mint keys or install an unbounded table. Use it
with `SessionCredential::authenticate(credential, &verifier, now)` before staged
admission. Keep session state scoped to the same authenticated connection or
session as before. Every request still checks current snapshot authorization.

For readiness, require both
`runtime.handle().readiness(now).is_ready()` and
`keys.monitor().report(now).ready`. Retain cloned handles/monitors for the
application. Key readiness means a live task with a fresh complete projection;
an authoritative empty catalogue is healthy even though no customer can
authenticate. Report `health`, `stats`, `revision`, `projected_keys`, `fetched_at`, and
`usable_until` without customer labels. `projected_keys` is the installed entry
count, not the number of customers currently authorized or individually
unexpired. A failed attempt is `Degraded` while the previous projection is
fresh; readiness becomes false at its exact exclusive deadline. Task death is
`Failed` and withdraws readiness immediately when observed.

Stop ingress and request admission-runtime shutdown to drain committed usage
and release leases in its supported order. Shut down the key manager as part of
the same application shutdown, either concurrently or after that drain; it
owns no leases or usage. Include its `shutdown_timeout` in the application's
total budget. Shutdown cancels a pending credential fetch and reports failures
and deadline expiry. Dropping either the manager or its unpolled shutdown
future aborts its owned task. Task teardown withdraws the published verifier
table; previously issued cached proofs retain their original expiry. Tokio
timeouts and aborts require cooperative tasks and do not preempt synchronous
CPU work or a blocking custom source.

## Freshness, rotation and outages

Defaults are a five-second refresh pause, five-second per-call timeout,
ten-second pass timeout, 256 records per page, 1024 page calls per pass,
30-second `max_age`, and five-second shutdown timeout. Page calls spent on
revision restarts count against the same budget. Hitting either budget
publishes nothing and reports degradation; increase the budget deliberately
for a larger catalogue. No partial table becomes a successful result.

Durations must be positive and representable, `fetch_timeout ≤ pass_timeout`,
and `max_age` must strictly exceed `refresh_interval + 2 * pass_timeout`.
The prior drain, pause, and next drain must fit inside a window measured from
the prior drain's start. The deadline must also fit in `Timestamp`. Configure
synchronized clocks across the fleet; session expiry uses caller-supplied time.

Every successful projection gives a credential evidence valid until the earlier
of its own `not_after` and **fetch start + max_age**. Network time consumes this
window. A response already past it cannot publish. Refresh failure retains the
previous table and its original deadline; it cannot renew authority. Thus a
feed outage denies authentication once the last successful window expires,
including on warm sessions. The request path does no fetching and reads no
clock itself: `SessionCredential` enforces the bound using caller-supplied time.

Key removal or rotation replaces the table on the next successful refresh,
immediately affecting new verifications. An already cached session can continue
through its original evidence deadline (at most `max_age` from that fetch's
start), unless snapshot withdrawal rejects it earlier. Refreshing other keys
does not renew that proof. This is bounded revocation, not instant cache
eviction. Do not expose authenticated traffic when only one readiness component
is healthy. Tune the freshness and snapshot windows to the revocation tolerance.

Customer key rotation can overlap old and new records; new principals also
need published snapshots. The HMAC secret itself is fixed for a manager's
lifetime: coordinated issuer/verifier replacement is required to change it.
The API does not introduce a multi-secret rotation scheme. Control-plane
service-account or bearer rotation remains independent and uses `HttpStore`'s
existing transport replacement.

Each pass has bounded page calls, transport bytes per call and elapsed time.
A completed refresh still materializes the whole active set, O(active keys)
memory, temporarily retaining the prior table. MemoryStore scans an ordered
unrevoked-key index with hash lookups; PostgreSQL uses a partial key-ID index.
Expired-but-unrevoked candidates may still be scanned. Paging removes retired
history and sorting from each page, not the need to inspect expiry candidates.
No request-path catalogue scan is added. A cache miss adds one immutable
projection load before HMAC lookup. Warm sessions keep the existing cache path
with an explicit finite expiry check; renewal allocates the new cached proof.

Operator key issuance and revocation over HTTP remain outside this API. Direct
issuers persist minted records before disclosure and project afterwards. The
pricing example uses `digest_credential` to import its public `demo-key-N`
fixtures into MemoryStore before starting this same manager; its asynchronous
builders ensure bootstrap completes before background reads start.

Credential activity is a separate projection of accepted committed usage.
Publishers set a matching `AccountSnapshot.key_id`; operator readers distinguish
unknown keys, missing observations and `last_committed_at`. An absent timestamp
never proves non-use. The usage writer reports attribution gaps and unavailable
support. See [Credential activity](CREDENTIAL_ACTIVITY.md) for coverage, atomicity,
operator reads and rollout. Activity never changes this feed's revision or
liveness decisions.
