# Control-plane security

Usage ingestion also derives non-authoritative credential activity from accepted
events. Its instance role and secured transport are unchanged. A publisher's
optional key ID must match the snapshot principal/account or publication returns
422 `invalid-credential-binding`. Unknown attribution at ingestion preserves the
bill and is reported; activity grants no lifecycle or authorization capability.
See [Credential activity](CREDENTIAL_ACTIVITY.md) for coverage and mixed-version
reporting. There is no additional activity HTTP endpoint.

`tollgate-server` authenticates every lease, snapshot, credential, usage, and administrative
route. `/livez` and `/readyz` accept unauthenticated probes. Readiness checks
storage reachability and the server-owned maintenance task's latest reclaim and
rollover outcomes. It does not certify that every configured identity is usable.
Monitor security reload warnings separately.
The independently owned reloader reports unexpected task exit at `error` with
`operation=security-reload` and `reason=unexpected-exit`; restart its owner to
restore refresh. The last valid configuration and signing-key expiry retain
their existing authority. Deliberately dropping the owner is not a failure alert.

## Server configuration

Set `TOLLGATE_SECURITY_CONFIG` to a JSON manifest. There is no default credential,
including on loopback. Relative paths resolve against the manifest's directory.
Unknown fields, duplicate credential mappings, malformed credentials, and invalid
TLS material reject the initial configuration. Failed reloads preserve the last
valid generation.

```json
{
  "tls": {
    "certificate": "server-chain.pem",
    "private_key": "server-key.pem",
    "client_ca": "instance-ca.pem"
  },
  "bearers": [
    {"identity": "deployment-operator", "role": "operator", "token_file": "operator.token"}
  ],
  "certificates": [
    {"identity": "instance-service", "role": "instance", "certificate": "instance-leaf.pem"}
  ],
  "google": {
    "audience": "https://tollgate.example.net",
    "subjects": [
      {"subject": "123456789012345678901", "identity": "ferro-risk", "role": "instance"}
    ]
  }
}
```

The subject above is illustrative; replace it with the service account's numeric
unique ID. `google`, `tls`, and `client_ca` are optional; `bearers` and
`certificates` default to empty lists. An empty role map intentionally denies all
protected operations. Keep the TLS block when withdrawing all identities from
an encrypted listener.

Use separate instance and operator identities. Roles are disjoint:

| Role | Routes under `/v1` |
| --- | --- |
| `instance` | `POST /leases/{acquire,release,consolidate,reclaim}`, `GET /snapshots`, `GET /snapshots/{principal}`, `GET /keys`, `POST /usage/ingest` |
| `operator` | `POST /admin/accounts`, `POST /admin/accounts/{id}/{deposit,status,capacity-class}`, `PUT /admin/snapshots/{principal}`, `DELETE /admin/snapshots/{principal}` |

`GET /keys` serves customer credential digests from the store's `KeyDirectory`.
That key space is separate from this server's control-plane bearer credentials,
which are loaded from the security manifest into their own HmacRegistry. The
route sends neither customer secrets nor the HMAC secret already held by a
customer verifier. See [credential projection](CREDENTIAL_PROJECTION.md) for
paging, clock, freshness and rollout contracts.

Static credential files contain 32–16,377 visible ASCII bytes; a trailing newline
is accepted. Generate at least 256 random bits and store them using a secret
manager or protected mounted file. Only HMAC digests remain in the loaded static
verifier. Identities are non-secret audit names, limited to 128 ASCII identifier
characters. Raw tokens, authorization headers, private keys, and JWT bodies are
not logged.

The server accepts a single `Authorization: Bearer …` header, bounded to 16 KiB.
Bearer verification uses `tollgate-auth::CredentialVerifier`. The configured role
map authorizes the verified principal. Invalid, missing, expired, or conflicting
credentials return RFC-7807 `401 authentication-required` with a Bearer challenge;
valid identities without the required role return `403 scope-forbidden`. These
checks precede path/body decoding and store calls. Existing JSON errors and wire
DTOs retain their meanings.

TLS uses rustls with safe protocol defaults. `TOLLGATE_BIND` defaults to
`127.0.0.1:8080`; a non-loopback bound address requires TLS and fails startup
without it. Network restriction alone does not permit remote plaintext. The
library `serve` function enforces the same rule. Embedders must use this listener
entry point; the standalone `router` is useful for in-process tests and cannot
validate an external listener it does not own. Its `/readyz` returns 503 because
it has no maintenance owner; `/livez` and authenticated API handlers still work.

An optional client CA requests and verifies client certificates while allowing
bearer-only callers and probes. A trusted certificate also needs an exact leaf
SHA-256 fingerprint mapping to gain authority; the manifest derives that
fingerprint from the configured leaf PEM. HTTP headers such as
`X-Forwarded-Client-Cert` never create an identity. If bearer and certificate
credentials are both supplied, they must resolve to the same name and role.

TLS handshakes run concurrently, with at most 128 pending tasks and a five-second
deadline each. Excess connections wait in the OS backlog. Library embedders may
set these bounds with `TlsConfig::with_handshake_limits`; the binary uses the
defaults. Dropping the listener aborts pending handshakes. SIGTERM and Ctrl-C
start graceful HTTP shutdown; cancelling the server also aborts its maintenance
task. This does not introduce a total deadline for all server HTTP requests.
The caller's shutdown future remains owned by `serve`. Cancellation or task
failure also releases its internal signal waiter and tells existing connections
to shut down.

## Maintenance readiness and recovery

`serve` starts maintenance immediately. `/readyz` stays 503 until both the reclaim
and budget-rollover passes reach a partial batch successfully, and returns 200
only while their latest outcomes are successful, the task is alive and the store
answers `ping`. A failed pass withdraws readiness immediately, even if the other
pass succeeds. Successful recovery of the failed operation restores its own
health; `/livez` remains 200 while the service can run. The handlers continue to
return their normal domain results while maintenance retries on its existing
cadence. No partial batch or failed call is treated as proof of rollback.

Each operation logs its first two consecutive failures at `warn`, the third and
subsequent failures at `error`. Monitor `operation` (`reclaim` or `budget-rollover`)
and `consecutive_failures`; one `info` event reports `after_failures` on recovery.
The three-attempt threshold is an alerting policy, not proof that a particular
lease TTL has expired. Inspect backend permissions, connectivity and transaction
health; successful ping alone does not prove that maintenance writes can run.
Completed progress fields survive a later failed batch. No backend details are
formatted into these events.

An unexpected maintenance return, cancellation or unwinding panic emits an
`error` with `operation=maintenance` and a static `reason`, then `serve` returns
an I/O error and stops listening. Process supervision should restart it. A build
using `panic=abort` terminates the process immediately on panic; it cannot emit
the unwind supervisor's event. Graceful shutdown withdraws readiness before
cancelling maintenance, and expected cancellation is not a task-failure alert.
Pending store calls may have committed effects even when cancelled; reconcile
with backend records rather than inferring rollback from shutdown.

This health policy observes completed outcomes and task liveness. It adds no
timeout or cancellation deadline to a backend call that remains pending, and it
does not certify that `SKIP LOCKED` left no rows with another replica. Backend
futures must yield to the executor for task supervision and cancellation to run.

There are no Rust signature, database or wire-schema changes. Readiness is
intentionally stricter: deployments should allow initial maintenance to complete
before routing traffic. Embedders using the bare router for a listener must move
to `serve`; their probe now stays 503 instead of claiming health without an
owned worker. Existing `TOLLGATE_RECLAIM_INTERVAL_SECS` retains its scheduling
meaning. Zero or a duration that cannot fit the monotonic clock is rejected
before starting tasks. Rolling back restores the old false-ready behavior.

## Instance shutdown accounting

The instance runtime has the same allocation boundary. A successful
`InstanceRuntime::shutdown` can leave a grant whose acquire or consolidation
result never reached the manager. Its known leases can all be released while
that unanswered capability still holds units in the backend. Retain a
`RuntimeHandle` and inspect `report().uncertain_acquires` and
`account_reports(now)` after the join, alongside the shutdown report's abandoned
leases, task failures and usage-drain counters. Uncertainty is a count of
possible grants, not a unit amount or proof that each call committed. Such units
remain in active grants until server maintenance reclaims them after expiry and
grace; do not credit them manually or classify the liquidity difference alone
as lost billing. Reconcile recorded usage and ledger conservation both before
and after reclamation. Expiry does not erase the runtime's historical counters.

## Instance clients and Cloud Run

`HttpStore::with_config` validates its URL, credentials, TLS roots and deadlines
before construction. HTTPS verifies the server certificate and host name. A
custom `root_ca_pem` replaces public trust roots; `identity_pem` contains the
client certificate chain followed by its private key. `StaticBearer` supplies a
rotatable token. `HttpStoreConfig` defaults to a two-second connection deadline
and a ten-second total request deadline, including credential retrieval and
response reading. Redirects and environment proxies are disabled. Plain HTTP
is accepted only for literal loopback addresses or `localhost`, which is pinned
to loopback rather than resolved through DNS. URL userinfo, queries and fragments
are refused.

Cloud Run instances can use their attached service account without distributing
per-replica secrets:

```rust,ignore
use tollgate_client::{GoogleIdentity, HttpStore, HttpStoreConfig};

let identity = GoogleIdentity::new("https://tollgate.example.net")?;
let store = HttpStore::with_config("https://tollgate.example.net", HttpStoreConfig {
    bearer: Some(identity),
    ..Default::default()
})?;
```

The provider obtains a Google ID token from the fixed metadata identity endpoint
with the configured audience and `Metadata-Flavor: Google`. It caches tokens for
60 seconds and bounds metadata requests to five seconds and 16 KiB. The server
accepts only RS256 signatures under Google's published keys, Google issuer names,
the exact configured audience, a nonempty subject, and unexpired verified evidence.
Authorization uses `sub`, not a mutable email claim. This is service-account
identity support; customer login, arbitrary OIDC issuers and user directories
are outside this API. Replicas sharing a service account share one identity and
revocation scope.

The server fetches Google signing keys off the handler path. Their usability is
bounded by the issuer's `Cache-Control`/`Age` policy and at most one hour after
fetch; absent cache metadata defaults to five minutes. Refresh is attempted at
half the remaining lifetime, between five seconds and five minutes. Fetch or
validation failures retain the preceding keys with their original expiry and
never extend identity validity. Expired or unknown keys fail closed until a
valid refresh succeeds. Internet access to Google's signing-key endpoint and
accurate server time are deployment prerequisites for this mode.

For the intended split, the Cloud Run client in `the client project` connects over TLS
to a directly encrypted server endpoint in `the server project`, with PostgreSQL
behind the server. A VM or GKE deployment can expose this TLS listener. Cloud Run's
usual server-side HTTP container termination is not an exemption from the
non-loopback plaintext rule; this feature does not provide a forwarded-identity
or trusted-proxy bypass.

References: [Google service-to-service identity](https://docs.cloud.google.com/run/docs/authenticating/service-to-service),
[Google ID token validation and key caching](https://developers.google.com/identity/openid-connect/openid-connect).

## Rotation and revocation

1. Write new versioned credential/certificate files and validate them. For CA
   changes, allow both old and new CAs and certificate identities during overlap.
2. Atomically replace the manifest only after all referenced files exist. The
   owned `SecurityReloader` checks every five seconds, stages the complete set,
   then publishes verification, role mapping and TLS as one immutable generation.
3. Rotate client bearers with `StaticBearer::replace`, or roots, mTLS identity and
   bearer provider together with `HttpStore::reconfigure`. Existing managers keep
   the same `Arc<HttpStore>`; new calls use the replacement. Invalid replacements
   preserve the working transport. Embedders own client file watching.
4. Remove retired mappings/roots after clients have moved. Monitor rejected calls
   and reload warnings. `StaticBearer::revoke` prevents subsequent client calls
   from silently becoming anonymous.

New requests, including those on keep-alive connections, use the current role
map and recheck a presented certificate against the current CA and verification
time. Requests already authorized retain their pinned generation. New TLS
handshakes use the configuration current at connection acceptance. Existing TLS
connections do not renegotiate the server certificate; clients needing immediate
server-trust withdrawal must replace their transport. Enabling or disabling TLS
requires a listener restart, so a reload cannot expose an encrypted service as
plaintext.

Usage ingestion treats authentication failures as retryable. Rotation can pause
billing delivery but must not turn a valid usage batch into a terminal refusal.
Existing writer buffering, backpressure and shutdown bounds still apply; a long
credential outage eventually denies admissions rather than losing usage silently.

## Backend failures and diagnostics

Backend errors carry arbitrary text. The server does not expose that text in
HTTP bodies, `ApiError` debug output or its backend-failure logs. Public storage
failures use `503 / storage / backend unavailable`; permanent usage refusals use
`422 / usage-refused / usage batch refused`. Existing domain codes, generation
responses and retry decisions are preserved. Credential-page failures retain
their existing `credential-source-unavailable` code and title.

HTTP problem responses with 5xx or `usage-refused` add an optional `error_id` containing 32
lowercase hexadecimal digits. Find the matching warning on `tollgate::diagnostics`
to identify the route template, status and code. The server generates this ID;
request headers cannot choose it. It is diagnostic context, not authorization
evidence. If system entropy is unavailable, the failure keeps its original
status, omits the ID and logs `error_id_unavailable=true`.

Retain this target at `warn` or above, for example with
`RUST_LOG=info,tollgate::diagnostics=warn`. Library embedders own subscriber
installation and log delivery. Correlation is limited by that delivery; it is
not a durable record across process death. `Problem`'s public Rust shape is
unchanged, and existing clients ignore the additive JSON field. HTTP consumers
that need the correlation ID can read `error_id` from the response object.

Readiness retains its empty 200/503 response and logs failures by operation.
Maintenance logs retain static operation codes and completed
progress counters; PostgreSQL startup logs identify initialization failure and
the configuration to check. Neither connection strings nor driver text are
logged. Inspect connectivity, backend health, migration status and appropriately
protected backend operational records using the incident's time and operation.
An error does not prove rollback: administrative audit receipts remain the
authority for confirmed writes, and ambiguous failures need reconciliation.

This change requires no schema or configuration migration and preserves the
public Rust error types. Deploying the server updates the public titles and adds
the optional field. Consumers must classify errors by status/code, not by parsing
the old backend-specific title. A rollback to an older server restores the
disclosure defect.

## Administrative audit

Every HTTP administrative operation reaching the store emits structured
`tollgate::audit` events with a random operation ID, stable actor, action, resource and server time.
`started` precedes the store call. `confirmed` carries the backend's typed
`AdminReceipt`: before/after values captured under the memory lock or inside the
PostgreSQL transaction that serialized the mutation. It never substitutes a
separate read that could describe somebody else's concurrent write.

Receipts identify changed fields: creation balance/status/class, deposited and
top-up totals, status, capacity class, or snapshot generation plus revocation
state. The resource identifies the account or principal. Snapshot receipts name
the immutable publication generation; they do not copy its full policy graph.
No-op operations report equal states. `failed` carries a stable code/status;
`cancelled_unknown` marks an interrupted operation. Neither invents a before/after
pair or asserts that a storage error ruled out a commit. Unmatched `started`
events after a process crash also require reconciliation.

The binary keeps audit events enabled even with `RUST_LOG=error`; library
embedders must install a subscriber that retains this target. Route these events
to the deployment's retained, access-controlled audit log and alert on delivery
failures. This is structured audit logging, not a durable transaction outbox:
process death or logging infrastructure failure can lose delivery after a commit.
No database migration or audit table is introduced. Direct `AdminStore` callers
receive receipts but remain responsible for attaching identity and persisting
their own audit record.

## Rollout and local development

This is an intentional Rust/configuration API break in unpublished crates:
`ServerState` requires security, `HttpStore` constructors return `Result`, and six
HTTP-facing `AdminStore` mutations return `AdminReceipt<T>` (read `.outcome` for
the previous result). Custom backends must capture receipts at their serialization
point. Wire payloads, existing domain error codes, and database schemas are
unchanged; 401/403 authentication codes are additive.

Bring up a secure endpoint against the existing PostgreSQL backend, configure
instance identity and TLS, then deploy clients using the new configuration. Keep
overlapping identities during rotation and remove the old endpoint once clients
have moved. Rollback must preserve an authenticated TLS endpoint; reverting to
an older unauthenticated server on a public listener is not a compatible rollback.
No schema rollback is required.

For a disposable loopback demonstration only:

```sh
mkdir -m 700 .local-control
printf '%s\n' 'demo-only-operator-token-do-not-deploy-98' > .local-control/operator.token
printf '%s\n' '{"bearers":[{"identity":"demo-operator","role":"operator","token_file":"operator.token"}]}' > .local-control/security.json
chmod 600 .local-control/*
TOLLGATE_SECURITY_CONFIG="$PWD/.local-control/security.json" cargo run -p tollgate-server
```

This uses the ephemeral memory backend and grants no instance role. Supply an
independent instance credential to exercise `HttpStore`, or run the generated
TLS/bearer/mTLS fixtures in `cargo test -p tollgate-server`. Keep local security
material out of version control. `--help` and `--version` work without secrets or
database configuration; `--` ends option processing.
