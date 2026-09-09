# Control-plane security

`tollgate-server` authenticates every lease, snapshot, credential, usage, and administrative
route. `/livez` and `/readyz` accept unauthenticated probes. Readiness checks
storage reachability; it does not certify that every configured identity is
usable. Monitor security reload warnings separately.

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
validate an external listener it does not own.

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
