# tollgate-auth

Credential verification for latency-critical services, from
[Tollgate](https://github.com/MorphIQ-Labs/tollgate): credential digests at
rest, and a session-scoped cache that takes the per-request digest off the
hot path without widening the revocation window.

On the development host (Apple M1 Pro), Tollgate's whole admission path
measures about 107 ns, while verifying a credential to reach it measures 800 ns
to 2 µs. Absolute values are host-dependent; the ratio is the point. This crate
owns the expensive step too, because getting its cache right is a security
convention that drifts when every embedder writes it again.

- `CredentialVerifier` is the seam: API-key digests, PASETO, JWT, or client
  certificates all fit behind it.
- `HmacRegistry` is the scheme in the box: server-issued API keys stored as
  HMAC-SHA256 digests, compared in constant time.
- `SessionCredential` verifies once per session and compares thereafter
  (about 16 ns per request on the same host). It clears a stale proof before
  verifying a replacement, never caches a failure, and compares the exact bytes
  that were verified.

A cached credential proves identity, never authorization: admission still
checks status, staleness, permissions, rate, and quota against the current
snapshot on every request, so revocation stays bounded by snapshot refresh.

## Issuing and verifying a key

```rust
use jiff::Timestamp;
use tollgate_auth::{HmacRegistry, SessionCredential};
use tollgate_core::KeyId;

// The server secret comes from the environment or a secret store.
let registry = HmacRegistry::new(b"server-secret-from-the-environment");

// Mint a credential: the secret is shown to its holder once; the digest is
// what a key directory stores.
let key = registry.mint(KeyId(1)).expect("system entropy");
registry.install([(key.principal, key.digest, None)]);

let session = SessionCredential::new(); // one per connection or TLS session
let now = Timestamp::from_second(1_755_600_000).unwrap();

// The first request verifies; later requests on the session compare.
assert_eq!(
    session.authenticate(Some(key.secret.as_slice()), &registry, now),
    Some(key.principal)
);
assert_eq!(
    session.authenticate(Some(key.secret.as_slice()), &registry, now),
    Some(key.principal)
);

// A wrong credential fails and leaves no proof behind.
assert_eq!(session.authenticate(Some(b"not-a-key"), &registry, now), None);
assert!(!session.is_authenticated());
```

## Contract

The cache's ordering rules and revocation bound are invariants in
[`INVARIANTS.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/INVARIANTS.md);
[`docs/CREDENTIAL_PROJECTION.md`](https://github.com/MorphIQ-Labs/tollgate/blob/main/docs/CREDENTIAL_PROJECTION.md)
covers distributing key sets to instances.

## License

MIT OR Apache-2.0, at your option. Tollgate is a product of MorphIQ Labs, a
trade name of Prophetizo LLC.
