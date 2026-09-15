//! Credential verification for latency-critical services.
//!
//! Tollgate's admission path costs about 107 ns. Verifying a credential to
//! reach it costs 800 ns to 2 µs — seven to twenty times everything the rest
//! of the stack does. A library that tunes the 107 ns while leaving the 2 µs
//! to each embedder is tuning the wrong end, so this crate owns the
//! credential step too.
//!
//! It is split the way the problem is:
//!
//! - **[`CredentialVerifier`]** is the seam. Credential *schemes* differ per
//!   deployment — API-key digests, PASETO, JWT, client certificates — and this
//!   crate does not pick one for you.
//! - **[`HmacRegistry`]** is the scheme in the box: server-issued API keys as
//!   HMAC-SHA256 digests, digests at rest, constant-time comparison.
//! - **[`SessionCredential`]** is the part worth centralising whichever scheme
//!   you use: in steady state it verifies once per session and compares
//!   thereafter, taking the per-request cost to ~16 ns. (Requests racing on a
//!   cold session may each verify; that costs the optimisation, never
//!   correctness.)
//!
//! The reason that split matters is drift. Getting the cache right means
//! getting an invalidation *ordering* right — clear the old proof before
//! verifying its replacement, never cache a failure, compare the exact bytes
//! that were verified. Left to each embedder, that is a security convention
//! upheld by caller discipline at many call sites, and those drift. Here it is
//! one implementation with its own tests and its own invariant.
//!
//! # A cached credential proves identity, never authorization
//!
//! A cache hit skips the credential check and **nothing else**. The caller
//! still runs admission against the current snapshot, so status, staleness,
//! permissions, rate and quota are decided fresh on every request. Revocation
//! therefore stays bounded by snapshot refresh exactly as it is without any
//! cache — including on a long-lived session that authenticated before the
//! revocation landed.
//!
//! An answer is also bounded by whatever validity the verifier attached to it,
//! so an expiring scheme cannot outlive its own `exp` just because the session
//! stayed open. [`HmacRegistry::install`] preserves each key's `not_after`;
//! convenience `install_credentials` attaches no expiry. A distributed
//! projection can further bound evidence by feed freshness, as the client's
//! `KeyManager` does. Removing a registry entry affects new verification;
//! cached evidence retains its original bound, with snapshot withdrawal still
//! checked on every request.
//!
//! ```
//! use jiff::Timestamp;
//! use tollgate_auth::{HmacRegistry, SessionCredential};
//!
//! let registry = HmacRegistry::new(b"secret-from-the-environment");
//! let issued = registry.install_credentials([b"demo-key-1".as_slice()])[0];
//!
//! // One session — a connection, a TLS session, whatever the transport calls it.
//! let session = SessionCredential::new();
//! // `now` comes from the caller: the request path does not read clocks.
//! let now = Timestamp::from_second(1_755_600_000).unwrap();
//!
//! // First request on it verifies; the rest compare.
//! assert_eq!(session.authenticate(Some(b"demo-key-1"), &registry, now), Some(issued));
//! assert_eq!(session.authenticate(Some(b"demo-key-1"), &registry, now), Some(issued));
//!
//! // A different credential re-verifies, and a bad one leaves nothing behind.
//! assert_eq!(session.authenticate(Some(b"wrong"), &registry, now), None);
//! assert!(!session.is_authenticated());
//! ```

mod hmac_registry;
mod session;
mod verifier;

pub use hmac_registry::HmacRegistry;
pub use session::SessionCredential;
pub use verifier::{CredentialIssuer, CredentialVerifier, Verified};
