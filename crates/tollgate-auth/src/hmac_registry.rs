//! The concrete verifier in the box: server-issued API keys held as
//! HMAC-SHA256 digests.
//!
//! The digest table is a *projection*, not the system of record. Durable
//! credential lifecycle lives in a `KeyDirectory` (`tollgate-store`), and the
//! control plane installs the active set here the way it installs account
//! snapshots into a `SnapshotMap`. That split is why verification can stay
//! I/O-free on the request path (INVARIANTS.md #5, #6) while issuance and
//! revocation remain transactional and fleet-wide.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hmac::{Hmac, Mac};
use jiff::Timestamp;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tollgate_core::{KeyId, Principal};
use zeroize::Zeroizing;

use crate::verifier::{CredentialVerifier, Verified};

type HmacSha256 = Hmac<Sha256>;

/// How many bytes of entropy a minted secret carries.
///
/// 32 bytes from the OS CSPRNG. The digest is HMAC-SHA256 and its own output
/// is 32 bytes, so a longer secret would not raise the strength of what is
/// stored, and a shorter one would be the weakest link in a chain whose other
/// links are all 256 bits.
const SECRET_BYTES: usize = 32;

/// A freshly minted credential, returned exactly once.
///
/// The secret is [`Zeroizing`], so the only copy the process holds is wiped
/// when this value drops. The digest is suitable for durable storage; debug
/// output exposes only the non-secret identifiers.
///
/// **Nothing can recover the secret from the record.** That is the point of
/// storing digests, and it is also the constraint on issuance ordering: the
/// record must be durable *before* the secret is disclosed, because a crash
/// between the two leaves a credential the server has never heard of and no
/// reconciliation can repair it.
pub struct MintedKey {
    /// The non-secret identifier for this credential, used to revoke it.
    pub key_id: KeyId,
    /// The principal it authenticates as: the digest's leading 128 bits.
    pub principal: Principal,
    /// HMAC-SHA256 of the secret under the server secret. This is what a
    /// [`KeyDirectory`](tollgate_store::KeyDirectory) stores.
    pub digest: [u8; 32],
    /// The raw credential. Hand it to its owner and drop it; it cannot be
    /// derived again from anything retained.
    pub secret: Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for MintedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintedKey")
            .field("key_id", &self.key_id)
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

/// One credential as the projection holds it.
#[derive(Clone, Copy)]
struct ProjectedKey {
    digest: [u8; 32],
    /// Surfaced through [`Verified::reusable_until`], so a session cache
    /// cannot outlive the credential it authenticated with.
    not_after: Option<Timestamp>,
}

/// Credentials held as HMAC-SHA256 digests under a server secret.
///
/// **Digests at rest is the non-negotiable property.** A raw-credential map
/// would verify in tens of nanoseconds, but every place the map lives —
/// process memory, a core dump, a backup — would then hold usable
/// credentials, so one read leaks every customer's key. Timing is not what
/// rules that out (credentials are high-entropy and the map is randomly
/// keyed); exposure is.
///
/// The truncated digest is the [`Principal`] admission is keyed by, and the
/// *full* digest is what the comparison decides on, so truncation is never the
/// deciding comparison.
///
/// **Why HMAC and not a plain SHA-256 digest**, which is cheaper: HMAC splits
/// the secret from the digest table, so neither alone verifies anything. That
/// is worth paying for at the rate a digest is actually computed — once per
/// session under [`SessionCredential`], not once per request. Measured on one
/// machine: HMAC-SHA256 741 ns against SHA-256 184 ns, a 4x difference that
/// amortises to about 5.6 ns per request at a hundred requests per session,
/// against a ~23 ns cached path neither touches. The cheaper digest buys a
/// saving that rounds to nothing and costs a real security property.
///
/// **The table is arc-swapped**, not locked. Installation is an operator-rate
/// event and verification is on the request path, so readers must never block
/// behind a writer — the same reasoning, and the same mechanism, as
/// `ArcSwapSnapshotMap`. A write clones the table; that cost is paid by
/// whoever is issuing keys, not by whoever is serving requests.
///
/// An embedder who disagrees is not stuck with this: implement
/// [`CredentialVerifier`] instead.
///
/// [`SessionCredential`]: crate::SessionCredential
pub struct HmacRegistry {
    secret: Zeroizing<Vec<u8>>,
    keys: ArcSwap<HashMap<u128, ProjectedKey>>,
}

impl std::fmt::Debug for HmacRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacRegistry")
            .field("projected_keys", &self.len())
            .finish_non_exhaustive()
    }
}

impl HmacRegistry {
    /// A registry keyed by `secret`, holding no credentials until one is
    /// installed. Load the secret from the environment or a secret store;
    /// never commit one.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        HmacRegistry {
            secret: Zeroizing::new(secret.to_vec()),
            keys: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    fn digest(&self, credential: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("any key length works");
        mac.update(credential);
        mac.finalize().into_bytes().into()
    }

    /// The stable identity of a digest: its leading 128 bits.
    fn fingerprint(digest: &[u8; 32]) -> u128 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        u128::from_be_bytes(bytes)
    }

    /// Derive the durable digest and identity of an existing credential without
    /// installing it. This supports fixture bootstrap and imports; new issuance
    /// should use [`Self::mint`] so entropy comes from the OS. Persist the digest
    /// before projecting it, and never persist or log the credential argument.
    pub fn digest_credential(&self, credential: &[u8]) -> (Principal, [u8; 32]) {
        let digest = self.digest(credential);
        (Principal(Self::fingerprint(&digest)), digest)
    }

    /// Generate a credential and return it once, with the non-secret record
    /// its durable half needs.
    ///
    /// The secret comes from the OS CSPRNG rather than from a caller, because
    /// "generate it properly, show it once, never store it" is exactly the
    /// part of issuance that goes wrong quietly, and it is not a thing each
    /// embedder should re-implement. The registry does **not** install the
    /// result: publish the record durably first, then project it.
    ///
    /// # Errors
    ///
    /// Fails only if the operating system cannot supply entropy, which is not
    /// a condition to paper over with a weaker source.
    pub fn mint(&self, key_id: KeyId) -> Result<MintedKey, EntropyUnavailable> {
        let mut secret = Zeroizing::new(vec![0u8; SECRET_BYTES]);
        getrandom::fill(secret.as_mut_slice()).map_err(|_| EntropyUnavailable)?;
        let (principal, digest) = self.digest_credential(&secret);
        Ok(MintedKey {
            key_id,
            principal,
            digest,
            secret,
        })
    }

    /// Replace the projection with the currently active credential set.
    ///
    /// Whole-table replacement, never a merge: the directory decides which
    /// credentials are live, and a registry that merged would keep verifying
    /// a credential the directory has already retired. This is the same
    /// contract `SnapshotMap` publication has, for the same reason.
    pub fn install(
        &self,
        keys: impl IntoIterator<Item = (Principal, [u8; 32], Option<Timestamp>)>,
    ) {
        let projected: HashMap<u128, ProjectedKey> = keys
            .into_iter()
            .map(|(principal, digest, not_after)| (principal.0, ProjectedKey { digest, not_after }))
            .collect();
        self.keys.store(Arc::new(projected));
    }

    /// Project a set of credentials this process did not mint.
    ///
    /// For fixtures, and for an embedder importing a key set it already
    /// holds. Prefer [`mint`](Self::mint) with a durable record: a credential
    /// accepted here was generated somewhere this crate cannot vouch for, and
    /// "shown once, never stored" then rests on the caller having done it
    /// right — the part of issuance most worth centralising.
    ///
    /// Returns the principal each credential authenticates as, in the order
    /// given.
    pub fn install_credentials(
        &self,
        credentials: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Vec<Principal> {
        let projected: Vec<_> = credentials
            .into_iter()
            .map(|credential| {
                let (principal, digest) = self.digest_credential(credential.as_ref());
                (principal, digest, None)
            })
            .collect();
        let principals = projected.iter().map(|(p, _, _)| *p).collect();
        self.install(projected);
        principals
    }

    /// How many credentials the projection currently holds. For readiness
    /// reporting and tests; the digests themselves are never exposed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.load().len()
    }

    /// Whether the projection is empty, which for a serving instance means
    /// every credential will be refused.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The operating system could not supply entropy for a new credential.
///
/// Its own type rather than a `bool` or a swallowed default: a mint that
/// cannot be random must fail loudly, because the alternative is issuing a
/// guessable credential that verifies perfectly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntropyUnavailable;

impl std::fmt::Display for EntropyUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the operating system could not supply entropy for a new credential")
    }
}

impl std::error::Error for EntropyUnavailable {}

impl CredentialVerifier for HmacRegistry {
    /// A credential's own expiry, when it carries one, travels back through
    /// [`Verified::reusable_until`] so the session cache cannot outlive it.
    /// A credential without one is indefinite: it stops being accepted when
    /// it stops being projected, or when the account behind it is revoked —
    /// and revocation travels by snapshot, which admission consults on every
    /// request regardless of any cache.
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        let digest = self.digest(credential);
        let principal = Self::fingerprint(&digest);
        let keys = self.keys.load();
        let projected = keys.get(&principal)?;
        if projected.digest.ct_eq(&digest).into() {
            Some(match projected.not_after {
                Some(until) => Verified::until(Principal(principal), until),
                None => Verified::indefinite(Principal(principal)),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_methods,
        reason = "unit tests that build an arbitrary `now` the assertions are relative to; \
                  no assertion here depends on what the clock actually said"
    )]
    #[test]
    fn credential_diagnostics_never_disclose_issuer_or_customer_secrets() {
        let secret = b"fixture-debug-redaction-hmac-secret-108";
        let registry = HmacRegistry::new(secret);
        let key = registry.mint(KeyId(1)).unwrap();
        registry.install([(key.principal, key.digest, None)]);
        let diagnostic = format!("{registry:?} {key:?}");
        for protected in [
            format!("{secret:?}"),
            format!("{:?}", *key.secret),
            format!("{:?}", key.digest),
        ] {
            assert!(!diagnostic.contains(&protected));
        }
        assert!(diagnostic.contains("projected_keys: 1"));
    }

    use super::*;

    fn registry() -> HmacRegistry {
        let registry = HmacRegistry::new(b"server-secret");
        registry.install_credentials([b"key-one".as_slice()]);
        registry
    }

    #[test]
    fn a_registered_credential_verifies_to_a_stable_principal_with_no_expiry() {
        let registry = registry();
        assert_eq!(registry.verify(b"key-one"), registry.verify(b"key-one"));
        let verified = registry.verify(b"key-one").expect("registered");
        assert_eq!(
            verified.reusable_until, None,
            "a server-issued key does not expire on its own; withdrawal travels by snapshot"
        );
    }

    #[test]
    fn an_unregistered_or_altered_credential_does_not_verify() {
        let registry = registry();
        assert_eq!(registry.verify(b"key-two"), None);
        assert_eq!(registry.verify(b"key-one "), None);
        assert_eq!(registry.verify(b""), None);
    }

    /// The secret is half the proof: a digest table lifted without it verifies
    /// nothing. This is the property HMAC is paid for, so it is checked rather
    /// than assumed.
    #[test]
    fn the_same_credential_under_a_different_secret_is_a_different_principal() {
        let other = HmacRegistry::new(b"a-different-secret");
        let elsewhere = other.install_credentials([b"key-one".as_slice()])[0];
        assert_ne!(
            registry().verify(b"key-one").expect("registered").principal,
            elsewhere
        );
    }

    /// The registry stores digests, never credentials. Nothing else in the
    /// crate would notice if that broke, and it is the whole reason for the
    /// design, so it gets its own witness.
    #[test]
    fn no_credential_is_retained_in_the_registry() {
        let registry = registry();
        let credential: &[u8] = b"key-one";
        for projected in registry.keys.load().values() {
            assert!(
                !projected
                    .digest
                    .windows(credential.len())
                    .any(|w| w == credential),
                "a stored digest contains the credential it came from"
            );
        }
        assert!(
            !registry
                .secret
                .windows(credential.len())
                .any(|w| w == credential),
            "the secret contains the credential"
        );
    }
}
