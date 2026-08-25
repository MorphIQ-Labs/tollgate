//! The concrete verifier in the box: server-issued API keys held as
//! HMAC-SHA256 digests.

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tollgate_core::Principal;

use crate::verifier::{CredentialVerifier, Verified};

type HmacSha256 = Hmac<Sha256>;

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
/// An embedder who disagrees is not stuck with this: implement
/// [`CredentialVerifier`] instead.
///
/// [`SessionCredential`]: crate::SessionCredential
#[derive(Debug)]
pub struct HmacRegistry {
    secret: Vec<u8>,
    digests: HashMap<u128, [u8; 32]>,
}

impl HmacRegistry {
    /// A registry keyed by `secret`. Load it from the environment or a secret
    /// store; never commit one.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        HmacRegistry {
            secret: secret.to_vec(),
            digests: HashMap::new(),
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

    /// Record `credential` and return the [`Principal`] it verifies to. The
    /// credential itself is not retained — only its digest.
    pub fn register(&mut self, credential: &[u8]) -> Principal {
        let digest = self.digest(credential);
        let principal = Self::fingerprint(&digest);
        self.digests.insert(principal, digest);
        Principal(principal)
    }
}

impl CredentialVerifier for HmacRegistry {
    /// Indefinite: a server-issued key carries no expiry of its own. It stops
    /// being accepted when it stops being registered, or when the account
    /// behind it is revoked — and revocation travels by snapshot, which
    /// admission consults on every request regardless of any cache.
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        let digest = self.digest(credential);
        let principal = Self::fingerprint(&digest);
        let stored = self.digests.get(&principal)?;
        if stored.ct_eq(&digest).into() {
            Some(Verified::indefinite(Principal(principal)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> HmacRegistry {
        let mut registry = HmacRegistry::new(b"server-secret");
        registry.register(b"key-one");
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
        let mut other = HmacRegistry::new(b"a-different-secret");
        let elsewhere = other.register(b"key-one");
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
        for digest in registry.digests.values() {
            assert!(
                !digest.windows(credential.len()).any(|w| w == credential),
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
