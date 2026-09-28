//! The seam: what it means to verify a credential, independent of how.

use jiff::Timestamp;
use tollgate_core::{KeyId, Principal};

use crate::hmac_registry::{EntropyUnavailable, MintedKey};

/// A successful verification: who presented the credential, and how long that
/// answer may be reused without asking again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verified {
    /// Who presented the credential: the identity it authenticates as, and
    /// nothing about what that identity may do.
    pub principal: Principal,
    /// The instant from which this answer must be re-derived.
    ///
    /// `None` means the credential does not expire of its own accord — a
    /// digest of a server-issued key is valid until it is withdrawn, and
    /// withdrawal travels by snapshot rather than by clock.
    ///
    /// **A scheme that carries an expiry must put it here.** A PASETO or JWT
    /// `exp`, a certificate's `notAfter`: without it, a session that
    /// authenticated once would keep spending an expired token for as long as
    /// it stayed connected, and admission could not compensate because expiry
    /// is a property of the credential and not of the account snapshot.
    pub reusable_until: Option<Timestamp>,
}

impl Verified {
    /// A credential that does not expire on its own.
    #[must_use]
    pub const fn indefinite(principal: Principal) -> Self {
        Verified {
            principal,
            reusable_until: None,
        }
    }

    /// A credential whose verification stops being reusable at `until`.
    #[must_use]
    pub const fn until(principal: Principal, until: Timestamp) -> Self {
        Verified {
            principal,
            reusable_until: Some(until),
        }
    }

    /// Whether this answer may still be reused at `now`.
    #[must_use]
    pub fn is_reusable_at(&self, now: Timestamp) -> bool {
        self.reusable_until.is_none_or(|until| now < until)
    }
}

/// Turns a presented credential into the [`Principal`] it authenticates as.
///
/// This is the vocabulary boundary. Credential *schemes* differ per deployment
/// — an API key digest, a PASETO token, a JWT signature, a client-certificate
/// fingerprint — and this crate does not pick one. What it does own is the
/// expensive and subtle part around them: doing this at most once per session
/// ([`SessionCredential`]), comparing in constant time, and never letting a
/// cached answer outlive either the credential that earned it or the validity
/// that credential carried.
///
/// [`HmacRegistry`](crate::HmacRegistry) is the implementation shipped in the
/// box, and is a reasonable default for server-issued API keys.
///
/// # Contract
///
/// - **Deterministic in the credential.** The same bytes must always yield the
///   same principal. Time-varying *validity* is expressed through
///   [`Verified::reusable_until`], not by returning different answers to the
///   same input — a verifier that did the latter would disagree with its own
///   cached result.
/// - **Constant-time in the secret.** Compare digests or signatures with
///   `subtle`, never `==`. A verifier that leaks by timing leaks through the
///   cache miss just as it would without one.
/// - **No I/O, no blocking, no locks held across it.** A miss runs on the
///   request path, so this inherits the request path's rules (INVARIANTS.md
///   GL-5, GL-6). A verifier needing a database belongs behind a snapshot, not
///   here.
/// - **Identity only.** Returning a `Principal` says who presented the
///   credential and nothing about what they may do. Status, permissions, rate
///   and quota are admission's decision, every request.
///
/// [`SessionCredential`]: crate::SessionCredential
pub trait CredentialVerifier {
    /// Verify `credential`, or return `None` if it authenticates as nobody.
    ///
    /// `credential` is the credential itself, with transport framing already
    /// removed — no `Bearer ` prefix, no header name, no cookie attributes.
    fn verify(&self, credential: &[u8]) -> Option<Verified>;
}

/// Minting the credentials a [`CredentialVerifier`] will later accept (GL-121).
///
/// Separate from verification on purpose, and not merged into it. Every
/// deployment verifies; only one that administers accounts over HTTP needs to
/// *mint*, and a server that never issues should not hold the capability to.
/// Keeping them apart lets a deployment answer "this instance does not issue
/// credentials" by simply not having an issuer, rather than by configuration
/// that could be got wrong.
///
/// **The secret exists exactly once.** An implementation returns it in
/// [`MintedKey`] and retains nothing from which it can be recovered — what
/// persists is a digest. A caller therefore has one opportunity to deliver it,
/// and losing it means revoking the credential and issuing another, never
/// asking for the same secret again.
///
/// **The secret is the presented form.** [`MintedKey::secret`] must be the
/// exact bytes the owner will present and the digest covers: visible ASCII
/// text, disclosed without re-encoding. An issuer that digests one form and
/// hands out another mints credentials no verifier accepts as presented.
pub trait CredentialIssuer {
    /// Mint a credential for `key_id`, chosen by the caller.
    ///
    /// The caller supplies the id so that a lost response is recoverable: the
    /// same request resent is refused as a duplicate by the directory instead
    /// of minting a second credential. An implementation must not derive the
    /// id from the secret, or the two would share a fate.
    fn mint(&self, key_id: KeyId) -> Result<MintedKey, EntropyUnavailable>;
}

impl<I: CredentialIssuer + ?Sized> CredentialIssuer for &I {
    fn mint(&self, key_id: KeyId) -> Result<MintedKey, EntropyUnavailable> {
        (**self).mint(key_id)
    }
}

impl<V: CredentialVerifier + ?Sized> CredentialVerifier for &V {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        (**self).verify(credential)
    }
}

impl<V: CredentialVerifier + ?Sized> CredentialVerifier for std::sync::Arc<V> {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        (**self).verify(credential)
    }
}

impl<V: CredentialVerifier + ?Sized> CredentialVerifier for Box<V> {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        (**self).verify(credential)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Fixed;
    impl CredentialVerifier for Fixed {
        fn verify(&self, credential: &[u8]) -> Option<Verified> {
            (credential == b"good").then(|| Verified::indefinite(Principal(1)))
        }
    }

    /// The blanket impls are what let an embedder hold its verifier however it
    /// needs to — behind a reference, an `Arc` shared across tasks, or a `Box`
    /// chosen at startup from configuration. A `dyn CredentialVerifier` is the
    /// whole reason the seam is a trait rather than a generic parameter, so
    /// each forwarding impl is exercised rather than assumed.
    #[test]
    fn every_forwarding_impl_reaches_the_verifier() {
        let expected = Some(Verified::indefinite(Principal(1)));

        assert_eq!(CredentialVerifier::verify(&&Fixed, b"good"), expected);
        assert_eq!(Arc::new(Fixed).verify(b"good"), expected);
        assert_eq!(Box::new(Fixed).verify(b"good"), expected);

        // Behind `dyn`, which is the shape that needs them most.
        let boxed: Box<dyn CredentialVerifier> = Box::new(Fixed);
        assert_eq!(boxed.verify(b"good"), expected);
        let shared: Arc<dyn CredentialVerifier> = Arc::new(Fixed);
        assert_eq!(shared.verify(b"good"), expected);

        // And a refusal forwards as a refusal, not as a swallowed `None`
        // indistinguishable from a broken delegation.
        assert_eq!(boxed.verify(b"bad"), None);
        assert_eq!(shared.verify(b"bad"), None);
    }

    #[test]
    fn an_indefinite_answer_is_reusable_at_any_instant() {
        let verified = Verified::indefinite(Principal(1));
        assert!(verified.is_reusable_at(Timestamp::from_second(0).unwrap()));
        assert!(verified.is_reusable_at(Timestamp::MAX));
    }

    /// The boundary is exclusive: at the expiry instant the answer is spent,
    /// matching how the rest of the stack treats `usable_until` (INVARIANTS GL-12).
    #[test]
    fn a_bounded_answer_expires_at_its_instant_not_after_it() {
        let until = Timestamp::from_second(60).unwrap();
        let verified = Verified::until(Principal(1), until);
        assert!(verified.is_reusable_at(Timestamp::from_second(59).unwrap()));
        assert!(!verified.is_reusable_at(until));
        assert!(!verified.is_reusable_at(Timestamp::from_second(61).unwrap()));
    }
}
