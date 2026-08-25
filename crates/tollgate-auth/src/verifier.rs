//! The seam: what it means to verify a credential, independent of how.

use jiff::Timestamp;
use tollgate_core::Principal;

/// A successful verification: who presented the credential, and how long that
/// answer may be reused without asking again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verified {
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
///   #5, #6). A verifier needing a database belongs behind a snapshot, not
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
    /// matching how the rest of the stack treats `usable_until` (INVARIANTS #12).
    #[test]
    fn a_bounded_answer_expires_at_its_instant_not_after_it() {
        let until = Timestamp::from_second(60).unwrap();
        let verified = Verified::until(Principal(1), until);
        assert!(verified.is_reusable_at(Timestamp::from_second(59).unwrap()));
        assert!(!verified.is_reusable_at(until));
        assert!(!verified.is_reusable_at(Timestamp::from_second(61).unwrap()));
    }
}
