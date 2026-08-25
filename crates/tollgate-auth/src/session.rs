//! The session-scoped credential cache: verify once, then compare.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use jiff::Timestamp;
use subtle::ConstantTimeEq;
use tollgate_core::Principal;
use zeroize::Zeroize;

use crate::verifier::{CredentialVerifier, Verified};

/// The credential a session has already had verified, and what it verified as.
#[derive(Debug)]
struct VerifiedCredential {
    /// The exact bytes that were verified. Retained so nothing else can reuse
    /// the principal, and so the value compared is always the value verified —
    /// caching some *other* proof alongside it is how those two drift apart.
    ///
    /// This is the one place in the design where a usable credential sits in
    /// memory, which is why it is wiped on drop rather than merely freed: the
    /// same "process memory, a core dump, a backup" threat that rules out a
    /// raw-credential registry applies to a live session's cache, only bounded
    /// by the number of open sessions instead of the whole customer base.
    credential: Box<[u8]>,
    verified: Verified,
}

impl VerifiedCredential {
    /// Wipe the credential in place.
    ///
    /// Separated from [`Drop`] so the wiping is observable while the bytes are
    /// still ours: reading them *after* the drop would be undefined behaviour,
    /// so there is no safe test on that side of it.
    fn wipe(&mut self) {
        self.credential.zeroize();
    }
}

impl Drop for VerifiedCredential {
    fn drop(&mut self) {
        self.wipe();
        #[cfg(test)]
        tests::record_wipe(&self.credential);
    }
}

/// Authentication state whose lifetime is exactly one session.
///
/// A "session" is whatever the embedder binds this to — an accepted TCP
/// connection, a TLS session, an HTTP/2 stream. This crate deliberately does
/// not know: binding it is transport-specific, and the mechanism is not.
///
/// **What this is for.** Verifying a credential costs real work — on one
/// machine ~800 ns for an HMAC-backed scheme, against ~107 ns for an entire
/// quota admission. Left per-request it dominates everything the rest of the
/// stack does. Doing it once per session and comparing thereafter brings the
/// per-request cost to ~16 ns.
///
/// **What it must never become.** A cached credential proves *identity*, never
/// *authorization*. A hit skips the verifier and nothing else: the caller
/// still runs admission against the current snapshot, so status, staleness,
/// permissions, rate and quota are decided fresh every request. Revocation
/// stays bounded by snapshot refresh exactly as it is without this cache.
///
/// A cached answer is also bounded by whatever validity the verifier attached
/// to it ([`Verified::reusable_until`]), so an expiring scheme — PASETO, JWT,
/// a client certificate — does not get to outlive its own expiry just because
/// the session stayed open.
///
/// Clones share one slot, so concurrent requests on a multiplexed session
/// authenticate independently without a lock.
#[derive(Debug, Clone, Default)]
pub struct SessionCredential {
    verified: Arc<ArcSwapOption<VerifiedCredential>>,
}

impl SessionCredential {
    /// An empty cache. Give each session its own.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve `credential` to a [`Principal`] as of `now`, verifying it only
    /// when this session has not already verified exactly these bytes, or when
    /// the previous answer is no longer reusable.
    ///
    /// `credential` is the credential itself, with any transport framing
    /// already removed — the `Bearer ` prefix, the header name, the cookie
    /// attributes. Pass the same bytes you would pass to
    /// [`CredentialVerifier::verify`]; this compares and verifies the same
    /// slice, so there is no second value to fall out of step with it.
    ///
    /// `now` is supplied by the caller rather than read here, because this
    /// runs on the request path and the request path does not read clocks
    /// (INVARIANTS.md #5).
    ///
    /// `None` means the session presented nothing, and clears any prior proof.
    ///
    /// The ordering below is load bearing: a credential that does not match
    /// the cached one invalidates the cache **before** its replacement is
    /// verified, so a failed verification can never leave the previous
    /// principal reusable. Failed verification is never cached.
    #[must_use]
    pub fn authenticate<V: CredentialVerifier + ?Sized>(
        &self,
        credential: Option<&[u8]>,
        verifier: &V,
        now: Timestamp,
    ) -> Option<Principal> {
        let Some(credential) = credential else {
            self.verified.store(None);
            return None;
        };

        let cached = self.verified.load();
        if let Some(cached) = cached.as_ref()
            && bool::from(cached.credential.as_ref().ct_eq(credential))
            && cached.verified.is_reusable_at(now)
        {
            return Some(cached.verified.principal);
        }

        // Reaching here means one of three things: nothing cached, different
        // bytes, or an answer whose validity has run out. All three ask the
        // verifier again — an expired answer is never extended on the grounds
        // that the bytes still match, because the credential behind them may
        // have been renewed or may now be dead.

        self.verified.store(None);
        let verified = verifier.verify(credential)?;
        // A verifier that hands back an already-expired answer is answering
        // "no" in a roundabout way; caching it would be caching a refusal.
        if !verified.is_reusable_at(now) {
            return None;
        }
        self.verified.store(Some(Arc::new(VerifiedCredential {
            credential: credential.into(),
            verified,
        })));
        Some(verified.principal)
    }

    /// Whether this session currently holds a verified credential. For tests
    /// and diagnostics; the credential itself is never exposed.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.verified.load().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HmacRegistry;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).expect("in range")
    }

    thread_local! {
        /// What each dropped cache entry's buffer looked like *after* its
        /// wipe, recorded from inside the drop — the last instant those bytes
        /// are still defined to read.
        static WIPED: std::cell::RefCell<Vec<Vec<u8>>> =
            const { std::cell::RefCell::new(Vec::new()) };
    }

    /// Called by `VerifiedCredential::drop`. Lives here so the witness and the
    /// thing it witnesses cannot drift into different files.
    pub(super) fn record_wipe(credential: &[u8]) {
        WIPED.with(|wiped| wiped.borrow_mut().push(credential.to_vec()));
    }

    fn take_wiped() -> Vec<Vec<u8>> {
        WIPED.with(|wiped| std::mem::take(&mut *wiped.borrow_mut()))
    }

    /// A live session holds a usable credential in memory — the one place in
    /// this design that does. `HmacRegistry`'s own doc names "process memory,
    /// a core dump, a backup" as the threat that rules out a raw-credential
    /// registry, and that threat applies here too, bounded by open sessions
    /// rather than by the whole customer base. So the bytes are wiped rather
    /// than merely freed, and this is the witness for it.
    #[test]
    fn a_dropped_cache_entry_is_wiped_not_merely_freed() {
        let verifier = Counting::new();
        let _ = take_wiped();
        {
            let session = SessionCredential::new();
            session
                .authenticate(Some(b"key-one"), &verifier, t(0))
                .expect("verifies");
            // Replacing the entry drops the old one.
            session
                .authenticate(Some(b"key-two"), &verifier, t(0))
                .expect("verifies");
        }

        let wiped = take_wiped();
        assert_eq!(wiped.len(), 2, "both cache entries were dropped");
        for buffer in wiped {
            assert!(
                !buffer.is_empty(),
                "the entry should still have its length, only zeroed contents"
            );
            assert!(
                buffer.iter().all(|byte| *byte == 0),
                "a dropped credential must be zeroed, got {buffer:?}"
            );
        }
    }

    /// Counts how often the underlying verifier is actually reached, so a test
    /// can assert the digest was *skipped* rather than merely that the answer
    /// was right — the difference between testing this optimisation and
    /// testing around it.
    ///
    /// That this is an ordinary `CredentialVerifier` and not a hole poked in
    /// the real one is the trait earning its keep.
    struct Counting {
        inner: HmacRegistry,
        calls: std::sync::atomic::AtomicU64,
    }

    impl Counting {
        fn new() -> Self {
            let mut inner = HmacRegistry::new(b"server-secret");
            inner.register(b"key-one");
            inner.register(b"key-two");
            Counting {
                inner,
                calls: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl CredentialVerifier for Counting {
        fn verify(&self, credential: &[u8]) -> Option<Verified> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.verify(credential)
        }
    }

    #[test]
    fn an_unchanged_credential_is_verified_once_per_session() {
        let verifier = Counting::new();
        let session = SessionCredential::new();
        let first = session
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");
        for _ in 0..16 {
            assert_eq!(
                session.authenticate(Some(b"key-one"), &verifier, t(0)),
                Some(first),
                "a repeat must resolve to the same principal"
            );
        }
        assert_eq!(
            verifier.calls(),
            1,
            "the credential must be verified once, not once per request"
        );
    }

    #[test]
    fn the_cache_is_isolated_per_session() {
        let verifier = Counting::new();
        let one = SessionCredential::new();
        let two = SessionCredential::new();
        let one_principal = one
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");
        assert!(one.is_authenticated());
        assert!(
            !two.is_authenticated(),
            "a second session must not inherit the first's proof"
        );
        let two_principal = two
            .authenticate(Some(b"key-two"), &verifier, t(0))
            .expect("verifies");
        assert_ne!(one_principal, two_principal);
        assert_eq!(
            verifier.calls(),
            2,
            "each session verifies for itself, and only once"
        );
    }

    /// The ordering that matters: a credential that does not match the cached
    /// one clears the proof *before* its replacement is verified, so a failed
    /// replacement cannot leave the previous principal reusable.
    #[test]
    fn a_failed_replacement_does_not_leave_the_previous_principal_usable() {
        let verifier = Counting::new();
        let session = SessionCredential::new();
        session
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");

        assert_eq!(
            session.authenticate(Some(b"not-registered"), &verifier, t(0)),
            None
        );
        assert!(
            !session.is_authenticated(),
            "a refused credential must not leave the prior proof standing"
        );
    }

    #[test]
    fn a_changed_credential_revalidates_as_the_new_principal() {
        let verifier = Counting::new();
        let session = SessionCredential::new();
        let one = session
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");
        let two = session
            .authenticate(Some(b"key-two"), &verifier, t(0))
            .expect("verifies");
        assert_ne!(one, two, "a different credential is a different principal");
        assert_eq!(
            session.authenticate(Some(b"key-two"), &verifier, t(0)),
            Some(two)
        );
        assert_eq!(verifier.calls(), 2, "only the change re-verified");
    }

    #[test]
    fn presenting_nothing_clears_the_proof() {
        let verifier = Counting::new();
        let session = SessionCredential::new();
        session
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");
        assert_eq!(session.authenticate(None, &verifier, t(0)), None);
        assert!(!session.is_authenticated());
    }

    /// A prefix or extension of the cached credential must not ride in on the
    /// comparison. `ct_eq` is length-checked, but this is the kind of property
    /// worth pinning rather than assuming.
    #[test]
    fn a_prefix_or_extension_of_the_cached_credential_is_not_accepted() {
        let verifier = Counting::new();
        let session = SessionCredential::new();
        session
            .authenticate(Some(b"key-one"), &verifier, t(0))
            .expect("verifies");
        assert_eq!(session.authenticate(Some(b"key-on"), &verifier, t(0)), None);
        assert_eq!(
            session.authenticate(Some(b"key-one-and-more"), &verifier, t(0)),
            None
        );
    }

    /// A verifier is pluggable, so the cache must work with one that has
    /// nothing to do with HMAC. This one authenticates anything starting with
    /// `tok-`, which no digest scheme would.
    #[test]
    fn the_cache_works_with_an_arbitrary_verifier() {
        struct PrefixScheme;
        impl CredentialVerifier for PrefixScheme {
            fn verify(&self, credential: &[u8]) -> Option<Verified> {
                credential
                    .strip_prefix(b"tok-")
                    .map(|rest| Verified::indefinite(Principal(rest.len() as u128)))
            }
        }
        let session = SessionCredential::new();
        assert_eq!(
            session.authenticate(Some(b"tok-abcd"), &PrefixScheme, t(0)),
            Some(Principal(4))
        );
        assert_eq!(
            session.authenticate(Some(b"nope"), &PrefixScheme, t(0)),
            None
        );
    }
}

#[cfg(test)]
mod expiry_tests {
    use super::*;
    use crate::verifier::Verified;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).expect("in range")
    }

    /// An expiring scheme — a PASETO or JWT `exp`, a certificate `notAfter` —
    /// hands back the instant its answer stops being reusable. Every call
    /// re-reads the token, so a renewed credential is honoured too.
    struct Expiring {
        expires_at: std::sync::Mutex<Timestamp>,
        calls: std::sync::atomic::AtomicU64,
    }

    impl Expiring {
        fn new(expires_at: Timestamp) -> Self {
            Expiring {
                expires_at: std::sync::Mutex::new(expires_at),
                calls: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl CredentialVerifier for Expiring {
        fn verify(&self, credential: &[u8]) -> Option<Verified> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let expires_at = *self.expires_at.lock().expect("not poisoned");
            (credential == b"token").then(|| Verified::until(Principal(7), expires_at))
        }
    }

    /// The hole a pluggable verifier opens if the cache is not time-bounded:
    /// a short-lived token presented once on a long-lived session would be
    /// honoured for as long as the session stayed open, and admission cannot
    /// compensate because expiry is a property of the credential, not of the
    /// account snapshot.
    #[test]
    fn a_cached_answer_does_not_outlive_the_validity_it_was_given() {
        let verifier = Expiring::new(t(60));
        let session = SessionCredential::new();

        assert_eq!(
            session.authenticate(Some(b"token"), &verifier, t(0)),
            Some(Principal(7))
        );
        assert_eq!(
            session.authenticate(Some(b"token"), &verifier, t(59)),
            Some(Principal(7)),
            "still inside its validity, so still a cache hit"
        );
        assert_eq!(verifier.calls(), 1, "no re-verification while valid");

        // At the expiry instant exactly, the cached answer is spent.
        *verifier.expires_at.lock().expect("not poisoned") = t(120);
        assert_eq!(
            session.authenticate(Some(b"token"), &verifier, t(60)),
            Some(Principal(7)),
            "the same bytes re-verify, and the renewed validity is honoured"
        );
        assert_eq!(
            verifier.calls(),
            2,
            "expiry forces exactly one re-verification"
        );
    }

    /// A token that has already expired by the time it is first presented is
    /// refused, and — critically — not cached, so it cannot be reused.
    #[test]
    fn an_already_expired_answer_is_refused_and_not_cached() {
        let verifier = Expiring::new(t(10));
        let session = SessionCredential::new();
        assert_eq!(session.authenticate(Some(b"token"), &verifier, t(20)), None);
        assert!(
            !session.is_authenticated(),
            "a refusal must leave nothing cached"
        );
    }

    /// An expired cached answer whose credential is now rejected outright must
    /// not fall back on the stale principal.
    #[test]
    fn an_expired_answer_that_no_longer_verifies_denies() {
        struct OnceValid(std::sync::atomic::AtomicBool);
        impl CredentialVerifier for OnceValid {
            fn verify(&self, _credential: &[u8]) -> Option<Verified> {
                if self.0.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    Some(Verified::until(Principal(7), t(60)))
                } else {
                    None
                }
            }
        }
        let verifier = OnceValid(std::sync::atomic::AtomicBool::new(true));
        let session = SessionCredential::new();
        assert_eq!(
            session.authenticate(Some(b"token"), &verifier, t(0)),
            Some(Principal(7))
        );
        assert_eq!(
            session.authenticate(Some(b"token"), &verifier, t(60)),
            None,
            "the credential stopped verifying, so expiry must not admit it"
        );
        assert!(!session.is_authenticated());
    }
}
