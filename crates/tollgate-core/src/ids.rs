//! Identifier newtypes.
//!
//! All identifiers are opaque 128-bit values so a store backend may use UUIDs
//! without this crate depending on a uuid library; 64-bit backends simply use
//! the low half. `FencingToken` and `Generation` are ordered u64 sequences,
//! but they serve different contracts: a fencing token identifies one lease
//! capability, while a generation rejects older account snapshots.

use core::fmt;

macro_rules! id128 {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(transparent))]
        pub struct $name(pub u128);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }
    };
}

id128!(
    /// An account: the owner of quota, limits, and permissions.
    AccountId
);
id128!(
    /// A credential within an account; optional in contexts that admit an
    /// already-verified principal without key attribution.
    KeyId
);
id128!(
    /// One allocated lease of units to one service instance.
    LeaseId
);
id128!(
    /// Idempotency key for usage accounting: one request, one charge.
    RequestId
);
id128!(
    /// The request path's lookup key: an opaque fingerprint of an
    /// already-verified credential. How it is derived (API-key HMAC,
    /// capability subject, session id) is the embedding service's concern —
    /// by the time it reaches admission, verification has happened.
    ///
    /// # A caller must not be able to choose these bits
    ///
    /// Derive the fingerprint under a secret the caller does not hold, as the
    /// reference embedding does with a truncated HMAC-SHA256 of the API key.
    /// **Never key admission by a raw client-supplied token.**
    ///
    /// The reason is not subtle. This value selects an account's snapshot, its
    /// lease and its rate limiter. A caller who can choose it can aim at
    /// another tenant's entry — spending their quota, drawing on their limiter,
    /// and being admitted under their permissions. No hashing choice defends
    /// against that; only the derivation does.
    ///
    /// Because the bits are unsteerable, admission hashes them with a fast
    /// non-cryptographic hasher rather than SipHash (see
    /// `tollgate_admission`'s `PrincipalHasher`). That is a *consequence* of
    /// the rule above, not an additional requirement: an embedder who breaks
    /// it has already lost the tenant isolation SipHash was never protecting,
    /// and would merely lose it more slowly.
    Principal
);

/// One lease's capability token, drawn from a strictly increasing per-account
/// allocation sequence.
///
/// The ordering supplies an audit trail; it is not an account-wide validity
/// epoch. A newer token does not invalidate an older active lease. Stores
/// require this token to match the record named by the accompanying lease ID
/// (and account ID for usage ingest; INVARIANTS.md #4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct FencingToken(pub u64);

impl fmt::Display for FencingToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic version of an account's compiled policy snapshot. A snapshot with
/// a generation older than the newest one an instance has seen is stale and
/// must not be (re-)installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Generation(pub u64);

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
