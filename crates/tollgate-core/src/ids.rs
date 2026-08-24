//! Identifier newtypes.
//!
//! All identifiers are opaque 128-bit values so a store backend may use UUIDs
//! without this crate depending on a uuid library; 64-bit backends simply use
//! the low half. `FencingToken` and `Generation` are ordered u64 sequences,
//! but they serve different contracts: a fencing token identifies one lease
//! capability, while a generation rejects older account snapshots.

use core::{fmt, str::FromStr};

/// A 128-bit identifier was not written in Tollgate's canonical textual form.
///
/// The wire form is exactly 32 lowercase hexadecimal digits, without a `0x`
/// prefix. Keeping the parser strict gives display output, paths, and JSON one
/// representation rather than a collection of equivalent spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseIdError;

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("identifier must be exactly 32 lowercase hexadecimal digits")
    }
}

impl std::error::Error for ParseIdError {}

fn parse_id(value: &str) -> Result<u128, ParseIdError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ParseIdError);
    }
    u128::from_str_radix(value, 16).map_err(|_| ParseIdError)
}

macro_rules! id128 {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u128);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_id(value).map(Self)
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_str(self)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct IdVisitor;

                impl serde::de::Visitor<'_> for IdVisitor {
                    type Value = $name;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str(
                            "an identifier containing exactly 32 lowercase hexadecimal digits",
                        )
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        value.parse().map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(IdVisitor)
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

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! canonical_id_tests {
        ($($name:ident),+ $(,)?) => {
            $(
                assert_eq!($name(0).to_string(), "00000000000000000000000000000000");
                assert_eq!($name(u128::MAX).to_string(), "ffffffffffffffffffffffffffffffff");
                assert_eq!(
                    "8000000000000000000000000000002a".parse::<$name>(),
                    Ok($name((1u128 << 127) | 0x2a)),
                );
            )+
        };
    }

    #[test]
    fn every_id_uses_the_same_fixed_width_lowercase_hexadecimal_text() {
        canonical_id_tests!(AccountId, KeyId, LeaseId, RequestId, Principal);
    }

    #[test]
    fn noncanonical_spellings_are_rejected() {
        for value in [
            "1",
            "00000000000000000000000000000001 ",
            "0x00000000000000000000000000000001",
            "0000000000000000000000000000000A",
            "gggggggggggggggggggggggggggggggg",
        ] {
            assert_eq!(value.parse::<AccountId>(), Err(ParseIdError), "{value:?}");
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn human_readable_serde_is_textual_and_strict() {
        macro_rules! assert_textual {
            ($name:ident) => {{
                let value = $name((1u128 << 127) | 0x2a);
                let encoded = serde_json::to_string(&value).unwrap();
                assert_eq!(encoded, r#""8000000000000000000000000000002a""#);
                assert_eq!(serde_json::from_str::<$name>(&encoded).unwrap(), value);
                assert!(serde_json::from_str::<$name>("42").is_err());
            }};
        }

        assert_textual!(AccountId);
        assert_textual!(KeyId);
        assert_textual!(LeaseId);
        assert_textual!(RequestId);
        assert_textual!(Principal);
    }
}
