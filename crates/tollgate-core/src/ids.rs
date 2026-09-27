//! Identifier newtypes.
//!
//! Tollgate's identifiers are opaque 128-bit values so a store backend may use
//! UUIDs without this crate depending on a uuid library; 64-bit backends simply
//! use the low half. [`PolicyRevision`] is the one 256-bit member: it is not
//! Tollgate's identifier at all but the *consumer's*, carried through
//! admission and billing and never interpreted here.
//!
//! Every one of them shares a single canonical spelling rule — a fixed number
//! of lowercase hexadecimal digits, no `0x` — enforced by one function so the
//! two widths cannot drift apart.
//!
//! `FencingToken` and `Generation` are ordered u64 sequences, but they serve
//! different contracts: a fencing token identifies one lease capability, while
//! a generation rejects older account snapshots.

use core::{fmt, str::FromStr};

/// An opaque identifier was not written in Tollgate's canonical textual form.
///
/// The wire form is a fixed number of lowercase hexadecimal digits, without a
/// `0x` prefix. Keeping the parser strict gives display output, paths, and JSON
/// one representation rather than a collection of equivalent spellings.
///
/// The expected width travels with the error because Tollgate has identifiers
/// of two widths — 32 digits for the 128-bit types, 64 for
/// [`PolicyRevision`] — and one rule serving both must be able to say which it
/// was applying. A single error type stating the width is one rule; a second
/// error type beside a second parser would be two rules that have to agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseIdError {
    expected_digits: usize,
}

impl ParseIdError {
    const fn new(expected_digits: usize) -> Self {
        Self { expected_digits }
    }

    /// How many lowercase hexadecimal digits the canonical form has.
    #[must_use]
    pub const fn expected_digits(self) -> usize {
        self.expected_digits
    }
}

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "identifier must be exactly {} lowercase hexadecimal digits",
            self.expected_digits
        )
    }
}

impl std::error::Error for ParseIdError {}

/// The one canonical-spelling rule, shared by every opaque identifier.
///
/// Width and character set only; what the digits decode *to* differs by type
/// and belongs to the caller. Uppercase is rejected, a `0x` prefix is rejected
/// (because `x` is not a hexadecimal digit), and the length must match
/// exactly — no padding, no truncation, no surrounding whitespace.
const fn validate_hex_digits(value: &str, expected_digits: usize) -> Result<(), ParseIdError> {
    if value.len() != expected_digits {
        return Err(ParseIdError::new(expected_digits));
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte.is_ascii_digit() || (byte >= b'a' && byte <= b'f')) {
            return Err(ParseIdError::new(expected_digits));
        }
        index += 1;
    }
    Ok(())
}

/// Digits per 128-bit identifier, and per 256-bit [`PolicyRevision`].
const ID_DIGITS: usize = 32;
const REVISION_DIGITS: usize = 64;

fn parse_id(value: &str) -> Result<u128, ParseIdError> {
    validate_hex_digits(value, ID_DIGITS)?;
    u128::from_str_radix(value, 16).map_err(|_| ParseIdError::new(ID_DIGITS))
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
/// (and account ID for usage ingest; INVARIANTS.md GL-4).
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

/// The consuming application's identity for the product policy compiled into a
/// snapshot — 256 opaque bits Tollgate carries and never interprets.
///
/// A service publishes compiled enforcement data (limits, a cost table,
/// permissions) derived from its own versioned product records. This is how it
/// says *which* records those were, so a response can report the policy that
/// priced a request and the billing event can be traced to the same one,
/// without any product vocabulary — plan, model, schedule, tier — entering
/// Tollgate. A content hash of the resolved inputs is the natural value; a
/// consumer with a shorter identifier zero-pads.
///
/// # Not a generation
///
/// [`Generation`] orders publication: it decides which snapshot is newer and
/// which is stale, and admission enforces it (INVARIANTS.md GL-15, GL-26). This
/// identifies the *inputs* compiled into a publication and carries no order at
/// all. Two generations can share a revision (the same policy republished after
/// a status change), and one generation carries exactly one revision. Neither
/// substitutes for the other, which is why this deliberately does **not**
/// derive `PartialOrd`/`Ord` as the identifier types do: comparing two of them
/// for order would be asking a question the value cannot answer, and the
/// answer would look plausible.
///
/// # Unstated is a value, not an error
///
/// [`Default`] is all zeroes and means "no revision stated". A control plane
/// that predates the field publishes snapshots without it, and they decode to
/// this rather than failing — the same fail-open-on-*identity* choice
/// `enforcement_mode` and `budget` make, and safe for the same reason: nothing
/// in Tollgate reads it, so an absent revision cannot change an enforcement
/// outcome.
///
/// # Wire form
///
/// Exactly 64 lowercase hexadecimal digits, under the same strict rule the
/// 128-bit identifiers use (INVARIANTS.md GL-21). Strictness matters more here
/// than elsewhere: the consumer compares these for equality to select its own
/// metadata, and two spellings of one revision would silently look like two
/// policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PolicyRevision(pub [u8; 32]);

impl PolicyRevision {
    /// The "no revision stated" value: all zeroes.
    pub const UNSTATED: Self = Self([0; 32]);

    /// Whether this is the unstated revision.
    #[must_use]
    pub const fn is_unstated(self) -> bool {
        let mut index = 0;
        while index < self.0.len() {
            if self.0[index] != 0 {
                return false;
            }
            index += 1;
        }
        true
    }

    /// The raw bytes, for a consumer storing or comparing them.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for PolicyRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for PolicyRevision {
    type Err = ParseIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_hex_digits(value, REVISION_DIGITS)?;
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let pair = &value[index * 2..index * 2 + 2];
            // The charset and width are already proven, so each pair is two
            // hexadecimal digits and this cannot fail. Delegating the nibble
            // arithmetic keeps it that way: a hand-rolled `(high << 4) | low`
            // reads correctly, but its `|` is indistinguishable from `^` and
            // `+` here because the halves never share a bit — an equivalent
            // mutant no test could ever kill, standing where a real one
            // should.
            *byte = u8::from_str_radix(pair, 16).map_err(|_| ParseIdError::new(REVISION_DIGITS))?;
        }
        Ok(Self(bytes))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for PolicyRevision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for PolicyRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RevisionVisitor;

        impl serde::de::Visitor<'_> for RevisionVisitor {
            type Value = PolicyRevision;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a policy revision containing exactly 64 lowercase hexadecimal digits",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                value.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(RevisionVisitor)
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
            assert_eq!(
                value.parse::<AccountId>(),
                Err(ParseIdError::new(ID_DIGITS)),
                "{value:?}"
            );
        }
    }

    /// The same rule at the other width, with the same rejection corpus scaled
    /// up — uppercase, a prefix, trailing space, a non-hex digit.
    #[test]
    fn noncanonical_revision_spellings_are_rejected() {
        let ok = "8".repeat(64);
        assert!(
            ok.parse::<PolicyRevision>().is_ok(),
            "the corpus baseline parses"
        );
        for value in [
            "1".to_string(),
            format!("{}{}", "0".repeat(63), "1 "),
            format!("0x{}", "0".repeat(64)),
            format!("{}A", "0".repeat(63)),
            "g".repeat(64),
        ] {
            assert_eq!(
                value.parse::<PolicyRevision>(),
                Err(ParseIdError::new(REVISION_DIGITS)),
                "{value:?}"
            );
        }
    }

    /// The two widths are enforced separately, and each rejects the other's
    /// canonical form. Sharing one rule must not mean sharing one width: a
    /// 32-digit value is a perfectly good identifier and not a revision at all.
    #[test]
    fn the_two_identifier_widths_reject_each_others_canonical_form() {
        let id_width = "0".repeat(32);
        let revision_width = "0".repeat(64);

        assert!(id_width.parse::<AccountId>().is_ok());
        assert_eq!(
            id_width.parse::<PolicyRevision>(),
            Err(ParseIdError::new(REVISION_DIGITS))
        );

        assert!(revision_width.parse::<PolicyRevision>().is_ok());
        assert_eq!(
            revision_width.parse::<AccountId>(),
            Err(ParseIdError::new(ID_DIGITS))
        );
    }

    /// The error says which width it was applying, so a caller reading it is
    /// not told a 64-digit value should have been 32.
    #[test]
    fn the_parse_error_names_the_width_it_expected() {
        let id_error = "".parse::<AccountId>().unwrap_err();
        assert_eq!(id_error.expected_digits(), 32);
        assert_eq!(
            id_error.to_string(),
            "identifier must be exactly 32 lowercase hexadecimal digits"
        );

        let revision_error = "".parse::<PolicyRevision>().unwrap_err();
        assert_eq!(revision_error.expected_digits(), 64);
        assert_eq!(
            revision_error.to_string(),
            "identifier must be exactly 64 lowercase hexadecimal digits"
        );
    }

    /// The unstated revision is a value, not an absence: it has a canonical
    /// spelling, it round-trips, and it reports itself as unstated.
    #[test]
    fn the_unstated_revision_is_all_zeroes_and_round_trips() {
        let unstated = PolicyRevision::default();
        assert_eq!(unstated, PolicyRevision::UNSTATED);
        assert!(unstated.is_unstated());
        assert_eq!(unstated.to_string(), "0".repeat(64));
        assert_eq!(unstated.to_string().parse::<PolicyRevision>(), Ok(unstated));

        let stated = PolicyRevision([0xab; 32]);
        assert!(!stated.is_unstated());
        assert_eq!(stated.to_string(), "ab".repeat(32));
        assert_eq!(stated.to_string().parse::<PolicyRevision>(), Ok(stated));
    }

    /// Every byte position survives the text round trip in the right order —
    /// a transposition would still be 64 valid digits, so the pattern is
    /// deliberately asymmetric.
    #[test]
    fn a_revision_round_trips_every_byte_position_in_order() {
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let revision = PolicyRevision(bytes);
        assert_eq!(
            revision.to_string(),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
        assert_eq!(revision.to_string().parse::<PolicyRevision>(), Ok(revision));
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

    /// The revision follows the same textual-and-strict Serde contract: a JSON
    /// string in canonical form, and a number is refused rather than coerced.
    #[cfg(feature = "serde")]
    #[test]
    fn revision_serde_is_textual_and_strict() {
        let value = PolicyRevision([0x8f; 32]);
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(encoded, format!("\"{}\"", "8f".repeat(32)));
        assert_eq!(
            serde_json::from_str::<PolicyRevision>(&encoded).unwrap(),
            value
        );
        // A number is refused, and the refusal says what was expected. The
        // visitor's `expecting` text is the only thing telling a caller what
        // shape the field wanted, so it is asserted rather than assumed —
        // otherwise it could return nothing at all and no test would notice.
        let wrong_type =
            serde_json::from_str::<PolicyRevision>("42").expect_err("a number is not a revision");
        assert!(
            wrong_type
                .to_string()
                .contains("exactly 64 lowercase hexadecimal digits"),
            "the type error must name the expected form, got {wrong_type}"
        );
        // An identifier-width string is not a revision either, and that
        // refusal carries the parse rule's own width.
        let wrong_width =
            serde_json::from_str::<PolicyRevision>(&format!("\"{}\"", "0".repeat(32)))
                .expect_err("32 digits is not a revision");
        assert!(
            wrong_width
                .to_string()
                .contains("exactly 64 lowercase hexadecimal digits"),
            "the width error must name the expected width, got {wrong_width}"
        );
    }
}
