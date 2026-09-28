//! Read-only, revisioned credential pages. Lifecycle authority stays in `KeyDirectory`.

use std::collections::HashSet;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use jiff::Timestamp;
use tollgate_core::{KeyId, Principal};

use crate::{KeyRecord, StoreError};

/// A page bounds transport memory, not the total credential catalogue.
/// At the maximum, the canonical wire page fits its derived ~1 MiB envelope.
pub const MAX_KEY_PAGE_LIMIT: usize = 4096;
/// The page size used when a caller does not choose one: the server's default
/// for an omitted `limit` query, and the client key manager's default.
pub const DEFAULT_KEY_PAGE_LIMIT: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// PostgreSQL stores revisions as nonnegative BIGINTs. Neither backend wraps.
pub const MAX_KEY_REVISION: u64 = i64::MAX as u64;

/// Refuse a page limit above [`MAX_KEY_PAGE_LIMIT`].
///
/// # Errors
///
/// A [`StoreError`] when `limit` exceeds the maximum.
pub fn validate_key_page_limit(limit: NonZeroUsize) -> Result<(), StoreError> {
    if limit.get() > MAX_KEY_PAGE_LIMIT {
        return Err(StoreError("credential page limit exceeds 4096".into()));
    }
    Ok(())
}

/// The verifier's view of one credential, with its stable pagination key.
/// Neither issuance authority nor customer/HMAC secrets cross this boundary.
#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct CredentialRecord {
    /// The credential's identifier: the page's sort key and pagination cursor.
    pub key_id: KeyId,
    /// The principal the credential authenticates as. [`CredentialSet::try_new`]
    /// requires it to equal the leading 128 bits of `digest`.
    pub principal: Principal,
    /// The verifier's HMAC-SHA256 digest of the secret. On the wire, exactly 64
    /// lowercase hexadecimal characters; redacted from `Debug` output.
    #[cfg_attr(feature = "wire", serde(with = "digest_hex"))]
    pub digest: [u8; 32],
    /// Required on the wire: explicit null means no individual expiry.
    #[cfg_attr(feature = "wire", serde(deserialize_with = "required_option"))]
    pub not_after: Option<Timestamp>,
}

impl std::fmt::Debug for CredentialRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialRecord")
            .field("key_id", &self.key_id)
            .field("principal", &self.principal)
            .field("digest", &"[redacted]")
            .field("not_after", &self.not_after)
            .finish()
    }
}

impl From<KeyRecord> for CredentialRecord {
    fn from(record: KeyRecord) -> Self {
        Self {
            key_id: record.key_id,
            principal: record.principal,
            digest: record.digest,
            not_after: record.not_after,
        }
    }
}

/// Validated identity evidence. Completeness across pages additionally requires
/// the source revision and terminal cursor, checked by the owning drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSet(Vec<CredentialRecord>);

impl CredentialSet {
    /// Validate records as one set of identity evidence.
    ///
    /// # Errors
    ///
    /// A [`StoreError`] when a record's principal is not the leading 128 bits of its
    /// digest, or when two records share a principal or a key id.
    pub fn try_new(records: Vec<CredentialRecord>) -> Result<Self, StoreError> {
        let mut principals = HashSet::with_capacity(records.len());
        let mut ids = HashSet::with_capacity(records.len());
        for record in &records {
            let mut prefix = [0; 16];
            prefix.copy_from_slice(&record.digest[..16]);
            if record.principal.0 != u128::from_be_bytes(prefix) {
                return Err(StoreError(
                    "credential digest does not identify its principal".into(),
                ));
            }
            if !principals.insert(record.principal) || !ids.insert(record.key_id) {
                return Err(StoreError(
                    "credential projection contains duplicate identities".into(),
                ));
            }
        }
        Ok(Self(records))
    }

    /// The validated records, in the order given.
    pub fn records(&self) -> &[CredentialRecord] {
        &self.0
    }
    /// Consume the set, returning its records.
    pub fn into_records(self) -> Vec<CredentialRecord> {
        self.0
    }
}

/// One bounded page, validated against the request that produced it. Records
/// and revision describe the same committed source read. `next_after` exists
/// only when lookahead found another active record; an exactly full final page
/// is terminal. Revisions advance on every mutation that can change the feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPage {
    revision: u64,
    as_of: Timestamp,
    keys: CredentialSet,
    next_after: Option<KeyId>,
    after: Option<KeyId>,
    limit: NonZeroUsize,
}

impl KeyPage {
    /// Validate one page against the request (`after`, `limit`) that produced it.
    /// `as_of` is the instant the source selected active records at.
    ///
    /// # Errors
    ///
    /// A [`StoreError`] when `limit` exceeds [`MAX_KEY_PAGE_LIMIT`]; `revision`
    /// exceeds [`MAX_KEY_REVISION`]; there are more than `limit` records; key ids do
    /// not strictly increase from `after`; a record's `not_after` is at or before
    /// `as_of`; `next_after` is present on a page that is not full or does not name
    /// its last record; or the records fail [`CredentialSet::try_new`].
    pub fn try_new(
        revision: u64,
        as_of: Timestamp,
        after: Option<KeyId>,
        limit: NonZeroUsize,
        keys: Vec<CredentialRecord>,
        next_after: Option<KeyId>,
    ) -> Result<Self, StoreError> {
        validate_key_page_limit(limit)?;
        if revision > MAX_KEY_REVISION || keys.len() > limit.get() {
            return Err(StoreError(
                "credential page exceeds its revision or record domain".into(),
            ));
        }
        let mut previous = after;
        for key in &keys {
            if previous.is_some_and(|id| key.key_id <= id)
                || key.not_after.is_some_and(|end| as_of >= end)
            {
                return Err(StoreError(
                    "credential page is unordered, repeated, or expired at its source".into(),
                ));
            }
            previous = Some(key.key_id);
        }
        if let Some(next) = next_after
            && (keys.len() != limit.get() || Some(next) != previous)
        {
            return Err(StoreError(
                "credential page continuation does not advance its full page".into(),
            ));
        }
        Ok(Self {
            revision,
            as_of,
            keys: CredentialSet::try_new(keys)?,
            next_after,
            after,
            limit,
        })
    }

    /// Check that this page answers the request (`after`, `limit`).
    ///
    /// # Errors
    ///
    /// A [`StoreError`] when either differs from the request the page was built for.
    pub fn validate_request(
        &self,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<(), StoreError> {
        if self.after != after || self.limit != limit {
            return Err(StoreError(
                "credential page belongs to a different request".into(),
            ));
        }
        Ok(())
    }

    /// The source revision the records were read at.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// The instant the source selected active records at.
    pub fn as_of(&self) -> Timestamp {
        self.as_of
    }
    /// The page's records, in strictly increasing key-id order.
    pub fn records(&self) -> &[CredentialRecord] {
        self.keys.records()
    }
    /// The cursor for the next page, which is this page's last key id, or `None`
    /// when the page is terminal.
    pub fn next_after(&self) -> Option<KeyId> {
        self.next_after
    }
    /// Consume the page, returning its records.
    pub fn into_records(self) -> Vec<CredentialRecord> {
        self.keys.into_records()
    }
}

/// A read-only source of revisioned, paginated active-credential pages: what
/// a serving instance drains to build its verifier projection (INVARIANTS.md
/// 34). It carries no lifecycle or administrative authority; that belongs to
/// [`KeyDirectory`](crate::KeyDirectory).
#[async_trait]
pub trait KeySource: Send + Sync {
    /// Read active records in strictly increasing key-id order. Direct stores
    /// use `now`; HTTP servers choose their own clock and return it as `as_of`.
    /// Reads must be coherent with the returned revision, including for an
    /// empty result. Never return a partial successful page after a failure.
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<KeyPage, StoreError>;
}

#[cfg(feature = "wire")]
pub(crate) fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

#[cfg(feature = "wire")]
mod digest_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(digest: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        use std::fmt::Write;
        let mut text = String::with_capacity(64);
        for byte in digest {
            write!(text, "{byte:02x}").expect("writing into a String cannot fail");
        }
        serializer.serialize_str(&text)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.len() != 64
            || !text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom(
                "credential digest must be 64 lowercase hexadecimal characters",
            ));
        }
        let mut digest = [0; 32];
        for (byte, pair) in digest.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
            let pair = std::str::from_utf8(pair).map_err(serde::de::Error::custom)?;
            *byte = u8::from_str_radix(pair, 16).map_err(serde::de::Error::custom)?;
        }
        Ok(digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(id: u128) -> CredentialRecord {
        let mut digest = [0; 32];
        digest[..16].copy_from_slice(&id.to_be_bytes());
        CredentialRecord {
            key_id: KeyId(id),
            principal: Principal(id),
            digest,
            not_after: None,
        }
    }
    #[test]
    fn page_owns_identity_order_cursor_expiry_and_limit_validation() {
        let now = Timestamp::UNIX_EPOCH;
        let limit = NonZeroUsize::new(2).unwrap();
        let page = |records, after, next| KeyPage::try_new(0, now, after, limit, records, next);
        let valid = page(vec![key(1), key(2)], None, Some(KeyId(2))).unwrap();
        assert!(valid.validate_request(None, limit).is_ok());
        assert!(valid.validate_request(Some(KeyId(1)), limit).is_err());
        assert!(
            valid
                .validate_request(None, NonZeroUsize::new(1).unwrap())
                .is_err()
        );
        assert!(page(vec![key(1), key(2)], None, None).is_ok());
        assert!(page(vec![], None, None).is_ok());
        assert!(page(vec![], None, Some(KeyId(1))).is_err());
        assert!(page(vec![key(1)], None, Some(KeyId(1))).is_err());
        assert!(page(vec![key(1), key(2)], None, Some(KeyId(1))).is_err());
        assert!(page(vec![key(2), key(1)], None, None).is_err());
        assert!(page(vec![key(1), key(1)], None, None).is_err());
        assert!(page(vec![key(1)], Some(KeyId(1)), None).is_err());
        assert!(page(vec![key(1), key(2), key(3)], None, None).is_err());
        let mut expired = key(1);
        expired.not_after = Some(now);
        assert!(page(vec![expired], None, None).is_err());
        expired.not_after = now.checked_add(jiff::SignedDuration::from_nanos(1)).ok();
        assert!(page(vec![expired], None, None).is_ok());
        let mut corrupt = key(1);
        corrupt.digest[0] ^= 1;
        assert!(page(vec![corrupt], None, None).is_err());
        let mut repeated_principal = key(1);
        repeated_principal.key_id = KeyId(2);
        assert!(page(vec![key(1), repeated_principal], None, None).is_err());
        assert!(KeyPage::try_new(MAX_KEY_REVISION + 1, now, None, limit, vec![], None).is_err());
        assert!(KeyPage::try_new(MAX_KEY_REVISION, now, None, limit, vec![], None).is_ok());
        assert!(validate_key_page_limit(NonZeroUsize::new(MAX_KEY_PAGE_LIMIT).unwrap()).is_ok());
        assert!(
            validate_key_page_limit(NonZeroUsize::new(MAX_KEY_PAGE_LIMIT + 1).unwrap()).is_err()
        );
    }
    #[test]
    fn record_diagnostics_redact_the_digest() {
        let record = key(0xabc);
        assert!(!format!("{record:?}").contains(&format!("{:?}", record.digest)));
    }
}
