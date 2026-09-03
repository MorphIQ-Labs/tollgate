//! Issue #104 stage one: the credential lifecycle seam, exercised end to end
//! across the two crates that own its halves.
//!
//! `tollgate-auth` mints and verifies but holds no durable state;
//! `tollgate-store` records and retires but never sees a secret. This crate is
//! the only one that sees both, so the flow they compose into is proven here.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_core::{AccountId, AccountStatus, CostUnits, KeyId};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, KeyDirectory, KeyError, KeyRecord, MemoryStore,
    Revocation,
};

const ACCOUNT: AccountId = AccountId(1);

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

async fn store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    AdminStore::create_account(
        store.as_ref(),
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(10_000),
            status: AccountStatus::Active,
        },
    )
    .await
    .unwrap();
    store
}

/// Rebuild the verifier's table from the directory: the projection step the
/// control plane owns.
async fn project(registry: &HmacRegistry, store: &Arc<MemoryStore>, now: Timestamp) {
    let active = store.active_keys(now).await.unwrap();
    registry.install(
        active
            .into_iter()
            .map(|record| (record.principal, record.digest, record.not_after)),
    );
}

/// The whole flow: mint, record durably, project, verify — then revoke,
/// re-project, and stop verifying. Each half is useless alone, which is the
/// argument for the seam.
#[tokio::test]
async fn a_minted_credential_verifies_until_it_is_revoked() {
    let store = store().await;
    let registry = HmacRegistry::new(b"server-secret");

    let minted = registry.mint(KeyId(1)).expect("entropy is available");
    // Durability before disclosure: the record commits, and only then is the
    // secret usable by anyone. A crash before this line loses nothing; a
    // crash after it loses nothing either.
    store
        .insert_key(KeyRecord {
            key_id: minted.key_id,
            account_id: ACCOUNT,
            principal: minted.principal,
            digest: minted.digest,
            not_after: None,
        })
        .await
        .expect("a fresh key on a live account is recorded");

    assert_eq!(
        registry.verify(&minted.secret),
        None,
        "minting alone must not authenticate: the projection has not been installed"
    );

    project(&registry, &store, t(0)).await;
    assert_eq!(
        registry.len(),
        1,
        "the projection holds the live credential"
    );
    assert!(!registry.is_empty());
    let verified = registry.verify(&minted.secret).expect("projected");
    assert_eq!(verified.principal, minted.principal);
    assert_eq!(
        verified.reusable_until, None,
        "a credential with no expiry of its own is indefinite"
    );

    assert_eq!(
        store.revoke_key(KeyId(1), t(10)).await,
        Ok(Revocation::Retired)
    );
    assert_eq!(
        store.revoke_key(KeyId(1), t(11)).await,
        Ok(Revocation::AlreadyRetired),
        "an operator retiring an already-retired key learns that, rather than a second success"
    );

    project(&registry, &store, t(11)).await;
    assert_eq!(
        registry.verify(&minted.secret),
        None,
        "a revoked credential stops verifying once the projection catches up"
    );
    assert_eq!(registry.len(), 0);
    assert!(registry.is_empty());
}

/// The credential's own expiry reaches the request path through
/// `Verified::reusable_until`, which is the field's documented purpose and
/// what stops a session cache outliving the key it authenticated with.
#[tokio::test]
async fn a_credentials_own_expiry_travels_to_the_verifier() {
    let store = store().await;
    let registry = HmacRegistry::new(b"server-secret");
    let minted = registry.mint(KeyId(2)).unwrap();
    store
        .insert_key(KeyRecord {
            key_id: minted.key_id,
            account_id: ACCOUNT,
            principal: minted.principal,
            digest: minted.digest,
            not_after: Some(t(100)),
        })
        .await
        .unwrap();

    project(&registry, &store, t(0)).await;
    assert_eq!(registry.len(), 1, "a live credential is projected");
    assert!(!registry.is_empty());
    let verified = registry.verify(&minted.secret).expect("projected");
    assert_eq!(verified.reusable_until, Some(t(100)));
    assert!(verified.is_reusable_at(t(99)));
    assert!(!verified.is_reusable_at(t(100)), "expiry is exclusive");

    // Past its own expiry the directory stops calling it active, so the next
    // projection drops it without anyone having to revoke it.
    project(&registry, &store, t(100)).await;
    assert!(
        registry.is_empty(),
        "an expired credential leaves the projection on its own"
    );
}

/// Two credentials for one account is what rotation-with-overlap is made of:
/// each mints its own principal, because a principal *is* the digest
/// fingerprint, so both can be live at once and the old one retired alone.
#[tokio::test]
async fn rotation_keeps_both_credentials_live_until_the_old_one_is_retired() {
    let store = store().await;
    let registry = HmacRegistry::new(b"server-secret");

    let old = registry.mint(KeyId(1)).unwrap();
    let new = registry.mint(KeyId(2)).unwrap();
    assert_ne!(
        old.principal, new.principal,
        "each credential authenticates as its own principal"
    );
    for (key, minted) in [(KeyId(1), &old), (KeyId(2), &new)] {
        store
            .insert_key(KeyRecord {
                key_id: key,
                account_id: ACCOUNT,
                principal: minted.principal,
                digest: minted.digest,
                not_after: None,
            })
            .await
            .unwrap();
    }

    project(&registry, &store, t(0)).await;
    assert_eq!(
        registry.len(),
        2,
        "both credentials are live during overlap"
    );
    assert!(registry.verify(&old.secret).is_some());
    assert!(registry.verify(&new.secret).is_some(), "overlap window");

    store.revoke_key(KeyId(1), t(10)).await.unwrap();
    project(&registry, &store, t(10)).await;
    assert_eq!(registry.len(), 1, "retiring one leaves exactly the other");
    assert_eq!(registry.verify(&old.secret), None, "the old key is retired");
    assert!(
        registry.verify(&new.secret).is_some(),
        "the replacement survives its predecessor's retirement"
    );
}

/// Issuance is never destructive, and never issues against an account that
/// cannot own it — both refused rather than silently absorbed.
#[tokio::test]
async fn issuance_refuses_a_duplicate_key_or_an_unknown_account() {
    let store = store().await;
    let registry = HmacRegistry::new(b"server-secret");
    let minted = registry.mint(KeyId(1)).unwrap();
    let record = KeyRecord {
        key_id: minted.key_id,
        account_id: ACCOUNT,
        principal: minted.principal,
        digest: minted.digest,
        not_after: None,
    };
    store.insert_key(record.clone()).await.unwrap();

    assert_eq!(
        store.insert_key(record.clone()).await,
        Err(KeyError::AlreadyExists),
        "an overwrite would retire a live credential without saying so"
    );
    assert_eq!(
        store
            .insert_key(KeyRecord {
                key_id: KeyId(9),
                account_id: AccountId(404),
                ..record
            })
            .await,
        Err(KeyError::UnknownAccount)
    );
    assert_eq!(
        store.revoke_key(KeyId(404), t(0)).await,
        Err(KeyError::UnknownKey)
    );
}

/// Every mint is independent: the same registry, the same secret, different
/// credentials. A generator that repeated itself would hand two customers the
/// same key and neither would ever know.
#[tokio::test]
async fn minting_never_repeats_a_credential() {
    let registry = HmacRegistry::new(b"server-secret");
    let mut seen = std::collections::HashSet::new();
    for id in 0..64u128 {
        let minted = registry.mint(KeyId(id)).unwrap();
        assert!(
            seen.insert(minted.secret.to_vec()),
            "the generator repeated a credential"
        );
        assert!(seen.insert(minted.digest.to_vec()), "digests collided");
    }
}
