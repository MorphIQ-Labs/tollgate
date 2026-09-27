//! Issue GL-104 stage one: the credential lifecycle seam, exercised end to end
//! across the two crates that own its halves.
//!
//! `tollgate-auth` mints and verifies but holds no durable state;
//! `tollgate-store` records and retires but never sees a secret. This crate is
//! the only one that sees both, so the flow they compose into is proven here.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, KeyDirectory, KeyError, KeyRecord, MemoryStore,
    Revocation,
};

const ACCOUNT: AccountId = AccountId(1);
const SECRET: &[u8] = b"fixture-managed-key-lifecycle-secret-108";

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
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    store
}

/// Start the supported manager and await its initial complete projection.
async fn project(store: &Arc<MemoryStore>, now: Timestamp) -> tollgate_client::KeyManager {
    let manager = tollgate_client::KeyManager::spawn(
        store.clone(),
        SECRET,
        Arc::new(tollgate_store::ManualClock::new(now)),
        tollgate_client::KeyManagerConfig {
            max_age: std::time::Duration::from_secs(120),
            ..tollgate_client::KeyManagerConfig::default()
        },
    )
    .unwrap();
    let mut monitor = manager.monitor();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !monitor.report(now).ready {
            monitor.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    manager
}

/// The whole flow: mint, record durably, project, verify — then revoke,
/// re-project, and stop verifying. Each half is useless alone, which is the
/// argument for the seam.
#[tokio::test]
async fn a_minted_credential_verifies_until_it_is_revoked() {
    let store = store().await;
    let registry = HmacRegistry::new(SECRET);

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

    let manager = project(&store, t(0)).await;
    assert_eq!(
        manager.monitor().report(t(0)).projected_keys,
        1,
        "the projection holds the live credential"
    );
    assert!(!(manager.monitor().report(t(0)).projected_keys == 0));
    let verified = manager
        .verifier()
        .verify(&minted.secret)
        .expect("projected");
    assert_eq!(verified.principal, minted.principal);
    assert_eq!(
        verified.reusable_until,
        Some(t(120)),
        "a credential with no individual expiry is bounded by projection freshness"
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

    manager.shutdown().await;
    let manager = project(&store, t(11)).await;
    assert_eq!(
        manager.verifier().verify(&minted.secret),
        None,
        "a revoked credential stops verifying once the projection catches up"
    );
    assert_eq!(manager.monitor().report(t(0)).projected_keys, 0);
    assert!((manager.monitor().report(t(0)).projected_keys == 0));
    manager.shutdown().await;
}

/// The credential's own expiry reaches the request path through
/// `Verified::reusable_until`, which is the field's documented purpose and
/// what stops a session cache outliving the key it authenticated with.
#[tokio::test]
async fn a_credentials_own_expiry_travels_to_the_verifier() {
    let store = store().await;
    let registry = HmacRegistry::new(SECRET);
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

    let manager = project(&store, t(0)).await;
    assert_eq!(
        manager.monitor().report(t(0)).projected_keys,
        1,
        "a live credential is projected"
    );
    assert!(!(manager.monitor().report(t(0)).projected_keys == 0));
    let verified = manager
        .verifier()
        .verify(&minted.secret)
        .expect("projected");
    assert_eq!(verified.reusable_until, Some(t(100)));
    assert!(verified.is_reusable_at(t(99)));
    assert!(!verified.is_reusable_at(t(100)), "expiry is exclusive");

    // Past its own expiry the directory stops calling it active, so the next
    // projection drops it without anyone having to revoke it.
    manager.shutdown().await;
    let manager = project(&store, t(100)).await;
    assert!(
        (manager.monitor().report(t(0)).projected_keys == 0),
        "an expired credential leaves the projection on its own"
    );
    manager.shutdown().await;
}

/// Two credentials for one account is what rotation-with-overlap is made of:
/// each mints its own principal, because a principal *is* the digest
/// fingerprint, so both can be live at once and the old one retired alone.
#[tokio::test]
async fn rotation_keeps_both_credentials_live_until_the_old_one_is_retired() {
    let store = store().await;
    let registry = HmacRegistry::new(SECRET);

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

    let manager = project(&store, t(0)).await;
    assert_eq!(
        manager.monitor().report(t(0)).projected_keys,
        2,
        "both credentials are live during overlap"
    );
    assert!(manager.verifier().verify(&old.secret).is_some());
    assert!(
        manager.verifier().verify(&new.secret).is_some(),
        "overlap window"
    );

    store.revoke_key(KeyId(1), t(10)).await.unwrap();
    manager.shutdown().await;
    let manager = project(&store, t(10)).await;
    assert_eq!(
        manager.monitor().report(t(0)).projected_keys,
        1,
        "retiring one leaves exactly the other"
    );
    assert_eq!(
        manager.verifier().verify(&old.secret),
        None,
        "the old key is retired"
    );
    assert!(
        manager.verifier().verify(&new.secret).is_some(),
        "the replacement survives its predecessor's retirement"
    );
    manager.shutdown().await;
}

/// Issuance is never destructive, and never issues against an account that
/// cannot own it — both refused rather than silently absorbed.
#[tokio::test]
async fn issuance_refuses_a_duplicate_key_or_an_unknown_account() {
    let store = store().await;
    let registry = HmacRegistry::new(SECRET);
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
    let registry = HmacRegistry::new(SECRET);
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
