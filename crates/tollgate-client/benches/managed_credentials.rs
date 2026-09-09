//! Managed verification against the same HMAC and finite-proof mechanisms.
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use jiff::Timestamp;
use std::{hint::black_box, sync::Arc};
use tollgate_auth::{CredentialVerifier, HmacRegistry, SessionCredential};
use tollgate_client::{KeyManager, KeyManagerConfig};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId};
use tollgate_store::{
    AccountConfig, GrantPolicy, KeyDirectory, KeyRecord, ManualClock, MemoryStore,
};

fn stamp(second: i64) -> Timestamp {
    Timestamp::from_second(second).unwrap()
}
fn benchmark(c: &mut Criterion) {
    const SECRET: &[u8] = b"fixture-managed-credential-benchmark-secret-108";
    const TOKEN: &[u8] = b"fixture-managed-credential-token";
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let registry = HmacRegistry::new(SECRET);
    let (principal, digest) = registry.digest_credential(TOKEN);
    registry.install([(principal, digest, Some(stamp(120)))]);
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let manager = rt.block_on(async {
        store
            .insert_key(KeyRecord {
                key_id: KeyId(1),
                account_id: AccountId(1),
                principal,
                digest,
                not_after: Some(stamp(120)),
            })
            .await
            .unwrap();
        let manager = KeyManager::spawn(
            store,
            SECRET,
            Arc::new(ManualClock::new(stamp(100))),
            KeyManagerConfig::default(),
        )
        .unwrap();
        let mut monitor = manager.monitor();
        while !monitor.report(stamp(100)).ready {
            monitor.changed().await.unwrap();
        }
        manager
    });
    let verifier = manager.verifier();
    let managed_session = SessionCredential::new();
    let direct_session = SessionCredential::new();
    assert_eq!(
        managed_session.authenticate(Some(TOKEN), &verifier, stamp(100)),
        Some(principal)
    );
    assert_eq!(
        direct_session.authenticate(Some(TOKEN), &registry, stamp(100)),
        Some(principal)
    );
    let mut group = c.benchmark_group("managed_credential");
    group.bench_function("cached", |b| {
        b.iter(|| {
            black_box(managed_session.authenticate(
                Some(black_box(TOKEN)),
                &verifier,
                black_box(stamp(110)),
            ))
        })
    });
    group.bench_function("cached_direct", |b| {
        b.iter(|| {
            black_box(direct_session.authenticate(
                Some(black_box(TOKEN)),
                &registry,
                black_box(stamp(110)),
            ))
        })
    });
    group.bench_function("verify", |b| {
        b.iter(|| black_box(verifier.verify(black_box(TOKEN))))
    });
    group.bench_function("verify_direct", |b| {
        b.iter(|| black_box(registry.verify(black_box(TOKEN))))
    });
    group.bench_function("cold_session", |b| {
        b.iter_batched(
            SessionCredential::new,
            |session| {
                black_box(session.authenticate(
                    Some(black_box(TOKEN)),
                    &verifier,
                    black_box(stamp(110)),
                ))
            },
            BatchSize::SmallInput,
        )
    });
    let old = HmacRegistry::new(SECRET);
    old.install([(principal, digest, Some(stamp(105)))]);
    group.bench_function("renew_session", |b| {
        b.iter_batched(
            || {
                let session = SessionCredential::new();
                assert_eq!(
                    session.authenticate(Some(TOKEN), &old, stamp(100)),
                    Some(principal)
                );
                session
            },
            |session| {
                black_box(session.authenticate(
                    Some(black_box(TOKEN)),
                    &verifier,
                    black_box(stamp(110)),
                ))
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("expired_session", |b| {
        b.iter_batched(
            || {
                let session = SessionCredential::new();
                assert_eq!(
                    session.authenticate(Some(TOKEN), &verifier, stamp(100)),
                    Some(principal)
                );
                session
            },
            |session| {
                black_box(session.authenticate(
                    Some(black_box(TOKEN)),
                    &verifier,
                    black_box(stamp(120)),
                ))
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
    rt.block_on(manager.shutdown());
}
criterion_group!(benches, benchmark);
criterion_main!(benches);
