//! Control-plane scale witnesses, intentionally separate from request latency.
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use jiff::Timestamp;
use std::{hint::black_box, num::NonZeroUsize, sync::Arc};
use tollgate_client::{KeyManager, KeyManagerConfig};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId, Principal};
use tollgate_store::{
    AccountConfig, GrantPolicy, KeyDirectory, KeyRecord, KeySource, ManualClock, MemoryStore,
};

async fn catalogue(active: u128, retired: u128) -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    for id in 0..active + retired {
        let mut digest = [0; 32];
        digest[..16].copy_from_slice(&id.to_be_bytes());
        store
            .insert_key(KeyRecord {
                key_id: KeyId(id),
                account_id: AccountId(1),
                principal: Principal(id),
                digest,
                not_after: None,
            })
            .await
            .unwrap();
        if id < retired {
            store
                .revoke_key(KeyId(id), Timestamp::UNIX_EPOCH)
                .await
                .unwrap();
        }
    }
    store
}
fn benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let now = Timestamp::UNIX_EPOCH;
    let mut group = c.benchmark_group("credential_page");
    for retired in [0, 1_000, 100_000] {
        let store = rt.block_on(catalogue(256, retired));
        group.bench_with_input(
            BenchmarkId::new("retired_history", retired),
            &store,
            |b, store| {
                b.iter(|| {
                    black_box(
                        rt.block_on(store.active_keys_page(
                            now,
                            None,
                            NonZeroUsize::new(64).unwrap(),
                        ))
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
    let mut group = c.benchmark_group("credential_projection");
    for count in [256, 4096, 16384] {
        let store = rt.block_on(catalogue(count, 0));
        group.bench_with_input(
            BenchmarkId::new("initial_pass", count),
            &store,
            |b, store| {
                b.iter(|| {
                    rt.block_on(async {
                        let manager = KeyManager::spawn(
                            store.clone(),
                            b"fixture-projection-scaling-secret-108",
                            Arc::new(ManualClock::new(now)),
                            KeyManagerConfig::default(),
                        )
                        .unwrap();
                        let mut monitor = manager.monitor();
                        while !monitor.report(now).ready {
                            monitor.changed().await.unwrap();
                        }
                        assert_eq!(monitor.report(now).projected_keys, count as usize);
                        black_box(manager.shutdown().await);
                    });
                })
            },
        );
    }
    group.finish();
}
criterion_group!(benches, benchmark);
criterion_main!(benches);
