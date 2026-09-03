use std::hint::black_box;

use jiff::Timestamp;
use tollgate_alloc_count::AllocScope;
use tollgate_auth::{HmacRegistry, SessionCredential};

tollgate_alloc_count::install!();

fn timestamp() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

#[test]
fn a_cached_credential_allocates_nothing() {
    let registry = HmacRegistry::new(b"allocation-test-secret");
    registry.install_credentials([b"credential-one".as_slice()]);
    let session = SessionCredential::new();
    let now = timestamp();

    let (_, cold) = AllocScope::measure(|| {
        black_box(
            session
                .authenticate(Some(b"credential-one"), &registry, now)
                .expect("registered credential"),
        );
    });
    tollgate_alloc_count::record_if_requested!("auth/cache_miss", "tollgate_cold_path", cold)
        .unwrap();
    assert!(
        !cold.is_allocation_free(),
        "the cache-miss control must prove the counter is live"
    );

    // Warm ArcSwap's current-thread debt state independently of the miss.
    black_box(
        session
            .authenticate(Some(b"credential-one"), &registry, now)
            .expect("cached credential"),
    );

    let (_, cached) = AllocScope::measure(|| {
        black_box(
            session
                .authenticate(Some(b"credential-one"), &registry, now)
                .expect("cached credential"),
        );
    });
    tollgate_alloc_count::record_if_requested!("auth/cache_hit", "tollgate", cached).unwrap();
    assert!(
        cached.is_allocation_free(),
        "cached authentication allocated: {cached:?}"
    );
}
