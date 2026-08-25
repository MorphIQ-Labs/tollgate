//! What the session cache saves, and what the digest choice underneath it
//! costs (#2).
//!
//! Two questions worth answering with numbers rather than intuition:
//!
//! 1. **How much does caching save?** Comparing a miss against a hit flatters
//!    the answer, because the miss now also *installs* the cache entry — work
//!    the uncached design never did. `verify_no_cache` is therefore the
//!    baseline the cached figure should be read against.
//! 2. **Is a cheaper digest worth taking?** Plain SHA-256 is materially faster
//!    than HMAC-SHA256. These price that swap *after* caching, which is the
//!    decision actually in front of anyone choosing a verifier.
//!
//! Absolute numbers are one machine's. Only `credential/verify_cached` is
//! gated, and only to catch the cache being bypassed entirely — the ratios,
//! not the nanoseconds, are what transfer.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use jiff::Timestamp;
use tollgate_auth::{CredentialVerifier, HmacRegistry, SessionCredential};

type HmacSha256 = Hmac<Sha256>;

const SECRET: &[u8] = b"benchmark-server-secret";
const CREDENTIAL: &[u8] = b"demo-key-1";

/// The caller's clock, read once outside the measured region — the request
/// path never reads one itself.
fn now() -> Timestamp {
    Timestamp::from_second(1_755_600_000).expect("in range")
}

fn registry() -> HmacRegistry {
    let mut registry = HmacRegistry::new(SECRET);
    registry.register(CREDENTIAL);
    registry
}

fn bench_credential(c: &mut Criterion) {
    let mut group = c.benchmark_group("credential");
    let registry = registry();

    // The uncached per-request cost: one HMAC over the credential, a
    // fingerprint, a map lookup and a constant-time compare.
    group.bench_function("verify_no_cache", |b| {
        b.iter(|| black_box(registry.verify(black_box(CREDENTIAL))))
    });

    // A cache miss: the same work, plus installing the entry. Dearer than the
    // line above by exactly that install, which is paid once per session
    // rather than once per request. A fresh session per iteration is what
    // forces the miss; criterion excludes the setup from the timing.
    let at = now();
    group.bench_function("verify_uncached", |b| {
        b.iter_batched(
            SessionCredential::new,
            |session| black_box(session.authenticate(black_box(Some(CREDENTIAL)), &registry, at)),
            criterion::BatchSize::SmallInput,
        )
    });

    // A cache hit: one atomic load and one constant-time comparison.
    let warm = SessionCredential::new();
    assert!(
        warm.authenticate(Some(CREDENTIAL), &registry, at).is_some(),
        "the cache must be warm before measuring a hit"
    );
    group.bench_function("verify_cached", |b| {
        b.iter(|| black_box(warm.authenticate(black_box(Some(CREDENTIAL)), &registry, at)))
    });

    group.finish();
}

/// The digest primitives alone — the whole of the HMAC-versus-SHA-256
/// question. Same input, same crates, so the difference is the construction
/// and nothing else.
fn bench_digest(c: &mut Criterion) {
    let mut group = c.benchmark_group("credential_digest");

    group.bench_function("hmac_sha256", |b| {
        b.iter(|| {
            let mut mac =
                HmacSha256::new_from_slice(black_box(SECRET)).expect("any key length works");
            mac.update(black_box(CREDENTIAL));
            black_box(mac.finalize().into_bytes())
        })
    });

    group.bench_function("sha256", |b| {
        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(black_box(CREDENTIAL));
            black_box(hasher.finalize())
        })
    });

    group.finish();
}

criterion_group!(benches, bench_credential, bench_digest);
criterion_main!(benches);
