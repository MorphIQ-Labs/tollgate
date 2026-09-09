use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tollgate_auth::{CredentialVerifier, HmacRegistry, SessionCredential};
use tollgate_client::{
    KeyManager, KeyManagerConfig, KeyManagerHealth, KeyManagerMonitor, KeyManagerReport,
};
use tollgate_core::KeyId;
use tollgate_store::{Clock, CredentialRecord, CredentialSet, KeySource, ManualClock, StoreError};

const SECRET: &[u8] = b"fixture-key-manager-hmac-secret-108";
fn t(second: i64) -> Timestamp {
    Timestamp::from_second(second).unwrap()
}
fn config() -> KeyManagerConfig {
    KeyManagerConfig {
        refresh_interval: Duration::from_secs(5),
        fetch_timeout: Duration::from_secs(2),
        pass_timeout: Duration::from_secs(5),
        max_age: Duration::from_secs(20),
        shutdown_timeout: Duration::from_secs(1),
        ..KeyManagerConfig::default()
    }
}

#[derive(Clone)]
enum Reply {
    Keys(CredentialSet),
    AdvanceClock(CredentialSet, Arc<ManualClock>, Timestamp),
    Error,
    Hung,
    Panic,
}
struct Source {
    reply: Mutex<Reply>,
    active: AtomicUsize,
    cancelled: AtomicUsize,
}
impl Source {
    fn new(keys: Vec<CredentialRecord>) -> Arc<Self> {
        Arc::new(Self {
            reply: Mutex::new(Reply::Keys(CredentialSet::try_new(keys).unwrap())),
            active: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
        })
    }
    fn set(&self, reply: Reply) {
        *self.reply.lock().unwrap() = reply;
    }
}
#[async_trait]
impl KeySource for Source {
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        let reply = self.reply.lock().unwrap().clone();
        match reply {
            Reply::Keys(keys) => page(keys, now, after, limit),
            Reply::AdvanceClock(keys, clock, now) => {
                clock.set(now);
                page(keys, now, after, limit)
            }
            Reply::Error => Err(StoreError("fixture outage".into())),
            Reply::Panic => panic!("fixture task death"),
            Reply::Hung => {
                struct Pending<'a>(&'a Source);
                impl Drop for Pending<'_> {
                    fn drop(&mut self) {
                        self.0.active.fetch_sub(1, Ordering::SeqCst);
                        self.0.cancelled.fetch_add(1, Ordering::SeqCst);
                    }
                }
                self.active.fetch_add(1, Ordering::SeqCst);
                let _pending = Pending(self);
                std::future::pending().await
            }
        }
    }
}
fn page(
    keys: CredentialSet,
    now: Timestamp,
    after: Option<KeyId>,
    limit: std::num::NonZeroUsize,
) -> Result<tollgate_store::KeyPage, StoreError> {
    let mut records: Vec<_> = keys
        .into_records()
        .into_iter()
        .filter(|key| after.is_none_or(|cursor| key.key_id > cursor))
        .filter(|key| key.not_after.is_none_or(|end| now < end))
        .take(limit.get() + 1)
        .collect();
    let next = if records.len() > limit.get() {
        records.pop();
        records.last().map(|key| key.key_id)
    } else {
        None
    };
    tollgate_store::KeyPage::try_new(1, now, after, limit, records, next)
}
fn minted(id: u128, until: Option<Timestamp>) -> (Vec<u8>, CredentialRecord) {
    let minted = HmacRegistry::new(SECRET).mint(KeyId(id)).unwrap();
    (
        minted.secret.to_vec(),
        CredentialRecord {
            key_id: minted.key_id,
            principal: minted.principal,
            digest: minted.digest,
            not_after: until,
        },
    )
}
async fn wait_for(
    monitor: &mut KeyManagerMonitor,
    now: Timestamp,
    condition: impl Fn(&KeyManagerReport) -> bool,
) -> KeyManagerReport {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let report = monitor.report(now);
            if condition(&report) {
                return report;
            }
            monitor
                .changed()
                .await
                .expect("task must remain alive until the expected progress");
        }
    })
    .await
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn complete_refresh_replaces_keys_and_cached_proofs_keep_their_original_deadline() {
    let (first, a) = minted(1, None);
    let (second, b) = minted(2, Some(t(113)));
    let source = Source::new(vec![a]);
    let clock = Arc::new(ManualClock::new(t(100)));
    let manager = KeyManager::spawn(source.clone(), SECRET, clock.clone(), config()).unwrap();
    let verifier = manager.verifier();
    let mut monitor = manager.monitor();
    assert!(!monitor.report(t(100)).ready);
    wait_for(&mut monitor, t(100), |r| r.ready).await;
    let session = SessionCredential::new();
    assert_eq!(
        session.authenticate(Some(&first), &verifier, t(100)),
        Some(a.principal)
    );
    assert_eq!(
        verifier.verify(&first).unwrap().reusable_until,
        Some(t(120))
    );
    source.set(Reply::Keys(CredentialSet::try_new(vec![b]).unwrap()));
    clock.set(t(105));
    tokio::time::advance(Duration::from_secs(5)).await;
    wait_for(&mut monitor, t(105), |r| r.stats.refreshes == 2).await;
    assert!(verifier.verify(&first).is_none());
    assert_eq!(
        verifier.verify(&second).unwrap().reusable_until,
        Some(t(113))
    );
    assert_eq!(
        session.authenticate(Some(&first), &verifier, t(119)),
        Some(a.principal)
    );
    assert!(
        session
            .authenticate(Some(&first), &verifier, t(120))
            .is_none()
    );
    let new_session = SessionCredential::new();
    assert!(
        new_session
            .authenticate(Some(&second), &verifier, t(113))
            .is_none()
    );
    source.set(Reply::Keys(CredentialSet::try_new(vec![]).unwrap()));
    clock.set(t(110));
    tokio::time::advance(Duration::from_secs(5)).await;
    let report = wait_for(&mut monitor, t(110), |r| r.stats.refreshes == 3).await;
    assert!(
        report.ready,
        "an authoritative empty feed is a healthy read"
    );
    assert_eq!(report.projected_keys, 0);
    assert!(verifier.verify(&second).is_none());
    assert!(!manager.shutdown().await.task_failed);
    assert!(!monitor.report(t(110)).ready);
}

#[tokio::test(start_paused = true)]
async fn an_outage_never_extends_projection_validity_and_recovery_replaces_it() {
    let (token, key) = minted(1, None);
    let source = Source::new(vec![key]);
    let clock = Arc::new(ManualClock::new(t(100)));
    let manager = KeyManager::spawn(source.clone(), SECRET, clock.clone(), config()).unwrap();
    let verifier = manager.verifier();
    let mut monitor = manager.monitor();
    wait_for(&mut monitor, t(100), |r| r.ready).await;
    source.set(Reply::Error);
    clock.set(t(105));
    tokio::time::advance(Duration::from_secs(5)).await;
    let report = wait_for(&mut monitor, t(105), |r| r.stats.failures == 1).await;
    assert_eq!(report.health, KeyManagerHealth::Degraded);
    assert!(report.ready);
    assert_eq!(report.usable_until, Some(t(120)));
    let session = SessionCredential::new();
    assert!(
        session
            .authenticate(Some(&token), &verifier, t(119))
            .is_some()
    );
    assert!(!monitor.report(t(120)).ready);
    assert!(
        session
            .authenticate(Some(&token), &verifier, t(120))
            .is_none()
    );
    source.set(Reply::Keys(CredentialSet::try_new(vec![key]).unwrap()));
    clock.set(t(120));
    tokio::time::advance(Duration::from_secs(5)).await;
    let report = wait_for(&mut monitor, t(120), |r| r.stats.refreshes == 2).await;
    assert!(report.ready);
    assert_eq!(report.usable_until, Some(t(140)));
    assert!(
        session
            .authenticate(Some(&token), &verifier, t(120))
            .is_some()
    );
    assert!(!manager.shutdown().await.task_failed);
}

#[tokio::test(start_paused = true)]
async fn hung_fetches_timeout_retry_and_shutdown_interrupts_the_pending_read() {
    let source = Source::new(vec![]);
    source.set(Reply::Hung);
    let clock = Arc::new(ManualClock::new(t(100)));
    let manager = KeyManager::spawn(source.clone(), SECRET, clock, config()).unwrap();
    let mut monitor = manager.monitor();
    tokio::task::yield_now().await;
    assert_eq!(source.active.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    let report = wait_for(&mut monitor, t(100), |r| r.stats.timeouts == 1).await;
    assert!(!report.ready);
    assert_eq!(report.stats.failures, 1);
    assert_eq!(source.cancelled.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(source.active.load(Ordering::SeqCst), 1);
    let start = tokio::time::Instant::now();
    let shutdown = manager.shutdown().await;
    assert_eq!(start, tokio::time::Instant::now());
    assert!(!shutdown.task_failed && !shutdown.deadline_expired);
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    assert_eq!(source.cancelled.load(Ordering::SeqCst), 2);
    assert_eq!(monitor.report(t(100)).health, KeyManagerHealth::Stopped);
    assert!(!monitor.report(t(100)).ready);
}

#[tokio::test(start_paused = true)]
async fn dropping_an_unpolled_shutdown_future_aborts_the_owned_refresh() {
    let source = Source::new(vec![]);
    source.set(Reply::Hung);
    let manager = KeyManager::spawn(
        source.clone(),
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    let monitor = manager.monitor();
    tokio::task::yield_now().await;
    assert_eq!(source.active.load(Ordering::SeqCst), 1);
    drop(manager.shutdown());
    tokio::task::yield_now().await;
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    assert_eq!(monitor.report(t(100)).health, KeyManagerHealth::Failed);
}

#[tokio::test(start_paused = true)]
async fn task_death_withdraws_new_verification_and_is_visible_to_the_monitor() {
    let (token, key) = minted(1, None);
    let source = Source::new(vec![key]);
    let manager = KeyManager::spawn(
        source.clone(),
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    let verifier = manager.verifier();
    let mut monitor = manager.monitor();
    wait_for(&mut monitor, t(100), |r| r.ready).await;
    source.set(Reply::Panic);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(monitor.report(t(100)).health, KeyManagerHealth::Failed);
    assert!(!monitor.report(t(100)).ready);
    assert!(verifier.verify(&token).is_none());
    assert!(manager.shutdown().await.task_failed);
}

#[test]
fn unusable_key_refresh_configuration_is_rejected_before_starting() {
    for bad in [
        Duration::ZERO,
        Duration::MAX,
        Duration::from_secs(i64::MAX as u64),
    ] {
        for field in 0..5 {
            let mut config = config();
            match field {
                0 => config.refresh_interval = bad,
                1 => config.fetch_timeout = bad,
                2 => config.max_age = bad,
                3 => config.shutdown_timeout = bad,
                _ => config.pass_timeout = bad,
            }
            assert!(config.validate().is_err());
        }
    }
    let mut config = config();
    config.max_age = config.refresh_interval + config.pass_timeout * 2;
    assert!(config.validate().is_err());
    config.max_age += Duration::from_nanos(1);
    assert!(config.validate().is_ok());
    config.fetch_timeout = config.pass_timeout;
    assert!(config.validate().is_ok());
    config.fetch_timeout += Duration::from_nanos(1);
    assert!(config.validate().is_err());
}

#[tokio::test(start_paused = true)]
async fn a_large_legitimate_projection_is_installed_without_truncation() {
    let records = (0..20_000u128)
        .map(|id| {
            let mut digest = [0; 32];
            digest[..16].copy_from_slice(&id.to_be_bytes());
            CredentialRecord {
                key_id: KeyId(id),
                principal: tollgate_core::Principal(id),
                digest,
                not_after: None,
            }
        })
        .collect();
    let manager = KeyManager::spawn(
        Source::new(records),
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        config(),
    )
    .unwrap();
    let mut monitor = manager.monitor();
    let report = wait_for(&mut monitor, t(100), |r| r.ready).await;
    assert_eq!(report.projected_keys, 20_000);
    assert_eq!(report.fetched_at, Some(t(100)));
    assert_eq!(
        report.usable_until,
        Some(t(100).checked_add(SignedDuration::from_secs(20)).unwrap())
    );
    assert!(!manager.shutdown().await.task_failed);
}

#[tokio::test(start_paused = true)]
async fn fetch_time_consumes_freshness_and_expired_responses_cannot_replace_the_table() {
    for completed in [119, 120, 121] {
        let (token, key) = minted(1, None);
        let clock = Arc::new(ManualClock::new(t(100)));
        let source = Source::new(vec![]);
        source.set(Reply::AdvanceClock(
            CredentialSet::try_new(vec![key]).unwrap(),
            clock.clone(),
            t(completed),
        ));
        let manager = KeyManager::spawn(source, SECRET, clock, config()).unwrap();
        let mut monitor = manager.monitor();
        let report = wait_for(&mut monitor, t(completed), |r| {
            r.stats.refreshes + r.stats.failures == 1
        })
        .await;
        assert_eq!(report.ready, completed < 120);
        if completed < 120 {
            assert_eq!(
                manager.verifier().verify(&token).unwrap().reusable_until,
                Some(t(120))
            );
            assert_eq!(report.fetched_at, Some(t(100)));
        } else {
            assert!(manager.verifier().verify(&token).is_none());
            assert_eq!(report.stats.failures, 1);
        }
        assert!(!manager.shutdown().await.task_failed);
    }
}

#[tokio::test(start_paused = true)]
async fn timestamp_overflow_cannot_renew_a_previously_valid_projection() {
    let (token, key) = minted(1, None);
    let clock = Arc::new(ManualClock::new(t(100)));
    let source = Source::new(vec![key]);
    assert!(KeyManager::spawn(source.clone(), b"short", clock.clone(), config()).is_err());
    assert!(
        KeyManager::spawn(
            source.clone(),
            SECRET,
            Arc::new(ManualClock::new(Timestamp::MAX)),
            config()
        )
        .is_err()
    );
    let manager = KeyManager::spawn(source, SECRET, clock.clone(), config()).unwrap();
    let mut monitor = manager.monitor();
    wait_for(&mut monitor, t(100), |r| r.ready).await;
    clock.set(Timestamp::MAX);
    tokio::time::advance(Duration::from_secs(5)).await;
    let report = wait_for(&mut monitor, Timestamp::MAX, |r| r.stats.failures == 1).await;
    assert!(!report.ready);
    assert_eq!(report.usable_until, Some(t(120)));
    assert_eq!(
        manager.verifier().verify(&token).unwrap().reusable_until,
        Some(t(120))
    );
    assert!(!manager.shutdown().await.task_failed);
}

struct Scripted {
    replies: Mutex<std::collections::VecDeque<Result<tollgate_store::KeyPage, StoreError>>>,
    cursors: Mutex<Vec<Option<KeyId>>>,
}
#[async_trait]
impl KeySource for Scripted {
    async fn active_keys_page(
        &self,
        _: Timestamp,
        after: Option<KeyId>,
        _: std::num::NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        self.cursors.lock().unwrap().push(after);
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(StoreError("script exhausted".into())))
    }
}
fn scripted(replies: Vec<tollgate_store::KeyPage>) -> Arc<Scripted> {
    Arc::new(Scripted {
        replies: Mutex::new(replies.into_iter().map(Ok).collect()),
        cursors: Mutex::new(vec![]),
    })
}
fn one_page(
    revision: u64,
    after: Option<KeyId>,
    record: CredentialRecord,
    more: bool,
) -> tollgate_store::KeyPage {
    tollgate_store::KeyPage::try_new(
        revision,
        t(100),
        after,
        std::num::NonZeroUsize::new(1).unwrap(),
        vec![record],
        more.then_some(record.key_id),
    )
    .unwrap()
}
fn paged_config(max_pages: usize) -> KeyManagerConfig {
    KeyManagerConfig {
        page_limit: std::num::NonZeroUsize::new(1).unwrap(),
        max_pages: std::num::NonZeroUsize::new(max_pages).unwrap(),
        ..config()
    }
}

#[tokio::test(start_paused = true)]
async fn revision_change_restarts_from_the_beginning_instead_of_omitting_a_new_key() {
    let (early, a) = minted(1, None);
    let (retired, b) = minted(2, None);
    let (last, c) = minted(3, None);
    // Between pages: revoke 2, insert 1 behind the cursor. Never publish 2+3.
    let source = scripted(vec![
        one_page(1, None, b, true),
        one_page(2, Some(b.key_id), c, false),
        one_page(2, None, a, true),
        one_page(2, Some(a.key_id), c, false),
    ]);
    let manager = KeyManager::spawn(
        source.clone(),
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        paged_config(4),
    )
    .unwrap();
    let verifier = manager.verifier();
    let report = wait_for(&mut manager.monitor(), t(100), |r| r.ready).await;
    assert_eq!(
        (
            report.stats.pages,
            report.stats.revision_conflicts,
            report.projected_keys
        ),
        (4, 1, 2)
    );
    assert_eq!(report.revision, Some(2));
    assert!(verifier.verify(&retired).is_none());
    assert!(verifier.verify(&early).is_some() && verifier.verify(&last).is_some());
    assert_eq!(
        *source.cursors.lock().unwrap(),
        vec![None, Some(KeyId(2)), None, Some(KeyId(1))]
    );
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn page_budget_and_revision_regression_preserve_the_previous_table_and_deadline() {
    let (old, a) = minted(1, None);
    let (new, b) = minted(2, None);
    let source = scripted(vec![
        one_page(3, None, a, false),
        one_page(4, None, a, true),
        one_page(5, Some(a.key_id), b, false),
        one_page(2, None, b, false),
    ]);
    let clock = Arc::new(ManualClock::new(t(100)));
    let manager = KeyManager::spawn(source, SECRET, clock.clone(), paged_config(2)).unwrap();
    let verifier = manager.verifier();
    let mut monitor = manager.monitor();
    wait_for(&mut monitor, t(100), |r| r.ready).await;
    for failures in 1..=2 {
        clock.set(t(100 + failures * 5));
        tokio::time::advance(Duration::from_secs(5)).await;
        let r = wait_for(&mut monitor, clock.now(), |r| {
            r.stats.failures == failures as u64
        })
        .await;
        assert_eq!(r.health, KeyManagerHealth::Degraded);
        assert_eq!(r.stats.page_budget_exceeded, 1);
        assert_eq!(r.revision, Some(3));
        assert_eq!(r.usable_until, Some(t(120)));
        assert!(verifier.verify(&new).is_none());
        assert_eq!(verifier.verify(&old).unwrap().reusable_until, Some(t(120)));
    }
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn page_budget_cannot_publish_an_incomplete_first_table() {
    let (_, a) = minted(1, None);
    let manager = KeyManager::spawn(
        scripted(vec![one_page(1, None, a, true)]),
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        paged_config(1),
    )
    .unwrap();
    let r = wait_for(&mut manager.monitor(), t(100), |r| r.stats.failures == 1).await;
    assert!(!r.ready);
    assert_eq!(r.projected_keys, 0);
    assert_eq!(r.stats.page_budget_exceeded, 1);
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn the_instance_clock_narrows_the_server_set_at_publication() {
    let (secret, a) = minted(1, Some(t(105)));
    let manager = KeyManager::spawn(
        scripted(vec![one_page(1, None, a, false)]),
        SECRET,
        Arc::new(ManualClock::new(t(105))),
        paged_config(1),
    )
    .unwrap();
    let r = wait_for(&mut manager.monitor(), t(105), |r| r.ready).await;
    assert_eq!(r.projected_keys, 0);
    assert!(manager.verifier().verify(&secret).is_none());
    manager.shutdown().await;
}

struct SlowPages(Arc<Scripted>);
#[async_trait]
impl KeySource for SlowPages {
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        self.0.active_keys_page(now, after, limit).await
    }
}
#[tokio::test(start_paused = true)]
async fn per_call_progress_does_not_reset_the_pass_deadline() {
    let (_, a) = minted(1, None);
    let (_, b) = minted(2, None);
    let source = Arc::new(SlowPages(scripted(vec![
        one_page(1, None, a, true),
        one_page(1, Some(a.key_id), b, false),
    ])));
    let manager = KeyManager::spawn(
        source,
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        KeyManagerConfig {
            pass_timeout: Duration::from_millis(2500),
            ..paged_config(4)
        },
    )
    .unwrap();
    let mut monitor = manager.monitor();
    // The monitor deadline here allows the manager's two-and-a-half-second
    // budget; paused time advances to the timers without a wall-clock sleep.
    tokio::time::timeout(Duration::from_secs(3), async {
        while monitor.report(t(100)).stats.failures == 0 {
            monitor.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let report = monitor.report(t(100));
    assert_eq!(report.stats.pages, 1);
    assert_eq!(report.stats.pass_timeouts, 1);
    assert_eq!(report.stats.timeouts, 0);
    assert!(!report.ready);
    assert_eq!(report.projected_keys, 0);
    manager.shutdown().await;
}

#[tokio::test]
async fn a_32_byte_hmac_secret_is_accepted_and_31_bytes_are_refused() {
    let source = Source::new(vec![]);
    let clock = Arc::new(ManualClock::new(t(100)));
    assert!(KeyManager::spawn(source.clone(), &[1; 31], clock.clone(), config()).is_err());
    let manager = KeyManager::spawn(source, &[1; 32], clock, config()).unwrap();
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_page_for_another_request_never_publishes() {
    let (_, a) = minted(1, None);
    let source = scripted(vec![one_page(1, Some(KeyId(0)), a, false)]);
    let manager = KeyManager::spawn(
        source,
        SECRET,
        Arc::new(ManualClock::new(t(100))),
        paged_config(1),
    )
    .unwrap();
    assert_eq!(
        manager.monitor().report(t(100)).health,
        KeyManagerHealth::Starting
    );
    let r = wait_for(&mut manager.monitor(), t(100), |r| r.stats.failures == 1).await;
    assert!(!r.ready);
    assert_eq!(r.projected_keys, 0);
    manager.shutdown().await;
}
