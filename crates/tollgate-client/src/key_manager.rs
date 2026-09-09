//! Owned, bounded refresh of a read-only credential projection.
//!
//! Run beside `InstanceRuntime`, using its snapshot refresh cadence. Keep the
//! returned verifier in HTTP authentication state and combine the monitor's
//! readiness with admission readiness. A credential's verification evidence is
//! bounded by both its own expiry and this feed's freshness deadline, so cached
//! sessions expire within `max_age` of the last successful fetch's start, even
//! after removal or an outage. Steady-state session
//! authentication continues to use `SessionCredential` without another lookup.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::watch;
use tollgate_auth::{CredentialVerifier, HmacRegistry, Verified};
use tollgate_store::{Clock, CredentialSet, KeySource, StoreError};
use zeroize::Zeroizing;

#[derive(Debug, Clone)]
pub struct KeyManagerConfig {
    /// Pause after each completed attempt. The first fetch starts immediately.
    pub refresh_interval: Duration,
    pub fetch_timeout: Duration,
    /// One budget for every page, restart, and projection build.
    pub pass_timeout: Duration,
    pub page_limit: NonZeroUsize,
    /// Total page calls per pass, including revision-conflict retries.
    pub max_pages: NonZeroUsize,
    /// Maximum evidence lifetime, measured from fetch START, including I/O.
    pub max_age: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for KeyManagerConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(5),
            fetch_timeout: Duration::from_secs(5),
            pass_timeout: Duration::from_secs(10),
            page_limit: tollgate_store::DEFAULT_KEY_PAGE_LIMIT,
            max_pages: NonZeroUsize::new(1024).unwrap(),
            max_age: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyManagerConfigError(pub &'static str);
impl std::fmt::Display for KeyManagerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for KeyManagerConfigError {}

impl KeyManagerConfig {
    pub fn validate(&self) -> Result<(), KeyManagerConfigError> {
        for duration in [
            self.refresh_interval,
            self.fetch_timeout,
            self.pass_timeout,
            self.max_age,
            self.shutdown_timeout,
        ] {
            if duration.is_zero()
                || tokio::time::Instant::now().checked_add(duration).is_none()
                || SignedDuration::try_from(duration).is_err()
            {
                return Err(KeyManagerConfigError(
                    "key manager durations must be positive and representable",
                ));
            }
        }
        tollgate_store::validate_key_page_limit(self.page_limit)
            .map_err(|_| KeyManagerConfigError("credential page limit exceeds 4096"))?;
        if self.fetch_timeout > self.pass_timeout {
            return Err(KeyManagerConfigError(
                "key fetch timeout exceeds the pass budget",
            ));
        }
        // Freshness starts before the prior successful fetch. Cover that
        // fetch, the pause, and the next fetch before it can replace the table.
        if self
            .pass_timeout
            .checked_mul(2)
            .and_then(|fetches| self.refresh_interval.checked_add(fetches))
            .is_none_or(|cycle| cycle >= self.max_age)
        {
            return Err(KeyManagerConfigError(
                "key projection max_age must exceed refresh_interval + 2 * pass_timeout",
            ));
        }
        Ok(())
    }
}

struct Projection {
    revision: u64,
    registry: HmacRegistry,
    fetched_at: Timestamp,
    usable_until: Timestamp,
    projected_keys: usize,
}

/// Verification capability only: callers cannot install an indefinite table,
/// mutate its freshness deadline, or mint with the manager's HMAC secret.
#[derive(Clone, Default)]
pub struct KeyVerifier {
    current: Arc<ArcSwapOption<Projection>>,
}

impl CredentialVerifier for KeyVerifier {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        self.current.load().as_ref()?.registry.verify(credential)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyManagerHealth {
    Starting,
    Healthy,
    Degraded,
    Stopped,
    Failed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct KeyManagerStats {
    pub attempts: u64,
    pub refreshes: u64,
    pub failures: u64,
    pub timeouts: u64,
    pub pages: u64,
    pub revision_conflicts: u64,
    pub pass_timeouts: u64,
    pub page_budget_exceeded: u64,
    pub counter_overflow: bool,
}

impl KeyManagerStats {
    fn increment(value: &mut u64, overflow: &mut bool) {
        if let Some(next) = value.checked_add(1) {
            *value = next;
        } else {
            *overflow = true;
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Progress {
    health: KeyManagerHealth,
    stats: KeyManagerStats,
}

#[derive(Debug, Clone, Copy)]
pub struct KeyManagerReport {
    pub health: KeyManagerHealth,
    pub stats: KeyManagerStats,
    /// Fresh feed and live task; an authoritative empty set is still fresh.
    pub ready: bool,
    /// Entries in the installed projection, not a count of usable customers.
    pub projected_keys: usize,
    pub revision: Option<u64>,
    pub fetched_at: Option<Timestamp>,
    pub usable_until: Option<Timestamp>,
}

#[derive(Clone)]
pub struct KeyManagerMonitor {
    progress: watch::Receiver<Progress>,
    verifier: KeyVerifier,
}
impl KeyManagerMonitor {
    pub fn report(&self, now: Timestamp) -> KeyManagerReport {
        let exited = self.progress.has_changed().is_err();
        let mut progress = *self.progress.borrow();
        if exited && progress.health != KeyManagerHealth::Stopped {
            progress.health = KeyManagerHealth::Failed;
        }
        let projection = self.verifier.current.load();
        let running = matches!(
            progress.health,
            KeyManagerHealth::Healthy | KeyManagerHealth::Degraded
        );
        KeyManagerReport {
            health: progress.health,
            stats: progress.stats,
            ready: running && projection.as_ref().is_some_and(|p| now < p.usable_until),
            projected_keys: projection.as_ref().map_or(0, |p| p.projected_keys),
            revision: projection.as_ref().map(|p| p.revision),
            fetched_at: projection.as_ref().map(|p| p.fetched_at),
            usable_until: projection.as_ref().map(|p| p.usable_until),
        }
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.progress.changed().await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct KeyManagerShutdownReport {
    pub stats: KeyManagerStats,
    pub task_failed: bool,
    pub deadline_expired: bool,
}

#[must_use = "retain the key manager to own credential refresh"]
pub struct KeyManager {
    task: Option<tokio::task::JoinHandle<()>>,
    stop: watch::Sender<bool>,
    monitor: KeyManagerMonitor,
    shutdown_timeout: Duration,
}

impl KeyManager {
    pub fn spawn(
        source: Arc<dyn KeySource>,
        secret: &[u8],
        clock: Arc<dyn Clock>,
        config: KeyManagerConfig,
    ) -> Result<Self, KeyManagerConfigError> {
        config.validate()?;
        if secret.len() < 32 {
            return Err(KeyManagerConfigError(
                "credential projection HMAC secret must contain at least 32 bytes",
            ));
        }
        let max_age = SignedDuration::try_from(config.max_age)
            .map_err(|_| KeyManagerConfigError("key projection max_age overflow"))?;
        clock.now().checked_add(max_age).map_err(|_| {
            KeyManagerConfigError("key projection deadline exceeds the timestamp range")
        })?;
        let verifier = KeyVerifier::default();
        let (stop, stopping) = watch::channel(false);
        let (publisher, progress) = watch::channel(Progress {
            health: KeyManagerHealth::Starting,
            stats: KeyManagerStats::default(),
        });
        let monitor = KeyManagerMonitor {
            progress,
            verifier: verifier.clone(),
        };
        let shutdown_timeout = config.shutdown_timeout;
        let secret = Zeroizing::new(secret.to_vec());
        let task = tokio::spawn(run(
            source,
            secret,
            clock,
            config,
            max_age,
            stopping,
            Publisher {
                verifier,
                sender: publisher,
            },
        ));
        Ok(Self {
            task: Some(task),
            stop,
            monitor,
            shutdown_timeout,
        })
    }

    pub fn verifier(&self) -> KeyVerifier {
        self.monitor.verifier.clone()
    }
    pub fn monitor(&self) -> KeyManagerMonitor {
        self.monitor.clone()
    }

    pub async fn shutdown(mut self) -> KeyManagerShutdownReport {
        crate::signal(&self.stop, true, "key-manager shutdown");
        let joined = tokio::time::timeout(
            self.shutdown_timeout,
            self.task.as_mut().expect("key manager owns its task"),
        )
        .await;
        let (task_failed, deadline_expired) = match joined {
            Ok(Ok(())) => (false, false),
            Ok(Err(error)) => {
                tracing::error!(%error, "key manager task died");
                (true, false)
            }
            Err(_) => {
                tracing::error!("key manager shutdown deadline expired");
                (true, true)
            }
        };
        KeyManagerShutdownReport {
            stats: self.monitor.progress.borrow().stats,
            task_failed,
            deadline_expired,
        }
    }
}

impl Drop for KeyManager {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

// Owned by the task, including during a pending fetch and unwind. New
// verifications stop when it drops; already cached proofs retain only their
// original bounded lifetime. No request-path watcher or clock read is needed.
struct Publisher {
    verifier: KeyVerifier,
    sender: watch::Sender<Progress>,
}
impl Drop for Publisher {
    fn drop(&mut self) {
        self.verifier.current.store(None);
    }
}

fn projection(
    secret: &[u8],
    keys: CredentialSet,
    started: Timestamp,
    until: Timestamp,
    clock: &dyn Clock,
    latest_source_time: Timestamp,
) -> Result<Projection, StoreError> {
    let now = clock.now().max(latest_source_time);
    let registry = HmacRegistry::new(secret);
    // Validation was owned by CredentialSet. An expiry reached while reading
    // is normal, not malformed input; expired entries cannot receive fresh life.
    registry.install(
        keys.records()
            .iter()
            .filter(|key| key.not_after.is_none_or(|end| now < end))
            .map(|key| {
                (
                    key.principal,
                    key.digest,
                    Some(key.not_after.map_or(until, |end| end.min(until))),
                )
            }),
    );
    // Building a large table consumes freshness too. This is the final check
    // before returning the complete candidate to the single publisher.
    if clock.now() >= until {
        return Err(StoreError(
            "credential projection expired before publication".into(),
        ));
    }
    let projected_keys = registry.len();
    Ok(Projection {
        revision: 0, // The owning drain attaches its coherent revision.
        registry,
        fetched_at: started,
        usable_until: until,
        projected_keys,
    })
}

async fn refresh(
    source: &dyn KeySource,
    secret: &[u8],
    clock: &dyn Clock,
    config: &KeyManagerConfig,
    max_age: SignedDuration,
    started: Timestamp,
    stats: &mut KeyManagerStats,
) -> Result<Projection, &'static str> {
    use tokio::time::Instant;
    let deadline = Instant::now() + config.pass_timeout;
    let until = started
        .checked_add(max_age)
        .map_err(|_| "timestamp-overflow")?;
    let mut revision = None;
    let mut after = None;
    let mut records = Vec::new();
    let mut latest_source_time = started;
    let mut requests = 0usize;
    loop {
        if Instant::now() >= deadline {
            KeyManagerStats::increment(&mut stats.pass_timeouts, &mut stats.counter_overflow);
            return Err("pass-deadline");
        }
        if requests == config.max_pages.get() {
            KeyManagerStats::increment(
                &mut stats.page_budget_exceeded,
                &mut stats.counter_overflow,
            );
            return Err("page-budget");
        }
        requests += 1; // bounded above by the nonzero usize configuration
        let call_deadline = deadline.min(Instant::now() + config.fetch_timeout);
        let page = match tokio::time::timeout_at(
            call_deadline,
            source.active_keys_page(clock.now(), after, config.page_limit),
        )
        .await
        {
            Ok(Ok(page)) => page,
            Ok(Err(_)) => return Err("source-read"),
            Err(_) => {
                if call_deadline == deadline {
                    KeyManagerStats::increment(
                        &mut stats.pass_timeouts,
                        &mut stats.counter_overflow,
                    );
                    return Err("pass-deadline");
                }
                KeyManagerStats::increment(&mut stats.timeouts, &mut stats.counter_overflow);
                return Err("call-deadline");
            }
        };
        page.validate_request(after, config.page_limit)
            .map_err(|_| "page-request-mismatch")?;
        KeyManagerStats::increment(&mut stats.pages, &mut stats.counter_overflow);
        if revision.is_some_and(|previous| previous != page.revision()) {
            // An insertion behind the cursor or revocation of an earlier page
            // invalidates the whole candidate. Restarts consume the same budget.
            KeyManagerStats::increment(&mut stats.revision_conflicts, &mut stats.counter_overflow);
            records.clear();
            revision = None;
            after = None;
            latest_source_time = started;
        } else {
            revision = Some(page.revision());
            latest_source_time = latest_source_time.max(page.as_of());
            after = page.next_after();
            records.extend(page.into_records());
            if after.is_none() {
                let keys = CredentialSet::try_new(records).map_err(|_| "invalid-identity-set")?;
                let mut candidate =
                    projection(secret, keys, started, until, clock, latest_source_time)
                        .map_err(|_| "expired-projection")?;
                if Instant::now() >= deadline {
                    KeyManagerStats::increment(
                        &mut stats.pass_timeouts,
                        &mut stats.counter_overflow,
                    );
                    return Err("pass-deadline");
                }
                candidate.revision = revision.expect("a terminal page supplied its revision");
                return Ok(candidate);
            }
        }
        tokio::task::yield_now().await;
    }
}

async fn run(
    source: Arc<dyn KeySource>,
    secret: Zeroizing<Vec<u8>>,
    clock: Arc<dyn Clock>,
    config: KeyManagerConfig,
    max_age: SignedDuration,
    mut stop: watch::Receiver<bool>,
    publisher: Publisher,
) {
    let mut progress = *publisher.sender.borrow();
    loop {
        let started = clock.now();
        KeyManagerStats::increment(
            &mut progress.stats.attempts,
            &mut progress.stats.counter_overflow,
        );
        publisher.sender.send_replace(progress);
        let result = tokio::select! {
            biased;
            _ = stop.changed() => break,
            result = refresh(&*source, &secret, &*clock, &config, max_age, started, &mut progress.stats) => result,
        };
        // Source revisions never move backwards across publications either.
        let result = result.and_then(|candidate| {
            if publisher
                .verifier
                .current
                .load()
                .as_ref()
                .is_some_and(|previous| candidate.revision < previous.revision)
            {
                Err("revision-regression")
            } else {
                Ok(candidate)
            }
        });
        match result {
            Ok(projection) => {
                publisher.verifier.current.store(Some(Arc::new(projection)));
                KeyManagerStats::increment(
                    &mut progress.stats.refreshes,
                    &mut progress.stats.counter_overflow,
                );
                if progress.health == KeyManagerHealth::Degraded {
                    tracing::info!("credential projection refresh recovered");
                }
                progress.health = KeyManagerHealth::Healthy;
            }
            Err(error) => {
                // A source error may contain a response body or key material.
                // Counters distinguish deadlines; do not echo arbitrary input.
                tracing::warn!(
                    timeouts = progress.stats.timeouts,
                    reason = error,
                    "credential projection refresh failed; retaining the previous expiry"
                );
                KeyManagerStats::increment(
                    &mut progress.stats.failures,
                    &mut progress.stats.counter_overflow,
                );
                progress.health = KeyManagerHealth::Degraded;
            }
        }
        publisher.sender.send_replace(progress);
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = tokio::time::sleep(config.refresh_interval) => {},
        }
    }
    progress.health = KeyManagerHealth::Stopped;
    publisher.sender.send_replace(progress);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tollgate_core::KeyId;
    use tollgate_store::CredentialRecord;

    proptest! {
        #[test]
        fn projected_evidence_is_bounded_by_both_source_expiry_and_fetch_start(
            started in -1_000_000i64..1_000_000,
            age in 1i64..10_000,
            elapsed in 0i64..20_000,
            expiry in proptest::option::of(-1_010_000i64..1_020_000),
        ) {
            let stamp = |s| Timestamp::from_second(s).unwrap();
            let secret = b"fixture-projection-proof-secret-108";
            let key = HmacRegistry::new(secret).mint(KeyId(1)).unwrap();
            let keys = CredentialSet::try_new(vec![CredentialRecord {
                key_id: key.key_id,
                principal: key.principal,
                digest: key.digest,
                not_after: expiry.map(stamp),
            }]).unwrap();
            let clock = tollgate_store::ManualClock::new(stamp(started + elapsed));
            let result = projection(secret, keys, stamp(started), stamp(started + age), &clock, stamp(started));
            if elapsed >= age {
                prop_assert!(result.is_err());
            } else {
                let table = result.unwrap();
                let proof = table.registry.verify(&key.secret);
                if expiry.is_some_and(|end| end <= started + elapsed) {
                    prop_assert!(proof.is_none());
                    prop_assert_eq!(table.projected_keys, 0);
                } else {
                    let deadline = proof.unwrap().reusable_until.unwrap().as_second();
                    prop_assert_eq!(deadline, expiry.map_or(started + age, |end| end.min(started + age)));
                    prop_assert!(deadline > started + elapsed);
                    prop_assert_eq!(table.projected_keys, 1);
                }
            }
        }
    }

    #[test]
    fn diagnostic_counter_overflow_is_visible_without_wrapping() {
        let mut count = u64::MAX;
        let mut overflow = false;
        KeyManagerStats::increment(&mut count, &mut overflow);
        assert_eq!(count, u64::MAX);
        assert!(overflow);
    }
}
