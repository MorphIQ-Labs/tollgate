//! The batched usage writer.
//!
//! A bounded mpsc channel separates the request path from billing I/O. The
//! request path reserves a channel slot *before* admitting work
//! ([`UsageRecorder::try_reserve`]); a full channel is
//! `DenyReason::AccountingBackpressure` — shed with zero units charged,
//! never a silent drop, never an unbounded block (INVARIANTS.md #8). The
//! permit outlives execution, and the shutdown drain below waits for it, so
//! a post-commit send is either ingested or explicitly counted — never
//! silently dropped (INVARIANTS.md #13).
//!
//! The writer task drains the channel into batches and ingests them through
//! the [`UsageSink`](tollgate_store::UsageSink). Ingest is idempotent on
//! request id (INVARIANTS.md #7), so retrying a whole batch after a backend
//! error is always safe. A failing backend is retried with backoff forever
//! while the channel backs up and sheds upstream — memory stays bounded at
//! one in-flight batch plus the channel.
//!
//! Every ingest is wall-clock bounded by
//! [`UsageWriterConfig::ingest_timeout`] (INVARIANTS.md #18): a sink that
//! hangs rather than erroring is indistinguishable from one that is merely
//! slow, and neither may park the task. A timed-out call is a failed
//! attempt — during shutdown it counts toward `lost`, never toward success.
//!
//! Shutdown is level-triggered: every loop consults the watch's *current*
//! value, never only its edge notification, so a shutdown signalled during a
//! retry backoff still reaches the bounded final flush (this was review
//! finding #2 — the original edge-triggered design could consume the
//! notification inside the retry loop and then wait forever for a second one
//! that never came). Dropping the [`UsageWriter`] handle without calling
//! [`shutdown`](UsageWriter::shutdown) aborts the task outright — enqueued
//! events are lost in that path, which is why graceful code always calls
//! `shutdown`.
//!
//! The final flush closes the channel (new reservations deny from that
//! instant), then drains with real receives — not a momentarily-empty peek —
//! until every outstanding permit resolves by sending or dropping, bounded
//! by [`UsageWriterConfig::shutdown_drain_deadline`]. Permits still
//! unresolved at the deadline are reported in [`WriterStats::unresolved`];
//! their charges are locally committed but unbilled, bounded thereafter by
//! TTL reclaim (INVARIANTS.md #9).
//!
//! Every charge that enters the queue is counted until the writer gives it a
//! billing outcome, in a counter held outside the task. A writer that dies
//! instead of reporting therefore still says how many committed charges it
//! was carrying: [`shutdown`](UsageWriter::shutdown) yields
//! [`WriterShutdownError`], never a zeroed [`WriterStats`] that would read
//! exactly like a clean run (INVARIANTS.md #8).
//!
//! Lifecycle order the embedder must follow: stop admitting, quiesce the
//! request tasks holding permits or committed `Committed` guards, `shutdown()`
//! this writer, and only then shut the lease manager down — events must land
//! while their lease is live (INVARIANTS.md #12). Size the drain deadline
//! within `expiry_safety_margin + reclaim_grace`, so a slow drain surfaces
//! as `rejected` at the sink rather than silent loss.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use jiff::{SignedDuration, Timestamp};
use tokio::sync::{mpsc, watch};
use tracing::Instrument as _;

use tollgate_core::{DenyReason, UsageEvent};
use tollgate_store::Clock;
use tollgate_store::{MAX_INGEST_BATCH, UsageSink};

#[derive(Debug, Clone, Copy)]
pub struct UsageWriterConfig {
    /// Channel capacity — the shed point. Size it to cover the sink's worst
    /// tolerable outage at peak admission rate.
    pub queue_capacity: usize,
    /// Largest batch handed to one `ingest` call.
    pub max_batch: usize,
    /// A partial batch is flushed after at most this long.
    pub flush_interval: std::time::Duration,
    /// Backoff between retries of a failing ingest.
    pub retry_backoff: std::time::Duration,
    /// Wall-clock bound on the shutdown drain: how long `shutdown` waits for
    /// outstanding permits (slots reserved by in-flight requests or committed
    /// committed guards) to resolve, *including* the ingest calls it makes along
    /// the way. Must be positive. Size it within
    /// `expiry_safety_margin + reclaim_grace` so an event landing at the end
    /// of the drain is still billable against its lease.
    pub shutdown_drain_deadline: std::time::Duration,
    /// Wall-clock bound on one `UsageSink::ingest` call. A sink that hangs
    /// rather than erroring would otherwise park the writer task forever, and
    /// with it every later shutdown step. Must be positive.
    pub ingest_timeout: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageWriterConfigError(pub &'static str);

impl std::fmt::Display for UsageWriterConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for UsageWriterConfigError {}

impl UsageWriterConfig {
    /// Every field is a contract, so none of them is silently repaired
    /// (INVARIANTS.md #16): a zero capacity or batch size has no sensible
    /// coercion, a zero backoff hot-spins against a failing sink, and a zero
    /// deadline or timeout reports failure without waiting at all.
    pub fn validate(&self) -> Result<(), UsageWriterConfigError> {
        if self.queue_capacity == 0 {
            return Err(UsageWriterConfigError("queue_capacity must be positive"));
        }
        if self.max_batch > MAX_INGEST_BATCH {
            return Err(UsageWriterConfigError(
                "max_batch exceeds the ingest endpoint's documented limit",
            ));
        }
        if self.max_batch == 0 {
            return Err(UsageWriterConfigError("max_batch must be positive"));
        }
        if self.flush_interval.is_zero() {
            return Err(UsageWriterConfigError("flush_interval must be positive"));
        }
        if self.retry_backoff.is_zero() {
            return Err(UsageWriterConfigError("retry_backoff must be positive"));
        }
        if self.shutdown_drain_deadline.is_zero() {
            return Err(UsageWriterConfigError(
                "shutdown_drain_deadline must be positive",
            ));
        }
        if self.ingest_timeout.is_zero() {
            return Err(UsageWriterConfigError("ingest_timeout must be positive"));
        }
        Ok(())
    }
}

/// A counter written from the request path, on a cache line of its own.
///
/// `#[repr(align(128))]` rounds the type's size up to its alignment, so no two
/// of these — and nothing else in the struct — can share a line on the
/// 128-byte Apple Silicon target or on 64-byte-line x86-64. The counters the
/// writer task alone updates need no such treatment: one writer cannot
/// contend with itself.
#[repr(align(128))]
#[derive(Debug)]
struct Contended(AtomicU64);

impl Contended {
    const fn zero() -> Self {
        Contended(AtomicU64::new(0))
    }

    #[inline]
    fn bump(&self, by: u64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Every accounting number the writer produces, held outside the task.
///
/// The tally used to be a `WriterStats` local on the task's stack, materialised
/// only when the task exited — so a process that crashed, was killed, or simply
/// kept running reported nothing, and `lost` was observable only after the one
/// kind of shutdown where loss is least likely (#38). Living out here it is
/// readable at any time, and it survives the task's death exactly as the
/// unaccounted count always has (INVARIANTS.md #8).
///
/// These are also the *only* copy: [`UsageWriter::shutdown`] returns a snapshot
/// of these counters rather than a parallel tally, so the running totals and the
/// final report cannot disagree.
///
/// `Relaxed` throughout, like the admission counters: nothing is published
/// through them, and atomicity — no lost increments — is all they need.
#[derive(Debug)]
pub struct WriterCounters {
    // Written only by the writer task, between batches.
    accepted: AtomicU64,
    duplicate: AtomicU64,
    rejected: AtomicU64,
    lost: AtomicU64,
    unresolved: AtomicU64,
    /// When the sink last answered an ingest, in milliseconds since the epoch.
    /// `i64::MIN` means "never": zero cannot be the sentinel, because the epoch
    /// itself is a legitimate timestamp that tests use routinely.
    last_ingest_ms: AtomicI64,
    // Written from the request path, by as many cores as serve requests.
    unaccounted: Contended,
    shed: Contended,
}

impl WriterCounters {
    #[must_use]
    pub const fn new() -> Self {
        WriterCounters {
            accepted: AtomicU64::new(0),
            duplicate: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            unresolved: AtomicU64::new(0),
            last_ingest_ms: AtomicI64::new(i64::MIN),
            unaccounted: Contended::zero(),
            shed: Contended::zero(),
        }
    }

    /// The sink answered. Recorded even when every event in the batch was
    /// refused: `rejected` is an answer, and what this timestamp distinguishes
    /// is a reachable sink from an unreachable one.
    fn record_ingest(&self, report: &tollgate_store::IngestReport, at: Timestamp) {
        self.accepted.fetch_add(report.accepted, Ordering::Relaxed);
        self.duplicate
            .fetch_add(report.duplicate, Ordering::Relaxed);
        self.rejected.fetch_add(report.rejected, Ordering::Relaxed);
        self.last_ingest_ms
            .store(at.as_millisecond(), Ordering::Relaxed);
    }

    fn record_lost(&self, events: u64) {
        self.lost.fetch_add(events, Ordering::Relaxed);
    }

    fn set_unresolved(&self, permits: u64) {
        self.unresolved.store(permits, Ordering::Relaxed);
    }

    /// One charge has entered the queue with no billing outcome yet.
    fn enqueued(&self) {
        self.unaccounted.bump(1);
    }

    /// `events` charges have been given a billing outcome — delivered or
    /// reported lost — so they no longer count as unaccounted.
    fn settled(&self, events: u64) {
        self.unaccounted.0.fetch_sub(events, Ordering::Relaxed);
    }

    /// A backpressure refusal, counted where it happens rather than by the
    /// caller: `try_reserve` is the only way to be refused, so counting here
    /// cannot be forgotten by an embedder.
    fn record_shed(&self) {
        self.shed.bump(1);
    }

    /// The five numbers [`UsageWriter::shutdown`] reports.
    #[must_use]
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            duplicate: self.duplicate.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            lost: self.lost.load(Ordering::Relaxed),
            unresolved: self.unresolved.load(Ordering::Relaxed),
        }
    }

    fn last_ingest_at(&self) -> Option<Timestamp> {
        let millis = self.last_ingest_ms.load(Ordering::Relaxed);
        (millis != i64::MIN)
            .then(|| Timestamp::from_millisecond(millis).ok())
            .flatten()
    }

    /// Everything readable while the writer runs, given the queue's shape —
    /// which only a holder of the channel can supply.
    fn health(&self, queue_depth: usize, queue_capacity: usize) -> WriterHealth {
        WriterHealth {
            stats: self.stats(),
            unaccounted: self.unaccounted.get(),
            shed: self.shed.get(),
            queue_depth,
            queue_capacity,
            last_ingest_at: self.last_ingest_at(),
        }
    }
}

impl Default for WriterCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A reading of the writer's accounting health, safe to serialise.
///
/// Note what `lost` does *not* tell you at runtime: the steady-state path
/// retries a failing sink forever, so nothing is declared lost until the final
/// flush gives up. A healthy-but-cut-off process reports `lost == 0` for its
/// whole life. The leading indicators are [`WriterHealth::ingest_age`], a
/// `queue_depth` approaching `queue_capacity`, and `stats.rejected` — which the
/// sink has already refused, and which is bounded billing loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterHealth {
    /// The same five numbers a clean shutdown returns, read live.
    pub stats: WriterStats,
    /// Charges in the queue with no billing outcome yet.
    pub unaccounted: u64,
    /// Requests refused for want of queue capacity (INVARIANTS.md #8).
    pub shed: u64,
    /// Slots currently held by queued events and outstanding permits.
    pub queue_depth: usize,
    /// The shed point: `queue_depth` reaching this denies the next request.
    pub queue_capacity: usize,
    /// When the sink last answered, or `None` if it never has.
    pub last_ingest_at: Option<Timestamp>,
}

impl WriterHealth {
    /// How long since the sink last answered — the signal that separates "no
    /// traffic" from "the sink has been unreachable for twenty minutes".
    /// `None` while no ingest has ever succeeded, which is also the state of a
    /// freshly started process.
    #[must_use]
    pub fn ingest_age(&self, now: Timestamp) -> Option<SignedDuration> {
        self.last_ingest_at.map(|at| now.duration_since(at))
    }
}

/// Cheap-to-clone handle for request handlers.
#[derive(Clone)]
pub struct UsageRecorder {
    tx: mpsc::Sender<UsageEvent>,
    counters: Arc<WriterCounters>,
}

impl UsageRecorder {
    /// Reserve accounting capacity for one request, *before* admission. A
    /// full queue denies here — before any units are reserved or any work
    /// runs.
    pub fn try_reserve(&self) -> Result<UsagePermit, DenyReason> {
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => Ok(UsagePermit {
                permit,
                counters: Arc::clone(&self.counters),
            }),
            Err(_) => {
                self.counters.record_shed();
                Err(DenyReason::AccountingBackpressure)
            }
        }
    }

    /// Whether the writer task has exited and can no longer accept events.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    pub(crate) async fn closed(&self) {
        self.tx.closed().await;
    }

    /// The writer's accounting health, readable at any time.
    ///
    /// Deliberately on the *recorder*: a service holds this handle in its
    /// request state, while the [`UsageWriter`] is usually moved into whatever
    /// owns shutdown — so exposing the numbers only there would put them out of
    /// reach of the endpoint that needs to report them.
    #[must_use]
    pub fn health(&self) -> WriterHealth {
        self.counters.health(
            self.tx.max_capacity() - self.tx.capacity(),
            self.tx.max_capacity(),
        )
    }
}

/// One reserved accounting slot. Send the committed request's event with
/// [`record`](UsagePermit::record); dropping the permit (deny, cancel,
/// zero-charge path) releases the slot.
pub struct UsagePermit {
    permit: mpsc::OwnedPermit<UsageEvent>,
    counters: Arc<WriterCounters>,
}

impl UsagePermit {
    pub fn record(self, event: UsageEvent) {
        // Counted from the moment it enters the queue until the writer gives
        // it a billing outcome; a writer that dies in between is therefore
        // able to say how many charges it was carrying.
        self.counters.enqueued();
        self.permit.send(event);
    }
}

impl tollgate_core::UsageSlot for UsagePermit {
    fn record(self, event: UsageEvent) {
        UsagePermit::record(self, event);
    }
}

/// Terminal accounting of a writer's lifetime, returned by
/// [`UsageWriter::shutdown`]. `lost` counts events a final flush could not
/// deliver; `unresolved` counts permits still outstanding when the drain
/// deadline expired — both reported, never silent.
///
/// Deliberately not [`Default`]: a zeroed report must never be conjurable
/// from a failure (`unwrap_or_default` on a dead task's `JoinError` is the
/// bug this type's history records — issue #41). Use [`WriterStats::ZERO`]
/// when a starting value is genuinely meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterStats {
    pub accepted: u64,
    pub duplicate: u64,
    pub rejected: u64,
    pub lost: u64,
    /// Permits (in-flight requests or committed guards) that neither sent
    /// nor dropped before the drain deadline. Their charges are locally
    /// committed but unbilled; TTL reclaim bounds them (INVARIANTS.md #9).
    pub unresolved: u64,
}

impl WriterStats {
    /// A writer that has accounted for nothing yet.
    pub const ZERO: WriterStats = WriterStats {
        accepted: 0,
        duplicate: 0,
        rejected: 0,
        lost: 0,
        unresolved: 0,
    };
}

/// The writer task ended without reporting: it panicked, or it was aborted.
/// `unaccounted` is a *lower bound* on committed charges left with no billing
/// record — the events that had entered the queue but had not yet been given
/// an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterShutdownError {
    pub unaccounted: u64,
    /// True when the task panicked, false when it was cancelled or aborted.
    pub panicked: bool,
}

impl std::fmt::Display for WriterShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cause = if self.panicked { "panicked" } else { "aborted" };
        write!(
            f,
            "usage writer {cause} before reporting; at least {} committed charge(s) have no billing record",
            self.unaccounted
        )
    }
}

impl std::error::Error for WriterShutdownError {}

/// Handle to the writer task.
pub struct UsageWriter {
    shutdown: watch::Sender<bool>,
    deadline: Arc<crate::ShutdownDeadline>,
    handle: Option<tokio::task::JoinHandle<WriterStats>>,
    counters: Arc<WriterCounters>,
    /// Only to read the queue's depth for [`UsageWriter::health`] — weak, so
    /// this handle never keeps the channel open by itself.
    queue: mpsc::WeakSender<UsageEvent>,
    queue_capacity: usize,
}

impl UsageWriter {
    pub fn spawn(
        sink: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        config: UsageWriterConfig,
    ) -> Result<(UsageRecorder, UsageWriter), UsageWriterConfigError> {
        config.validate()?;
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        // A weak handle lets the drain count still-outstanding permits at
        // the deadline without holding the channel open itself.
        let weak = tx.downgrade();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let counters = Arc::new(WriterCounters::new());
        let deadline = Arc::new(crate::ShutdownDeadline::default());
        let handle = tokio::spawn(
            run(
                Writer {
                    sink,
                    clock,
                    config,
                    counters: Arc::clone(&counters),
                    deadline: Arc::clone(&deadline),
                },
                rx,
                weak.clone(),
                shutdown_rx,
            )
            // One writer serves every account, so the span carries the
            // queue's shape; account and lease identify individual events.
            .instrument(tracing::info_span!(
                "usage_writer",
                queue_capacity = config.queue_capacity,
                max_batch = config.max_batch
            )),
        );
        Ok((
            UsageRecorder {
                tx,
                counters: Arc::clone(&counters),
            },
            UsageWriter {
                shutdown,
                deadline,
                handle: Some(handle),
                counters,
                queue: weak,
                queue_capacity: config.queue_capacity,
            },
        ))
    }

    /// The writer's accounting health, readable at any time — the same numbers
    /// [`UsageRecorder::health`] reports, for an embedder that holds this half.
    #[must_use]
    pub fn health(&self) -> WriterHealth {
        let depth = self
            .queue
            .upgrade()
            .map(|tx| tx.max_capacity() - tx.capacity())
            .unwrap_or(0);
        self.counters.health(depth, self.queue_capacity)
    }

    /// Flush everything already enqueued, then stop. Call this *before*
    /// releasing leases — events must land while their lease is live.
    ///
    /// A writer task that died instead of reporting yields
    /// [`WriterShutdownError`] carrying the charges it was still holding —
    /// never a zeroed [`WriterStats`], which would be indistinguishable from
    /// a clean shutdown (INVARIANTS.md #8).
    pub async fn shutdown(mut self) -> Result<WriterStats, WriterShutdownError> {
        crate::signal(&self.shutdown, true, "usage-writer shutdown");
        let Some(handle) = self.handle.as_mut() else {
            // Unreachable through the public API: `shutdown` consumes the
            // handle, and `Drop` runs only afterwards.
            return Err(self.died(false));
        };
        // Borrow the handle so cancelling this future cannot detach billing.
        match handle.await {
            Ok(stats) => Ok(stats),
            Err(join) => Err(self.died(join.is_panic())),
        }
    }

    /// Constrain cleanup before signalling it, so a runtime's total budget
    /// governs the actual receives, ingests, and backoffs.
    pub(crate) fn stop_at(&self, deadline: tokio::time::Instant) {
        self.deadline.constrain(deadline);
        crate::signal(&self.shutdown, true, "usage-writer shutdown");
    }

    fn died(&self, panicked: bool) -> WriterShutdownError {
        WriterShutdownError {
            unaccounted: self.counters.unaccounted.get(),
            panicked,
        }
    }
}

impl Drop for UsageWriter {
    fn drop(&mut self) {
        // Dropped without shutdown(): abort rather than leak a detached
        // task. Ungraceful by definition — enqueued events die with it.
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Why the fill loop stopped collecting.
enum FillOutcome {
    /// Batch full or flush interval lapsed: deliver and keep running.
    Flush,
    /// Shutdown observed (signal, dropped sender, or closed channel):
    /// proceed to the bounded final flush.
    Stop,
}

/// The writer's collaborators, fixed for the task's lifetime.
struct Writer {
    sink: Arc<dyn UsageSink>,
    clock: Arc<dyn Clock>,
    config: UsageWriterConfig,
    counters: Arc<WriterCounters>,
    deadline: Arc<crate::ShutdownDeadline>,
}

impl Writer {
    /// Every event in `batch` now has a billing outcome, so it no longer
    /// counts against what a dying writer would be holding.
    fn account_for(&self, batch: &[UsageEvent]) {
        self.counters.settled(batch.len() as u64);
    }
}

async fn run(
    writer: Writer,
    mut rx: mpsc::Receiver<UsageEvent>,
    weak: mpsc::WeakSender<UsageEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> WriterStats {
    let config = writer.config;
    let max_batch = config.max_batch;
    let mut batch: Vec<UsageEvent> = Vec::with_capacity(max_batch);

    loop {
        // Level check at every loop boundary: a shutdown observed anywhere
        // below (including inside the retry backoff) lands here.
        if *shutdown.borrow() {
            return final_flush(&writer, &mut rx, &weak, &mut batch).await;
        }

        let deadline = tokio::time::sleep(config.flush_interval);
        tokio::pin!(deadline);
        let outcome = loop {
            // `recv_many`'s limit is the number of events appended, not the
            // final vector length. Restrict it to the remaining capacity so
            // one ingest call can never exceed `max_batch`.
            let remaining = max_batch - batch.len();
            tokio::select! {
                received = rx.recv_many(&mut batch, remaining) => {
                    if received == 0 {
                        break FillOutcome::Stop;
                    }
                    if batch.len() >= max_batch {
                        break FillOutcome::Flush;
                    }
                }
                _ = &mut deadline, if !batch.is_empty() => break FillOutcome::Flush,
                changed = shutdown.changed() => {
                    // Err = sender dropped without shutdown(); treat both as
                    // stop so the task can never outlive its handle usefully.
                    if changed.is_err() || *shutdown.borrow() {
                        break FillOutcome::Stop;
                    }
                }
            }
        };

        match outcome {
            FillOutcome::Stop => {
                return final_flush(&writer, &mut rx, &weak, &mut batch).await;
            }
            FillOutcome::Flush => {
                // Retry until delivered or shutdown interrupts; either way
                // the loop-top level check decides what happens next.
                flush_retrying(&writer, &mut batch, &mut shutdown).await;
                if shutdown.has_changed().is_err() {
                    // Sender gone: same stop path as above.
                    return final_flush(&writer, &mut rx, &weak, &mut batch).await;
                }
            }
        }
    }
}

/// Ingest `batch`, retrying with backoff until it is delivered (batch
/// cleared) or shutdown is observed (batch left intact for the final flush).
async fn flush_retrying(
    writer: &Writer,
    batch: &mut Vec<UsageEvent>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let Writer {
        sink,
        clock,
        config,
        counters,
        ..
    } = writer;
    // An outage is a *duration*, not an event: retrying forever is the
    // designed behavior, so the only way it becomes visible is by reporting
    // when it began, and how long it lasted once it ends. `WriterStats` sees
    // none of this — a recovered outage produces a perfectly clean report.
    let mut outage: Option<(tokio::time::Instant, u64)> = None;
    loop {
        // A sink that hangs is indistinguishable from one that is merely slow,
        // and neither may park this task: the timeout turns both into the
        // ordinary retry path.
        // The same instant the batch is ingested with is the one recorded as
        // the last time the sink answered, so the health reading and the
        // ledger agree about when this batch happened.
        let now = clock.now();
        let ingest = tokio::time::timeout(config.ingest_timeout, sink.ingest(batch, now));
        let outcome = tokio::select! {
            outcome = ingest => outcome,
            _ = shutdown.changed() => return,
        };
        match outcome {
            Ok(Ok(report)) => {
                if let Some((began, attempts)) = outage {
                    tracing::info!(
                        attempts,
                        outage_ms = began.elapsed().as_millis(),
                        "usage sink recovered"
                    );
                }
                counters.record_ingest(&report, now);
                writer.account_for(batch);
                batch.clear();
                return;
            }
            // A refusal is a fact about this batch, not about the sink's
            // availability: replaying it unchanged earns the same answer
            // forever, and every event queued behind it waits for a recovery
            // that cannot come. Counted lost and dropped, so the queue drains
            // and later events bill — a permanent configuration or contract
            // error must not become an unbounded billing outage (#61).
            //
            // `lost` is the honest word for it: these events entered the queue
            // and will never reach the ledger. INVARIANTS #8 asks that they be
            // counted rather than silently dropped, not that they be delivered
            // by a sink that refuses them.
            Ok(Err(refused)) if !refused.is_retryable() => {
                counters.record_lost(batch.len() as u64);
                tracing::error!(
                    events = batch.len(),
                    %refused,
                    "usage sink refused this batch and will refuse it again; \
                     counted lost so later events are not blocked behind it"
                );
                writer.account_for(batch);
                batch.clear();
                return;
            }
            outcome => {
                let timed_out = outcome.is_err();
                let attempts = match &mut outage {
                    Some((_, attempts)) => {
                        *attempts += 1;
                        *attempts
                    }
                    none => {
                        // First failure of this outage: say so once at warn,
                        // then stay quiet at debug so a long outage does not
                        // become a log flood.
                        *none = Some((tokio::time::Instant::now(), 1));
                        tracing::warn!(
                            events = batch.len(),
                            timed_out,
                            "usage sink failing; batching up and retrying"
                        );
                        1
                    }
                };
                if attempts > 1 {
                    tracing::debug!(attempts, timed_out, "usage sink still failing");
                }
                tokio::select! {
                    _ = tokio::time::sleep(config.retry_backoff) => {}
                    _ = shutdown.changed() => {}
                }
                // Level check covers every wake-up path: backoff elapsed,
                // signal received, or sender dropped.
                if *shutdown.borrow() || shutdown.has_changed().is_err() {
                    if let Some((began, attempts)) = outage {
                        tracing::warn!(
                            attempts,
                            outage_ms = began.elapsed().as_millis(),
                            events = batch.len(),
                            "shutdown observed during a sink outage; \
                             the final flush decides these events' fate"
                        );
                    }
                    return;
                }
            }
        }
    }
}

/// Drain the channel until every outstanding permit resolves or the
/// configured deadline expires, giving each batch a bounded number of
/// delivery attempts. Whatever cannot be delivered is *reported* lost;
/// permits still outstanding at the deadline are *reported* unresolved. A
/// deadline expiry can therefore never look like a clean flush, and the
/// drain can never block past its bound.
async fn final_flush(
    writer: &Writer,
    rx: &mut mpsc::Receiver<UsageEvent>,
    weak: &mpsc::WeakSender<UsageEvent>,
    batch: &mut Vec<UsageEvent>,
) -> WriterStats {
    let config = &writer.config;
    let max_batch = config.max_batch;
    // Refuse new reservations from this instant. Permits already handed out
    // keep their slots and can still deliver into the drain below; a real
    // recv (unlike try_recv, for which a reserved-but-unsent slot is
    // indistinguishable from "done") yields None only once every one of
    // them has sent or dropped.
    rx.close();
    let deadline = writer.deadline.within(config.shutdown_drain_deadline);
    let mut drained = false;
    let mut expired = false;
    loop {
        while batch.len() < max_batch && !drained && !expired {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(event)) => batch.push(event),
                Ok(None) => drained = true,
                Err(_) => expired = true,
            }
        }
        // No post-deadline sweep is needed: `timeout_at` polls the receive
        // first, so an event already queued is still delivered above even
        // once the deadline has passed. `expired` therefore means the queue
        // is empty and some permit is still outstanding — and each drained
        // event resolves a permit, so the finite permit set bounds the loop.
        if batch.is_empty() {
            if expired {
                writer.counters.set_unresolved(outstanding_permits(weak));
            }
            return writer.counters.stats();
        }
        flush_bounded(writer, batch, deadline).await;
    }
}

/// Ingest `batch` with a bounded number of attempts, none of which may run
/// past the drain `deadline`; an undeliverable batch is counted lost. The
/// batch is cleared either way.
async fn flush_bounded(
    writer: &Writer,
    batch: &mut Vec<UsageEvent>,
    deadline: tokio::time::Instant,
) {
    let Writer {
        sink,
        clock,
        config,
        counters,
        ..
    } = writer;
    const FINAL_FLUSH_ATTEMPTS: u32 = 3;
    let mut delivered = false;
    for attempt in 1..=FINAL_FLUSH_ATTEMPTS {
        // Two bounds, whichever is sooner: one call may not exceed the ingest
        // timeout, and the drain as a whole may not exceed its deadline. A
        // timed-out call is a failed attempt — never a silent success.
        let attempt_deadline = deadline.min(tokio::time::Instant::now() + config.ingest_timeout);
        let now = clock.now();
        match tokio::time::timeout_at(attempt_deadline, sink.ingest(batch, now)).await {
            Ok(Ok(report)) => {
                counters.record_ingest(&report, now);
                delivered = true;
                break;
            }
            Ok(Err(_)) if attempt < FINAL_FLUSH_ATTEMPTS => {
                // The backoff sleeps into whatever budget is left, never past
                // it. Sleeping the full `retry_backoff` here overran the
                // deadline by up to `2 * retry_backoff`, because the comment
                // claiming otherwise was attached to the *timeout* arm while
                // this one — an ordinary store error, and the common case —
                // slept unconditionally (#63).
                //
                // That overrun is not merely a slow shutdown. The drain's
                // budget is sized inside `expiry_safety_margin + reclaim_grace`
                // (#12), so overrunning it releases leases past the window
                // that keeps a straggler billable: events that do land arrive
                // against a lease the allocator has re-granted, and are
                // refused. Bounding by attempt *count* alone is not a bound
                // (#18).
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + config.retry_backoff),
                )
                .await;
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
            }
            // Once the deadline has passed there is no budget left to back
            // off into, and none to make another attempt with.
            Err(_) => break,
            Ok(Err(_)) => {}
        }
    }
    if !delivered {
        counters.record_lost(batch.len() as u64);
    }
    // Delivered or lost, the batch has been reported either way.
    writer.account_for(batch);
    batch.clear();
}

/// Slots still held at the drain deadline. The upgrade succeeds exactly
/// while some permit keeps the channel alive — which is when there is
/// something to report — and the momentary strong sender is dropped
/// immediately, so it cannot mask completion.
fn outstanding_permits(weak: &mpsc::WeakSender<UsageEvent>) -> u64 {
    weak.upgrade()
        .map(|tx| (tx.max_capacity() - tx.capacity()) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod layout_tests {
    use super::{Contended, UsageWriter, UsageWriterConfig};
    use jiff::Timestamp;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn a_runtime_stop_closes_the_queue_and_bounds_the_actual_drain() {
        let (recorder, writer) = UsageWriter::spawn(
            tollgate_store::MemoryStore::new(tollgate_store::GrantPolicy::default()).unwrap(),
            Arc::new(crate::ManualClock::new(
                Timestamp::from_second(100).unwrap(),
            )),
            UsageWriterConfig {
                queue_capacity: 1,
                max_batch: 1,
                flush_interval: std::time::Duration::from_millis(1),
                retry_backoff: std::time::Duration::from_millis(1),
                shutdown_drain_deadline: std::time::Duration::from_millis(100),
                ingest_timeout: std::time::Duration::from_millis(1),
            },
        )
        .unwrap();
        let permit = recorder.try_reserve().unwrap();
        let began = tokio::time::Instant::now();
        writer.stop_at(began + std::time::Duration::from_millis(5));
        tokio::time::timeout(std::time::Duration::from_millis(1), recorder.closed())
            .await
            .unwrap();
        let stats = writer.shutdown().await.unwrap();
        assert_eq!(stats.unresolved, 1);
        assert_eq!(began.elapsed(), std::time::Duration::from_millis(5));
        drop(permit);
    }

    #[test]
    fn request_path_counters_are_isolated_on_supported_cache_lines() {
        assert_eq!(align_of::<Contended>(), 128);
        assert_eq!(size_of::<Contended>(), 128);
    }
}
