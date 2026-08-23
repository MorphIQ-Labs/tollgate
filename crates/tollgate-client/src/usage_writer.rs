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
//! request tasks holding permits or committed `ChargeGuard`s, `shutdown()`
//! this writer, and only then shut the lease manager down — events must land
//! while their lease is live (INVARIANTS.md #12). Size the drain deadline
//! within `expiry_safety_margin + reclaim_grace`, so a slow drain surfaces
//! as `rejected` at the sink rather than silent loss.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use tollgate_core::{DenyReason, UsageEvent};
use tollgate_store::Clock;
use tollgate_store::UsageSink;

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
    /// `ChargeGuard`s) to resolve, *including* the ingest calls it makes along
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

/// Charges that entered the queue and have no billing outcome yet. Held
/// outside the writer task, so a task that dies still leaves the count of
/// what it was carrying (INVARIANTS.md #8).
type Unaccounted = Arc<std::sync::atomic::AtomicU64>;

/// Cheap-to-clone handle for request handlers.
#[derive(Clone)]
pub struct UsageRecorder {
    tx: mpsc::Sender<UsageEvent>,
    unaccounted: Unaccounted,
}

impl UsageRecorder {
    /// Reserve accounting capacity for one request, *before* admission. A
    /// full queue denies here — before any units are reserved or any work
    /// runs.
    pub fn try_reserve(&self) -> Result<UsagePermit, DenyReason> {
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => Ok(UsagePermit {
                permit,
                unaccounted: Arc::clone(&self.unaccounted),
            }),
            Err(_) => Err(DenyReason::AccountingBackpressure),
        }
    }

    /// Whether the writer task has exited and can no longer accept events.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// One reserved accounting slot. Send the committed request's event with
/// [`record`](UsagePermit::record); dropping the permit (deny, cancel,
/// zero-charge path) releases the slot.
pub struct UsagePermit {
    permit: mpsc::OwnedPermit<UsageEvent>,
    unaccounted: Unaccounted,
}

impl UsagePermit {
    pub fn record(self, event: UsageEvent) {
        // Counted from the moment it enters the queue until the writer gives
        // it a billing outcome; a writer that dies in between is therefore
        // able to say how many charges it was carrying.
        self.unaccounted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.permit.send(event);
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
    handle: Option<tokio::task::JoinHandle<WriterStats>>,
    unaccounted: Unaccounted,
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
        let unaccounted: Unaccounted = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handle = tokio::spawn(run(
            Writer {
                sink,
                clock,
                config,
                unaccounted: Arc::clone(&unaccounted),
            },
            rx,
            weak,
            shutdown_rx,
        ));
        Ok((
            UsageRecorder {
                tx,
                unaccounted: Arc::clone(&unaccounted),
            },
            UsageWriter {
                shutdown,
                handle: Some(handle),
                unaccounted,
            },
        ))
    }

    /// Flush everything already enqueued, then stop. Call this *before*
    /// releasing leases — events must land while their lease is live.
    ///
    /// A writer task that died instead of reporting yields
    /// [`WriterShutdownError`] carrying the charges it was still holding —
    /// never a zeroed [`WriterStats`], which would be indistinguishable from
    /// a clean shutdown (INVARIANTS.md #8).
    pub async fn shutdown(mut self) -> Result<WriterStats, WriterShutdownError> {
        let _ = self.shutdown.send(true);
        let Some(handle) = self.handle.take() else {
            // Unreachable through the public API: `shutdown` consumes the
            // handle, and `Drop` runs only afterwards.
            return Err(self.died(false));
        };
        match handle.await {
            Ok(stats) => Ok(stats),
            Err(join) => Err(self.died(join.is_panic())),
        }
    }

    fn died(&self, panicked: bool) -> WriterShutdownError {
        WriterShutdownError {
            unaccounted: self.unaccounted.load(std::sync::atomic::Ordering::Relaxed),
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
    unaccounted: Unaccounted,
}

impl Writer {
    /// Every event in `batch` now has a billing outcome, so it no longer
    /// counts against what a dying writer would be holding.
    fn account_for(&self, batch: &[UsageEvent]) {
        self.unaccounted
            .fetch_sub(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn run(
    writer: Writer,
    mut rx: mpsc::Receiver<UsageEvent>,
    weak: mpsc::WeakSender<UsageEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> WriterStats {
    let config = writer.config;
    let mut stats = WriterStats::ZERO;
    let max_batch = config.max_batch;
    let mut batch: Vec<UsageEvent> = Vec::with_capacity(max_batch);

    loop {
        // Level check at every loop boundary: a shutdown observed anywhere
        // below (including inside the retry backoff) lands here.
        if *shutdown.borrow() {
            return final_flush(&writer, &mut rx, &weak, &mut batch, stats).await;
        }

        let deadline = tokio::time::sleep(config.flush_interval);
        tokio::pin!(deadline);
        let outcome = loop {
            tokio::select! {
                event = rx.recv() => match event {
                    Some(event) => {
                        batch.push(event);
                        if batch.len() >= max_batch {
                            break FillOutcome::Flush;
                        }
                    }
                    None => break FillOutcome::Stop,
                },
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
                return final_flush(&writer, &mut rx, &weak, &mut batch, stats).await;
            }
            FillOutcome::Flush => {
                // Retry until delivered or shutdown interrupts; either way
                // the loop-top level check decides what happens next.
                flush_retrying(&writer, &mut batch, &mut stats, &mut shutdown).await;
                if shutdown.has_changed().is_err() {
                    // Sender gone: same stop path as above.
                    return final_flush(&writer, &mut rx, &weak, &mut batch, stats).await;
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
    stats: &mut WriterStats,
    shutdown: &mut watch::Receiver<bool>,
) {
    let Writer {
        sink,
        clock,
        config,
        ..
    } = writer;
    loop {
        // A sink that hangs is indistinguishable from one that is merely slow,
        // and neither may park this task: the timeout turns both into the
        // ordinary retry path.
        match tokio::time::timeout(config.ingest_timeout, sink.ingest(batch, clock.now())).await {
            Ok(Ok(report)) => {
                stats.accepted += report.accepted;
                stats.duplicate += report.duplicate;
                stats.rejected += report.rejected;
                writer.account_for(batch);
                batch.clear();
                return;
            }
            Ok(Err(_)) | Err(_) => {
                tokio::select! {
                    _ = tokio::time::sleep(config.retry_backoff) => {}
                    _ = shutdown.changed() => {}
                }
                // Level check covers every wake-up path: backoff elapsed,
                // signal received, or sender dropped.
                if *shutdown.borrow() || shutdown.has_changed().is_err() {
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
    mut stats: WriterStats,
) -> WriterStats {
    let config = &writer.config;
    let max_batch = config.max_batch;
    // Refuse new reservations from this instant. Permits already handed out
    // keep their slots and can still deliver into the drain below; a real
    // recv (unlike try_recv, for which a reserved-but-unsent slot is
    // indistinguishable from "done") yields None only once every one of
    // them has sent or dropped.
    rx.close();
    let deadline = tokio::time::Instant::now() + config.shutdown_drain_deadline;
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
                stats.unresolved = outstanding_permits(weak);
            }
            return stats;
        }
        flush_bounded(writer, batch, &mut stats, deadline).await;
    }
}

/// Ingest `batch` with a bounded number of attempts, none of which may run
/// past the drain `deadline`; an undeliverable batch is counted lost. The
/// batch is cleared either way.
async fn flush_bounded(
    writer: &Writer,
    batch: &mut Vec<UsageEvent>,
    stats: &mut WriterStats,
    deadline: tokio::time::Instant,
) {
    let Writer {
        sink,
        clock,
        config,
        ..
    } = writer;
    const FINAL_FLUSH_ATTEMPTS: u32 = 3;
    let mut delivered = false;
    for attempt in 1..=FINAL_FLUSH_ATTEMPTS {
        // Two bounds, whichever is sooner: one call may not exceed the ingest
        // timeout, and the drain as a whole may not exceed its deadline. A
        // timed-out call is a failed attempt — never a silent success.
        let attempt_deadline = deadline.min(tokio::time::Instant::now() + config.ingest_timeout);
        match tokio::time::timeout_at(attempt_deadline, sink.ingest(batch, clock.now())).await {
            Ok(Ok(report)) => {
                stats.accepted += report.accepted;
                stats.duplicate += report.duplicate;
                stats.rejected += report.rejected;
                delivered = true;
                break;
            }
            Ok(Err(_)) if attempt < FINAL_FLUSH_ATTEMPTS => {
                tokio::time::sleep(config.retry_backoff).await;
            }
            // The deadline governs the retries too: once it has passed there
            // is no budget left to back off into.
            Err(_) => break,
            Ok(Err(_)) => {}
        }
    }
    if !delivered {
        stats.lost += batch.len() as u64;
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
