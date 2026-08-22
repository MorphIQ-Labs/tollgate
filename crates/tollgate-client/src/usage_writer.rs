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
    /// `ChargeGuard`s) to resolve. Must be positive. Size it within
    /// `expiry_safety_margin + reclaim_grace` so an event landing at the end
    /// of the drain is still billable against its lease.
    pub shutdown_drain_deadline: std::time::Duration,
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
    /// A zero drain deadline would report every outstanding permit as
    /// unresolved without waiting at all; refuse it before the task starts
    /// (INVARIANTS.md #16). Broader field validation is tracked by #34.
    pub fn validate(&self) -> Result<(), UsageWriterConfigError> {
        if self.shutdown_drain_deadline.is_zero() {
            return Err(UsageWriterConfigError(
                "shutdown_drain_deadline must be positive",
            ));
        }
        Ok(())
    }
}

/// Cheap-to-clone handle for request handlers.
#[derive(Clone)]
pub struct UsageRecorder {
    tx: mpsc::Sender<UsageEvent>,
}

impl UsageRecorder {
    /// Reserve accounting capacity for one request, *before* admission. A
    /// full queue denies here — before any units are reserved or any work
    /// runs.
    pub fn try_reserve(&self) -> Result<UsagePermit, DenyReason> {
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => Ok(UsagePermit(permit)),
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
pub struct UsagePermit(mpsc::OwnedPermit<UsageEvent>);

impl UsagePermit {
    pub fn record(self, event: UsageEvent) {
        self.0.send(event);
    }
}

/// Terminal accounting of a writer's lifetime, returned by
/// [`UsageWriter::shutdown`]. `lost` counts events a final flush could not
/// deliver; `unresolved` counts permits still outstanding when the drain
/// deadline expired — both reported, never silent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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

/// Handle to the writer task.
pub struct UsageWriter {
    shutdown: watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<WriterStats>>,
}

impl UsageWriter {
    pub fn spawn(
        sink: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        config: UsageWriterConfig,
    ) -> Result<(UsageRecorder, UsageWriter), UsageWriterConfigError> {
        config.validate()?;
        let (tx, rx) = mpsc::channel(config.queue_capacity.max(1));
        // A weak handle lets the drain count still-outstanding permits at
        // the deadline without holding the channel open itself.
        let weak = tx.downgrade();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(sink, clock, config, rx, weak, shutdown_rx));
        Ok((
            UsageRecorder { tx },
            UsageWriter {
                shutdown,
                handle: Some(handle),
            },
        ))
    }

    /// Flush everything already enqueued, then stop. Call this *before*
    /// releasing leases — events must land while their lease is live.
    pub async fn shutdown(mut self) -> WriterStats {
        let _ = self.shutdown.send(true);
        match self.handle.take() {
            Some(handle) => handle.await.unwrap_or_default(),
            None => WriterStats::default(),
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

async fn run(
    sink: Arc<dyn UsageSink>,
    clock: Arc<dyn Clock>,
    config: UsageWriterConfig,
    mut rx: mpsc::Receiver<UsageEvent>,
    weak: mpsc::WeakSender<UsageEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> WriterStats {
    let mut stats = WriterStats::default();
    let max_batch = config.max_batch.max(1);
    let mut batch: Vec<UsageEvent> = Vec::with_capacity(max_batch);

    loop {
        // Level check at every loop boundary: a shutdown observed anywhere
        // below (including inside the retry backoff) lands here.
        if *shutdown.borrow() {
            return final_flush(&sink, &clock, &config, &mut rx, &weak, &mut batch, stats).await;
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
                return final_flush(&sink, &clock, &config, &mut rx, &weak, &mut batch, stats)
                    .await;
            }
            FillOutcome::Flush => {
                // Retry until delivered or shutdown interrupts; either way
                // the loop-top level check decides what happens next.
                flush_retrying(
                    &sink,
                    &clock,
                    &config,
                    &mut batch,
                    &mut stats,
                    &mut shutdown,
                )
                .await;
                if shutdown.has_changed().is_err() {
                    // Sender gone: same stop path as above.
                    return final_flush(&sink, &clock, &config, &mut rx, &weak, &mut batch, stats)
                        .await;
                }
            }
        }
    }
}

/// Ingest `batch`, retrying with backoff until it is delivered (batch
/// cleared) or shutdown is observed (batch left intact for the final flush).
async fn flush_retrying(
    sink: &Arc<dyn UsageSink>,
    clock: &Arc<dyn Clock>,
    config: &UsageWriterConfig,
    batch: &mut Vec<UsageEvent>,
    stats: &mut WriterStats,
    shutdown: &mut watch::Receiver<bool>,
) {
    loop {
        match sink.ingest(batch, clock.now()).await {
            Ok(report) => {
                stats.accepted += report.accepted;
                stats.duplicate += report.duplicate;
                stats.rejected += report.rejected;
                batch.clear();
                return;
            }
            Err(_) => {
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
    sink: &Arc<dyn UsageSink>,
    clock: &Arc<dyn Clock>,
    config: &UsageWriterConfig,
    rx: &mut mpsc::Receiver<UsageEvent>,
    weak: &mpsc::WeakSender<UsageEvent>,
    batch: &mut Vec<UsageEvent>,
    mut stats: WriterStats,
) -> WriterStats {
    let max_batch = config.max_batch.max(1);
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
        flush_bounded(sink, clock, config, batch, &mut stats).await;
    }
}

/// Ingest `batch` with a bounded number of attempts; an undeliverable batch
/// is counted lost. The batch is cleared either way.
async fn flush_bounded(
    sink: &Arc<dyn UsageSink>,
    clock: &Arc<dyn Clock>,
    config: &UsageWriterConfig,
    batch: &mut Vec<UsageEvent>,
    stats: &mut WriterStats,
) {
    const FINAL_FLUSH_ATTEMPTS: u32 = 3;
    let mut delivered = false;
    for attempt in 1..=FINAL_FLUSH_ATTEMPTS {
        match sink.ingest(batch, clock.now()).await {
            Ok(report) => {
                stats.accepted += report.accepted;
                stats.duplicate += report.duplicate;
                stats.rejected += report.rejected;
                delivered = true;
                break;
            }
            Err(_) if attempt < FINAL_FLUSH_ATTEMPTS => {
                tokio::time::sleep(config.retry_backoff).await;
            }
            Err(_) => {}
        }
    }
    if !delivered {
        stats.lost += batch.len() as u64;
    }
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
