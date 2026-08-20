//! The batched usage writer.
//!
//! A bounded mpsc channel separates the request path from billing I/O. The
//! request path reserves a channel slot *before* admitting work
//! ([`UsageRecorder::try_reserve`]); a full channel is
//! `DenyReason::AccountingBackpressure` — shed with zero units charged,
//! never a silent drop, never an unbounded block (INVARIANTS.md #8). The
//! permit outlives execution, so the post-commit send can never fail.
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
/// deliver — reported, never silent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterStats {
    pub accepted: u64,
    pub duplicate: u64,
    pub rejected: u64,
    pub lost: u64,
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
    ) -> (UsageRecorder, UsageWriter) {
        let (tx, rx) = mpsc::channel(config.queue_capacity.max(1));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(sink, clock, config, rx, shutdown_rx));
        (
            UsageRecorder { tx },
            UsageWriter {
                shutdown,
                handle: Some(handle),
            },
        )
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
    mut shutdown: watch::Receiver<bool>,
) -> WriterStats {
    let mut stats = WriterStats::default();
    let max_batch = config.max_batch.max(1);
    let mut batch: Vec<UsageEvent> = Vec::with_capacity(max_batch);

    loop {
        // Level check at every loop boundary: a shutdown observed anywhere
        // below (including inside the retry backoff) lands here.
        if *shutdown.borrow() {
            return final_flush(&sink, &clock, &config, &mut rx, &mut batch, stats).await;
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
                return final_flush(&sink, &clock, &config, &mut rx, &mut batch, stats).await;
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
                    return final_flush(&sink, &clock, &config, &mut rx, &mut batch, stats).await;
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

/// Drain the channel and give the remaining batch a bounded number of
/// delivery attempts; whatever cannot be delivered is *reported* lost.
async fn final_flush(
    sink: &Arc<dyn UsageSink>,
    clock: &Arc<dyn Clock>,
    config: &UsageWriterConfig,
    rx: &mut mpsc::Receiver<UsageEvent>,
    batch: &mut Vec<UsageEvent>,
    mut stats: WriterStats,
) -> WriterStats {
    const FINAL_FLUSH_ATTEMPTS: u32 = 3;
    let max_batch = config.max_batch.max(1);
    loop {
        while batch.len() < max_batch {
            let Ok(event) = rx.try_recv() else {
                break;
            };
            batch.push(event);
        }
        if batch.is_empty() {
            return stats;
        }

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
}
