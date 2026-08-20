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
//! the [`UsageSink`](quota_store::UsageSink). Ingest is idempotent on
//! request id (INVARIANTS.md #7), so retrying a whole batch after a backend
//! error is always safe. A failing backend is retried with backoff forever
//! while the channel backs up and sheds upstream — memory stays bounded at
//! one in-flight batch plus the channel.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use quota_core::{DenyReason, UsageEvent};
use quota_store::UsageSink;

use crate::clock::Clock;

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
    handle: tokio::task::JoinHandle<WriterStats>,
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
        (UsageRecorder { tx }, UsageWriter { shutdown, handle })
    }

    /// Flush everything already enqueued, then stop. Call this *before*
    /// releasing leases — events must land while their lease is live.
    pub async fn shutdown(self) -> WriterStats {
        let _ = self.shutdown.send(true);
        self.handle.await.unwrap_or_default()
    }
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
        // Fill the batch until full, the flush interval lapses with content,
        // or shutdown is signalled.
        let deadline = tokio::time::sleep(config.flush_interval);
        tokio::pin!(deadline);
        let stop = loop {
            tokio::select! {
                event = rx.recv() => match event {
                    Some(event) => {
                        batch.push(event);
                        if batch.len() >= max_batch {
                            break false;
                        }
                    }
                    None => break true,
                },
                _ = &mut deadline, if !batch.is_empty() => break false,
                _ = shutdown.changed() => break *shutdown.borrow(),
            }
        };

        if stop {
            // Drain whatever is already in the channel, then final-flush.
            while let Ok(event) = rx.try_recv() {
                batch.push(event);
            }
            if !batch.is_empty()
                && !flush(&sink, &clock, &config, &mut batch, &mut stats, None).await
            {
                stats.lost += batch.len() as u64;
            }
            return stats;
        }

        if !batch.is_empty() {
            // Retry forever while running; the bounded channel sheds
            // upstream in the meantime. Shutdown interrupts the retry loop
            // and the final flush above gets one bounded chance.
            let _ = flush(
                &sink,
                &clock,
                &config,
                &mut batch,
                &mut stats,
                Some(&mut shutdown),
            )
            .await;
        }
    }
}

/// Ingest `batch`, retrying on storage errors. Returns true when the batch
/// was delivered (batch is cleared); false when interrupted by shutdown or —
/// with `shutdown: None` (final flush) — after exhausting bounded retries.
async fn flush(
    sink: &Arc<dyn UsageSink>,
    clock: &Arc<dyn Clock>,
    config: &UsageWriterConfig,
    batch: &mut Vec<UsageEvent>,
    stats: &mut WriterStats,
    mut shutdown: Option<&mut watch::Receiver<bool>>,
) -> bool {
    const FINAL_FLUSH_ATTEMPTS: u32 = 3;
    let mut attempts = 0u32;
    loop {
        match sink.ingest(batch, clock.now()).await {
            Ok(report) => {
                stats.accepted += report.accepted;
                stats.duplicate += report.duplicate;
                stats.rejected += report.rejected;
                batch.clear();
                return true;
            }
            Err(_) => {
                attempts += 1;
                match shutdown.as_deref_mut() {
                    Some(rx) => {
                        tokio::select! {
                            _ = tokio::time::sleep(config.retry_backoff) => {}
                            _ = rx.changed() => {}
                        }
                        if *rx.borrow() {
                            return false; // final flush will retry once more
                        }
                    }
                    None => {
                        if attempts >= FINAL_FLUSH_ATTEMPTS {
                            return false;
                        }
                        tokio::time::sleep(config.retry_backoff).await;
                    }
                }
            }
        }
    }
}
