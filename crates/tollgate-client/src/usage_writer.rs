//! The batched usage writer.
//!
//! A bounded queue separates the request path from billing I/O. The request
//! path reserves a slot *before* admitting work
//! ([`UsageRecorder::try_reserve`]); a full queue is
//! `DenyReason::AccountingBackpressure` — shed with zero units charged,
//! never a silent drop, never an unbounded block (INVARIANTS.md #8). The
//! permit outlives execution, and the shutdown drain below waits for it, so
//! a post-commit send is either ingested or explicitly counted — never
//! silently dropped (INVARIANTS.md #13).
//!
//! The queue is a set of lanes, each a bounded mpsc channel, chosen by the
//! request thread's sticky locality and sized to split `queue_capacity`
//! exactly (#137). A request reserves in its own lane and tries the others
//! before shedding, so the shed point is the whole queue. Lanes keep one
//! thread's sender count, semaphore, tail and entry counter off every other
//! thread's lines; one shared channel put all of them, for every account, on
//! the same few. Events within a lane keep their order; events that overflow
//! into another lane are delivered in that lane's order.
//!
//! The writer never parks on a lane, so a send wakes nothing. It drains every
//! lane on its flush tick — when a partial batch was due anyway — and earlier
//! when a lane reaches its ring point, which rings a doorbell once per fill
//! rather than once per event.
//!
//! The writer task drains the lanes into batches and ingests them through
//! the [`UsageSink`]. Ingest is idempotent on
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
//! The final flush closes every lane (new reservations deny from that
//! instant), then drains until every lane reports disconnected — empty *and*
//! every outstanding permit resolved by sending or dropping, never merely
//! momentarily empty — waking on each permit that resolves, bounded
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

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use jiff::{SignedDuration, Timestamp};
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::{Notify, mpsc, watch};
use tracing::Instrument as _;

use tollgate_core::{DenyReason, LocalSharding, Locality, UsageEvent};
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
    unattributed: AtomicU64,
    attribution_unreported_batches: AtomicU64,
    attribution_degraded: AtomicBool,
    counter_overflow: AtomicBool,
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
    /// Charges the writer has given an outcome. Written only by the task.
    settled: AtomicU64,
    /// Each lane's count of charges that entered it (#137). Set once when the
    /// writer spawns; a counter set nobody attached reads as holding nothing.
    lanes: OnceLock<Arc<[LaneStats]>>,
    // Written from the request path, by as many cores as serve requests.
    shed: Contended,
}

impl WriterCounters {
    #[must_use]
    pub const fn new() -> Self {
        WriterCounters {
            unattributed: AtomicU64::new(0),
            attribution_unreported_batches: AtomicU64::new(0),
            attribution_degraded: AtomicBool::new(false),
            counter_overflow: AtomicBool::new(false),
            accepted: AtomicU64::new(0),
            duplicate: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            unresolved: AtomicU64::new(0),
            last_ingest_ms: AtomicI64::new(i64::MIN),
            settled: AtomicU64::new(0),
            lanes: OnceLock::new(),
            shed: Contended::zero(),
        }
    }

    /// The sink answered. Recorded even when every event in the batch was
    /// refused: `rejected` is an answer, and what this timestamp distinguishes
    /// is a reachable sink from an unreachable one.
    fn record_ingest(&self, report: &tollgate_store::IngestReport, at: Timestamp) {
        self.add_outcome(&self.accepted, report.accepted);
        self.add_outcome(&self.duplicate, report.duplicate);
        self.add_outcome(&self.rejected, report.rejected);
        match report.unattributed {
            Some(n) => self.add_outcome(&self.unattributed, n),
            None => self.add_outcome(&self.attribution_unreported_batches, 1),
        }
        let degraded = report.unattributed != Some(0);
        let previous = self.attribution_degraded.swap(degraded, Ordering::Relaxed);
        if degraded && !previous {
            tracing::warn!(coverage_complete = false, unattributed = ?report.unattributed,
                "credential activity coverage is incomplete or unavailable");
        } else if previous && !degraded {
            tracing::info!(
                coverage_complete = true,
                "credential attribution reporting recovered for this batch"
            );
        }
        self.last_ingest_ms
            .store(at.as_millisecond(), Ordering::Relaxed);
    }

    fn record_lost(&self, events: u64) {
        self.add_outcome(&self.lost, events);
    }

    // Off-path cumulative outcomes saturate visibly. The request-side queue
    // accounting counters retain their existing mechanism and budget.
    fn add_outcome(&self, counter: &AtomicU64, delta: u64) {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                old.checked_add(delta)
            })
            .is_err()
        {
            counter.store(u64::MAX, Ordering::Relaxed);
            if !self.counter_overflow.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    counter_overflow = true,
                    "usage outcome counter overflow; totals are saturated"
                );
            }
        }
    }

    fn set_unresolved(&self, permits: u64) {
        self.unresolved.store(permits, Ordering::Relaxed);
    }

    /// `events` charges have been given a billing outcome — delivered or
    /// reported lost — so they no longer count as unaccounted.
    fn settled(&self, events: u64) {
        self.settled.fetch_add(events, Ordering::Relaxed);
    }

    /// Charges in the queue with no billing outcome yet: every lane's entries
    /// less everything the writer has settled. Each entry is counted before its
    /// event is sent, and settled only after it is received, so the difference
    /// never undercounts what a dying writer holds.
    fn unaccounted(&self) -> u64 {
        let entered = self.lanes.get().map_or(0, |lanes| {
            lanes.iter().fold(0u64, |total, lane| {
                total.wrapping_add(lane.enqueued.load(Ordering::Relaxed))
            })
        });
        entered.saturating_sub(self.settled.load(Ordering::Relaxed))
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
            unattributed: self.unattributed.load(Ordering::Relaxed),
            attribution_unreported_batches: self
                .attribution_unreported_batches
                .load(Ordering::Relaxed),
            counter_overflow: self.counter_overflow.load(Ordering::Relaxed),
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
            unaccounted: self.unaccounted(),
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
    /// The same accounting and coverage diagnostics a clean shutdown returns, read live.
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

/// One lane's request-written state, on a cache line of its own (#137).
#[repr(align(128))]
#[derive(Debug)]
struct LaneStats {
    /// Charges that entered this lane. Read with every other lane's, less what
    /// the writer settled, as the instance's unaccounted count.
    enqueued: AtomicU64,
    /// Set by the send that finds the lane at its ring point, cleared by the
    /// writer before it drains the lane: one doorbell per fill, not per event.
    rung: AtomicBool,
}

/// What the lanes and the writer share. Never cloned per request: a permit
/// reaches it through its own lane, so no request touches this `Arc`'s count.
#[derive(Debug)]
struct Queue {
    stats: Arc<[LaneStats]>,
    /// Wakes the writer early: a lane reaching its ring point, a lane whose
    /// last handle went away, and, while draining, every permit that resolves.
    doorbell: Notify,
    /// Set by the final flush before it closes the lanes. Read on every permit
    /// release and never written again, so it costs a shared read, not a write.
    draining: AtomicBool,
    /// How full a lane gets before its send rings the doorbell.
    ring_at: usize,
    counters: Arc<WriterCounters>,
}

/// Rings the doorbell when a lane's last handle goes away. A field of its own,
/// declared after the sender, so the ring comes *after* the sender drops and
/// the woken writer sees the lane disconnected rather than merely empty.
#[derive(Debug)]
struct RingOnDrop(Arc<Queue>);

impl Drop for RingOnDrop {
    fn drop(&mut self) {
        self.0.doorbell.notify_one();
    }
}

/// One lane of the usage queue: a bounded channel a subset of request threads
/// reserve in, on lines no other lane's threads write.
#[repr(align(128))]
#[derive(Debug)]
struct Lane {
    tx: mpsc::Sender<UsageEvent>,
    index: usize,
    queue: RingOnDrop,
}

/// Cheap-to-clone handle for request handlers.
///
/// The queue is partitioned into lanes by the request thread's sticky
/// locality (#137). One shared channel put every request of every account on
/// the same sender count, semaphore, tail and waker lines, and woke the writer
/// once per event: eight threads measured 3.2 µs per reserve-and-record. A
/// request reserves in its own lane and tries the others before shedding, so
/// the shed point is still exactly `queue_capacity` — a partition, not a
/// reservation.
#[derive(Clone)]
pub struct UsageRecorder {
    lanes: Arc<[Arc<Lane>]>,
    layout: LocalSharding,
    queue: Arc<Queue>,
}

impl UsageRecorder {
    /// Reserve accounting capacity for one request, *before* admission. A
    /// full queue denies here — before any units are reserved or any work
    /// runs.
    pub fn try_reserve(&self) -> Result<UsagePermit, DenyReason> {
        let count = self.lanes.len();
        let mut index = Locality::current().index(self.layout);
        for _ in 0..count {
            let lane = &self.lanes[index];
            match lane.tx.clone().try_reserve_owned() {
                Ok(permit) => {
                    return Ok(UsagePermit {
                        permit: Some(permit),
                        lane: Arc::clone(lane),
                    });
                }
                // A full lane is not a full queue: another lane may have room.
                Err(TrySendError::Full(_)) => {}
                // Closed lanes close together, at shutdown.
                Err(TrySendError::Closed(_)) => break,
            }
            index += 1;
            if index == count {
                index = 0;
            }
        }
        self.queue.counters.record_shed();
        Err(DenyReason::AccountingBackpressure)
    }

    /// Whether the writer task has exited and can no longer accept events.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.lanes[0].tx.is_closed()
    }

    pub(crate) async fn closed(&self) {
        // Every lane closes together: the final flush closes them all, and the
        // task holds every receiver.
        self.lanes[0].tx.closed().await;
    }

    /// The writer's accounting health, readable at any time.
    ///
    /// Deliberately on the *recorder*: a service holds this handle in its
    /// request state, while the [`UsageWriter`] is usually moved into whatever
    /// owns shutdown — so exposing the numbers only there would put them out of
    /// reach of the endpoint that needs to report them.
    #[must_use]
    pub fn health(&self) -> WriterHealth {
        let (depth, capacity) = self.lanes.iter().fold((0, 0), |(depth, capacity), lane| {
            (
                depth + lane.tx.max_capacity() - lane.tx.capacity(),
                capacity + lane.tx.max_capacity(),
            )
        });
        self.queue.counters.health(depth, capacity)
    }
}

/// One reserved accounting slot. Send the committed request's event with
/// [`record`](UsagePermit::record); dropping the permit (deny, cancel,
/// zero-charge path) releases the slot.
pub struct UsagePermit {
    /// `None` only after `record` has sent through it.
    permit: Option<mpsc::OwnedPermit<UsageEvent>>,
    lane: Arc<Lane>,
}

impl UsagePermit {
    pub fn record(mut self, event: UsageEvent) {
        let permit = self
            .permit
            .take()
            .expect("a permit is consumed only by record, which consumes the permit");
        let queue = &self.lane.queue.0;
        let stats = &queue.stats[self.lane.index];
        // Counted from the moment it enters the queue until the writer gives
        // it a billing outcome; a writer that dies in between is therefore
        // able to say how many charges it was carrying. Counted before the
        // send, so the writer can never settle an event that was not counted.
        stats.enqueued.fetch_add(1, Ordering::Relaxed);
        let tx = permit.send(event);
        // No wake per event: the writer drains on its flush tick. A lane that
        // reaches its ring point rings once, so a burst cannot outrun the tick.
        if tx.max_capacity() - tx.capacity() >= queue.ring_at
            && !stats.rung.load(Ordering::Relaxed)
            && !stats.rung.swap(true, Ordering::AcqRel)
        {
            queue.doorbell.notify_one();
        }
    }
}

impl Drop for UsagePermit {
    fn drop(&mut self) {
        // Release the slot first, so a draining writer woken below finds it
        // released rather than still outstanding.
        drop(self.permit.take());
        let queue = &self.lane.queue.0;
        if queue.draining.load(Ordering::Acquire) {
            queue.doorbell.notify_one();
        }
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
    /// Confirmed unattributed newly accepted events. Lost acknowledgements
    /// followed by duplicate replies cannot reconstruct historical counts.
    pub unattributed: u64,
    /// Acknowledged batches whose sink did not report attribution support.
    pub attribution_unreported_batches: u64,
    /// At least one cumulative outcome exceeded u64; its value is saturated.
    pub counter_overflow: bool,
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
        unattributed: 0,
        attribution_unreported_batches: 0,
        counter_overflow: false,
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
    /// this handle never keeps a lane open by itself.
    queue: Arc<[mpsc::WeakSender<UsageEvent>]>,
    queue_capacity: usize,
}

/// The fewest slots a lane is given.
///
/// A lane smaller than a burst turns the sibling fallback into the routine
/// path, and events that overflow into another lane are delivered in that
/// lane's order rather than their thread's. Within one lane a thread's events
/// keep their order; a queue too small for two such lanes keeps one, exactly
/// the single channel it replaced.
const MIN_LANE_CAPACITY: usize = 64;

/// How many lanes a queue of `capacity` slots gets: the host's parallelism as
/// a power of two, so the locality reduction is a mask, but never so many that
/// a lane falls below [`MIN_LANE_CAPACITY`].
fn lane_count(capacity: usize) -> usize {
    let parallelism = LocalSharding::available_parallelism()
        .get()
        .next_power_of_two();
    let affordable = capacity / MIN_LANE_CAPACITY;
    if affordable < 2 {
        return 1;
    }
    // A power of two no larger than what the capacity affords.
    parallelism.min(1 << affordable.ilog2())
}

/// `total` slots split exactly across `count` lanes; the sizes sum to `total`.
fn lane_capacity(total: usize, count: usize, index: usize) -> usize {
    total / count + usize::from(index < total % count)
}

impl UsageWriter {
    pub fn spawn(
        sink: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        config: UsageWriterConfig,
    ) -> Result<(UsageRecorder, UsageWriter), UsageWriterConfigError> {
        config.validate()?;
        Ok(Self::spawn_lanes(
            sink,
            clock,
            config,
            lane_count(config.queue_capacity),
        ))
    }

    /// `spawn` with an explicit lane count, so the multi-lane paths can be
    /// tested on any host. `config` is already validated and `count` is at
    /// least one and at most `queue_capacity`.
    fn spawn_lanes(
        sink: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        config: UsageWriterConfig,
        count: usize,
    ) -> (UsageRecorder, UsageWriter) {
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..count)
            .map(|index| mpsc::channel(lane_capacity(config.queue_capacity, count, index)))
            .unzip();
        // Weak handles let the drain count still-outstanding permits at the
        // deadline without holding any lane open itself.
        let weak: Arc<[mpsc::WeakSender<UsageEvent>]> =
            senders.iter().map(mpsc::Sender::downgrade).collect();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let counters = Arc::new(WriterCounters::new());
        let stats: Arc<[LaneStats]> = (0..count)
            .map(|_| LaneStats {
                enqueued: AtomicU64::new(0),
                rung: AtomicBool::new(false),
            })
            .collect();
        counters
            .lanes
            .set(Arc::clone(&stats))
            .expect("a fresh counter set has no lanes yet");
        let smallest_lane = config.queue_capacity / count;
        let queue = Arc::new(Queue {
            stats,
            doorbell: Notify::new(),
            draining: AtomicBool::new(false),
            // Ring at a batch, or at half a small lane so a burst rings before
            // the lane is full rather than only as it sheds.
            ring_at: config.max_batch.min(smallest_lane / 2).max(1),
            counters: Arc::clone(&counters),
        });
        let lanes: Arc<[Arc<Lane>]> = senders
            .into_iter()
            .enumerate()
            .map(|(index, tx)| {
                Arc::new(Lane {
                    tx,
                    index,
                    queue: RingOnDrop(Arc::clone(&queue)),
                })
            })
            .collect();
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
                Lanes {
                    rx: receivers,
                    weak: Arc::clone(&weak),
                    queue: Arc::clone(&queue),
                },
                shutdown_rx,
            )
            // One writer serves every account, so the span carries the
            // queue's shape; account and lease identify individual events.
            .instrument(tracing::info_span!(
                "usage_writer",
                queue_capacity = config.queue_capacity,
                lanes = count,
                max_batch = config.max_batch
            )),
        );
        (
            UsageRecorder {
                lanes,
                layout: LocalSharding::new(
                    std::num::NonZeroUsize::new(count).expect("lane_count is at least one"),
                ),
                queue,
            },
            UsageWriter {
                shutdown,
                deadline,
                handle: Some(handle),
                counters,
                queue: weak,
                queue_capacity: config.queue_capacity,
            },
        )
    }

    /// The writer's accounting health, readable at any time — the same numbers
    /// [`UsageRecorder::health`] reports, for an embedder that holds this half.
    #[must_use]
    pub fn health(&self) -> WriterHealth {
        let depth = self
            .queue
            .iter()
            .filter_map(mpsc::WeakSender::upgrade)
            .map(|tx| tx.max_capacity() - tx.capacity())
            .sum();
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
            unaccounted: self.counters.unaccounted(),
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

/// The writer's half of the lanes.
struct Lanes {
    rx: Vec<mpsc::Receiver<UsageEvent>>,
    weak: Arc<[mpsc::WeakSender<UsageEvent>]>,
    queue: Arc<Queue>,
}

/// What one pass over the lanes found.
enum Collected {
    /// `batch` reached `max_batch`: deliver it before collecting more.
    Full,
    /// Every lane is empty for now. `disconnected` when no lane can ever yield
    /// again: every handle is gone, or the lanes were closed and every permit
    /// has resolved.
    Empty { disconnected: bool },
}

impl Lanes {
    /// Move whatever the lanes hold into `batch`, up to `max_batch`, without
    /// waiting. A lane's doorbell flag is cleared *before* the lane is read, so
    /// a send that lands after the read rings again rather than waiting a tick.
    fn collect(&mut self, batch: &mut Vec<UsageEvent>, max_batch: usize) -> Collected {
        let mut disconnected = true;
        for (index, rx) in self.rx.iter_mut().enumerate() {
            self.queue.stats[index].rung.store(false, Ordering::Release);
            loop {
                if batch.len() >= max_batch {
                    return Collected::Full;
                }
                match rx.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(TryRecvError::Empty) => {
                        disconnected = false;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
        Collected::Empty { disconnected }
    }
}

async fn run(writer: Writer, mut lanes: Lanes, mut shutdown: watch::Receiver<bool>) -> WriterStats {
    let config = writer.config;
    let max_batch = config.max_batch;
    let mut batch: Vec<UsageEvent> = Vec::with_capacity(max_batch);

    loop {
        // Level check at every loop boundary: a shutdown observed anywhere
        // below (including inside the retry backoff) lands here.
        if *shutdown.borrow() {
            return final_flush(&writer, &mut lanes, &mut batch).await;
        }

        // The writer never parks on a lane's receiver, so a send finds no
        // waker to wake (#137). It drains on this tick, which is when a
        // partial batch was due anyway, and earlier only when a lane rings.
        let deadline = tokio::time::sleep(config.flush_interval);
        tokio::pin!(deadline);
        let stop = loop {
            let due = tokio::select! {
                () = lanes.queue.doorbell.notified() => false,
                () = &mut deadline => true,
                changed = shutdown.changed() => {
                    // Err = sender dropped without shutdown(); treat both as
                    // stop so the task can never outlive its handle usefully.
                    if changed.is_err() || *shutdown.borrow() {
                        break true;
                    }
                    continue;
                }
            };
            let disconnected = loop {
                match lanes.collect(&mut batch, max_batch) {
                    Collected::Full => {
                        // Retry until delivered or shutdown interrupts; either
                        // way the loop-top level check decides what happens next.
                        flush_retrying(&writer, &mut batch, &mut shutdown).await;
                        if *shutdown.borrow() || shutdown.has_changed().is_err() {
                            break false;
                        }
                    }
                    Collected::Empty { disconnected } => break disconnected,
                }
            };
            if *shutdown.borrow() || shutdown.has_changed().is_err() {
                break true;
            }
            if due && !batch.is_empty() {
                flush_retrying(&writer, &mut batch, &mut shutdown).await;
            }
            // Every handle is gone: nothing can arrive, so stop as the channel's
            // close used to (the recorder's lanes ring as they drop).
            if disconnected {
                break true;
            }
            if due {
                break false;
            }
        };
        if stop {
            return final_flush(&writer, &mut lanes, &mut batch).await;
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
        let ingest =
            tokio::time::timeout(config.ingest_timeout, ingest_checked(&**sink, batch, now));
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
    lanes: &mut Lanes,
    batch: &mut Vec<UsageEvent>,
) -> WriterStats {
    let config = &writer.config;
    let max_batch = config.max_batch;
    // Refuse new reservations from this instant. Permits already handed out
    // keep their slots and can still deliver into the drain below. A closed
    // lane reports disconnected only once it is empty and every one of its
    // permits has sent or dropped — never merely because it is momentarily
    // empty — which is the done signal the single channel's `recv` gave.
    // Every permit that resolves from here rings the doorbell, so the drain
    // waits on it rather than polling.
    lanes.queue.draining.store(true, Ordering::Release);
    for rx in &mut lanes.rx {
        rx.close();
    }
    let deadline = writer.deadline.within(config.shutdown_drain_deadline);
    let mut expired = false;
    loop {
        let drained = loop {
            match lanes.collect(batch, max_batch) {
                Collected::Full => flush_bounded(writer, batch, deadline).await,
                Collected::Empty { disconnected } => break disconnected,
            }
        };
        if drained || expired {
            if !batch.is_empty() {
                flush_bounded(writer, batch, deadline).await;
            }
            // A final sweep after the deadline has already collected anything
            // that was queued, so what remains outstanding is permits.
            if !drained {
                writer
                    .counters
                    .set_unresolved(outstanding_permits(&lanes.weak));
            }
            return writer.counters.stats();
        }
        // Each wake is a resolved permit, a dropped lane, or the deadline; the
        // finite permit set bounds the loop. One more sweep follows an expiry.
        if tokio::time::timeout_at(deadline, lanes.queue.doorbell.notified())
            .await
            .is_err()
        {
            expired = true;
        }
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
        match tokio::time::timeout_at(attempt_deadline, ingest_checked(&**sink, batch, now)).await {
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

async fn ingest_checked(
    sink: &dyn UsageSink,
    events: &[UsageEvent],
    now: Timestamp,
) -> Result<tollgate_store::IngestReport, tollgate_store::IngestError> {
    let report = sink.ingest(events, now).await?;
    report
        .validate(events.len())
        .map_err(tollgate_store::IngestError::Unavailable)?;
    Ok(report)
}

/// Slots still held at the drain deadline. The upgrade succeeds exactly
/// while some permit keeps the channel alive — which is when there is
/// something to report — and the momentary strong sender is dropped
/// immediately, so it cannot mask completion.
fn outstanding_permits(lanes: &[mpsc::WeakSender<UsageEvent>]) -> u64 {
    lanes
        .iter()
        .filter_map(mpsc::WeakSender::upgrade)
        .map(|tx| (tx.max_capacity() - tx.capacity()) as u64)
        .sum()
}

#[cfg(test)]
mod layout_tests {
    #[test]
    fn attribution_and_existing_outcome_counters_saturate_visibly() {
        use super::*;
        use tracing_subscriber::layer::SubscriberExt;
        #[derive(Clone)]
        struct OverflowEvents(Arc<AtomicU64>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for OverflowEvents {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct Fields(bool);
                impl tracing::field::Visit for Fields {
                    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {
                    }
                    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                        if field.name() == "counter_overflow" {
                            self.0 = value;
                        }
                    }
                }
                let mut fields = Fields(false);
                event.record(&mut fields);
                if fields.0 {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        let events = Arc::new(AtomicU64::new(0));
        // This unit-test binary has one subscriber. Overflow is reached only
        // here; global installation also makes tracing's callsite cache stable.
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(OverflowEvents(events.clone())),
        )
        .unwrap();
        let counters = WriterCounters::new();
        for counter in [
            &counters.accepted,
            &counters.duplicate,
            &counters.rejected,
            &counters.lost,
            &counters.unattributed,
            &counters.attribution_unreported_batches,
        ] {
            counter.store(u64::MAX - 1, Ordering::Relaxed);
            counters.add_outcome(counter, 1);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
            counters.add_outcome(counter, 1);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }
        assert!(counters.stats().counter_overflow);
        assert_eq!(events.load(Ordering::Relaxed), 1);
        let counters = WriterCounters::new();
        counters.record_ingest(
            &tollgate_store::IngestReport {
                accepted: 4,
                duplicate: 2,
                rejected: 1,
                unattributed: Some(3),
            },
            Timestamp::UNIX_EPOCH,
        );
        counters.record_ingest(
            &tollgate_store::IngestReport::default(),
            Timestamp::UNIX_EPOCH,
        );
        let stats = counters.stats();
        assert_eq!(
            (
                stats.accepted,
                stats.duplicate,
                stats.rejected,
                stats.unattributed,
                stats.attribution_unreported_batches
            ),
            (4, 2, 1, 3, 1)
        );
        assert!(!stats.counter_overflow);
    }

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

#[cfg(test)]
mod lane_tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use tollgate_core::{AccountId, CostUnits, PolicyRevision, RequestId, UsageSource};
    use tollgate_store::{IngestError, IngestReport};

    /// Accepts everything and remembers which request ids it was given.
    #[derive(Default)]
    struct Recording(Mutex<Vec<u128>>);

    #[async_trait::async_trait]
    impl UsageSink for Recording {
        async fn ingest(
            &self,
            events: &[UsageEvent],
            _now: Timestamp,
        ) -> Result<IngestReport, IngestError> {
            self.0
                .lock()
                .unwrap()
                .extend(events.iter().map(|event| event.request_id.0));
            Ok(IngestReport {
                accepted: events.len() as u64,
                unattributed: Some(0),
                ..IngestReport::default()
            })
        }
    }

    fn event(id: u128) -> UsageEvent {
        UsageEvent::new(
            RequestId(id),
            AccountId(1),
            UsageSource::Overage,
            CostUnits(1),
            Timestamp::from_second(100).unwrap(),
            PolicyRevision::UNSTATED,
            None,
        )
    }

    fn config(queue_capacity: usize, max_batch: usize) -> UsageWriterConfig {
        UsageWriterConfig {
            queue_capacity,
            max_batch,
            flush_interval: Duration::from_secs(60),
            retry_backoff: Duration::from_millis(10),
            shutdown_drain_deadline: Duration::from_secs(5),
            ingest_timeout: Duration::from_secs(5),
        }
    }

    fn spawn(
        sink: &Arc<Recording>,
        queue_capacity: usize,
        max_batch: usize,
        lanes: usize,
    ) -> (UsageRecorder, UsageWriter) {
        UsageWriter::spawn_lanes(
            Arc::clone(sink) as Arc<dyn UsageSink>,
            Arc::new(crate::ManualClock::new(
                Timestamp::from_second(100).unwrap(),
            )),
            config(queue_capacity, max_batch),
            lanes,
        )
    }

    #[test]
    fn lanes_partition_the_capacity_exactly_and_never_go_below_their_floor() {
        for (total, count) in [(256, 4), (4_096, 16), (100, 3), (7, 7)] {
            let sizes: Vec<usize> = (0..count).map(|i| lane_capacity(total, count, i)).collect();
            assert_eq!(sizes.iter().sum::<usize>(), total, "{total}/{count}");
            assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1);
        }
        assert_eq!(lane_count(1), 1);
        assert_eq!(
            lane_count(MIN_LANE_CAPACITY * 2 - 1),
            1,
            "one lane below two floors"
        );
        for capacity in [128, 4_096, 65_536] {
            let count = lane_count(capacity);
            assert!(count.is_power_of_two());
            assert!(
                capacity / count >= MIN_LANE_CAPACITY,
                "{capacity} -> {count}"
            );
        }
    }

    /// A full lane is not a full queue: one thread, whose own lane fills first,
    /// still reserves every slot of every lane, and the next request sheds at
    /// exactly `queue_capacity` (INVARIANTS.md #8).
    #[tokio::test(start_paused = true)]
    async fn the_shed_point_is_the_whole_queue_across_lanes() {
        let sink = Arc::new(Recording::default());
        let (recorder, writer) = spawn(&sink, 256, 64, 4);
        let permits: Vec<_> = (0..256).map(|_| recorder.try_reserve().unwrap()).collect();
        assert_eq!(recorder.health().queue_depth, 256);
        assert_eq!(recorder.health().queue_capacity, 256);
        assert_eq!(
            recorder.try_reserve().err(),
            Some(DenyReason::AccountingBackpressure)
        );
        assert_eq!(recorder.health().shed, 1);
        drop(permits);
        assert_eq!(recorder.health().queue_depth, 0);
        assert!(writer.shutdown().await.unwrap().unresolved == 0);
    }

    /// Events in every lane are delivered by the drain, and permits still held
    /// in several lanes at the deadline are all reported unresolved.
    #[tokio::test(start_paused = true)]
    async fn the_drain_delivers_every_lane_and_reports_every_lanes_permits() {
        let sink = Arc::new(Recording::default());
        let (recorder, writer) = spawn(&sink, 256, 256, 4);
        // 200 records from one thread fill its lane and spill into the rest.
        for id in 0..200 {
            recorder.try_reserve().unwrap().record(event(id));
        }
        assert_eq!(recorder.health().unaccounted, 200);
        let held: Vec<_> = (0..40).map(|_| recorder.try_reserve().unwrap()).collect();
        let stats = writer.shutdown().await.unwrap();
        let mut delivered = sink.0.lock().unwrap().clone();
        delivered.sort_unstable();
        assert_eq!(
            delivered,
            (0..200).collect::<Vec<_>>(),
            "every lane drained"
        );
        assert_eq!(stats.accepted, 200);
        assert_eq!(
            stats.unresolved, 40,
            "permits held across lanes are all reported"
        );
        assert_eq!(recorder.health().unaccounted, 0);
        drop(held);
    }

    /// A permit that resolves during the drain wakes it: the drain returns as
    /// soon as the last one does, not at its deadline.
    #[tokio::test(start_paused = true)]
    async fn a_resolving_permit_in_any_lane_completes_the_drain() {
        let sink = Arc::new(Recording::default());
        let (recorder, writer) = spawn(&sink, 256, 64, 4);
        let held: Vec<_> = (0..100).map(|_| recorder.try_reserve().unwrap()).collect();
        let began = tokio::time::Instant::now();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            for (id, permit) in (0..).zip(held) {
                permit.record(event(id));
            }
        });
        let stats = writer.shutdown().await.unwrap();
        release.await.unwrap();
        assert_eq!(stats.accepted, 100);
        assert_eq!(stats.unresolved, 0);
        assert_eq!(
            began.elapsed(),
            Duration::from_millis(50),
            "woken, not timed out"
        );
    }

    /// A lane that reaches its ring point is delivered before the flush tick,
    /// so a burst cannot back up to the shed point waiting for it.
    #[tokio::test(start_paused = true)]
    async fn a_full_batch_is_delivered_before_the_tick() {
        let sink = Arc::new(Recording::default());
        let (recorder, writer) = spawn(&sink, 256, 16, 4);
        for id in 0..16 {
            recorder.try_reserve().unwrap().record(event(id));
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(sink.0.lock().unwrap().len(), 16, "delivered without a tick");
        assert_eq!(writer.shutdown().await.unwrap().accepted, 16);
    }

    /// Below the ring point nothing wakes the writer; the tick delivers.
    #[tokio::test(start_paused = true)]
    async fn a_partial_batch_waits_for_the_tick() {
        let sink = Arc::new(Recording::default());
        let (recorder, writer) = spawn(&sink, 256, 64, 4);
        for id in 0..3 {
            recorder.try_reserve().unwrap().record(event(id));
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(sink.0.lock().unwrap().is_empty(), "no wake per event");
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(sink.0.lock().unwrap().len(), 3, "the tick delivered it");
        assert_eq!(writer.shutdown().await.unwrap().accepted, 3);
    }

    /// Dropping every recorder handle stops the writer without a tick: the
    /// lanes ring as their last handles go.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_recorder_stops_the_writer_promptly() {
        let sink = Arc::new(Recording::default());
        let (recorder, mut writer) = spawn(&sink, 256, 64, 4);
        recorder.try_reserve().unwrap().record(event(1));
        drop(recorder);
        let handle = writer.handle.take().unwrap();
        let stats = tokio::time::timeout(Duration::from_millis(1), handle)
            .await
            .expect("stopped before any tick")
            .unwrap();
        assert_eq!(stats.accepted, 1);
    }
}
