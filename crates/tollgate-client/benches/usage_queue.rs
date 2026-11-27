//! The usage queue under contention (GL-137).
//!
//! Every admitted request reserves a slot in, and later sends one event into,
//! the one usage queue an instance has. GL-133 measured that pair at ~0.27 µs
//! sequentially and ~0.85 µs at ten connections inside the example service;
//! these rows reproduce it without HTTP and split it into its parts:
//!
//! - `reserve_release`: take a slot and give it back unused, the zero-charge
//!   path. Clone of the sender plus the semaphore, and nothing reaches the
//!   writer.
//! - `reserve_record`: take a slot and send the event, which also enqueues it
//!   and notifies the writer task.
//!
//! Each runs uncontended and with seven background threads reserving and
//! releasing at full speed, so the difference between a row and its contended
//! twin isolates the shared lines. The background threads do not record: eight
//! threads recording in a tight loop outrun the one writer task that drains
//! them, fill the queue and shed, and the row would price the writer's drain
//! rate rather than the request path. A service sheds at that rate too, orders
//! of magnitude above any request rate it serves.
//!
//! One recording thread can outrun the writer too, whenever the host starves
//! it (GH-93). The recording rows therefore time chunks smaller than the queue
//! and wait, off the clock, for the writer to take each one.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use jiff::Timestamp;
use tollgate_client::{ManualClock, UsageRecorder, UsageWriter, UsageWriterConfig};
use tollgate_core::{AccountId, CostUnits, PolicyRevision, RequestId, UsageEvent, UsageSource};
use tollgate_store::{IngestError, IngestReport, UsageSink};

/// The most events recorded before the writer must have taken them: a quarter
/// of the queue, so it cannot fill however slowly the writer runs.
const CHUNK: u64 = (QUEUE_CAPACITY / 4) as u64;
const QUEUE_CAPACITY: usize = 65_536;

/// Accepts every batch at once, so the rows price the queue and not a store.
#[derive(Default)]
struct AcceptAll {
    ingested: AtomicU64,
}

#[async_trait]
impl UsageSink for AcceptAll {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        self.ingested
            .fetch_add(events.len() as u64, Ordering::Release);
        Ok(IngestReport {
            accepted: events.len() as u64,
            ..IngestReport::default()
        })
    }
}

fn now() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

fn event() -> UsageEvent {
    UsageEvent::new(
        RequestId(1),
        AccountId(1),
        UsageSource::Overage,
        CostUnits(1),
        now(),
        PolicyRevision::UNSTATED,
        None,
    )
}

/// The example service's shape: a large queue that never sheds under these
/// rows, the same batch size and flush interval.
fn config() -> UsageWriterConfig {
    UsageWriterConfig {
        queue_capacity: QUEUE_CAPACITY,
        max_batch: 256,
        flush_interval: Duration::from_millis(25),
        retry_backoff: Duration::from_millis(50),
        shutdown_drain_deadline: Duration::from_secs(5),
        ingest_timeout: Duration::from_secs(5),
    }
}

fn reserve_release(recorder: &UsageRecorder) {
    black_box(recorder.try_reserve().expect("the queue never fills here"));
}

fn reserve_record(recorder: &UsageRecorder, event: &UsageEvent) {
    recorder
        .try_reserve()
        .expect("the queue never fills here")
        .record(*event);
}

/// Time `reserve_record` alone, never letting more than [`CHUNK`] events wait
/// for the writer. `recorded` counts every event sent through `recorder`.
fn record_bounded(b: &mut Bencher, recorder: &UsageRecorder, sink: &AcceptAll, recorded: &mut u64) {
    let event = event();
    b.iter_custom(|iterations| {
        let mut spent = Duration::ZERO;
        let mut left = iterations;
        while left > 0 {
            let deadline = Instant::now() + Duration::from_secs(30);
            while sink.ingested.load(Ordering::Acquire) < *recorded {
                assert!(Instant::now() < deadline, "the writer stopped draining");
                std::thread::sleep(Duration::from_micros(50));
            }
            let chunk = left.min(CHUNK);
            let start = Instant::now();
            for _ in 0..chunk {
                reserve_record(recorder, &event);
            }
            spent += start.elapsed();
            *recorded += chunk;
            left -= chunk;
        }
        spent
    });
}

/// Seven threads running `work` until dropped, joined before the next row.
struct Background {
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Background {
    fn spawn(recorder: &UsageRecorder, work: fn(&UsageRecorder, &UsageEvent)) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(std::sync::Barrier::new(8));
        let workers = (0..7)
            .map(|_| {
                let (recorder, stop, ready) =
                    (recorder.clone(), Arc::clone(&stop), Arc::clone(&ready));
                std::thread::spawn(move || {
                    let event = event();
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        work(&recorder, &event);
                    }
                })
            })
            .collect();
        ready.wait();
        Self { stop, workers }
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

fn bench_usage_queue(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let sink = Arc::new(AcceptAll::default());
    let (recorder, writer) = rt.block_on(async {
        UsageWriter::spawn(sink.clone(), Arc::new(ManualClock::new(now())), config()).unwrap()
    });
    let mut recorded = 0;
    let mut group = c.benchmark_group("usage_queue");

    group.bench_function("reserve_release", |b| b.iter(|| reserve_release(&recorder)));
    group.bench_function("reserve_record", |b| {
        record_bounded(b, &recorder, &sink, &mut recorded)
    });
    {
        let _load = Background::spawn(&recorder, |recorder, _| reserve_release(recorder));
        group.bench_function("reserve_release_contended_8", |b| {
            b.iter(|| reserve_release(&recorder))
        });
    }
    {
        let _load = Background::spawn(&recorder, |recorder, _| reserve_release(recorder));
        group.bench_function("reserve_record_contended_8", |b| {
            record_bounded(b, &recorder, &sink, &mut recorded)
        });
    }
    group.finish();

    drop(recorder);
    rt.block_on(async { writer.shutdown().await.unwrap() });
}

criterion_group!(benches, bench_usage_queue);
criterion_main!(benches);
