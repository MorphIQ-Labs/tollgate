//! The usage queue under contention (#137).
//!
//! Every admitted request reserves a slot in, and later sends one event into,
//! the one usage queue an instance has. #133 measured that pair at ~0.27 µs
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

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use jiff::Timestamp;
use tollgate_client::{ManualClock, UsageRecorder, UsageWriter, UsageWriterConfig};
use tollgate_core::{AccountId, CostUnits, PolicyRevision, RequestId, UsageEvent, UsageSource};
use tollgate_store::{IngestError, IngestReport, UsageSink};

/// Accepts every batch at once, so the rows price the queue and not a store.
struct AcceptAll;

#[async_trait]
impl UsageSink for AcceptAll {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
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
        queue_capacity: 65_536,
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
    let (recorder, writer) = rt.block_on(async {
        UsageWriter::spawn(
            Arc::new(AcceptAll),
            Arc::new(ManualClock::new(now())),
            config(),
        )
        .unwrap()
    });
    let event = event();
    let mut group = c.benchmark_group("usage_queue");

    group.bench_function("reserve_release", |b| b.iter(|| reserve_release(&recorder)));
    group.bench_function("reserve_record", |b| {
        b.iter(|| reserve_record(&recorder, &event))
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
            b.iter(|| reserve_record(&recorder, &event))
        });
    }
    group.finish();

    drop(recorder);
    rt.block_on(async { writer.shutdown().await.unwrap() });
}

criterion_group!(benches, bench_usage_queue);
criterion_main!(benches);
