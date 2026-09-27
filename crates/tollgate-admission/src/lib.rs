//! The per-request admission pipeline.
//!
//! The staged path performs, in order:
//!
//! 1. snapshot lookup by [`Principal`] (in-memory map, negative-cached),
//! 2. account status / staleness / permission checks,
//! 3. batch-cap check and cost quote (direct-indexed table),
//! 4. request-count and weighted local rate-token consumption (`governor`),
//! 5. principal and account concurrency acquisition,
//! 6. lease debit, opening the typed pending state.
//!
//! Nothing in this crate performs I/O, takes a blocking lock on the request
//! path, or reads a wall/business clock for a policy decision: `now` is an
//! argument. Misses deny — resolution is the background plane's job
//! (INVARIANTS.md GL-5).
//!
//! Two dependencies do their own bookkeeping underneath that, and the budget
//! counts it rather than pretending it away. `governor` reads its own
//! monotonic clock for bucket arithmetic. [`MokaSnapshotMap`] reads one too,
//! and roughly every sixty-fourth lookup its housekeeper takes a
//! *non-blocking* `try_lock` and drains its read log inline — updating the
//! frequency sketch, and evicting when the cache is at capacity. Neither is a
//! source of snapshot or lease truth, and neither can block a request; both
//! are measured mechanism costs, carried by the `admission/snapshot_lookup_*`
//! rows and by `moka_reads_stay_within_their_amortized_allocation_budget`.
//! [`ArcSwapSnapshotMap`] takes no lock on a read at all.
//!
//! That prohibition covers logging too, so what this plane reports about
//! itself is a tally rather than an event stream: every outcome lands in
//! [`AdmissionCounters`], indexed by reason, and an embedder exports it from
//! off the request path.
//!
//! [`AdmissionEngine::begin`] owns the lookup and returns a generation-pinned
//! [`RequestContext`]; [`RequestContext::admit`] consumes it after body
//! decoding without another map lookup.
//!
//! Two interchangeable snapshot-map implementations exist behind
//! [`SnapshotMap`] — [`MokaSnapshotMap`] and [`ArcSwapSnapshotMap`] — because
//! the design review deliberately treats the cache choice as an empirical
//! question for the perf gate, not a foregone conclusion.

pub mod capacity;
pub mod counters;
pub mod engine;
pub mod generation_model;
mod history;
pub mod maps;
pub mod state;

pub use capacity::{
    CapacityConfigError, CapacityEvidence, CapacityGate, CapacityOccupancy, CapacityPermit,
    ExecutionCapacityGate, ExecutionCapacityMode, ExecutionPermit, NoCapacityPermit, NoGate,
};
pub use counters::{AdmissionCounters, CommitRefusal, CountersSnapshot};
pub use engine::{AdmissionEngine, Committed, Pending, ReadyToStart, Released, RequestContext};
pub use generation_model::{Watermark, accept_positive, accept_revoked, accept_unknown};
pub use history::{
    PublicationError, RefreshBatch, Refreshed, SnapshotHistoryStats, SnapshotRefresh,
};
pub use maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
pub use state::{
    AccountAdmissionState, LeaseSlot, MapEntry, Principal, PublishableSnapshotUpdate, SnapshotMap,
    SnapshotUpdate,
};
#[doc(inline)]
pub use tollgate_core::CancelHandle;

// Compiles and runs the README's examples as doctests without adding them to
// the rendered documentation, so the README cannot drift from the API.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
