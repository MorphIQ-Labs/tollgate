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
//! Nothing in this crate performs I/O, takes a lock on the request path, or
//! reads a clock (`now` is an argument; `governor` uses its own monotonic
//! clock for bucket arithmetic only). Misses deny — resolution is the
//! background plane's job (INVARIANTS.md #5).
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

pub mod counters;
pub mod engine;
pub mod generation_model;
pub mod maps;
pub mod state;

pub use counters::{AdmissionCounters, CountersSnapshot};
pub use engine::{
    AdmissionEngine, CapacityEvidence, CapacityGate, CapacityPermit, Committed, NoCapacityPermit,
    NoGate, Pending, ReadyToStart, Released, RequestContext,
};
pub use generation_model::{Watermark, accept_positive, accept_revoked, accept_unknown};
pub use maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
pub use state::{
    AccountAdmissionState, LeaseSlot, MapEntry, Principal, PublishableSnapshotUpdate, SnapshotMap,
    SnapshotUpdate,
};
