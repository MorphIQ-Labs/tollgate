//! The per-request admission pipeline.
//!
//! One call — [`AdmissionEngine::admit`] — performs, in order:
//!
//! 1. snapshot lookup by [`Principal`] (in-memory map, negative-cached),
//! 2. account status / staleness / permission checks,
//! 3. batch-cap check and cost quote (direct-indexed table),
//! 4. weighted local rate-token consumption (`governor`),
//! 5. lease debit, opening the reservation state machine.
//!
//! Nothing in this crate performs I/O, takes a lock on the request path, or
//! reads a clock (`now` is an argument; `governor` uses its own monotonic
//! clock for bucket arithmetic only). Misses deny — resolution is the
//! background plane's job (INVARIANTS.md #5).
//!
//! Two interchangeable snapshot-map implementations exist behind
//! [`SnapshotMap`] — [`MokaSnapshotMap`] and [`ArcSwapSnapshotMap`] — because
//! the design review deliberately treats the cache choice as an empirical
//! question for the perf gate, not a foregone conclusion.

pub mod engine;
pub mod maps;
pub mod state;

pub use engine::{AdmissionEngine, AdmissionRequest, Admitted};
pub use maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
pub use state::{AccountAdmissionState, LeaseSlot, MapEntry, Principal, SnapshotMap};
