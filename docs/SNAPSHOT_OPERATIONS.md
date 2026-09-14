# Snapshot distribution health and refusals

`SnapshotManager::ready()` reports the configured resolution rule while the
task is alive: Fixed requires every tracked principal to be resolved; All
requires some resolved principal, or an empty catalogue. A fresh negative
resolution counts as resolved. The admission map independently refuses unknown,
revoked and stale requests. `InstanceRuntime` adds funding and other background
task health to this snapshot rule.

The snapshot and lease managers store `false` before their health watch closes
on normal exit, unwind or task cancellation. Retained receivers can read that
value directly. Channel closure still distinguishes an exited task from a live
but unhealthy one. Cancellation takes effect when the executor destroys the
future; it cannot preempt blocking custom source code. The publishers are owned
before spawning, so cancellation before the first poll also closes with false.
Graceful shutdown remains the way to drain usage and return quota.

Read `SnapshotManager::counters().snapshot()` directly, or use
`RuntimeHandle::report().snapshots`. The pricing example exports the same
snapshot counters under `snapshots` in `/metrics`:

| Field | Meaning |
| --- | --- |
| `refresh_attempts` | Completed fetch attempts processed by the manager |
| `refresh_failures` | Fetches that returned a source error |
| `refresh_timeouts` | Fetches abandoned at the configured time limit |
| `discovery_failures` | Failed or timed-out catalogue enumerations |
| `refused_updates` | Pushes or fetched updates refused by generation ordering |
| `history_evictions` | Histories reclaimed under capacity pressure; visible entries also removed |
| `publication_failures` | Reservation or publication refused by a retention/fence check |
| `unresolved` | Principals unresolved at the last resolution pass |

An ordinary refresh of an already installed positive at the same generation is
quiet and does not increment `refused_updates`. Newer positives, accepted
revocations and unversioned absences also leave that counter unchanged. The
counter includes older positives/revocations and positive replays at a retained
revocation's generation. A refusal preserves the previous resolution, deadline
and generation; the existing refresh retry backoff still applies.

Each refusal emits a warning with `origin` (`push` or `refresh`), `principal`,
`kind` (`positive` or `revoked`), `offered` generation and `retained` watermark.
Use these fields to identify a delayed publisher or lagging source replica.
Recover the source's authoritative generation; do not clear tombstones or lower
watermarks to make a replay pass. The aggregate counter supports alerting even
when event collection is disabled, and carries no per-principal metric labels.

Interpret counters with task health. A stopped manager may retain
`unresolved = 0` while readiness is false. All mode can remain ready with a
nonzero unresolved count. A flat attempt counter alone does not establish task
death: an empty catalogue or negative-cache schedule can also explain it.

Snapshot maps retain a bounded number of generation histories, including pending
source reads. `MokaSnapshotMap::new(max_capacity)` uses that many history slots
(at least one when visible caching is disabled); `with_capacities` configures
visible and history budgets independently. ArcSwap defaults to 65,536 histories
and 4,096 visible negatives, with `with_capacities` for explicit budgets.
`InstanceRuntimeConfig::snapshot_history_capacity` is a required `NonZeroUsize`.
Choose it to cover the maximum simultaneously served principal set and refresh
working set. Fixed sets exceeding the budget fail startup validation. All mode
can exceed it, but eviction then denies principals until authoritative refetch;
`unresolved` and `history_evictions` expose that capacity pressure. For a service
that must serve its whole catalogue continuously, provision the budget for that
catalogue rather than treating churn eviction as equivalent availability.

Ordinary visible eviction and expiry preserve generation history. History
reclamation removes visible entries and outstanding read permissions together;
TTL expiry alone never deletes a watermark. A push for forgotten history is a
refetch hint, not authority to reinstall its payload. A fresh source read must
start after reservation and must observe the durable authoritative store.
Configure HTTP/store adapters against the authoritative primary, bypassing
response caches and lagging replicas for reconstruction. An unavailable primary
must return an error. Source errors leave misses denying, increment
`refresh_failures`, and retry on the existing refresh/negative-retry schedule;
an unknown response also denies and cannot reopen forgotten history to pushes.
`publication_failures` reports superseded reads or failed reservations separately.

For direct `SnapshotMap` users, handle the `Result` from every publication. On
`RefreshRequired`, call `prepare_refreshes`, immediately process its `evicted`
principals, invoke each returned read's `fetch` (or synchronous `read`) with a
new authoritative operation, translate the result with `filter_map`, and publish
through `apply_refreshed_many_at`. A `Superseded` response must be discarded and
retried with a new reservation/read. Split batches at `generation_capacity`.
Read identities use checked allocation; exhaustion requires replacing the map
and rebuilding from the source, preserving live account lease slots. Never
attach an old response to a new read or clear durable tombstones to recover.
The running `SnapshotManager` owns this publication/resolution protocol; avoid
independent publishers mutating its map and bypassing its readiness bookkeeping.

The history bound covers generation entries and their ordering index, not the
source catalogue, tracked membership, caller-owned in-flight responses, or
irreversible account spend history. `history_stats` reports retained occupancy.
Increasing the budget requires constructing a replacement map/runtime and doing
a fresh initial load; durable store state remains authoritative. Drain and shut
down the old runtime normally so accounting and lease retirement complete.

Rust callers must handle publication results and supply the runtime history
budget; custom `SnapshotMap` adapters must forward the new refresh, occupancy and
visibility methods when wrapping a bounded map. Unbounded custom maps retain
compatibility defaults and do not acquire a bounded-retention guarantee. Metrics
structs add `history_evictions` and `publication_failures`; JSON consumers should
accept these additional numeric fields. Rebuild callers against the same tag.
There is no database or snapshot-wire migration. Older and newer instances may
coexist against the same durable source, provided reconstruction reads satisfy
the source-ordering contract. No request-path or threshold changes are involved.
