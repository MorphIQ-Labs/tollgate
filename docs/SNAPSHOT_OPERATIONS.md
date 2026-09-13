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

This correction adds `SnapshotStats::refused_updates` and adds
`refused_updates` and the previously omitted `refresh_timeouts` to the pricing
example's metrics object. Rust callers constructing or exhaustively
destructuring those public structs must include the new fields or use `..`
when destructuring. JSON consumers should accept the additional numeric
fields. Rebuild affected callers against the same tag; no database migration,
wire snapshot change, configuration change or coordinated restart is required.
No request-path code or benchmark threshold changes are involved.
