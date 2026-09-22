# Instance-local sharding

Opt-in. The default, `LocalSharding::SINGLE`, is one shard and this document
does not apply to it.

## What it buys, and the condition attached

A sharded instance keeps one copy of each account's mutable hot-path state per
shard — admission state, lease counters, rate buckets, outcome tallies — so that
worker threads hammering the same account stop fighting over the same cache
lines. Measured on a 24-core x86_64 development host, eight threads on one
account cost **176 ns** per admission when each held its own shard and **862 ns**
when two shared one: a factor of 4.9 (#123). Across eight *different* accounts
the same collision cost ×1.38, because those threads share less per-shard state
to begin with.

That benefit has a condition, and the condition is a property of your
deployment rather than of the library:

> Each request-serving thread must hold an affinity that no other
> request-serving thread shares, after reduction by the shard count.

Affinities come from one process-global counter. A thread takes its number on
its **first** use of the request path, not when it is spawned, and numbers are
never recycled — a thread that took one and exited keeps it. `Locality::index`
reduces the number onto the shard count, so two threads whose numbers are
congruent modulo that count share every sharded structure they touch.

The counter hands out `0, 1, 2, …`, so the condition holds exactly while the
affinities handed out do not outnumber the shards.

## Choosing a shard count

Start at the runtime's worker-thread count and validate on your own host.

A Tokio multi-thread runtime defaults to one worker per logical CPU, and
`LocalSharding::available_parallelism()` returns the same number, so those two
agree unless you have configured `worker_threads` yourself. If you have, size
the shards to the workers, not to the CPUs.

Sizing **below** the worker count guarantees crowding: there are not enough
shards to go round, and some workers will share. Sizing above it costs memory
per account and buys nothing.

Threads that are not serving requests still consume affinities if they reach
the request path, and every one they take displaces a worker. The library no
longer does this to you — control-plane reads inside `tollgate-client` use a
fixed observer affinity and claim nothing (#124) — but an embedder that admits
from `spawn_blocking`, from a second runtime, or from short-lived threads is
spending the same budget.

## Reading the report

`RuntimeHandle::report()` carries a `sharding` block:

```rust
let occupancy = handle.report().sharding;
if occupancy.is_crowded() {
    // `occupancy.shards` shards, `occupancy.affinities_assigned` affinities,
    // `occupancy.crowded_shards()` of them carrying more than one.
}
```

Embedders that drive `AdmissionEngine` without `InstanceRuntime` can read the
same thing from the map's own layout:

```rust
let occupancy = engine.map().local_sharding().occupancy();
```

`is_crowded()` is true when this process has handed out more affinities than it
has shards. It is a statement about affinities, not about live threads: the
process can say how many it issued but not which threads still hold them,
because nothing is recycled. That is the useful reading rather than a weaker
one — an affinity a departed thread took still displaces every affinity issued
after it.

Both reads are control-plane calls. No admission decision consults them, and
they take no locks.

## What to do about a crowded instance

1. **Compare `shards` against your worker count.** If shards are fewer, raise
   them to match and redeploy. This is the common case and the only one with a
   configuration fix.
2. **If they already match, something else is consuming affinities.** Look for
   request-path calls from outside the worker pool: `spawn_blocking`, a second
   runtime, or threads created per unit of work. Move that work onto the worker
   pool or stop admitting from it.
3. **Do not raise the shard count to out-run the leak.** Affinities are never
   recycled, so a process that issues them continuously will crowd any count
   you pick; raising it delays the report without changing the outcome.

A crowded instance is not incorrect. Every accounting and authorization
guarantee holds unchanged — affinity is a cache-locality hint and never
authorization evidence. What is lost is the performance the layout was enabled
to buy, which is why this is reported rather than refused.

## Related

- `docs/DESIGN.md` — why sharding is opt-in, and what each sharded component
  partitions.
- `INVARIANTS.md` #40 — the property stated as a contract, and what enforces
  which half of it.
- `docs/PERFORMANCE.md` — running the benchmarks that price the layout.
