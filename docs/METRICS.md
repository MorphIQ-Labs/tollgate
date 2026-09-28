# Metrics reference

Every counter, gauge and report field an operator reads from a running
instance, where it comes from, and what a change in it means. The thresholds
and the reasoning behind them are in the design record's
[Observability](DESIGN.md#observability) section; this page is the field-by-field
reference and defers to that section, in particular
[Signals worth a response](DESIGN.md#signals-worth-a-response), for when to act.

The numbers come from these library surfaces:

| Surface | Read it with | Defined in |
|---|---|---|
| Admission counters | `AdmissionCounters::snapshot()` → `CountersSnapshot` | [`counters.rs`](../crates/tollgate-admission/src/counters.rs) |
| Runtime report | `RuntimeHandle::report()`, `readiness(now)`, `funding(now)`, `account_reports(now)` | [`runtime.rs`](../crates/tollgate-client/src/runtime.rs), [`registry.rs`](../crates/tollgate-client/src/registry.rs) |
| Refill (lease) counters | `LeaseCounters::snapshot()` → `LeaseStats` | [`lease_manager.rs`](../crates/tollgate-client/src/lease_manager.rs) |
| Snapshot counters | `SnapshotCounters::snapshot()` → `SnapshotStats` | [`snapshot_manager.rs`](../crates/tollgate-client/src/snapshot_manager.rs) |
| Accounting health | `UsageRecorder::health()` / `UsageWriter::health()` → `WriterHealth` | [`usage_writer.rs`](../crates/tollgate-client/src/usage_writer.rs) |
| Credential feed | `KeyManagerMonitor::report(now)` → `KeyManagerReport` | [`key_manager.rs`](../crates/tollgate-client/src/key_manager.rs) |
| Budget rollover | `PeriodRollerMonitor::report()` → `PeriodRollerReport` | [`period_roller.rs`](../crates/tollgate-client/src/period_roller.rs) |

All of them are **per instance** and **since process start**: nothing survives
a restart, and a fleet view is the scraper's job to aggregate. Counters are
cumulative, so read them as rates between two scrapes. Gauges (marked below)
are the current value. None of these reads is consulted by an admission
decision.

Units: *requests*, *events*, *leases* and the like are plain counts. *Cost
units* are whole `CostUnits`, never a currency; converting to money is the
application's job.

## The `pricing-api` `/metrics` document

[`examples/pricing-api`](../examples/pricing-api/src/lib.rs) publishes these
surfaces as one JSON document on `GET /metrics`. It is one way to expose them,
not a product surface: an embedder chooses its own format. The field names are
the example's and match the library names one for one, except where a table
below says otherwise.

Every labelled map (`denials`, `commit_refusals`, `input_rejections`,
`refill.refusals`, the `_by_class` maps) carries **every** label, zeros
included, in a fixed order. A zero means "has not happened", never "this
counter does not exist", and two scrapes diff cleanly.

Nested blocks are `null` when their plane is not running: `accounting`,
`refill`, `snapshots` and `contention` are absent when admission is disabled,
and `capacity` is absent when no execution-capacity gate is installed. Absent is
the honest answer for a disabled plane; zeroes would read as an exhausted one.

### Admission outcomes

From `CountersSnapshot`, plus the example's own input counters.

| Field | Kind | Unit | Counts | A non-zero or rising value means |
|---|---|---|---|---|
| `admitted` | counter | requests | Requests admitted by the engine, funded or on overage. | Traffic. The four execution outcomes below partition it exactly. |
| `units_admitted` | counter | cost units | Units *quoted* by admitted requests. | Demand, not billing: a request admitted and cancelled before execution counts here and is charged nothing. Usage events are billing truth. |
| `denied` | counter | requests | Sum of `denials`: every pre-admission refusal. | Refusals of any kind; read `denials` for which. |
| `denials.<reason>` | counter | requests | Pre-admission refusals per `DenyReason`, one label per reason. | See [Deny-reason labels](#deny-reason-labels). |
| `contexts_abandoned` | counter | requests | Requests authenticated by stage one that never reached stage two: a body that failed to read, a client that went away. | Neither an admission nor a refusal. A flood of them is load that would otherwise look like an idle instance. |
| `input_rejections.<code>` | counter | requests | The example's own extractor refusals, labelled `malformed-body`, `unsupported-media-type`, `body-too-large`, `missing-connection-state`. Not counted in `denied`. | Clients sending bodies the service cannot decode. `missing-connection-state` is a server-side wiring fault, answered 500. |
| `counter_overflow` | flag | — | `true` when a runtime aggregate (refill totals, restarts, grant counts) could not be represented exactly. | Aggregated runtime totals are saturated, not wrapped; treat them as lower bounds. |
| `managed_accounts` | gauge | accounts | Accounts whose lease manager is running or lingering. | Size of the funded working set on this instance. |
| `unfundable_accounts` | gauge | accounts | Eligible accounts that cannot currently fund a request: no active, unexpired snapshot whose lease is usable or whose overage cap has headroom. | A funding problem on this instance. Readiness depends on it. |

### Execution outcomes

What became of every admitted request. `execution_started`,
`canceled_before_start`, `capacity_shed` and `refused_at_start` partition
`admitted`, so no reader has to infer one from a difference.

| Field | Kind | Unit | Counts | A non-zero or rising value means |
|---|---|---|---|---|
| `execution_started` | counter | requests | Requests cleared to run. | Useful work. |
| `canceled_before_start` | counter | requests | Admitted requests resolved for zero before execution start: cancelled, or abandoned while pending. | Clients giving up between admission and execution; charged nothing. |
| `capacity_shed` | counter | requests | Admitted requests refused by the execution-capacity gate and released for zero. | The instance's execution capacity, not the account's quota, is the limit. |
| `execution_started_by_class.<class>`, `capacity_shed_by_class.<class>` | counter | requests | The two totals above split by capacity class, labelled `Assured` and `BestEffort`. Each map sums to its total. | Which class is being shed. Shedding `Assured` work means the reserve is undersized. |
| `refused_at_start` | counter | requests | Sum of `commit_refusals`. | Requests admitted and then refused at execution start. |
| `commit_refusals.<label>` | counter | requests | Refusals at execution start: `funding_expired`, `overage_cap_exhausted`, `overage_cap_temporarily_exhausted`, `cancelled`. Never counted in `denied`. | `funding_expired`: the funding lease lapsed between admission and start and no overage fallback applied. The two overage labels: an elastic fallback did not fit the cap (the temporary one also counts a commit that met another commit in progress). `cancelled`: a cancellation won the race. |
| `committed_at_overage`, `units_committed_at_overage` | counter | requests, cost units | Commits that settled against overage because their funding lease lapsed after admission. Disjoint from `admitted_overage`. | Elastic credit extended because a lease *did* fund the request and then expired before work began. Rising means leases expire close to use; see lease timing. |
| `admitted_overage`, `units_admitted_overage` | counter | requests, cost units | Admissions no lease funded, under `Elastic`. Included in `admitted` / `units_admitted`, never instead of them. | Credit being extended: the leading indicator of an invoice. |

### Capacity

From the execution-capacity gate's occupancy. Pool sizes and free counts only,
never labelled by account. The free counts are live reads and therefore
estimates.

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `capacity.shared_total` | gauge | slots | Configured size of the shared pool. |
| `capacity.shared_available` | gauge | slots | Free slots in the shared pool now. At zero, work that needs the shared pool is shed. |
| `capacity.reserve_total` | gauge | slots | Configured reserve. Zero under `Uniform`, which has no reserve rather than an empty one. |
| `capacity.reserve_available` | gauge | slots | Free reserve slots now. |

### Funding estimates

From `RuntimeHandle::funding(now)` (`RuntimeFundingReport`). Diagnostic
estimates read off the request path.

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `total_lease_remaining` | gauge | cost units | Units left on every installed instance lease; `null` when none exist. Falling toward zero with `denials.lease_exhausted` rising is a refill that cannot keep up. |
| `total_overage_spent` | counter | cost units | Lifetime overage spent across retained account slots. |
| `total_overage_cap` | gauge | cost units | Sum of currently eligible accounts' largest published elastic caps; `null` when no account contributes. Neither this nor `total_overage_spent` is a fleet limit: each cap applies per instance. |
| `earliest_lease_usable_until` | gauge | timestamp | The earliest usability deadline (`expires_at - safety_margin`, the admission boundary) among eligible accounts' installed leases. A value in the past means some account is running on no usable lease. |

### Contention

From `RuntimeReport::contention` (`ContentionReport`). Admission exchanges —
lease and overage debits and concurrency-gauge acquisitions — that lost a race
to another core. Cumulative lower bounds; compare two scrapes.

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `contention.contended_exchanges` | counter | exchanges | Lost exchanges across every retained account. |
| `contention.hottest[]` | list | — | At most eight `{account, contended_exchanges}` entries, most contended first, accounts with a zero count omitted. An account that climbs here is written from several cores at once, the condition [instance-local sharding](LOCAL_SHARDING.md) exists for. |

### `accounting`

From `WriterHealth`, the usage writer's live health.

| Field | Kind | Unit | Counts | A non-zero or rising value means |
|---|---|---|---|---|
| `accepted` | counter | events | Events the sink recorded. | Normal billing flow. |
| `duplicate` | counter | events | Events whose request id the sink had already recorded. | Idempotent replay after a lost acknowledgement, not loss. |
| `rejected` | counter | events | Events the sink refused: unknown lease, lease-capability mismatch, or no remaining lease capacity. | Bounded billing loss that has already happened. Normal is zero. Readiness drops while it is non-zero. |
| `lost` | counter | events | Events a final flush could not deliver. | Committed charges that never reached the ledger. It moves only at shutdown, because the running writer retries forever; it is a confirmation, not a warning. |
| `unaccounted` | gauge | events | Charges queued with no billing outcome yet. | Work in flight to the sink. Growing with `ingest_age_seconds` means the sink is not answering. |
| `shed` | counter | requests | Requests refused for want of queue capacity. `pricing-api` also counts each refused reservation under `denials.accounting_backpressure`. | The sink is behind and admission is protecting the queue. Charged zero. |
| `queue_depth` | gauge | slots | Slots held by queued events and outstanding permits. | Backpressure before it sheds. |
| `queue_capacity` | gauge | slots | The shed point: `queue_depth` reaching it denies the next request. | Fixed by configuration. |
| `last_ingest_at` | gauge | timestamp | When the sink last answered; `null` if it never has. | — |
| `ingest_age_seconds` | gauge | seconds (whole) | Time since the sink last answered; `null` if it never has. | The runtime alarm for an unreachable sink. It separates "no traffic" from "the sink has been down for twenty minutes". |

The library's `WriterStats` (inside `WriterHealth::stats`) carries four more
fields the example does not publish:

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `unattributed` | counter | events | Confirmed newly accepted events without credential attribution. See [attribution coverage](CREDENTIAL_ACTIVITY.md#attribution-coverage). |
| `attribution_unreported_batches` | counter | batches | Acknowledged batches whose sink did not report attribution support. Non-zero means activity data from this sink is incomplete, not that keys were unused. |
| `unresolved` | counter | permits | Permits that neither sent nor dropped before the drain deadline. Their charges are committed locally but unbilled; TTL reclaim bounds them. Moves only at shutdown. |
| `counter_overflow` | flag | — | At least one cumulative outcome exceeded `u64` and is saturated. |

### `refill`

From `LeaseStats`, summed over every account's lease manager. `null` when the
runtime's aggregate overflowed, in which case `counter_overflow` is `true`.

| Field | Kind | Unit | Counts | A non-zero or rising value means |
|---|---|---|---|---|
| `acquired` | counter | leases | Acquires that returned a grant (consolidations included). | Refill activity. |
| `acquired_units` | counter | cost units | Units granted across those acquires. Adaptive allocation can grant less than the target. | Funding pulled from the control plane. |
| `acquire_timeouts` | counter | calls | Acquires the allocator did not answer within `store_call_timeout`. | A slow allocator. Not a refusal: the grant may have been made and not reported. |
| `refused` | counter | calls | Sum of `refusals`. | Refill refusals of any kind. |
| `refusals.<label>` | counter | calls | Acquire refusals per `AllocateError`: `unknown_account`, `account_inactive`, `insufficient_balance`, `invalid_ttl`, `unknown_lease`, `fenced`, `lease_not_active`, `invalid_release`, `storage`, `balance_exhausted`, `balance_insufficient`. | Which refill problem an instance has. `insufficient_balance` (and the attested `balance_*` pair) is an account that is genuinely out of funds; `storage` is a control plane this instance cannot reach. The HTTP forms of these are in [Errors](ERRORS.md). |
| `released` | counter | leases | Leases the allocator no longer holds open for this instance. | Normal rotation. |
| `abandoned` | counter | leases | Leases a shutdown could not return within its budget. | Units stranded until TTL reclaim. Moves only at shutdown; rising across restarts means the shutdown release deadline is too tight. |

The library's `LeaseStats` carries three more fields the example does not
publish:

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `uncertain_acquires` | counter | calls | Acquires or consolidations that timed out or returned `storage`; each may have committed a grant nobody heard about. |
| `consolidated` | counter | rotations | Rotations that folded a refused lease's unspent units into its replacement. Each one records that the instance refused work the account could fund; a rising rate means `target_grant` is undersized against the largest quote. |
| `consolidations_deferred` | counter | rotations | Consolidations postponed because the refused lease still had a reservation in flight. Rising against a flat `consolidated` means requests never leave the lease idle long enough. |

### `snapshots`

From `SnapshotStats`. The [snapshot operations guide](SNAPSHOT_OPERATIONS.md)
covers the refusal fields and recovery procedure.

| Field | Kind | Unit | Counts | A non-zero or rising value means |
|---|---|---|---|---|
| `refresh_attempts` | counter | fetches | Fetches attempted, one per principal per pass. | A flat rate means no fetches; check task health before inferring an outage. |
| `refresh_failures` | counter | fetches | Fetches the source could not answer. | Known principals are going stale; `unresolved` shows it once their resolution lapses. |
| `refresh_timeouts` | counter | fetches | Fetches abandoned at `fetch_timeout`. | Source latency, not a catalogue problem. |
| `discovery_failures` | counter | enumerations | Principal enumerations the source could not answer. | The tracked set is frozen: new principals never appear, while everything known keeps working. |
| `refused_updates` | counter | updates | Pushes or fetched updates refused by generation ordering, excluding an unchanged positive at the installed generation. | A stale or revoked update was offered. |
| `history_evictions` | counter | histories | Histories reclaimed under snapshot capacity pressure. | Snapshot capacity is too small; each eviction needs a fresh authoritative read to recover. |
| `publication_failures` | counter | publications | Reservations or publications refused by history retention or a superseded source read. | Distinct from source failures and generation refusals. |
| `unresolved` | gauge | principals | Principals with no currently valid resolution at the last pass. | Non-zero with `denials.unknown_principal` rising means distribution, not credentials. It reports the last pass, not task liveness. |

## Deny-reason labels

`denials` has one label per `DenyReason`, in slot order. The label is
`DenyReason::name()`; the meaning, retry advice and the example's HTTP status
for each are in [Errors](ERRORS.md#deny-reasons).

`unknown_principal`, `account_suspended`, `account_closed`,
`snapshot_expired`, `missing_permission`, `request_too_large`,
`unpriced_operation`, `rate_limited`, `request_rate_limited`,
`concurrency_limited`, `unpriceable_under_limits`, `lease_unavailable`,
`lease_expired`, `lease_exhausted`, `overage_cap_exhausted`, `cost_overflow`,
`accounting_backpressure`, `overage_cap_temporarily_exhausted`,
`overage_commit_in_progress`, `empty_workload`, `funding_expired_at_start`,
`capacity_unavailable`, `balance_exhausted`, `balance_insufficient`.

The labels most worth watching, and what their rise means, are in
[Signals worth a response](DESIGN.md#signals-worth-a-response):
`lease_exhausted` with balance left is a refill problem, not enforcement;
`unknown_principal` is either rotation or distribution, told apart by
`snapshots.unresolved`; `snapshot_expired` is an outage failing closed;
`accounting_backpressure` is a sink falling behind.

`accounting_backpressure` is decided by the embedder before admission, so it
counts only if the service calls `AdmissionCounters::record_deny` when it
sheds; `pricing-api` does. Refusals at execution start are never counted here;
they are `commit_refusals`.

The `AdmissionCounters` tallies are relaxed atomics that wrap at `u64::MAX`
rather than saturate: a wrapped monitoring counter is not a correctness event,
and at a billion admissions a second `admitted` needs centuries to wrap. A
`snapshot()` is lock-free, so it is not a single instant; skew between fields
is microseconds.

## Runtime report fields not in `/metrics`

`RuntimeHandle` exposes more than the example publishes. These are library
fields an embedder can export.

### `RuntimeReport`

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `retained_accounts` | gauge | accounts | Account slots the registry retains, including ones no longer managed. Slots keep irreversible overage spend for the process lifetime. |
| `managed_accounts` | gauge | accounts | Accounts in the `Running` or `Lingering` phase. |
| `lingering_accounts` | gauge | accounts | Managed accounts in `Lingering`. |
| `retiring_accounts` | gauge | accounts | Accounts whose manager is retiring. |
| `restarting_accounts` | gauge | accounts | Accounts in `Backoff`: a lease manager died and is waiting to be restarted. |
| `manager_restarts` | counter | restarts | Lease-manager restarts across all accounts. |
| `unrecovered_grants` | counter | grants | Known grants lost with a dead task, excluding its recoverable current slot. Units that come back only at TTL reclaim. |
| `uncertain_acquires` | counter | calls | Acquires whose grant outcome is unknown, including interrupted calls. Not confirmed grants or units. |
| `counter_overflow` | flag | — | An aggregate saturated. |
| `refill` | — | — | `LeaseStats` summed over accounts; see [`refill`](#refill). |
| `snapshots` | — | — | `SnapshotStats`; see [`snapshots`](#snapshots). |
| `accounting` | — | — | `WriterHealth`; see [`accounting`](#accounting). |
| `sharding` | — | — | `ShardOccupancy`: `shards` and `affinities_assigned`, with `is_crowded()` and `crowded_shards()`. See [Reading the report](LOCAL_SHARDING.md#reading-the-report). |
| `contention` | — | — | See [Contention](#contention). |

`account_reports(now)` returns the same data per account (`phase`, `eligible`,
`fundable`, `task_healthy`, `restarts`, `unrecovered_grants`,
`uncertain_acquires`, `refill`). It is deliberately separate from metric
labels: labelling a metric by account is unbounded cardinality.

### `RuntimeReadiness`

What `readiness(now)` decides and why. `is_ready()` is the probe answer.

| Field | Kind | Meaning |
|---|---|---|
| `stopping` | flag | Shutdown has been requested; readiness is withdrawn. |
| `snapshots_ready` | flag | The snapshot task is alive and the resolution rule holds: every tracked principal resolved under `Fixed`, some resolved (or none tracked) under `All`. |
| `background_healthy` | flag | No background task failed or stopped, every eligible account is managed, and none is `Faulted`. |
| `accounting_healthy` | flag | The recorder is open, `lost` and `rejected` are zero, and the queue is below capacity. |
| `eligible_accounts` | gauge (accounts) | Accounts eligible now. |
| `unfundable_accounts` | gauge (accounts) | Eligible accounts that cannot fund a request now. |
| `unmanaged_accounts` | gauge (accounts) | Eligible accounts with no healthy running or lingering manager. |
| `unresolved_principals` | gauge (principals) | Tracked principals with no valid resolution now. |

`/readyz` false while every task is alive is not a crash; see
[Signals worth a response](DESIGN.md#signals-worth-a-response).

## Credential feed

`KeyManagerMonitor::report(now)` returns a `KeyManagerReport`. `pricing-api`
reads its `ready` bit in `/readyz`. Configuration and outage behavior are in
[Freshness, rotation and outages](CREDENTIAL_PROJECTION.md#freshness-rotation-and-outages).

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `health` | state | — | `Starting`, `Healthy`, `Degraded` (the last refresh failed; the previous table and its original deadline stay), `Stopped`, or `Failed` (the task exited without being asked). |
| `ready` | flag | — | The task is running and the installed projection is still usable at `now`. An authoritative empty set is still ready. |
| `projected_keys` | gauge | records | Entries in the installed projection, not a count of usable customers. |
| `revision` | gauge | — | Source revision of the installed projection. |
| `fetched_at`, `usable_until` | gauge | timestamp | When the installed projection was fetched and when its authority ends. Once `usable_until` passes without a refresh, authentication fails closed. |
| `stats.attempts` | counter | passes | Refresh passes started. |
| `stats.refreshes` | counter | passes | Passes that published a projection. |
| `stats.failures` | counter | passes | Passes that published nothing. Each one moves `health` to `Degraded`. |
| `stats.timeouts` | counter | calls | Page calls abandoned at `fetch_timeout`. |
| `stats.pass_timeouts` | counter | passes | Passes abandoned at `pass_timeout`. |
| `stats.pages` | counter | pages | Pages received and validated. |
| `stats.revision_conflicts` | counter | restarts | Passes restarted because the source revision changed between pages. Rising means the catalogue changes faster than a pass completes. |
| `stats.page_budget_exceeded` | counter | passes | Passes that hit `max_pages`. Rising means the catalogue has outgrown the page budget. |
| `stats.counter_overflow` | flag | — | A stats counter could not be incremented. |

## Budget rollover

A direct-store service that applies budget schedules runs a `PeriodRoller`.
`PeriodRollerMonitor::report()` returns a `PeriodRollerReport`. The counters
are confirmed observations, not a second ledger: an unanswered call may have
committed more than they report.

| Field | Kind | Unit | Meaning |
|---|---|---|---|
| `health` | state | — | `Starting`, `Healthy` (the last pass drained to a partial batch), `Degraded`, `Stopped`, or `Failed` (the task exited without being asked). |
| `last_successful_cutoff` | gauge | timestamp | Cutoff of the last completed pass. Falling behind wall time means periods are crossed late. |
| `counter_overflow` | flag | — | A total could not be represented; totals are incomplete, never wrapped. |
| `stats.passes_started`, `passes_completed`, `passes_incomplete` | counter | passes | Passes begun, drained to completion, and interrupted. |
| `stats.batches` | counter | batches | Rollover batches committed. |
| `stats.accounts_rolled` | counter | accounts | Account period crossings confirmed. |
| `stats.deposited_units` | counter | cost units (`u128`) | Allowance deposited by those crossings. |
| `stats.expired_units` | counter | cost units (`u128`) | Unspent allowance expired by those crossings. |
| `stats.failures` | counter | calls | Store calls that failed. Confirmed batches stay committed; affected accounts keep last period's allowance until a later pass succeeds. |
| `stats.call_timeouts`, `stats.pass_timeouts` | counter | calls | Calls abandoned at the per-call or per-pass deadline. |
| `stats.uncertain_calls` | counter | calls | Calls with unknown effects: store errors, timeouts, interrupted calls. |

## Control-plane server

`tollgate-server` exports no counters. It reports through `tracing` events
(maintenance sweep outcomes with `consecutive_failures`, audit events, and
failure diagnostics carrying an `error_id`) and through `/readyz`. See
[Control-plane security](CONTROL_PLANE_SECURITY.md) for audit collection and
the [HTTP API](HTTP_API.md) for the probes.
