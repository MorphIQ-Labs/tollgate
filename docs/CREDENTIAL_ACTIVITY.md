# Credential activity

`KeyDirectory::credential_activity(&[KeyId])` reports the latest recorded
attributable commitment for each requested key. `Committed { last_committed_at }`
is the maximum execution-start timestamp from accepted usage events, normalized
to PostgreSQL's microsecond precision. It is an instance-supplied timestamp,
not independent evidence from the server's clock.

The other states distinguish an unknown credential from a known credential
with no recorded attributable commitment. Neither absence nor an old timestamp
establishes that revocation is harmless. Requests denied or cancelled before
execution are absent; so are committed events that never reached the ledger,
were refused there, or lacked valid attribution. A running execution can still
own an event that has not been emitted. This API is not an authentication audit.

## Publishing and reading

A key-scoped publisher sets `AccountSnapshotBuilder::key_id` to the issued key's
ID. Both stores verify the binding against the publishing Principal and snapshot
AccountId before replacing state or pushing it. Unknown or mismatched bindings
return `PublishSnapshotError::CredentialMismatch`; the existing HTTP publication
endpoint returns 422 `invalid-credential-binding`. MemoryStore's inherent helper
and AdminStore implementation share credential, status and capacity-class checks.
Custom snapshot sources/direct map installers remain trusted publishers.

The pinned snapshot supplies the key ID to the committed UsageEvent. There is
no verifier lookup, clock read or activity write in authentication, and no new
background task. Existing UsageWriter batching and transport deliver the event.
Ingest checks the key's AccountId independently. Unknown and different-account
keys leave the bill intact and supply no activity. Retired and expired keys
retain their identity and can receive delayed committed events.

The read is an operator-side direct-store capability on KeyDirectory; there is
no activity HTTP endpoint and HttpStore gains no lifecycle authority. Input
order and repeated IDs are preserved. Output memory grows with the explicitly
requested list. PostgreSQL bounds each query internally and fails the whole
read on error; successive chunks need not describe one instant.

## Attribution coverage

Successful ingest replies partition each submitted event into accepted,
duplicate or rejected. Only newly accepted events supply activity evidence.
Replays retain the first event's identity even when the submitted key ID or
timestamp differs. Older/equal timestamps are attributable monotonic no-ops.

`IngestReport.unattributed` counts newly accepted events that lack an existing,
same-account credential. Its range is zero through `accepted`. `Some(0)` is a
confirmed zero; `None` (JSON null or a missing field from an older server) means
attribution reporting is unavailable. Duplicate and rejected events do not
contribute to this count. Counts measure events before grouping by credential.

UsageWriter exposes confirmed unattributed counts, the number of replies lacking
attribution support, and a sticky counter-overflow flag in live/shutdown stats.
Coverage gaps and recovery emit structured events. Saturation never wraps totals.
These are acknowledged-outcome counters: a lost reply followed by a duplicate
reply cannot recover the original accepted/unattributed count. They do not prove
complete historical coverage. Existing lost/unresolved usage diagnostics remain
relevant to activity too.

Both custom and HTTP sink replies must account for the complete batch before
the writer releases evidence. Malformed counts, partial HTTP success and oversized
acknowledgements remain uncertain/retryable failures. The HTTP success contract is
200 without Content-Range. Domain attribution mistakes do not reject billing;
storage failures still roll back the complete ingest transaction and retry.

## Persistence and rollout

The activity-only compatibility discussion below applies to migration 0014.
Deployments including credential expiry migration 0018 must follow the
[coordinated source and session upgrade](CREDENTIAL_PROJECTION.md#expiry-precision-and-upgrade);
its expiry-column fence supersedes the earlier additive-schema rollout.

Migration 0014 adds a nullable `key_id` to usage rows and a separate
`tollgate_credential_activity` table. The usage column deliberately has no key FK:
an unknown attribution must not lose a bill. The aggregate references retained
credential rows and advances only to a newer accepted timestamp in the same
transaction as billing. Replays are not historical backfill.

Neither write touches the credential table or 0013's revision triggers. Account
snapshots, key-feed payloads and the conservation equation gain no activity term.
Activity is never an authorization input. Deterministic key ordering bounds
transaction lock ordering; conditional updates avoid rewriting equal/older maxima.

Prepare migration-aware PostgreSQL binaries and a tested rollback build before
applying 0014. `PostgresStore::connect` runs SQLx's migration validation: a
released v0.17 binary knows only through 0013 and refuses a cold start after
0014 is recorded. Already-running old processes can continue using the additive
schema, but prevent old PostgreSQL embedders from restarting or scaling out
during that transition. New binaries apply pending migrations before serving.

For rollback, retain the column/table, migration history and a rollback binary
whose embedded catalogue includes the unchanged 0014 migration. Validate that
binary's older business logic against the upgraded schema before deployment.
An unmodified v0.17 binary is not a cold-rollback artifact. Never delete SQLx
history or disable its unknown-migration guard to force an old binary to start.
Any later schema removal needs a forward migration after writers stop. These
startup constraints qualify the additive-schema notes in the migration header.

Update servers before enabling producers and key-scoped publishers. Existing
writers omit key_id; new servers report their accepted events
as unattributed. Old servers accept new payloads while ignoring the additive
field; new clients report unavailable attribution support. Validate any existing
key annotations against their directory bindings before upgrading publication.
Pre-upgrade events with unknown identity stay unknown.

The Rust API changes are intentional in unpublished crates: UsageEvent::new,
Reservation::usage_event, KeyDirectory, publication errors/the inherent helper's
result, IngestReport and writer statistics. Wire additions default safely without
pretending unknown coverage is zero. The widest event is 410 bytes, including a
present key ID, expanded negative year and fractional timestamp; a full 4,096-event
batch fits the existing 2 MiB request ceiling. Serialization tests pin both bounds.

## Assurance

Invariant 35 names the owning operations and test witnesses. The shared
credential_activity scenarios run against both backends; PostgreSQL additionally
tests restart, rollback, source metadata and conflicting request IDs. Loopback
tests exercise actual authentication, commitment and activity over HTTP, TLS bearer
and mTLS. The pricing example issues and publishes matching key IDs.

`formal/lean/Tollgate/CredentialActivity.lean` proves exact-model max laws,
first-event replay identity, report partitioning, failed transaction preservation
and revision isolation. Backend execution, timestamp representation, authentication
and delivery are implementation assumptions, checked separately by Rust tests and
mutation/performance gates. The new commit benchmarks construct and emit events;
the existing reserve/commit CAS benchmarks alone do not exercise that work.
The older-catalogue restart witness pins SQLx's unknown-migration refusal and
confirms that reconnecting with the current catalogue preserves activity history.

The paired measurements in
[`testing/credential_activity_evidence.json`](../testing/credential_activity_evidence.json)
record the baseline revision, host, profiles and individual results. UsageEvent
grows from 128 to 160 bytes on that host, adding 32 bytes per queued event.
The twelve event-emission cases range from 0.956 to 1.049 times their old means;
these are measurements, not a zero-cost claim. Six attributed/unattributed
ratios run in the ordinary performance gate with a 1.25 ceiling. PostgreSQL
measurements cover 256 distinct accounts and a single frequently updated key;
they include real advancing maxima and matching old-binary workloads.
Updating 256 distinct credentials measured 14.7 ms per batch versus 9.5 ms
before; the single-credential case measured 5.6 ms versus 5.8 ms. The extra
per-credential database writes have a throughput cost even though the warmed
owned commit path remains allocation-free. Size sink capacity using the attributed workload.
These local PostgreSQL latencies are capacity evidence, not portable thresholds.
The reproducible [query-plan fixture](../testing/credential_activity_plan.sql)
measures bounded joins against a 100,000-key catalogue in temporary tables.
