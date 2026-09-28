# Changelog

All notable Tollgate changes are recorded here. A release is prepared in an
ordinary merge request that bumps the workspace version and writes the section
below; the `tag-release` job then cuts the `v{version}` tag and the GitLab
release from that section when the merge request lands.

## [Unreleased]

### Fixed

- Deposit overflow now returns the additive `422 balance-overflow` problem
  code instead of retryable `503 storage`, in both backends (#40). Deposits
  outside PostgreSQL's unit range are refused the same way; neither funding
  counter changes. Rust callers matching `AllocateError` exhaustively must
  handle the new `BalanceOverflow` variant.

## [0.30.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.30.1...v0.30.2) - 2026-09-28

### Other

- prepare the public launch (#36)

## [0.30.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.30.0...v0.30.1) - 2026-09-27

### Fixed

- *(release)* build the release commit safely at real size (#9)
- *(release)* create the release commit through the API so it is signed (#8)

### Other

- release on GitHub: prepare, tag, publish (#6)
- make the seven library crates publishable to crates.io (#5)
- describe the GitHub process, and add a security policy (#4)
- a README for every published crate (#3)
- move the gates to GitHub Actions (#2)
- publish the design record
- prepare the public release

## [0.30.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.29.3...v0.30.0) - 2026-09-25

### Added

- *(server)* [**breaking**] #143 manifest-configured credential issuer and key-bound snapshots

## [0.29.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.29.2...v0.29.3) - 2026-09-25

### Fixed

- *(release)* #142 tag the release commit, not the release merge

## [0.29.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.29.1...v0.29.2) - 2026-09-25

### Added

- *(gates)* #141 derive per-row allowances from retained run history

## [0.29.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.29.0...v0.29.1) - 2026-09-25

### Fixed

- *(auth)* export the types CredentialIssuer names; rustdoc the whole workspace

## [0.29.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.28.1...v0.29.0) - 2026-09-25

### Added

- *(admission)* [**breaking**] #139 count contention in the overage and gauge loops

## [0.28.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.28.0...v0.28.1) - 2026-09-24

### Other

- *(admission)* #140 gate per-account line migration, keep the layout

## [0.28.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.27.0...v0.28.0) - 2026-09-24

### Added

- *(admission)* [**breaking**] #134 report per-account lease contention

### Other

- *(admission)* #138 attribute cross-account admit cost to line migration
- *(client)* #137 partition the usage queue into lanes
- *(gates)* #125 size the pinned contended rows to controlled-host envelopes
- *(admission)* #134 correct why lease sharding is opt-in
- *(admission)* [**breaking**] #132 shard outcome tallies under every layout

## [0.27.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.26.0...v0.27.0) - 2026-09-24

### Fixed

- *(allocator)* [**breaking**] forfeit unreleased leases at reclaim

## [0.26.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.25.1...v0.26.0) - 2026-09-23

### Fixed

- *(allocator)* [**breaking**] #131 grow consolidation to a proven quote

## [0.25.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.25.0...v0.25.1) - 2026-09-23

### Other

- *(admission)* #129 recalibrate after account exhaustion evidence

## [0.25.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.24.0...v0.25.0) - 2026-09-23

### Added

- *(admission)* [**breaking**] confirm when remaining funding cannot cover a quote

## [0.24.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.23.3...v0.24.0) - 2026-09-23

### Fixed

- *(admission)* [**breaking**] distinguish account exhaustion from lease gaps

## [0.23.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.23.2...v0.23.3) - 2026-09-22

### Added

- *(core)* report shard occupancy and preserve request affinities

### Fixed

- *(docs)* declare the published seam, and give its rule a backstop

## [0.23.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.23.1...v0.23.2) - 2026-09-22

### Other

- add the embedder integration guide for the request-path seam

## [0.23.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.23.0...v0.23.1) - 2026-09-22

### Fixed

- *(test)* make the sharded benchmark and audit-capture fixtures deterministic

## [0.23.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.5...v0.23.0) - 2026-09-15

### Fixed

- *(admin)* [**breaking**] preserve mutation intent and audit predecessors

## [0.22.5](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.4...v0.22.5) - 2026-09-15

### Added

- *(server)* administer accounts, budgets, and credentials over HTTP

## [0.22.4](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.3...v0.22.4) - 2026-09-15

### Other

- *(ci)* #114 #119 pin the benchmark profile and record the baseline whole

## [0.22.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.2...v0.22.3) - 2026-09-14

### Other

- *(store)* make the backends' mirrored scenarios drive the same contract
- *(lint)* deny nondeterministic APIs, and fix the two outputs that depended on a hash seed

## [0.22.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.1...v0.22.2) - 2026-09-14

### Other

- *(admission)* record SnapshotMap's forward-or-inherit rule on the trait

## [0.22.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.22.0...v0.22.1) - 2026-09-14

### Other

- *(store)* one delegating test double, and the trait-default rule it encodes

## [0.22.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.21.3...v0.22.0) - 2026-09-14

### Fixed

- *(store)* [**breaking**] order the expiry sweep by the index it already had

### Other

- *(deps)* centralize shared declarations and make allow reasons mechanical
- *(design)* correct the at-capacity map-gap claim
- *(admission)* move the lease handle into the reservation

## [0.21.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.21.2...v0.21.3) - 2026-09-14

### Fixed

- *(admission)* state the request path's real lock budget, and measure the cache full

## [0.21.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.21.1...v0.21.2) - 2026-09-14

### Fixed

- *(docs)* align rate retry contract and core validation

## [0.21.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.21.0...v0.21.1) - 2026-09-14

### Fixed

- *(load-gate)* report execution failures without panicking
- *(ci)* validate invariant witness references

## [0.21.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.20.0...v0.21.0) - 2026-09-13

### Fixed

- *(client)* [**breaking**] report snapshot refusals and clear exited task health

## [0.20.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.19.0...v0.20.0) - 2026-09-13

### Fixed

- *(store)* [**breaking**] preserve credential expiry through projections
- *(store)* [**breaking**] preserve durable lease expiry and reclaim precision

## [0.19.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.18.2...v0.19.0) - 2026-09-12

### Fixed

- *(http)* [**breaking**] preserve exact lease TTLs across transports
- *(test)* observe funding before requiring elastic lease admissions
- *(test)* reconcile unanswered grants after shutdown
- *(server)* supervise maintenance and withdraw unhealthy readiness

## [0.18.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.18.1...v0.18.2) - 2026-09-12

### Fixed

- *(server)* keep backend details out of public diagnostics
- *(store)* isolate invalid usage and enforce accounting domains

## [0.18.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.18.0...v0.18.1) - 2026-09-11

### Fixed

- *(gates)* enforce baseline provenance and restore direct quoting

## [0.18.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.17.0...v0.18.0) - 2026-09-10

### Added

- *(core/store)* [**breaking**] #105 derive credential last-committed from usage

### Changed

- Run timed Criterion and loopback load validation locally; CI retains benchmark compilation, allocation assertions, and formal/mutation assurance.

## [0.17.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.16.0...v0.17.0) - 2026-09-09

### Added

- *(server/client)* [**breaking**] #108 expose active credential digests to data-plane instances

## [0.16.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.15.0...v0.16.0) - 2026-09-09

### Added

- *(server)* [**breaking**] secure the control-plane link (#98)
- *(client)* #107 drive periodic budget rollover

## [0.15.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.7...v0.15.0) - 2026-09-08

### Added

- *(client)* [**breaking**] #95 orchestrate the multi-account admission lifecycle

### Fixed

- *(client)* [**breaking**] #109 fold a refused lease's unspent units into its replacement

## [0.14.7](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.6...v0.14.7) - 2026-09-07

### Added

- *(example)* #99 measure the reserve under mixed-class load

### Other

- *(core)* #111 decide the shard reduction once instead of per lookup
- *(ci)* #99 measure the capacity gate enabled and disabled

## [0.14.6](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.5...v0.14.6) - 2026-09-05

### Added

- *(admission)* #99 reserve execution capacity for assured work

### Fixed

- *(ci)* #112 stop the perf gate ruling on measurements it called unreadable

## [0.14.5](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.4...v0.14.5) - 2026-09-05

### Added

- *(core)* #99 make execution capacity class an account-owned fact

## [0.14.4](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.3...v0.14.4) - 2026-09-04

### Fixed

- *(ci)* #110 stop a dependency's housekeeping failing the allocation gate

## [0.14.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.2...v0.14.3) - 2026-09-04

### Added

- *(core)* #94 carry an opaque policy revision through admission

## [0.14.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.1...v0.14.2) - 2026-09-04

### Added

- *(admission)* #93 count every post-admission transition

## [0.14.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.14.0...v0.14.1) - 2026-09-04

### Added

- *(admission)* #93 share the charge state with a cancel handle
- *(core)* #93 commit a lapsed lease as overage in one transition

## [0.14.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.13.0...v0.14.0) - 2026-09-04

### Added

- *(core)* #97 instance-visible budget and remaining estimate
- *(store)* #97 periodic budgets with period-end expiry

### Fixed

- *(store)* [**breaking**] #61 bound the usage batch and stop a refusal wedging the writer

### Other

- #106 run the blocking assurance gates on every merge request

## [0.13.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.12.1...v0.13.0) - 2026-09-03

### Fixed

- *(client)* #63 bound the final flush's backoff by the drain deadline
- *(client)* #62 keep every lease on the books until released or reported
- *(ci)* #60 stop the formal gate passing over a sorry
- *(client)* [**breaking**] #59 bound principal enumeration on the wall clock
- *(store)* #58 hold one guard across publish_snapshot's check and write

## [0.12.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.12.0...v0.12.1) - 2026-09-03

### Fixed

- *(store)* #57 make MemoryStore's failing operations move nothing
- *(store)* #56 read the conservation equation at one instant

## [0.12.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.11.0...v0.12.0) - 2026-09-03

### Added

- *(store)* #104 durable credential directory with a PostgreSQL backend
- *(auth)* [**breaking**] #104 make the credential digest table a durable-backed projection

## [0.11.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.10.2...v0.11.0) - 2026-09-03

### Fixed

- *(client)* [**breaking**] #103 abandon a hung snapshot fetch so the sweep can return

## [0.10.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.10.1...v0.10.2) - 2026-09-03

### Fixed

- *(client)* #78 bound the release pass, not just its calls

## [0.10.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.10.0...v0.10.1) - 2026-09-02

### Other

- *(client)* #102 remove the stale deprecation allow in readiness_scaling

## [0.10.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.9.0...v0.10.0) - 2026-09-02

### Added

- *(core)* [**breaking**] check per-class work permissions over a heterogeneous workload

## [0.9.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.6...v0.9.0) - 2026-09-02

### Added

- *(core)* export DiscardedUsage, the reference UsageSlot

### Other

- *(admission)* [**breaking**] #102 drop the deprecated one-shot and ChargeGuard surfaces

## [0.8.6](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.5...v0.8.6) - 2026-09-02

### Added

- *(admission)* #91 add staged admission contexts
- *(admission)* #91 enforce request-rate and concurrency guards

## [0.8.5](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.4...v0.8.5) - 2026-09-01

### Added

- *(core)* #91 establish staged admission data contracts

## [0.8.4](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.3...v0.8.4) - 2026-08-28

### Other

- *(gates)* #90 establish embedding regression gates
- *(gates)* #90 add allocation and structural assertions

## [0.8.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.2...v0.8.3) - 2026-08-28

### Other

- *(design)* #96 fix the staged admission interface shape

## [0.8.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.1...v0.8.2) - 2026-08-25

### Fixed

- *(release)* sync path dependency bumps

## [0.8.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.8.0...v0.8.1) - 2026-08-25

### Fixed

- *(ci)* synchronize release preparation

## [0.8.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.7.0...v0.8.0) - 2026-08-25

### Added

- *(auth)* [**breaking**] #2 credential verification in the library, cached per session

### Fixed

- *(ci)* prepare releases after every merge

## [0.7.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.6.0...v0.7.0) - 2026-08-25

### Added

- *(core)* [**breaking**] #1 elastic enforcement mode
- *(ci)* restore the version-bump half of the release mechanism

### Fixed

- *(ci)* stage only the files prepare-release edits

## [0.6.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.5.0...v0.6.0) - 2026-08-24

### Fixed

- *(admission)* [**breaking**] #53 an absence is not a revocation. Only a
  generation the source published a revocation at may refuse that same
  generation back. A generation this instance merely observed still orders
  snapshots — a strictly older one is refused — but asserts nothing about the
  principal being dead, so the same generation arriving again is a
  re-observation. Conflating the two stranded any principal whose row went
  briefly absent: the absence inherited the positive's generation and then
  refused it back forever. This is #17's rule — keyed on what the source
  answered, never on what the instance remembers — applied to admission rather
  than to TTL selection. Invariant 15 and `formal/lean/Tollgate/SnapshotCache.lean`
  are updated to match.
- *(ci)* add `dependencies: []` so jobs do not fetch artifacts they never read.

### Documentation

- carry the Repository and Branch Settings section into the engineering contract.

## [0.5.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.4.0...v0.5.0) - 2026-08-24

### Fixed

- reject only the publish registry list, and restore the manifest guard

### Other

- Merge branch 'refactor/51-unify-account-suspension' into 'main'
- *(store)* [**breaking**] #51 unify account suspension into one status

## [0.4.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.3.1...v0.4.0) - 2026-08-24

### Other

- Merge branch 'perf/52-churned-catalogue-sweep' into 'main'
- *(client)* [**breaking**] #52 stop refetching churned principals every sweep
- release v0.3.1
- update Cargo.lock dependencies
- Merge branch 'test/27-set-active-parity' into 'main'
- *(postgres)* baseline existing mutation surface

## [0.3.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.3.0...v0.3.1) - 2026-08-24

### Other

- *(postgres)* baseline existing mutation surface

## [0.3.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.14...v0.3.0) - 2026-08-24

### Other

- *(wire)* [**breaking**] standardize 128-bit ID encoding

## [0.2.14](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.13...v0.2.14) - 2026-08-24

### Added

- *(client)* #48 discover the tracked principal set at runtime

## [0.2.13](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.12...v0.2.13) - 2026-08-24

### Other

- *(store)* define lease-scoped fencing contract

## [0.2.12](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.11...v0.2.12) - 2026-08-24

### Other

- #43 mutation-test core, admission and store
- harden dependency and load assurance gates
- *(client)* #20 serialize the usage batch without copying it

## [0.2.11](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.10...v0.2.11) - 2026-08-23

### Other

- Merge branch 'ci/17-gate-load-ratios' into 'main'
- gate load overhead ratios
- *(store)* #12 index the reconciliation query's account filter

## [0.2.10](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.9...v0.2.10) - 2026-08-23

### Other

- *(store)* #23 index active leases and report memory growth

## [0.2.9](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.8...v0.2.9) - 2026-08-23

### Other

- Merge branch 'perf/11-concurrent-load-gate' into 'main'

## [0.2.8](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.7...v0.2.8) - 2026-08-23

### Other

- *(admission)* #9 hash principals with foldhash, not SipHash

## [0.2.7](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.6...v0.2.7) - 2026-08-23

### Other

- *(pricing-api)* #16 one Option instead of five and a flag
- *(store)* decouple shared types from memory backend

## [0.2.6](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.5...v0.2.6) - 2026-08-23

### Other

- #49 let the perf gate report an untrusted run
- *(postgres)* use set-wise usage updates

## [0.2.5](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.4...v0.2.5) - 2026-08-23

### Other

- *(admission)* #8 stop rescanning the limiter registry on every install

## [0.2.4](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.3...v0.2.4) - 2026-08-23

### Other

- *(client)* #22 index the resolution deadlines instead of rescanning them

## [0.2.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.2...v0.2.3) - 2026-08-23

### Other

- *(store)* bound expired lease reclaim

## [0.2.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.1...v0.2.2) - 2026-08-23

### Added

- *(client)* #10 refill on the debit that crosses low water, not the next tick

## [0.2.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.2.0...v0.2.1) - 2026-08-23

### Added

- *(client)* #4 make the refill and snapshot planes scrapeable

### Fixed

- *(store-postgres)* await rollback before returning

## [0.2.0](https://github.com/MorphIQ-Labs/tollgate/compare/v0.1.4...v0.2.0) - 2026-08-23

### Added

- *(client)* #38 export accounting health continuously, not only at shutdown

### Fixed

- *(snapshot)* [**breaking**] validate batch quotes against burst

### Other

- *(server)* feature-gate postgres backend

## [0.1.4](https://github.com/MorphIQ-Labs/tollgate/compare/v0.1.3...v0.1.4) - 2026-08-23

### Added

- *(admission)* #37 count admissions and denials without touching the request-path budget

### Other

- *(client)* batch usage events with recv_many

## [0.1.3](https://github.com/MorphIQ-Labs/tollgate/compare/v0.1.2...v0.1.3) - 2026-08-23

### Added

- *(client)* #36 replace every silent control-plane discard with a structured event

## [0.1.2](https://github.com/MorphIQ-Labs/tollgate/compare/v0.1.1...v0.1.2) - 2026-08-23

### Fixed

- *(admission)* #40 distinguish an unadmittable schedule from throttling
- *(client)* #34 #42 bound background store calls and surface release refusals

## [0.1.1](https://github.com/MorphIQ-Labs/tollgate/compare/v0.1.0...v0.1.1) - 2026-08-23

### Fixed

- #28 examine every parked lease during release quiescence
- #32 drain outstanding usage permits during bounded shutdown
- #41 report unaccounted charges when the usage writer dies

### Other

- #44 restore a packageable canonical release baseline
- #46 remove the manifest-level registry guard that broke packaging

## [0.1.0](https://github.com/MorphIQ-Labs/tollgate/releases/tag/v0.1.0) - 2026-08-22

### Fixed

- *(admission)* #14 bound and refresh negative snapshot cache
- address follow-up review findings

### Other

- lease lifecycle: safety margin, commit expiry recheck, reclaim grace (review #1)
- Rename project: quota-service -> tollgate
