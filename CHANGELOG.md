# Changelog

All notable Tollgate changes are recorded here. A release is prepared in an
ordinary merge request that bumps the workspace version and writes the section
below; the `tag-release` job then cuts the `v{version}` tag and the GitLab
release from that section when the merge request lands.

## [Unreleased]

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
