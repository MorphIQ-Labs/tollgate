# Changelog

All notable Tollgate changes are recorded here by release-plz from conventional
merge request titles.

## [Unreleased]

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
