# Changelog

All notable Tollgate changes are recorded here by release-plz from conventional
merge request titles.

## [Unreleased]

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
