# Contributing to Tollgate

Tollgate provides quota admission and usage accounting for latency-critical services: fenced quota leases spent through local atomic counters, immutable account snapshots driving admission, and idempotent batched usage events driving billing.

## The engineering contract

**[`AGENTS.md`](AGENTS.md) is the working contract for this repository** — the request-path/control-plane split, design constraints, invariants and trust boundaries, assurance requirements, coding style, merge-request and release rules, and the definition of done. Read it before your first change. It applies to human and agent contributors alike; `CLAUDE.md` is a one-line include of it so agent tooling reads the same file.

`INVARIANTS.md` is the testable contract and `docs/DESIGN.md` records architecture and rationale. This page covers only setup and process.

## Setup

```sh
git config core.hooksPath .githooks
docker compose up -d          # PostgreSQL for the store suite
```

## Running the gates locally

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate \
  cargo test -p tollgate-store-postgres
```

Run timed performance checks locally and include their reports in performance-sensitive merge requests and release validation. Remote CI compiles benchmarks and enforces deterministic allocation counts; it does not execute Criterion or load measurements. Absolute thresholds and recorded baselines require their calibrated host, and even ratios need comparable measurement conditions. See [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) for commands, provenance and review requirements. `AGENTS.md` has the full command inventory.

A backend behavior change must update both the memory and PostgreSQL implementations and their mirrored scenario tests.

## Proposing a change

1. Find or open an issue stating the objective, scope, and acceptance criteria.
2. Branch as `<type>/<slug>` using the conventional-commit vocabulary (`feat/`, `fix/`, `docs/`, `chore/`, …).
3. Open a merge request whose **title is a conventional commit**, optionally scoped (`feat(admission): install_many bulk write`) — CI enforces this, and the squash subject becomes the single commit on `main`. A release is a merge request that bumps the workspace version and writes the `CHANGELOG.md` section; the `tag-release` job cuts the tag when it lands.
4. In the description, describe behavioral impact, list the validation performed, and call out invariant, migration, API, or threshold changes, with benchmark evidence for performance-sensitive work. Close the issues it resolves with `Closes #N`.
5. Resolve every discussion; `main` accepts no direct pushes.

## Issue references

References written `GL-N`, in code, documentation, and commit messages, and
`!N` merge-request references, point to the project's former GitLab tracker,
which is not public. The reasoning each one records is in the surrounding
text or in [`docs/DESIGN.md`](docs/DESIGN.md). Plain `#N` references are
issues in this repository.

## Reporting a vulnerability

Do not open a public issue. Contact the maintainers privately.
