# Contributing to Tollgate

Tollgate provides quota admission and usage accounting for latency-critical services: fenced quota leases spent through local atomic counters, immutable account snapshots driving admission, and idempotent batched usage events driving billing.

## The engineering contract

**[`AGENTS.md`](AGENTS.md) is the working contract for this repository** — the request-path/control-plane split, design constraints, invariants and trust boundaries, assurance requirements, coding style, pull-request and release rules, and the definition of done. Read it before your first change. It applies to human and agent contributors alike; `CLAUDE.md` is a one-line include of it so agent tooling reads the same file.

`INVARIANTS.md` is the testable contract and `docs/DESIGN.md` records architecture and rationale. This page covers only setup and process.

## Documentation

The [documentation site](https://morphiq-labs.github.io/tollgate/) is built from `docs/` with [mdBook](https://rust-lang.github.io/mdBook/); `docs/SUMMARY.md` is its table of contents. Write documents as ordinary repository Markdown: links to files outside `docs/` work on GitHub and are rewritten for the site. `mdbook build` fails on a link to a path or heading that does not exist, and on a document under `docs/` that `SUMMARY.md` does not list, so a new document goes into `SUMMARY.md` in the same change. A file outside `docs/` can appear as a page through a stub whose first line is `<!-- repo-page: PATH -->` (see `docs/site/`). A tutorial that quotes a program declares it with `<!-- excerpts-of: PATH -->`; every Rust block in that document must then be a verbatim excerpt of the file, which the ordinary test suite compiles and runs (see `docs/GETTING_STARTED.md`). Build locally with `cargo install --locked mdbook --version "$(cat .cargo/mdbook-version)"` and then `mdbook serve --open`.

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

./scripts/check_advisories.sh              # RustSec, yanked and informational advisories
./scripts/check_formal.sh                  # the Lean proofs
./scripts/check_formal_mutants.sh          # mutation testing of the Lean models
./scripts/check_perf_thresholds.sh         # hot-path microbenchmark gate
./scripts/check_load_thresholds.sh         # local ratios and controlled-host absolutes

# Mutation testing: what a branch changed, or one crate's whole surface.
TOLLGATE_PG_URL=postgres://tollgate:tollgate@127.0.0.1:5433/tollgate \
  ./scripts/check_mutations.sh --diff main
./scripts/check_mutations.sh --package tollgate-core
```

The workspace's minimum supported Rust version is 1.89, and every pull request checks the locked workspace, with all features and targets, on Rust 1.89.0. `rust-toolchain.toml` separately pins a newer toolchain for development and the primary CI jobs, so formatting, linting and release tooling are reproducible; that pin does not replace the MSRV. A change to the declared minimum and the `msrv` job land together.

Run timed performance checks locally and include their reports in performance-sensitive pull requests and release validation. Remote CI compiles benchmarks and enforces deterministic allocation counts; it does not execute Criterion or load measurements. Absolute thresholds and recorded baselines require their calibrated host, and even ratios need comparable measurement conditions. See [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) for commands, provenance and review requirements. `AGENTS.md` has the full command inventory.

A backend behavior change must update both the memory and PostgreSQL implementations and their mirrored scenario tests.

## Proposing a change

1. Find or open an issue stating the objective, scope, and acceptance criteria.
2. Branch as `<type>/<slug>` using the conventional-commit vocabulary (`feat/`, `fix/`, `docs/`, `chore/`, …).
3. Open a pull request whose **title is a conventional commit**, optionally scoped (`feat(admission): install_many bulk write`) — CI enforces this, and the squash subject becomes the single commit on `main`. A release is a pull request that bumps the workspace version and writes the `CHANGELOG.md` section; the `tag-release` job cuts the tag when it lands.
4. In the description, describe behavioral impact, list the validation performed, and call out invariant, migration, API, or threshold changes, with benchmark evidence for performance-sensitive work. Close the issues it resolves with `Closes #N`.
5. Resolve every conversation; `main` accepts no direct pushes.

## Issue references

References written `GL-N`, in code, documentation, and commit messages, and
`!N` merge-request references, point to the project's former GitLab tracker,
which is not public. The reasoning each one records is in the surrounding
text or in [`docs/DESIGN.md`](docs/DESIGN.md). Plain `#N` references are
issues in this repository.

## Reporting a vulnerability

Do not open a public issue. See [`SECURITY.md`](SECURITY.md).
