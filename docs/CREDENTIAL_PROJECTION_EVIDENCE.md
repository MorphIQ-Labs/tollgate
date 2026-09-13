# Credential projection evidence (#108)

Measured on 2026-09-09 on `mistral`, Apple M1 Pro, Darwin 25.6.0, Rust 1.97.1.
Crates use the ordinary Criterion release profile; loopback load uses
`production` (fat LTO, panic abort). These measurements accompany the owning
invariants and tests; they do not prove cryptography, scheduler behavior or
SQL-to-Rust refinement.

## Request and session cost

`./scripts/check_perf_thresholds.sh` passed all 46 benchmark rows. The new
same-run ratios passed: cached managed/direct **0.992**, uncached
managed/direct **1.011**, each bounded at 1.25. Historical baseline enforcement
was not enabled (`active_host` unset); the existing gate reported some unstable
unrelated rows as inconclusive. No existing threshold was relaxed. New baseline
rows record this run on the same host as the baseline header.

| Managed credential operation | Mean |
| --- | ---: |
| Warm finite cached proof | 36.98 ns |
| Direct verifier, same finite cached proof | 37.26 ns |
| Uncached managed verification | 802.43 ns |
| Uncached direct verification | 793.49 ns |
| Cold session authentication | 1,022.32 ns |
| Expired proof renewed from a fresh table | 1,088.38 ns |
| Expired session refused | 988.27 ns |

The existing indefinite-cache fixture measured 16.50 ns. Finite proof expiry
activates the existing timestamp comparison, so its cost must be stated rather
than claiming that every warm credential remains at the indefinite-cache cost.
A cold managed verification adds one projection load; warm hits do not consult
the registry. Expired proofs are checked using caller-supplied time.

`./scripts/check_allocations.sh` passed, including required report scopes
`auth/projected_cache_hit` and `auth/projected_expiry`: both allocate zero.
Renewal creates new cached evidence and is measured separately from warm hits.

The production loopback load gate passed:

| Paired workload | Admission/baseline p50 | Required maximum |
| --- | ---: | ---: |
| Sequential | 1.022 | 1.150 |
| Ten connections, same account | 1.029 | 1.200 |
| Ten distinct accounts | 1.006 | Informational |

Admitted throughput was 29,344 requests/s sequentially and 104,924 requests/s
with ten connections. The example imports its documented `demo-key-N` tokens
into durable MemoryStore records, then authenticates through KeyManager. Its
API test additionally retires a stored key, refreshes, and checks that new
verification refuses it with no extra usage charge. Account snapshot checks
continue on every request, independently of cached credential identity.

## Page and publication scaling

Reproduce with:

```sh
cargo bench -p tollgate-client --bench key_projection_scaling -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 30
```

The page fixture holds 256 active keys and returns 64 per read. Retired history
is deliberately before the cursor's first active key.

| Retired keys retained in memory | Page mean |
| ---: | ---: |
| 0 | 4.310 µs |
| 1,000 | 4.312 µs |
| 100,000 | 4.354 µs |

| Active keys | Initial complete manager pass, including startup/shutdown |
| ---: | ---: |
| 256 | 67.03 µs |
| 4,096 | 871.28 µs |
| 16,384 | 3.860 ms |

The ordered unrevoked-key index removes retired-history scans. Expired but
unrevoked rows remain candidates, and the whole installed table still costs
O(active keys) memory. The per-pass deadline and total page-call budget include
revision-conflict restarts and candidate construction.

PostgreSQL 16 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` used 100,000 retired rows
and 256 active rows, with `LIMIT 65` (64 records plus lookahead). Both the first
page and a cursor in the live set used `tollgate_credential_keys_projection`:
65 rows, zero rows removed by the expiry filter, four shared blocks, no sort.
Execution times were 0.038 ms and 0.027 ms. The fixture was inserted in a
transaction and rolled back; plans are in the generated
`reports/credential_pg_plans.json` artifact. Reproduce against the local fixture:

```sh
docker compose exec -T postgres psql -U tollgate -d tollgate \
  -v ON_ERROR_STOP=1 -f - < testing/credential_projection_plans.sql
```

These are query-plan observations, not a latency promise for arbitrary expiry
distributions or remote databases.

Those PostgreSQL observations used the pre-0018 expiry schema. The runnable
fixture now uses exact timestamp pairs; #118 does not claim a fresh timing
measurement or reuse these historical query timings as validation of that
schema. Its correctness evidence covers both page predicates and the complete
HTTP/session boundary without changing the measured request-path code.

## Reproducible assurance

The invariant witnesses live in INVARIANTS.md 27 and 34. Core commands:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
TOLLGATE_REQUIRE_PG=1 TOLLGATE_PG_URL=<local-fixture-url> \
  cargo test --workspace --all-features
./scripts/check_formal.sh
TOLLGATE_PG_URL=<local-fixture-url> ./scripts/check_mutations.sh --diff main
./scripts/check_allocations.sh
./scripts/check_perf_thresholds.sh
./scripts/check_load_thresholds.sh
./scripts/check_advisories.sh
./scripts/check_ci_rules.sh
```

The Lean model proves exact fixed-catalogue drains, no omissions or duplicates,
whole replacement, failed/mixed-revision preservation and finite expiry bounds.
Backend tests cover revisions from the same committed read, legacy writes,
rollback, no-op idempotency and overflow. HTTP tests cover the role boundary,
server-selected time, strict query and digest decoding, complete response
semantics, and exact body bounds with and without Content-Length.
