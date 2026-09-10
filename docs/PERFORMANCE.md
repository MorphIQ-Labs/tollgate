# Local performance validation

Timed Criterion benchmarks and loopback load tests run locally. Remote CI
compiles every benchmark and enforces deterministic allocation counts, but it
does not execute timed performance jobs. A green pipeline therefore establishes
no latency or throughput verdict. Performance-sensitive changes and releases
need reviewed local evidence before merge.

## Run and retain evidence

Use a quiet host and an isolated checkout/target directory. Do not run builds,
mutation testing, load tests or another benchmark suite alongside a measurement.
Keep baseline and candidate build directories separate; shared Cargo artifacts
previously caused an invalid comparison. Run the full scripts for acceptance:
filtered benchmarks are useful diagnostics but do not replace the full workload.

```sh
./scripts/check_perf_thresholds.sh
./scripts/check_load_thresholds.sh
./scripts/check_allocations.sh
```

Criterion uses the ordinary release profile. Load tests use the production
profile (fat LTO, `panic=abort`). Preserve `reports/perf_gate_report.json`,
`reports/load_gate_report.json`, the allocation report and command logs with the
review evidence. Reports are generated artifacts, not files to add to the source
tree. Attach them to the MR or retain an inspectable evidence artifact and link it.

Record the tested commit and any uncommitted changes, host/CPU, OS, Rust version,
profile, commands, date, background load and verdict. Record all failures and
inconclusive results as well as successes. Changing a measurement environment is
a diagnostic step; repeated retries until one passes are not a regression fix.

Absolute bounds are calibrated measurements. The current recorded regression
baseline identifies `mistral-apple-m1-pro`; only that actual host should set
`TOLLGATE_PERF_HOST=mistral-apple-m1-pro` when running the Criterion script to
activate its baseline comparisons. A different or unset host skips the recorded
baseline, and that limitation must appear in the MR. Never set a host label to
make foreign measurements appear comparable.

On another host, `./scripts/check_perf_thresholds.sh --ratios-only` can provide
local diagnostic ratios while skipping absolute and recorded-baseline decisions.
Its historical unreadable-run abstention remains part of the CLI contract: an
exit code of zero is not by itself proof of a readable performance pass. Inspect
the report's trust and verdict fields. The load command's `--evidence` mode and
`testing/load_thresholds_ci.json` remain available as diagnostic configuration;
they do not schedule remote CI or establish full acceptance.

## Review and CI contract

Performance acceptance is a local review responsibility. There is currently no
CI attestation that a reviewer ran or inspected these reports. A missing local
report is an incomplete performance-sensitive change even when CI is green.
Unreadable measurements require a suitable measurement environment; genuine
regressions require a fix or an explicitly justified threshold/baseline change.
This policy changes where measurements run, not the benchmark workloads,
thresholds, request-path constraints or baseline data.

CI continues to require format, lint, toolchain compatibility, tests, PostgreSQL
parity, dependency advisories, benchmark compilation, allocation assertions,
formal verification and mutation assurance. Formal and mutation jobs run on
every merge request regardless of its target. `scripts/check_ci_rules.sh`
enforces their blocking status and rejects restoration of the retired timed
performance jobs or direct timed benchmark commands.

The decision follows release !177's inconsistent ratios on unchanged source:
one run failed reserved/uniform capacity; the next passed that comparison and
failed disabled-capacity/admission. Separately tagged runner registrations also
reported the same host identity. Infrastructure investigation remains tracked in
#113; remote timing must not be restored without a separately reviewed design
and evidence of repeatable measurement conditions. See `docs/DESIGN.md` for the
incident evidence and decision history.
