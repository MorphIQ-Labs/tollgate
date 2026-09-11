# Local performance validation

Timed Criterion benchmarks and loopback load tests run locally. Remote CI
compiles every benchmark and enforces deterministic allocation counts, but it
does not execute timed performance jobs. A green pipeline therefore establishes
no latency or throughput verdict. Performance-sensitive changes and releases
need reviewed local evidence before merge.

## Run and retain evidence

Use a quiet host on AC power and an isolated checkout/target directory. Keep
power mode consistent throughout a calibration series. Do not run builds,
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
activate its baseline comparisons. Never set a host label to make foreign
measurements appear comparable.

A full run that does not enforce the baseline exits `4` and reports
`UNENFORCED`. That is deliberate: a full report is what a reviewer accepts a
performance-sensitive change on, and one with every regression row marked
`baseline-skipped` established nothing about this host. Do not treat it as
evidence, and do not clear it by unsetting the host.

On another host, `./scripts/check_perf_thresholds.sh --ratios-only` can provide
local diagnostic ratios while skipping absolute and recorded-baseline decisions.
Its historical unreadable-run abstention remains part of the CLI contract: an
exit code of zero is not by itself proof of a readable performance pass. Inspect
the report's trust, drift and verdict fields. `drift` gives the median and
quartiles of every row's ratio against the baseline: a run whose median sits
well above ×1.00 was slow as a whole, and the rows that crossed their bound in
it are the ones with the least headroom rather than the ones that changed. The
load command's `--evidence` mode and `testing/load_thresholds_ci.json` remain
available as diagnostic configuration; they do not schedule remote CI or
establish full acceptance.

An isolated checkout starts with no `reports/perf_gate_history.json`, so the
run-to-run trust verdict is `no_history` unless you carry one. Pass
`--history <path>` through to the gate to point successive runs at a stable
file when you want that verdict.

## Recalibrating the baseline

`testing/perf_baseline.json` is generated, never hand-edited. A readable,
complete full run with recording provenance deposits its measurements under
`target/perf-samples`. `--record` takes the per-row median over distinct runs
at the current revision in the same measurement environment:

```sh
# start a fresh series at the revision you are recording
TOLLGATE_PERF_HOST=mistral-apple-m1-pro ./scripts/check_perf_thresholds.sh --fresh-samples
TOLLGATE_PERF_HOST=mistral-apple-m1-pro ./scripts/check_perf_thresholds.sh
TOLLGATE_PERF_HOST=mistral-apple-m1-pro ./scripts/check_perf_thresholds.sh --record
```

Recording refuses with fewer than three distinct readable samples, because a
baseline says where a benchmark usually lands and one run cannot. It refuses a run the
gate would not read, a row that is missing from any sample, a partial manifest,
a dirty worktree, provenance it was not given, and a host that does not match
the file it would replace. It stages and validates before promoting, so a
refusal leaves the previous baseline intact. Widened per-row `max_regression`
values carry forward, and the file records how many runs its medians came from.

Each sample retains the revision, host ID, target architecture, CPU, OS,
compiler and Criterion profile. All must match the recording context; other
contexts are skipped with a diagnostic. The freshness marker's timestamp
identifies the benchmark run, so processing the same marker and estimates
again counts once, even if a sample file has been copied. A conflicting
copy or a retry with different means or provenance is refused. Create the
marker once before each benchmark suite and keep it unchanged when retrying
the checker. Legacy samples without run identity and environment are skipped
with a warning; collect a new series using `--fresh-samples`.

Sample collection is optional during ordinary gating. Dirty worktrees,
missing recording provenance or sample-write failures produce a diagnostic
without changing the normal verdict. `--ratios-only` does not deposit samples.
Recording still requires complete, readable measurements and valid provenance;
invoking the binary directly with `--record` also requires `--samples <dir>`.

The recording destination supplies the host check and carried regression
bounds even when `--baseline` is absent or points to another file. `--baseline`
selects only the input used to judge this run. A successful recording does not
change that verdict or replace the required fresh validation run.

Sample publication is atomic and exclusive: concurrent retries retain one
complete sample. Baseline recording exclusively reserves its sibling
`.json.staged` file before reading the destination, validates the replacement,
then renames it atomically. If interrupted, confirm the recorder has stopped
before removing its leftover staging file and retrying. Do not remove an active
writer's staging file. A refusal preserves the previous baseline.

Then validate with a fresh full run that exits 0 with `enforced: true`, and put
both the recalibration and that validating report in the merge request.

Leave the host idle between runs — several minutes, not seconds. Back-to-back
full-suite runs on Apple Silicon degrade: in the #114 measurements the first
run after an idle gap was clean while the two immediately following it both
came back `unreadable`, with ten of fifty-eight rows exceeding the confidence
-interval bound. Judge a run by the report, not by how quiet the machine looked
when it started: one #114 run began at 73% idle under a load average of 2.62,
finished at 88% idle, was marked `trusted` with only two unstable rows, and
still put nineteen rows over their bound on a `drift` of ×1.048. Idle and load
average are context; `drift` and `trust` are the verdict.

A row whose measurement moved because a change added work to its path is
recalibrated by the change that added the work — not later, and not alone.

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
