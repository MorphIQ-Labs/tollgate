#!/usr/bin/env bash
# Runs the dev-only allocation and one-lookup assertions independently of
# Criterion. Counts are structural and current-thread scoped, so this gate is
# valid on shared CI hosts.
set -euo pipefail

cd "$(dirname "$0")/.."

# A missing tool must fail the gate, not skip a check. The reverse-dependency
# assertions below read a pipeline's exit status, where "command not found"
# and "nothing matched" are indistinguishable — so the tools are required up
# front and each `cargo tree` result is captured before it is inspected.
for tool in cargo jq grep; do
  command -v "$tool" > /dev/null 2>&1 || {
    echo "allocation gate: required tool '$tool' is not installed" >&2
    exit 1
  }
done

REPORT="reports/allocation_report.json"
rm -f "$REPORT"
mkdir -p reports
export TOLLGATE_ALLOC_REPORT="$PWD/$REPORT"

cargo test --locked -p tollgate-alloc-count -- --test-threads=1
cargo test --locked -p tollgate-core --test allocations -- --test-threads=1
cargo test --locked -p tollgate-auth --test allocations -- --test-threads=1
cargo test --locked -p tollgate-admission --test allocations -- --test-threads=1
cargo test --locked -p tollgate-client --test allocations -- --test-threads=1

release_graph="$(cargo tree --locked -e normal -p pricing-api -p tollgate-server)"
if printf '%s\n' "$release_graph" | grep -q 'tollgate-alloc-count v'; then
  echo "allocation gate: test harness entered a release binary dependency graph" >&2
  exit 1
fi

reverse_deps="$(cargo tree --locked --workspace -e normal -i tollgate-alloc-count --prefix none)"
# An empty or unrecognizable graph would make the "no other dependent" test
# below vacuously true, so require the crate's own line before trusting it.
if ! printf '%s\n' "$reverse_deps" | grep -q '^tollgate-alloc-count '; then
  echo "allocation gate: reverse-dependency graph did not list tollgate-alloc-count" >&2
  exit 1
fi
if printf '%s\n' "$reverse_deps" | grep -v '^$' | grep -qv '^tollgate-alloc-count '; then
  echo "allocation gate: test harness has a normal reverse dependency" >&2
  exit 1
fi

# `tollgate` scopes must be allocation-free. The other attributions name what
# an allocation belongs to, and each is bounded by the test that records it
# rather than merely exempted here — an exemption that cannot fail would
# witness nothing:
#
#   tollgate_opt_in       GL-93's shared cancel state; exactly one allocation,
#                         checked below, so its cost cannot drift upward.
#   dependency_amortized  a dependency's own housekeeping, which Tollgate does
#                         not own and the dependency does not promise to avoid
#                         (moka drains its read log on a 300ms timer). Held to
#                         a per-operation budget by `assert_amortized_bound`.
#   tollgate_cold_path    off the request path by construction.
#   caller / consumer_executor
#                         the embedder's own allocations, reported so the
#                         boundary between ours and theirs stays visible.
jq -s -e '
  length > 0
  and any(.[]; .scope == "embedding/attributed_commit_through_record" and .attribution == "tollgate")
  and any(.[]; .scope == "auth/projected_cache_hit" and .attribution == "tollgate")
  and any(.[]; .scope == "auth/projected_expiry" and .attribution == "tollgate")
  and any(.[]; .scope == "caller/request_buffer" and .attribution == "caller")
  and any(.[]; .scope == "consumer/executor_job" and .attribution == "consumer_executor")
  and any(.[]; .scope == "reservation/commit_split" and .attribution == "tollgate_opt_in")
  and all(.
    [];
    if .attribution == "tollgate"
    then .allocations.alloc_calls == 0
      and .allocations.alloc_zeroed_calls == 0
      and .allocations.realloc_calls == 0
    elif .attribution == "tollgate_opt_in"
    then .allocations.alloc_calls == 1
      and .allocations.alloc_zeroed_calls == 0
      and .allocations.realloc_calls == 0
    else true
    end
  )
' "$REPORT" >/dev/null

echo "allocation gate: PASS ($REPORT)"
