# quota-service invariants

A change that violates one of these is a defect even when every test passes.
Each invariant names the test(s) that enforce it; a new invariant is not "done"
until it has one.

1. **Bounded spend.** Total committed usage across all instances never exceeds
   the units allocated to the account, under any interleaving of concurrent
   clients. Enforced by fenced leases: units are spent only from a lease, and a
   lease's units were atomically debited from the account at allocation.
   *Tests:* `no_double_spend_across_instances` (memory and Postgres variants),
   `quota-core` reservation proptests.

2. **Zero charge before execution.** A reservation that never reaches
   `commit_at_execution_start` charges zero units, and its units return to the
   local lease. Dropping a pending reservation is a release, not a leak.
   *Tests:* `reservation::tests::{drop_releases_pending, cancel_charges_zero}`.

3. **Atomic commit-vs-cancel.** Commit and cancel race on a single atomic
   transition; exactly one wins. A cancelled reservation can never later
   commit; a committed reservation reports its full charge to a late
   canceller. *Tests:* `reservation::tests::commit_cancel_race_one_winner`.

4. **Fencing is absolute.** A lease holder with a stale fencing token is
   rejected by the allocator, by renewal, and by the usage sink — regardless
   of timing. Fencing tokens are strictly monotonic per account.
   *Tests:* `fenced_out_holder_rejected` (store suites).

5. **Fail closed, zero I/O.** Unknown principal, suspended/closed account,
   expired snapshot, missing permission, exhausted or expired lease: all deny
   locally. The request path performs no database, file, lock-file, or network
   access — not even on a miss. *Tests:* `quota-core` deny-path unit tests;
   admission-crate miss tests assert no store calls from the request path.

6. **Foreground isolation.** Lease refill and snapshot replacement never block
   an in-flight request. *Tests:* client refill tests; `lease/refill_offpath`
   bench assertion.

7. **Idempotent accounting.** Replaying a usage batch (same request IDs) never
   double-bills. *Tests:* `usage_replay_is_idempotent` (store suites).

8. **Accounting backpressure sheds.** When the usage queue is full, new work is
   refused with zero units charged. Usage events are never silently dropped
   and enqueue never blocks unboundedly. *Tests:* client writer overflow tests.

9. **Crash leak is bounded by TTL.** A crashed lease holder strands its unspent
   units only until the lease TTL expires, after which the allocator reclaims
   them. *Tests:* `expired_lease_units_reclaimed` (store suites).

10. **Not ready until admissible.** An instance reports ready only after its
    snapshot set has loaded; fail-closed correctness must not masquerade as
    availability. *Tests:* server/client readiness tests.

11. **Checked arithmetic only.** Cost and lease arithmetic never wraps; any
    overflow is an explicit error that denies (fail closed), never a wrap to a
    small charge. *Tests:* `quota-core` proptests.

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero.
