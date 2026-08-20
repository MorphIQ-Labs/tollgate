# tollgate invariants

A change that violates one of these is a defect even when every test passes.
Each invariant names the test(s) that enforce it; a new invariant is not "done"
until it has one.

1. **Bounded spend.** Total committed usage across all instances never exceeds
   the units allocated to the account, under any interleaving of concurrent
   clients. Enforced by fenced leases: units are spent only from a lease, and a
   lease's units were atomically debited from the account at allocation.
   *Tests:* `no_double_spend_across_instances` (memory and Postgres variants),
   `tollgate-core` reservation proptests.

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
   access — not even on a miss. *Tests:* `tollgate-core` deny-path unit tests;
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
    small charge. *Tests:* `tollgate-core` proptests.

12. **No commit outside the usability window.** A lease is locally usable
    until `expires_at - safety margin`; both debits and commits stop there,
    and the allocator reclaims only after `expires_at + grace` (accepting
    releases and late usage through the window). Work committed inside the
    window therefore always has margin + grace to be flushed and billed;
    work cannot commit against capacity the allocator may have re-granted.
    *Tests:* `reservation::tests::{commit_after_window_closes_releases_for_zero,
    safety_margin_closes_window_before_expiry}`,
    `reclaim_waits_for_grace_and_release_works_within_it` (both store suites).

13. **A committed charge is always emitted.** Committing through
    `ChargeGuard` binds the billing event to the pre-reserved queue permit;
    normal completion, early return, panic unwind, and task abort all
    enqueue it. A spent lease with no billing event requires losing the
    whole process. *Tests:* `panic_after_commit_still_bills`.

14. **Account creation is never destructive.** Recreating an existing
    account is a surfaced `AlreadyExists` in every backend — never an
    overwrite, never a silent no-op. *Tests:*
    `recreate_account_is_refused_and_nondestructive` (both store suites).

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero.
