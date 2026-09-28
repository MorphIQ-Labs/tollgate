//! The admission pipeline itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CancelHandle, CommitError, CommitFunding, CostQuote, CostUnits, DenyReason,
    Generation, Locality, OpIndex, PermissionBits, PolicyRevision, QuoteError, RequestId,
    Reservation, SharedCharge, UsageEvent, UsageSlot, UsageSource,
};

use crate::capacity::{CapacityEvidence, CapacityGate, CapacityPermit};
use crate::counters::{AdmissionCounters, CommitRefusal};
use crate::state::{AccountAdmissionState, ConcurrencyGuard, MapEntry, Principal, SnapshotMap};

/// Generation-pinned stage-one evidence, owned across body decoding.
#[derive(Debug)]
pub struct RequestContext {
    /// `Option` so `admit` can *move* the state out rather than clone it.
    ///
    /// The counter below needs a `Drop`, and a type with `Drop` cannot be
    /// destructured — but cloning the `Arc` to work around that would put a
    /// contended refcount bump on the hottest staged path for the sake of a
    /// counter that only fires when the request ends early. `Option<Arc<_>>`
    /// is niche-optimized to the same size as the `Arc`, so taking it costs
    /// one null write and no atomic, and the `None` it leaves behind is
    /// exactly the "this context was consumed" signal `Drop` needs.
    state: Option<Arc<AccountAdmissionState>>,
    locality: Locality,
}

impl RequestContext {
    /// The account snapshot this request pinned in
    /// [`AdmissionEngine::begin`]; a later publication does not replace it
    /// (INVARIANTS.md 26).
    #[must_use]
    pub fn snapshot(&self) -> &AccountSnapshot {
        &self.state().snapshot
    }

    /// The resolved limits of the pinned snapshot, such as the per-request
    /// item cap `admit` enforces.
    #[must_use]
    pub fn limits(&self) -> &tollgate_core::ResolvedLimits {
        &self.state().snapshot.limits
    }

    /// The generation of the pinned snapshot: the one every later stage of
    /// this request decides against.
    #[must_use]
    pub fn generation(&self) -> Generation {
        self.state().snapshot.generation
    }

    /// The consuming application's identity for the policy governing this
    /// request (GL-94).
    ///
    /// Pinned with everything else: a revision republished after this stage
    /// belongs to the next request, not this one. That is what lets a
    /// consumer resolve its own customer-visible metadata locally, with no
    /// I/O, and know it describes the policy that actually priced the work.
    #[must_use]
    pub fn policy_revision(&self) -> PolicyRevision {
        self.state().snapshot.policy_revision
    }

    /// The pinned state. Present for the whole of a context's public life:
    /// only `admit` takes it, and `admit` consumes the context.
    #[inline]
    fn state(&self) -> &AccountAdmissionState {
        self.state
            .as_ref()
            .expect("a request context holds its pinned state until admit consumes it")
    }

    /// What the account can still spend this period, as this instance best
    /// knows it — see [`AccountAdmissionState::estimate_remaining`] for what
    /// the estimate is wrong by and why it is never an authorization input.
    ///
    /// Reachable from all three response-producing stages, because a caller
    /// answering "how much is left" has to answer it on a denial as well: this
    /// stage is the only one a denied request still holds, `Pending` is what a
    /// cancelled one holds, and [`Committed`] is what a served one holds.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self.state().estimate_remaining()
    }

    /// Price the decoded workload and reserve funding for it: stage two of
    /// admission, run after body decoding without another map lookup.
    ///
    /// `workload` is `(operation, item count)` pairs, quoted against the
    /// pinned snapshot's cost table; entries with a zero count are skipped.
    /// In order, this checks that the snapshot is still valid at `now`, that
    /// the workload prices, that the pinned snapshot grants the permissions
    /// its operation classes require, and that the item count is within the
    /// per-request cap; then it takes the request-count and weighted rate
    /// tokens, acquires principal and account concurrency, and debits the
    /// lease (or, under [`EnforcementMode::Elastic`], overage when the lease
    /// cannot fund the quote). `slot` is bound now and receives the usage
    /// event when the eventual [`Committed`] guard drops (INVARIANTS.md 13).
    ///
    /// On `Err` no funding is reserved, no concurrency is held, and the
    /// refusal is already counted in [`AdmissionCounters`]. Rate tokens taken
    /// before a later refusal are not returned. Performs no I/O and does not
    /// block.
    ///
    /// [`EnforcementMode::Elastic`]: tollgate_core::EnforcementMode::Elastic
    pub fn admit<O: OpIndex, S: UsageSlot>(
        self,
        workload: &[(O, u64)],
        slot: S,
        now: Timestamp,
    ) -> Result<Pending<S>, DenyReason> {
        let mut this = self;
        let locality = this.locality;
        // Taking the state is both the ownership transfer `admit(self)` always
        // performed and the signal that this context was not abandoned: `Drop`
        // sees `None` and counts nothing. No clone, so the hot path pays no
        // refcount for a counter that fires only when a request ends early.
        let state = this
            .state
            .take()
            .expect("a request context holds its pinned state until admit consumes it");
        let quote = match compile_workload(&state.snapshot, workload, now) {
            Ok(quote) => quote,
            Err(reason) => {
                state.counters.record_deny_at(&reason, locality);
                return Err(reason);
            }
        };
        admit_priced(state, locality, quote, now).map(|priced| Pending {
            concurrency: priced.concurrency,
            funding: Funding::Owned(priced.reservation),
            slot,
            quote: priced.quote,
            locality,
        })
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        let Some(state) = &self.state else {
            // `admit` took the state, so stage two ran and recorded its own
            // outcome.
            return;
        };
        // Reached only when the context was never consumed by `admit`: the
        // request authenticated and pinned a generation, then ended before
        // stage two — a failed body read, a client disconnect, a refused
        // decode. No pending funding exists and no admission outcome was
        // decided, so this is neither an admission nor a denial. Counting it
        // is what keeps an instance that authenticates a flood it never admits
        // distinguishable from one serving nothing (INVARIANTS.md GL-20).
        state.counters.record_context_abandoned();
    }
}

/// Where a staged request's funding lives: owned outright, or shared with an
/// asynchronous canceller.
///
/// Both arms resolve through the same `&Reservation`, so there is one commit
/// path and one cancel path regardless of which the consumer chose. Splitting
/// changes who may *ask* for a cancellation, never how the race is decided.
#[derive(Debug)]
enum Funding {
    /// The default. Allocation-free, and no external cancel race.
    Owned(Reservation),
    /// Opted into by [`ReadyToStart::split`]; one `Arc` per request.
    Shared(WorkerShare),
}

impl Funding {
    #[inline]
    fn reservation(&self) -> &Reservation {
        match self {
            Self::Owned(reservation) => reservation,
            Self::Shared(shared) => shared.0.reservation(),
        }
    }

    #[inline]
    fn cancel_requested(&self) -> bool {
        match self {
            // Nobody else holds a handle, so nobody can have asked.
            Self::Owned(_) => false,
            Self::Shared(shared) => shared.0.cancel_requested(),
        }
    }
}

/// The worker's half of a shared charge.
///
/// Its `Drop` is the reason this is a named type rather than a bare `Arc`. An
/// `Owned` reservation is released by `Reservation::drop`, but a shared one is
/// co-owned by the cancel handle — so waiting for the reservation's own drop
/// would hold funding until the *asynchronous* side also let go, which is an
/// unbounded interval after the worker abandoned the request. Releasing here
/// refunds at the instant the worker gives up, and is a no-op on a phase that
/// already resolved, so a committed guard's eventual drop changes nothing.
///
/// The guard sits inside the enum rather than on `Funding` itself so that
/// `Funding` stays freely movable: `commit` and `split` both destructure the
/// staged types, and a `Drop` on the enum would make that impossible without
/// `unsafe`.
#[derive(Debug)]
struct WorkerShare(Arc<SharedCharge>);

impl Drop for WorkerShare {
    fn drop(&mut self) {
        self.0.reservation().cancel();
    }
}

/// Funding is reserved, but execution capacity has not yet been acquired.
///
/// The reservation, the concurrency guard, and the usage slot have no public
/// accessors. Every transition out of this state consumes it, so safe code
/// cannot retain funding while releasing occupancy, and cannot reach the
/// reservation to resolve it out of band.
///
/// The reservation is unreachable because the field is private, and the error
/// code is pinned so this witness cannot pass for an unrelated reason: making
/// the field public compiles (E0616 disappears), and renaming it reports E0609
/// instead. Either way the doctest fails and the invariant is re-examined.
///
/// ```compile_fail,E0616
/// use tollgate_core::DiscardedUsageSlot;
///
/// fn detach(pending: tollgate_admission::Pending<DiscardedUsageSlot>) {
///     let _raw = pending.reservation;
/// }
/// ```
///
/// Its companion: the supported path compiles, so the refusal above can never
/// be a refusal of an API that stopped existing.
///
/// ```
/// use tollgate_core::DiscardedUsageSlot;
///
/// # fn release(
/// #     pending: tollgate_admission::Pending<DiscardedUsageSlot>,
/// # ) -> tollgate_admission::Released {
/// pending.cancel()
/// # }
/// ```
#[derive(Debug)]
pub struct Pending<S: UsageSlot> {
    concurrency: ConcurrencyGuard,
    funding: Funding,
    slot: S,
    quote: CostQuote,
    /// The locality `begin` pinned, carried so later phases tally into the
    /// same counter shard the admission did rather than re-reading the
    /// thread-local — which, after a Tokio worker hop, would be a different
    /// shard for the same request.
    locality: Locality,
}

impl<S: UsageSlot> Pending<S> {
    /// The price `admit` computed and reserved: the full charge if execution
    /// starts.
    #[must_use]
    pub fn quote(&self) -> CostQuote {
        self.quote
    }

    /// The account snapshot this request pinned in
    /// [`AdmissionEngine::begin`], unchanged by later publications.
    #[must_use]
    pub fn snapshot(&self) -> &AccountSnapshot {
        &self.concurrency.state().snapshot
    }

    /// The resolved limits of the pinned snapshot.
    #[must_use]
    pub fn limits(&self) -> &tollgate_core::ResolvedLimits {
        &self.snapshot().limits
    }

    /// See [`RequestContext::policy_revision`]. Still the revision this
    /// request pinned, whatever the control plane has published since.
    #[must_use]
    pub fn policy_revision(&self) -> PolicyRevision {
        self.snapshot().policy_revision
    }

    /// See [`RequestContext::estimate_remaining`]. This request's own quote is
    /// already counted: admission tallies the units it reserved.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self.concurrency.state().estimate_remaining()
    }

    /// Ask `gate` for execution capacity for this request, using the
    /// capacity class and generation of the pinned snapshot.
    ///
    /// On success the funding and the permit travel together in
    /// [`ReadyToStart`]. On refusal the reservation is released for zero, the
    /// shed is counted against the request's class, and the gate's
    /// [`DenyReason`] is returned with the [`Released`] proof. Never waits for
    /// capacity: a gate refuses rather than queues.
    pub fn acquire_capacity<G: CapacityGate>(
        self,
        gate: &G,
    ) -> Result<ReadyToStart<S, G::Permit>, (DenyReason, Released)> {
        // Built from the same immutable snapshot that authorized and priced
        // this request, so the class cannot come from caller input and cannot
        // change between admission and the gate's decision (GL-99, and the
        // generation pinning of INVARIANTS.md GL-26).
        let snapshot = &self.concurrency.state().snapshot;
        let evidence =
            CapacityEvidence::new(snapshot.capacity_class, snapshot.generation, self.locality);
        match gate.acquire(evidence) {
            Ok(permit) => Ok(ReadyToStart {
                pending: self,
                permit,
            }),
            Err(denied) => {
                // The gate decided this, so the gate's own tally records it
                // rather than requiring every embedder to remember to — and it
                // claims the terminal slot, so the guard's `Drop` does not
                // also report a cancellation for the same request.
                let mut pending = self;
                // Against the class whose work was refused, so an operator can
                // see whether the reserve is doing its job or the instance is
                // simply too small (GL-99).
                pending
                    .counters()
                    .record_capacity_shed_for(pending.concurrency.state().snapshot.capacity_class);
                pending.concurrency.mark_terminal_recorded();
                Err((denied, Released))
            }
        }
    }

    /// Resolve this request before execution for zero charge.
    ///
    /// Releases the reserved funding and the concurrency slots; the request
    /// is counted as cancelled before start.
    pub fn cancel(self) -> Released {
        // The guard's `Drop` records the terminal outcome; cancelling here
        // only resolves the funding.
        self.funding.reservation().cancel();
        Released
    }

    #[inline]
    fn counters(&self) -> &AdmissionCounters {
        &self.concurrency.state().counters
    }
}

/// Funding and execution capacity are both held; committing consumes this
/// proof immediately before the computational kernel starts.
#[derive(Debug)]
pub struct ReadyToStart<S: UsageSlot, P: CapacityPermit> {
    pending: Pending<S>,
    permit: P,
}

impl<S: UsageSlot, P: CapacityPermit> ReadyToStart<S, P> {
    /// Commit the charge because the kernel is about to start, and return
    /// the guard that must be held while it runs.
    ///
    /// From a successful commit the full quote stands however execution
    /// ends, and the usage event is emitted when the [`Committed`] guard
    /// drops. The lease's usability window is rechecked at `now`. If it has
    /// lapsed, [`EnforcementMode::Strict`] refuses with
    /// [`CommitError::Denied`]; [`EnforcementMode::Elastic`] settles the same
    /// charge against overage instead, refusing only if the overage cap
    /// cannot hold it.
    ///
    /// On `Err` the caller must not execute: the funding is already released
    /// for zero. [`CommitError::Denied`] carries the refusal reason;
    /// [`CommitError::Cancelled`] means a [`CancelHandle`] from
    /// [`split`](Self::split) won the race first.
    ///
    /// [`EnforcementMode::Strict`]: tollgate_core::EnforcementMode::Strict
    /// [`EnforcementMode::Elastic`]: tollgate_core::EnforcementMode::Elastic
    #[must_use = "the kernel may run only while holding the returned Committed guard"]
    pub fn commit(
        self,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<Committed<S, P>, (CommitError, Released)> {
        let Pending {
            concurrency,
            funding,
            slot,
            quote: _,
            locality,
        } = self.pending;
        let mut concurrency = concurrency;
        // Whether the lease funded this admission, read before the commit can
        // change it: an admission the lease funded that settles against
        // overage is the commit-time transition, and it is a different fact
        // from an admission no lease could fund.
        let admitted_on_lease = !funding.reservation().admitted_as_overage();
        // Scoped so the borrow of the pinned state ends before the terminal
        // tally below needs the guard mutably. The pinned snapshot decides
        // what a lapsed lease means, and the counter comes from the same slot
        // that funded the reservation.
        let committed = {
            let state = concurrency.state();
            let commit_funding =
                CommitFunding::from_mode(state.snapshot.enforcement_mode, state.lease.overage());
            funding
                .reservation()
                .commit_at_execution_start(now, commit_funding)
        };
        let units = match committed {
            Ok(units) => units,
            // Core already released for zero and classified the refusal —
            // expired funding, or an overage cap the fallback could not fit
            // inside. Each keeps its own retry class through to the embedder.
            Err(denied @ CommitError::Denied(_)) => {
                if let Some(refusal) = CommitRefusal::from_commit_error(&denied) {
                    concurrency.state().counters.record_commit_refusal(refusal);
                }
                concurrency.mark_terminal_recorded();
                return Err((denied, Released));
            }
            Err(CommitError::AlreadyReleased | CommitError::Cancelled) => {
                // A cancellation won the phase. That is Tollgate's outcome, so
                // the guard's default cancellation tally is exactly right and
                // is deliberately left armed here.
                return Err((CommitError::Cancelled, Released));
            }
            Err(CommitError::AlreadyCommitted) => {
                debug_assert!(
                    matches!(funding, Funding::Shared(_)),
                    "an owned ready state can commit only once"
                );
                return Err((CommitError::Cancelled, Released));
            }
        };
        // The revision comes from the same pinned snapshot that priced the
        // request, so the billing record names the policy the response will.
        let policy_revision = concurrency.state().snapshot.policy_revision;
        let key_id = concurrency.state().snapshot.key_id;
        let event = funding
            .reservation()
            .usage_event(request_id, now, policy_revision, key_id)
            .expect("a committed reservation produces usage evidence");
        concurrency
            .state()
            .counters
            .record_execution_started_for(concurrency.state().snapshot.capacity_class, locality);
        if admitted_on_lease && event.source == UsageSource::Overage {
            concurrency
                .state()
                .counters
                .record_committed_at_overage(units);
        }
        concurrency.mark_terminal_recorded();
        Ok(Committed {
            event: Some(event),
            slot: Some(slot),
            units,
            request_id,
            // Retained so a committed kernel can still observe a late
            // cancellation request, and so a late `CancelHandle::cancel`
            // finds the committed phase rather than a freed reservation.
            funding,
            _concurrency: concurrency,
            _capacity: self.permit,
        })
    }

    /// Share this request's charge state with an asynchronous canceller.
    ///
    /// The handle can cancel and observe; it has no method that starts
    /// execution, so a waiter cannot commit a charge behind the worker's back:
    ///
    /// ```compile_fail,E0599
    /// # use tollgate_core::{CancelHandle, RequestId};
    /// # fn waiter_cannot_commit(handle: CancelHandle, request_id: RequestId, now: jiff::Timestamp) {
    /// handle.commit(request_id, now);
    /// # }
    /// ```
    ///
    /// Its companion, so the refusal above is a refusal to *commit* rather
    /// than a refusal of a handle that stopped existing:
    ///
    /// ```
    /// # use tollgate_core::{CancelHandle, CancelOutcome};
    /// # fn waiter_may_cancel(handle: CancelHandle) -> CancelOutcome {
    /// let _asked = handle.is_cancelled();
    /// handle.cancel()
    /// # }
    /// ```
    ///
    /// Returns the same worker-owned value plus a [`CancelHandle`] for the
    /// side that races it — a timeout, a client disconnect, a shutdown. Both
    /// halves resolve the *same* compare-exchange, so exactly one of commit
    /// and cancel wins and Tollgate remains the single authority for whether
    /// the request charged.
    ///
    /// Opt-in, and the one allocation this lifecycle adds: an inline executor
    /// never calls it and its path stays allocation-free. A consumer that
    /// moves an unsplit value to its worker has chosen to have no external
    /// cancel race.
    ///
    /// The handle can cancel and observe; it cannot commit, and it cannot take
    /// the usage slot, concurrency guard, or capacity permit out of the
    /// worker's value. Those are released exactly once, by the side that owns
    /// them, when it observes the cancellation and drops — so a consumer whose
    /// executor queues work must discard cancelled jobs and quiesce them at
    /// shutdown rather than leaving them parked.
    #[must_use = "the returned state is the only one that can commit; dropping it releases the request"]
    pub fn split(self) -> (Self, CancelHandle) {
        let ReadyToStart { pending, permit } = self;
        let Pending {
            concurrency,
            funding,
            slot,
            quote,
            locality,
        } = pending;
        let (shared, handle) = match funding {
            Funding::Owned(reservation) => {
                let (shared, handle) = reservation.split();
                (WorkerShare(shared), handle)
            }
            // Splitting twice hands out a second handle to the same charge
            // rather than building a second one; both cancel the same phase.
            Funding::Shared(shared) => {
                let handle = shared.0.cancel_handle();
                (shared, handle)
            }
        };
        (
            ReadyToStart {
                pending: Pending {
                    concurrency,
                    funding: Funding::Shared(shared),
                    slot,
                    quote,
                    locality,
                },
                permit,
            },
            handle,
        )
    }

    /// Resolve this request before execution for zero charge, releasing its
    /// funding, concurrency slots and execution-capacity permit.
    pub fn cancel(self) -> Released {
        self.pending.cancel()
    }
}

/// Typed evidence that a request resolved before execution with zero charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Released;

/// The only proof that the computational kernel may run.
///
/// The guard owns the committed charge for the whole execution: its `Drop`
/// records the billing event into the usage slot bound at admission and only
/// then releases concurrency and execution capacity. Normal completion, early
/// return, panic unwind, and task abort at an await point all run `Drop`, so a
/// committed charge cannot be spent-but-unbilled.
///
/// # Panic boundary: who owns what
///
/// `Drop` is safe to run while unwinding, and that is Tollgate's half of the
/// contract. The event is built at commit rather than at drop, so the drop
/// path takes no lock that could be poisoned, allocates nothing, and cannot
/// fail. There is no `Mutex` anywhere in the charge state — the shared cancel
/// path is a compare-exchange, which is what makes it usable from a thread
/// that is already panicking.
///
/// **The consumer owns the panic boundary itself**, because Tollgate does not
/// run the kernel and cannot wrap it. This matters concretely for a Rayon-style
/// executor: a panic in a closure given to `spawn` propagates at the join and
/// can abort a pool thread, so without a consumer-installed `catch_unwind` (or
/// equivalent) around the kernel, the guard is *leaked* rather than dropped on
/// the worker — and a leaked guard emits nothing. Scope the guard to the
/// computational kernel only, never across response serialization or an
/// unrelated asynchronous wait, and drop it inside that boundary.
///
/// Under the `production` profile (`panic=abort`) unwinding does not exist, so
/// none of this applies and INVARIANTS.md GL-13 keeps its stated boundary: a
/// spent lease with no billing event requires losing the whole process.
///
/// That guarantee is worth nothing if the guard can be dropped at the point it
/// is produced, so discarding execution-start evidence is a compile-time error
/// under the standard `unused_must_use` lint:
///
/// ```compile_fail
/// # #![deny(unused_must_use)]
/// use tollgate_core::DiscardedUsageSlot;
/// use tollgate_admission::{NoCapacityPermit, ReadyToStart, Released};
///
/// # fn discard(
/// #     ready: ReadyToStart<DiscardedUsageSlot, NoCapacityPermit>,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<(), (tollgate_core::CommitError, Released)> {
/// ready.commit(request_id, now)?;
/// # Ok(())
/// # }
/// ```
///
/// Its companion: binding the guard compiles, so the refusal above is a
/// refusal to *discard* the guard rather than a refusal of a call that stopped
/// type-checking.
///
/// ```
/// # #![deny(unused_must_use)]
/// use tollgate_core::DiscardedUsageSlot;
/// use tollgate_admission::{Committed, NoCapacityPermit, ReadyToStart, Released};
///
/// # fn hold(
/// #     ready: ReadyToStart<DiscardedUsageSlot, NoCapacityPermit>,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<Committed<DiscardedUsageSlot, NoCapacityPermit>, (tollgate_core::CommitError, Released)> {
/// ready.commit(request_id, now)
/// # }
/// ```
#[derive(Debug)]
#[must_use = "hold this guard for the full execution lifetime: dropping it emits the billing event and releases occupancy"]
pub struct Committed<S: UsageSlot, P: CapacityPermit> {
    event: Option<UsageEvent>,
    slot: Option<S>,
    units: CostUnits,
    request_id: RequestId,
    funding: Funding,
    _concurrency: ConcurrencyGuard,
    _capacity: P,
}

impl<S: UsageSlot, P: CapacityPermit> Committed<S, P> {
    /// The units charged: the request's full quote, which stands however
    /// execution ends.
    #[must_use]
    pub fn units(&self) -> CostUnits {
        self.units
    }

    /// The request identifier passed to [`ReadyToStart::commit`], and
    /// carried by the usage event this guard emits.
    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// The policy revision this charge is billed under (GL-94).
    ///
    /// Read from the usage event this guard will emit, not from the snapshot
    /// again. A consumer returns this in its response metadata, so taking it
    /// from the event makes "what the response says" and "what the bill says"
    /// the same value by construction rather than by two lookups agreeing.
    #[must_use]
    pub fn policy_revision(&self) -> PolicyRevision {
        self.event
            .as_ref()
            .map_or(PolicyRevision::UNSTATED, |event| event.policy_revision)
    }

    /// See [`RequestContext::estimate_remaining`]. This is the one a response
    /// carries: the charge for this request is already in it.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self._concurrency.state().estimate_remaining()
    }

    /// Whether someone has asked for this request to stop since it started.
    ///
    /// The charge is settled and stands in full either way — commit won its
    /// race, and a later cancellation reports `AlreadyCommitted` without
    /// moving funding. This answers a different question: whether anybody is
    /// still waiting for the result. A long kernel may poll it and return
    /// early rather than finish work whose caller has gone, and it will still
    /// be billed for what it started.
    ///
    /// Always `false` for a request that never split: with no handle
    /// outstanding, nobody can have asked.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.funding.cancel_requested()
    }
}

impl<S: UsageSlot, P: CapacityPermit> Drop for Committed<S, P> {
    fn drop(&mut self) {
        // Unwind-safe by construction: the event was built at commit, so this
        // takes no lock that could be poisoned, allocates nothing, and cannot
        // fail. Tollgate does not run the kernel, so the consumer owns the
        // panic boundary that guarantees this runs on the worker — see the
        // type docs.
        if let (Some(event), Some(slot)) = (self.event.take(), self.slot.take()) {
            slot.record(event);
        }
    }
}

/// The engine: a snapshot map plus the pipeline. Generic over the map so the
/// moka and arc-swap candidates compete under identical logic.
pub struct AdmissionEngine<M: SnapshotMap> {
    map: M,
}

impl<M: SnapshotMap> AdmissionEngine<M> {
    /// Build an engine over `map`, which supplies both the snapshots and the
    /// [`AdmissionCounters`] the engine reports into.
    #[must_use]
    pub fn new(map: M) -> Self {
        AdmissionEngine { map }
    }

    /// Control-plane surface: the underlying map, for installs/invalidation.
    #[must_use]
    pub fn map(&self) -> &M {
        &self.map
    }

    /// Observability surface: what this instance has admitted and refused.
    #[must_use]
    pub fn counters(&self) -> &AdmissionCounters {
        self.map.counters()
    }

    /// Resolve and authorize one immutable generation before body decoding.
    pub fn begin(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
    ) -> Result<RequestContext, DenyReason> {
        let locality = Locality::current();
        let outcome = self.begin_inner(principal, required, now, locality);
        if let Err(reason) = &outcome {
            self.map.counters().record_deny_at(reason, locality);
        }
        outcome
    }

    fn begin_inner(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
        locality: Locality,
    ) -> Result<RequestContext, DenyReason> {
        let state = match self.map.get_at(&principal, locality) {
            Some(MapEntry::Present(state)) => state,
            Some(MapEntry::NegativeUntil { .. }) | None => {
                return Err(DenyReason::UnknownPrincipal);
            }
        };
        state.snapshot.admit(now, required)?;
        Ok(RequestContext {
            state: Some(state),
            locality,
        })
    }
}

#[derive(Debug)]
struct Priced {
    concurrency: ConcurrencyGuard,
    reservation: Reservation,
    quote: CostQuote,
}

fn compile_workload<O: OpIndex>(
    snapshot: &AccountSnapshot,
    workload: &[(O, u64)],
    now: Timestamp,
) -> Result<CostQuote, DenyReason> {
    if now >= snapshot.valid_until {
        return Err(DenyReason::SnapshotExpired);
    }
    let (quote, items, required) =
        snapshot
            .cost_table
            .quote_workload(workload)
            .map_err(|error| match error {
                QuoteError::EmptyWorkload => DenyReason::EmptyWorkload,
                QuoteError::UnknownOperation { .. } => DenyReason::UnpricedOperation,
                QuoteError::Overflow => DenyReason::CostOverflow,
            })?;
    // Work permission, as distinct from the route permission `begin` already
    // checked. It is only knowable here: which classes a request touches is a
    // property of its decoded workload, not of the route it arrived on. The
    // bits were folded by the quote's own pass, so this consults neither the
    // workload nor the map a second time.
    if !snapshot.permissions.contains_all(required) {
        return Err(DenyReason::MissingPermission);
    }
    if items > snapshot.limits.max_items_per_request() {
        return Err(DenyReason::RequestTooLarge {
            max_items: snapshot.limits.max_items_per_request(),
        });
    }
    Ok(quote)
}

fn admit_priced(
    state: Arc<AccountAdmissionState>,
    locality: Locality,
    quote: CostQuote,
    now: Timestamp,
) -> Result<Priced, DenyReason> {
    // Account-wide policy has one mutable authority shared by every principal.
    // Load it once so rate and concurrency decisions are from one generation
    // even if the control plane publishes concurrently.
    let account = state.limiter.load();
    let rate = account.rate();

    // 4. Request-count token. Every priced request costs exactly one,
    //    independently of its cost-weighted charge.
    if let Some(requests) = rate.requests() {
        match requests.check_n_at(NonZeroU32::MIN, locality) {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                return deny_priced(&state, DenyReason::RequestRateLimited, locality);
            }
        }
    }

    // 5. Weighted rate token. Cost-weighted: heavy requests draw down the
    //    bucket proportionally. A disabled bucket performs no governor
    //    check and the carried legacy pair remains rollout data only.
    //
    //    A weight beyond the bucket's whole burst can never pass, however
    //    long the caller waits — that is a schedule whose batch cap admits
    //    a quote its burst cannot hold, and it is reported as such rather
    //    than as throttling (GL-40). Deciding it here, in full width against
    //    the configured burst, is what keeps the u32 conversion below
    //    honest: the weight is known to fit the bucket before it is
    //    narrowed, so narrowing can no longer disguise an unadmittable
    //    request as an ordinary empty bucket.
    if let Some(weighted_rate) = rate.policy().weighted_rate() {
        if quote.total.get() > weighted_rate.burst_units() {
            return deny_priced(
                &state,
                DenyReason::UnpriceableUnderLimits {
                    weight: quote.total,
                    burst_units: CostUnits(weighted_rate.burst_units()),
                },
                locality,
            );
        }
        let weight = u32::try_from(quote.total.get()).unwrap_or(u32::MAX);
        match NonZeroU32::new(weight) {
            // Zero-cost requests draw no token; the minimum-charge floor
            // makes this unreachable for any real table.
            None => {}
            Some(n) => match rate
                .weighted()
                .expect("configured weighted rate has an installed bucket")
                .check_n_at(n, locality)
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return deny_priced(&state, DenyReason::RateLimited, locality),
                // Unreachable: the check above clears the quote against
                // the whole burst, and split buckets are sized so every
                // legitimate maximum quote fits at least one shard.
                Err(_) => {
                    return deny_priced(
                        &state,
                        DenyReason::UnpriceableUnderLimits {
                            weight: quote.total,
                            burst_units: CostUnits(weighted_rate.burst_units()),
                        },
                        locality,
                    );
                }
            },
        }
    }

    // 6. Concurrency: principal first, then account. The RAII guard undoes
    //    either acquisition on every later refusal and remains held by
    //    `Admitted` for the caller's full in-flight interval.
    let concurrency = match AccountAdmissionState::acquire_concurrency(
        state,
        account.max_concurrent_requests(),
        locality,
    ) {
        Ok(concurrency) => concurrency,
        Err((reason, state)) => return deny_priced(&state, reason, locality),
    };

    // 7. Quota: debit the lease and open the state machine. Note the
    //    deliberate ordering — a lease-denied request has still consumed
    //    its rate token, because it did arrive and was priced.
    //
    //    Under `EnforcementMode::Elastic` a lease that cannot fund the
    //    quote is not the end of the request. Three conditions say the
    //    same thing — this instance holds no capacity for these units —
    //    and they are the only three the mode intercepts:
    //
    //    - `LeaseUnavailable`: no lease at all, the cold-start and
    //      control-plane-outage case;
    //    - `LeaseExpired`: the lease's local window lapsed before refill
    //      replaced it;
    //    - `LeaseExhausted`: the lease is live and empty.
    //
    //    Nothing above this point is intercepted, and that is the whole
    //    safety argument. An unknown principal, a suspended or closed
    //    account, a stale snapshot, a missing permission, an oversized
    //    batch, an unpriced operation, a cost overflow, an empty rate
    //    bucket — every one of those still denies with zero charge under
    //    either mode, because none of them is a statement about funding
    //    (INVARIANTS.md GL-1, GL-5).
    let reservation = match reserve_from_lease(concurrency.state(), quote.total, now, locality) {
        Ok(reservation) => reservation,
        Err(denied) => match reserve_from_overage(concurrency.state(), quote.total, denied) {
            Ok(reservation) => reservation,
            Err(reason) => {
                // Refundable pending occupancy and a commit in progress can
                // recover locally even when the central balance is spent.
                // Evidence is an upper bound on remaining funding, so it can
                // only refuse a quote no refill could fund; a quote within it
                // keeps the lease refusal's transient advice.
                let reason = if matches!(
                    reason,
                    DenyReason::LeaseUnavailable
                        | DenyReason::LeaseExpired
                        | DenyReason::LeaseExhausted { .. }
                        | DenyReason::OverageCapExhausted { .. }
                ) {
                    match concurrency.state().lease.funding_evidence(now) {
                        Some(remaining) if remaining.is_zero() => DenyReason::BalanceExhausted,
                        Some(remaining) if quote.total > remaining => {
                            DenyReason::BalanceInsufficient { remaining }
                        }
                        _ => reason,
                    }
                } else {
                    reason
                };
                concurrency
                    .state()
                    .counters
                    .record_deny_at(&reason, locality);
                return Err(reason);
            }
        },
    };

    if reservation.admitted_as_overage() {
        concurrency
            .state()
            .counters
            .record_admit_overage_at(quote.total, locality);
    } else {
        concurrency
            .state()
            .counters
            .record_admit_at(quote.total, locality);
    }

    Ok(Priced {
        concurrency,
        reservation,
        quote,
    })
}

#[inline]
fn deny_priced(
    state: &AccountAdmissionState,
    reason: DenyReason,
    locality: Locality,
) -> Result<Priced, DenyReason> {
    state.counters.record_deny_at(&reason, locality);
    Err(reason)
}

#[inline]
fn reserve_from_lease(
    state: &AccountAdmissionState,
    units: CostUnits,
    now: Timestamp,
    locality: Locality,
) -> Result<Reservation, DenyReason> {
    let lease = state
        .lease
        .load_at(locality)
        .ok_or(DenyReason::LeaseUnavailable)?;
    // Moved, not borrowed: `load_at` already owns this handle, and the
    // reservation is where it lives from here (GL-79).
    Reservation::reserve_at_locality(lease, units, now, locality)
}

/// The elastic half: extend unfunded credit, or return the lease's own
/// refusal untouched.
///
/// `denied` is carried through rather than re-derived, so a `Strict`
/// account reports exactly the reason it reported before this branch
/// existed — including `LeaseExhausted`'s `remaining`, which a second
/// lookup could not reproduce.
#[inline]
fn reserve_from_overage(
    state: &AccountAdmissionState,
    units: CostUnits,
    denied: DenyReason,
) -> Result<Reservation, DenyReason> {
    let Some(overage_cap) = state.snapshot.enforcement_mode.overage_cap() else {
        return Err(denied);
    };
    if !matches!(
        denied,
        DenyReason::LeaseUnavailable | DenyReason::LeaseExpired | DenyReason::LeaseExhausted { .. }
    ) {
        return Err(denied);
    }
    // No refill signal is raised here, and the reason is worth recording
    // because the opposite looks necessary. `LocalLease::try_debit`
    // announces the low-water *crossing*, which for an exhausted lease
    // already happened on the debit that drained it — before any request
    // reached this branch. For an absent or expired lease there is no
    // `RefillSignal` to raise at all, and recovery is the lease manager's
    // poll, exactly as it is under `Strict`. Elastic mode therefore does
    // not suppress refill; it runs alongside a refill already in flight.
    Reservation::reserve_overage(state.lease.overage(), units, overage_cap)
}

#[cfg(test)]
mod tests {
    #[test]
    fn emitted_usage_pins_the_key_and_cancelled_work_emits_nothing() {
        #[derive(Debug)]
        struct Capture(std::sync::mpsc::Sender<UsageEvent>);
        impl UsageSlot for Capture {
            fn record(self, event: UsageEvent) {
                self.0.send(event).unwrap();
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(10000)));
        let mut original = (*snapshot(AccountStatus::Active)).clone();
        original.key_id = Some(tollgate_core::KeyId(1));
        engine
            .map()
            .install(Principal(1), Arc::new(original.clone()), slot.clone())
            .unwrap();
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .unwrap();
        original.generation = Generation(2);
        original.key_id = Some(tollgate_core::KeyId(2));
        engine
            .map()
            .install(Principal(1), Arc::new(original), slot)
            .unwrap();
        let committed = context
            .admit(&[(Op::Price, 1)], Capture(tx.clone()), t(0))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(105), t(1))
            .unwrap();
        assert!(
            rx.try_recv().is_err(),
            "the execution guard still owns emission"
        );
        drop(committed);
        let event = rx.try_recv().unwrap();
        assert_eq!(event.key_id, Some(tollgate_core::KeyId(1)));
        assert_eq!(event.occurred_at, t(1));
        let pending = engine
            .begin(Principal(1), PermissionBits::bit(0), t(2))
            .unwrap()
            .admit(&[(Op::Price, 1)], Capture(tx), t(2))
            .unwrap();
        pending.cancel();
        assert!(rx.try_recv().is_err());
    }

    use super::*;
    use crate::capacity::{ExecutionCapacityGate, ExecutionCapacityMode, ExecutionPermit, NoGate};
    use crate::maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
    use crate::state::LeaseSlot;
    use tollgate_core::EnforcementMode;
    use tollgate_core::{
        AccountId, AccountStatus, BudgetView, CancelOutcome, CapacityClass, CostTable, CostUnits,
        DiscardedUsage, DiscardedUsageSlot, FencingToken, Generation, LeaseGrant, LeaseId,
        LocalLease, LocalSharding, PublishableSnapshot, ResolvedLimits, Retry,
    };

    #[derive(Clone, Copy)]
    enum Op {
        Price,
        Unpriced,
    }
    impl OpIndex for Op {
        fn index(&self) -> usize {
            *self as usize
        }
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn snapshot(status: AccountStatus) -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                status,
                t(10_000),
                PermissionBits::bit(0),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .weight(&Op::Price, CostUnits(1))
                        .build(),
                ),
            )
            .build(),
        )
    }

    /// The same fixture carrying a stated policy revision (GL-94).
    fn snapshot_with_revision(revision: PolicyRevision) -> Arc<AccountSnapshot> {
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.policy_revision = revision;
        Arc::new(snapshot)
    }

    /// A snapshot granting the route bit but not the work bit a class needs.
    fn snapshot_without_work_permission() -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                t(10_000),
                // Route permission only. The class below wants bit 5.
                PermissionBits::bit(0),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .class(&Op::Price, CostUnits(1), PermissionBits::bit(5))
                        .build(),
                ),
            )
            .build(),
        )
    }

    fn lease(units: u64) -> Arc<LocalLease> {
        lease_until(units, t(10_000))
    }

    fn lease_until(units: u64, expires_at: Timestamp) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at,
            },
            CostUnits::ZERO,
        ))
    }

    fn engine_with(
        status: AccountStatus,
        lease_units: Option<u64>,
    ) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        if let Some(units) = lease_units {
            drop(slot.replace(lease(units)));
        }
        engine
            .map()
            .install(Principal(1), snapshot(status), slot)
            .unwrap();
        engine
    }

    /// An engine whose account is elastic with `overage_cap`, and whose slot
    /// holds `lease_units` if any. `None` is the cold-start / lost-lease
    /// state; `Some(0)` is a live but empty lease.
    fn elastic_engine(
        overage_cap: u64,
        lease_units: Option<u64>,
    ) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        if let Some(units) = lease_units {
            drop(slot.replace(lease(units)));
        }
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(overage_cap),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();
        engine
    }

    /// An account's concurrency-gauge races reach its slot's contention total
    /// (GL-139): eight threads admitting for one account lose gauge exchanges,
    /// and the slot counts more than the lease alone recorded.
    #[test]
    fn concurrency_gauge_contention_reaches_the_account_total() {
        // No rate limit, so every admission reaches the gauges.
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(u64::MAX / 2)));
        let mut unlimited = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        unlimited.limits = ResolvedLimits::new(64);
        engine
            .map()
            .install(Principal(1), Arc::new(unlimited), slot)
            .unwrap();
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        let slot = Arc::clone(&state.lease);
        let lease = slot.load().unwrap();
        let gauges = || slot.contended_exchanges() - lease.contended_debits();
        for _ in 0..50 {
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| {
                        for _ in 0..20_000 {
                            let _ = engine.admit_one(request(1), t(0)).unwrap().cancel();
                        }
                    });
                }
            });
            if gauges() > 0 {
                break;
            }
        }
        assert!(
            gauges() > 0,
            "eight admitting threads never lost a gauge race"
        );
        assert_eq!(
            state.account_concurrency_in_flight(),
            0,
            "every permit released"
        );
    }

    /// An uncontended admission records no gauge contention.
    #[cfg(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "lse")
    ))]
    #[test]
    fn an_uncontended_admission_records_no_contention() {
        let engine = engine_with(AccountStatus::Active, Some(1_000_000));
        for _ in 0..100 {
            let _ = engine.admit_one(request(1), t(0)).unwrap().cancel();
        }
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        assert_eq!(state.lease.contended_exchanges(), 0);
    }

    #[test]
    fn authoritative_exhaustion_classifies_only_failed_local_funding() {
        for units in [None, Some(0), Some(100)] {
            let engine = engine_with(AccountStatus::Active, units);
            let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
                panic!()
            };
            state.lease.funding_attempt().shortfall(
                tollgate_core::BalanceExhaustion {
                    period_end: Some(t(100)),
                }
                .into(),
            );
            let outcome = engine.admit_one(request(1), t(0));
            if units == Some(100) {
                assert!(outcome.is_ok(), "usable local credit takes precedence");
            } else {
                assert_eq!(outcome.unwrap_err(), DenyReason::BalanceExhausted);
                assert_eq!(
                    state.counters.snapshot().denials[DenyReason::BalanceExhausted.index()],
                    1
                );
                assert_eq!(
                    engine.admit_one(request(1), t(100)).unwrap_err().retry(),
                    Retry::Transient
                );
            }
        }
        let engine = engine_with(AccountStatus::Active, None);
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
    }

    #[test]
    fn exhausted_evidence_survives_local_expiry_but_not_its_period_end() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(0, t(5))));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot.clone())
            .unwrap();
        slot.funding_attempt().shortfall(
            tollgate_core::BalanceExhaustion {
                period_end: Some(t(100)),
            }
            .into(),
        );
        for now in [t(0), t(5), t(99)] {
            assert_eq!(
                engine.admit_one(request(1), now).unwrap_err(),
                DenyReason::BalanceExhausted
            );
        }
        drop(slot.take());
        assert_eq!(
            engine.admit_one(request(1), t(99)).unwrap_err(),
            DenyReason::BalanceExhausted
        );
        assert_eq!(
            engine.admit_one(request(1), t(100)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
    }

    #[test]
    fn exhaustion_is_shared_by_account_and_isolated_from_other_accounts() {
        for shards in [1, 8] {
            let sharding = LocalSharding::new(std::num::NonZeroUsize::new(shards).unwrap());
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
            let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
            engine
                .map()
                .install(Principal(1), snapshot(AccountStatus::Active), slot.clone())
                .unwrap();
            slot.funding_attempt()
                .shortfall(tollgate_core::BalanceExhaustion { period_end: None }.into());
            engine
                .map()
                .install(Principal(2), snapshot(AccountStatus::Active), slot)
                .unwrap();
            let mut unrelated = (*snapshot(AccountStatus::Active)).clone();
            unrelated.account_id = AccountId(2);
            engine
                .map()
                .install(
                    Principal(3),
                    Arc::new(unrelated),
                    LeaseSlot::with_sharding(AccountId(2), sharding),
                )
                .unwrap();
            for principal in [Principal(1), Principal(2)] {
                assert_eq!(
                    engine
                        .admit_one(
                            TestRequest {
                                principal,
                                ..request(1)
                            },
                            t(0)
                        )
                        .unwrap_err(),
                    DenyReason::BalanceExhausted
                );
            }
            assert_eq!(
                engine
                    .admit_one(
                        TestRequest {
                            principal: Principal(3),
                            ..request(1)
                        },
                        t(0)
                    )
                    .unwrap_err(),
                DenyReason::LeaseUnavailable
            );
        }
    }

    #[test]
    fn accepted_funding_changes_from_independently_versioned_principals_clear_exhaustion() {
        for shards in [1, 8] {
            let sharding = LocalSharding::new(std::num::NonZeroUsize::new(shards).unwrap());
            for change_mode in [false, true] {
                let maps: [Arc<dyn SnapshotMap>; 2] = [
                    Arc::new(ArcSwapSnapshotMap::with_sharding(sharding)),
                    Arc::new(MokaSnapshotMap::with_sharding(8, sharding)),
                ];
                for map in maps {
                    let engine = AdmissionEngine::new(map);
                    let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
                    let mut next = (*snapshot(AccountStatus::Active)).clone();
                    for (principal, generation) in [(Principal(1), 7), (Principal(2), 2)] {
                        next.generation = Generation(generation);
                        engine
                            .map()
                            .install(principal, Arc::new(next.clone()), slot.clone())
                            .unwrap();
                    }
                    let evidence = tollgate_core::BalanceExhaustion { period_end: None };
                    slot.funding_attempt().shortfall(evidence.into());
                    assert_eq!(
                        engine.admit_one(request(1), t(0)).unwrap_err(),
                        DenyReason::BalanceExhausted
                    );
                    let late = slot.funding_attempt();
                    next.generation = Generation(3);
                    if change_mode {
                        next.enforcement_mode = EnforcementMode::Elastic {
                            overage_cap: CostUnits(100),
                        };
                    } else {
                        next.budget = Some(BudgetView {
                            balance_at_publish: CostUnits(100),
                            period_end: None,
                        });
                    }
                    engine
                        .map()
                        .install(Principal(2), Arc::new(next.clone()), slot.clone())
                        .unwrap();
                    assert!(
                        !slot
                            .funding_evidence(t(0))
                            .is_some_and(|remaining| remaining.is_zero())
                    );
                    late.shortfall(evidence.into());
                    // P1 still uses strict mode, but P2's accepted change clears
                    // the evidence for the whole account, including late replies.
                    assert_eq!(
                        engine.admit_one(request(1), t(0)).unwrap_err(),
                        DenyReason::LeaseUnavailable
                    );
                    slot.funding_attempt().shortfall(evidence.into());
                    next.generation = Generation(4);
                    engine
                        .map()
                        .install(Principal(2), Arc::new(next), slot.clone())
                        .unwrap();
                    assert!(
                        slot.funding_evidence(t(0))
                            .is_some_and(|remaining| remaining.is_zero()),
                        "unchanged funding"
                    );
                    for generation in [3, 4] {
                        let mut replay = (*snapshot(AccountStatus::Active)).clone();
                        replay.generation = Generation(generation);
                        engine
                            .map()
                            .install(Principal(2), Arc::new(replay), slot.clone())
                            .unwrap();
                        assert!(
                            slot.funding_evidence(t(0))
                                .is_some_and(|remaining| remaining.is_zero()),
                            "rejected replay"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn central_exhaustion_does_not_hide_refundable_elastic_capacity() {
        let engine = elastic_engine(51, None);
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        state
            .lease
            .funding_attempt()
            .shortfall(tollgate_core::BalanceExhaustion { period_end: None }.into());
        let pending = engine.admit_one(request(1), t(0)).unwrap();
        assert!(matches!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::OverageCapTemporarilyExhausted { .. }
        ));
        drop(pending);
        let ready = engine
            .admit_one(request(1), t(0))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap();
        drop(ready.commit(RequestId(1), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::BalanceExhausted
        );
    }

    fn shortfall(remaining: u64) -> tollgate_core::BalanceShortfall {
        tollgate_core::BalanceShortfall {
            remaining: CostUnits(remaining),
            period_end: Some(t(100)),
        }
    }

    /// GL-130: an account with funding, but less than a quote, refuses that
    /// quote as not retryable and keeps admitting quotes that fit. A quote
    /// here is 50 fixed + 1 per item.
    #[test]
    fn insufficient_funding_refuses_only_quotes_above_evidence() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        // No evidence: an unfundable quote is still only a lease refusal.
        assert!(matches!(
            engine.admit_one(request(20), t(0)).unwrap_err(),
            DenyReason::LeaseExhausted { .. }
        ));
        state.lease.funding_attempt().shortfall(shortfall(60));
        let pending = engine.admit_one(request(1), t(0));
        assert!(pending.is_ok(), "usable local credit takes precedence");
        drop(pending);
        let denied = engine.admit_one(request(20), t(0)).unwrap_err();
        assert_eq!(
            denied,
            DenyReason::BalanceInsufficient {
                remaining: CostUnits(60)
            }
        );
        assert_eq!(denied.retry(), Retry::Never);
        assert_eq!(
            state.counters.snapshot().denials[denied.index()],
            1,
            "the refusal is tallied in its own slot"
        );
        // With the lease holding back 51 units, a quote of exactly the
        // evidenced remaining is refused by the lease, and it is a lease gap:
        // the boundary is inclusive, and so is any quote below it.
        let _held = engine.admit_one(request(1), t(0)).unwrap();
        for items in [10, 5] {
            assert!(matches!(
                engine.admit_one(request(items), t(0)).unwrap_err(),
                DenyReason::LeaseExhausted { .. }
            ));
        }
        assert_eq!(
            engine.admit_one(request(11), t(0)).unwrap_err(),
            DenyReason::BalanceInsufficient {
                remaining: CostUnits(60)
            }
        );
        // The stored period end bounds the evidence.
        assert!(matches!(
            engine.admit_one(request(20), t(100)).unwrap_err(),
            DenyReason::LeaseExhausted { .. }
        ));
    }

    #[test]
    fn central_shortfall_does_not_hide_elastic_capacity() {
        let engine = elastic_engine(51, None);
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        state.lease.funding_attempt().shortfall(shortfall(10));
        let overage = engine.admit_one(request(1), t(0)).unwrap();
        assert!(overage.funding.reservation().admitted_as_overage());
    }

    #[test]
    fn grant_evidence_is_published_with_its_lease() {
        let engine = engine_with(AccountStatus::Active, Some(0));
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        let slot = &state.lease;
        slot.funding_attempt().shortfall(shortfall(0));
        let mut grant = *lease(40).grant();
        grant.fencing_token = FencingToken(2);
        let old = slot.funding_attempt().granted(
            Arc::new(LocalLease::new(grant, CostUnits::ZERO)),
            Some(shortfall(40)),
        );
        drop(old);
        assert_eq!(
            slot.funding_evidence(t(0)),
            Some(CostUnits(40)),
            "the grant cleared older exhaustion and kept its own evidence"
        );
        assert_eq!(
            engine.admit_one(request(20), t(0)).unwrap_err(),
            DenyReason::BalanceInsufficient {
                remaining: CostUnits(40)
            }
        );

        // A grant without evidence (an older server) clears and publishes
        // nothing.
        grant.fencing_token = FencingToken(3);
        drop(
            slot.funding_attempt()
                .granted(Arc::new(LocalLease::new(grant, CostUnits::ZERO)), None),
        );
        assert_eq!(slot.funding_evidence(t(0)), None);

        // A funding change accepted while the call was out wins over the
        // grant's older ledger reading; the grant itself still installs.
        let late = slot.funding_attempt();
        let mut next = (*snapshot(AccountStatus::Active)).clone();
        next.generation = Generation(2);
        next.budget = Some(BudgetView {
            balance_at_publish: CostUnits(1_000),
            period_end: None,
        });
        engine
            .map()
            .install(Principal(1), Arc::new(next), slot.clone())
            .unwrap();
        grant.fencing_token = FencingToken(4);
        drop(late.granted(
            Arc::new(LocalLease::new(grant, CostUnits::ZERO)),
            Some(shortfall(40)),
        ));
        assert_eq!(slot.funding_evidence(t(0)), None);
        assert_eq!(
            slot.load().map(|lease| lease.grant().fencing_token),
            Some(FencingToken(4))
        );
    }

    #[test]
    fn funding_publication_invalidates_late_exhaustion_responses() {
        let engine = engine_with(AccountStatus::Active, Some(0));
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!()
        };
        let slot = &state.lease;
        let evidence = tollgate_core::BalanceExhaustion { period_end: None };
        slot.funding_attempt().shortfall(evidence.into());
        let late = slot.funding_attempt();
        // Restoring the same capability does not manufacture new funding.
        let old = slot.take().unwrap();
        drop(slot.replace(old));
        assert!(
            slot.funding_evidence(t(0))
                .is_some_and(|remaining| remaining.is_zero())
        );
        let mut grant = *lease(100).grant();
        grant.fencing_token = FencingToken(2);
        drop(slot.replace(Arc::new(LocalLease::new(grant, CostUnits::ZERO))));
        late.shortfall(evidence.into());
        assert!(
            !slot
                .funding_evidence(t(0))
                .is_some_and(|remaining| remaining.is_zero()),
            "late refusal cannot undo a grant"
        );
        slot.funding_attempt().shortfall(evidence.into());
        let late = slot.funding_attempt();
        let mut next = (*snapshot(AccountStatus::Active)).clone();
        next.generation = Generation(2);
        next.budget = Some(BudgetView {
            balance_at_publish: CostUnits(100),
            period_end: Some(t(100)),
        });
        engine
            .map()
            .install(Principal(1), Arc::new(next.clone()), slot.clone())
            .unwrap();
        late.shortfall(evidence.into());
        assert!(
            !slot
                .funding_evidence(t(0))
                .is_some_and(|remaining| remaining.is_zero()),
            "late refusal cannot undo new funding policy"
        );
        slot.funding_attempt().shortfall(evidence.into());
        // An unrelated generation refresh must not erase known exhaustion.
        next.generation = Generation(3);
        engine
            .map()
            .install(Principal(1), Arc::new(next), slot.clone())
            .unwrap();
        assert!(
            slot.funding_evidence(t(0))
                .is_some_and(|remaining| remaining.is_zero())
        );
        let mut stale = (*snapshot(AccountStatus::Active)).clone();
        stale.generation = Generation(2);
        stale.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(100),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(stale), slot.clone())
            .unwrap();
        assert!(
            slot.funding_evidence(t(0))
                .is_some_and(|remaining| remaining.is_zero()),
            "a rejected publication cannot invalidate evidence"
        );
    }

    /// The three lease conditions elastic mode intercepts, each of which says
    /// "this instance holds no capacity for these units" and nothing else. A
    /// quote here is 50 fixed + 1 per item.
    #[test]
    fn elastic_admits_past_every_lease_condition_a_strict_account_denies() {
        for (name, lease_units, expected_strict) in [
            ("no lease at all", None, DenyReason::LeaseUnavailable),
            (
                "a live but empty lease",
                Some(0),
                DenyReason::LeaseExhausted {
                    remaining: CostUnits::ZERO,
                },
            ),
        ] {
            let strict = engine_with(AccountStatus::Active, lease_units);
            assert_eq!(
                strict.admit_one(request(1), t(0)).unwrap_err(),
                expected_strict,
                "strict must still deny with {name}"
            );

            let elastic = elastic_engine(1_000, lease_units);
            let admitted = elastic
                .admit_one(request(1), t(0))
                .unwrap_or_else(|denied| panic!("elastic denied {name}: {denied}"));
            assert!(admitted.funding.reservation().admitted_as_overage());
            assert_eq!(admitted.quote.total, CostUnits(51));
        }
    }

    /// The expired-lease case needs its own clock, so it is separated from the
    /// loop above rather than folded in with a synthetic timestamp.
    #[test]
    fn elastic_admits_past_an_expired_lease() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(1_000, t(5))));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1_000),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();

        let admitted = engine.admit_one(request(1), t(10)).expect("elastic admits");
        assert!(admitted.funding.reservation().admitted_as_overage());
    }

    /// The safety argument in one test: elasticity is a statement about
    /// *funding*, so every refusal that is not about funding still denies with
    /// zero charge and no overage claimed.
    #[test]
    fn elastic_does_not_relax_any_refusal_that_is_not_about_funding() {
        let overage_spent = |engine: &AdmissionEngine<ArcSwapSnapshotMap>| {
            let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
                panic!("principal present");
            };
            state.lease.overage().spent()
        };

        // Suspended and closed accounts: step 2, before the quota step.
        for status in [AccountStatus::Suspended, AccountStatus::Closed] {
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
            let slot = LeaseSlot::for_account(AccountId(1));
            let mut snapshot = AccountSnapshot::clone(&snapshot(status));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(1_000),
            };
            engine
                .map()
                .install(Principal(1), Arc::new(snapshot), slot)
                .unwrap();
            assert!(engine.admit_one(request(1), t(0)).is_err(), "{status:?}");
            assert_eq!(overage_spent(&engine), CostUnits::ZERO);
        }

        // A stale snapshot, an oversized batch, and an unpriced operation.
        let engine = elastic_engine(1_000, None);
        assert_eq!(
            engine.admit_one(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        assert_eq!(
            engine.admit_one(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
                        op: &Op::Unpriced,
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::UnpricedOperation
        );
        assert_eq!(
            overage_spent(&engine),
            CostUnits::ZERO,
            "no refusal above the quota step may claim credit"
        );

        // An unknown principal never reaches an account at all.
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
                        principal: Principal(99),
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    /// The cap bounds the account, not the request: repeated admissions
    /// accumulate against it and the refusal reports the exhausted local
    /// overage source without claiming that central funding is exhausted.
    #[test]
    fn elastic_refuses_once_the_local_overage_cap_is_spent() {
        // Two quotes of 51 fit in 102; the third does not.
        let engine = elastic_engine(102, Some(0));
        for _ in 0..2 {
            let admitted = engine.admit_one(request(1), t(0)).expect("within the cap");
            admitted
                .funding
                .reservation()
                .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
                .expect("commit");
        }
        let denied = engine.admit_one(request(1), t(0)).unwrap_err();
        assert_eq!(
            denied,
            DenyReason::OverageCapExhausted {
                spent: CostUnits(102),
                overage_cap: CostUnits(102),
            }
        );
        assert_eq!(denied.retry(), Retry::Transient);
    }

    /// Exhausting the local overage allowance does not establish that the
    /// account itself needs funding: an ordinary background grant can make
    /// the unchanged request admissible without changing the deposit or cap.
    #[test]
    fn committed_overage_exhaustion_remains_retryable_after_lease_refill() {
        for (name, initial_lease, now) in [
            ("unavailable", None, t(0)),
            ("exhausted", Some(lease(0)), t(0)),
            ("expired", Some(lease_until(1_000, t(5))), t(10)),
        ] {
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
            let slot = LeaseSlot::for_account(AccountId(1));
            if let Some(initial_lease) = initial_lease {
                drop(slot.replace(initial_lease));
            }
            let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(51),
            };
            engine
                .map()
                .install(Principal(1), Arc::new(snapshot), Arc::clone(&slot))
                .unwrap();

            engine
                .admit_one(request(1), now)
                .unwrap_or_else(|denied| panic!("the cap must cover {name}: {denied}"))
                .funding
                .reservation()
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .expect("commit the local overage");

            let denied = engine.admit_one(request(1), now).unwrap_err();
            assert!(
                matches!(denied, DenyReason::OverageCapExhausted { .. }),
                "the stable local cap reason must survive {name}"
            );
            assert_eq!(denied.retry(), Retry::Transient, "lease was {name}");

            drop(slot.replace(lease(51)));
            let admitted = engine
                .admit_one(request(1), now)
                .unwrap_or_else(|denied| panic!("a refill must recover {name}: {denied}"));
            assert!(!admitted.funding.reservation().admitted_as_overage());
        }
    }

    /// The two mechanisms this merge put in the same slot had never met: an
    /// account can be elastic *and* sharded, and the slot now holds one
    /// unsharded overage counter beside N per-locality lease views.
    ///
    /// The failure this pins is the plausible one, and it only shows itself
    /// across threads. Had the counter been partitioned the way the grant is —
    /// one per view, the layout every neighbour in this struct uses — each
    /// locality would have carried its own full cap, and an eight-shard
    /// instance would extend `8 x overage_cap` while every doc, metric, and
    /// INVARIANTS GL-1 still said `overage_cap`. A single-threaded test cannot
    /// see that: one thread has one locality, and one locality's private
    /// counter refuses at the cap exactly like a shared one. So the requests
    /// have to arrive from different threads, which is where `Locality`
    /// assigns different values.
    #[test]
    fn a_sharded_slot_does_not_multiply_the_overage_cap() {
        const THREADS: usize = 8;
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(THREADS).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(0)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(102),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();
        let engine = Arc::new(engine);

        // Two quotes of 51 fit in a cap of 102. Eight threads ask; the answer
        // is two whatever order they arrive in, because the cap comparison
        // lives inside the counter's compare-exchange.
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let engine = Arc::clone(&engine);
                    scope.spawn(move || match engine.admit_one(request(1), t(0)) {
                        Ok(admitted) => {
                            assert!(admitted.funding.reservation().admitted_as_overage());
                            admitted
                                .funding
                                .reservation()
                                .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
                                .expect("commit");
                            1
                        }
                        Err(denied) => {
                            match denied {
                                DenyReason::OverageCapExhausted { spent, overage_cap }
                                | DenyReason::OverageCapTemporarilyExhausted {
                                    spent,
                                    overage_cap,
                                }
                                | DenyReason::OverageCommitInProgress { spent, overage_cap } => {
                                    assert_eq!(spent, CostUnits(102));
                                    assert_eq!(overage_cap, CostUnits(102));
                                }
                                other => panic!("unexpected cap refusal: {other}"),
                            }
                            0
                        }
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("no thread panicked"))
                .sum::<usize>()
        });

        assert_eq!(
            admitted, 2,
            "the cap is per account per instance, not per locality"
        );
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits(102));
        assert!(matches!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::OverageCapExhausted { .. }
        ));
    }

    /// The other half of the same interaction: a sharded slot whose lease can
    /// still fund the quote must debit that lease's local view, not reach for
    /// overage. A `load_at` that missed its view would look exactly like an
    /// unavailable lease and silently start billing overage instead.
    #[test]
    fn a_sharded_slot_still_prefers_the_lease_that_can_fund_the_quote() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(1_000)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1_000),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();

        let admitted = engine
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
        assert!(!admitted.funding.reservation().admitted_as_overage());
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits::ZERO);
    }

    /// A pending overage can temporarily fill the cap, but its denial must say
    /// that cancellation can recover: no funding or allowance change is
    /// required. Cancelling charges zero and returns the credit.
    #[test]
    fn pending_overage_saturation_is_transient_until_cancel() {
        let engine = elastic_engine(51, Some(0));
        let admitted = engine.admit_one(request(1), t(0)).expect("within the cap");
        let denied = engine.admit_one(request(1), t(0)).unwrap_err();
        assert_eq!(
            denied,
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            }
        );
        assert_eq!(denied.retry(), Retry::Transient);
        assert_eq!(
            admitted.funding.reservation().cancel(),
            CancelOutcome::ZeroCharged
        );
        engine
            .admit_one(request(1), t(0))
            .expect("the cancelled credit is available again");
    }

    /// A lease that can still fund the quote is used, and the overage counter
    /// stays untouched: elastic mode is a fallback, never a preference.
    #[test]
    fn elastic_prefers_the_lease_while_it_can_fund_the_quote() {
        let engine = elastic_engine(1_000, Some(1_000));
        let admitted = engine
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
        assert!(!admitted.funding.reservation().admitted_as_overage());
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits::ZERO);
        assert_eq!(state.lease.load().unwrap().remaining(), CostUnits(949));
    }

    /// INVARIANTS.md GL-20: an overage admission is counted under `admitted`
    /// like any other, *and* under its own qualifier. Readers of `admitted`
    /// must not have to add two numbers to get the total.
    #[test]
    fn an_overage_admission_is_counted_twice_over_and_a_refusal_once() {
        let engine = elastic_engine(51, Some(0));
        // Held, not dropped: an unresolved reservation releases its credit on
        // drop, so a test that lets one fall out of scope would be measuring
        // the refund rather than the cap.
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");
        engine
            .admit_one(request(1), t(0))
            .expect_err("beyond the cap");

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.units_admitted, 51);
        assert_eq!(counters.admitted_overage, 1);
        assert_eq!(counters.units_admitted_overage, 51);
        assert_eq!(counters.denied(), 1);
        assert_eq!(
            counters.denials[DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits::ZERO,
                overage_cap: CostUnits::ZERO,
            }
            .index()],
            1
        );

        // A lease-funded admission moves only the unqualified counters.
        let strict = engine_with(AccountStatus::Active, Some(1_000));
        strict
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
        let counters = strict.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.admitted_overage, 0);
        assert_eq!(counters.units_admitted_overage, 0);
    }

    /// Every principal of an account draws from one cap. Two API keys must
    /// not double an account's credit — the same rule the rate limiter follows
    /// for the same reason.
    #[test]
    fn every_principal_of_an_account_shares_one_cap() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(0)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(51),
        };
        let snapshot = Arc::new(snapshot);
        for principal in [Principal(1), Principal(2)] {
            engine
                .map()
                .install(principal, Arc::clone(&snapshot), Arc::clone(&slot))
                .unwrap();
        }

        let _held = engine
            .admit_one(request(1), t(0))
            .expect("the first key spends");
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
                        principal: Principal(2),
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            },
            "a second key must not double the account's credit"
        );
    }

    /// Divergent caps across an account's principals bound the account by the
    /// *largest* of them, never their sum. This is the property that makes the
    /// mode safe without an account-level operator action: one shared counter
    /// means N credentials cannot multiply an account's credit the way N
    /// per-principal limiters would have multiplied its rate (review finding
    /// GL-4). What divergence costs is that lowering a cap does not bind until
    /// every principal of the account is republished — the same of every other
    /// per-principal policy value, `ResolvedLimits` included.
    #[test]
    fn divergent_caps_bound_an_account_by_the_largest_not_the_sum() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(0)));
        for (principal, cap) in [(Principal(1), 51u64), (Principal(2), 102)] {
            let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(cap),
            };
            engine
                .map()
                .install(principal, Arc::new(snapshot), Arc::clone(&slot))
                .unwrap();
        }

        let held: Vec<_> = (0..2)
            .map(|_| {
                engine
                    .admit_one(
                        TestRequest {
                            principal: Principal(2),
                            ..request(1)
                        },
                        t(0),
                    )
                    .expect("the larger cap admits two")
            })
            .collect();
        assert_eq!(slot.overage().spent(), CostUnits(102));

        // The sum of the two caps is 153. If the caps combined rather than
        // maximised, a third request would fit; it must not.
        assert!(
            engine
                .admit_one(
                    TestRequest {
                        principal: Principal(2),
                        ..request(1)
                    },
                    t(0)
                )
                .is_err(),
            "caps must not add up across an account's principals"
        );
        // And the smaller cap is already over its own limit, so it refuses too.
        assert!(engine.admit_one(request(1), t(0)).is_err());
        drop(held);
    }

    /// Republishing a snapshot must not reset the counter: the cap bounds an
    /// account's exposure over time, and a control plane that publishes
    /// frequently would otherwise hand out unlimited credit in cap-sized
    /// slices.
    #[test]
    fn republishing_a_snapshot_does_not_reset_the_cap() {
        let engine = elastic_engine(51, Some(0));
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");

        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        let slot = Arc::clone(&state.lease);
        let mut next = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        next.generation = Generation(2);
        next.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(51),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(next), slot)
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            }
        );
    }

    /// Raising the cap takes effect on the next request with no reconciliation
    /// step, because the cap is read from the snapshot rather than stored
    /// beside the counter.
    #[test]
    fn a_republished_cap_takes_effect_immediately() {
        let engine = elastic_engine(51, Some(0));
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");
        assert!(engine.admit_one(request(1), t(0)).is_err());

        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        let slot = Arc::clone(&state.lease);
        let mut next = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        next.generation = Generation(2);
        next.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(102),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(next), slot)
            .unwrap();

        engine
            .admit_one(request(1), t(0))
            .expect("the raised cap admits the next request");
    }

    /// One request's inputs, for the tests whose subject is a single
    /// admission outcome rather than the staging itself. The staged tests
    /// above drive `begin` and `RequestContext::admit` directly.
    #[derive(Debug, Clone, Copy)]
    struct TestRequest<'a, O: OpIndex> {
        principal: Principal,
        required: PermissionBits,
        op: &'a O,
        items: u64,
    }

    impl<M: SnapshotMap> AdmissionEngine<M> {
        /// Stage one and stage two in one call, so a test that is about
        /// pricing, funding, or a deny reason does not restate the handoff.
        fn admit_one<O: OpIndex>(
            &self,
            request: TestRequest<'_, O>,
            now: Timestamp,
        ) -> Result<Pending<DiscardedUsageSlot>, DenyReason> {
            self.begin(request.principal, request.required, now)
                .and_then(|context| {
                    context.admit(
                        &[(request.op, request.items)],
                        DiscardedUsage::new().slot(),
                        now,
                    )
                })
        }
    }

    // --- Instance-visible balance (GL-97) -------------------------------------
    //
    // A quote here is 50 fixed + 1 per item, so `request(n)` costs `50 + n`.

    /// An engine whose snapshot carries `balance_at_publish`, funded by a
    /// lease large enough that the estimate is never what refuses a request.
    fn budgeted_engine(balance: u64, generation: u64) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        install_budget(&engine, balance, generation);
        engine
    }

    fn install_budget(engine: &AdmissionEngine<ArcSwapSnapshotMap>, balance: u64, generation: u64) {
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.generation = Generation(generation);
        snapshot.budget = Some(BudgetView {
            balance_at_publish: CostUnits(balance),
            period_end: None,
        });
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();
    }

    fn estimate(engine: &AdmissionEngine<ArcSwapSnapshotMap>) -> Option<CostUnits> {
        engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("an active principal begins")
            .estimate_remaining()
    }

    /// `None` is not zero. A snapshot published by a control plane that
    /// predates this field would otherwise make every instance tell every
    /// caller they were out of quota — a number nobody could stand behind,
    /// reported with the same confidence as a real one.
    #[test]
    fn an_instance_reports_no_estimate_when_the_control_plane_reported_no_budget() {
        let engine = engine_with(AccountStatus::Active, Some(1_000));
        assert_eq!(estimate(&engine), None);
    }

    /// The subtraction that makes the number worth publishing: the ledger's
    /// figure ages, and the instance knows exactly how much of that ageing it
    /// caused itself.
    #[test]
    fn an_estimate_subtracts_what_this_instance_admitted_since_publication() {
        let engine = budgeted_engine(1_000, 1);
        assert_eq!(estimate(&engine), Some(CostUnits(1_000)));

        let pending = engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(
            pending.estimate_remaining(),
            Some(CostUnits(940)),
            "this request's own 60-unit quote is already counted"
        );
        let _released = pending.cancel();

        assert_eq!(
            estimate(&engine),
            Some(CostUnits(940)),
            "a cancelled admission still reads as spent: the estimate errs low, \
             which is the safe direction for a number a customer acts on"
        );
    }

    /// A republish is a fresh statement of the ledger, so the instance's own
    /// tally has to restart with it. Carrying the old baseline forward would
    /// double-count every unit spent before the refresh.
    #[test]
    fn a_republish_rebases_the_estimate() {
        let engine = budgeted_engine(1_000, 1);
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(estimate(&engine), Some(CostUnits(940)));

        install_budget(&engine, 800, 2);

        assert_eq!(
            estimate(&engine),
            Some(CostUnits(800)),
            "the new figure already accounts for what was spent before it"
        );
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(estimate(&engine), Some(CostUnits(740)));
    }

    /// Spend outruns the published figure whenever the fleet is busy between
    /// refreshes. Zero is the floor; a wrapped estimate would report a
    /// customer's exhausted quota as astronomically large.
    #[test]
    fn an_estimate_saturates_at_zero_rather_than_wrapping() {
        let engine = budgeted_engine(100, 1);
        engine.admit_one(request(10), t(0)).expect("admits");
        engine.admit_one(request(10), t(0)).expect("admits");

        assert_eq!(estimate(&engine), Some(CostUnits::ZERO));
    }

    /// The stage a response is actually built from. `Committed` is what a
    /// served request holds while its kernel runs, so an embedder answering
    /// "how much is left" on a successful response reads it here — and it must
    /// give the same answer the earlier stages would, not a fresh `None`
    /// because the guard forgot to carry the state.
    #[test]
    fn a_committed_request_reports_the_estimate_its_response_carries() {
        let engine = budgeted_engine(1_000, 1);

        let committed = engine
            .admit_one(request(10), t(0))
            .expect("admits")
            .acquire_capacity(&NoGate)
            .expect("NoGate always permits")
            .commit(RequestId(1), t(0))
            .expect("a fresh reservation commits");

        assert_eq!(
            committed.estimate_remaining(),
            Some(CostUnits(940)),
            "the 60 units this request was charged are already subtracted"
        );

        // And it keeps moving as the instance serves more, rather than being
        // frozen at whatever the first response happened to see.
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(committed.estimate_remaining(), Some(CostUnits(880)));
    }

    /// The estimate is a report, never an input. Admission denies from the
    /// lease and the ledger (INVARIANTS.md GL-1); if a stale published zero
    /// could refuse, a refresh delay would become an outage.
    #[test]
    fn an_exhausted_estimate_does_not_deny() {
        let engine = budgeted_engine(0, 1);

        let pending = engine
            .admit_one(request(10), t(0))
            .expect("the lease funds this request whatever the published balance says");

        assert_eq!(pending.estimate_remaining(), Some(CostUnits::ZERO));
        let _released = pending.cancel();
    }

    fn request(items: u64) -> TestRequest<'static, Op> {
        request_for(Principal(1), items)
    }

    fn request_for(principal: Principal, items: u64) -> TestRequest<'static, Op> {
        TestRequest {
            principal,
            required: PermissionBits::bit(0),
            op: &Op::Price,
            items,
        }
    }

    #[test]
    fn full_pipeline_admits_and_commits() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let admitted = engine.admit_one(request(14), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(64));
        admitted
            .funding
            .reservation()
            .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            .unwrap();
    }

    /// A real gate with no capacity left, which is what a shed looks like in
    /// production.
    ///
    /// GL-93 needed a `RefusingGate` double because `NoGate` is infallible and
    /// the traits are sealed, so nothing could reach the shed path. GL-99 makes
    /// the double unnecessary: a `Uniform` gate of one unit, with that unit
    /// held, refuses for the real reason through the real code.
    fn saturated_gate() -> (ExecutionCapacityGate, ExecutionPermit) {
        let gate = ExecutionCapacityGate::new(
            ExecutionCapacityMode::Uniform {
                total: NonZeroU32::new(1).unwrap(),
            },
            LocalSharding::SINGLE,
        )
        .expect("one unit is a valid configuration")
        .expect("an enabled mode yields a gate");
        let held = gate
            .acquire(CapacityEvidence::new(
                CapacityClass::Assured,
                Generation(1),
                Locality::current(),
            ))
            .expect("the only unit is free");
        (gate, held)
    }

    /// A shed request is counted at the stage that shed it, released for zero,
    /// and never added to the pre-admission denial total — it was already
    /// counted under `admitted`.
    #[test]
    fn a_capacity_shed_is_counted_without_a_second_denial() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        let installed = lease(10_000);
        drop(slot.replace(Arc::clone(&installed)));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot)
            .unwrap();
        let before = installed.remaining();

        let (gate, _held) = saturated_gate();
        let (reason, _released) = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&gate)
            .expect_err("a saturated gate refuses");
        assert_eq!(
            reason,
            DenyReason::CapacityUnavailable,
            "a shed says the instance is full, not that the account did \
             something wrong"
        );
        assert_eq!(reason.retry(), Retry::Transient);

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.capacity_shed, 1);
        assert_eq!(
            counters.canceled_before_start, 0,
            "a shed is its own outcome, not a cancellation"
        );
        assert_eq!(counters.execution_started, 0);
        assert_eq!(
            counters.denied(),
            0,
            "the request was admitted; the shed belongs to a later stage"
        );
        assert_eq!(
            counters.execution_started
                + counters.canceled_before_start
                + counters.capacity_shed
                + counters.refused_at_start(),
            counters.admitted,
            "a shed still partitions `admitted` exactly"
        );
        assert_eq!(
            installed.remaining(),
            before,
            "a shed request is released for zero"
        );
    }

    /// Every admitted request reaches exactly one terminal counter, and none
    /// of them touches the pre-admission denial total.
    ///
    /// This is the arithmetic INVARIANTS.md GL-20 asks for: `admitted` is the
    /// total, and `execution_started + canceled_before_start + capacity_shed +
    /// refused_at_start` accounts for all of it. A request counted as both an
    /// admission and a denial is the contradictory identity the invariant
    /// exists to prevent.
    #[test]
    fn every_admitted_request_reaches_exactly_one_terminal_counter() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));

        // Committed.
        drop(
            engine
                .admit_one(request(1), t(0))
                .unwrap()
                .acquire_capacity(&NoGate)
                .unwrap()
                .commit(RequestId(1), t(0))
                .unwrap(),
        );
        // Explicitly cancelled while pending.
        engine.admit_one(request(1), t(0)).unwrap().cancel();
        // Abandoned while pending, with no explicit call at all.
        drop(engine.admit_one(request(1), t(0)).unwrap());
        // Abandoned after acquiring capacity.
        drop(
            engine
                .admit_one(request(1), t(0))
                .unwrap()
                .acquire_capacity(&NoGate)
                .unwrap(),
        );

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 4);
        assert_eq!(counters.execution_started, 1);
        assert_eq!(counters.canceled_before_start, 3);
        assert_eq!(counters.capacity_shed, 0);
        assert_eq!(counters.refused_at_start(), 0);
        assert_eq!(
            counters.execution_started
                + counters.canceled_before_start
                + counters.capacity_shed
                + counters.refused_at_start(),
            counters.admitted,
            "every admitted request must reach exactly one terminal counter"
        );
        assert_eq!(
            counters.denied(),
            0,
            "a post-admission outcome is never a pre-admission denial"
        );
    }

    /// A context that `begin` produced and nothing consumed is counted as its
    /// own phase outcome — neither an admission nor a denial.
    ///
    /// Without this an instance authenticating a flood of requests whose
    /// bodies never arrive is indistinguishable from one serving nothing.
    #[test]
    fn an_abandoned_context_is_counted_and_denies_nothing() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        drop(
            engine
                .begin(Principal(1), PermissionBits::bit(0), t(0))
                .expect("stage one authorizes"),
        );

        let counters = engine.counters().snapshot();
        assert_eq!(counters.contexts_abandoned, 1);
        assert_eq!(counters.admitted, 0, "no pending funding was created");
        assert_eq!(counters.denied(), 0, "and nothing was refused");
    }

    /// Consuming a context through `admit` is not abandoning it, whether the
    /// admission succeeds or is refused at stage two.
    #[test]
    fn a_consumed_context_is_never_counted_as_abandoned() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        engine.admit_one(request(1), t(0)).unwrap().cancel();
        assert_eq!(engine.counters().snapshot().contexts_abandoned, 0);

        // A stage-two refusal also consumed its context.
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        engine
            .admit_one(request(u64::MAX), t(0))
            .expect_err("an oversized workload is refused at stage two");
        let counters = engine.counters().snapshot();
        assert_eq!(counters.contexts_abandoned, 0);
        assert_eq!(counters.denied(), 1, "the refusal is a stage-two denial");
    }

    /// A commit-time funding refusal is counted under its own reason, at the
    /// stage that decided it — never added to the pre-admission denial total,
    /// which would make one request both an admission and a refusal.
    #[test]
    fn a_commit_time_funding_refusal_is_counted_under_its_own_reason() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot)
            .unwrap();

        engine
            .admit_one(request(1), t(99))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(1), t(100))
            .expect_err("an expired lease cannot commit");

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.refused_at_start(), 1);
        assert_eq!(
            counters.commit_refusals[CommitRefusal::FundingExpired.index()],
            1
        );
        assert_eq!(counters.execution_started, 0);
        assert_eq!(
            counters.canceled_before_start, 0,
            "a funding refusal is not a cancellation"
        );
        assert_eq!(
            counters.denied(),
            0,
            "the request was admitted; its refusal belongs to a later stage"
        );
        assert_eq!(
            counters
                .commit_refusals_by_name()
                .find(|(name, _)| *name == "funding_expired")
                .map(|(_, count)| count),
            Some(1)
        );
    }

    /// A commit-time elastic fallback gets its own qualifier, disjoint from
    /// the admission-time one: this request *was* funded by a lease, and
    /// changed funding at execution start.
    #[test]
    fn a_commit_time_fallback_is_counted_under_its_own_qualifier() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(10_000),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();

        let committed = engine
            .admit_one(request(1), t(99))
            .unwrap()
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(1), t(100))
            .expect("an elastic lapse settles against overage");
        let units = committed.units();
        drop(committed);

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.execution_started, 1);
        assert_eq!(counters.committed_at_overage, 1);
        assert_eq!(counters.units_committed_at_overage, units.get());
        assert_eq!(
            counters.admitted_overage, 0,
            "a lease funded the admission; only the commit moved to overage"
        );
    }

    /// The two elastic qualifiers answer different questions and must not be
    /// conflated: an admission no lease could fund is not a commit that
    /// changed funding.
    #[test]
    fn an_admission_time_overage_is_not_counted_as_a_commit_time_one() {
        let engine = elastic_engine(10_000, None);
        drop(
            engine
                .admit_one(request(1), t(0))
                .unwrap()
                .acquire_capacity(&NoGate)
                .unwrap()
                .commit(RequestId(1), t(0))
                .expect("overage funds the request"),
        );

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted_overage, 1);
        assert_eq!(
            counters.committed_at_overage, 0,
            "the commit changed no funding source"
        );
        assert_eq!(counters.execution_started, 1);
    }

    /// The revision reaches the billing record, and every stage reports the
    /// same one. A consumer returns it in its response metadata, so "what the
    /// response says" and "what the bill says" must be one value rather than
    /// two lookups that happen to agree.
    #[test]
    fn a_committed_event_carries_the_pinned_revision() {
        let revision = PolicyRevision([0x11; 32]);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(10_000)));
        engine
            .map()
            .install(Principal(1), snapshot_with_revision(revision), slot)
            .unwrap();

        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("stage one authorizes");
        assert_eq!(context.policy_revision(), revision);

        let pending = context
            .admit(&[(&Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect("the request admits");
        assert_eq!(pending.policy_revision(), revision);

        let committed = pending
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(0))
            .expect("the request commits");
        assert_eq!(committed.policy_revision(), revision);
    }

    /// An unstated revision is carried as such rather than becoming an error
    /// or a fabricated value — the reader-first half of the rollout, seen from
    /// the request path.
    #[test]
    fn an_unstated_revision_reaches_the_charge_unstated() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let committed = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(0))
            .expect("the request commits");
        assert_eq!(committed.policy_revision(), PolicyRevision::UNSTATED);
    }

    /// Revision and generation are different facts and must not be conflated.
    ///
    /// A republication that changes only the revision is a *newer generation*
    /// carrying different application identity; a request already begun keeps
    /// the one it pinned (INVARIANTS.md GL-26), and the next request sees the
    /// new one. This is the test that would fail if either value were ever
    /// derived from the other.
    #[test]
    fn a_republished_revision_does_not_reach_an_already_pinned_request() {
        let first = PolicyRevision([0x01; 32]);
        let second = PolicyRevision([0x02; 32]);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(10_000)));
        engine
            .map()
            .install(
                Principal(1),
                snapshot_with_revision(first),
                Arc::clone(&slot),
            )
            .unwrap();

        // Pinned before the republication.
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("stage one authorizes");

        let mut next = AccountSnapshot::clone(&snapshot_with_revision(second));
        next.generation = Generation(2);
        engine
            .map()
            .install(Principal(1), Arc::new(next), slot)
            .unwrap();

        assert_eq!(
            context.policy_revision(),
            first,
            "an in-flight request keeps the revision it pinned"
        );
        assert_eq!(context.generation(), Generation(1));

        let committed = context
            .admit(&[(&Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(0))
            .expect("the request commits");
        assert_eq!(
            committed.policy_revision(),
            first,
            "and bills under it, not under the republished one"
        );

        // The next request sees the new revision at the new generation.
        let later = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("stage one authorizes");
        assert_eq!(later.policy_revision(), second);
        assert_eq!(later.generation(), Generation(2));
    }

    #[test]
    fn a_staged_context_keeps_principal_policy_after_republication_and_owner_drop() {
        fn owned_send_sync<T: Send + Sync + 'static>(_: &T) {}
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .unwrap();
        owned_send_sync(&context);
        let mut replacement = AccountSnapshot::clone(&snapshot(AccountStatus::Suspended));
        replacement.generation = Generation(2);
        replacement.limits = ResolvedLimits::new(1).with_weighted_rate(1_000_000, 1_000_000);
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(replacement),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
        assert!(
            engine
                .begin(Principal(1), PermissionBits::bit(0), t(0))
                .is_err()
        );
        drop(engine);

        std::thread::spawn(move || {
            assert_eq!(context.generation(), Generation(1));
            assert_eq!(context.snapshot().status, AccountStatus::Active);
            let pending = context
                .admit(&[(&Op::Price, 2)], DiscardedUsage::new().slot(), t(0))
                .unwrap();
            assert_eq!(pending.quote().total, CostUnits(52));
            pending.cancel();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn stage_two_checks_the_pinned_expiry_after_republication() {
        for now in [t(9), t(10)] {
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
            let slot = LeaseSlot::for_account(AccountId(1));
            drop(slot.replace(lease(10_000)));
            let mut original = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
            original.valid_until = t(10);
            engine
                .map()
                .install(Principal(1), Arc::new(original), Arc::clone(&slot))
                .unwrap();
            let context = engine
                .begin(Principal(1), PermissionBits::bit(0), t(0))
                .unwrap();
            let mut replacement = AccountSnapshot::clone(&snapshot(AccountStatus::Suspended));
            replacement.generation = Generation(2);
            engine
                .map()
                .install(Principal(1), Arc::new(replacement), slot)
                .unwrap();
            let result = context.admit(&[(&Op::Price, 1)], DiscardedUsage::new().slot(), now);
            if now == t(9) {
                result
                    .expect("the pinned active snapshot is still fresh")
                    .cancel();
            } else {
                assert!(
                    matches!(result, Err(DenyReason::SnapshotExpired)),
                    "{result:?}"
                );
            }
        }
    }

    #[test]
    fn shared_counters_follow_engines_and_owned_contexts() {
        fn check(map: impl SnapshotMap) {
            let map = Arc::new(map);
            let slot = LeaseSlot::for_account(AccountId(1));
            drop(slot.replace(lease(10_000)));
            map.install(Principal(1), snapshot(AccountStatus::Active), slot)
                .unwrap();
            let first = AdmissionEngine::new(Arc::clone(&map));
            let second = AdmissionEngine::new(Arc::clone(&map));
            let context = first
                .begin(Principal(1), PermissionBits::bit(0), t(0))
                .unwrap();
            assert!(
                second
                    .begin(Principal(2), PermissionBits::bit(0), t(0))
                    .is_err()
            );
            assert_eq!(first.counters().snapshot().denied(), 1);
            drop(first);
            context
                .admit(&[(&Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
                .unwrap()
                .cancel();
            assert_eq!(second.counters().snapshot().admitted, 1);
            assert_eq!(map.counters().snapshot().canceled_before_start, 1);
            assert_eq!(second.counters().snapshot().denied(), 1);
        }
        check(ArcSwapSnapshotMap::new());
        check(crate::MokaSnapshotMap::new(1_000));
    }

    #[test]
    fn a_staged_context_observes_account_rate_published_after_begin() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .unwrap();
        let mut replacement = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        replacement.generation = Generation(2);
        replacement.limits = ResolvedLimits::new(1).with_weighted_rate(1_000_000, 51);
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(replacement),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
        // The old principal permits two items, quoted at 52. Account rate is
        // current, so its new 51-unit burst refuses before any lease debit.
        let result = context.admit(&[(&Op::Price, 2)], DiscardedUsage::new().slot(), t(0));
        assert!(
            matches!(
                result,
                Err(DenyReason::UnpriceableUnderLimits {
                    weight: CostUnits(52),
                    burst_units: CostUnits(51)
                })
            ),
            "{result:?}"
        );
        assert_eq!(engine.counters().snapshot().admitted, 0);
        assert_eq!(engine.counters().snapshot().denied(), 1);
    }

    /// The consumer topology this whole lifecycle exists for: an asynchronous
    /// waiter holds a cancel handle while a worker thread holds the only value
    /// that can commit. The worker wins, and the charge stands.
    #[test]
    fn a_split_worker_that_commits_first_charges_in_full() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let (ready, handle) = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .split();

        let committed = ready.commit(RequestId(1), t(0)).expect("the worker wins");
        assert!(!committed.is_cancelled(), "nobody has asked yet");

        assert_eq!(
            handle.cancel(),
            CancelOutcome::AlreadyCommitted {
                units: committed.units()
            }
        );
        assert!(
            committed.is_cancelled(),
            "a running kernel can see its caller has gone"
        );
    }

    /// The other side of the race: cancellation lands first, so the worker
    /// gets no guard and must not execute.
    #[test]
    fn a_split_cancel_before_start_leaves_the_worker_without_a_guard() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let (ready, handle) = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .split();

        assert_eq!(handle.cancel(), CancelOutcome::ZeroCharged);
        let (error, _released) = ready
            .commit(RequestId(1), t(0))
            .expect_err("a cancelled request must not produce an execution guard");
        assert_eq!(error, CommitError::Cancelled);
    }

    /// A worker that abandons a split request must refund *immediately*, not
    /// when the asynchronous side eventually lets go of its handle.
    ///
    /// This is what the worker-side share guard buys. Leaving the release to
    /// the reservation's own drop would hold funding for as long as any handle
    /// outlived the worker — an unbounded interval, during which the account's
    /// own retries see capacity that nothing is using.
    #[test]
    fn dropping_the_worker_side_refunds_before_the_cancel_handle_does() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        let installed = lease(10_000);
        drop(slot.replace(Arc::clone(&installed)));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot)
            .unwrap();
        let before = installed.remaining();

        let (ready, handle) = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .split();
        assert!(
            installed.remaining() < before,
            "admission debited the lease"
        );

        // The worker gives up. The handle is deliberately still alive.
        drop(ready);
        assert_eq!(
            installed.remaining(),
            before,
            "the refund must not wait for the cancel handle"
        );
        assert!(!handle.is_cancelled(), "nobody asked; the worker just left");
        assert_eq!(handle.cancel(), CancelOutcome::ZeroCharged);
        assert_eq!(
            installed.remaining(),
            before,
            "and it is refunded exactly once"
        );
    }

    /// An unsplit request has no handle outstanding, so nothing can have asked
    /// it to stop and it pays nothing for the capability.
    #[test]
    fn an_unsplit_committed_guard_is_never_cancelled() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let committed = engine
            .admit_one(request(1), t(0))
            .expect("the request admits")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(0))
            .expect("the request commits");
        assert!(!committed.is_cancelled());
    }

    /// The staged path under `Strict`: a lease whose window lapsed between
    /// admission and execution start produces **no** `Committed` guard, so the
    /// kernel cannot run. `Committed` is the only proof execution may begin,
    /// and the type system is what enforces that here.
    #[test]
    fn a_strict_expiry_at_execution_start_produces_no_committed_guard() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot)
            .unwrap();

        let ready = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled");

        // The window lapses before the worker starts.
        let (error, _released) = ready
            .commit(RequestId(1), t(100))
            .expect_err("an expired lease must not produce an execution guard");
        assert_eq!(
            error,
            CommitError::Denied(DenyReason::FundingExpiredAtStart)
        );
    }

    /// The same lapse under `Elastic` settles against overage instead of
    /// refusing: the worker gets its guard, the charge stands in full, and the
    /// bill names no lease — the lease that funded admission was returned.
    #[test]
    fn an_elastic_expiry_at_execution_start_produces_a_guard_billed_as_overage() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(10_000),
        };
        let overage = Arc::clone(slot.overage());
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();

        let pending = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable");
        let quote = pending.quote().total;
        let committed = pending
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(100))
            .expect("an elastic lapse settles against overage");

        assert_eq!(committed.units(), quote, "the full quote still stands");
        assert_eq!(
            overage.spent(),
            quote,
            "the charge moved to the overage counter"
        );
    }

    /// A commit-time fallback that cannot fit inside the cap reports the
    /// overage counter's own refusal, with its own retry classification —
    /// never relabelled as expired funding, which would tell the caller the
    /// lease was the problem.
    #[test]
    fn a_commit_time_fallback_beyond_the_cap_reports_the_overage_refusal() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        // A cap far below any quote this request can produce.
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1),
        };
        engine
            .map()
            .install(Principal(1), Arc::new(snapshot), slot)
            .unwrap();

        let (error, _released) = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(100))
            .expect_err("a fallback beyond the cap cannot produce a guard");
        let CommitError::Denied(reason) = error else {
            panic!("expected a classified funding denial, got {error:?}")
        };
        assert!(
            matches!(reason, DenyReason::OverageCapExhausted { .. }),
            "expected the overage counter's own refusal, got {reason:?}"
        );
        assert_eq!(reason.retry(), Retry::Transient);
    }

    #[test]
    fn unknown_principal_denies() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn negative_cache_denies() {
        let engine = AdmissionEngine::new(MokaSnapshotMap::new(10));
        engine.map().install_unknown(Principal(1), t(100)).unwrap();
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn suspended_account_denies() {
        let engine = engine_with(AccountStatus::Suspended, Some(10_000));
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::AccountSuspended
        );
    }

    #[test]
    fn batch_cap_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        assert_eq!(
            engine.admit_one(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
    }

    #[test]
    fn unpriced_operation_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let req = TestRequest {
            principal: Principal(1),
            required: PermissionBits::bit(0),
            op: &Op::Unpriced,
            items: 1,
        };
        assert_eq!(
            engine.admit_one(req, t(0)).unwrap_err(),
            DenyReason::UnpricedOperation
        );
    }

    #[test]
    fn missing_lease_denies_cold_start() {
        let engine = engine_with(AccountStatus::Active, None);
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
    }

    #[test]
    fn exhausted_lease_denies_and_charges_zero() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        // First request (51 units) fits; second is denied by remaining=9.
        let admitted = engine.admit_one(request(1), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(51));
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseExhausted {
                remaining: CostUnits(9)
            }
        );
        // Cancelling the first returns its units; admission works again.
        assert_eq!(
            admitted.funding.reservation().cancel(),
            CancelOutcome::ZeroCharged
        );
        engine.admit_one(request(1), t(0)).unwrap();
    }

    #[test]
    fn rate_limiter_weights_by_cost() {
        // Burst of 1000 units, negligible refill within the test: exactly ten
        // 100-unit requests fit the burst (GCRA's boundary is inclusive), and
        // the eleventh is rate limited even though the lease has plenty left.
        let mut snapshot = (*snapshot(AccountStatus::Active)).clone();
        snapshot.limits = ResolvedLimits::new(64).with_weighted_rate(1, 1_000);
        let snapshot = Arc::new(snapshot);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));
        engine.map().install(Principal(1), snapshot, slot).unwrap();

        let req = request(50); // 50 + 50 fixed = 100 units
        for _ in 0..10 {
            let admitted = engine.admit_one(req, t(0)).unwrap();
            assert_eq!(admitted.quote.total, CostUnits(100));
        }
        assert_eq!(
            engine.admit_one(req, t(0)).unwrap_err(),
            DenyReason::RateLimited,
            "an empty-but-refilling bucket is throttling, not misconfiguration"
        );
    }

    #[test]
    fn request_rate_limiter_counts_requests_not_cost() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(1).unwrap(), NonZeroU32::new(2).unwrap());
        let engine = engine_with_limits(limits);

        // Different quotes each consume one request token.
        drop(engine.admit_one(request(1), t(0)).unwrap());
        drop(engine.admit_one(request(64), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited
        );
    }

    #[test]
    fn disabled_weighted_rate_performs_no_weighted_check() {
        let engine = engine_with_limits(
            ResolvedLimits::new(64).with_weighted_rate_compatibility_fallback(1, 1),
        );

        // Every request quotes far beyond the fallback burst. A new reader
        // treats that pair as rollout data when the explicit flag disables
        // the bucket, while an old reader conservatively keeps enforcing it.
        for _ in 0..3 {
            drop(engine.admit_one(request(64), t(0)).unwrap());
        }
    }

    #[test]
    fn changing_disabled_weighted_fallback_does_not_refill_request_rate() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut original = (*snapshot(AccountStatus::Active)).clone();
        original.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(1, 1)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine
            .map()
            .install(Principal(1), Arc::new(original), Arc::clone(&slot))
            .unwrap();

        drop(engine.admit_one(request(1), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited
        );

        let mut compatibility_only = (*snapshot(AccountStatus::Active)).clone();
        compatibility_only.generation = Generation(2);
        compatibility_only.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(2, 2)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(compatibility_only),
                Arc::clone(&slot),
            )
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited,
            "inactive compatibility metadata cannot refill an unchanged request bucket"
        );
    }

    #[test]
    fn enabling_request_rate_does_not_refill_weighted_rate() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut weighted_only = (*snapshot(AccountStatus::Active)).clone();
        weighted_only.limits = ResolvedLimits::new(64).with_weighted_rate(1, 51);
        engine
            .map()
            .install(Principal(1), Arc::new(weighted_only), Arc::clone(&slot))
            .unwrap();

        drop(engine.admit_one(request(1), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RateLimited
        );

        let mut request_rate_added = (*snapshot(AccountStatus::Active)).clone();
        request_rate_added.generation = Generation(2);
        request_rate_added.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1, 51)
            .with_request_rate(
                NonZeroU32::new(1_000_000).unwrap(),
                NonZeroU32::new(1_000_000).unwrap(),
            );
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(request_rate_added),
                Arc::clone(&slot),
            )
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RateLimited,
            "changing request-rate policy cannot refill an unchanged weighted bucket"
        );
    }

    /// Two valid principal snapshots can share an account and generation but
    /// disagree about whether weighted rate is enabled. The account bucket
    /// selected by the first publication must be the policy advertised by the
    /// second installed state too; otherwise admission dereferences a bucket
    /// the state does not contain and aborts in production.
    #[test]
    fn divergent_enabled_snapshot_cannot_outlive_a_disabled_account_bucket() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine
            .map()
            .install(Principal(1), Arc::new(disabled), Arc::clone(&slot))
            .unwrap();

        let mut enabled = (*snapshot(AccountStatus::Active)).clone();
        enabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(10).unwrap(), NonZeroU32::new(10).unwrap());
        engine
            .map()
            .install(Principal(2), Arc::new(enabled), Arc::clone(&slot))
            .unwrap();

        let admitted = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .expect("the canonical disabled account bucket performs no check");
        let MapEntry::Present(state) = engine.map().get(&Principal(2)).unwrap() else {
            panic!("principal present");
        };
        let account = state.limiter.current();
        assert_eq!(account.rate().policy().weighted_rate(), None);
        assert_eq!(
            account
                .rate()
                .policy()
                .request_rate()
                .expect("the canonical request bucket is enabled")
                .burst_requests(),
            NonZeroU32::MIN,
        );
        drop(admitted);
    }

    /// The inverse mismatch must not let a principal whose submitted snapshot
    /// disables weighted rate bypass the enabled account bucket it joined.
    #[test]
    fn divergent_disabled_snapshot_cannot_bypass_an_enabled_account_bucket() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut enabled = (*snapshot(AccountStatus::Active)).clone();
        enabled.limits = ResolvedLimits::new(64).with_weighted_rate(1, 51);
        engine
            .map()
            .install(Principal(1), Arc::new(enabled), Arc::clone(&slot))
            .unwrap();

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.limits =
            ResolvedLimits::new(64).with_weighted_rate_compatibility_fallback(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(2), Arc::new(disabled), Arc::clone(&slot))
            .unwrap();

        drop(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .expect("the first request consumes the whole account burst"),
        );
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::RateLimited
        );
    }

    /// Tightening a shared bucket's safe shard count publishes one new
    /// account authority. An already-installed principal must load that same
    /// authority on its next request instead of continuing to refill and
    /// spend an old partition in parallel.
    #[test]
    fn shard_tightening_cannot_leave_two_spendable_account_buckets() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(1_000_000)));

        let priced = |quote: u64| {
            let snapshot = Arc::new(
                AccountSnapshot::builder(
                    AccountId(1),
                    Generation(1),
                    AccountStatus::Active,
                    t(10_000),
                    PermissionBits::bit(0),
                    ResolvedLimits::new(1).with_weighted_rate(1, 800),
                    Arc::new(
                        CostTable::builder(CostUnits(quote), CostUnits(quote))
                            .weight(&Op::Price, CostUnits::ZERO)
                            .build(),
                    ),
                )
                .build(),
            );
            PublishableSnapshot::try_new(snapshot).unwrap()
        };
        engine
            .map()
            .install_publishable(Principal(1), priced(100), Arc::clone(&slot))
            .unwrap();
        engine
            .map()
            .install_publishable(Principal(2), priced(500), Arc::clone(&slot))
            .unwrap();

        for _ in 0..8 {
            drop(
                engine
                    .admit_one(request_for(Principal(1), 1), t(0))
                    .expect("the shared 800-unit burst admits eight 100-unit requests"),
            );
        }
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::RateLimited,
            "the heavier principal cannot retain a second account bucket"
        );
    }

    #[test]
    fn account_concurrency_is_held_for_the_admitted_lifetime() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(1).unwrap(), None)
            .unwrap();
        let engine = engine_with_limits(limits);

        let first = engine.admit_one(request(1), t(0)).unwrap();
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited
        );
        drop(first);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn every_principal_observes_the_canonical_account_concurrency_ceiling() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut bounded = (*snapshot(AccountStatus::Active)).clone();
        bounded.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(bounded), Arc::clone(&slot))
            .unwrap();

        let mut unbounded = (*snapshot(AccountStatus::Active)).clone();
        unbounded.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(2), Arc::new(unbounded), Arc::clone(&slot))
            .unwrap();

        let mut wider = (*snapshot(AccountStatus::Active)).clone();
        wider.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(8).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(3), Arc::new(wider), Arc::clone(&slot))
            .unwrap();

        let held = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .expect("the canonical account slot is initially free");
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "an unbounded sibling must not bypass the account's selected ceiling"
        );
        assert_eq!(
            engine
                .admit_one(request_for(Principal(3), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "a larger sibling value cannot widen the selected account ceiling"
        );
        drop(held);
    }

    #[test]
    fn newer_account_concurrency_policy_reaches_existing_principals() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut original = (*snapshot(AccountStatus::Active)).clone();
        original.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(original), Arc::clone(&slot))
            .unwrap();

        let mut narrower = (*snapshot(AccountStatus::Active)).clone();
        narrower.generation = Generation(2);
        narrower.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(2), Arc::new(narrower), Arc::clone(&slot))
            .unwrap();

        let held = engine
            .admit_one(request_for(Principal(1), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "an existing principal must load the newer account ceiling"
        );
        drop(held);
    }

    #[test]
    fn enabling_account_concurrency_counts_already_in_flight_work() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let held = {
            let mut unlimited = (*snapshot(AccountStatus::Active)).clone();
            unlimited.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
            engine
                .map()
                .install(Principal(1), Arc::new(unlimited), Arc::clone(&slot))
                .unwrap();
            engine.admit_one(request(1), t(0)).unwrap()
        };

        let mut limited = (*snapshot(AccountStatus::Active)).clone();
        limited.generation = Generation(2);
        limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(limited), Arc::clone(&slot))
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "enabling a ceiling must include work admitted while enforcement was disabled"
        );
        drop(held);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn enabling_principal_concurrency_counts_already_in_flight_work() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut account_only = (*snapshot(AccountStatus::Active)).clone();
        account_only.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(account_only), Arc::clone(&slot))
            .unwrap();
        let held = engine.admit_one(request(1), t(0)).unwrap();

        let mut principal_limited = (*snapshot(AccountStatus::Active)).clone();
        principal_limited.generation = Generation(2);
        principal_limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), Some(NonZeroU32::MIN))
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(principal_limited), Arc::clone(&slot))
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "a newly enabled principal ceiling must include existing work for that principal"
        );
        drop(held);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn reenabled_account_concurrency_counts_work_admitted_while_disabled() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));

        let mut initially_limited = (*snapshot(AccountStatus::Active)).clone();
        initially_limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(initially_limited), Arc::clone(&slot))
            .unwrap();
        let before_disable = engine.admit_one(request(1), t(0)).unwrap();

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.generation = Generation(2);
        disabled.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(1), Arc::new(disabled), Arc::clone(&slot))
            .unwrap();
        let while_disabled = engine.admit_one(request(1), t(0)).unwrap();

        let mut reenabled = (*snapshot(AccountStatus::Active)).clone();
        reenabled.generation = Generation(3);
        reenabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(reenabled), Arc::clone(&slot))
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "disabling enforcement must not erase occupancy seen after re-enable"
        );
        drop(before_disable);
        drop(while_disabled);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn a_principal_ceiling_narrows_without_bypassing_the_account_ceiling() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(
                NonZeroU32::new(2).unwrap(),
                Some(NonZeroU32::new(1).unwrap()),
            )
            .unwrap();
        for principal in [Principal(1), Principal(2)] {
            let mut account = (*snapshot(AccountStatus::Active)).clone();
            account.limits = limits;
            engine
                .map()
                .install(principal, Arc::new(account), Arc::clone(&slot))
                .unwrap();
        }

        let first = engine
            .admit_one(request_for(Principal(1), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "the principal-local ceiling binds first"
        );
        let second = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "the shared account ceiling still binds"
        );
        drop(first);
        drop(second);
    }

    #[test]
    fn a_later_funding_refusal_releases_concurrency_but_keeps_rate_tokens() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap())
            .with_concurrency(NonZeroU32::new(1).unwrap(), None)
            .unwrap();
        let mut account = (*snapshot(AccountStatus::Active)).clone();
        account.limits = limits;
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(account),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited,
            "the request token is not refunded after a funding refusal"
        );
        let state = match engine.map().get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state,
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert_eq!(state.account_concurrency_in_flight(), 0);
    }

    /// Builds an engine whose account carries the given limits.
    fn engine_with_limits(limits: ResolvedLimits) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let mut snapshot = (*snapshot(AccountStatus::Active)).clone();
        snapshot.limits = limits;
        let snapshot = Arc::new(snapshot);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000_000)));
        engine.map().install(Principal(1), snapshot, slot).unwrap();
        engine
    }

    /// Issue GL-40: a batch cap that admits a quote larger than the whole burst
    /// is a misconfigured schedule. Reporting it as throttling invites a retry
    /// that can never succeed.
    #[test]
    fn batch_cap_above_burst_is_unpriceable_not_throttled() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(
            1_000,
            // 64 items quote 50 + 64 = 114 units: inside the batch cap, past
            // the burst, and unadmittable however long the caller waits.
            64,
        ));

        assert_eq!(
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // Repeating never converts it into ordinary throttling.
        assert_eq!(
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // A request the burst *can* hold still admits: the deny is about this
        // request's weight, not a wedged account.
        engine.admit_one(request(1), t(0)).unwrap();
    }

    /// The burst bound is inclusive, matching GCRA's own inclusive boundary
    /// (see `rate_limiter_weights_by_cost`): a quote of exactly the burst is
    /// admissible, so it must not be pre-empted as unpriceable. An off-by-one
    /// here would deny requests governor would have accepted.
    #[test]
    fn weight_equal_to_burst_admits() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(
            1, // 64 items quote exactly 50 + 64 = 114 units.
            114,
        ));

        let admitted = engine.admit_one(request(64), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(114));
        // The bucket is now empty, so the next one is ordinary throttling —
        // never the terminal reason.
        assert_eq!(
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::RateLimited
        );
    }

    /// A zero burst is not silently repaired into a burst of one: every
    /// priced request is refused, and says why.
    ///
    /// Publication rejects this schedule outright
    /// (`WeightedRateOutsideGovernorDomain`), so the configuration under test
    /// is reachable only through the unvalidated `SnapshotMap::install` seam.
    /// That seam is exactly what the runtime `UnpriceableUnderLimits` check
    /// exists to backstop: the governor quota is clamped to a nonzero burst
    /// for construction, and the full-width comparison must still refuse.
    #[test]
    fn zero_burst_denies_every_priced_request() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(1_000, 0));

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(51),
                burst_units: CostUnits::ZERO,
            }
        );
    }

    /// A quote wider than the bucket's u32 domain is unadmittable by
    /// construction; narrowing must not disguise it as an empty bucket.
    #[test]
    fn quote_beyond_the_bucket_domain_is_unpriceable() {
        let engine = engine_with_limits(
            ResolvedLimits::new(u64::MAX).with_weighted_rate(1_000, u64::from(u32::MAX)),
        );

        // 50 fixed + items: the first quote to exceed the burst.
        let items = u64::from(u32::MAX);
        assert_eq!(
            engine.admit_one(request(items), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(items + 50),
                burst_units: CostUnits(u64::from(u32::MAX)),
            }
        );
    }

    /// The account's rate bucket is shared by every principal, but the split
    /// is sized from a *snapshot's* largest quote. Sizing it from whichever
    /// principal installed first left a sibling with a heavier cost table
    /// unable to spend its largest quote in any shard — reported as
    /// `UnpriceableUnderLimits` for a request the account's burst can hold,
    /// and admitted before the split existed (INVARIANTS.md GL-5).
    #[test]
    fn a_shared_split_bucket_never_wedges_a_principal_the_burst_can_hold() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let limits = ResolvedLimits::new(64).with_weighted_rate(800, 800);

        // A light key: 64 items quote 2 + 64 = 66 units, so eight buckets of
        // a hundred each can hold one.
        let mut light = (*snapshot(AccountStatus::Active)).clone();
        light.limits = limits;
        light.cost_table = Arc::new(
            CostTable::builder(CostUnits(2), CostUnits(1))
                .weight(&Op::Price, CostUnits(1))
                .build(),
        );
        let light = PublishableSnapshot::try_new(Arc::new(light)).unwrap();

        // A heavy key of the same account at the same generation: 64 items
        // quote 2 + 640 = 642 units. Publication accepts it — it fits the
        // account's 800-unit burst — so admission must too.
        let mut heavy = (*snapshot(AccountStatus::Active)).clone();
        heavy.limits = limits;
        heavy.cost_table = Arc::new(
            CostTable::builder(CostUnits(2), CostUnits(1))
                .weight(&Op::Price, CostUnits(10))
                .build(),
        );
        let heavy = PublishableSnapshot::try_new(Arc::new(heavy)).unwrap();

        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        drop(slot.replace(lease(1_000_000)));
        engine
            .map()
            .install_publishable(Principal(1), light, Arc::clone(&slot))
            .unwrap();
        engine
            .map()
            .install_publishable(Principal(2), heavy, slot)
            .unwrap();

        let admitted = engine
            .admit_one(
                TestRequest {
                    principal: Principal(2),
                    required: PermissionBits::bit(0),
                    op: &Op::Price,
                    items: 64,
                },
                t(0),
            )
            .expect("a quote within the account's burst is admissible");
        assert_eq!(admitted.quote.total, CostUnits(642));
        assert_eq!(
            admitted.funding.reservation().cancel(),
            CancelOutcome::ZeroCharged
        );
    }

    /// The counters must attribute each outcome to the right slot and leave
    /// every other slot alone. The sequence deliberately mixes reasons raised
    /// inside `admit_inner` with ones that only arrive through `?` from
    /// `AccountSnapshot::admit` and `Reservation::reserve` — the latter are
    /// exactly the ones per-site instrumentation would have missed.
    #[test]
    fn counters_attribute_every_outcome() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));

        // Two admissions: 1 item quotes 51 units, 14 items quote 64.
        engine.admit_one(request(1), t(0)).unwrap();
        engine.admit_one(request(14), t(0)).unwrap();
        // Raised in the pipeline itself.
        engine.admit_one(request(65), t(0)).unwrap_err();
        engine.admit_one(request(65), t(0)).unwrap_err();
        // Propagated out of `AccountSnapshot::admit`: staleness is decided
        // against `valid_until`, never by an inline refresh.
        assert_eq!(
            engine.admit_one(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        // Propagated out of `AccountSnapshot::admit`: permissions.
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
                        principal: Principal(1),
                        required: PermissionBits::bit(3),
                        op: &Op::Price,
                        items: 1,
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::MissingPermission
        );

        let snapshot = engine.counters().snapshot();
        assert_eq!(snapshot.admitted, 2);
        assert_eq!(snapshot.units_admitted, 115, "51 + 64 units quoted");
        assert_eq!(snapshot.denied(), 4);
        let denials: Vec<_> = snapshot
            .denials_by_name()
            .filter(|(_, count)| *count > 0)
            .collect();
        assert_eq!(
            denials,
            vec![
                ("snapshot_expired", 1),
                ("missing_permission", 1),
                ("request_too_large", 2),
            ],
            "each reason in its own slot, and nothing in the others"
        );
    }

    /// Every reason the engine can produce must reach its own slot. Six of
    /// these had no engine-level test before the counters needed one, so a
    /// reason could have been produced and never observed here.
    #[test]
    fn each_reason_reaches_its_own_slot() {
        // Closed and suspended accounts, and an unknown principal.
        for (status, expected) in [
            (AccountStatus::Suspended, DenyReason::AccountSuspended),
            (AccountStatus::Closed, DenyReason::AccountClosed),
        ] {
            let engine = engine_with(status, Some(10_000));
            assert_eq!(engine.admit_one(request(1), t(0)).unwrap_err(), expected);
            let snapshot = engine.counters().snapshot();
            assert_eq!(snapshot.denials[expected.index()], 1);
            assert_eq!(snapshot.denied(), 1, "exactly one slot moved");
        }

        // A lease past its expiry: `Reservation::reserve` refuses it, and the
        // reason propagates through `?`. The lease must lapse well before the
        // snapshot does, or the staleness check upstream would answer first
        // and this slot would never be reached.
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease_until(10_000, t(100))));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot)
            .unwrap();
        assert_eq!(
            engine.admit_one(request(1), t(200)).unwrap_err(),
            DenyReason::LeaseExpired
        );
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::LeaseExpired.index()],
            1
        );

        // Cost overflow: a table whose weight cannot be multiplied out.
        let mut overflowing = (*snapshot(AccountStatus::Active)).clone();
        overflowing.limits = ResolvedLimits::new(u64::MAX).with_weighted_rate(u64::MAX, u64::MAX);
        overflowing.cost_table = Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&Op::Price, CostUnits(u64::MAX / 2))
                .build(),
        );
        let overflowing = Arc::new(overflowing);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(u64::MAX)));
        engine
            .map()
            .install(Principal(1), overflowing, slot)
            .unwrap();
        assert_eq!(
            engine.admit_one(request(4), t(0)).unwrap_err(),
            DenyReason::CostOverflow
        );
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::CostOverflow.index()],
            1
        );

        // AccountingBackpressure is decided before admission by the embedder,
        // so the engine cannot raise it — recording it is the caller's job,
        // and the slot exists for exactly that (INVARIANTS.md GL-8).
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        engine
            .counters()
            .record_deny(&DenyReason::AccountingBackpressure);
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::AccountingBackpressure.index()],
            1
        );
    }

    /// A denial charges zero units, so it must never move `units_admitted` —
    /// the counter an operator is most likely to misread as money.
    #[test]
    fn denied_requests_add_no_units() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        // Held, not dropped: an uncommitted reservation returns its units on
        // drop, so releasing it here would refill the lease and the next
        // request would be admitted instead of refused.
        let held = engine.admit_one(request(1), t(0)).unwrap();
        // The lease now has 9 units left: the next request is refused.
        engine.admit_one(request(1), t(0)).unwrap_err();

        let snapshot = engine.counters().snapshot();
        assert_eq!(snapshot.admitted, 1);
        assert_eq!(snapshot.units_admitted, 51);
        assert_eq!(
            snapshot.denials[DenyReason::LeaseExhausted {
                remaining: CostUnits(9)
            }
            .index()],
            1
        );
        drop(held);
    }

    /// The lease slot is shared: a refill installed after a cold-start deny
    /// admits without reinstalling the snapshot.
    #[test]
    fn refill_after_cold_start_recovers() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        engine
            .map()
            .install(
                Principal(1),
                snapshot(AccountStatus::Active),
                Arc::clone(&slot),
            )
            .unwrap();
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
        drop(slot.replace(lease(10_000)));
        engine.admit_one(request(1), t(0)).unwrap();
    }

    /// Work permission is checked at stage two, and refusing costs nothing.
    ///
    /// `begin` cannot make this decision: which classes a request touches is a
    /// property of its decoded workload. The account here passes the route
    /// check and still may not run the class it asked for.
    #[test]
    fn a_workload_requiring_ungranted_bits_is_denied_at_stage_two() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000)));
        engine
            .map()
            .install(Principal(1), snapshot_without_work_permission(), slot)
            .unwrap();

        // Stage one succeeds: the route permission is granted.
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("route permission is granted");

        let denied = context
            .admit(&[(Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect_err("the class requires a bit the account lacks");
        assert_eq!(denied, DenyReason::MissingPermission);
    }

    /// A class the account *is* entitled to still admits.
    #[test]
    fn a_workload_within_granted_bits_admits() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        drop(slot.replace(lease(1_000)));
        let snapshot = Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                t(10_000),
                PermissionBits::bit(0).union(PermissionBits::bit(5)),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .class(&Op::Price, CostUnits(1), PermissionBits::bit(5))
                        .build(),
                ),
            )
            .build(),
        );
        engine.map().install(Principal(1), snapshot, slot).unwrap();

        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("route permission is granted");
        let pending = context
            .admit(&[(Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect("the account holds the class bit");
        assert_eq!(pending.quote().total, CostUnits(51));
    }
}
