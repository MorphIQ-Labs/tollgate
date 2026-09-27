/-!
Server maintenance readiness (GL-71). Each pass publication is atomic; stop is
an independent, terminal bit; channel closure witnesses task exit. This model
proves the predicate over observations, not scheduler fairness, backend progress,
network delivery, or Tokio's memory model. Rust tests separately exercise task
ownership, watch closure, HTTP observations and the u64 counter boundary.
-/
namespace Tollgate.ServerMaintenance

structure State where
  live : Bool
  stopping : Bool
  reclaim : Bool
  rollover : Bool

inductive Event
  | reclaim (succeeded : Bool)
  | rollover (succeeded : Bool)
  | stop
  | exit

def ready (s : State) : Bool := s.live && !s.stopping && s.reclaim && s.rollover

def step (s : State) : Event → State
  | .reclaim succeeded => { s with reclaim := succeeded }
  | .rollover succeeded => { s with rollover := succeeded }
  | .stop => { s with stopping := true }
  | .exit => { s with live := false }

def run (s : State) : List Event → State
  | [] => s
  | event :: rest => run (step s event) rest

def initial : State := ⟨true, false, false, false⟩

theorem startup_is_not_ready : ready initial = false := by rfl

theorem a_failed_pass_withdraws_readiness (s : State) :
    ready (step s (.reclaim false)) = false ∧
    ready (step s (.rollover false)) = false := by
  simp [ready, step]

theorem the_other_pass_cannot_repair_a_failure (s : State) :
    ready (step (step s (.reclaim false)) (.rollover true)) = false ∧
    ready (step (step s (.rollover false)) (.reclaim true)) = false := by
  simp [ready, step]

theorem both_passes_recover_a_live_running_task (s : State)
    (live : s.live = true) (running : s.stopping = false) :
    ready (step (step s (.reclaim true)) (.rollover true)) = true := by
  simp [ready, step, live, running]

theorem stopping_is_preserved (s : State) (e : Event) (h : s.stopping = true) :
    (step s e).stopping = true := by
  cases e <;> simp [step, h]

theorem exit_is_preserved (s : State) (e : Event) (h : s.live = false) :
    (step s e).live = false := by
  cases e <;> simp [step, h]

theorem no_trace_can_restore_readiness_after_stop (s : State) (events : List Event)
    (h : s.stopping = true) : ready (run s events) = false := by
  induction events generalizing s with
  | nil => simp [run, ready, h]
  | cons e es ih => exact ih (step s e) (stopping_is_preserved s e h)

theorem no_trace_can_restore_readiness_after_exit (s : State) (events : List Event)
    (h : s.live = false) : ready (run s events) = false := by
  induction events generalizing s with
  | nil => simp [run, ready, h]
  | cons e es ih => exact ih (step s e) (exit_is_preserved s e h)

end Tollgate.ServerMaintenance
