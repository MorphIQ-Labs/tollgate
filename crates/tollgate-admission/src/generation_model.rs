//! Pure generation-ordering model shared by the cache implementations.
//!
//! `formal/lean/Tollgate/SnapshotCache.lean` proves the corresponding state
//! transitions preserve watermark monotonicity and reject resurrection after
//! negative-entry eviction. Keeping the production decision functions pure
//! makes the proof-to-code correspondence inspectable and property-testable.
//!
//! The central distinction is [`Watermark`]'s: a generation this instance
//! merely *observed* is not the same fact as a generation the source
//! *published a revocation at*, and only the second may refuse an equal
//! generation. Collapsing the two is GL-53 — a principal whose row briefly went
//! absent could never be restored, because the absence inherited the
//! generation of the positive it replaced and then refused it back.
//!
//! **Public on purpose.** `SnapshotManager` applies the same rule one layer up,
//! before it ever reaches a map, so it calls these functions rather than
//! keeping a second copy of the comparison. Two copies that must agree is how
//! GL-53 stayed invisible: the client's gate short-circuited first, and fixing
//! the map alone would have changed nothing.

use tollgate_core::Generation;

/// A principal's durable generation, and why it is durable.
///
/// The two variants take different rules and that asymmetry is the whole
/// point:
///
/// - [`Watermark::Revoked`] is a statement the source published — "this
///   generation is dead". A positive *at* it must be refused, or a replayed
///   snapshot resurrects a revoked credential (INVARIANTS.md GL-15).
/// - [`Watermark::Positive`] is only what this instance last saw. It orders
///   snapshots so a delayed older one cannot roll the account back, but it
///   asserts nothing about the generation being dead — so the same generation
///   arriving again is a re-observation, not a resurrection.
///
/// The rule the variants encode: a watermark's tag must say what the *source*
/// said. `Revoked(g)` means a revocation at `g` was published; `Positive(g)`
/// means a positive at `g` was observed. Mis-tagging one as the other is
/// precisely the defect — an absence laundered into a revocation.
///
/// That rule is a convention here, not a construction. Both variants are
/// public because the client builds a `Positive` from the snapshot it just
/// installed, which is the same observation [`accept_positive`] records; a
/// sealed constructor would not prevent a caller picking the wrong one anyway.
/// What *is* enforced is the transitions: [`accept_unknown`] returns `current`
/// untouched rather than re-tagging it, and [`accept_revoked`] is the only
/// function that produces `Revoked`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Watermark {
    /// The source published a revocation at this generation.
    Revoked(Generation),
    /// The newest positive this instance has installed.
    Positive(Generation),
}

impl Watermark {
    /// The generation the watermark holds, whichever variant carries it.
    #[must_use]
    pub fn generation(self) -> Generation {
        match self {
            Watermark::Revoked(generation) | Watermark::Positive(generation) => generation,
        }
    }

    /// Whether this is a published revocation, which refuses a positive at its
    /// own generation as well as older ones.
    #[must_use]
    pub fn is_revoked(self) -> bool {
        matches!(self, Watermark::Revoked(_))
    }
}

/// Decide whether a positive snapshot may replace the visible state and
/// return the next durable watermark.
///
/// `visible` is whether the principal currently has a request-visible positive
/// entry. It is what keeps a duplicate publish of a live snapshot an
/// idempotent no-op: without it, relaxing the equal-generation rule would turn
/// every republish into a fresh install and, on the copy-on-write map, a clone
/// of the whole map.
///
/// Refuse a strictly older generation always. Refuse an equal one only when it
/// is dead (a revocation) or already installed (visible). Otherwise accept —
/// which is what lets a principal return from an absence at the generation it
/// always had.
pub fn accept_positive(
    current: Option<Watermark>,
    incoming: Generation,
    visible: bool,
) -> (Option<Watermark>, bool) {
    let refused = current.is_some_and(|current| {
        incoming < current.generation()
            || (incoming == current.generation() && (current.is_revoked() || visible))
    });
    if refused {
        (current, false)
    } else {
        (Some(Watermark::Positive(incoming)), true)
    }
}

/// Decide whether a published revocation may replace the visible state.
///
/// Accepts at equality, unlike [`accept_positive`]: re-revoking at the same
/// generation is the source restating a fact, and the watermark it leaves is
/// the one that must refuse a replay.
pub fn accept_revoked(
    current: Option<Watermark>,
    incoming: Generation,
) -> (Option<Watermark>, bool) {
    match current {
        Some(current) if incoming < current.generation() => (Some(current), false),
        _ => (Some(Watermark::Revoked(incoming)), true),
    }
}

/// Decide whether an absent row may replace the visible state.
///
/// Always accepted — an absence denies locally — and it **never touches the
/// watermark**. The source said nothing about any generation, so there is
/// nothing here to raise, lower, or re-tag. In particular an absence landing
/// on a [`Watermark::Positive`] leaves it a `Positive`: turning it into a
/// `Revoked` would assert a revocation nobody published, and would strand the
/// principal at its own generation (GL-53).
pub fn accept_unknown(current: Option<Watermark>) -> (Option<Watermark>, bool) {
    (current, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The case GL-53 was: observe a generation, lose the row, see the same
    /// generation again. Nothing was revoked, so nothing may refuse it.
    #[test]
    fn a_generation_survives_an_absence_and_returns_unchanged() {
        let (after_install, installed) = accept_positive(None, Generation(5), false);
        assert!(installed);
        let (after_absence, denied) = accept_unknown(after_install);
        assert!(denied, "an absence always denies locally");
        assert_eq!(
            after_absence,
            Some(Watermark::Positive(Generation(5))),
            "an absence must not re-tag an observation as a revocation"
        );

        let (_, restored) = accept_positive(after_absence, Generation(5), false);
        assert!(restored, "the principal returns at the generation it had");
    }

    /// The case that must NOT loosen: a real revocation refuses its own
    /// generation back, which is INVARIANTS.md GL-15.
    #[test]
    fn a_revocation_refuses_its_own_generation_back() {
        let (revoked, _) = accept_revoked(Some(Watermark::Positive(Generation(5))), Generation(5));
        assert_eq!(revoked, Some(Watermark::Revoked(Generation(5))));

        let (after, accepted) = accept_positive(revoked, Generation(5), false);
        assert!(!accepted, "a replay at the tombstone's generation is dead");
        assert_eq!(after, revoked, "and the refusal moves nothing");
    }

    /// A duplicate publish of a *live* snapshot stays a no-op, so relaxing the
    /// equal-generation rule does not turn every republish into a full
    /// copy-on-write install.
    #[test]
    fn a_visible_snapshot_refuses_its_own_generation_again() {
        let current = Some(Watermark::Positive(Generation(5)));
        let (after, accepted) = accept_positive(current, Generation(5), true);
        assert!(!accepted);
        assert_eq!(after, current);
    }

    proptest! {
        #[test]
        fn accepted_transitions_never_lower_the_watermark(
            current in proptest::option::of(any::<u64>()),
            incoming in any::<u64>(),
            revoked_current in any::<bool>(),
            visible in any::<bool>(),
        ) {
            let current = current.map(|generation| if revoked_current {
                Watermark::Revoked(Generation(generation))
            } else {
                Watermark::Positive(Generation(generation))
            });
            let (positive, _) = accept_positive(current, Generation(incoming), visible);
            let (revoked, _) = accept_revoked(current, Generation(incoming));
            let (unknown, _) = accept_unknown(current);
            let floor = current.map(Watermark::generation);
            prop_assert!(positive.map(Watermark::generation) >= floor);
            prop_assert!(revoked.map(Watermark::generation) >= floor);
            prop_assert_eq!(unknown, current);
        }

        #[test]
        fn revoked_generation_rejects_replay_after_visible_entry_is_evicted(
            current in any::<u64>(),
            revoked in any::<u64>(),
            replay in any::<u64>(),
        ) {
            prop_assume!(replay <= revoked);
            let (watermark, _) = accept_revoked(
                Some(Watermark::Positive(Generation(current))),
                Generation(revoked),
            );
            // Only an accepted revocation makes a generation dead. A delayed
            // older one is refused and leaves the observation it found --
            // which is a change from the pre-#53 model, where refusing still
            // left something the next positive read as a tombstone.
            let expected = if revoked < current {
                Watermark::Positive(Generation(current))
            } else {
                Watermark::Revoked(Generation(revoked))
            };
            prop_assert_eq!(watermark, Some(expected));

            // Eviction changes only request-visible state, so the watermark is
            // passed unchanged to the next transition -- and `visible: false`
            // is precisely the evicted case, which must still refuse.
            let (after_replay, replay_accepted) =
                accept_positive(watermark, Generation(replay), false);
            prop_assert!(!replay_accepted);
            prop_assert_eq!(after_replay, watermark);
        }

        /// An absence never converts an observation into a revocation, at any
        /// generation. This is the property whose failure is GL-53.
        #[test]
        fn an_absence_never_makes_a_generation_dead(
            observed in any::<u64>(),
        ) {
            let (installed, _) = accept_positive(None, Generation(observed), false);
            let (after_absence, _) = accept_unknown(installed);
            let (_, returned) = accept_positive(after_absence, Generation(observed), false);
            prop_assert!(returned);
        }
    }
}
