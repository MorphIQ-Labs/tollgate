//! Pure generation-ordering model shared by the cache implementations.
//!
//! `formal/lean/Tollgate/SnapshotCache.lean` proves the corresponding state
//! transitions preserve watermark monotonicity and reject resurrection after
//! negative-entry eviction. Keeping the production decision functions pure
//! makes the proof-to-code correspondence inspectable and property-testable.

use tollgate_core::Generation;

/// Decide whether a positive snapshot may replace the visible state and
/// return the next durable watermark.
pub(crate) fn accept_positive(
    current: Option<Generation>,
    incoming: Generation,
) -> (Option<Generation>, bool) {
    match current {
        Some(current) if incoming <= current => (Some(current), false),
        _ => (Some(incoming), true),
    }
}

/// Decide whether an authoritative negative may replace the visible state.
/// `None` means "never known": it may deny locally but carries no generation
/// and therefore cannot lower an existing watermark.
pub(crate) fn accept_negative(
    current: Option<Generation>,
    incoming: Option<Generation>,
) -> (Option<Generation>, bool) {
    match (current, incoming) {
        (current, None) => (current, true),
        (Some(current), Some(incoming)) if incoming < current => (Some(current), false),
        (_, Some(incoming)) => (Some(incoming), true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn accepted_transitions_never_lower_the_watermark(
            current in proptest::option::of(any::<u64>()),
            incoming in any::<u64>(),
        ) {
            let current = current.map(Generation);
            let (positive, _) = accept_positive(current, Generation(incoming));
            let (negative, _) = accept_negative(current, Some(Generation(incoming)));
            prop_assert!(positive >= current);
            prop_assert!(negative >= current);
        }

        #[test]
        fn revoked_generation_rejects_replay_after_visible_entry_is_evicted(
            current in any::<u64>(),
            revoked in any::<u64>(),
            replay in any::<u64>(),
        ) {
            prop_assume!(replay <= revoked);
            let (watermark, _) = accept_negative(
                Some(Generation(current)),
                Some(Generation(revoked)),
            );
            prop_assert_eq!(watermark, Some(Generation(current.max(revoked))));

            // Eviction changes only request-visible state, so the watermark
            // is passed unchanged to the next transition.
            let (after_replay, replay_accepted) =
                accept_positive(watermark, Generation(replay));
            prop_assert!(!replay_accepted);
            prop_assert_eq!(after_replay, watermark);
        }
    }
}
