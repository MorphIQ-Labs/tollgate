//! Periodic allowances: what an account is granted each period, and what
//! happens to what it did not spend.
//!
//! Tollgate stores a schedule and applies it; it does not interpret product
//! vocabulary. A plan that calls its allowance "included FCUs" and its period
//! "a calendar month" compiles to a [`BudgetSchedule`] here, and the ledger
//! knows only units and instants.
//!
//! The rollover itself is a control-plane operation on the store
//! (`AdminStore::roll_period`), not something this module performs: only the
//! store can make the deposit and the expiry one transaction, and only the
//! store can make two replicas racing a boundary produce one of each.

use jiff::civil::date;
use jiff::{Timestamp, ToSpan};

use crate::units::CostUnits;

/// How often an allowance is replenished.
///
/// One variant today. The enum exists rather than a bare "monthly" flag
/// because the calendar arithmetic differs per period in ways a duration
/// cannot express — months are not a fixed number of seconds — so a later
/// weekly or annual period is a variant here rather than a second field
/// somewhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Period {
    /// Boundaries at 00:00:00 UTC on the first of each calendar month.
    ///
    /// UTC and not a customer's local zone: a boundary that moved with a
    /// timezone would make "the 1st" ambiguous across a fleet, and two
    /// replicas in different regions could each believe they were first to
    /// roll. One instant, one boundary, everywhere.
    UtcCalendarMonth,
}

impl Period {
    /// Every period a rollover pass must sweep.
    ///
    /// A backend rolls one statement per entry, because the boundary is a
    /// property of the period: a weekly schedule and a monthly one are due at
    /// different instants. A variant missing from this list would simply never
    /// be rolled — silently, and only visibly as an account whose allowance
    /// stopped arriving — so `every_period_is_swept` matches exhaustively over
    /// the enum to make the omission a compile error instead.
    pub const ALL: &'static [Period] = &[Period::UtcCalendarMonth];

    /// The stored name of this period, for backends that persist a schedule.
    ///
    /// A name and not an ordinal, for the reason [`AccountStatus::as_str`]
    /// gives: a variant added ahead of this one in the enum would silently
    /// re-label every stored row, where an unrecognized name is a decode error
    /// the backend reports.
    ///
    /// [`AccountStatus::as_str`]: crate::AccountStatus::as_str
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Period::UtcCalendarMonth => "utc_calendar_month",
        }
    }

    /// The first instant of the period containing `now`.
    ///
    /// This is the value the ledger stamps and compares against, so it is the
    /// definition of "which period is this": two instants belong to the same
    /// period exactly when this returns the same answer for both.
    #[must_use]
    pub fn start_of(self, now: Timestamp) -> Timestamp {
        match self {
            Period::UtcCalendarMonth => {
                let zoned = now.to_zoned(jiff::tz::TimeZone::UTC);
                date(zoned.year(), zoned.month(), 1)
                    .to_zoned(jiff::tz::TimeZone::UTC)
                    .expect("the first of a month is a valid civil date in UTC")
                    .timestamp()
            }
        }
    }

    /// The first instant of the period after the one containing `now`, which
    /// is also the instant this period's allowance stops being spendable.
    ///
    /// Half-open, like every other deadline here: an instant exactly at the
    /// boundary belongs to the *new* period.
    ///
    /// Month arithmetic is `jiff`'s problem rather than ours — 31 January plus
    /// one month, February in a leap year, and December's wrap into the next
    /// year are exactly the cases a hand-rolled version gets wrong.
    #[must_use]
    pub fn end_after(self, now: Timestamp) -> Timestamp {
        match self {
            Period::UtcCalendarMonth => {
                let start = self.start_of(now);
                start
                    .to_zoned(jiff::tz::TimeZone::UTC)
                    .checked_add(1.month())
                    .expect("one month past a month start is representable")
                    .timestamp()
            }
        }
    }
}

/// What happens to an allowance's unspent units at a period boundary.
///
/// One variant today, and it is the one the product needs: an allowance that
/// resets. Carry-over is a variant here when something asks for it, not a
/// boolean that would leave "how much carries over" unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Rollover {
    /// Unspent allowance does not carry over: at the boundary it is expired
    /// and the new period starts at exactly `allowance`.
    #[default]
    None,
}

impl Rollover {
    /// The stored name of this rule. See [`Period::as_str`].
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Rollover::None => "none",
        }
    }
}

/// An account's periodic allowance.
///
/// Absent means "no schedule": the account keeps the manual-deposit behaviour
/// it has always had, and a rollover pass leaves it alone. That is why this is
/// stored as an `Option` on the account rather than as a schedule with a zero
/// allowance — zero is a schedule that expires everything each month, which is
/// a very different thing from having no schedule at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BudgetSchedule {
    /// Units deposited at each period boundary.
    pub allowance: CostUnits,
    pub period: Period,
    pub rollover: Rollover,
}

impl BudgetSchedule {
    /// A monthly allowance that does not carry over — the shape every
    /// consumer wants today, named so a caller does not have to spell out two
    /// single-variant enums to say it.
    #[must_use]
    pub const fn monthly(allowance: CostUnits) -> Self {
        BudgetSchedule {
            allowance,
            period: Period::UtcCalendarMonth,
            rollover: Rollover::None,
        }
    }
}

/// What an instance is told about its account's budget, carried by the
/// snapshot (GL-97).
///
/// A *projection of the ledger at publication*, not a live balance: the
/// request path performs no I/O, so this is the last thing the control plane
/// said, and it ages between refreshes. Readers combine it with what the
/// instance has spent since — see `estimate_remaining` in
/// `tollgate-admission` — and the result is an estimate that names itself one.
///
/// The store stamps it. A publisher cannot supply it, because a balance is not
/// a compiled policy decision the way permissions and limits are: it moves
/// constantly and has exactly one authority. That is why
/// [`AccountSnapshot`](crate::AccountSnapshot) has no builder setter for it,
/// and `PublishableSnapshot::with_budget` is the only way to attach one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BudgetView {
    /// Everything the account could still spend when this snapshot was
    /// published — its balance *plus* the unspent remainder of every active
    /// lease, because units out on lease are still the account's.
    ///
    /// Equivalently, and this is how a backend computes it in one row read:
    /// what the account was funded with, minus what it has consumed or lost.
    pub balance_at_publish: CostUnits,
    /// When the current period's allowance stops being spendable, for an
    /// account that has a [`BudgetSchedule`]. `None` means no schedule — the
    /// balance does not expire — and is not the same as "unknown".
    pub period_end: Option<Timestamp>,
}

/// Allocator evidence that all account funding has been consumed or lost,
/// including units held in leases. Missing usage cannot establish this proof.
/// A period end bounds its validity; no end means an unscheduled balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BalanceExhaustion {
    #[cfg_attr(feature = "serde", serde(deserialize_with = "required_period_end"))]
    pub period_end: Option<Timestamp>,
}

/// Allocator evidence of an account's remaining funding: what it was funded
/// with minus what it has consumed or lost, including units held in leases.
///
/// An upper bound on what the account can still spend. Unreported
/// consumption can only lower true remaining funding, so a quote above
/// `remaining` cannot be funded until new funding or a new period arrives.
/// The converse does not hold: a quote within it may still find no lease.
/// `period_end` bounds validity as it does for [`BalanceExhaustion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BalanceShortfall {
    pub remaining: CostUnits,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "required_period_end"))]
    pub period_end: Option<Timestamp>,
}

impl BalanceShortfall {
    /// Zero remaining is exhaustion, the one shortfall no quote survives.
    #[must_use]
    pub fn exhaustion(self) -> Option<BalanceExhaustion> {
        self.remaining.is_zero().then_some(BalanceExhaustion {
            period_end: self.period_end,
        })
    }
}

impl From<BalanceExhaustion> for BalanceShortfall {
    fn from(evidence: BalanceExhaustion) -> Self {
        BalanceShortfall {
            remaining: CostUnits::ZERO,
            period_end: evidence.period_end,
        }
    }
}

#[cfg(feature = "serde")]
fn required_period_end<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

impl BudgetView {
    /// Called by a store against its transaction's current ledger, never an
    /// instance's stale snapshot or remaining-balance estimate. Exhaustion is
    /// [`BalanceShortfall::exhaustion`] of the result.
    #[must_use]
    pub fn shortfall(self) -> BalanceShortfall {
        BalanceShortfall {
            remaining: self.balance_at_publish,
            period_end: self.period_end,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> Timestamp {
        s.parse().expect("a valid RFC 3339 instant")
    }

    #[test]
    fn a_month_starts_at_midnight_utc_on_the_first() {
        let period = Period::UtcCalendarMonth;
        assert_eq!(
            period.start_of(at("2026-03-17T09:41:12Z")),
            at("2026-03-01T00:00:00Z")
        );
        assert_eq!(
            period.start_of(at("2026-03-01T00:00:00Z")),
            at("2026-03-01T00:00:00Z"),
            "an instant exactly at a boundary belongs to the period it opens"
        );
    }

    /// The cases a hand-rolled month would get wrong, which is why this
    /// delegates to `jiff` rather than adding 30 days.
    #[test]
    fn month_ends_handle_short_months_leap_years_and_the_year_wrap() {
        let period = Period::UtcCalendarMonth;
        for (now, expected) in [
            // 31-day month into a 28-day one: the end is March, not "the 31st
            // of February".
            ("2026-01-31T23:59:59Z", "2026-02-01T00:00:00Z"),
            ("2026-02-14T00:00:00Z", "2026-03-01T00:00:00Z"),
            // Leap year: February has a 29th, and the boundary is still the
            // 1st of March.
            ("2028-02-29T12:00:00Z", "2028-03-01T00:00:00Z"),
            // The year wrap.
            ("2026-12-25T00:00:00Z", "2027-01-01T00:00:00Z"),
        ] {
            assert_eq!(
                period.end_after(at(now)),
                at(expected),
                "period containing {now} must end at {expected}"
            );
        }
    }

    /// Two instants are in the same period exactly when they share a start,
    /// which is the comparison the ledger's idempotency rests on.
    #[test]
    fn the_period_start_is_what_makes_two_instants_the_same_period() {
        let period = Period::UtcCalendarMonth;
        let early = period.start_of(at("2026-05-01T00:00:00Z"));
        let late = period.start_of(at("2026-05-31T23:59:59Z"));
        let next = period.start_of(at("2026-06-01T00:00:00Z"));
        assert_eq!(early, late);
        assert_ne!(late, next);
        assert_eq!(period.end_after(at("2026-05-31T23:59:59Z")), next);
    }

    /// A new period variant must join `ALL`, or nothing would ever roll it.
    /// The match is exhaustive on purpose: adding a variant stops this
    /// compiling, and the fix is one line in the list above.
    #[test]
    fn every_period_is_swept() {
        for period in Period::ALL {
            match period {
                Period::UtcCalendarMonth => {}
            }
        }
        assert_eq!(Period::ALL.len(), 1, "every variant is listed exactly once");
    }

    /// The names a backend writes into a row. Pinned, because changing one
    /// silently orphans every schedule already stored under the old spelling —
    /// a rename is a migration, not an edit.
    #[test]
    fn stored_names_are_stable() {
        assert_eq!(Period::UtcCalendarMonth.as_str(), "utc_calendar_month");
        assert_eq!(Rollover::None.as_str(), "none");
    }

    #[test]
    fn a_monthly_schedule_spells_out_both_defaults() {
        let schedule = BudgetSchedule::monthly(CostUnits(10_000));
        assert_eq!(schedule.allowance, CostUnits(10_000));
        assert_eq!(schedule.period, Period::UtcCalendarMonth);
        assert_eq!(schedule.rollover, Rollover::None);
    }
}
