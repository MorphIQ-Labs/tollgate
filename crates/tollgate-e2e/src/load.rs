//! Continuous customer traffic, and what must hold while it runs.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::StatusCode;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tollgate_core::AccountId;
use tollgate_store::wire::AccountResponse;

use crate::{App, ControlPlane, Customer, DEADLINE, Refused, eventually};

/// No request in any scenario costs more than this.
const PRICE_CEILING: u64 = 1_000;

/// What one customer's requests were charged, kept so that a ledger read
/// concurrent with traffic can still be bounded from both sides.
#[derive(Default)]
pub struct Tally {
    /// Units stated by responses received.
    charged: AtomicU64,
    /// `charged`, plus the most that requests still unanswered can cost.
    ceiling: AtomicU64,
    served: AtomicU64,
    last: AtomicU64,
    refusals: Mutex<BTreeMap<String, u64>>,
}

impl Tally {
    /// Price one contract, counting the outcome.
    ///
    /// # Errors
    ///
    /// The service's refusal.
    ///
    /// # Panics
    ///
    /// When a request costs more than any policy here allows.
    pub async fn price(&self, app: &App, secret: &str) -> Result<u64, Refused> {
        self.ceiling.fetch_add(PRICE_CEILING, Ordering::SeqCst);
        let outcome = app.price(secret).await;
        let units = *outcome.as_ref().unwrap_or(&0);
        assert!(units <= PRICE_CEILING, "a request cost {units}");
        self.charged.fetch_add(units, Ordering::SeqCst);
        self.ceiling
            .fetch_sub(PRICE_CEILING - units, Ordering::SeqCst);
        match &outcome {
            Ok(units) => {
                self.served.fetch_add(1, Ordering::SeqCst);
                self.last.store(*units, Ordering::SeqCst);
            }
            Err(refused) => {
                let code = refused.code.clone().unwrap_or_default();
                *self
                    .refusals
                    .lock()
                    .expect("refusal lock")
                    .entry(code)
                    .or_default() += 1;
            }
        }
        outcome
    }

    /// Units that responses have stated.
    #[must_use]
    pub fn charged(&self) -> u64 {
        self.charged.load(Ordering::SeqCst)
    }

    /// The most that can have been charged, counting unanswered requests.
    #[must_use]
    pub fn ceiling(&self) -> u64 {
        self.ceiling.load(Ordering::SeqCst)
    }

    /// Requests served.
    #[must_use]
    pub fn served(&self) -> u64 {
        self.served.load(Ordering::SeqCst)
    }

    /// What the latest served response was charged.
    #[must_use]
    pub fn last_charge(&self) -> u64 {
        self.last.load(Ordering::SeqCst)
    }

    /// Refusals so far with any of `codes`.
    ///
    /// # Panics
    ///
    /// If another thread panicked while counting a refusal.
    #[must_use]
    pub fn refusals(&self, codes: &[&str]) -> u64 {
        let refusals = self.refusals.lock().expect("refusal lock");
        codes.iter().filter_map(|code| refusals.get(*code)).sum()
    }

    /// Every refusal so far, by code.
    ///
    /// # Panics
    ///
    /// If another thread panicked while counting a refusal.
    #[must_use]
    pub fn refusals_by_code(&self) -> BTreeMap<String, u64> {
        self.refusals.lock().expect("refusal lock").clone()
    }
}

/// Customers sending requests one after another until stopped.
///
/// Each worker is a sequential client of one instance, so it requires of every
/// response what holds however far behind that instance is: a refusal is for
/// want of funding or capacity, never of identity, and its price never falls,
/// which with prices that only rise means the instance's policy generation
/// never moves backwards (INVARIANTS.md GL-15).
pub struct Load {
    stop: watch::Sender<bool>,
    workers: JoinSet<()>,
}

impl Default for Load {
    fn default() -> Self {
        Load {
            stop: watch::Sender::new(false),
            workers: JoinSet::new(),
        }
    }
}

impl Load {
    /// Add one client of `app`, pausing `pause` between requests. The customer
    /// must already be served by that instance.
    pub fn client(&mut self, app: Arc<App>, secret: String, tally: Arc<Tally>, pause: Duration) {
        let stop = self.stop.subscribe();
        self.workers.spawn(async move {
            let mut floor = 0;
            while !*stop.borrow() {
                match tally.price(&app, &secret).await {
                    Ok(units) => {
                        assert!(units >= floor, "a price fell from {floor} to {units}");
                        floor = units;
                    }
                    Err(refused) => assert!(
                        [
                            StatusCode::PAYMENT_REQUIRED,
                            StatusCode::TOO_MANY_REQUESTS,
                            StatusCode::SERVICE_UNAVAILABLE,
                        ]
                        .contains(&refused.status),
                        "an authorized customer was refused: {refused:?}"
                    ),
                }
                tokio::time::sleep(pause).await;
            }
        });
    }

    /// Fail now if a client has failed.
    ///
    /// # Panics
    ///
    /// With the client's own panic.
    pub fn check(&mut self) {
        while let Some(finished) = self.workers.try_join_next() {
            surface(finished);
        }
    }

    /// Stop every client once its request in flight is answered.
    ///
    /// # Panics
    ///
    /// With the first client's panic, if one failed.
    pub async fn stop(mut self) {
        self.stop.send_replace(true);
        while let Some(finished) = self.workers.join_next().await {
            surface(finished);
        }
    }
}

fn surface(finished: Result<(), JoinError>) {
    if let Err(error) = finished {
        std::panic::resume_unwind(error.into_panic());
    }
}

/// A strict account's ledger, observed repeatedly while traffic runs.
pub struct LedgerWatch {
    account: AccountId,
    deposited: u64,
    /// The one further deposit allowed to appear, while a boundary is crossed.
    arriving: Option<u64>,
    previous: Option<AccountResponse>,
}

impl LedgerWatch {
    /// Watch an account whose deposits stand at `deposited`.
    #[must_use]
    pub fn new(account: AccountId, deposited: u64) -> Self {
        LedgerWatch {
            account,
            deposited,
            arriving: None,
            previous: None,
        }
    }

    /// Deposits as last confirmed.
    #[must_use]
    pub fn deposited(&self) -> u64 {
        self.deposited
    }

    /// A boundary is about to be crossed: exactly `allowance` more may be
    /// deposited, once.
    pub fn expect_deposit(&mut self, allowance: u64) {
        assert!(self.arriving.is_none(), "a boundary is already open");
        self.arriving = Some(self.deposited + allowance);
    }

    /// Read the ledger and require what holds at every observation: it
    /// conserves; its totals only grow; deposits are exactly what was funded;
    /// nothing is billed that was not executed; and what was executed or
    /// expired was funded (INVARIANTS.md GL-1, GL-7, GL-28).
    ///
    /// `tallies` are every client of this account.
    ///
    /// # Panics
    ///
    /// When any of those does not hold.
    pub async fn observe(&mut self, plane: &ControlPlane, tallies: &[&Tally]) -> AccountResponse {
        let executed: u64 = tallies.iter().map(|tally| tally.charged()).sum();
        let view = plane.ledger(self.account).await;
        let at_most: u64 = tallies.iter().map(|tally| tally.ceiling()).sum();
        let deposited = view.deposited.get();
        if Some(deposited) == self.arriving {
            self.deposited = deposited;
            self.arriving = None;
        }
        assert_eq!(deposited, self.deposited, "unexpected deposits: {view:?}");
        assert_eq!(view.overage_recorded.get(), 0, "strict: {view:?}");
        assert!(
            executed + view.expired_allowance.get() <= deposited,
            "{executed} executed beside what expired exceeds the funding: {view:?}"
        );
        assert!(
            view.settled_usage.get() <= at_most,
            "billed more than the {at_most} executed: {view:?}"
        );
        if let Some(previous) = &self.previous {
            assert!(
                view.settled_usage >= previous.settled_usage
                    && view.expired_allowance >= previous.expired_allowance,
                "a ledger total fell: {previous:?} then {view:?}"
            );
        }
        self.previous = Some(view);
        view
    }
}

/// With the customer's traffic stopped, wait for every instance's estimate to
/// reach the ledger's figure once all executed usage is delivered
/// (INVARIANTS.md GL-28).
pub async fn estimates_reach_ledger(
    plane: &ControlPlane,
    apps: &[&App],
    customer: &Customer,
    executed: u64,
) {
    eventually("the estimates to reach the ledger", DEADLINE, || async {
        let view = plane.ledger(customer.account).await;
        let truth = (view.deposited.get() + view.overage_recorded.get())
            .checked_sub(executed + view.expired_allowance.get() + view.settlement_loss.get());
        let mut seen = Vec::with_capacity(apps.len());
        for app in apps {
            seen.push(app.budget(&customer.secret).await);
        }
        // Loss awaiting late usage counts those units twice until it arrives.
        if truth.is_some() && seen.iter().all(|seen| *seen == truth) {
            Ok(())
        } else {
            Err(format!("{seen:?}, expected {truth:?} from {view:?}"))
        }
    })
    .await;
}
