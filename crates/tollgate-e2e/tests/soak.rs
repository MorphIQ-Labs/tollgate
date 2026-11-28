//! Sustained load across lease renewal, snapshot refresh and budget-period
//! boundaries, on a deployment of three instances.
//!
//! Long running and local: ignored by default, so neither `cargo test` nor the
//! pull-request gate runs it. Run `./scripts/check_e2e_soak.sh`;
//! `TOLLGATE_E2E_SOAK_SECS` sets how long load is sustained (default 600).
//!
//! A boundary is crossed by moving the stored period markers back a month
//! ([`ControlPlane::end_budget_period`]). Everything that follows is the
//! deployment's own: the server's maintenance pass deposits and expires,
//! leases from the closed period settle, and instances refresh. No instance's
//! clock passes a period end, so what depends on that is not exercised here.

#![allow(
    clippy::disallowed_methods,
    reason = "a scenario over real processes reads the wall clock they run on"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use jiff::{SignedDuration, Timestamp};
use tollgate_core::{AccountId, Period};
use tollgate_e2e::{
    AppOptions, ControlPlane, Customer, DEADLINE, LedgerWatch, Load, Policy, Settings, Tally,
    estimates_reach_ledger, eventually,
};
use tollgate_store::wire::AccountResponse;

const INSTANCES: usize = 3;
/// How often a period ends.
const PERIOD: Duration = Duration::from_secs(45);
/// How often every policy is republished, at a higher price.
const REPUBLISH: Duration = Duration::from_secs(7);
/// Far shorter than the run: an instance that stopped refreshing would refuse.
const VALIDITY: SignedDuration = SignedDuration::from_secs(30);
const LEASE_TTL: Duration = Duration::from_secs(6);
const LEASE_UNITS: u64 = 2_000;
/// How long a condition may take before the run is declared stuck. Generous,
/// since nothing is asserted of speed and the host runs other work.
const PATIENCE: Duration = Duration::from_secs(120);
/// Refusals that say funding, rather than capacity, ran out.
const UNFUNDED: [&str; 3] = [
    "quota-exhausted",
    "balance-exhausted",
    "balance-insufficient",
];

/// The accounts under load, by their place in [`PLAN`].
const AMPLE: usize = 0;
const TIGHT: usize = 1;
const SPARSE: usize = 2;
const STEADY: usize = 3;

struct Plan {
    name: &'static str,
    /// Manual units, which no boundary may touch.
    top_up: u64,
    allowance: Option<u64>,
    clients_per_instance: usize,
    /// Each client's pause between requests.
    pause_ms: u64,
}

const PLAN: [Plan; 4] = [
    // Allowance left over at every boundary, beside a top-up.
    Plan {
        name: "ample",
        top_up: 5_000_000,
        allowance: Some(1_000_000_000),
        clients_per_instance: 2,
        pause_ms: 2,
    },
    // Spent out every period, so only a new one restores service.
    Plan {
        name: "tight",
        top_up: 0,
        allowance: Some(100_000),
        clients_per_instance: 1,
        pause_ms: 5,
    },
    // Barely used, so leases straddle boundaries holding unspent units.
    Plan {
        name: "sparse",
        top_up: 0,
        allowance: Some(1_000_000),
        clients_per_instance: 1,
        pause_ms: 1_500,
    },
    // No schedule: other accounts' boundaries must not touch it.
    Plan {
        name: "steady",
        top_up: 1_000_000_000,
        allowance: None,
        clients_per_instance: 1,
        pause_ms: 10,
    },
];

/// One account under load.
struct Account {
    plan: &'static Plan,
    customer: Customer,
    /// One tally for each instance.
    tallies: Vec<Arc<Tally>>,
    watch: LedgerWatch,
    /// What had been charged when the open period began.
    charged_before_period: u64,
}

impl Account {
    fn charged(&self) -> u64 {
        self.tallies.iter().map(|tally| tally.charged()).sum()
    }

    async fn observe(&mut self, plane: &ControlPlane) -> AccountResponse {
        let tallies: Vec<&Tally> = self.tallies.iter().map(Arc::as_ref).collect();
        self.watch.observe(plane, &tallies).await
    }

    fn served(&self) -> Vec<u64> {
        self.tallies.iter().map(|tally| tally.served()).collect()
    }

    fn unfunded(&self) -> Vec<u64> {
        self.tallies
            .iter()
            .map(|tally| tally.refusals(&UNFUNDED))
            .collect()
    }
}

/// Whether every instance's count has passed its mark.
fn each_past(counts: &[u64], marks: &[u64]) -> bool {
    counts.iter().zip(marks).all(|(count, mark)| count > mark)
}

struct Deployment {
    plane: ControlPlane,
    apps: Vec<Arc<tollgate_e2e::App>>,
    accounts: Vec<Account>,
    load: Load,
    policy: Policy,
    /// When the next republication is due, or `None` once the last is out.
    republish_at: Option<Instant>,
    /// Instances' latest estimates of the account never funded again.
    steady_estimates: Vec<u64>,
    boundaries: u64,
    /// Instances' refusals of the tight account before the open period.
    unfunded_before: Vec<u64>,
    /// What the sparse account had expired just after the latest boundary.
    sparse_expired: Option<u64>,
    /// Periods after which a lease from the closed one expired more units.
    straddled: u64,
}

impl Deployment {
    async fn start(settings: Settings) -> Self {
        let plane = ControlPlane::start(settings).await;
        let mut accounts = Vec::new();
        for (id, plan) in (1..).zip(&PLAN) {
            let customer = plane.onboard(id, plan.top_up).await;
            let mut watch = LedgerWatch::new(customer.account, plan.top_up);
            if let Some(allowance) = plan.allowance {
                watch.expect_deposit(allowance);
                plane.set_allowance(customer.account, allowance).await;
                let funded = plan.top_up + allowance;
                plane
                    .ledger_reaches("the first allowance", customer.account, DEADLINE, |view| {
                        view.deposited.get() == funded
                    })
                    .await;
                watch.observe(&plane, &[]).await;
            }
            accounts.push(Account {
                plan,
                customer,
                tallies: (0..INSTANCES).map(|_| Arc::default()).collect(),
                watch,
                charged_before_period: 0,
            });
        }

        let options = AppOptions {
            lease_ttl: LEASE_TTL,
            lease_units: LEASE_UNITS,
            ..AppOptions::default()
        };
        let mut apps = Vec::new();
        for instance in 0..INSTANCES {
            let name = format!("app-{instance}");
            apps.push(Arc::new(plane.start_app(&name, options).await));
        }
        let mut load = Load::default();
        for account in &accounts {
            for (app, tally) in apps.iter().zip(&account.tallies) {
                eventually("the first priced response", DEADLINE, || async {
                    tally
                        .price(app, &account.customer.secret)
                        .await
                        .map_err(|refused| format!("{refused:?}"))
                })
                .await;
                for _ in 0..account.plan.clients_per_instance {
                    load.client(
                        Arc::clone(app),
                        account.customer.secret.clone(),
                        Arc::clone(tally),
                        Duration::from_millis(account.plan.pause_ms),
                    );
                }
            }
        }
        Deployment {
            plane,
            apps,
            accounts,
            load,
            policy: Policy {
                valid_for: VALIDITY,
                ..Policy::INITIAL
            },
            // Due at once: the onboarding policy is valid for an hour, and the
            // run depends on the short one.
            republish_at: Some(Instant::now()),
            steady_estimates: vec![u64::MAX; INSTANCES],
            boundaries: 0,
            unfunded_before: vec![0; INSTANCES],
            sparse_expired: None,
            straddled: 0,
        }
    }

    async fn republish(&mut self) {
        self.policy.generation += 1;
        self.policy.request_price += 1;
        for account in &self.accounts {
            self.plane.publish(&account.customer, self.policy).await;
        }
    }

    /// One round of observations of every account, and a republication when one
    /// is due.
    async fn observe(&mut self) -> Vec<AccountResponse> {
        self.load.check();
        if self.republish_at.is_some_and(|due| Instant::now() >= due) {
            self.republish().await;
            self.republish_at = Some(Instant::now() + REPUBLISH);
        }
        let mut views = Vec::with_capacity(self.accounts.len());
        for (index, account) in self.accounts.iter_mut().enumerate() {
            // Estimates first: each is an earlier reading of a ledger whose
            // funding only grows.
            let mut estimates = Vec::with_capacity(INSTANCES);
            for app in &self.apps {
                estimates.push(app.budget(&account.customer.secret).await);
            }
            let view = account.observe(&self.plane).await;
            let at_most: u64 = account.tallies.iter().map(|tally| tally.ceiling()).sum();
            for (instance, estimate) in estimates.into_iter().enumerate() {
                let Some(estimate) = estimate else { continue };
                assert!(
                    estimate <= view.deposited.get(),
                    "{}: an estimate of {estimate} exceeds all funding: {view:?}",
                    account.plan.name
                );
                if index != STEADY {
                    continue;
                }
                // Never funded again: never below what the ledger can come to
                // hold, and never rising (INVARIANTS.md GL-28).
                let floor = view
                    .deposited
                    .get()
                    .saturating_sub(at_most + view.settlement_loss.get());
                assert!(estimate >= floor, "{estimate} is below {floor}");
                let held = &mut self.steady_estimates[instance];
                assert!(estimate <= *held, "an estimate rose to {estimate}");
                *held = estimate;
            }
            views.push(view);
        }
        views
    }

    /// Observe until `done`.
    async fn until(&mut self, what: &str, done: impl Fn(&Self, &[AccountResponse]) -> bool) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            let views = self.observe().await;
            if done(self, &views) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {views:#?}\n{}",
                self.summary()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// End every scheduled account's period and require what a boundary owes:
    /// one allowance deposited once, unspent allowance expired and nothing
    /// else, and service restored to an account that had run out.
    async fn cross_boundary(&mut self) {
        let begun = Instant::now();
        // The tight account has run out and every instance has refused it, so
        // service beyond its present funding can only be the new allowance's.
        self.until("the tight account to run out", |deployment, views| {
            let tight = &deployment.accounts[TIGHT];
            let spendable = views[TIGHT].deposited.get() - views[TIGHT].expired_allowance.get();
            spendable.saturating_sub(tight.charged()) < LEASE_UNITS
                && each_past(&tight.unfunded(), &deployment.unfunded_before)
        })
        .await;

        let before = self.observe().await;
        if let Some(after_last) = self.sparse_expired
            && before[SPARSE].expired_allowance.get() > after_last
        {
            self.straddled += 1;
        }
        let mut closing: Vec<AccountId> = Vec::new();
        for account in &mut self.accounts {
            if let Some(allowance) = account.plan.allowance {
                account.watch.expect_deposit(allowance);
                account.charged_before_period = account.charged();
                closing.push(account.customer.account);
            }
        }
        // Counted while it cannot be served, before its new allowance.
        let exhausted = self.accounts[TIGHT].served();
        let ran_out = begun.elapsed();
        self.plane.end_budget_period(&closing).await;
        let moved = begun.elapsed();
        self.boundaries += 1;

        let current = Period::UtcCalendarMonth.start_of(Timestamp::now());
        self.until("the server to cross the boundary", |deployment, views| {
            deployment
                .accounts
                .iter()
                .zip(views.iter().zip(&before))
                .all(|(account, (view, before))| {
                    account.plan.allowance.is_none_or(|allowance| {
                        view.period_start == current
                            && view.deposited.get() == before.deposited.get() + allowance
                    })
                })
        })
        .await;
        let crossed = begun.elapsed();
        let after = self.observe().await;
        for (account, view) in self.accounts.iter().zip(&after) {
            let closed = account.plan.allowance.unwrap_or(0) * self.boundaries;
            assert!(
                view.expired_allowance.get() <= closed,
                "{}: more expired than the closed periods allowed: {view:?}",
                account.plan.name
            );
        }
        assert!(
            after[AMPLE].expired_allowance > before[AMPLE].expired_allowance,
            "unspent allowance did not expire: {:?}",
            after[AMPLE]
        );
        self.sparse_expired = Some(after[SPARSE].expired_allowance.get());

        // Eventually every instance serves every account again, and the
        // account that had run out is served more than it was ever funded.
        let mut marks: Vec<Vec<u64>> = self.accounts.iter().map(Account::served).collect();
        // It may spend the whole new allowance before this point.
        marks[TIGHT] = exhausted;
        let spent_out = before[TIGHT].deposited.get() - before[TIGHT].expired_allowance.get();
        self.until("service across the boundary", |deployment, _| {
            deployment.accounts[TIGHT].charged() > spent_out
                && deployment
                    .accounts
                    .iter()
                    .zip(&marks)
                    .all(|(account, marks)| each_past(&account.served(), marks))
        })
        .await;
        self.unfunded_before = self.accounts[TIGHT].unfunded();
        // Where the time went, for whoever reads a slow or failed run. Nothing
        // is asserted of it.
        eprintln!(
            "boundary {}: ran out after {ran_out:.1?}, ended after {moved:.1?}, \
             crossed after {crossed:.1?}, served after {:.1?}",
            self.boundaries,
            begun.elapsed()
        );
    }

    fn summary(&self) -> String {
        let mut summary = format!(
            "{} boundaries, {} straddled, {} publications",
            self.boundaries,
            self.straddled,
            self.policy.generation - 1
        );
        for account in &self.accounts {
            summary.push_str(&format!(
                "\n{}: deposited {} charged {}",
                account.plan.name,
                account.watch.deposited(),
                account.charged()
            ));
            for (instance, tally) in account.tallies.iter().enumerate() {
                summary.push_str(&format!(
                    "\n  app-{instance}: served {} refused {:?}",
                    tally.served(),
                    tally.refusals_by_code()
                ));
            }
        }
        summary
    }
}

fn sustained_for() -> Duration {
    let seconds = std::env::var("TOLLGATE_E2E_SOAK_SECS").map_or(600, |value| {
        value
            .parse()
            .expect("TOLLGATE_E2E_SOAK_SECS is a number of seconds")
    });
    let duration = Duration::from_secs(seconds);
    assert!(
        duration >= PERIOD * 3,
        "TOLLGATE_E2E_SOAK_SECS must cover at least three {PERIOD:?} periods"
    );
    duration
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-running and local: ./scripts/check_e2e_soak.sh"]
async fn sustained_load_across_renewal_refresh_and_period_boundaries() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let duration = sustained_for();
    // A real month boundary during the run would deposit an allowance the
    // scenario did not ask for.
    let now = Timestamp::now();
    let margin = SignedDuration::try_from(duration * 2).expect("a short run");
    assert!(
        now + margin < Period::UtcCalendarMonth.end_after(now),
        "a calendar month ends during this run; start it after the boundary"
    );

    let mut deployment = Deployment::start(settings).await;
    let started = Instant::now();
    let mut boundary_at = started + PERIOD;
    while started.elapsed() < duration {
        deployment.observe().await;
        if Instant::now() >= boundary_at {
            deployment.cross_boundary().await;
            boundary_at += PERIOD;
            eprintln!("{:>4}s elapsed", started.elapsed().as_secs());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // One last publication, then one last boundary to fund every account:
    // every instance comes to charge the last price for each of them. It
    // stays valid however long that takes, as nothing will follow it.
    deployment.republish_at = None;
    deployment.policy.valid_for = Policy::INITIAL.valid_for;
    deployment.republish().await;
    let price = deployment.policy.charge();
    deployment.cross_boundary().await;
    deployment
        .until("the last publication everywhere", |deployment, _| {
            deployment.accounts.iter().all(|account| {
                account
                    .tallies
                    .iter()
                    .all(|tally| tally.last_charge() == price)
            })
        })
        .await;
    eprintln!("{}", deployment.summary());

    // The run exercised what it claims to have.
    assert!(
        deployment.straddled > 0,
        "no lease from a closed period settled afterwards"
    );
    assert!(
        deployment.accounts[TIGHT]
            .tallies
            .iter()
            .map(|tally| tally.refusals(&["balance-exhausted", "balance-insufficient"]))
            .sum::<u64>()
            > 0,
        "no instance acted on the ledger's evidence that funding ran out"
    );
    let rotations = duration.as_secs() / LEASE_TTL.as_secs();
    for (instance, app) in deployment.apps.iter().enumerate() {
        let acquired = app.counter("/refill/acquired").await;
        let released = app.counter("/refill/released").await;
        eprintln!("app-{instance}: {acquired} leases acquired, {released} released");
        assert!(
            acquired >= rotations && released >= rotations,
            "leases were not renewed throughout"
        );
    }

    let Deployment {
        plane,
        apps,
        accounts,
        load,
        boundaries,
        ..
    } = deployment;
    load.stop().await;
    let instances: Vec<_> = apps.iter().map(Arc::as_ref).collect();
    for account in &accounts {
        estimates_reach_ledger(&plane, &instances, &account.customer, account.charged()).await;
    }
    drop(instances);
    for app in apps {
        Arc::into_inner(app)
            .expect("the load has released it")
            .terminate()
            .await;
    }
    for mut account in accounts {
        let closed = account.observe(&plane).await;
        let allowance = account.plan.allowance.unwrap_or(0);
        // The first allowance, and one for each boundary: never two.
        let deposited = account.plan.top_up
            + allowance * (boundaries + u64::from(account.plan.allowance.is_some()));
        assert_eq!(closed.deposited.get(), deposited, "{closed:?}");
        assert_eq!(closed.settled_usage.get(), account.charged(), "{closed:?}");
        assert_eq!(closed.outstanding_lease_grants.get(), 0, "{closed:?}");
        assert_eq!(closed.settlement_loss.get(), 0, "{closed:?}");
        match account.plan.allowance {
            // Manual units survive every boundary: what is left is all of
            // them beside the open period's allowance, less at most what was
            // charged since that period began.
            Some(allowance) => assert!(
                closed.balance.get() + (account.charged() - account.charged_before_period)
                    >= account.plan.top_up + allowance,
                "units are missing from the open period or the top-up: {closed:?}"
            ),
            None => assert_eq!(closed.expired_allowance.get(), 0, "{closed:?}"),
        }
    }
}
