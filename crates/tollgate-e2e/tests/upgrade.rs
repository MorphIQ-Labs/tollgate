//! Mixed versions: an earlier release's instances against this server, and this
//! release's instances against an earlier server, over one PostgreSQL database.
//!
//! Each assertion is either an invariant, which no mixture of versions may
//! break, or a compatibility statement a release made. The statements are
//! GH-77's, in `docs/SNAPSHOT_OPERATIONS.md`: upgrade the control plane first;
//! older instances ignore the added `consumed` total; a control plane that
//! sends none has its budget figure taken only from publications; there is no
//! database migration.
//!
//! The earlier releases are built from their tags, so these are ignored by
//! default and are not part of the pull-request gate. Run
//! `./scripts/check_e2e_upgrade.sh`.

use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use tollgate_e2e::{
    App, AppOptions, Build, ControlPlane, Customer, DEADLINE, LedgerWatch, Load, Policy, Settings,
    Tally, estimates_reach_ledger, eventually,
};

const BALANCE: u64 = 1_000_000_000;

/// An earlier release, and what it is known to do.
struct Release {
    tag: &'static str,
    version: &'static str,
    /// Whether it carries GH-77: budget figures ordered by ledger totals and
    /// refreshed on every read.
    orders_budget_figures: bool,
}

const V0_33_0: Release = Release {
    tag: "v0.33.0",
    version: "0.33.0",
    orders_budget_figures: true,
};

const V0_32_2: Release = Release {
    tag: "v0.32.2",
    version: "0.32.2",
    orders_budget_figures: false,
};

/// Short enough to be renewed many times, small enough that instances keep
/// asking the server for more.
fn options() -> AppOptions {
    AppOptions {
        lease_ttl: Duration::from_secs(10),
        lease_units: 5_000,
        ..AppOptions::default()
    }
}

/// Wait until the instance serves the customer, counting what it charged.
async fn first_served(app: &App, customer: &Customer, tally: &Tally) {
    eventually("the first priced response", DEADLINE, || async {
        tally
            .price(app, &customer.secret)
            .await
            .map_err(|refused| format!("{refused:?}"))
    })
    .await;
}

/// Serve `count` further requests, each of which must succeed.
async fn spend(app: &App, customer: &Customer, tally: &Tally, count: usize) {
    first_served(app, customer, tally).await;
    for _ in 1..count {
        tally
            .price(app, &customer.secret)
            .await
            .expect("a funded, authorized request is served");
    }
}

/// Serve the credential until the instance refuses it as unauthenticated.
async fn refused_as_revoked(app: &App, customer: &Customer, tally: &Tally) {
    eventually("revocation to reach the instance", DEADLINE, || async {
        match tally.price(app, &customer.secret).await {
            Ok(units) => Err(format!("still served, charging {units}")),
            Err(refused) if refused.status == StatusCode::UNAUTHORIZED => Ok(()),
            Err(refused) => Err(format!("{refused:?}")),
        }
    })
    .await;
}

/// An instance under continuous load for one customer.
struct Instance {
    app: Arc<App>,
    tally: Arc<Tally>,
    load: Load,
}

impl Instance {
    async fn start(plane: &ControlPlane, build: &Build, name: &str, customer: &Customer) -> Self {
        let app = Arc::new(plane.start_app_from(build, name, options()).await);
        let tally = Arc::<Tally>::default();
        first_served(&app, customer, &tally).await;
        let mut instance = Instance {
            app,
            tally,
            load: Load::default(),
        };
        instance.resume(customer);
        instance
    }

    fn resume(&mut self, customer: &Customer) {
        for _ in 0..2 {
            self.load.client(
                Arc::clone(&self.app),
                customer.secret.clone(),
                Arc::clone(&self.tally),
                Duration::from_millis(5),
            );
        }
    }

    async fn pause(&mut self) {
        std::mem::take(&mut self.load).stop().await;
    }

    /// Replace it as an orchestrator would: it must drain and exit cleanly.
    async fn retire(self) {
        self.load.stop().await;
        Arc::into_inner(self.app)
            .expect("the load has released it")
            .terminate()
            .await;
    }
}

/// The ledger of the customer under load, watched across every instance that
/// has served it.
struct Watched {
    watch: LedgerWatch,
    tallies: Vec<Arc<Tally>>,
}

impl Watched {
    fn new(customer: &Customer) -> Self {
        Watched {
            watch: LedgerWatch::new(customer.account, BALANCE),
            tallies: Vec::new(),
        }
    }

    fn charged(&self) -> u64 {
        self.tallies.iter().map(|tally| tally.charged()).sum()
    }

    /// Observe the ledger until `done`, with every instance's load alive.
    async fn until(
        &mut self,
        what: &str,
        plane: &ControlPlane,
        instances: &mut [Instance],
        done: impl AsyncFn(&[Instance]) -> bool,
    ) {
        let deadline = std::time::Instant::now() + DEADLINE;
        loop {
            for instance in instances.iter_mut() {
                instance.load.check();
            }
            let tallies: Vec<&Tally> = self.tallies.iter().map(Arc::as_ref).collect();
            let view = self.watch.observe(plane, &tallies).await;
            if done(instances).await {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}: {view:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Require each instance to be refilled by the server now running, serve
    /// from that, and have it billed.
    async fn served_by_this_server(&mut self, plane: &ControlPlane, instances: &mut [Instance]) {
        let mut marks = Vec::new();
        for instance in instances.iter() {
            marks.push((
                instance.app.counter("/refill/acquired").await,
                instance.app.counter("/accounting/accepted").await,
            ));
        }
        self.until(
            "every instance to be funded and billed",
            plane,
            instances,
            async |instances| {
                for (instance, (acquired, accepted)) in instances.iter().zip(&marks) {
                    if instance.app.counter("/refill/acquired").await <= *acquired
                        || instance.app.counter("/accounting/accepted").await <= *accepted
                    {
                        return false;
                    }
                }
                true
            },
        )
        .await;
    }
}

/// Stop every instance's load, then require each of `claimed` to reach the
/// ledger's figure without any publication (INVARIANTS.md GL-28).
async fn figures_converge(
    plane: &ControlPlane,
    instances: &mut [Instance],
    claimed: &[bool],
    customer: &Customer,
    watched: &Watched,
) {
    for instance in instances.iter_mut() {
        instance.pause().await;
    }
    let apps: Vec<&App> = instances
        .iter()
        .zip(claimed)
        .filter_map(|(instance, claimed)| claimed.then_some(instance.app.as_ref()))
        .collect();
    estimates_reach_ledger(plane, &apps, customer, watched.charged()).await;
    for instance in instances {
        instance.resume(customer);
    }
}

/// Require a drained deployment to have billed exactly what was charged.
async fn reconciled(plane: &ControlPlane, customer: &Customer, charged: u64) {
    assert!(charged > 0, "the customer was never served");
    let closed = plane.ledger(customer.account).await;
    assert_eq!(closed.deposited.get(), BALANCE, "{closed:?}");
    assert_eq!(closed.settled_usage.get(), charged, "{closed:?}");
    assert_eq!(closed.outstanding_lease_grants.get(), 0, "{closed:?}");
    assert_eq!(closed.settlement_loss.get(), 0, "{closed:?}");
    assert_eq!(closed.balance.get(), BALANCE - charged, "{closed:?}");
}

/// Upgrade a running deployment of `release`: the server first, then one
/// instance at a time, under load throughout.
async fn upgrade_from(release: &Release) {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let old = settings.previous(release.tag);
    old.assert_version(release.version);
    let new = settings.current().clone();
    let mut plane = ControlPlane::start_on(settings, &old).await;
    let customer = plane.onboard(1, BALANCE).await;
    let doomed = plane.onboard(2, BALANCE).await;
    let doomed_tally = Tally::default();
    let mut watched = Watched::new(&customer);
    let mut fleet = Vec::new();
    for name in ["old-a", "old-b"] {
        let instance = Instance::start(&plane, &old, name, &customer).await;
        watched.tallies.push(Arc::clone(&instance.tally));
        spend(&instance.app, &doomed, &doomed_tally, 5).await;
        fleet.push(instance);
    }
    watched.served_by_this_server(&plane, &mut fleet).await;

    // The control plane first, over the database the old one migrated. Old
    // instances are funded and billed by it.
    plane.replace_server(&new).await;
    watched.served_by_this_server(&plane, &mut fleet).await;
    // They accept what it publishes, `consumed` and all, and the credentials
    // it issues.
    let policy = Policy {
        generation: 2,
        request_price: 60,
        ..Policy::INITIAL
    };
    plane.publish(&customer, policy).await;
    watched
        .until(
            "the new server's publication to reach old instances",
            &plane,
            &mut fleet,
            async |fleet| {
                fleet
                    .iter()
                    .all(|instance| instance.tally.last_charge() == policy.charge())
            },
        )
        .await;
    let newcomer = plane.onboard(3, BALANCE).await;
    let newcomer_tally = Tally::default();
    for instance in &fleet {
        spend(&instance.app, &newcomer, &newcomer_tally, 3).await;
    }

    // A mixed fleet.
    let replacement = Instance::start(&plane, &new, "new-a", &customer).await;
    watched.tallies.push(Arc::clone(&replacement.tally));
    fleet.remove(0).retire().await;
    fleet.push(replacement);
    spend(&fleet[1].app, &doomed, &doomed_tally, 3).await;
    watched.served_by_this_server(&plane, &mut fleet).await;
    plane.revoke(&doomed).await;
    for instance in &fleet {
        refused_as_revoked(&instance.app, &doomed, &doomed_tally).await;
    }
    // Nothing is claimed of an older instance's figure unless its release
    // already ordered them.
    let claimed = [release.orders_budget_figures, true];
    figures_converge(&plane, &mut fleet, &claimed, &customer, &watched).await;

    // The last old instance.
    let replacement = Instance::start(&plane, &new, "new-b", &customer).await;
    watched.tallies.push(Arc::clone(&replacement.tally));
    fleet.remove(0).retire().await;
    fleet.push(replacement);
    watched.served_by_this_server(&plane, &mut fleet).await;
    figures_converge(&plane, &mut fleet, &[true, true], &customer, &watched).await;

    for instance in fleet {
        instance.retire().await;
    }
    reconciled(&plane, &customer, watched.charged()).await;
    reconciled(&plane, &doomed, doomed_tally.charged()).await;
    reconciled(&plane, &newcomer, newcomer_tally.charged()).await;
}

/// Wait until every instance has delivered the usage of everything it served.
async fn usage_delivered(instances: &[(App, Tally)]) {
    for (app, tally) in instances {
        eventually("usage to be delivered", DEADLINE, || async {
            let accepted = app.counter("/accounting/accepted").await;
            (accepted == tally.served())
                .then_some(())
                .ok_or_else(|| format!("{accepted} of {}", tally.served()))
        })
        .await;
    }
}

/// Require every instance to report one of `figures` at every observation,
/// across enough further refreshes to have fetched every principal again.
async fn figures_stay(instances: &[(App, Tally)], customer: &Customer, figure: u64) {
    for (app, _) in instances {
        let enough = app.counter("/snapshots/refresh_attempts").await + 20;
        eventually("further refreshes", DEADLINE, || async {
            let seen = app.budget(&customer.secret).await;
            assert_eq!(seen, Some(figure), "the figure moved without a publication");
            let attempts = app.counter("/snapshots/refresh_attempts").await;
            (attempts >= enough)
                .then_some(())
                .ok_or_else(|| format!("{attempts} of {enough} refreshes"))
        })
        .await;
    }
}

/// This release's instances against `release`'s server: the order the release
/// notes advise against, which must still be correct. Then the server is
/// upgraded beneath them.
///
/// These instances' usage counters count only the customers named here, so
/// each instance is billed through one tally.
async fn new_instances_against(release: &Release) {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let old = settings.previous(release.tag);
    old.assert_version(release.version);
    let new = settings.current().clone();
    let mut plane = ControlPlane::start_on(settings, &old).await;
    let customer = plane.onboard(1, BALANCE).await;
    let doomed = plane.onboard(2, BALANCE).await;
    let mut instances = Vec::new();
    for name in ["new-a", "new-b"] {
        instances.push((plane.start_app(name, options()).await, Tally::default()));
    }
    let charged = std::cell::Cell::new(0);
    let doomed_charged = std::cell::Cell::new(0);
    // One tally per instance counts both customers; each request's charge is
    // also attributed to the customer it was for.
    let serve = async |instances: &[(App, Tally)], who: &Customer, total: &std::cell::Cell<u64>| {
        for (app, tally) in instances {
            let before = tally.charged();
            spend(app, who, tally, 20).await;
            total.set(total.get() + tally.charged() - before);
        }
    };
    serve(&instances, &customer, &charged).await;
    serve(&instances, &doomed, &doomed_charged).await;
    usage_delivered(&instances).await;
    let apps: Vec<&App> = instances.iter().map(|(app, _)| app).collect();

    if release.orders_budget_figures {
        estimates_reach_ledger(&plane, &apps, &customer, charged.get()).await;
    } else {
        // "new instances then take the figure only from publications they
        // install": the one onboarding made, whatever has been spent since.
        figures_stay(&instances, &customer, BALANCE).await;
        let published = BALANCE - charged.get();
        plane
            .publish(
                &customer,
                Policy {
                    generation: 2,
                    ..Policy::INITIAL
                },
            )
            .await;
        for app in &apps {
            eventually("the republished figure", DEADLINE, || async {
                let seen = app.budget(&customer.secret).await;
                assert!(
                    seen == Some(BALANCE) || seen == Some(published),
                    "{seen:?} came from no publication"
                );
                (seen == Some(published))
                    .then_some(())
                    .ok_or_else(|| format!("{seen:?}, expected {published}"))
            })
            .await;
        }
        serve(&instances, &customer, &charged).await;
        usage_delivered(&instances).await;
        figures_stay(&instances, &customer, published).await;
    }

    plane.revoke(&doomed).await;
    for (app, tally) in &instances {
        let before = tally.charged();
        refused_as_revoked(app, &doomed, tally).await;
        doomed_charged.set(doomed_charged.get() + tally.charged() - before);
    }

    // The server catches up: the same instances now converge by refresh alone.
    plane.replace_server(&new).await;
    serve(&instances, &customer, &charged).await;
    estimates_reach_ledger(&plane, &apps, &customer, charged.get()).await;
    drop(apps);

    for (app, _) in instances {
        app.terminate().await;
    }
    reconciled(&plane, &customer, charged.get()).await;
    reconciled(&plane, &doomed, doomed_charged.get()).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds earlier releases: ./scripts/check_e2e_upgrade.sh"]
async fn a_rolling_upgrade_from_0_33_0_loses_and_repeats_nothing() {
    upgrade_from(&V0_33_0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds earlier releases: ./scripts/check_e2e_upgrade.sh"]
async fn a_rolling_upgrade_from_0_32_2_loses_and_repeats_nothing() {
    upgrade_from(&V0_32_2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds earlier releases: ./scripts/check_e2e_upgrade.sh"]
async fn new_instances_serve_correctly_against_a_0_33_0_server() {
    new_instances_against(&V0_33_0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds earlier releases: ./scripts/check_e2e_upgrade.sh"]
async fn new_instances_serve_correctly_against_a_0_32_2_server() {
    new_instances_against(&V0_32_2).await;
}
