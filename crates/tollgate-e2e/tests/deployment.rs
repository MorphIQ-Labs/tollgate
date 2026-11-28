//! Tollgate as deployed: PostgreSQL, a `tollgate-server` process and two
//! application processes, driven only through public HTTP.
//!
//! The deployment is eventually consistent, so the scenarios assert two kinds
//! of thing and nothing about how soon: what must hold at every observation,
//! whatever has propagated so far, and what must be true once the system has
//! been left to converge. Each assertion is the invariant's, not a record of
//! what a run happened to do.
//!
//! The ledger reports usage as settled only once its lease closes
//! (`docs/ACCOUNT_ADMINISTRATION.md`), so exact reconciliation follows a
//! drain.
//!
//! Run with `./scripts/check_e2e.sh`.

use std::cell::Cell;
use std::time::Duration;

use reqwest::StatusCode;
use tollgate_core::{CostUnits, EnforcementMode};
use tollgate_e2e::{
    App, AppOptions, ControlPlane, Customer, DEADLINE, Settings, Transport, eventually,
};

const BALANCE: u64 = 1_000_000;

/// Wait for every instance's estimate to reach what the ledger will hold once
/// all `charged` units are delivered, with no funding in between.
///
/// On the way, however far behind an instance is, its estimate never falls
/// below that figure and never rises (INVARIANTS.md GL-28).
async fn estimates_converge(apps: &[&App], customer: &Customer, charged: u64) {
    let truth = BALANCE - charged;
    // The last figure each instance reported, kept across polls.
    let held: Vec<Cell<u64>> = apps.iter().map(|_| Cell::new(BALANCE)).collect();
    eventually("the estimates to converge", DEADLINE, || async {
        for (app, held) in apps.iter().zip(&held) {
            // An instance still discovering the account reports nothing.
            let Some(seen) = app.budget(&customer.secret).await else {
                continue;
            };
            assert!(seen >= truth, "{seen} is below the ledger's {truth}");
            assert!(
                seen <= held.get(),
                "an estimate rose from {} to {seen}",
                held.get()
            );
            held.set(seen);
        }
        let seen: Vec<u64> = held.iter().map(Cell::get).collect();
        if seen.iter().all(|seen| *seen == truth) {
            Ok(())
        } else {
            Err(format!("{seen:?}, expected {truth}"))
        }
    })
    .await;
}

/// Drain every instance, then require the ledger to have settled exactly what
/// responses charged, with nothing held and nothing lost.
async fn drain_and_reconcile(
    plane: &ControlPlane,
    apps: Vec<App>,
    customer: &Customer,
    charged: u64,
) {
    for app in apps {
        app.terminate().await;
    }
    let closed = plane.ledger(customer.account).await;
    assert_eq!(closed.settled_usage.get(), charged);
    assert_eq!(closed.outstanding_lease_grants.get(), 0);
    assert_eq!(closed.settlement_loss.get(), 0);
    assert_eq!(closed.balance.get(), BALANCE - charged);
}

/// Serve `count` requests, each of which must succeed, and return the units the
/// responses charged. The first waits for the credential and funding to reach
/// the instance; its refusals charge nothing.
async fn spend(app: &App, customer: &Customer, count: usize) -> u64 {
    let mut charged = eventually("the first priced response", DEADLINE, || async {
        app.price(&customer.secret)
            .await
            .map_err(|refused| format!("{refused:?}"))
    })
    .await;
    for _ in 1..count {
        charged += app
            .price(&customer.secret)
            .await
            .expect("a funded, authorized request is served");
    }
    charged
}

#[tokio::test]
async fn traffic_across_instances_is_billed_to_the_ledger() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let first = plane.start_app("app-a", AppOptions::default()).await;
    let second = plane.start_app("app-b", AppOptions::default()).await;

    let charged = spend(&first, &customer, 20).await + spend(&second, &customer, 15).await;
    assert!(charged > 0);
    // A drained instance delivers its usage and returns what it did not spend.
    drain_and_reconcile(&plane, vec![first, second], &customer, charged).await;
}

/// Spend on one instance reaches every instance's estimate through ordinary
/// refresh (GL-77, INVARIANTS.md GL-28).
#[tokio::test]
async fn budget_estimates_converge_across_instances_without_publication() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let first = plane.start_app("app-a", AppOptions::default()).await;
    let second = plane.start_app("app-b", AppOptions::default()).await;
    let both = [&first, &second];
    estimates_converge(&both, &customer, 0).await;

    // Each instance learns of the other's spend, not only its own.
    let charged = spend(&first, &customer, 30).await;
    estimates_converge(&both, &customer, charged).await;
    let charged = charged + spend(&second, &customer, 10).await;
    estimates_converge(&both, &customer, charged).await;

    // An instance started afterwards reaches current funding too.
    let late = plane.start_app("app-late", AppOptions::default()).await;
    estimates_converge(&[&late], &customer, charged).await;
    // It has only ever been asked for balances, some before it could answer:
    // none of that is traffic (INVARIANTS.md GL-20).
    assert_eq!(late.admission_counters().await, [0, 0, 0]);
}

#[tokio::test]
async fn a_revoked_credential_is_refused_by_every_instance() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let revoked = plane.onboard(1, BALANCE).await;
    let bystander = plane.onboard(2, BALANCE).await;
    let apps = [
        plane.start_app("app-a", AppOptions::default()).await,
        plane.start_app("app-b", AppOptions::default()).await,
    ];
    let charged = Cell::new(0);
    for app in &apps {
        charged.set(charged.get() + spend(app, &revoked, 5).await);
    }

    plane.revoke(&revoked).await;
    for app in &apps {
        // Requests served before propagation are charged and must be billed.
        eventually("revocation to reach the instance", DEADLINE, || async {
            match app.price(&revoked.secret).await {
                Ok(units) => {
                    charged.set(charged.get() + units);
                    Err(format!("still served, charging {units}"))
                }
                Err(refused) if refused.status == StatusCode::UNAUTHORIZED => Ok(()),
                Err(refused) => Err(format!("{refused:?}")),
            }
        })
        .await;
    }
    // Once refused, it stays refused, and its neighbour is unaffected.
    let mut neighbour = 0;
    for app in &apps {
        for _ in 0..10 {
            let refused = app
                .price(&revoked.secret)
                .await
                .expect_err("a revoked credential is never served again");
            assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
        }
        neighbour += spend(app, &bystander, 3).await;
    }
    for app in apps {
        app.terminate().await;
    }
    for (customer, charged) in [(&revoked, charged.get()), (&bystander, neighbour)] {
        let closed = plane.ledger(customer.account).await;
        assert_eq!(closed.settled_usage.get(), charged);
        assert_eq!(closed.outstanding_lease_grants.get(), 0);
        assert_eq!(closed.balance.get(), BALANCE - charged);
    }
}

#[tokio::test]
async fn instances_serve_through_a_server_restart_and_then_settle() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let mut plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let first = plane.start_app("app-a", AppOptions::default()).await;
    let second = plane.start_app("app-b", AppOptions::default()).await;
    let mut charged = spend(&first, &customer, 5).await + spend(&second, &customer, 5).await;

    plane.kill_server();
    // The request path needs no control plane while its grants last.
    for _ in 0..20 {
        for app in [&first, &second] {
            charged += app
                .price(&customer.secret)
                .await
                .expect("served from local state during the outage");
        }
    }

    plane.start_server().await;
    // Usage queued during the outage reaches the restarted server, once.
    estimates_converge(&[&first, &second], &customer, charged).await;
    drain_and_reconcile(&plane, vec![first, second], &customer, charged).await;
}

/// A killed instance's work never becomes spendable again (INVARIANTS.md
/// GL-9). How much of its usage arrived first is a matter of timing, so the
/// bill is bounded rather than fixed: at least everything else, at most
/// everything executed.
#[tokio::test]
async fn a_killed_instance_does_not_return_spendable_units() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let victim = plane
        .start_app(
            "app-victim",
            AppOptions {
                lease_ttl: Duration::from_secs(5),
                // The case under test: usage still queued when the instance dies.
                usage_flush: Duration::from_secs(600),
                ..AppOptions::default()
            },
        )
        .await;
    let survivor = plane.start_app("app-survivor", AppOptions::default()).await;
    let unaccounted = spend(&victim, &customer, 10).await;
    victim.kill();

    // The survivor keeps serving, and its own drain is clean.
    let charged = spend(&survivor, &customer, 15).await;
    survivor.terminate().await;
    // What is still outstanding belonged to the dead instance. Reclaim waits
    // out its lease and the server's grace period.
    let closed = plane
        .ledger_reaches(
            "the dead instance's leases to be reclaimed",
            customer.account,
            Duration::from_secs(90),
            |view| view.outstanding_lease_grants.get() == 0,
        )
        .await;
    let executed = charged + unaccounted;
    let billed = closed.settled_usage.get();
    assert!(
        (charged..=executed).contains(&billed),
        "billed {billed}, outside {charged}..={executed}"
    );
    assert!(
        closed.balance.get() <= BALANCE - executed,
        "executed work became spendable again: {closed:?}"
    );
    // Whatever was not billed is reported as loss, never absorbed.
    assert!(billed + closed.settlement_loss.get() >= executed);
    assert_eq!(
        closed.balance.get() + closed.settled_usage.get() + closed.settlement_loss.get(),
        BALANCE
    );
}

/// Serve until the instance refuses with one of `codes`, adding what it charged
/// to `charged`. Refusals before then charge nothing and are retried: funding
/// may still be on its way.
async fn spend_until(app: &App, customer: &Customer, codes: &[&str], charged: &Cell<u64>) {
    eventually("the funding refusal", DEADLINE, || async {
        loop {
            match app.price(&customer.secret).await {
                Ok(units) => charged.set(charged.get() + units),
                Err(refused) => {
                    let code = refused.code.as_deref().unwrap_or_default();
                    return codes
                        .contains(&code)
                        .then_some(())
                        .ok_or_else(|| format!("{refused:?}"));
                }
            }
        }
    })
    .await;
}

/// An elastic account is served past its deposit, each instance extending at
/// most the published cap, and every unfunded unit reaches the ledger. A strict
/// account on the same instances never exceeds its deposit (INVARIANTS.md
/// GL-1, GL-5).
#[tokio::test]
async fn elastic_overage_is_capped_per_instance_and_recorded() {
    const DEPOSIT: u64 = 5_000;
    const CAP: u64 = 3_000;
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let elastic = plane
        .onboard_with(
            1,
            DEPOSIT,
            EnforcementMode::Elastic {
                overage_cap: CostUnits(CAP),
            },
        )
        .await;
    let strict = plane.onboard(2, DEPOSIT).await;
    let options = AppOptions {
        lease_units: 1_000,
        ..AppOptions::default()
    };
    let apps = [
        plane.start_app("app-a", options).await,
        plane.start_app("app-b", options).await,
    ];

    let elastic_charged = Cell::new(0);
    let strict_charged = Cell::new(0);
    for app in &apps {
        spend_until(app, &elastic, &["overage-cap-exhausted"], &elastic_charged).await;
        spend_until(
            app,
            &strict,
            &[
                "quota-exhausted",
                "balance-exhausted",
                "balance-insufficient",
            ],
            &strict_charged,
        )
        .await;
        assert!(
            strict_charged.get() <= DEPOSIT,
            "a strict account overspent"
        );
    }
    // Each instance has extended no more than the cap, and has extended some.
    let mut extended = 0;
    for app in &apps {
        let spent = app.overage_spent().await;
        assert!((1..=CAP).contains(&spent), "an instance extended {spent}");
        extended += spent;
    }
    for app in apps {
        app.terminate().await;
    }

    let closed = plane.ledger(elastic.account).await;
    assert_eq!(closed.settled_usage.get(), elastic_charged.get());
    assert_eq!(closed.outstanding_lease_grants.get(), 0);
    assert_eq!(closed.settlement_loss.get(), 0);
    assert_eq!(closed.overage_recorded.get(), extended);
    assert!(elastic_charged.get() <= DEPOSIT + 2 * CAP);

    let closed = plane.ledger(strict.account).await;
    assert_eq!(closed.settled_usage.get(), strict_charged.get());
    assert_eq!(closed.overage_recorded.get(), 0);
    assert_eq!(closed.balance.get(), DEPOSIT - strict_charged.get());
}

/// With its database unreachable the server cannot grant or ingest, instances
/// serve from what they hold, and afterwards everything is billed exactly once.
#[tokio::test]
async fn a_database_outage_delays_billing_without_losing_or_repeating_it() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let first = plane.start_app("app-a", AppOptions::default()).await;
    let second = plane.start_app("app-b", AppOptions::default()).await;
    let mut charged = spend(&first, &customer, 5).await + spend(&second, &customer, 5).await;

    for fault in [tollgate_e2e::Link::cut, tollgate_e2e::Link::stall] {
        fault(plane.database_link());
        assert!(
            !plane
                .ledger_answers(customer.account, Duration::from_secs(2))
                .await,
            "the control plane still reads its ledger, so nothing is broken"
        );
        for _ in 0..20 {
            for app in [&first, &second] {
                charged += app
                    .price(&customer.secret)
                    .await
                    .expect("served from local state during the outage");
            }
        }
        plane.database_link().heal();
        estimates_converge(&[&first, &second], &customer, charged).await;
    }
    drain_and_reconcile(&plane, vec![first, second], &customer, charged).await;
}

/// A partitioned instance serves from what it holds and affects no other. Once
/// the partition heals its usage arrives, once.
#[tokio::test]
async fn a_partitioned_instance_serves_locally_and_settles_after_healing() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start(settings).await;
    let customer = plane.onboard(1, BALANCE).await;
    let link = plane.link_to_server().await;
    let isolated = plane
        .start_app_via("app-isolated", AppOptions::default(), &link)
        .await;
    let connected = plane
        .start_app("app-connected", AppOptions::default())
        .await;
    let mut charged = spend(&isolated, &customer, 5).await + spend(&connected, &customer, 5).await;
    let mut newcomers = Vec::new();

    let faults = [tollgate_e2e::Link::stall, tollgate_e2e::Link::cut];
    for (newcomer, fault) in (2..).zip(faults) {
        fault(&link);
        // A credential issued during the partition reaches only the connected
        // side, which is what shows there are two sides.
        let issued = plane.onboard(newcomer, BALANCE).await;
        let on_connected = spend(&connected, &issued, 1).await;
        let refused = isolated
            .price(&issued.secret)
            .await
            .expect_err("an instance cannot know what it cannot have heard");
        assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
        for _ in 0..20 {
            for app in [&isolated, &connected] {
                charged += app
                    .price(&customer.secret)
                    .await
                    .expect("both sides of a partition serve from their grants");
            }
        }
        // The ledger conserves whatever has not arrived.
        let during = plane.ledger(customer.account).await;
        assert!(during.settled_usage.get() <= charged);
        link.heal();
        estimates_converge(&[&isolated, &connected], &customer, charged).await;
        // Healed, the isolated side learns of it without help.
        let on_isolated = spend(&isolated, &issued, 1).await;
        newcomers.push((issued, on_connected + on_isolated));
    }
    drain_and_reconcile(&plane, vec![isolated, connected], &customer, charged).await;
    for (issued, charged) in newcomers {
        let closed = plane.ledger(issued.account).await;
        assert_eq!(closed.settled_usage.get(), charged);
        assert_eq!(closed.balance.get(), BALANCE - charged);
    }
}

/// Billing and convergence hold over a private CA, with a bearer credential.
#[tokio::test]
async fn a_deployment_over_tls_bills_and_settles() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start_with(settings, Transport::Tls).await;
    let customer = plane.onboard(1, BALANCE).await;
    let app = plane.start_app("app-tls", AppOptions::default()).await;
    let charged = spend(&app, &customer, 15).await;
    estimates_converge(&[&app], &customer, charged).await;
    drain_and_reconcile(&plane, vec![app], &customer, charged).await;
}

/// Under mutual TLS the certificate is the whole identity. A certificate the
/// same CA signed and the server does not map gets nothing: no credentials, no
/// funding.
#[tokio::test]
async fn mutual_tls_serves_a_mapped_instance_and_nothing_else() {
    let Some(settings) = Settings::from_env() else {
        return;
    };
    let plane = ControlPlane::start_with(settings, Transport::MutualTls).await;
    let customer = plane.onboard(1, BALANCE).await;
    let stranger = plane
        .start_unknown_app("app-stranger", AppOptions::default())
        .await;
    let app = plane.start_app("app-mtls", AppOptions::default()).await;
    let charged = spend(&app, &customer, 15).await;
    // The mapped instance has converged, so the stranger has had at least as
    // long and many refresh intervals besides.
    estimates_converge(&[&app], &customer, charged).await;
    assert!(!stranger.ready().await);
    stranger
        .price(&customer.secret)
        .await
        .expect_err("an unmapped certificate serves no customer");
    stranger.terminate().await;
    drain_and_reconcile(&plane, vec![app], &customer, charged).await;
}
