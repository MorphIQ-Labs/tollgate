//! The background mechanisms, each disabled by one edit, with the tests that
//! must then fail.
//!
//! A change that adds a background mechanism adds its entry here, and one that
//! moves an edited site updates the pattern. An edit disables the mechanism and
//! leaves the code compiling; a witness fails by its own assertion within the
//! gate's bound.

use super::{Mutant, Target, Witness};

const CLIENT_HTTP_SECURITY: &str = "crates/tollgate-client/src/http_security.rs";
const CLIENT_KEY_MANAGER: &str = "crates/tollgate-client/src/key_manager.rs";
const CLIENT_LEASE_MANAGER: &str = "crates/tollgate-client/src/lease_manager.rs";
const CLIENT_PERIOD_ROLLER: &str = "crates/tollgate-client/src/period_roller.rs";
const CLIENT_RUNTIME: &str = "crates/tollgate-client/src/runtime.rs";
const CLIENT_SNAPSHOT_MANAGER: &str = "crates/tollgate-client/src/snapshot_manager.rs";
const CLIENT_USAGE_WRITER: &str = "crates/tollgate-client/src/usage_writer.rs";
const SERVER_CONFIG: &str = "crates/tollgate-server/src/config.rs";
const SERVER_LIB: &str = "crates/tollgate-server/src/lib.rs";
const SERVER_TRANSPORT: &str = "crates/tollgate-server/src/transport.rs";
const STORE_POSTGRES_LIB: &str = "crates/tollgate-store-postgres/src/lib.rs";
const STORE_MEMORY: &str = "crates/tollgate-store/src/memory.rs";
const EXAMPLE_LIB: &str = "examples/pricing-api/src/lib.rs";

const fn client(target: Target, test: &'static str) -> Witness {
    Witness {
        package: "tollgate-client",
        target,
        test,
    }
}

const fn server(target: Target, test: &'static str) -> Witness {
    Witness {
        package: "tollgate-server",
        target,
        test,
    }
}

const fn store(target: Target, test: &'static str) -> Witness {
    Witness {
        package: "tollgate-store",
        target,
        test,
    }
}

const fn store_postgres(target: Target, test: &'static str) -> Witness {
    Witness {
        package: "tollgate-store-postgres",
        target,
        test,
    }
}

const fn pricing_api(target: Target, test: &'static str) -> Witness {
    Witness {
        package: "pricing-api",
        target,
        test,
    }
}

/// Every mutant the gate runs.
pub const CATALOG: &[Mutant] = &[
    // ---- Snapshot manager ----
    Mutant {
        id: "snapshot-sweep",
        disables: "the periodic refresh of resolved principals",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "&resolutions.due_for_sweep(),",
        replace: "&[],",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "revocation_reaches_instances_via_refresh"),
            client(Target::Test("snapshot_manager"), "revocation_still_propagates_within_the_refresh_interval"),
            client(Target::Test("snapshot_manager"), "a_status_change_reaches_every_instance_through_refresh"),
            client(Target::Test("snapshot_manager"), "an_unreadable_principal_keeps_its_snapshot_until_expiry_or_repair"),
            client(Target::Test("snapshot_manager"), "same_generation_refresh_repairs_a_visible_cache_eviction"),
            client(Target::Test("budget_estimate"), "settled_spend_converges_after_refresh_without_policy_republication"),
            client(Target::Test("budget_estimate"), "refresh_delivers_each_account_its_own_observation"),
            client(Target::Test("budget_estimate"), "failed_and_stale_refreshes_keep_the_newest_observation"),
            client(Target::Test("runtime_orchestration"), "a_refresh_outage_preserves_a_fresh_accounts_manager"),
        ],
    },
    Mutant {
        id: "snapshot-tick-discovery",
        disables: "re-enumeration of principals on each refresh tick",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "                if config.principals.discovers()
                    && discover(",
        replace: "                if false
                    && discover(",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_principal_published_after_start_is_discovered_and_served"),
            client(Target::Test("runtime_orchestration"), "catalogue_removal_withdraws_a_still_fresh_map_entry_and_can_restore_it"),
        ],
    },
    Mutant {
        id: "snapshot-startup-discovery",
        disables: "enumeration before the initial load",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "    if config.principals.discovers()
        && discover(&source",
        replace: "    if false
        && discover(&source",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_discovered_catalogue_larger_than_history_is_refreshed_in_bounded_batches"),
        ],
    },
    Mutant {
        id: "snapshot-initial-retry",
        disables: "retrying a failed initial load",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "        pending = failed;
",
        replace: "        pending = failed;
        pending.clear();
",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_failed_initial_load_is_retried_until_the_source_answers"),
        ],
    },
    Mutant {
        id: "snapshot-initial-retry-backoff",
        disables: "the initial load's own retry interval",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "            tokio::select! {
                _ = tokio::time::sleep(config.retry_backoff) => {}",
        replace: "            tokio::select! {
                _ = tokio::time::sleep(config.refresh_interval) => {}",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_failed_initial_load_is_retried_until_the_source_answers"),
        ],
    },
    Mutant {
        id: "snapshot-push",
        disables: "applying pushed updates",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "push = updates.recv(), if !updates_closed =>",
        replace: "push = updates.recv(), if false && !updates_closed =>",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "push_update_propagates_permission_change"),
            client(Target::Test("snapshot_manager"), "unknown_principal_resolves_once_published"),
            client(Target::Test("snapshot_manager"), "a_push_reinstates_an_absent_principal_at_its_own_generation"),
        ],
    },
    Mutant {
        id: "snapshot-push-discovery",
        disables: "discovering a principal from the push that names it",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "                        resolutions.track(push.principal);
",
        replace: "",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_push_discovers_the_principal_it_names"),
            client(Target::Test("runtime_orchestration"), "a_push_only_source_reports_unresolved_principals_without_enumeration"),
        ],
    },
    Mutant {
        id: "snapshot-lag-recovery",
        disables: "refetching after the push stream lagged",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "&resolutions.all_tracked(),",
        replace: "&[],",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_lagged_push_stream_refetches_every_tracked_principal"),
        ],
    },
    Mutant {
        id: "snapshot-lag-recovery-filtered",
        disables: "refetching negative resolutions after a lag",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "&resolutions.all_tracked(),",
        replace: "&resolutions.due_for_sweep(),",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_lagged_push_stream_refetches_every_tracked_principal"),
        ],
    },
    Mutant {
        id: "snapshot-negative-refetch",
        disables: "the targeted refetch when a negative TTL expires",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "let due = resolutions.due_for_refetch(now, config.max_concurrent_fetches);",
        replace: "let due: Vec<Principal> = Vec::new();",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "negative_ttl_retry_is_backed_off_and_recovers_without_push"),
            client(Target::Test("snapshot_manager"), "a_reinstated_principal_comes_back_on_the_revoked_ttl"),
            client(Target::Test("snapshot_manager"), "a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl"),
        ],
    },
    Mutant {
        id: "snapshot-refetch-backoff",
        disables: "backing off a targeted refetch that failed",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "                        resolutions.back_off(principal, retry_at);
",
        replace: "                        let _unused = (principal, retry_at);
",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "negative_ttl_retry_is_backed_off_and_recovers_without_push"),
            client(Target::Test("snapshot_manager"), "a_refused_answer_is_retried_with_backoff_not_at_source_latency"),
            client(Target::Test("snapshot_manager"), "an_abandoned_fetch_is_throttled_like_a_refusal"),
        ],
    },
    Mutant {
        id: "snapshot-control-wakeup",
        disables: "waking at the next expiry or refetch deadline",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "_ = &mut control_wakeup => {",
        replace: "_ = &mut control_wakeup, if false => {",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "readiness_falls_at_expiry_between_refreshes"),
            client(Target::Test("snapshot_manager"), "negative_ttl_retry_is_backed_off_and_recovers_without_push"),
        ],
    },
    Mutant {
        id: "snapshot-sweep-readiness-wake",
        disables: "withdrawing readiness while a refresh is in flight",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "_ = &mut readiness_check => {",
        replace: "_ = &mut readiness_check, if false => {",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "readiness_falls_if_refresh_hangs_across_snapshot_expiry"),
        ],
    },
    Mutant {
        id: "snapshot-budget-observation",
        disables: "delivering a refreshed budget observation",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "&& !slots.slot(snapshot.account_id).observe_budget(view)",
        replace: "&& false && !slots.slot(snapshot.account_id).observe_budget(view)",
        witnesses: &[
            client(Target::Test("budget_estimate"), "settled_spend_converges_after_refresh_without_policy_republication"),
            client(Target::Test("budget_estimate"), "refresh_delivers_each_account_its_own_observation"),
        ],
    },
    Mutant {
        id: "snapshot-fetch-timeout",
        disables: "abandoning a hung fetch",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "    for _ in 0..config.max_concurrent_fetches {
        let Some(read) = pending.next() else {
            break;
        };
        let source = Arc::clone(source);
        let bound = config.fetch_timeout;",
        replace: "    for _ in 0..config.max_concurrent_fetches {
        let Some(read) = pending.next() else {
            break;
        };
        let source = Arc::clone(source);
        let bound = std::time::Duration::from_secs(86_400);",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_hung_fetch_is_abandoned_so_the_sweep_keeps_running"),
            client(Target::Test("snapshot_manager"), "an_abandoned_fetch_is_throttled_like_a_refusal"),
        ],
    },
    Mutant {
        id: "snapshot-enumeration-timeout",
        disables: "abandoning a hung enumeration",
        file: CLIENT_SNAPSHOT_MANAGER,
        find: "let enumerate = || tokio::time::timeout(config.enumeration_timeout, source.principals());",
        replace: "let enumerate = || tokio::time::timeout(std::time::Duration::from_secs(86_400), source.principals());",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "a_hung_enumeration_is_abandoned_so_the_loop_keeps_sweeping"),
        ],
    },
    // ---- Lease manager ----
    Mutant {
        id: "lease-poll-tick",
        disables: "the periodic refill pass",
        file: CLIENT_LEASE_MANAGER,
        find: "            _ = tick.tick() => {}
",
        replace: "            _ = tick.tick(), if false => {}
",
        witnesses: &[
            client(Target::Test("runtime"), "refill_installs_lease_on_cold_start"),
            client(Target::Test("runtime"), "expired_slot_fails_closed_then_recovers"),
        ],
    },
    Mutant {
        id: "lease-refill-doorbell",
        disables: "refilling on the debit that crosses low water",
        file: CLIENT_LEASE_MANAGER,
        find: "() = refill.requested() => {}",
        replace: "() = refill.requested(), if false => {}",
        witnesses: &[
            client(Target::Test("runtime"), "refill_begins_on_the_crossing_debit_not_the_next_tick"),
            client(Target::Test("runtime"), "a_burst_across_a_rotation_never_denies_a_funded_account"),
        ],
    },
    Mutant {
        id: "lease-window-rotation",
        disables: "rotating a lease whose usability window closed",
        file: CLIENT_LEASE_MANAGER,
        find: "Some(lease) if now >= lease.usable_until() => {",
        replace: "Some(lease) if false && now >= lease.usable_until() => {",
        witnesses: &[
            client(Target::Test("runtime"), "usability_window_rollover_returns_unspent_capacity"),
            client(Target::Test("runtime"), "expired_slot_fails_closed_then_recovers"),
        ],
    },
    Mutant {
        id: "lease-consolidation",
        disables: "consolidating a lease that refused work",
        file: CLIENT_LEASE_MANAGER,
        find: "RefillVerdict::Refused => Rotation::Consolidate,",
        replace: "RefillVerdict::Refused => Rotation::Idle,",
        witnesses: &[
            client(Target::Test("runtime"), "a_refused_lease_consolidates_rather_than_stranding_the_tail"),
            client(Target::Test("runtime"), "a_shrinking_policy_funds_a_quote_above_half_the_balance"),
        ],
    },
    Mutant {
        id: "lease-release-retry-error",
        disables: "retrying a release the store failed",
        file: CLIENT_LEASE_MANAGER,
        find: r#"                    tracing::warn!(lease = %lease_id, %error, "release failed; retrying next tick");
                    retry.push(lease);"#,
        replace: r#"                    tracing::warn!(lease = %lease_id, %error, "release failed; retrying next tick");"#,
        witnesses: &[
            client(Target::Test("runtime"), "a_failed_release_is_retried_while_the_manager_runs"),
            client(Target::Lib, "lease_manager::tests::storage_failure_does_not_skip_the_next_parked_lease"),
        ],
    },
    Mutant {
        id: "lease-release-retry-timeout",
        disables: "retrying a release that timed out",
        file: CLIENT_LEASE_MANAGER,
        find: r#"                tracing::debug!(lease = %lease_id, "release timed out; retrying next tick");
                retry.push(lease);"#,
        replace: r#"                tracing::debug!(lease = %lease_id, "release timed out; retrying next tick");"#,
        witnesses: &[
            client(Target::Test("runtime"), "a_failed_release_is_retried_while_the_manager_runs"),
            client(Target::Lib, "lease_manager::tests::hung_release_times_out_and_reparks"),
        ],
    },
    Mutant {
        id: "lease-release-retry-busy",
        disables: "keeping a lease parked while a request holds it",
        file: CLIENT_LEASE_MANAGER,
        find: "            retry.push(lease);
            continue;
        }
        let grant = lease.grant();",
        replace: "            continue;
        }
        let grant = lease.grant();",
        witnesses: &[
            client(Target::Lib, "lease_manager::tests::unquiesced_lease_is_never_released"),
        ],
    },
    Mutant {
        id: "lease-acquire-timeout",
        disables: "abandoning a hung acquire",
        file: CLIENT_LEASE_MANAGER,
        find: "        let acquire = tokio::time::timeout(
            config.store_call_timeout,",
        replace: "        let acquire = tokio::time::timeout(
            std::time::Duration::from_secs(86_400),",
        witnesses: &[
            client(Target::Test("runtime"), "a_hung_acquire_is_counted_as_a_timeout_not_a_refusal"),
        ],
    },
    Mutant {
        id: "lease-consolidate-timeout",
        disables: "abandoning a hung consolidation",
        file: CLIENT_LEASE_MANAGER,
        find: "    let call = tokio::time::timeout(
        config.store_call_timeout,
        allocator.consolidate(",
        replace: "    let call = tokio::time::timeout(
        std::time::Duration::from_secs(86_400),
        allocator.consolidate(",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "ambiguous_consolidations_remain_visible_after_a_clean_shutdown"),
        ],
    },
    Mutant {
        id: "lease-release-pass-bound",
        disables: "bounding a release pass against a hung store",
        file: CLIENT_LEASE_MANAGER,
        find: "let call_deadline = deadline.min(tokio::time::Instant::now() + config.store_call_timeout);
        let call = tokio::time::timeout_at(",
        replace: "let call_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(86_400);
        let call = tokio::time::timeout_at(",
        witnesses: &[
            client(Target::Test("runtime"), "a_refill_does_not_wait_behind_the_release_pass"),
            client(Target::Lib, "lease_manager::tests::a_release_pass_costs_one_budget_whatever_the_parked_count"),
        ],
    },
    // ---- Usage writer ----
    Mutant {
        id: "usage-flush-interval",
        disables: "flushing a partial batch on the interval",
        file: CLIENT_USAGE_WRITER,
        find: "() = &mut deadline => true,",
        replace: "() = &mut deadline, if false => true,",
        witnesses: &[
            client(Target::Test("runtime"), "writer_retries_through_outage_without_losing_events"),
            client(Target::Lib, "usage_writer::lane_tests::a_partial_batch_waits_for_the_tick"),
        ],
    },
    Mutant {
        id: "usage-doorbell",
        disables: "draining a full batch before the interval",
        file: CLIENT_USAGE_WRITER,
        find: "() = lanes.queue.doorbell.notified() => false,",
        replace: "() = lanes.queue.doorbell.notified(), if false => false,",
        witnesses: &[
            client(Target::Lib, "usage_writer::lane_tests::a_full_batch_is_delivered_before_the_tick"),
        ],
    },
    Mutant {
        id: "usage-retry",
        disables: "retrying a batch the sink could not take",
        file: CLIENT_USAGE_WRITER,
        find: "Ok(Err(refused)) if !refused.is_retryable() => {",
        replace: "Ok(Err(refused)) if { true } => {",
        witnesses: &[
            client(Target::Test("runtime"), "writer_retries_through_outage_without_losing_events"),
            client(Target::Test("runtime"), "shutdown_after_recovery_delivers_everything"),
        ],
    },
    Mutant {
        id: "usage-refused-drop",
        disables: "dropping a batch the sink will always refuse",
        file: CLIENT_USAGE_WRITER,
        find: "Ok(Err(refused)) if !refused.is_retryable() => {",
        replace: "Ok(Err(refused)) if false && !refused.is_retryable() => {",
        witnesses: &[
            client(Target::Test("runtime"), "a_refused_batch_is_counted_lost_rather_than_retried_forever"),
        ],
    },
    Mutant {
        id: "usage-retry-backoff",
        disables: "waiting between retries",
        file: CLIENT_USAGE_WRITER,
        find: "_ = tokio::time::sleep(config.retry_backoff) => {}",
        replace: "_ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}",
        witnesses: &[
            client(Target::Test("runtime"), "a_failing_sink_is_retried_once_per_backoff"),
        ],
    },
    Mutant {
        id: "usage-ingest-timeout",
        disables: "abandoning a hung ingest while running",
        file: CLIENT_USAGE_WRITER,
        find: "tokio::time::timeout(config.ingest_timeout, ingest_checked(&**sink, batch, now));",
        replace: "tokio::time::timeout(std::time::Duration::from_secs(86_400), ingest_checked(&**sink, batch, now));",
        witnesses: &[
            client(Target::Test("runtime"), "a_hung_ingest_is_abandoned_and_delivered_while_the_writer_runs"),
        ],
    },
    // ---- Key manager ----
    Mutant {
        id: "key-refresh",
        disables: "the periodic credential projection refresh",
        file: CLIENT_KEY_MANAGER,
        find: "_ = tokio::time::sleep(config.refresh_interval) => {},",
        replace: "_ = std::future::pending::<()>() => {},",
        witnesses: &[
            client(Target::Test("key_lifecycle"), "a_minted_credential_verifies_until_it_is_revoked"),
            client(Target::Test("key_lifecycle"), "a_credentials_own_expiry_travels_to_the_verifier"),
            client(Target::Test("key_lifecycle"), "rotation_keeps_both_credentials_live_until_the_old_one_is_retired"),
            client(Target::Test("key_manager"), "an_outage_never_extends_projection_validity_and_recovery_replaces_it"),
        ],
    },
    Mutant {
        id: "key-call-timeout",
        disables: "abandoning a hung key page or pass",
        file: CLIENT_KEY_MANAGER,
        find: "let call_deadline = deadline.min(Instant::now() + config.fetch_timeout);",
        replace: "let call_deadline = Instant::now() + std::time::Duration::from_secs(86_400);",
        witnesses: &[
            client(Target::Test("key_manager"), "hung_fetches_timeout_retry_and_shutdown_interrupts_the_pending_read"),
            client(Target::Test("key_manager"), "per_call_progress_does_not_reset_the_pass_deadline"),
        ],
    },
    Mutant {
        id: "key-revision-restart",
        disables: "restarting a pass whose revision moved",
        file: CLIENT_KEY_MANAGER,
        find: "            records.clear();
            revision = None;
            after = None;
",
        replace: r#"            return Err("revision-conflict");
"#,
        witnesses: &[
            client(Target::Test("key_manager"), "revision_change_restarts_from_the_beginning_instead_of_omitting_a_new_key"),
        ],
    },
    // ---- Period roller ----
    Mutant {
        id: "roller-poll",
        disables: "the periodic rollover pass",
        file: CLIENT_PERIOD_ROLLER,
        find: "() = tokio::time::sleep(config.poll_interval) => {}",
        replace: "() = std::future::pending::<()>() => {}",
        witnesses: &[
            client(Target::Test("period_roller"), "failure_preserves_confirmed_progress_and_waits_before_retrying"),
            client(Target::Test("period_roller"), "a_hung_call_times_out_and_the_next_pass_recovers"),
            client(Target::Test("period_roller"), "the_exact_boundary_renews_one_allowance_and_preserves_topups"),
        ],
    },
    Mutant {
        id: "roller-fresh-cutoff",
        disables: "reading the clock for each pass",
        file: CLIENT_PERIOD_ROLLER,
        find: "    while !*stopping.borrow() {
        let cutoff = clock.now();",
        replace: "    let frozen = clock.now();
    while !*stopping.borrow() {
        let cutoff = frozen;",
        witnesses: &[
            client(Target::Test("period_roller"), "the_exact_boundary_renews_one_allowance_and_preserves_topups"),
        ],
    },
    Mutant {
        id: "roller-backlog-drain",
        disables: "draining saturated batches in one pass",
        file: CLIENT_PERIOD_ROLLER,
        find: "if !batch.is_saturated() {",
        replace: "if true || !batch.is_saturated() {",
        witnesses: &[
            client(Target::Test("period_roller"), "startup_drains_saturated_batches_without_waiting_for_a_tick"),
        ],
    },
    Mutant {
        id: "roller-call-timeout",
        disables: "abandoning a hung rollover call",
        file: CLIENT_PERIOD_ROLLER,
        find: "let call_deadline = (now + config.call_timeout).min(deadline);",
        replace: "let call_deadline = now + std::time::Duration::from_secs(86_400);",
        witnesses: &[
            client(Target::Test("period_roller"), "a_hung_call_times_out_and_the_next_pass_recovers"),
            client(Target::Test("period_roller"), "a_pass_deadline_can_interrupt_a_call_before_its_own_timeout"),
        ],
    },
    // ---- Instance runtime ----
    Mutant {
        id: "runtime-binding-changes",
        disables: "waking when account bindings change",
        file: CLIENT_RUNTIME,
        find: "changed = changes.changed() => {",
        replace: "changed = changes.changed(), if false => {",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_discovered_account_is_funded_without_restarting_the_instance"),
            client(Target::Test("runtime_orchestration"), "suspension_retires_funding_and_reinstatement_starts_it_again"),
        ],
    },
    Mutant {
        id: "runtime-expiry-wake",
        disables: "waking when an account's freshness expires",
        file: CLIENT_RUNTIME,
        find: "let wake = [expiry, timer]",
        replace: "let wake = [expiry.filter(|_| false), timer]",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "freshness_expiry_retires_a_manager_without_another_snapshot_notification"),
        ],
    },
    Mutant {
        id: "runtime-timers",
        disables: "acting on linger and restart timers",
        file: CLIENT_RUNTIME,
        find: "let timer = supervisor.timers.first().map(|(at, _)| *at);",
        replace: "let timer: Option<Instant> = None;",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "repeated_inactive_publications_do_not_extend_the_initial_linger"),
            client(Target::Test("runtime_orchestration"), "a_dead_manager_is_joined_then_restarted_with_backoff"),
        ],
    },
    Mutant {
        id: "runtime-manager-restart",
        disables: "restarting a dead lease manager",
        file: CLIENT_RUNTIME,
        find: "let retry = restart && !faulted && self.accounts[&account].desired;",
        replace: "let retry = false && restart && !faulted && self.accounts[&account].desired;",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_dead_manager_is_joined_then_restarted_with_backoff"),
        ],
    },
    Mutant {
        id: "runtime-restart-backoff",
        disables: "backing off before a restart",
        file: CLIENT_RUNTIME,
        find: "self.arm(account, self.config.manager_restart_backoff);",
        replace: "self.arm(account, Duration::ZERO);",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_dead_manager_is_joined_then_restarted_with_backoff"),
        ],
    },
    Mutant {
        id: "runtime-idle-linger",
        disables: "retiring an account after its idle linger",
        file: CLIENT_RUNTIME,
        find: "self.arm(account, self.config.idle_account_linger);",
        replace: "self.arm(account, Duration::from_secs(86_400));",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "reactivation_during_linger_reuses_the_manager_and_later_reactivation_releases_then_refills"),
            client(Target::Test("runtime_orchestration"), "repeated_inactive_publications_do_not_extend_the_initial_linger"),
        ],
    },
    Mutant {
        id: "runtime-cleanup-join",
        disables: "observing a retired manager's exit",
        file: CLIENT_RUNTIME,
        find: "exit = supervisor.cleanup.join_next(), if !supervisor.cleanup.is_empty() => {",
        replace: "exit = supervisor.cleanup.join_next(), if false => {",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "reactivation_during_release_waits_for_the_old_manager_before_reporting_managed"),
            client(Target::Test("runtime_orchestration"), "a_dead_manager_is_joined_then_restarted_with_backoff"),
        ],
    },
    Mutant {
        id: "runtime-manager-monitor",
        disables: "watching each manager's health",
        file: CLIENT_RUNTIME,
        find: "notice = supervisor.monitors.join_next_with_id(), if !supervisor.monitors.is_empty() => {",
        replace: "notice = supervisor.monitors.join_next_with_id(), if false => {",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_dead_manager_is_joined_then_restarted_with_backoff"),
            client(Target::Test("runtime_orchestration"), "an_accounting_integrity_fault_stops_the_instance_without_restart"),
        ],
    },
    Mutant {
        id: "runtime-snapshot-death",
        disables: "stopping when the snapshot task dies",
        file: CLIENT_RUNTIME,
        find: "() = async { while ready.changed().await.is_ok() {} } => {",
        replace: "() = async { while ready.changed().await.is_ok() {} }, if false => {",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_dead_snapshot_task_stops_the_runtime"),
        ],
    },
    Mutant {
        id: "runtime-writer-death",
        disables: "stopping when the usage writer dies",
        file: CLIENT_RUNTIME,
        find: "() = shared.recorder.closed() => {",
        replace: "() = shared.recorder.closed(), if false => {",
        witnesses: &[
            client(Target::Test("runtime_orchestration"), "a_dead_usage_writer_stops_the_runtime_and_releases_its_leases"),
        ],
    },
    // ---- Control-plane client identity ----
    Mutant {
        id: "identity-metadata-refresh",
        disables: "replacing a cached identity token at its deadline",
        file: CLIENT_HTTP_SECURITY,
        find: "&& now < *until
",
        replace: "&& (now < *until || true)
",
        witnesses: &[
            client(Target::Lib, "http_security::tests::google_metadata_cache_refreshes_at_its_exact_deadline_and_never_caches_failure"),
        ],
    },
    // ---- Server maintenance ----
    Mutant {
        id: "server-maintenance-tick",
        disables: "every maintenance pass after the first",
        file: SERVER_LIB,
        find: "    loop {
        tick.tick().await;",
        replace: "    tick.tick().await;
    let mut first = true;
    loop {
        if !std::mem::take(&mut first) {
            std::future::pending::<()>().await;
        }",
        witnesses: &[
            server(Target::Test("sweep"), "the_server_reclaims_expired_leases_on_its_interval"),
            server(Target::Test("sweep"), "work_that_falls_due_while_the_server_runs_is_done_by_a_later_tick"),
        ],
    },
    Mutant {
        id: "server-fresh-cutoff",
        disables: "reading the clock for each pass",
        file: SERVER_LIB,
        find: "        let now = clock.now();
        let mut progress = Progress::default();",
        replace: "        static HELD: std::sync::OnceLock<jiff::Timestamp> = std::sync::OnceLock::new();
        let now = *HELD.get_or_init(|| clock.now());
        let mut progress = Progress::default();",
        witnesses: &[
            server(Target::Test("sweep"), "work_that_falls_due_while_the_server_runs_is_done_by_a_later_tick"),
        ],
    },
    Mutant {
        id: "server-rollover-pass",
        disables: "the rollover pass",
        file: SERVER_LIB,
        find: "        if roll_due_periods(store.as_ref(), now, &mut health)
            .await
            .is_break()",
        replace: "        if false
            && roll_due_periods(store.as_ref(), now, &mut health)
                .await
                .is_break()",
        witnesses: &[
            server(Target::Test("sweep"), "the_server_rolls_due_budget_periods_on_its_interval"),
        ],
    },
    Mutant {
        id: "server-reclaim-backlog",
        disables: "draining saturated reclaim batches",
        file: SERVER_LIB,
        find: "                    if !batch.is_saturated() {
                        break Ok(());",
        replace: "                    if true {
                        break Ok(());",
        witnesses: &[
            server(Target::Test("sweep"), "one_scheduled_sweep_drains_every_saturated_batch"),
        ],
    },
    Mutant {
        id: "server-rollover-backlog",
        disables: "draining saturated rollover batches",
        file: SERVER_LIB,
        find: "                if !batch.is_saturated() {
                    break;",
        replace: "                if true {
                    break;",
        witnesses: &[
            server(Target::Test("sweep"), "a_tick_reports_the_accounts_it_had_to_defer"),
        ],
    },
    Mutant {
        id: "server-maintenance-death",
        disables: "stopping the server when maintenance dies",
        file: SERVER_LIB,
        find: "result = &mut sweeper.join => {",
        replace: "result = &mut sweeper.join, if false => {",
        witnesses: &[
            server(Target::Test("sweep"), "a_panicked_maintenance_call_stops_the_server_with_a_safe_error"),
        ],
    },
    // ---- Server security and transport ----
    Mutant {
        id: "security-reload-period",
        disables: "the periodic security reload",
        file: SERVER_CONFIG,
        find: "tokio::time::sleep(Duration::from_secs(5)).await;",
        replace: "std::future::pending::<()>().await;",
        witnesses: &[
            server(Target::Test("security_config"), "the_reloader_applies_file_changes_and_recovers_from_a_broken_manifest"),
        ],
    },
    Mutant {
        id: "security-reload-apply",
        disables: "installing what the reloader loads",
        file: SERVER_CONFIG,
        find: "Ok(Some(loaded)) => match loader.install(loaded, &security) {",
        replace: "Ok(Some(loaded)) => match { drop(loaded); Ok::<(), SecurityError>(()) } {",
        witnesses: &[
            server(Target::Test("security_config"), "the_reloader_applies_file_changes_and_recovers_from_a_broken_manifest"),
        ],
    },
    Mutant {
        id: "security-key-refresh",
        disables: "refreshing signing keys when they are due",
        file: SERVER_CONFIG,
        find: "if tokio::time::Instant::now() >= self.next_key_attempt {",
        replace: "if self.keys.is_none() {",
        witnesses: &[
            server(Target::Lib, "config::tests::signing_key_refresh_honors_success_cadence_and_failure_backoff"),
            server(Target::Lib, "config::tests::failed_key_refresh_never_extends_verified_identity_validity"),
        ],
    },
    Mutant {
        id: "security-key-backoff",
        disables: "backing off after a failed signing-key fetch",
        file: SERVER_CONFIG,
        find: "self.next_key_attempt = tokio::time::Instant::now() + Duration::from_secs(300);",
        replace: "self.next_key_attempt = tokio::time::Instant::now();",
        witnesses: &[
            server(Target::Lib, "config::tests::signing_key_refresh_honors_success_cadence_and_failure_backoff"),
        ],
    },
    Mutant {
        id: "server-handshake-timeout",
        disables: "abandoning a stalled TLS handshake",
        file: SERVER_TRANSPORT,
        find: "let timeout = tls.handshake_timeout;",
        replace: "let timeout = Duration::from_secs(86_400);",
        witnesses: &[
            server(Target::Lib, "transport::tests::pending_tls_handshakes_are_bounded_expire_and_drop_with_the_listener"),
        ],
    },
    // ---- Store pushes: memory ----
    Mutant {
        id: "memory-push-publish",
        disables: "pushing a publication",
        file: STORE_MEMORY,
        find: "                snapshot_audit(inner.snapshots.get(&principal)),
            )
        };
        if let Some(snapshot) = published {",
        replace: "                snapshot_audit(inner.snapshots.get(&principal)),
            )
        };
        if let Some(snapshot) = published.filter(|_| false) {",
        witnesses: &[
            store(Target::Test("store_suite"), "snapshot_publish_fetch_and_push"),
        ],
    },
    Mutant {
        id: "memory-push-publish-inherent",
        disables: "pushing a publication made through the inherent helper",
        file: STORE_MEMORY,
        find: "            publish_locked(&mut inner, principal, snapshot)?
        };
        if let Some(snapshot) = published {",
        replace: "            publish_locked(&mut inner, principal, snapshot)?
        };
        if let Some(snapshot) = published.filter(|_| false) {",
        witnesses: &[
            client(Target::Test("snapshot_manager"), "push_update_propagates_permission_change"),
            client(Target::Test("snapshot_manager"), "unknown_principal_resolves_once_published"),
        ],
    },
    Mutant {
        id: "memory-push-removal",
        disables: "pushing a removal",
        file: STORE_MEMORY,
        find: "        if receipt.before != receipt.after
            && let AdminState::Snapshot { generation, .. } = receipt.after
",
        replace: "        if false
            && let AdminState::Snapshot { generation, .. } = receipt.after
",
        witnesses: &[
            store(Target::Test("store_suite"), "snapshot_publish_fetch_and_push"),
        ],
    },
    Mutant {
        id: "memory-push-key",
        disables: "pushing a key-bound publication",
        file: STORE_MEMORY,
        find: "            (principal, published, before, after)
        };
        if let Some(snapshot) = published {",
        replace: "            (principal, published, before, after)
        };
        if let Some(snapshot) = published.filter(|_| false) {",
        witnesses: &[
            store(Target::Test("credential_activity"), "key_bound_publication_resolves_the_principal_in_the_store"),
        ],
    },
    Mutant {
        id: "memory-push-status",
        disables: "pushing a status change",
        file: STORE_MEMORY,
        find: r#""status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let planned_pushes = republished;
        let republished = planned_pushes.len();
        for (principal, snapshot) in planned_pushes {"#,
        replace: r#""status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let planned_pushes = republished;
        let republished = planned_pushes.len();
        for (principal, snapshot) in planned_pushes.into_iter().take(0) {"#,
        witnesses: &[
            store(Target::Test("store_suite"), "a_status_change_pushes_every_republished_principal"),
        ],
    },
    Mutant {
        id: "memory-push-class",
        disables: "pushing a capacity class change",
        file: STORE_MEMORY,
        find: r#"                 subscribers will resync"
            );
        }
        let planned_pushes = republished;
        let republished = planned_pushes.len();
        for (principal, snapshot) in planned_pushes {"#,
        replace: r#"                 subscribers will resync"
            );
        }
        let planned_pushes = republished;
        let republished = planned_pushes.len();
        for (principal, snapshot) in planned_pushes.into_iter().take(0) {"#,
        witnesses: &[
            store(Target::Test("store_suite"), "a_capacity_class_change_pushes_every_republished_principal"),
        ],
    },
    // ---- Store pushes: PostgreSQL ----
    Mutant {
        id: "postgres-push-publish",
        disables: "pushing a publication",
        file: STORE_POSTGRES_LIB,
        find: "let (written, published, before, after) = finish_transaction(tx, result).await?;
        if written {",
        replace: "let (written, published, before, after) = finish_transaction(tx, result).await?;
        if false && written {",
        witnesses: &[
            store_postgres(Target::Test("postgres_suite"), "publish_pushes_to_subscribers_only_when_the_row_changes"),
        ],
    },
    Mutant {
        id: "postgres-push-removal",
        disables: "pushing a removal",
        file: STORE_POSTGRES_LIB,
        find: "        if receipt.before != receipt.after
            && let AdminState::Snapshot { generation, .. } = receipt.after
",
        replace: "        if false
            && let AdminState::Snapshot { generation, .. } = receipt.after
",
        witnesses: &[
            store_postgres(Target::Test("postgres_suite"), "publish_pushes_to_subscribers_only_when_the_row_changes"),
        ],
    },
    Mutant {
        id: "postgres-push-key",
        disables: "pushing a key-bound publication",
        file: STORE_POSTGRES_LIB,
        find: "            finish_transaction(tx, result).await?;
        if written {
            self.push_to_subscribers",
        replace: "            finish_transaction(tx, result).await?;
        if false && written {
            self.push_to_subscribers",
        witnesses: &[
            store_postgres(Target::Test("credential_activity"), "key_bound_publication_resolves_the_principal_in_the_store"),
        ],
    },
    Mutant {
        id: "postgres-push-status",
        disables: "pushing a status change",
        file: STORE_POSTGRES_LIB,
        find: r#""status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished {"#,
        replace: r#""status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished.into_iter().take(0) {"#,
        witnesses: &[
            store_postgres(Target::Test("postgres_suite"), "a_status_change_pushes_every_republished_principal"),
        ],
    },
    Mutant {
        id: "postgres-push-class",
        disables: "pushing a capacity class change",
        file: STORE_POSTGRES_LIB,
        find: r#"                 subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished {"#,
        replace: r#"                 subscribers will resync"
            );
        }
        let count = republished.len();
        for (principal, snapshot) in republished.into_iter().take(0) {"#,
        witnesses: &[
            store_postgres(Target::Test("postgres_suite"), "a_capacity_class_change_pushes_every_republished_principal"),
        ],
    },
    // ---- Example service ----
    Mutant {
        id: "example-republish",
        disables: "the demo control plane's periodic republication",
        file: EXAMPLE_LIB,
        find: "        tick.tick().await;
        let Some(next) = generation.checked_add(1) else {",
        replace: "        std::future::pending::<()>().await;
        let Some(next) = generation.checked_add(1) else {",
        witnesses: &[
            pricing_api(Target::Lib, "tests::republishing_advances_the_generation_each_time"),
        ],
    },
];
