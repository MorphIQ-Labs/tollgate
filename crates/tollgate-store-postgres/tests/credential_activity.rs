#[path = "../../tollgate-store/tests/support/credential_activity.rs"]
mod scenarios;
use tollgate_store::{GrantPolicy, KeyDirectory, UsageSink};
use tollgate_store_postgres::PostgresStore;
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn postgres_startup_never_ignores_an_unknown_applied_migration() {
    let _guard = DB_LOCK.lock().await;
    let Some(_store) = store().await else { return };
    let url = std::env::var("TOLLGATE_PG_URL").unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    // Synthetic metadata exists only in this isolated fixture. Production
    // operators must never delete real migration history to bypass startup.
    sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, 'fixture unknown migration', true, $2, 0)")
        .bind(i64::MAX).bind(vec![0u8; 48]).execute(&pool).await.unwrap();
    let outcome = PostgresStore::connect(&url, GrantPolicy::default()).await;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = $1")
        .bind(i64::MAX)
        .execute(&pool)
        .await
        .unwrap();
    assert!(outcome.is_err(), "unknown applied schemas must fail closed");
    PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn older_migration_catalogues_refuse_restart_without_erasing_history() {
    use sqlx::Connection;
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    store
        .ingest(
            &[scenarios::event(105, Some(1), 20)],
            jiff::Timestamp::UNIX_EPOCH,
        )
        .await
        .unwrap();
    // Released v0.17 knows migrations through 0013. Exercise SQLx's actual
    // startup guard with that catalogue, not only legacy INSERT shapes.
    let mut legacy = sqlx::migrate!("./migrations");
    legacy.migrations = std::borrow::Cow::Owned(
        legacy
            .iter()
            .filter(|migration| migration.version <= 13)
            .cloned()
            .collect(),
    );
    let url = std::env::var("TOLLGATE_PG_URL").unwrap();
    let mut connection = sqlx::PgConnection::connect(&url).await.unwrap();
    assert!(matches!(
        legacy.run(&mut connection).await,
        Err(sqlx::migrate::MigrateError::VersionMissing(14))
    ));
    // Closing a failed startup connection also releases its advisory lock.
    connection.close().await.unwrap();
    let reopened = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .credential_activity(&[tollgate_core::KeyId(1)])
            .await
            .unwrap()[0]
            .state,
        tollgate_store::CredentialActivityState::Committed {
            last_committed_at: jiff::Timestamp::from_second(20).unwrap()
        }
    );
}

#[tokio::test]
async fn credential_expiry_preserves_the_final_fractional_second() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    let mut digest = [0x77; 32];
    digest[..16].copy_from_slice(&999u128.to_be_bytes());
    store
        .insert_key(tollgate_store::KeyRecord {
            key_id: tollgate_core::KeyId(999),
            account_id: tollgate_core::AccountId(1),
            principal: tollgate_core::Principal(999),
            digest,
            not_after: Some(jiff::Timestamp::MAX),
        })
        .await
        .unwrap();
    let keys = store
        .active_keys(jiff::Timestamp::UNIX_EPOCH)
        .await
        .unwrap();
    let until = keys
        .iter()
        .find(|key| key.key_id == tollgate_core::KeyId(999))
        .unwrap()
        .not_after
        .unwrap();
    assert_eq!(
        until.as_microsecond(),
        jiff::Timestamp::MAX.as_microsecond()
    );
}

#[tokio::test]
async fn activity_failure_rolls_back_billing_and_source_metadata() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    let pool = sqlx::PgPool::connect(&std::env::var("TOLLGATE_PG_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE tollgate_credential_activity ADD CONSTRAINT fixture_refuse CHECK (false) NOT VALID")
        .execute(&pool).await.unwrap();
    let event = scenarios::event(105, Some(1), 10);
    let failed = store.ingest(&[event], jiff::Timestamp::UNIX_EPOCH).await;
    sqlx::query("ALTER TABLE tollgate_credential_activity DROP CONSTRAINT fixture_refuse")
        .execute(&pool)
        .await
        .unwrap();
    assert!(failed.unwrap_err().is_retryable());
    let records: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tollgate_usage_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        records, 0,
        "failed activity promotion must roll back the source row"
    );
    assert_eq!(
        store
            .credential_activity(&[tollgate_core::KeyId(1)])
            .await
            .unwrap()[0]
            .state,
        tollgate_store::CredentialActivityState::Unobserved
    );
    let report = store
        .ingest(&[event], jiff::Timestamp::UNIX_EPOCH)
        .await
        .unwrap();
    assert_eq!(
        (report.accepted, report.duplicate, report.unattributed),
        (1, 0, Some(0))
    );
    let usage: i64 =
        sqlx::query_scalar("SELECT usage_recorded FROM tollgate_accounts WHERE account_id = $1")
            .bind(1u128.to_be_bytes().to_vec())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(usage, 1);
}

#[tokio::test]
async fn a_commit_failure_after_activity_staging_preserves_the_predecessor() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    let pool = sqlx::PgPool::connect(&std::env::var("TOLLGATE_PG_URL").unwrap())
        .await
        .unwrap();
    store
        .ingest(
            &[scenarios::event(104, Some(1), 10)],
            jiff::Timestamp::UNIX_EPOCH,
        )
        .await
        .unwrap();
    sqlx::query("CREATE FUNCTION fixture_reject_activity_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture deferred commit failure'; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE CONSTRAINT TRIGGER fixture_activity_commit AFTER INSERT OR UPDATE ON tollgate_credential_activity DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fixture_reject_activity_commit()")
        .execute(&pool).await.unwrap();
    let event = scenarios::event(105, Some(1), 20);
    let failed = store.ingest(&[event], jiff::Timestamp::UNIX_EPOCH).await;
    sqlx::query("DROP TRIGGER fixture_activity_commit ON tollgate_credential_activity")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fixture_reject_activity_commit()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(failed.unwrap_err().is_retryable());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tollgate_usage_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    let usage: i64 =
        sqlx::query_scalar("SELECT usage_recorded FROM tollgate_accounts WHERE account_id = $1")
            .bind(1u128.to_be_bytes().to_vec())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(usage, 1);
    assert_eq!(
        store
            .credential_activity(&[tollgate_core::KeyId(1)])
            .await
            .unwrap()[0]
            .state,
        tollgate_store::CredentialActivityState::Committed {
            last_committed_at: jiff::Timestamp::from_second(10).unwrap()
        }
    );
    let retry = store
        .ingest(&[event], jiff::Timestamp::UNIX_EPOCH)
        .await
        .unwrap();
    assert_eq!((retry.accepted, retry.duplicate), (1, 0));
}

#[tokio::test]
async fn activity_and_source_identity_survive_restart_and_reset_together() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    store
        .ingest(
            &[scenarios::event(105, Some(1), 20)],
            jiff::Timestamp::UNIX_EPOCH,
        )
        .await
        .unwrap();
    let url = std::env::var("TOLLGATE_PG_URL").unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let recorded: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT key_id FROM tollgate_usage_events WHERE request_id = $1")
            .bind(105u128.to_be_bytes().to_vec())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recorded, Some(1u128.to_be_bytes().to_vec()));
    // Old writers omit the additive column. The historical identity remains
    // unknown; a later duplicate may not invent it.
    sqlx::query("INSERT INTO tollgate_usage_events (request_id, account_id, units, occurred_at_us) VALUES ($1,$2,0,0)")
        .bind(106u128.to_be_bytes().to_vec()).bind(1u128.to_be_bytes().to_vec()).execute(&pool).await.unwrap();
    let replay = store
        .ingest(
            &[scenarios::event(106, Some(2), 90)],
            jiff::Timestamp::UNIX_EPOCH,
        )
        .await
        .unwrap();
    assert_eq!((replay.accepted, replay.duplicate), (0, 1));
    let reopened = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .unwrap();
    let activity = reopened
        .credential_activity(&[tollgate_core::KeyId(1), tollgate_core::KeyId(2)])
        .await
        .unwrap();
    assert_eq!(
        activity[0].state,
        tollgate_store::CredentialActivityState::Committed {
            last_committed_at: jiff::Timestamp::from_second(20).unwrap(),
        }
    );
    assert_eq!(
        activity[1].state,
        tollgate_store::CredentialActivityState::Unobserved
    );
    reopened.truncate_all().await.unwrap();
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tollgate_credential_activity")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn competing_request_ids_preserve_the_first_attribution() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    let a = [scenarios::event(1, Some(1), 10)];
    let b = [scenarios::event(1, Some(2), 90)];
    let (first, second) = tokio::join!(
        store.ingest(&a, jiff::Timestamp::UNIX_EPOCH),
        store.ingest(&b, jiff::Timestamp::UNIX_EPOCH)
    );
    assert!(first.is_ok() || second.is_ok());
    for (outcome, batch) in [(first, a), (second, b)] {
        if let Err(error) = outcome {
            assert!(error.is_retryable());
            assert_eq!(
                store
                    .ingest(&batch, jiff::Timestamp::UNIX_EPOCH)
                    .await
                    .unwrap()
                    .duplicate,
                1
            );
        }
    }
    let states = store
        .credential_activity(&[tollgate_core::KeyId(1), tollgate_core::KeyId(2)])
        .await
        .unwrap();
    assert_eq!(
        states
            .iter()
            .filter(|row| matches!(
                row.state,
                tollgate_store::CredentialActivityState::Committed { .. }
            ))
            .count(),
        1
    );
}
async fn store() -> Option<std::sync::Arc<PostgresStore>> {
    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        assert!(
            std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
            "PostgreSQL is required"
        );
        eprintln!("SKIPPED: TOLLGATE_PG_URL is unset");
        return None;
    };
    let store = PostgresStore::connect(&url, GrantPolicy::default())
        .await
        .expect("isolated PostgreSQL fixture connects");
    store.truncate_all().await.unwrap();
    Some(store)
}

#[tokio::test]
async fn committed_usage_attributes_each_event_and_preserves_replay_identity() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::committed_usage_attributes_each_event_and_preserves_replay_identity(&*store).await;
}

#[tokio::test]
async fn missing_unknown_and_wrong_account_attribution_preserve_billing() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::missing_unknown_and_wrong_account_attribution_preserve_billing(&*store).await;
}

#[tokio::test]
async fn retired_activity_never_changes_the_credential_revision() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::retired_activity_never_changes_the_credential_revision(&*store).await;
}

#[tokio::test]
async fn activity_reads_preserve_every_requested_key_and_its_state() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::activity_reads_preserve_every_requested_key_and_its_state(&*store).await;
}

#[tokio::test]
async fn publication_checks_the_stated_credential_binding() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::publication_checks_the_stated_credential_binding(&*store).await;
}

#[tokio::test]
async fn concurrent_activity_commits_converge_to_the_maximum() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::concurrent_activity_commits_converge_to_the_maximum(&*store).await;
}

#[tokio::test]
async fn activity_uses_durable_microsecond_precision() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::activity_uses_durable_microsecond_precision(&*store).await;
}

#[tokio::test]
async fn a_failed_batch_preserves_activity_and_canonical_events() {
    let _guard = DB_LOCK.lock().await;
    let Some(store) = store().await else { return };
    scenarios::setup(&*store).await;
    scenarios::a_failed_batch_preserves_activity_and_canonical_events(&*store).await;
}
