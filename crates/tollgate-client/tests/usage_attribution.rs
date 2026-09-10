use async_trait::async_trait;
use jiff::Timestamp;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tollgate_client::{UsageWriter, UsageWriterConfig};
use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, KeyId, PolicyRevision, Principal,
    RequestId, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, CredentialActivityState, GrantPolicy, IngestError, IngestReport, KeyDirectory,
    KeyRecord, ManualClock, MemoryStore, StoreError, UsageSink,
};

struct UncertainSink {
    store: Arc<MemoryStore>,
    calls: AtomicUsize,
    commit_before_lost_reply: bool,
}

#[async_trait]
impl UsageSink for UncertainSink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        let first = self.calls.fetch_add(1, Ordering::Relaxed) == 0;
        if first && !self.commit_before_lost_reply {
            return Ok(IngestReport::default()); // Incomplete success from a custom sink.
        }
        let report = self.store.ingest(events, now).await?;
        if first {
            Err(IngestError::Unavailable(StoreError(
                "fixture lost acknowledgement".into(),
            )))
        } else {
            Ok(report)
        }
    }
}

fn config() -> UsageWriterConfig {
    UsageWriterConfig {
        queue_capacity: 4,
        max_batch: 1,
        flush_interval: Duration::from_millis(1),
        retry_backoff: Duration::from_millis(1),
        ingest_timeout: Duration::from_millis(10),
        shutdown_drain_deadline: Duration::from_secs(1),
    }
}

#[tokio::test(start_paused = true)]
async fn both_writer_drains_preserve_evidence_until_a_complete_acknowledgement() {
    for shutdown_immediately in [false, true] {
        for commit_before_lost_reply in [false, true] {
            let store = MemoryStore::new(GrantPolicy::default()).unwrap();
            store.create_account(AccountConfig {
                account_id: AccountId(1),
                initial_balance: CostUnits(10),
                status: AccountStatus::Active,
                capacity_class: CapacityClass::Assured,
            });
            let mut digest = [0x88; 32];
            digest[..16].copy_from_slice(&1u128.to_be_bytes());
            store
                .insert_key(KeyRecord {
                    key_id: KeyId(1),
                    account_id: AccountId(1),
                    principal: Principal(1),
                    digest,
                    not_after: None,
                })
                .await
                .unwrap();
            let sink = Arc::new(UncertainSink {
                store: store.clone(),
                calls: AtomicUsize::new(0),
                commit_before_lost_reply,
            });
            let (recorder, writer) = UsageWriter::spawn(
                sink.clone(),
                Arc::new(ManualClock::new(Timestamp::UNIX_EPOCH)),
                config(),
            )
            .unwrap();
            recorder.try_reserve().unwrap().record(UsageEvent::new(
                RequestId(105),
                AccountId(1),
                UsageSource::Overage,
                CostUnits(1),
                Timestamp::UNIX_EPOCH,
                PolicyRevision::UNSTATED,
                Some(KeyId(1)),
            ));
            if !shutdown_immediately {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while sink.calls.load(Ordering::Relaxed) < 2 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
            }
            let stats = writer.shutdown().await.unwrap();
            assert_eq!(sink.calls.load(Ordering::Relaxed), 2);
            assert_eq!((stats.lost, stats.unresolved, stats.rejected), (0, 0, 0));
            assert_eq!(stats.accepted + stats.duplicate, 1);
            assert_eq!(stats.duplicate, u64::from(commit_before_lost_reply));
            assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(1));
            assert_eq!(
                store.credential_activity(&[KeyId(1)]).await.unwrap()[0].state,
                CredentialActivityState::Committed {
                    last_committed_at: Timestamp::UNIX_EPOCH
                }
            );
        }
    }
}
