//! One store double for every test crate, so that delegation is written
//! once and the trait defaults are decided once (#83).
//!
//! Each of the seven store traits is implemented for [`DelegatingStore<S>`].
//! A method with no hook falls through to `S`; a method with a hook calls it.
//! Two methods are exceptions, because they carry a default body and the
//! right answer differs between them -- see `reclaim_expired` and
//! `principals` below. Those two comments are the point of this file.
//!
//! `RejectingStore` is the inner store for doubles that assert a code path
//! touches *nothing* it does not hook: every un-hooked method panics and
//! names itself.
#![allow(
    dead_code,
    reason = "shared fixture module; each test binary uses a subset"
)]
#![deny(
    clippy::missing_trait_methods,
    reason = "a trait method that gains a default body must not be inherited \
              here by accident -- this file is the one place the forward/inherit \
              decision is made, and omitting a method is how #83 arose 44 times"
)]

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountStatus, BudgetSchedule, CapacityClass, CostUnits, FencingToken, KeyId,
    LeaseId, Principal, PublishableSnapshot, UsageEvent,
};
use tollgate_store::{
    AccountConfig, AccountView, AdminReceipt, AdminStore, AllocateError, Allocation, BudgetError,
    CreateAccountError, CredentialActivity, IngestError, IngestReport, KeyDirectory, KeyError,
    KeyPage, KeyRecord, KeySource, KeySummary, LeaseAllocator, PublishSnapshotError, ReclaimBatch,
    ReclaimedLease, Revocation, RolloverBatch, SetStatusError, SnapshotPush, SnapshotResolution,
    SnapshotSource, StatusChange, StoreError, StoreHealth, UsageSink, drain_reclaim_expired,
};

/// A hook's future. Owned and `'static`: nothing borrowed from the wrapper
/// survives into it, which is what keeps closure inference working at the
/// call sites. A hook that needs shared state captures an `Arc`.
pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// One async override: the wrapped store, the method's owned arguments, its
/// result. `Arc` rather than `Box` so the wrapper stays cheap to clone.
type Hook<S, A, R> = Arc<dyn Fn(Arc<S>, A) -> BoxFut<R> + Send + Sync + 'static>;

/// `SnapshotSource::subscribe` is not async.
type SyncHook<S, R> = Arc<dyn Fn(Arc<S>) -> R + Send + Sync + 'static>;

/// Wraps `S` and forwards every store trait to it, except where a hook says
/// otherwise.
///
/// `S` is generic rather than `dyn`, so `DelegatingStore<S>` implements
/// exactly the traits `S` implements -- which is what lets it wrap a partial
/// store such as `HttpStore` without inventing the traits it lacks.
// One alias per method. Spelled out rather than inlined so the struct below
// reads as a list of methods, and so `clippy::type_complexity` has a name to
// point at instead of a nested `Option<Arc<dyn Fn(..) -> Pin<Box<..>>>>`.
type AcquireHook<S> =
    Hook<S, (AccountId, CostUnits, SignedDuration, Timestamp), Result<Allocation, AllocateError>>;
type ReleaseHook<S> =
    Hook<S, (LeaseId, FencingToken, CostUnits, Timestamp), Result<(), AllocateError>>;
type ConsolidateHook<S> = Hook<
    S,
    (
        LeaseId,
        FencingToken,
        CostUnits,
        CostUnits,
        SignedDuration,
        Timestamp,
    ),
    Result<Allocation, AllocateError>,
>;
type ReclaimExpiredBatchHook<S> =
    Hook<S, (Timestamp, NonZeroUsize), Result<ReclaimBatch, StoreError>>;
type ReclaimExpiredHook<S> = Hook<S, Timestamp, Result<Vec<ReclaimedLease>, StoreError>>;
type SnapshotHook<S> = Hook<S, Principal, Result<SnapshotResolution, StoreError>>;
type SubscribeHook<S> = SyncHook<S, broadcast::Receiver<SnapshotPush>>;
type PrincipalsHook<S> = Hook<S, (), Result<Option<Vec<Principal>>, StoreError>>;
type IngestHook<S> = Hook<S, (Vec<UsageEvent>, Timestamp), Result<IngestReport, IngestError>>;
type CreateAccountHook<S> = Hook<S, AccountConfig, Result<AdminReceipt<()>, CreateAccountError>>;
type DepositHook<S> = Hook<S, (AccountId, CostUnits), Result<AdminReceipt<()>, AllocateError>>;
type SetAccountStatusHook<S> =
    Hook<S, (AccountId, AccountStatus), Result<AdminReceipt<StatusChange>, SetStatusError>>;
type SetCapacityClassHook<S> =
    Hook<S, (AccountId, CapacityClass), Result<AdminReceipt<StatusChange>, SetStatusError>>;
type SetBudgetScheduleHook<S> =
    Hook<S, (AccountId, Option<BudgetSchedule>), Result<AdminReceipt<()>, BudgetError>>;
type RollDuePeriodsHook<S> = Hook<S, (Timestamp, NonZeroUsize), Result<RolloverBatch, StoreError>>;
type PublishSnapshotHook<S> =
    Hook<S, (Principal, PublishableSnapshot), Result<AdminReceipt<()>, PublishSnapshotError>>;
type RemoveSnapshotHook<S> = Hook<S, Principal, Result<AdminReceipt<()>, StoreError>>;
type AccountViewHook<S> = Hook<S, AccountId, Result<Option<AccountView>, StoreError>>;
type ActiveKeysPageHook<S> =
    Hook<S, (Timestamp, Option<KeyId>, NonZeroUsize), Result<KeyPage, StoreError>>;
type CredentialActivityHook<S> = Hook<S, Vec<KeyId>, Result<Vec<CredentialActivity>, StoreError>>;
type InsertKeyHook<S> = Hook<S, KeyRecord, Result<(), KeyError>>;
type RevokeKeyHook<S> = Hook<S, (KeyId, Timestamp), Result<Revocation, KeyError>>;
type RevokeKeyAuditedHook<S> =
    Hook<S, (KeyId, Timestamp), Result<AdminReceipt<Revocation>, KeyError>>;
type ActiveKeysHook<S> = Hook<S, Timestamp, Result<Vec<KeyRecord>, StoreError>>;
type AccountKeysHook<S> =
    Hook<S, (AccountId, Option<KeyId>, NonZeroUsize), Result<Vec<KeySummary>, StoreError>>;
type InsertKeyWithinHook<S> = Hook<S, (KeyRecord, NonZeroUsize, Timestamp), Result<(), KeyError>>;
type InsertKeyWithinAuditedHook<S> =
    Hook<S, (KeyRecord, NonZeroUsize, Timestamp), Result<AdminReceipt<()>, KeyError>>;
type PingHook<S> = Hook<S, (), Result<(), StoreError>>;

pub struct DelegatingStore<S> {
    inner: Arc<S>,
    // LeaseAllocator
    acquire: Option<AcquireHook<S>>,
    release: Option<ReleaseHook<S>>,
    consolidate: Option<ConsolidateHook<S>>,
    reclaim_expired_batch: Option<ReclaimExpiredBatchHook<S>>,
    reclaim_expired: Option<ReclaimExpiredHook<S>>,
    // SnapshotSource
    snapshot: Option<SnapshotHook<S>>,
    subscribe: Option<SubscribeHook<S>>,
    principals: Option<PrincipalsHook<S>>,
    // UsageSink
    ingest: Option<IngestHook<S>>,
    // AdminStore
    create_account: Option<CreateAccountHook<S>>,
    deposit: Option<DepositHook<S>>,
    set_account_status: Option<SetAccountStatusHook<S>>,
    set_capacity_class: Option<SetCapacityClassHook<S>>,
    set_budget_schedule: Option<SetBudgetScheduleHook<S>>,
    roll_due_periods: Option<RollDuePeriodsHook<S>>,
    publish_snapshot: Option<PublishSnapshotHook<S>>,
    remove_snapshot: Option<RemoveSnapshotHook<S>>,
    account_view: Option<AccountViewHook<S>>,
    // KeySource
    active_keys_page: Option<ActiveKeysPageHook<S>>,
    // KeyDirectory
    credential_activity: Option<CredentialActivityHook<S>>,
    insert_key: Option<InsertKeyHook<S>>,
    revoke_key: Option<RevokeKeyHook<S>>,
    revoke_key_audited: Option<RevokeKeyAuditedHook<S>>,
    active_keys: Option<ActiveKeysHook<S>>,
    account_keys: Option<AccountKeysHook<S>>,
    insert_key_within: Option<InsertKeyWithinHook<S>>,
    insert_key_within_audited: Option<InsertKeyWithinAuditedHook<S>>,
    // StoreHealth
    ping: Option<PingHook<S>>,
}

impl<S: Send + Sync + 'static> DelegatingStore<S> {
    /// Forward everything to `inner`.
    pub fn wrapping(inner: Arc<S>) -> Self {
        Self {
            inner,
            acquire: None,
            release: None,
            consolidate: None,
            reclaim_expired_batch: None,
            reclaim_expired: None,
            snapshot: None,
            subscribe: None,
            principals: None,
            ingest: None,
            create_account: None,
            deposit: None,
            set_account_status: None,
            set_capacity_class: None,
            set_budget_schedule: None,
            roll_due_periods: None,
            publish_snapshot: None,
            remove_snapshot: None,
            account_view: None,
            active_keys_page: None,
            credential_activity: None,
            insert_key: None,
            revoke_key: None,
            revoke_key_audited: None,
            active_keys: None,
            account_keys: None,
            insert_key_within: None,
            insert_key_within_audited: None,
            ping: None,
        }
    }

    /// The wrapped store, for a hook that wants it outside a call.
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }

    /// Replace [`LeaseAllocator::acquire`].
    pub fn on_acquire<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, CostUnits, SignedDuration, Timestamp) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<Allocation, AllocateError>> + Send + 'static,
    {
        self.acquire = Some(Arc::new(move |inner, (account, requested, ttl, now)| {
            Box::pin(f(inner, account, requested, ttl, now))
        }));
        self
    }

    /// Replace [`LeaseAllocator::release`].
    pub fn on_release<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, LeaseId, FencingToken, CostUnits, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AllocateError>> + Send + 'static,
    {
        self.release = Some(Arc::new(
            move |inner, (lease_id, fencing_token, unspent, now)| {
                Box::pin(f(inner, lease_id, fencing_token, unspent, now))
            },
        ));
        self
    }

    /// Replace [`LeaseAllocator::consolidate`].
    pub fn on_consolidate<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(
                Arc<S>,
                LeaseId,
                FencingToken,
                CostUnits,
                CostUnits,
                SignedDuration,
                Timestamp,
            ) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<Allocation, AllocateError>> + Send + 'static,
    {
        self.consolidate = Some(Arc::new(
            move |inner, (lease_id, fencing_token, unspent, requested, ttl, now)| {
                Box::pin(f(
                    inner,
                    lease_id,
                    fencing_token,
                    unspent,
                    requested,
                    ttl,
                    now,
                ))
            },
        ));
        self
    }

    /// Replace [`LeaseAllocator::reclaim_expired_batch`].
    pub fn on_reclaim_expired_batch<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Timestamp, NonZeroUsize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ReclaimBatch, StoreError>> + Send + 'static,
    {
        self.reclaim_expired_batch = Some(Arc::new(move |inner, (now, limit)| {
            Box::pin(f(inner, now, limit))
        }));
        self
    }

    /// Replace [`LeaseAllocator::reclaim_expired`].
    pub fn on_reclaim_expired<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<ReclaimedLease>, StoreError>> + Send + 'static,
    {
        self.reclaim_expired = Some(Arc::new(move |inner, now| Box::pin(f(inner, now))));
        self
    }

    /// Replace [`SnapshotSource::snapshot`].
    pub fn on_snapshot<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Principal) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SnapshotResolution, StoreError>> + Send + 'static,
    {
        self.snapshot = Some(Arc::new(move |inner, principal| {
            Box::pin(f(inner, principal))
        }));
        self
    }

    /// Replace [`SnapshotSource::subscribe`].
    pub fn on_subscribe<F>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>) -> broadcast::Receiver<SnapshotPush> + Send + Sync + 'static,
    {
        self.subscribe = Some(Arc::new(f));
        self
    }

    /// Replace [`SnapshotSource::principals`].
    pub fn on_principals<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Vec<Principal>>, StoreError>> + Send + 'static,
    {
        self.principals = Some(Arc::new(move |inner, ()| Box::pin(f(inner))));
        self
    }

    /// Replace [`UsageSink::ingest`].
    pub fn on_ingest<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Vec<UsageEvent>, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<IngestReport, IngestError>> + Send + 'static,
    {
        self.ingest = Some(Arc::new(move |inner, (events, now)| {
            Box::pin(f(inner, events, now))
        }));
        self
    }

    /// Replace [`AdminStore::create_account`].
    pub fn on_create_account<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountConfig) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, CreateAccountError>> + Send + 'static,
    {
        self.create_account = Some(Arc::new(move |inner, config| Box::pin(f(inner, config))));
        self
    }

    /// Replace [`AdminStore::deposit`].
    pub fn on_deposit<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, CostUnits) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, AllocateError>> + Send + 'static,
    {
        self.deposit = Some(Arc::new(move |inner, (account, units)| {
            Box::pin(f(inner, account, units))
        }));
        self
    }

    /// Replace [`AdminStore::set_account_status`].
    pub fn on_set_account_status<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, AccountStatus) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<StatusChange>, SetStatusError>> + Send + 'static,
    {
        self.set_account_status = Some(Arc::new(move |inner, (account, status)| {
            Box::pin(f(inner, account, status))
        }));
        self
    }

    /// Replace [`AdminStore::set_capacity_class`].
    pub fn on_set_capacity_class<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, CapacityClass) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<StatusChange>, SetStatusError>> + Send + 'static,
    {
        self.set_capacity_class = Some(Arc::new(move |inner, (account, class)| {
            Box::pin(f(inner, account, class))
        }));
        self
    }

    /// Replace [`AdminStore::set_budget_schedule`].
    pub fn on_set_budget_schedule<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, Option<BudgetSchedule>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, BudgetError>> + Send + 'static,
    {
        self.set_budget_schedule = Some(Arc::new(move |inner, (account, schedule)| {
            Box::pin(f(inner, account, schedule))
        }));
        self
    }

    /// Replace [`AdminStore::roll_due_periods`].
    pub fn on_roll_due_periods<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Timestamp, NonZeroUsize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RolloverBatch, StoreError>> + Send + 'static,
    {
        self.roll_due_periods = Some(Arc::new(move |inner, (now, limit)| {
            Box::pin(f(inner, now, limit))
        }));
        self
    }

    /// Replace [`AdminStore::publish_snapshot`].
    pub fn on_publish_snapshot<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Principal, PublishableSnapshot) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, PublishSnapshotError>> + Send + 'static,
    {
        self.publish_snapshot = Some(Arc::new(move |inner, (principal, snapshot)| {
            Box::pin(f(inner, principal, snapshot))
        }));
        self
    }

    /// Replace [`AdminStore::remove_snapshot`].
    pub fn on_remove_snapshot<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Principal) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, StoreError>> + Send + 'static,
    {
        self.remove_snapshot = Some(Arc::new(move |inner, principal| {
            Box::pin(f(inner, principal))
        }));
        self
    }

    /// Replace [`AdminStore::account_view`].
    pub fn on_account_view<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<AccountView>, StoreError>> + Send + 'static,
    {
        self.account_view = Some(Arc::new(move |inner, account| Box::pin(f(inner, account))));
        self
    }

    /// Replace [`KeySource::active_keys_page`].
    pub fn on_active_keys_page<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Timestamp, Option<KeyId>, NonZeroUsize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<KeyPage, StoreError>> + Send + 'static,
    {
        self.active_keys_page = Some(Arc::new(move |inner, (now, after, limit)| {
            Box::pin(f(inner, now, after, limit))
        }));
        self
    }

    /// Replace [`KeyDirectory::credential_activity`].
    pub fn on_credential_activity<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Vec<KeyId>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<CredentialActivity>, StoreError>> + Send + 'static,
    {
        self.credential_activity = Some(Arc::new(move |inner, keys| Box::pin(f(inner, keys))));
        self
    }

    /// Replace [`KeyDirectory::insert_key`].
    pub fn on_insert_key<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, KeyRecord) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), KeyError>> + Send + 'static,
    {
        self.insert_key = Some(Arc::new(move |inner, record| Box::pin(f(inner, record))));
        self
    }

    /// Replace [`KeyDirectory::revoke_key`].
    pub fn on_revoke_key<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, KeyId, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Revocation, KeyError>> + Send + 'static,
    {
        self.revoke_key = Some(Arc::new(move |inner, (key_id, now)| {
            Box::pin(f(inner, key_id, now))
        }));
        self
    }

    /// Replace [`KeyDirectory::revoke_key_audited`].
    pub fn on_revoke_key_audited<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, KeyId, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<Revocation>, KeyError>> + Send + 'static,
    {
        self.revoke_key_audited = Some(Arc::new(move |inner, (key_id, now)| {
            Box::pin(f(inner, key_id, now))
        }));
        self
    }

    /// Replace [`KeyDirectory::active_keys`].
    pub fn on_active_keys<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<KeyRecord>, StoreError>> + Send + 'static,
    {
        self.active_keys = Some(Arc::new(move |inner, now| Box::pin(f(inner, now))));
        self
    }

    /// Replace [`KeyDirectory::account_keys`].
    pub fn on_account_keys<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, AccountId, Option<KeyId>, NonZeroUsize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<KeySummary>, StoreError>> + Send + 'static,
    {
        self.account_keys = Some(Arc::new(move |inner, (account, after, limit)| {
            Box::pin(f(inner, account, after, limit))
        }));
        self
    }

    /// Replace [`KeyDirectory::insert_key_within`].
    pub fn on_insert_key_within<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, KeyRecord, NonZeroUsize, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), KeyError>> + Send + 'static,
    {
        self.insert_key_within = Some(Arc::new(move |inner, (record, max_active, now)| {
            Box::pin(f(inner, record, max_active, now))
        }));
        self
    }

    /// Replace [`KeyDirectory::insert_key_within_audited`].
    pub fn on_insert_key_within_audited<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>, KeyRecord, NonZeroUsize, Timestamp) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AdminReceipt<()>, KeyError>> + Send + 'static,
    {
        self.insert_key_within_audited = Some(Arc::new(move |inner, (record, max_active, now)| {
            Box::pin(f(inner, record, max_active, now))
        }));
        self
    }

    /// Replace [`StoreHealth::ping`].
    pub fn on_ping<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Arc<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StoreError>> + Send + 'static,
    {
        self.ping = Some(Arc::new(move |inner, ()| Box::pin(f(inner))));
        self
    }
}

#[async_trait]
impl<S> LeaseAllocator for DelegatingStore<S>
where
    S: LeaseAllocator + Send + Sync + 'static,
{
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        match &self.acquire {
            Some(hook) => hook(Arc::clone(&self.inner), (account, requested, ttl, now)).await,
            None => LeaseAllocator::acquire(&*self.inner, account, requested, ttl, now).await,
        }
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError> {
        match &self.release {
            Some(hook) => {
                hook(
                    Arc::clone(&self.inner),
                    (lease_id, fencing_token, unspent, now),
                )
                .await
            }
            None => {
                LeaseAllocator::release(&*self.inner, lease_id, fencing_token, unspent, now).await
            }
        }
    }

    async fn consolidate(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        match &self.consolidate {
            Some(hook) => {
                hook(
                    Arc::clone(&self.inner),
                    (lease_id, fencing_token, unspent, requested, ttl, now),
                )
                .await
            }
            None => {
                LeaseAllocator::consolidate(
                    &*self.inner,
                    lease_id,
                    fencing_token,
                    unspent,
                    requested,
                    ttl,
                    now,
                )
                .await
            }
        }
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        match &self.reclaim_expired_batch {
            Some(hook) => hook(Arc::clone(&self.inner), (now, limit)).await,
            None => LeaseAllocator::reclaim_expired_batch(&*self.inner, now, limit).await,
        }
    }

    // DELIBERATELY NOT FORWARDED. This default body is written over
    // `self.reclaim_expired_batch`, so running it against `self` re-enters
    // an `on_reclaim_expired_batch` hook. Forwarding to the inner store
    // would rebind that `self` and silently discard the hook -- and with
    // it every failure a test injects there. `crates/tollgate-server`'s
    // `/reclaim` route calls this method, so the bypass would be live.
    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        match &self.reclaim_expired {
            Some(hook) => hook(Arc::clone(&self.inner), now).await,
            None => drain_reclaim_expired(self, now).await,
        }
    }
}

#[async_trait]
impl<S> SnapshotSource for DelegatingStore<S>
where
    S: SnapshotSource + Send + Sync + 'static,
{
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        match &self.snapshot {
            Some(hook) => hook(Arc::clone(&self.inner), principal).await,
            None => SnapshotSource::snapshot(&*self.inner, principal).await,
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        match &self.subscribe {
            Some(hook) => hook(Arc::clone(&self.inner)),
            None => SnapshotSource::subscribe(&*self.inner),
        }
    }

    // DELIBERATELY FORWARDED, not inherited. The trait default is the
    // sentinel `Ok(None)` -- "this source cannot enumerate" -- and all
    // three real stores override it with a catalogue. Inheriting it is
    // exactly the defect in #83: a wrapper reports no catalogue while the
    // store it wraps has one. A double that wants the sentinel asks for
    // it by name, with `on_principals`.
    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        match &self.principals {
            Some(hook) => hook(Arc::clone(&self.inner), ()).await,
            None => SnapshotSource::principals(&*self.inner).await,
        }
    }
}

#[async_trait]
impl<S> UsageSink for DelegatingStore<S>
where
    S: UsageSink + Send + Sync + 'static,
{
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        match &self.ingest {
            Some(hook) => hook(Arc::clone(&self.inner), (events.to_vec(), now)).await,
            None => UsageSink::ingest(&*self.inner, events, now).await,
        }
    }
}

#[async_trait]
impl<S> AdminStore for DelegatingStore<S>
where
    S: AdminStore + Send + Sync + 'static,
{
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<AdminReceipt<()>, CreateAccountError> {
        match &self.create_account {
            Some(hook) => hook(Arc::clone(&self.inner), config).await,
            None => AdminStore::create_account(&*self.inner, config).await,
        }
    }

    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<AdminReceipt<()>, AllocateError> {
        match &self.deposit {
            Some(hook) => hook(Arc::clone(&self.inner), (account, units)).await,
            None => AdminStore::deposit(&*self.inner, account, units).await,
        }
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<AdminReceipt<StatusChange>, SetStatusError> {
        match &self.set_account_status {
            Some(hook) => hook(Arc::clone(&self.inner), (account, status)).await,
            None => AdminStore::set_account_status(&*self.inner, account, status).await,
        }
    }

    async fn set_capacity_class(
        &self,
        account: AccountId,
        class: CapacityClass,
    ) -> Result<AdminReceipt<StatusChange>, SetStatusError> {
        match &self.set_capacity_class {
            Some(hook) => hook(Arc::clone(&self.inner), (account, class)).await,
            None => AdminStore::set_capacity_class(&*self.inner, account, class).await,
        }
    }

    async fn set_budget_schedule(
        &self,
        account: AccountId,
        schedule: Option<BudgetSchedule>,
    ) -> Result<AdminReceipt<()>, BudgetError> {
        match &self.set_budget_schedule {
            Some(hook) => hook(Arc::clone(&self.inner), (account, schedule)).await,
            None => AdminStore::set_budget_schedule(&*self.inner, account, schedule).await,
        }
    }

    async fn roll_due_periods(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<RolloverBatch, StoreError> {
        match &self.roll_due_periods {
            Some(hook) => hook(Arc::clone(&self.inner), (now, limit)).await,
            None => AdminStore::roll_due_periods(&*self.inner, now, limit).await,
        }
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<AdminReceipt<()>, PublishSnapshotError> {
        match &self.publish_snapshot {
            Some(hook) => hook(Arc::clone(&self.inner), (principal, snapshot)).await,
            None => AdminStore::publish_snapshot(&*self.inner, principal, snapshot).await,
        }
    }

    async fn remove_snapshot(&self, principal: Principal) -> Result<AdminReceipt<()>, StoreError> {
        match &self.remove_snapshot {
            Some(hook) => hook(Arc::clone(&self.inner), principal).await,
            None => AdminStore::remove_snapshot(&*self.inner, principal).await,
        }
    }

    async fn account_view(&self, account: AccountId) -> Result<Option<AccountView>, StoreError> {
        match &self.account_view {
            Some(hook) => hook(Arc::clone(&self.inner), account).await,
            None => AdminStore::account_view(&*self.inner, account).await,
        }
    }
}

#[async_trait]
impl<S> KeySource for DelegatingStore<S>
where
    S: KeySource + Send + Sync + 'static,
{
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<KeyPage, StoreError> {
        match &self.active_keys_page {
            Some(hook) => hook(Arc::clone(&self.inner), (now, after, limit)).await,
            None => KeySource::active_keys_page(&*self.inner, now, after, limit).await,
        }
    }
}

#[async_trait]
impl<S> KeyDirectory for DelegatingStore<S>
where
    S: KeyDirectory + Send + Sync + 'static,
{
    async fn credential_activity(
        &self,
        keys: &[KeyId],
    ) -> Result<Vec<CredentialActivity>, StoreError> {
        match &self.credential_activity {
            Some(hook) => hook(Arc::clone(&self.inner), keys.to_vec()).await,
            None => KeyDirectory::credential_activity(&*self.inner, keys).await,
        }
    }

    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError> {
        match &self.insert_key {
            Some(hook) => hook(Arc::clone(&self.inner), record).await,
            None => KeyDirectory::insert_key(&*self.inner, record).await,
        }
    }

    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError> {
        match &self.revoke_key {
            Some(hook) => hook(Arc::clone(&self.inner), (key_id, now)).await,
            None => KeyDirectory::revoke_key(&*self.inner, key_id, now).await,
        }
    }

    async fn revoke_key_audited(
        &self,
        key_id: KeyId,
        now: Timestamp,
    ) -> Result<AdminReceipt<Revocation>, KeyError> {
        match &self.revoke_key_audited {
            Some(hook) => hook(Arc::clone(&self.inner), (key_id, now)).await,
            None => KeyDirectory::revoke_key_audited(&*self.inner, key_id, now).await,
        }
    }

    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        match &self.active_keys {
            Some(hook) => hook(Arc::clone(&self.inner), now).await,
            None => KeyDirectory::active_keys(&*self.inner, now).await,
        }
    }

    async fn account_keys(
        &self,
        account: AccountId,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<KeySummary>, StoreError> {
        match &self.account_keys {
            Some(hook) => hook(Arc::clone(&self.inner), (account, after, limit)).await,
            None => KeyDirectory::account_keys(&*self.inner, account, after, limit).await,
        }
    }

    async fn insert_key_within(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<(), KeyError> {
        match &self.insert_key_within {
            Some(hook) => hook(Arc::clone(&self.inner), (record, max_active, now)).await,
            None => KeyDirectory::insert_key_within(&*self.inner, record, max_active, now).await,
        }
    }

    async fn insert_key_within_audited(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<AdminReceipt<()>, KeyError> {
        match &self.insert_key_within_audited {
            Some(hook) => hook(Arc::clone(&self.inner), (record, max_active, now)).await,
            None => {
                KeyDirectory::insert_key_within_audited(&*self.inner, record, max_active, now).await
            }
        }
    }
}

#[async_trait]
impl<S> StoreHealth for DelegatingStore<S>
where
    S: StoreHealth + Send + Sync + 'static,
{
    async fn ping(&self) -> Result<(), StoreError> {
        match &self.ping {
            Some(hook) => hook(Arc::clone(&self.inner), ()).await,
            None => StoreHealth::ping(&*self.inner).await,
        }
    }
}

/// An inner store whose every method panics, naming itself.
///
/// For doubles whose claim is that a code path touches *nothing* but the
/// methods hooked on top of it -- `readiness.rs` is the example. The panic is
/// the assertion, so this must not be replaced by a real store.
///
/// It overrides the two defaulted methods too, so that forgetting to state a
/// default's behaviour is a panic here rather than a silent answer.
pub struct RejectingStore {
    reason: &'static str,
}

impl RejectingStore {
    pub fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

/// A double that rejects every store call it is not explicitly given.
pub fn rejecting(reason: &'static str) -> DelegatingStore<RejectingStore> {
    DelegatingStore::wrapping(Arc::new(RejectingStore::new(reason)))
}

#[async_trait]
impl LeaseAllocator for RejectingStore {
    async fn acquire(
        &self,
        _account: AccountId,
        _requested: CostUnits,
        _ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        unreachable!("{}: LeaseAllocator::acquire", self.reason)
    }

    async fn release(
        &self,
        _lease_id: LeaseId,
        _fencing_token: FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), AllocateError> {
        unreachable!("{}: LeaseAllocator::release", self.reason)
    }

    async fn consolidate(
        &self,
        _lease_id: LeaseId,
        _fencing_token: FencingToken,
        _unspent: CostUnits,
        _requested: CostUnits,
        _ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        unreachable!("{}: LeaseAllocator::consolidate", self.reason)
    }

    async fn reclaim_expired_batch(
        &self,
        _now: Timestamp,
        _limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        unreachable!("{}: LeaseAllocator::reclaim_expired_batch", self.reason)
    }

    async fn reclaim_expired(&self, _now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        unreachable!("{}: LeaseAllocator::reclaim_expired", self.reason)
    }
}

#[async_trait]
impl SnapshotSource for RejectingStore {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        unreachable!("{}: SnapshotSource::snapshot", self.reason)
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        unreachable!("{}: SnapshotSource::subscribe", self.reason)
    }

    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        unreachable!("{}: SnapshotSource::principals", self.reason)
    }
}

#[async_trait]
impl UsageSink for RejectingStore {
    async fn ingest(
        &self,
        _events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        unreachable!("{}: UsageSink::ingest", self.reason)
    }
}

#[async_trait]
impl AdminStore for RejectingStore {
    async fn create_account(
        &self,
        _config: AccountConfig,
    ) -> Result<AdminReceipt<()>, CreateAccountError> {
        unreachable!("{}: AdminStore::create_account", self.reason)
    }

    async fn deposit(
        &self,
        _account: AccountId,
        _units: CostUnits,
    ) -> Result<AdminReceipt<()>, AllocateError> {
        unreachable!("{}: AdminStore::deposit", self.reason)
    }

    async fn set_account_status(
        &self,
        _account: AccountId,
        _status: AccountStatus,
    ) -> Result<AdminReceipt<StatusChange>, SetStatusError> {
        unreachable!("{}: AdminStore::set_account_status", self.reason)
    }

    async fn set_capacity_class(
        &self,
        _account: AccountId,
        _class: CapacityClass,
    ) -> Result<AdminReceipt<StatusChange>, SetStatusError> {
        unreachable!("{}: AdminStore::set_capacity_class", self.reason)
    }

    async fn set_budget_schedule(
        &self,
        _account: AccountId,
        _schedule: Option<BudgetSchedule>,
    ) -> Result<AdminReceipt<()>, BudgetError> {
        unreachable!("{}: AdminStore::set_budget_schedule", self.reason)
    }

    async fn roll_due_periods(
        &self,
        _now: Timestamp,
        _limit: NonZeroUsize,
    ) -> Result<RolloverBatch, StoreError> {
        unreachable!("{}: AdminStore::roll_due_periods", self.reason)
    }

    async fn publish_snapshot(
        &self,
        _principal: Principal,
        _snapshot: PublishableSnapshot,
    ) -> Result<AdminReceipt<()>, PublishSnapshotError> {
        unreachable!("{}: AdminStore::publish_snapshot", self.reason)
    }

    async fn remove_snapshot(&self, _principal: Principal) -> Result<AdminReceipt<()>, StoreError> {
        unreachable!("{}: AdminStore::remove_snapshot", self.reason)
    }

    async fn account_view(&self, _account: AccountId) -> Result<Option<AccountView>, StoreError> {
        unreachable!("{}: AdminStore::account_view", self.reason)
    }
}

#[async_trait]
impl KeySource for RejectingStore {
    async fn active_keys_page(
        &self,
        _now: Timestamp,
        _after: Option<KeyId>,
        _limit: NonZeroUsize,
    ) -> Result<KeyPage, StoreError> {
        unreachable!("{}: KeySource::active_keys_page", self.reason)
    }
}

#[async_trait]
impl KeyDirectory for RejectingStore {
    async fn credential_activity(
        &self,
        _keys: &[KeyId],
    ) -> Result<Vec<CredentialActivity>, StoreError> {
        unreachable!("{}: KeyDirectory::credential_activity", self.reason)
    }

    async fn insert_key(&self, _record: KeyRecord) -> Result<(), KeyError> {
        unreachable!("{}: KeyDirectory::insert_key", self.reason)
    }

    async fn revoke_key(&self, _key_id: KeyId, _now: Timestamp) -> Result<Revocation, KeyError> {
        unreachable!("{}: KeyDirectory::revoke_key", self.reason)
    }

    async fn revoke_key_audited(
        &self,
        _key_id: KeyId,
        _now: Timestamp,
    ) -> Result<AdminReceipt<Revocation>, KeyError> {
        unreachable!("{}: KeyDirectory::revoke_key_audited", self.reason)
    }

    async fn active_keys(&self, _now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        unreachable!("{}: KeyDirectory::active_keys", self.reason)
    }

    async fn account_keys(
        &self,
        _account: AccountId,
        _after: Option<KeyId>,
        _limit: NonZeroUsize,
    ) -> Result<Vec<KeySummary>, StoreError> {
        unreachable!("{}: KeyDirectory::account_keys", self.reason)
    }

    async fn insert_key_within(
        &self,
        _record: KeyRecord,
        _max_active: NonZeroUsize,
        _now: Timestamp,
    ) -> Result<(), KeyError> {
        unreachable!("{}: KeyDirectory::insert_key_within", self.reason)
    }

    async fn insert_key_within_audited(
        &self,
        _record: KeyRecord,
        _max_active: NonZeroUsize,
        _now: Timestamp,
    ) -> Result<AdminReceipt<()>, KeyError> {
        unreachable!("{}: KeyDirectory::insert_key_within_audited", self.reason)
    }
}

#[async_trait]
impl StoreHealth for RejectingStore {
    async fn ping(&self) -> Result<(), StoreError> {
        unreachable!("{}: StoreHealth::ping", self.reason)
    }
}
