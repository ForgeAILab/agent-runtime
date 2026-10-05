//! Volatile LCM timelines for ephemeral sessions.
//!
//! An ephemeral session never reads or writes configured persistence, so its
//! LCM must not claim or append to a durable host timeline either. The
//! coordinator keeps one in-memory timeline per live ephemeral session and
//! routes every store call by the view's timeline and grant: a host view can
//! never reach a volatile store, and a volatile view never reaches the host.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use agent_runtime_core::ids::SessionId;
use agent_runtime_lcm::{
    AppendResult, CommitResult, CondensationCommit, ExpansionRequest, InMemoryLcmStore,
    LcmAppendRequest, LcmClaimResult, LcmEntry, LcmError, LcmExpansion, LcmNode, LcmNodeId,
    LcmRange, LcmReader, LcmRevision, LcmSequence, LcmStore, LcmTimelineId, LcmView, LcmWriter,
    LeafCommit, TruncateResult,
};
use agent_runtime_registry::RegistryRevision;

use super::LcmTimelineBinding;

pub(super) type VolatileStores = Arc<Mutex<BTreeMap<LcmTimelineId, Arc<InMemoryLcmStore>>>>;
pub(super) type VolatileBindings = Arc<Mutex<BTreeMap<SessionId, LcmTimelineBinding>>>;

/// Host store plus the volatile timelines of live ephemeral sessions.
#[derive(Debug)]
pub(super) struct RoutingStore {
    durable: Arc<dyn LcmStore>,
    volatile: VolatileStores,
}

impl RoutingStore {
    pub(super) fn new(durable: Arc<dyn LcmStore>, volatile: VolatileStores) -> Self {
        Self { durable, volatile }
    }

    fn route(&self, view: &LcmView) -> Arc<dyn LcmStore> {
        let volatile = self
            .volatile
            .lock()
            .expect("volatile LCM stores poisoned")
            .get(view.timeline_id())
            .cloned();
        match volatile {
            Some(store) if store.authorize_view(view).is_ok() => store,
            _ => self.durable.clone(),
        }
    }
}

/// Releases one ephemeral session's volatile timeline when the session ends.
pub(crate) struct EphemeralLcmGuard {
    session: SessionId,
    timeline: LcmTimelineId,
    stores: VolatileStores,
    bindings: VolatileBindings,
    claims: Arc<Mutex<BTreeMap<SessionId, (LcmTimelineId, u64)>>>,
}

impl std::fmt::Debug for EphemeralLcmGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EphemeralLcmGuard")
            .field("session", &self.session)
            .field("timeline", &self.timeline)
            .finish()
    }
}

impl Drop for EphemeralLcmGuard {
    fn drop(&mut self) {
        if let Ok(mut stores) = self.stores.lock() {
            stores.remove(&self.timeline);
        }
        if let Ok(mut bindings) = self.bindings.lock() {
            if bindings
                .get(&self.session)
                .is_some_and(|binding| binding.timeline == self.timeline)
            {
                bindings.remove(&self.session);
            }
        }
        if let Ok(mut claims) = self.claims.lock() {
            if claims
                .get(&self.session)
                .is_some_and(|(timeline, _)| *timeline == self.timeline)
            {
                claims.remove(&self.session);
            }
        }
    }
}

pub(super) fn register(
    session: &SessionId,
    stores: &VolatileStores,
    bindings: &VolatileBindings,
    claims: &Arc<Mutex<BTreeMap<SessionId, (LcmTimelineId, u64)>>>,
) -> Result<EphemeralLcmGuard, agent_runtime_core::error::RuntimeError> {
    let timeline = LcmTimelineId::new(format!("ephemeral-{}", uuid::Uuid::new_v4().simple()));
    let store = Arc::new(InMemoryLcmStore::new(timeline.clone()));
    let binding = LcmTimelineBinding::new(
        session.clone(),
        timeline.clone(),
        RegistryRevision::new("runtime-ephemeral-timeline-1"),
        store.authority(),
    )?;
    let mut registered = bindings.lock().expect("volatile LCM bindings poisoned");
    if registered.contains_key(session) {
        return Err(agent_runtime_core::error::RuntimeError::conflict(
            "ephemeral LCM timeline is already registered for this session",
        ));
    }
    stores
        .lock()
        .expect("volatile LCM stores poisoned")
        .insert(timeline.clone(), store);
    registered.insert(session.clone(), binding);
    Ok(EphemeralLcmGuard {
        session: session.clone(),
        timeline,
        stores: stores.clone(),
        bindings: bindings.clone(),
        claims: claims.clone(),
    })
}

#[async_trait]
impl LcmReader for RoutingStore {
    fn store_revision(&self) -> RegistryRevision {
        self.durable.store_revision()
    }

    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.route(view).authorize_view(view)
    }

    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.route(view).current_revision(view).await
    }

    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.route(view).load_range(view, range, limit).await
    }

    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.route(view).active_nodes(view).await
    }

    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.route(view).node(view, node_id).await
    }

    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.route(view).expand(view, request).await
    }
}

#[async_trait]
impl LcmWriter for RoutingStore {
    async fn claim(
        &self,
        view: &LcmView,
        owner: &SessionId,
        generation: u64,
    ) -> Result<LcmClaimResult, LcmError> {
        self.route(view).claim(view, owner, generation).await
    }

    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.route(view).append(view, request).await
    }

    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.route(view).commit_leaf(view, request).await
    }

    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.route(view).commit_condensation(view, request).await
    }

    async fn truncate_from(
        &self,
        view: &LcmView,
        from: LcmSequence,
    ) -> Result<TruncateResult, LcmError> {
        self.route(view).truncate_from(view, from).await
    }
}
