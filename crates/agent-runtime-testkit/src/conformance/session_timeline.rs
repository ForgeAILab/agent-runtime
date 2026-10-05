//! Reusable U7/U8 host fixture: authorized multi-timeline reference persistence.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agent_runtime::harness::{
    LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, LcmTimelineResolver,
};
use agent_runtime::lcm::*;
use agent_runtime::prelude::*;
use async_trait::async_trait;

/// Host-owned bindings and stores. No consumer-domain authority is inferred.
#[derive(Debug, Default)]
pub struct TimelineHost {
    stores: Mutex<BTreeMap<LcmTimelineId, Arc<InMemoryLcmStore>>>,
    bindings: Mutex<BTreeMap<SessionId, LcmTimelineBinding>>,
}

impl TimelineHost {
    /// Installs one empty reference timeline and its host-authorized binding.
    pub fn bind_new(&self, session: &SessionId, timeline: LcmTimelineId) -> Arc<InMemoryLcmStore> {
        let store = Arc::new(InMemoryLcmStore::new(timeline.clone()));
        self.stores
            .lock()
            .unwrap()
            .insert(timeline.clone(), store.clone());
        self.bindings.lock().unwrap().insert(
            session.clone(),
            LcmTimelineBinding::new(
                session.clone(),
                timeline,
                RegistryRevision::new("testkit-binding-1"),
                store.authority(),
            )
            .unwrap(),
        );
        store
    }

    /// Explicitly authorizes another session against an existing timeline.
    pub fn bind_existing(&self, session: &SessionId, timeline: &LcmTimelineId) {
        let store = self.stores.lock().unwrap()[timeline].clone();
        self.bindings.lock().unwrap().insert(
            session.clone(),
            LcmTimelineBinding::new(
                session.clone(),
                timeline.clone(),
                RegistryRevision::new("testkit-binding-1"),
                store.authority(),
            )
            .unwrap(),
        );
    }

    fn store(&self, view: &LcmView) -> Result<Arc<InMemoryLcmStore>, LcmError> {
        let store = self
            .stores
            .lock()
            .unwrap()
            .get(view.timeline_id())
            .cloned()
            .ok_or(LcmError::Unauthorized)?;
        store.authorize_view(view)?;
        Ok(store)
    }

    /// Builds the neutral coordinator, sharing U6's planner sizer.
    pub fn coordinator(self: &Arc<Self>) -> Arc<LcmCoordinator> {
        let mut policy = LcmCoordinatorPolicy {
            input_budget_tokens: 1_000,
            ..Default::default()
        };
        policy.pressure.retain_recent_entries = 0;
        Arc::new(
            LcmCoordinator::new(
                self.clone(),
                Arc::new(super::lcm::FakeLcmSummaryModel::from_texts([
                    "bounded summary",
                    "bounded summary",
                    "bounded summary",
                    "bounded summary",
                ])),
                self.clone(),
                policy,
            )
            .unwrap(),
        )
    }
}

impl LcmTimelineResolver for TimelineHost {
    fn resolve(&self, session: &SessionId) -> Result<LcmTimelineBinding, RuntimeError> {
        self.bindings
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| RuntimeError::approval("host has not authorized this session"))
    }

    fn new_timeline(
        &self,
        session: &SessionId,
        _previous: &LcmTimelineBinding,
    ) -> Result<LcmTimelineBinding, RuntimeError> {
        let timeline = LcmTimelineId::new(format!("timeline:{}", session.as_str()));
        if self.stores.lock().unwrap().contains_key(&timeline) {
            return self.resolve(session);
        }
        self.bind_new(session, timeline);
        self.resolve(session)
    }

    fn continue_timeline(
        &self,
        session: &SessionId,
        previous: &LcmTimelineBinding,
    ) -> Result<LcmTimelineBinding, RuntimeError> {
        self.bind_existing(session, &previous.timeline);
        self.resolve(session)
    }
}

#[async_trait]
impl LcmReader for TimelineHost {
    fn store_revision(&self) -> RegistryRevision {
        RegistryRevision::new("testkit-multi-timeline-1")
    }
    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.store(view).map(|_| ())
    }
    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.store(view)?.current_revision(view).await
    }
    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.store(view)?.load_range(view, range, limit).await
    }
    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.store(view)?.active_nodes(view).await
    }
    async fn node(&self, view: &LcmView, node: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.store(view)?.node(view, node).await
    }
    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.store(view)?.expand(view, request).await
    }
}

#[async_trait]
impl LcmWriter for TimelineHost {
    async fn claim(
        &self,
        view: &LcmView,
        owner: &SessionId,
        generation: u64,
    ) -> Result<LcmClaimResult, LcmError> {
        self.store(view)?.claim(view, owner, generation).await
    }
    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.store(view)?.append(view, request).await
    }
    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.store(view)?.commit_leaf(view, request).await
    }
    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.store(view)?.commit_condensation(view, request).await
    }
    async fn truncate_from(
        &self,
        view: &LcmView,
        from: LcmSequence,
    ) -> Result<TruncateResult, LcmError> {
        self.store(view)?.truncate_from(view, from).await
    }
}
