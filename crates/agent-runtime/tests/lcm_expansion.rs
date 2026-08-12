//! Runtime-owned authorization and redaction tests for bounded LCM expansion.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agent_runtime::core::content::Message;
use agent_runtime::core::event::{LcmLifecycleKind, LcmLifecycleReason, RuntimeEvent};
use agent_runtime::core::provider::ModelId;
use agent_runtime::harness::{
    ExpansionItem, ExpansionRequest, LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding,
    LcmView, LcmViewAuthority, StaticLcmTimelineResolver,
};
use agent_runtime::lcm::{
    AppendResult, CommitResult, CondensationCommit, LcmAppendRequest, LcmClassification, LcmEdge,
    LcmEntry, LcmEntryId, LcmError, LcmExpansion, LcmNode, LcmNodeId, LcmNodeKind, LcmReader,
    LcmRevision, LcmSequence, LcmSourceMetadata, LcmSummaryError, LcmSummaryModel,
    LcmSummaryModelRequest, LcmSummaryModelResponse, LcmTimelineId, LcmWriter, LeafCommit,
    Sensitivity, SummaryProvenance,
};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::registry::{Fingerprint, RegistryRevision, TrustClass};
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use async_trait::async_trait;
use futures_util::StreamExt;

const TIMELINE: &str = "expansion-test-timeline";
const AUTHORIZATION: &str = "expansion-test-binding-v1";
const STORE_REVISION: &str = "expansion-test-store-v1";

#[derive(Debug)]
struct NoopSummaryModel;

#[async_trait]
impl LcmSummaryModel for NoopSummaryModel {
    fn id(&self) -> &str {
        "expansion-test-model"
    }

    fn revision(&self) -> &RegistryRevision {
        static REVISION: std::sync::OnceLock<RegistryRevision> = std::sync::OnceLock::new();
        REVISION.get_or_init(|| RegistryRevision::new("expansion-test-model-v1"))
    }

    async fn summarize(
        &self,
        _request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        panic!("LCM expansion must not invoke the summary model")
    }
}

#[derive(Debug)]
struct ExpansionStore {
    timeline: LcmTimelineId,
    authority: LcmViewAuthority,
    entries: BTreeMap<LcmEntryId, LcmEntry>,
    node: LcmNode,
    expand_calls: Mutex<usize>,
}

impl ExpansionStore {
    fn new() -> Arc<Self> {
        let timeline = LcmTimelineId::new(TIMELINE);
        let classification = LcmClassification::new(Sensitivity::Public, TrustClass::UserContent);
        let mut entries = BTreeMap::new();
        let mut edges = Vec::new();
        for sequence in 0..3_u64 {
            let id = LcmEntryId::new(format!("expansion-entry-{sequence}"));
            let entry = LcmEntry::new(
                timeline.clone(),
                id.clone(),
                LcmSequence::new(sequence),
                Message::user(format!("entry body {sequence}")),
                LcmSourceMetadata::new(classification.clone()),
            );
            entries.insert(id.clone(), entry);
            edges.push(LcmEdge::Entry(id));
        }
        let source_fingerprint = Fingerprint::of("expansion-source");
        let provenance = SummaryProvenance::Deterministic {
            revision: RegistryRevision::new("expansion-deterministic-v1"),
        };
        let node = LcmNode {
            timeline_id: timeline.clone(),
            id: LcmNodeId::new("expansion-node"),
            kind: LcmNodeKind::Leaf,
            range: agent_runtime::lcm::LcmRange::new(LcmSequence::new(0), LcmSequence::new(2))
                .expect("valid expansion range"),
            edges,
            source_fingerprint,
            summary_revision: RegistryRevision::new("expansion-summary-v1"),
            summary: "protected summary body".into(),
            policy_revision: RegistryRevision::new("expansion-policy-v1"),
            algorithm_revision: RegistryRevision::new("expansion-algorithm-v1"),
            sizer_revision: RegistryRevision::new("expansion-sizer-v1"),
            provenance,
            token_count: 1,
            source_token_count: 3,
            classification,
            revision: LcmRevision::new(1),
            superseded_by: None,
            operation_id: agent_runtime::lcm::LcmOperationId::new("expansion-operation"),
            operation_fingerprint: agent_runtime::lcm::LcmOperationFingerprint::from(
                Fingerprint::of("expansion-operation"),
            ),
        };
        Arc::new(Self {
            timeline,
            authority: LcmViewAuthority::new(),
            entries,
            node,
            expand_calls: Mutex::new(0),
        })
    }

    fn authority(&self) -> LcmViewAuthority {
        self.authority.clone()
    }

    fn expand_calls(&self) -> usize {
        *self.expand_calls.lock().expect("expand call lock")
    }

    fn authorize(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authority.authorize(view)?;
        if view.timeline_id() != &self.timeline
            || view.authorization_revision() != Some(AUTHORIZATION)
        {
            return Err(LcmError::Unauthorized);
        }
        Ok(())
    }
}

#[async_trait]
impl LcmReader for ExpansionStore {
    fn store_revision(&self) -> RegistryRevision {
        RegistryRevision::new(STORE_REVISION)
    }

    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authorize(view)
    }

    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize(view)?;
        Ok(self.node.revision)
    }

    async fn load_range(
        &self,
        view: &LcmView,
        range: agent_runtime::lcm::LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.authorize(view)?;
        Ok(self
            .entries
            .values()
            .filter(|entry| {
                entry.sequence.get() >= range.start.get() && entry.sequence.get() <= range.end.get()
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.authorize(view)?;
        Ok(vec![self.node.clone()])
    }

    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.authorize(view)?;
        (node_id == &self.node.id)
            .then_some(self.node.clone())
            .ok_or(LcmError::MissingSource)
    }

    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.authorize(view)?;
        *self.expand_calls.lock().expect("expand call lock") += 1;
        if request.node_id != self.node.id || request.limit == 0 {
            return Err(LcmError::MissingSource);
        }
        let offset = request.cursor.map_or(0, |cursor| {
            if cursor.node_id == self.node.id
                && cursor.source_fingerprint == self.node.source_fingerprint
            {
                cursor.offset
            } else {
                usize::MAX
            }
        });
        if offset == usize::MAX || offset > self.node.edges.len() {
            return Err(LcmError::InvalidCursor);
        }
        let end = offset
            .saturating_add(request.limit)
            .min(self.node.edges.len());
        let items = self.node.edges[offset..end]
            .iter()
            .map(|edge| match edge {
                LcmEdge::Entry(id) => self
                    .entries
                    .get(id)
                    .cloned()
                    .map(ExpansionItem::Entry)
                    .ok_or(LcmError::MissingSource),
                LcmEdge::Node(_) => Err(LcmError::MissingSource),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let complete = end == self.node.edges.len();
        Ok(LcmExpansion {
            node_id: self.node.id.clone(),
            source_fingerprint: self.node.source_fingerprint.clone(),
            items,
            complete,
            next_cursor: (!complete).then_some(agent_runtime::lcm::LcmExpansionCursor {
                node_id: self.node.id.clone(),
                offset: end,
                source_fingerprint: self.node.source_fingerprint.clone(),
            }),
        })
    }
}

#[async_trait]
impl LcmWriter for ExpansionStore {
    async fn append(
        &self,
        _view: &LcmView,
        _request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        Err(LcmError::StoreFailure)
    }

    async fn commit_leaf(
        &self,
        _view: &LcmView,
        _request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        Err(LcmError::StoreFailure)
    }

    async fn commit_condensation(
        &self,
        _view: &LcmView,
        _request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        Err(LcmError::StoreFailure)
    }
}

fn runtime_for(session: &str, store: Arc<ExpansionStore>, authority: LcmViewAuthority) -> Runtime {
    let session_id = agent_runtime::core::ids::SessionId::new(session);
    let binding = LcmTimelineBinding::new(
        session_id,
        LcmTimelineId::new(TIMELINE),
        RegistryRevision::new(AUTHORIZATION),
        authority,
    )
    .expect("valid expansion binding");
    let coordinator = LcmCoordinator::new(
        store,
        Arc::new(NoopSummaryModel),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 128_000,
            ..LcmCoordinatorPolicy::default()
        },
    )
    .expect("valid expansion coordinator");
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("provider must not run")))
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(128_000, 128_000, 4_096),
            ),
        )
        .lcm(Arc::new(coordinator))
        .build()
        .expect("runtime builds")
}

async fn next_lcm_event(
    stream: &mut agent_runtime::runtime::RuntimeEventStream,
) -> agent_runtime::core::event::EventEnvelope {
    stream.next().await.expect("one LCM event")
}

#[tokio::test]
async fn authorized_expansion_is_complete_and_emits_one_event() {
    let store = ExpansionStore::new();
    let runtime = runtime_for("expansion-authorized", store.clone(), store.authority());
    let session = runtime
        .start_session(
            StartSession::new().with_id(agent_runtime::core::ids::SessionId::new(
                "expansion-authorized",
            )),
        )
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    let expansion = session
        .expand_lcm(ExpansionRequest::new(LcmNodeId::new("expansion-node"), 8))
        .await
        .expect("authorized expansion");
    assert!(expansion.complete);
    assert_eq!(expansion.items.len(), 3);
    assert_eq!(store.expand_calls(), 1);

    let event = next_lcm_event(&mut events).await;
    let RuntimeEvent::LcmLifecycle {
        kind,
        reason,
        metadata,
    } = event.payload
    else {
        panic!("expected LCM lifecycle event")
    };
    assert_eq!(kind, LcmLifecycleKind::Expansion);
    assert_eq!(reason, Some(LcmLifecycleReason::Authorized));
    assert_eq!(metadata.expanded_count, Some(3));
    assert!(metadata.dag_revision.is_none());
    assert!(metadata.timeline_id.is_some());
    assert!(metadata.node_id.is_some());
    assert!(metadata.operation_fingerprint.is_some());
}

#[tokio::test]
async fn bounded_expansion_continuation_is_redaction_safe() {
    let store = ExpansionStore::new();
    let runtime = runtime_for("expansion-bounded", store.clone(), store.authority());
    let session = runtime
        .start_session(
            StartSession::new().with_id(agent_runtime::core::ids::SessionId::new(
                "expansion-bounded",
            )),
        )
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    let first = session
        .expand_lcm(ExpansionRequest::new(LcmNodeId::new("expansion-node"), 1))
        .await
        .expect("first bounded page");
    assert!(!first.complete);
    let cursor = first.next_cursor.clone().expect("continuation cursor");
    let event = next_lcm_event(&mut events).await;
    let RuntimeEvent::LcmLifecycle {
        reason, metadata, ..
    } = event.payload
    else {
        panic!("expected expansion event")
    };
    assert_eq!(reason, Some(LcmLifecycleReason::Bounded));
    assert_eq!(metadata.expanded_count, Some(1));
    assert!(metadata.expansion_cursor.is_some());

    let second = session
        .expand_lcm(ExpansionRequest::from_cursor(cursor, 8))
        .await
        .expect("continuation page");
    assert!(second.complete);
    assert_eq!(second.items.len(), 2);
    let event = next_lcm_event(&mut events).await;
    assert!(matches!(
        event.payload,
        RuntimeEvent::LcmLifecycle {
            kind: LcmLifecycleKind::Expansion,
            reason: Some(LcmLifecycleReason::Authorized),
            ..
        }
    ));
    assert_eq!(store.expand_calls(), 2);
}

#[tokio::test]
async fn unauthorized_and_unknown_expansion_do_not_leak_existence() {
    let store = ExpansionStore::new();
    let runtime = runtime_for(
        "expansion-unauthorized",
        store.clone(),
        LcmViewAuthority::new(),
    );
    let session = runtime
        .start_session(
            StartSession::new().with_id(agent_runtime::core::ids::SessionId::new(
                "expansion-unauthorized",
            )),
        )
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    let request = ExpansionRequest::new(LcmNodeId::new("expansion-node"), 1);
    let error = session
        .expand_lcm(request.clone())
        .await
        .expect_err("denied");
    assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::Approval);
    let event = next_lcm_event(&mut events).await;
    let RuntimeEvent::LcmLifecycle {
        kind,
        reason,
        metadata,
    } = event.payload
    else {
        panic!("expected failure event")
    };
    assert_eq!(kind, LcmLifecycleKind::Failure);
    assert_eq!(reason, Some(LcmLifecycleReason::Unauthorized));
    assert!(metadata.timeline_id.is_none());
    assert!(metadata.node_id.is_none());
    assert!(metadata.source_fingerprint.is_none());
    assert!(metadata.expansion_cursor.is_none());
    assert!(metadata.operation_fingerprint.is_some());
    assert_eq!(store.expand_calls(), 0, "store must reject before lookup");

    let authorized_runtime = runtime_for("expansion-unknown", store.clone(), store.authority());
    let unknown = authorized_runtime
        .start_session(
            StartSession::new().with_id(agent_runtime::core::ids::SessionId::new(
                "expansion-unknown",
            )),
        )
        .await
        .expect("session starts");
    let mut unknown_events = unknown.subscribe();
    let error = unknown
        .expand_lcm(ExpansionRequest::new(LcmNodeId::new("does-not-exist"), 1))
        .await
        .expect_err("unknown target");
    assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::NotFound);
    let event = next_lcm_event(&mut unknown_events).await;
    let RuntimeEvent::LcmLifecycle {
        reason, metadata, ..
    } = event.payload
    else {
        panic!("expected failure event")
    };
    assert_eq!(reason, Some(LcmLifecycleReason::NotFound));
    assert!(metadata.timeline_id.is_none());
    assert!(metadata.node_id.is_none());
}

#[tokio::test]
async fn missing_coordinator_emits_typed_failure_without_mutation() {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("provider must not run")))
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(128_000, 128_000, 4_096),
            ),
        )
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let before = session.snapshot();
    let mut events = session.subscribe();
    let error = session
        .expand_lcm(ExpansionRequest::new(LcmNodeId::new("opaque-node"), 1))
        .await
        .expect_err("missing coordinator");
    assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::Config);
    let event = next_lcm_event(&mut events).await;
    assert!(matches!(
        event.payload,
        RuntimeEvent::LcmLifecycle {
            kind: LcmLifecycleKind::Failure,
            reason: Some(LcmLifecycleReason::InvalidInput),
            ..
        }
    ));
    let after = session.snapshot();
    assert_eq!(before.history, after.history);
    assert_eq!(before.usage, after.usage);
    assert_eq!(before.manifests, after.manifests);
    assert_eq!(before.extension_state, after.extension_state);
}
