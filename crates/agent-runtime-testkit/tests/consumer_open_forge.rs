//! Open Forge adapter contract suite (cross-consumer compatibility gate).

use std::sync::Arc;

use serde_json::json;

use agent_runtime::prelude::*;
use agent_runtime_testkit::conformance::{event_schema, runtime as rt};
use agent_runtime_testkit::{RecordingObserver, consumers, scenarios};

#[tokio::test]
async fn open_forge_adapter_passes_shared_conformance() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime =
        consumers::open_forge::build(provider, observer.clone()).expect("open forge runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    event_schema::assert_versioned_and_roundtrips(&observer.events());
}

use agent_runtime::harness::StaticLcmTimelineResolver;
use agent_runtime::lcm::{
    AppendResult, CommitResult, CondensationCommit, ExpansionRequest, LcmAppendRequest, LcmEntry,
    LcmError, LcmExpansion, LcmNode, LcmNodeId, LcmRange, LcmRevision, LcmTimelineId, LeafCommit,
};
use agent_runtime_lcm::testing::InMemoryLcmStore;

/// Every operation authorizes before returning even a synthetic conflict.
#[derive(Debug)]
struct ConflictingStore(InMemoryLcmStore);

#[async_trait::async_trait]
impl LcmReader for ConflictingStore {
    fn store_revision(&self) -> RegistryRevision {
        self.0.store_revision()
    }
    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.0.authorize_view(view)
    }
    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize_view(view)?;
        Err(LcmError::RevisionConflict {
            expected: LcmRevision::INITIAL,
            actual: LcmRevision::new(1),
        })
    }
    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.0.load_range(view, range, limit).await
    }
    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.0.active_nodes(view).await
    }
    async fn node(&self, view: &LcmView, id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.0.node(view, id).await
    }
    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.0.expand(view, request).await
    }
}

#[async_trait::async_trait]
impl LcmWriter for ConflictingStore {
    async fn claim(
        &self,
        view: &LcmView,
        owner: &agent_runtime_core::ids::SessionId,
        generation: u64,
    ) -> Result<agent_runtime_lcm::LcmClaimResult, LcmError> {
        let _ = (owner, generation);
        self.authorize_view(view)?;
        Ok(agent_runtime::lcm::LcmClaimResult::Claimed)
    }

    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.0.append(view, request).await
    }
    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.0.commit_leaf(view, request).await
    }
    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.0.commit_condensation(view, request).await
    }
}

#[derive(Debug)]
struct UnusedSummaryModel(RegistryRevision);

#[async_trait::async_trait]
impl LcmSummaryModel for UnusedSummaryModel {
    fn id(&self) -> &str {
        "fixture.summary"
    }
    fn revision(&self) -> &RegistryRevision {
        &self.0
    }
    async fn summarize(
        &self,
        _request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, agent_runtime::lcm::LcmSummaryError> {
        panic!("preflight conflict or denial must not invoke a summary model")
    }
}

#[tokio::test]
async fn open_forge_authorized_conflict_and_revoked_view_retain_classes_without_provider_io() {
    for revoked in [false, true] {
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let session_id = SessionId::new("fixture.session");
        let timeline = LcmTimelineId::new("fixture.timeline");
        let store = Arc::new(ConflictingStore(InMemoryLcmStore::new(timeline.clone())));
        let authority = store.0.authority();
        let binding = LcmTimelineBinding::new(
            session_id.clone(),
            timeline,
            RegistryRevision::new("binding-1"),
            authority.clone(),
        )
        .unwrap();
        let coordinator = Arc::new(
            LcmCoordinator::new(
                store.clone(),
                Arc::new(UnusedSummaryModel(RegistryRevision::new("summary-1"))),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                LcmCoordinatorPolicy {
                    input_budget_tokens: 32_000,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .lcm(coordinator)
            .observer(observer.clone())
            .retry(RetryPolicy::immediate(3))
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .unwrap();
        if revoked {
            authority.revoke();
        }
        session.run(UserInput::text("hi")).await.unwrap();
        let payloads = observer.payloads();
        let errors = payloads
            .iter()
            .filter_map(|event| match event {
                RuntimeEvent::Error { error } => Some(error),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1);
        let (kind, class) = if revoked {
            (
                ErrorKind::Approval,
                FailureClass::PolicyDenied {
                    stage: FailureStage::PreProvider,
                },
            )
        } else {
            (
                ErrorKind::Conflict,
                FailureClass::StateConflict {
                    stage: FailureStage::PreProvider,
                    component: FailureComponent::Lcm,
                },
            )
        };
        assert_eq!(errors[0].kind, kind);
        assert_eq!(errors[0].class, class);
        assert!(!errors[0].retryable);
        assert!(provider.requests().is_empty());
        assert!(
            !payloads
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptStarted { .. }))
        );
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Failed,
                ..
            })
        ));
        if revoked {
            assert_eq!(store.0.entry_count(), 0);
        }
        event_schema::assert_versioned_and_roundtrips(&observer.events());
    }
}

#[tokio::test]
async fn open_forge_recovers_exact_boundaries_with_bounded_diagnostics() {
    agent_runtime_testkit::conformance::manifests::assert_protected_manifest_recovery().await;
}

#[tokio::test]
async fn open_forge_history_lcm_isolation_gate() {
    agent_runtime_testkit::conformance::history::assert_authorized_accounting().await;
}

#[tokio::test]
async fn forge_opt_in_normalization_keeps_canonical_schema_and_denial() {
    agent_runtime_testkit::conformance::normalization::assert_opt_in_normalization_preserves_denial_and_schema().await;
}
