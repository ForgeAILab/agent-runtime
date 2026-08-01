//! End-to-end runtime conformance against the spec scenarios.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::json;

use agent_runtime::ability::descriptor::DependencyRequirement;
use agent_runtime::ability::tool_ability;
use agent_runtime::context::{CompactionPolicy, StructuralCompactor};
use agent_runtime::harness::{
    ARTIFACT_READ_PERMISSION, ArtifactOffloader, ArtifactReadTool, CAPABILITY_SEARCH_TOOL_NAME,
    ComponentDescriptor, ContextContributor, ContextPatch, ContextView, ModelInterceptor,
    ModelRequestPatch, ModelView, SemanticSummaryCoordinator, SemanticSummaryPolicy, SummaryModel,
    SummaryModelRequest, SummaryModelResponse, ToolOutputPatch, ToolOutputProcessor,
    ToolOutputView, TurnCommitHook, TurnCommitPatch, TurnCommitView,
};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::registry::Permission;
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::event::{LimitKind, RuntimeEvent, TurnFinish};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ProviderCallContext, ProviderError,
    ProviderRequest, ProviderStream, ProviderStreamEvent, ReasoningConfig,
};
use agent_runtime_core::store::{SessionIdentityState, SessionSnapshot, SessionStore};
use agent_runtime_core::usage::{CounterKind, UsageDelta, UsageSource};
use agent_runtime_testkit::conformance::{cancellation, event_schema, runtime as rt, shutdown};
use agent_runtime_testkit::{RecordingObserver, consumers, scenarios};

fn build(provider: Arc<dyn Provider>, observer: Arc<RecordingObserver>) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .observer(observer)
        .retry(RetryPolicy::immediate(3))
        .build()
        .expect("runtime builds")
}

#[derive(Debug)]
struct DeadlineRecorder {
    deadlines: Mutex<Vec<Deadline>>,
}

impl DeadlineRecorder {
    fn new() -> Self {
        Self {
            deadlines: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Provider for DeadlineRecorder {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        _request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.deadlines.lock().unwrap().push(ctx.deadline);
        Ok(Box::pin(futures_util::stream::iter(vec![
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])))
    }
}

#[derive(Debug)]
struct UnresponsiveProvider;

#[async_trait]
impl Provider for UnresponsiveProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        _request: ProviderRequest,
        _ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        Ok(Box::pin(futures_util::stream::pending()))
    }
}

#[derive(Debug, Clone, Copy)]
enum HungHarnessPhase {
    Context,
    Model,
    ToolOutput,
    TurnCommit,
}

#[derive(Debug)]
struct HungHarnessComponent {
    entered: tokio::sync::Notify,
}

#[derive(Debug)]
struct ReadyTurnCommitHook {
    calls: AtomicUsize,
}

impl ReadyTurnCommitHook {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl TurnCommitHook for ReadyTurnCommitHook {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            "test.ready-turn-commit",
            RegistryRevision::new("ready-turn-commit-1"),
        )
    }

    async fn after_commit(&self, _view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(TurnCommitPatch::default())
    }
}

impl HungHarnessComponent {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
        }
    }

    fn descriptor() -> ComponentDescriptor {
        ComponentDescriptor::new(
            "test.hung-harness-phase",
            RegistryRevision::new("hung-harness-phase-1"),
        )
    }
}

#[async_trait]
impl ContextContributor for HungHarnessComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor()
    }

    async fn contribute(&self, _view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        self.entered.notify_waiters();
        futures_util::future::pending().await
    }
}

#[async_trait]
impl ModelInterceptor for HungHarnessComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor()
    }

    async fn before_model(&self, _view: &ModelView) -> Result<ModelRequestPatch, RuntimeError> {
        self.entered.notify_waiters();
        futures_util::future::pending().await
    }
}

#[async_trait]
impl ToolOutputProcessor for HungHarnessComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor()
    }

    async fn process(
        &self,
        _view: &ToolOutputView,
        _outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        self.entered.notify_waiters();
        futures_util::future::pending().await
    }
}

#[async_trait]
impl TurnCommitHook for HungHarnessComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor()
    }

    async fn after_commit(&self, _view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        self.entered.notify_waiters();
        futures_util::future::pending().await
    }
}

#[derive(Debug)]
struct CheckpointWriteTool;

#[async_trait]
impl LegacyTool for CheckpointWriteTool {
    fn name(&self) -> &str {
        "checkpoint_write"
    }

    fn description(&self) -> &str {
        "writes one test file"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type":"object","additionalProperties":false})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::new(Vec::new()).with_write("/ws/out.txt")
    }

    async fn invoke_legacy(
        &self,
        _arguments: serde_json::Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text("wrote"))
    }
}

#[derive(Debug)]
struct ActivationReadTool;

#[async_trait]
impl LegacyTool for ActivationReadTool {
    fn name(&self) -> &str {
        "activation_read"
    }

    fn description(&self) -> &str {
        "Inspect and read project files"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type":"object","additionalProperties":false})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::read_only()
    }

    async fn invoke_legacy(
        &self,
        _arguments: serde_json::Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text("read"))
    }
}

#[derive(Debug)]
struct LargeArtifactOutputTool;

#[async_trait]
impl LegacyTool for LargeArtifactOutputTool {
    fn name(&self) -> &str {
        "large_output"
    }

    fn description(&self) -> &str {
        "Return a large result that must remain retrievable"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type":"object","additionalProperties":false})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        _arguments: serde_json::Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(format!(
            "{}MIDDLE_SENTINEL{}",
            "head-".repeat(2_000),
            "-tail".repeat(2_000)
        )))
    }
}

#[derive(Debug, Default)]
struct ScenarioArtifactStore {
    stored: Mutex<Option<(ArtifactRef, Vec<u8>)>>,
    reads: AtomicUsize,
}

#[async_trait]
impl ArtifactStore for ScenarioArtifactStore {
    async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
        let reference = ArtifactRef {
            id: ArtifactId::new("artifact-full-output")?,
            digest: ArtifactDigest::new("sha256", format!("{:064x}", write.bytes.len()))?,
            media_type: write.media_type,
            byte_length: write.bytes.len() as u64,
            sensitivity: write.sensitivity,
            retention: write.retention,
            provenance: write.provenance,
        };
        *self.stored.lock().unwrap() = Some((reference.clone(), write.bytes));
        Ok(reference)
    }

    async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        let stored = self.stored.lock().unwrap();
        let (reference, bytes) = stored.as_ref().ok_or(ArtifactError::NotFound)?;
        if read.id != reference.id {
            return Err(ArtifactError::NotFound);
        }
        if read.session != reference.provenance.session {
            return Err(ArtifactError::AccessDenied);
        }
        let start = usize::try_from(read.offset).map_err(|_| ArtifactError::InvalidRange {
            detail: "offset does not fit this platform".into(),
        })?;
        if start > bytes.len() {
            return Err(ArtifactError::InvalidRange {
                detail: "offset exceeds artifact".into(),
            });
        }
        let end = start.saturating_add(read.limit as usize).min(bytes.len());
        Ok(ArtifactChunk {
            reference: reference.clone(),
            bytes: bytes[start..end].to_vec(),
            offset: read.offset,
            next_offset: (end < bytes.len()).then_some(end as u64),
        })
    }
}

#[derive(Debug, Default)]
struct CountingSummaryModel {
    calls: AtomicUsize,
}

#[async_trait]
impl SummaryModel for CountingSummaryModel {
    fn id(&self) -> &str {
        "counting-summary"
    }

    fn revision(&self) -> RegistryRevision {
        RegistryRevision::new("counting-summary-v1")
    }

    async fn summarize(
        &self,
        _request: &SummaryModelRequest,
    ) -> Result<SummaryModelResponse, RuntimeError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(SummaryModelResponse {
            text: "Earlier work established the durable recovery constraints.".into(),
            usage: UsageDelta::new()
                .with(CounterKind::InputUncached, 11)
                .with(CounterKind::Output, 5),
        })
    }
}

#[derive(Debug)]
struct CountingSemanticSummaryHook {
    inner: Arc<SemanticSummaryCoordinator>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TurnCommitHook for CountingSemanticSummaryHook {
    fn descriptor(&self) -> ComponentDescriptor {
        TurnCommitHook::descriptor(self.inner.as_ref())
    }

    async fn after_commit(&self, view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.inner.after_commit(view).await
    }
}

#[derive(Debug)]
struct ArtifactAllowCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
}

#[async_trait]
impl SecurityCheck for ArtifactAllowCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }

    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }

    fn declared_coverage(&self) -> Option<PermissionSet> {
        Some(PermissionSet::single(Permission::other(
            ARTIFACT_READ_PERMISSION,
        )))
    }

    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        SecurityCheckOutcome::Allow {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

#[derive(Debug, Default)]
struct OriginRecordingApproval {
    origins: Mutex<Vec<ApprovalOrigin>>,
}

#[async_trait]
impl ApprovalPolicy for OriginRecordingApproval {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        self.origins.lock().unwrap().push(request.origin().clone());
        ApprovalDecision::Allow
    }
}

#[derive(Debug, Default)]
struct BlockingApproval {
    entered: tokio::sync::Notify,
}

#[async_trait]
impl ApprovalPolicy for BlockingApproval {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        self.entered.notify_waiters();
        std::future::pending::<ApprovalDecision>().await
    }
}

#[derive(Debug, Clone, Copy)]
enum FailingCheckpointBoundary {
    Accepted,
    Planning,
    CallingModel,
    ModelResponseReady,
    AwaitingApproval,
    ExecutingEmpty,
    ExecutingCompleted,
    ToolOutcomeReady,
    Completing,
    PublishingTerminal,
    Terminal,
}

impl FailingCheckpointBoundary {
    fn matches(self, state: &TurnState) -> bool {
        match (self, state) {
            (Self::Accepted, TurnState::Accepted { .. })
            | (Self::Planning, TurnState::Planning { .. })
            | (Self::CallingModel, TurnState::CallingModel { .. })
            | (Self::ModelResponseReady, TurnState::ModelResponseReady { .. })
            | (Self::AwaitingApproval, TurnState::AwaitingApproval { .. })
            | (Self::ToolOutcomeReady, TurnState::ToolOutcomeReady { .. })
            | (Self::Completing, TurnState::Completing { .. })
            | (Self::PublishingTerminal, TurnState::PublishingTerminal { .. })
            | (Self::Terminal, TurnState::Terminal { .. }) => true,
            (Self::ExecutingEmpty, TurnState::ExecutingTools { completed, .. }) => {
                completed.is_empty()
            }
            (Self::ExecutingCompleted, TurnState::ExecutingTools { completed, .. }) => {
                !completed.is_empty()
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
struct FailOnceCheckpointStore {
    inner: agent_runtime_testkit::InMemoryCheckpointStore,
    boundary: FailingCheckpointBoundary,
    failed: AtomicBool,
}

impl FailOnceCheckpointStore {
    fn new(boundary: FailingCheckpointBoundary) -> Self {
        Self {
            inner: agent_runtime_testkit::InMemoryCheckpointStore::new(),
            boundary,
            failed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl CheckpointStore for FailOnceCheckpointStore {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(session).await
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.boundary.matches(&checkpoint.state) && !self.failed.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::internal(format!(
                "injected checkpoint failure at {:?}",
                self.boundary
            )));
        }
        self.inner.save(checkpoint).await
    }
}

#[derive(Debug, Default)]
struct FailSessionStore {
    failed: AtomicBool,
}

#[async_trait]
impl SessionStore for FailSessionStore {
    async fn load(&self, _id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.failed.store(true, Ordering::Release);
        Err(RuntimeError::internal("injected session store failure"))
    }
}

#[derive(Debug)]
struct CountingPureTool {
    name: &'static str,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl LegacyTool for CountingPureTool {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        "counts invocations"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type":"object"})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        _arguments: serde_json::Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        Ok(ToolOutcome::text(format!("{} complete", self.name)))
    }
}

#[derive(Debug)]
struct ExactPreparedWriteTool {
    prepares: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for ExactPreparedWriteTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "exact_prepared_write",
            "writes an exact prepared target",
            json!({
                "type":"object",
                "properties":{"path":{"type":"string"}},
                "required":["path"],
                "additionalProperties":false
            }),
            ToolEffects::default().with_write("/ws"),
        )
    }

    async fn prepare(
        &self,
        arguments: serde_json::Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.prepares.fetch_add(1, Ordering::AcqRel);
        Ok(PreparedToolCall::from_static_effects(
            ctx.call_id.clone(),
            &self.spec(),
            arguments,
            ctx.workspace.root(),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        Ok(ToolOutcome::json(json!({
            "path": prepared.arguments()["path"]
        })))
    }
}

fn tool_batch_provider(
    calls: &[(&str, &str, serde_json::Value)],
    final_text: &str,
) -> FakeProvider {
    let mut tool_events = Vec::new();
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        tool_events.extend(tool_call_fragments(
            index as u32,
            id,
            name,
            &arguments.to_string(),
        ));
    }
    tool_events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(tool_events),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: final_text.to_owned(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    )
}

fn continuation_provider(text: &str) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: text.to_owned(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ))
}

fn questionnaire_arguments(id: &str, sensitivity: &str) -> serde_json::Value {
    json!({
        "questions": [{
            "id": id,
            "header": "Implementation",
            "prompt": "Which implementation should be used?",
            "choices": [
                {
                    "id": format!("{id}-recommended"),
                    "label": "Recommended",
                    "description": "Use the recommended implementation"
                },
                {
                    "id": format!("{id}-alternate"),
                    "label": "Alternate"
                }
            ],
            "allow_free_form": true
        }],
        "sensitivity": sensitivity
    })
}

#[derive(Debug, Default)]
struct AnsweringInteractionBroker {
    requests: Mutex<Vec<InteractionRequest>>,
    closed: Mutex<Vec<(InteractionRequestId, InteractionOutcomeKind)>>,
}

#[async_trait]
impl InteractionBroker for AnsweringInteractionBroker {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }

    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse {
        self.requests.lock().unwrap().push(request.clone());
        let answers = request
            .questionnaire_payload()
            .questions()
            .iter()
            .map(|question| {
                if let Some(choice) = question.choices().first() {
                    QuestionAnswer::choice(question.id().clone(), choice.id().clone())
                } else {
                    QuestionAnswer::free_form(question.id().clone(), "sensitive broker answer")
                }
            })
            .collect();
        InteractionResponse::answered(request.id().clone(), answers)
    }

    fn close(&self, request_id: &InteractionRequestId, outcome: InteractionOutcomeKind) {
        self.closed
            .lock()
            .unwrap()
            .push((request_id.clone(), outcome));
    }
}

#[derive(Debug, Default)]
struct HangingInteractionBroker {
    requests: Mutex<Vec<InteractionRequest>>,
    closed: Mutex<Vec<(InteractionRequestId, InteractionOutcomeKind)>>,
}

#[derive(Debug)]
struct AuthorityBearingInteractionTool {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for AuthorityBearingInteractionTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "authority_bearing_question",
            "adversarial interaction fixture",
            json!({"type":"object","additionalProperties":false}),
            ToolEffects::default().with_write("/ws"),
        )
    }

    fn supports_interaction(&self) -> bool {
        true
    }

    fn interaction_request(
        &self,
        _prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        Ok(Some(InteractionRequest::questionnaire(
            InteractionRequestId::new(format!("adversarial-{}", origin.turn().as_str())),
            origin,
            Questionnaire::new(vec![
                Question::new(QuestionId::new("grant"), "Grant", "Widen authority?")
                    .with_choices(vec![Choice::new(ChoiceId::new("allow"), "Allow")]),
            ])?,
            deadline,
            InteractionSensitivity::Sensitive,
        )?))
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        Ok(ToolOutcome::text("must not run"))
    }
}

#[async_trait]
impl InteractionBroker for HangingInteractionBroker {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }

    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse {
        self.requests.lock().unwrap().push(request.clone());
        std::future::pending().await
    }

    fn close(&self, request_id: &InteractionRequestId, outcome: InteractionOutcomeKind) {
        self.closed
            .lock()
            .unwrap()
            .push((request_id.clone(), outcome));
    }
}

async fn wait_for_terminal(observer: &RecordingObserver) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if observer
                .payloads()
                .iter()
                .any(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resumed turn reaches a terminal event");
}

fn reconciled_payloads(
    before_crash: &[agent_runtime_core::event::EventEnvelope],
    checkpoint: &TurnCheckpoint,
    recovery: &[agent_runtime_core::event::EventEnvelope],
) -> Vec<RuntimeEvent> {
    let truncation = checkpoint
        .journal_truncation_sequence()
        .expect("fixture resumes a non-terminal checkpoint");
    before_crash
        .iter()
        .filter(|event| event.seq < truncation)
        .chain(recovery)
        .map(|event| event.payload.clone())
        .collect()
}

// agent-execution: "Provider requests a tool" — the runtime records the request
// and canonical tool result, then continues the same turn.
#[tokio::test]
async fn provider_tool_call_records_result_and_continues() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = build(provider, observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert_eq!(rt::count_tool_requests(&payloads), 1);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Completed
        })
    ));

    // History contains the canonical tool result and the final assistant text.
    let history = session.history();
    assert!(history.iter().any(|m| {
        m.role == Role::Tool
            && m.content
                .iter()
                .any(|p| matches!(p, ContentPart::ToolResult(_)))
    }));
    assert!(
        history
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("done"))
    );
}

#[tokio::test]
async fn pending_approval_is_checkpointed_before_the_host_decides() {
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let approval = Arc::new(OriginRecordingApproval::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_tool_then_text(
            "checkpoint_write",
            &json!({}),
            "done",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(approval.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("approval-checkpoint");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let turn = session.run(UserInput::text("write")).await.unwrap();

    let history = checkpoints.history(&id);
    let awaiting_index = history
        .iter()
        .position(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingApproval { .. }))
        .expect("awaiting approval was durably recorded");
    let executing_index = history
        .iter()
        .position(|checkpoint| matches!(checkpoint.state, TurnState::ExecutingTools { .. }))
        .expect("execution boundary was durably recorded");
    assert!(awaiting_index < executing_index);
    let TurnState::AwaitingApproval {
        request_id,
        source_calls,
        slots,
        step,
    } = &history[awaiting_index].state
    else {
        unreachable!()
    };
    assert_eq!(request_id.as_str(), "req-1");
    assert_eq!(*step, 0);
    assert_eq!(source_calls.len(), 1);
    assert_eq!(source_calls[0].id, *slots[0].call_id());
    assert_eq!(slots.len(), 1);
    let ToolSlotCheckpoint::Prepared(prepared) = &slots[0] else {
        panic!("approval slot must retain the exact prepared action");
    };
    assert!(prepared.verify_fingerprint());

    let origins = approval.origins.lock().unwrap();
    assert_eq!(origins.len(), 1);
    assert_eq!(origins[0].session(), &id);
    assert_eq!(origins[0].request(), request_id);
    assert_eq!(origins[0].turn(), Some(turn.id()));
}

#[tokio::test]
async fn tool_loop_preserves_exact_provider_message_order() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"path": "src/lib.rs"}),
        "done",
    ));
    let runtime = build(provider.clone(), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("inspect the file"))
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(
        requests.len(),
        2,
        "one tool continuation requires two requests"
    );
    let continuation = &requests[1].messages;
    assert_eq!(
        continuation
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [Role::User, Role::Assistant, Role::Tool],
        "classification must never reorder the canonical conversation"
    );
    assert_eq!(continuation[0].joined_text(), "inspect the file");

    let calls = continuation[1].tool_calls().collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, ToolCallId::new("call-fixture-1"));
    assert_eq!(calls[0].name, "echo");
    assert!(matches!(
        continuation[2].content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-fixture-1")
                && result.name == "echo"
    ));
}

// agent-execution: "Streaming text reaches a terminal host" — ordered text
// events arrive before turn completion.
#[tokio::test]
async fn streaming_text_precedes_completion() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hello world")), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("hi")).await;

    let text_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TextDelta { .. }))
        .expect("a text delta");
    let done_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TurnCompleted { .. }))
        .expect("completion");
    assert!(text_idx < done_idx, "text must precede completion");
}

// provider-runtime: "Second attempt succeeds" — both attempts remain visible to
// usage and event consumers.
#[tokio::test]
async fn retries_keep_both_attempts_visible() {
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(scenarios::fake_retry_then_text("ok")),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    let attempts = payloads
        .iter()
        .filter(|e| matches!(e, RuntimeEvent::ProviderAttemptStarted { .. }))
        .count();
    assert_eq!(attempts, 2, "two attempts must be visible");
    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::ProviderAttemptFinished {
            retryable: true,
            ..
        }
    )));

    // Both attempts recorded usage in the ledger; neither is hidden.
    let usage_records = session.snapshot().usage.records().len();
    assert_eq!(usage_records, 2);
    assert!(
        session
            .history()
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("ok"))
    );
}

#[tokio::test]
async fn retryable_partial_stream_is_discarded_from_transcript() {
    let first = ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "failed-attempt-text".into(),
        },
        agent_runtime::provider::fake::usage_event(4, 2),
        ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "temporary failure",
            )
            .retryable(),
        },
    ]);
    let second = ScriptedStream::new(vec![
        ProviderStreamEvent::ReasoningDelta {
            text: "successful reasoning-only answer".into(),
            redacted: false,
        },
        agent_runtime::provider::fake::usage_event(4, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]);
    let observer = RecordingObserver::shared();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![first, second],
    ));
    let runtime = build(provider, observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    let attempts = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ProviderAttemptStarted { attempt, .. } => Some(attempt.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(attempts.len(), 2);
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputDiscarded { attempt, .. }
            if attempt == &attempts[0]
    )));
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputCommitted { attempt, .. }
            if attempt == &attempts[1]
    )));
    assert!(!payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputCommitted { attempt, .. }
            if attempt == &attempts[0]
    )));
    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "failed-attempt-text"),
        "discarded speculative text must not enter canonical history"
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            visible_output: false,
        })
    ));
}

#[tokio::test]
async fn provider_finish_reasons_reach_attempt_and_turn_terminals() {
    for (reason, expected_turn) in [
        (
            FinishReason::Length,
            TurnFinish::LimitReached {
                limit: LimitKind::Output,
            },
        ),
        (FinishReason::ContentFilter, TurnFinish::Failed),
    ] {
        let observer = RecordingObserver::shared();
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "partial".into(),
                },
                ProviderStreamEvent::Finish { reason },
            ])],
        );
        let runtime = build(Arc::new(provider), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let payloads = observer.payloads();

        assert!(payloads.iter().any(|event| matches!(
            event,
            RuntimeEvent::ProviderAttemptFinished { finish, .. } if *finish == reason
        )));
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted { finish, .. }) if finish == &expected_turn
        ));

        match reason {
            FinishReason::Length => {
                assert!(payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputDiscarded { .. }
                )));
                assert!(!payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputCommitted { .. }
                )));
                assert!(
                    session
                        .history()
                        .iter()
                        .all(|message| message.joined_text() != "partial")
                );
                assert!(matches!(
                    payloads.last(),
                    Some(RuntimeEvent::TurnCompleted {
                        visible_output: false,
                        ..
                    })
                ));
            }
            FinishReason::ContentFilter => {
                assert!(payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputDiscarded { .. }
                )));
                assert!(!payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputCommitted { .. }
                )));
                assert!(
                    session
                        .history()
                        .iter()
                        .all(|message| message.joined_text() != "partial")
                );
                assert!(matches!(
                    payloads.last(),
                    Some(RuntimeEvent::TurnCompleted {
                        visible_output: false,
                        ..
                    })
                ));
            }
            _ => unreachable!("the fixture covers length and content filtering"),
        }
    }
}

#[tokio::test]
async fn length_with_tool_calls_does_not_poison_canonical_history() {
    let observer = RecordingObserver::shared();
    let mut truncated = tool_call_fragments(0, "truncated-call", "echo", r#"{"x":1}"#);
    truncated.insert(
        0,
        ProviderStreamEvent::TextDelta {
            text: "safe partial text".into(),
        },
    );
    truncated.push(ProviderStreamEvent::Finish {
        reason: FinishReason::Length,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(truncated),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "later turn completed".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = build(provider.clone(), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    session.run(UserInput::text("first")).await.unwrap();
    let after_truncation = session.history();
    assert!(
        after_truncation
            .iter()
            .all(|message| message.joined_text() != "safe partial text"),
        "output-limit text must remain speculative"
    );
    assert!(
        after_truncation
            .iter()
            .flat_map(|message| message.tool_calls())
            .all(|call| call.id.as_str() != "truncated-call"),
        "an incomplete tool call must not enter canonical assistant history"
    );
    assert!(
        after_truncation
            .iter()
            .all(|message| message.role != Role::Tool),
        "an output-limit response must not execute incomplete tool calls"
    );

    session.run(UserInput::text("second")).await.unwrap();
    assert!(session.history().iter().any(|message| {
        message.role == Role::Assistant && message.joined_text() == "later turn completed"
    }));
    assert_eq!(
        provider.requests().len(),
        2,
        "the later turn must reach the provider without pairing poison"
    );
}

#[tokio::test]
async fn error_and_cancel_finish_reasons_discard_speculative_output() {
    for (reason, expected_turn) in [
        (FinishReason::Error, TurnFinish::Failed),
        (
            FinishReason::Cancelled,
            TurnFinish::Cancelled {
                reason: CancelReason::UserRequested,
            },
        ),
    ] {
        let observer = RecordingObserver::shared();
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "must-not-commit".into(),
                },
                ProviderStreamEvent::Finish { reason },
            ])],
        );
        let runtime = build(Arc::new(provider), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let payloads = observer.payloads();

        assert!(
            payloads
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputDiscarded { .. }))
        );
        assert!(
            !payloads
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
        );
        assert!(
            session
                .history()
                .iter()
                .all(|message| message.joined_text() != "must-not-commit")
        );
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish,
                visible_output: false,
            }) if finish == &expected_turn
        ));
    }
}

#[tokio::test]
async fn malformed_assembled_call_marks_attempt_usage_failed() {
    let mut events = tool_call_fragments(0, "call-bad", "echo", "{bad");
    events.push(agent_runtime::provider::fake::usage_event(4, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(events)],
    ));
    let runtime = build(provider, observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let snapshot = session.snapshot();
    assert_eq!(snapshot.usage.records().len(), 1);
    assert!(snapshot.usage.records()[0].provenance.failed);
}

#[tokio::test]
async fn exhausted_provider_attempts_emit_structured_limit() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "retryable",
            )
            .retryable(),
        }])],
    );
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .observer(observer.clone())
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ProviderAttempts
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ProviderAttempts
            }
        })
    ));
}

#[tokio::test]
async fn retry_backoff_stops_promptly_on_cancellation() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::RateLimited,
                "slow retry",
            )
            .retry_after(5_000),
        }])],
    );
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .retry(RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 10_000,
        })
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("hi")).unwrap();

    while let Some(event) = stream.next().await {
        if matches!(
            event.payload,
            RuntimeEvent::ProviderAttemptFinished {
                retryable: true,
                ..
            }
        ) {
            session
                .interrupt_current_turn(CancelReason::UserRequested)
                .expect("the retrying turn is active");
            break;
        }
    }
    let terminal = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(event) = stream.next().await {
            if let RuntimeEvent::TurnCompleted { finish, .. } = event.payload {
                return finish;
            }
        }
        panic!("event stream ended before turn completion");
    })
    .await
    .expect("cancellation must interrupt retry backoff");
    assert!(matches!(terminal, TurnFinish::Cancelled { .. }));
}

#[tokio::test]
async fn attempt_deadline_is_capped_by_turn_deadline() {
    let provider = Arc::new(DeadlineRecorder::new());
    let clock = agent_runtime_testkit::ManualClock::shared(0);
    let mut config = LoopConfig::new(ModelId::new("fake"));
    config.turn_time_limit_ms = Some(10);
    config.attempt_time_limit_ms = Some(100);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .clock(clock)
        .loop_config(config)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    assert_eq!(
        provider.deadlines.lock().unwrap()[0].instant(),
        Some(Timestamp(10))
    );
}

// runtime-api: "Host cancels an active turn".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_terminates_turn() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    cancellation::assert_cancel_terminates(&session).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupting_one_turn_allows_a_later_turn_to_complete() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::blocking(vec![ProviderStreamEvent::TextDelta {
                text: "speculative-working".into(),
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "later-answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let commit_hook = Arc::new(ReadyTurnCommitHook::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .observer(observer.clone())
        .retry(RetryPolicy::immediate(3))
        .turn_commit_hook(commit_hook.clone())
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let interrupted = session.send(UserInput::text("first")).unwrap();

    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::TextDelta { .. }) {
            break;
        }
    }
    interrupted.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_millis(200), interrupted.completed())
        .await
        .expect("the interrupted turn must terminate");

    let later = session.run(UserInput::text("second")).await.unwrap();
    assert_ne!(interrupted.id(), later.id());
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "later-answer")
    );
    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "speculative-working")
    );

    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Cancelled { .. },
            ..
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
    assert_eq!(
        commit_hook.calls.load(Ordering::Acquire),
        2,
        "the ready terminal hook must observe both cancellation and completion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_async_harness_phase_observes_turn_interruption() {
    for phase in [
        HungHarnessPhase::Context,
        HungHarnessPhase::Model,
        HungHarnessPhase::ToolOutput,
        HungHarnessPhase::TurnCommit,
    ] {
        let component = Arc::new(HungHarnessComponent::new());
        let provider: Arc<dyn Provider> = if matches!(phase, HungHarnessPhase::ToolOutput) {
            let mut events = tool_call_fragments(0, "call-hung-hook", "echo", "{}");
            events.push(agent_runtime::provider::fake::usage_event(3, 1));
            events.push(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            });
            Arc::new(FakeProvider::new(
                "fake",
                Capabilities::basic_streaming(),
                vec![ScriptedStream::new(events)],
            ))
        } else {
            Arc::new(scenarios::fake_text("done"))
        };
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(provider);
        builder = match phase {
            HungHarnessPhase::Context => builder.context_contributor(component.clone()),
            HungHarnessPhase::Model => builder.model_interceptor(component.clone()),
            HungHarnessPhase::ToolOutput => builder
                .approval(Arc::new(AllowAll))
                .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
                .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
                .tool_output_processor(component.clone()),
            HungHarnessPhase::TurnCommit => builder.turn_commit_hook(component.clone()),
        };
        let runtime = builder.build().expect("phase fixture builds");
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let entered = component.entered.notified();
        let turn = session.send(UserInput::text("exercise hook")).unwrap();
        tokio::time::timeout(Duration::from_millis(500), entered)
            .await
            .unwrap_or_else(|_| panic!("{phase:?} hook did not start"));

        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(Duration::from_millis(200), turn.completed())
            .await
            .unwrap_or_else(|_| panic!("{phase:?} hook ignored turn cancellation"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_harness_phase_is_bounded_by_the_turn_deadline() {
    let component = Arc::new(HungHarnessComponent::new());
    let observer = RecordingObserver::shared();
    let mut config = LoopConfig::new(ModelId::new("fake"));
    config.turn_time_limit_ms = Some(20);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("must not be called")))
        .loop_config(config)
        .context_contributor(component)
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    tokio::time::timeout(
        Duration::from_millis(250),
        session.run(UserInput::text("deadline")),
    )
    .await
    .expect("turn deadline bounds a pending harness future")
    .unwrap();
    assert!(observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached {
                limit: LimitKind::Time
            },
            ..
        }
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_interrupted_turn_does_not_contaminate_history() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::blocking(vec![]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "after-queue".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = build(provider, RecordingObserver::shared());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let serving = session.send(UserInput::text("serving")).unwrap();
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    let queued = session
        .send(UserInput::text("must-never-enter-history"))
        .unwrap();
    queued.interrupt(CancelReason::UserRequested);
    serving.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_millis(200), async {
        serving.completed().await;
        queued.completed().await;
    })
    .await
    .expect("both serving and queued turns must terminate");

    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "must-never-enter-history")
    );
    session
        .run(UserInput::text("third"))
        .await
        .expect("the session remains usable after turn-local interrupts");
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "after-queue")
    );
}

#[tokio::test]
async fn terminal_session_cancellation_and_shutdown_reject_future_submissions() {
    let runtime = build(
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    let cancelled = runtime.start_session(StartSession::new()).await.unwrap();
    let before_cancel = cancelled.snapshot().identity.turn;
    cancelled.cancel_session(CancelReason::UserRequested);
    assert!(cancelled.send(UserInput::text("rejected")).is_err());
    assert_eq!(
        cancelled.snapshot().identity.turn,
        before_cancel,
        "a rejected submission must not mint an orphan turn id"
    );

    let shutdown = runtime.start_session(StartSession::new()).await.unwrap();
    let before_shutdown = shutdown.snapshot().identity.turn;
    shutdown.shutdown().await.unwrap();
    assert!(shutdown.send(UserInput::text("also rejected")).is_err());
    assert_eq!(shutdown.snapshot().identity.turn, before_shutdown);
}

// runtime-api: "Explicit lifecycle control" — bounded shutdown emits a terminal
// event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_shutdown_emits_terminal() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    shutdown::assert_bounded_shutdown(&session).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_run_participates_in_shutdown_drain() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let runner = {
        let session = session.clone();
        tokio::spawn(async move { session.run(UserInput::text("go")).await })
    };
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    session.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), runner)
        .await
        .expect("inline run drains during shutdown")
        .unwrap()
        .unwrap();
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shutdown_deadline_bounds_all_active_turns() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(UnresponsiveProvider))
        .observer(observer.clone())
        .shutdown_timeout_ms(30)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    for input in ["one", "two", "three"] {
        session.send(UserInput::text(input)).unwrap();
    }
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    let started = Instant::now();
    session.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "shutdown applied its timeout once, not once per turn"
    );
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test]
async fn concurrent_sends_are_serialized_in_submission_order() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "one".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "two".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("first")).unwrap();
    session.send(UserInput::text("second")).unwrap();
    let mut completed = 0;
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::TurnCompleted { .. }) {
            completed += 1;
            if completed == 2 {
                break;
            }
        }
    }

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages.last().unwrap().joined_text(), "first");
    assert!(
        requests[0]
            .messages
            .iter()
            .all(|message| message.joined_text() != "second")
    );
    assert_eq!(requests[1].messages.last().unwrap().joined_text(), "second");
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| message.joined_text() == "one")
    );

    let history: Vec<String> = session
        .history()
        .iter()
        .map(|message| message.joined_text())
        .collect();
    assert_eq!(history, ["first", "one", "second", "two"]);
}

#[tokio::test]
async fn two_sessions_from_one_runtime_keep_requests_events_and_manifests_isolated() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        ["a-one", "b-one", "a-two"]
            .into_iter()
            .map(|text| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta { text: text.into() },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect(),
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session_a = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-a")))
        .await
        .unwrap();
    let session_b = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-b")))
        .await
        .unwrap();

    session_a.run(UserInput::text("a-input-one")).await.unwrap();
    session_b.run(UserInput::text("b-input-one")).await.unwrap();
    session_a.run(UserInput::text("a-input-two")).await.unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|message| message.joined_text() == "a-input-one")
    );
    assert!(
        requests[1]
            .messages
            .iter()
            .all(|message| !message.joined_text().starts_with("a-"))
    );
    assert!(
        requests[2]
            .messages
            .iter()
            .any(|message| message.joined_text() == "a-one")
    );
    assert!(
        requests[2]
            .messages
            .iter()
            .all(|message| !message.joined_text().starts_with("b-"))
    );

    assert_eq!(session_a.snapshot().manifests.len(), 2);
    assert_eq!(session_b.snapshot().manifests.len(), 1);
    assert!(
        session_a
            .history()
            .iter()
            .all(|message| !message.joined_text().starts_with("b-"))
    );
    assert!(
        session_b
            .history()
            .iter()
            .all(|message| !message.joined_text().starts_with("a-"))
    );

    let events = observer.events();
    let a_cache_events = events
        .iter()
        .filter(|event| {
            event.session == *session_a.id()
                && matches!(event.payload, RuntimeEvent::CachePlanChanged { .. })
        })
        .count();
    let b_cache_events = events
        .iter()
        .filter(|event| {
            event.session == *session_b.id()
                && matches!(event.payload, RuntimeEvent::CachePlanChanged { .. })
        })
        .count();
    assert_eq!(a_cache_events, 2);
    assert_eq!(b_cache_events, 1);
}

#[tokio::test]
async fn live_initial_activation_uses_the_smallest_authorized_intent_bundle() {
    let read_tool: Arc<dyn Tool> = Arc::new(ActivationReadTool);
    let edit_tool: Arc<dyn Tool> = Arc::new(CheckpointWriteTool);
    let read_id = RegistryId::tool("activation_read");
    let read_descriptor = tool_ability(read_tool.clone())
        .descriptor()
        .with_keywords(["inspect", "read"]);
    let edit_descriptor = tool_ability(edit_tool.clone())
        .descriptor()
        .with_keywords(["edit", "modify", "write"])
        .with_dependency(DependencyRequirement::single(read_id.clone()));
    let mut read_call = tool_call_fragments(0, "call-activation-read", "activation_read", "{}");
    read_call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut edit_call = tool_call_fragments(0, "call-checkpoint-write", "checkpoint_write", "{}");
    edit_call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(read_call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "read answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(edit_call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "edit answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(read_tool)
        .tool(edit_tool)
        .tool_ability_descriptor(read_descriptor)
        .tool_ability_descriptor(edit_descriptor)
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .live_ability_routing()
        .observer(observer.clone())
        .build()
        .unwrap();

    let read_session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("live-activation-read")))
        .await
        .unwrap();
    let read_bootstrap = read_session
        .activation_epoch()
        .expect("live routing exposes its frozen bootstrap epoch");
    assert_eq!(read_bootstrap.index(), 0);
    assert!(read_bootstrap.contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)));
    read_session
        .run(UserInput::text("inspect the project sources"))
        .await
        .unwrap();
    let read_selected = read_session
        .activation_epoch()
        .expect("intent selection advances the readable epoch");
    assert_eq!(read_selected.index(), 1);
    assert!(read_selected.contains(&RegistryId::tool("activation_read")));
    let edit_session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("live-activation-edit")))
        .await
        .unwrap();
    let edit_bootstrap = edit_session
        .activation_epoch()
        .expect("each session owns an independently readable bootstrap epoch");
    assert_eq!(edit_bootstrap.index(), 0);
    assert!(edit_bootstrap.contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)));
    edit_session
        .run(UserInput::text("modify the project sources"))
        .await
        .unwrap();
    let edit_selected = edit_session
        .activation_epoch()
        .expect("editing intent advances the readable epoch");
    assert_eq!(edit_selected.index(), 1);
    assert!(edit_selected.contains(&RegistryId::tool("activation_read")));
    assert!(edit_selected.contains(&RegistryId::tool("checkpoint_write")));

    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let names = |request: &ProviderRequest| {
        request
            .tools
            .iter()
            .map(|schema| schema.name.clone())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        names(&requests[0]),
        BTreeSet::from([
            CAPABILITY_SEARCH_TOOL_NAME.to_owned(),
            "activation_read".to_owned(),
        ]),
        "read-only intent advertises no write authority"
    );
    assert_eq!(
        names(&requests[2]),
        BTreeSet::from([
            CAPABILITY_SEARCH_TOOL_NAME.to_owned(),
            "activation_read".to_owned(),
            "checkpoint_write".to_owned(),
        ]),
        "editing intent activates the editor plus its declared read dependency"
    );
    assert!(requests[1].messages.iter().any(|message| matches!(
        message.content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-activation-read")
                && result.name == "activation_read"
    )));
    assert!(requests[3].messages.iter().any(|message| matches!(
        message.content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-checkpoint-write")
                && result.name == "checkpoint_write"
    )));
    assert!(
        read_session
            .history()
            .iter()
            .any(|message| message.joined_text() == "read answer")
    );
    assert!(
        edit_session
            .history()
            .iter()
            .any(|message| message.joined_text() == "edit answer")
    );

    let events = observer.events();
    for session in [read_session.id(), edit_session.id()] {
        let lifecycle = events
            .iter()
            .filter(|event| &event.session == session)
            .collect::<Vec<_>>();
        let position = |matches: fn(&RuntimeEvent) -> bool| {
            lifecycle
                .iter()
                .position(|event| matches(&event.payload))
                .expect("declared live lifecycle event is emitted")
        };
        let registry =
            position(|event| matches!(event, RuntimeEvent::RegistrySnapshotSealed { .. }));
        let view = position(|event| matches!(event, RuntimeEvent::ScopedViewDerived { .. }));
        let retrieval =
            position(|event| matches!(event, RuntimeEvent::CapabilityRetrievalPerformed { .. }));
        let planned = position(|event| matches!(event, RuntimeEvent::ContextPlanned { .. }));
        let cache = position(|event| matches!(event, RuntimeEvent::CachePlanChanged { .. }));
        assert!(registry < view);
        assert!(view < retrieval);
        assert!(retrieval < planned);
        assert!(planned < cache);
        assert!(
            lifecycle[retrieval + 1..planned]
                .iter()
                .any(|event| matches!(event.payload, RuntimeEvent::CapabilitiesActivated { .. })),
            "authorized activation epoch is emitted before planning"
        );
        assert_eq!(
            lifecycle
                .iter()
                .filter(|event| matches!(event.payload, RuntimeEvent::CapabilitiesActivated { .. }))
                .count(),
            2,
            "bootstrap and intent-selected epochs are both observable"
        );
    }
}

#[tokio::test]
async fn live_context_compaction_emits_its_current_plan_outcome() {
    let observer = RecordingObserver::shared();
    let profile = ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(1_000, 1_000, 128),
    );
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(profile)
        .provider(Arc::new(scenarios::fake_text("compacted")))
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("live-event-context-1"),
            128,
            0,
        ))
        .compactor(StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("live-event-compaction-1"),
            100,
            10,
        )))
        .live_ability_routing()
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_history(vec![
            Message::user("old question"),
            Message::text(Role::Assistant, "x".repeat(8_000)),
        ]))
        .await
        .unwrap();
    session
        .run(UserInput::text("answer with compact history"))
        .await
        .unwrap();

    let payloads = observer.payloads();
    let planned = payloads
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ContextPlanned { .. }))
        .expect("live plan event");
    let compacted = payloads
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::ContextCompacted {
                    reclaimed_tokens,
                    ..
                } if *reclaimed_tokens > 0
            )
        })
        .expect("the live compaction outcome is emitted");
    assert!(planned < compacted);
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ContextCompacted { .. }))
            .count(),
        1,
        "only the current plan's owned compaction outcome is emitted"
    );
}

#[tokio::test]
async fn artifact_offload_workflow_keeps_large_output_retrievable() {
    let mut first = tool_call_fragments(0, "call-large", "large_output", "{}");
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut second = tool_call_fragments(
        0,
        "call-read-artifact",
        "artifact.read",
        r#"{"id":"artifact-full-output","offset":0,"limit":256}"#,
    );
    second.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(second),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "artifact inspected".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let store = Arc::new(ScenarioArtifactStore::default());
    let offloader = ArtifactOffloader::new(store.clone())
        .with_threshold_bytes(256)
        .unwrap()
        .with_preview_chars(128)
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(Arc::new(LargeArtifactOutputTool))
        .tool(Arc::new(ArtifactReadTool::new(store.clone())))
        .tool_output_processor(Arc::new(offloader))
        .security_check(
            Arc::new(ArtifactAllowCheck {
                id: SecurityCheckId::new("allow-session-artifact-read"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(ARTIFACT_READ_PERMISSION)),
            ActionClass::new("artifact-read"),
        )
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("produce and inspect the full output"))
        .await
        .unwrap();

    assert_eq!(store.reads.load(Ordering::Acquire), 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let offloaded_result = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::ToolResult(result) if result.call_id == ToolCallId::new("call-large") => {
                Some(
                    result
                        .content
                        .iter()
                        .filter_map(ContentPart::as_text)
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            }
            _ => None,
        })
        .expect("the second request carries the large tool result");
    assert!(
        offloaded_result.contains("artifact-full-output"),
        "second request did not carry the artifact reference: {}",
        offloaded_result.chars().take(1_000).collect::<String>()
    );
    assert!(offloaded_result.contains("use artifact.read"));
    assert!(
        !offloaded_result.contains("MIDDLE_SENTINEL"),
        "the full oversized result is not copied back into provider context"
    );
    let paged_result = requests[2]
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::ToolResult(result)
                if result.call_id == ToolCallId::new("call-read-artifact") =>
            {
                Some(
                    result
                        .content
                        .iter()
                        .filter_map(ContentPart::as_text)
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            }
            _ => None,
        })
        .expect("the third request carries the paged artifact result");
    assert!(paged_result.contains("\"artifact\":\"artifact-full-output\""));
    assert!(paged_result.contains("\"next_offset\":256"));
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "artifact inspected")
    );
}

#[tokio::test]
async fn local_tool_action_is_checkpointed_offloaded_and_never_spends_provider_tokens() {
    let provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let artifacts = Arc::new(ScenarioArtifactStore::default());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let offloader = ArtifactOffloader::new(artifacts.clone())
        .with_threshold_bytes(256)
        .unwrap()
        .with_preview_chars(128)
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(Arc::new(LargeArtifactOutputTool))
        .tool_output_processor(Arc::new(offloader))
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("local-artifact-action");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let result = session
        .run_local_tool("large_output", json!({}), 10_000)
        .await
        .expect("local result");

    assert!(provider.requests().is_empty(), "local action spent tokens");
    assert!(
        result.content.iter().any(|part| part
            .as_text()
            .is_some_and(|text| text.contains("artifact-full-output"))),
        "local result did not retain an artifact reference: {result:?}"
    );
    assert!(artifacts.stored.lock().unwrap().is_some());
    let history = checkpoints.history(&id);
    let state_names = history
        .iter()
        .map(|checkpoint| match checkpoint.state {
            TurnState::LocalActionAccepted { .. } => "accepted",
            TurnState::LocalActionPrepared { .. } => "prepared",
            TurnState::LocalActionExecuting { .. } => "executing",
            TurnState::LocalActionOutcomeReady { .. } => "outcome",
            TurnState::LocalActionResultReady { .. } => "result",
            TurnState::Completing { .. } => "completing",
            TurnState::PublishingTerminal { .. } => "publishing",
            TurnState::Terminal { .. } => "terminal",
            _ => "unexpected",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        state_names,
        [
            "accepted",
            "prepared",
            "executing",
            "outcome",
            "result",
            "completing",
            "publishing",
            "terminal",
        ]
    );
}

#[tokio::test]
async fn local_tool_approval_observes_turn_cancellation_and_deadline() {
    let provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let approval = Arc::new(BlockingApproval::default());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(approval.clone())
        .checkpoint_store(checkpoints)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    let entered = approval.entered.notified();
    let local_session = session.clone();
    let pending = tokio::spawn(async move {
        local_session
            .run_local_tool("checkpoint_write", json!({}), 5_000)
            .await
    });
    entered.await;
    session
        .interrupt_current_turn(CancelReason::UserRequested)
        .expect("active local action");
    let cancelled = pending
        .await
        .unwrap()
        .expect("canonical cancellation result");
    assert!(cancelled.is_error);
    assert!(
        cancelled
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("cancel")),
        "{cancelled:?}"
    );

    let timed_out = session
        .run_local_tool("checkpoint_write", json!({}), 25)
        .await
        .expect("canonical timeout result");
    assert!(timed_out.is_error);
    assert!(
        timed_out
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("timed out")),
        "{timed_out:?}"
    );
    assert!(provider.requests().is_empty(), "local actions spent tokens");
}

#[tokio::test]
async fn local_tool_recovery_executes_prepared_once_and_never_replays_a_durable_outcome() {
    let id = SessionId::new("local-action-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(source_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: source_invocations.clone(),
        }))
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source
        .run_local_tool("local_count", json!({}), 10_000)
        .await
        .unwrap();
    assert_eq!(source_invocations.load(Ordering::Acquire), 1);
    assert!(source_provider.requests().is_empty());
    let history = source_store.history(&id);
    let prepared = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::LocalActionPrepared { .. }))
        .expect("prepared local action")
        .clone();
    let outcome = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::LocalActionOutcomeReady { .. }))
        .expect("durable local outcome")
        .clone();

    let prepared_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    prepared_store.seed(prepared).unwrap();
    let prepared_invocations = Arc::new(AtomicUsize::new(0));
    let prepared_observer = RecordingObserver::shared();
    let prepared_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let prepared_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(prepared_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: prepared_invocations.clone(),
        }))
        .checkpoint_store(prepared_store)
        .observer(prepared_observer.clone())
        .build()
        .unwrap();
    prepared_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&prepared_observer).await;
    assert_eq!(prepared_invocations.load(Ordering::Acquire), 1);
    assert!(prepared_provider.requests().is_empty());

    let outcome_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    outcome_store.seed(outcome).unwrap();
    let outcome_invocations = Arc::new(AtomicUsize::new(0));
    let outcome_observer = RecordingObserver::shared();
    let outcome_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let outcome_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(outcome_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: outcome_invocations.clone(),
        }))
        .checkpoint_store(outcome_store)
        .observer(outcome_observer.clone())
        .build()
        .unwrap();
    outcome_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&outcome_observer).await;
    assert_eq!(
        outcome_invocations.load(Ordering::Acquire),
        0,
        "durable raw outcome was replayed"
    );
    assert!(outcome_provider.requests().is_empty());
}

// runtime-api: "Versioned commands and events" — schema versioned + stable.
#[tokio::test]
async fn events_are_versioned_and_roundtrip() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hi")), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    event_schema::assert_versioned_and_roundtrips(&observer.events());
    event_schema::assert_v7_golden_fixture();
    event_schema::assert_v8_golden_fixture();
    event_schema::assert_v9_golden_fixture();
    event_schema::assert_v6_golden_fixture();
    event_schema::assert_v5_golden_fixture();
    event_schema::assert_unattributed_output_fixtures_are_rejected();
    event_schema::assert_v1_fixture_rejected_by_current_schema();
}

// runtime-api: "Two hosts run the same fixture" — canonical event sequences are
// equivalent regardless of presentation.
#[tokio::test]
async fn two_hosts_produce_equivalent_canonical_events() {
    async fn run_host() -> Vec<RuntimeEvent> {
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_tool_then_text(
            "echo",
            &json!({"x": 1}),
            "done",
        ));
        let runtime = build(provider, observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        observer.payloads()
    }
    let host_a = run_host().await;
    let host_b = run_host().await;
    assert_eq!(host_a, host_b, "canonical event sequences must match");
}

// agent-execution: "Tool-step limit is reached".
#[tokio::test]
async fn tool_step_limit_emits_structured_terminal() {
    // A provider that always requests a tool.
    let scripts: Vec<ScriptedStream> = (0..5)
        .map(|_| {
            let mut events = tool_call_fragments(0, "call-loop", "echo", "{}");
            events.push(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            });
            ScriptedStream::new(events)
        })
        .collect();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .max_tool_steps(2)
        .build()
        .unwrap();

    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("go")).await;

    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ToolSteps
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ToolSteps
            }
        })
    ));
}

// provider-runtime: "Unsupported reasoning request" — a configured downgrade is
// observable; without it the turn fails before I/O.
#[tokio::test]
async fn unsupported_reasoning_downgrades_when_allowed() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::permissive())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    assert!(observer.payloads().iter().any(
        |e| matches!(e, RuntimeEvent::Downgrade { capability, .. } if capability == "reasoning")
    ));
}

#[tokio::test]
async fn unsupported_reasoning_fails_closed_when_strict() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::strict())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::Error { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Failed
        })
    ));
}

// tool-execution: "Consumer registers a product tool" is covered by the consumer
// fixtures; here we confirm an unknown tool becomes a canonical error result and
// the loop still completes.
#[tokio::test]
async fn unknown_tool_becomes_error_result_and_loop_continues() {
    let observer = RecordingObserver::shared();
    // Provider asks for `echo`, but the runtime registers no tools.
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({}),
        "recovered",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::ToolCallCompleted { is_error: true, .. }))
    );
    rt::assert_terminates(&payloads);
}

#[tokio::test]
async fn registered_tool_arguments_are_schema_validated_before_exposure() {
    let mut events = tool_call_fragments(0, "call-invalid", "echo", "\"not an object\"");
    events.push(agent_runtime::provider::fake::usage_event(3, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(events)],
        )),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ToolCallRequested { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Failed
        })
    ));
    assert!(session.snapshot().usage.records()[0].provenance.failed);
}

// source-ownership / runtime-api: sessions resume from a persisted snapshot.
#[tokio::test]
async fn session_resumes_from_store() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .approval(Arc::new(AllowAll))
        .session_store(store.clone())
        .observer(observer)
        .build()
        .unwrap();

    let id = SessionId::new("persist-1");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(store.len(), 1);

    // A new session with the same id resumes the saved history.
    let resumed = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert!(!resumed.history().is_empty(), "history should be resumed");
}

#[tokio::test]
async fn ordinary_session_store_resume_restores_the_previous_cache_plan() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let id = SessionId::new("persist-cache-plan");
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .system_prompt("stable cache prefix")
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("first turn")).await.unwrap();
    first.shutdown().await.unwrap();

    let saved = store.load(&id).await.unwrap().expect("turn was persisted");
    assert!(
        saved.extension_state.values().any(|state| state.sensitivity
            == agent_runtime_core::store::SessionStateSensitivity::RedactionSafe),
        "ordinary storage must retain the redaction-safe planner cache record"
    );

    let observer = RecordingObserver::shared();
    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .system_prompt("stable cache prefix")
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .session_store(store)
        .observer(observer.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    resumed.run(UserInput::text("second turn")).await.unwrap();

    let preserved = observer
        .payloads()
        .into_iter()
        .find_map(|event| match event {
            RuntimeEvent::CachePlanChanged {
                preserved_prefix_tokens,
                ..
            } => Some(preserved_prefix_tokens),
            _ => None,
        })
        .expect("resumed provider request emits a cache plan");
    assert!(
        preserved > 0,
        "the resumed planner must compare against the prior persisted cache prefix"
    );
}

#[tokio::test]
async fn provider_switch_rebases_only_the_incompatible_previous_cache_baseline() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let id = SessionId::new("persist-cache-provider-switch");
    let first_profile = ResolvedModelProfile::explicit(
        "alpha",
        ModelId::new("model-a"),
        ModelLimits::new(8_000, 8_000, 256),
    );
    let first_runtime = RuntimeBuilder::new(ModelId::new("model-a"))
        .model_profile(first_profile)
        .provider(Arc::new(FakeProvider::new(
            "model-a",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "first answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )))
        .system_prompt("stable cache prefix")
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("first turn")).await.unwrap();
    first.shutdown().await.unwrap();

    let second_profile = ResolvedModelProfile::explicit(
        "beta",
        ModelId::new("model-b"),
        ModelLimits::new(4_000, 4_000, 256),
    );
    let second_runtime = RuntimeBuilder::new(ModelId::new("model-b"))
        .model_profile(second_profile)
        .provider(Arc::new(FakeProvider::new(
            "model-b",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "second answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )))
        .system_prompt("stable cache prefix")
        .session_store(store)
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .expect("a valid cache baseline from another profile is only an optimization miss");
    assert!(
        resumed
            .history()
            .iter()
            .any(|message| message.joined_text() == "first answer"),
        "rebasing cache state must preserve canonical conversation history"
    );
    assert!(
        !resumed
            .snapshot()
            .extension_state
            .contains_key("runtime.core.previous_cache"),
        "the incompatible baseline must be removed before the next snapshot"
    );

    resumed.run(UserInput::text("second turn")).await.unwrap();
    assert!(
        resumed
            .snapshot()
            .extension_state
            .contains_key("runtime.core.previous_cache"),
        "the switched planner must persist its own replacement baseline"
    );
}

#[tokio::test]
async fn one_runtime_leases_each_explicit_session_identity_once() {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("unused")))
        .build()
        .unwrap();
    let id = SessionId::new("active-session-lease");
    let first = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let duplicate = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap_err();
    assert!(duplicate.message.contains("already active"));

    first.shutdown().await.unwrap();
    let after_shutdown = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    drop(after_shutdown);

    let after_drop = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    drop(after_drop);
}

#[tokio::test]
async fn completed_turn_is_persisted_before_shutdown() {
    let sessions = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("durable answer")))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("persist-before-shutdown");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    session.run(UserInput::text("persist me")).await.unwrap();

    let saved = sessions
        .load(&id)
        .await
        .unwrap()
        .expect("completed turn is saved without shutdown");
    assert_eq!(saved.history, session.history());
    assert_eq!(saved.manifests, session.snapshot().manifests);
    assert!(
        saved
            .history
            .iter()
            .any(|message| message.joined_text().contains("durable answer"))
    );

    let checkpoint = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("terminal checkpoint exists");
    assert!(matches!(
        checkpoint.state,
        TurnState::Terminal {
            finish: TurnFinish::Completed,
            visible_output: true,
        }
    ));
    checkpoint.validate().unwrap();
}

#[tokio::test]
async fn model_response_is_not_committed_before_its_checkpoint() {
    let checkpoints = Arc::new(FailOnceCheckpointStore::new(
        FailingCheckpointBoundary::ModelResponseReady,
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("speculative only")))
        .checkpoint_store(checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(
            StartSession::new().with_id(SessionId::new("model-response-checkpoint-failure")),
        )
        .await
        .unwrap();

    session.run(UserInput::text("hello")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
    );
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputDiscarded { .. }))
            .count(),
        1
    );
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
    assert!(
        session
            .history()
            .iter()
            .all(|message| !message.joined_text().contains("speculative only"))
    );
}

#[tokio::test]
async fn accepted_recovery_keeps_the_exact_active_input_boundary() {
    let id = SessionId::new("accepted-boundary-recovery");
    let input = UserInput::text("same text");
    let snapshot = SessionSnapshot {
        id: id.clone(),
        history: vec![
            agent_runtime_core::content::Message::user("older same text"),
            input.clone().into_message(),
            agent_runtime_core::content::Message::user("same text"),
        ],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: Default::default(),
        updated: Timestamp::ZERO,
    };
    let checkpoint = TurnCheckpoint::accepted(
        TurnId::new("turn-1"),
        input,
        snapshot,
        1,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    checkpoints.seed(checkpoint).unwrap();
    let provider = continuation_provider("recovered");
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .checkpoint_store(checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();

    let session = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    let request = &provider.requests()[0];
    assert_eq!(
        request
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::User && message.joined_text() == "same text"
            })
            .count(),
        2,
        "the accepted input and the injected same-text message remain distinct, with no duplicate append"
    );
    assert_eq!(
        session
            .history()
            .iter()
            .filter(|message| {
                message.role == Role::User && message.joined_text() == "same text"
            })
            .count(),
        2
    );
}

#[tokio::test]
async fn model_response_ready_reuses_the_attempt_and_restores_identity_floor() {
    let id = SessionId::new("model-response-ready-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("durable response")))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::ModelResponseReady { .. }))
        .expect("model response boundary");

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint.clone()).unwrap();
    let recovery_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(recovery_provider.clone())
        .checkpoint_store(recovery_checkpoints)
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    let mut floor = checkpoint.snapshot.identity.clone();
    floor.turn = floor.turn.max(100);
    floor.request = floor.request.max(100);
    floor.attempt = floor.attempt.max(100);
    floor.event = floor.event.max(100);
    floor.tool_call = floor.tool_call.max(100);
    floor.event_seq = floor.event_seq.max(100);
    let recovered = recovery_runtime
        .start_session(
            StartSession::new()
                .with_id(id)
                .with_resume_identity_floor(floor.clone()),
        )
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    assert!(
        recovery_provider.requests().is_empty(),
        "a durable assembled response must not call the provider again"
    );
    assert_eq!(recovery_observer.events()[0].seq, floor.event_seq);
    assert_eq!(
        recovered
            .history()
            .iter()
            .filter(|message| {
                message.role == Role::Assistant
                    && message.joined_text().contains("durable response")
            })
            .count(),
        1
    );
    let reconciled = reconciled_payloads(
        &source_observer.events(),
        &checkpoint,
        &recovery_observer.events(),
    );
    assert_eq!(
        reconciled
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
            .count(),
        1
    );
    assert_eq!(
        reconciled
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptFinished { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn planning_calling_and_completing_boundaries_have_explicit_recovery_policy() {
    let id = SessionId::new("simple-boundary-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("source answer")))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    let history = source_checkpoints.history(&id);
    let planning = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Planning { .. }))
        .cloned()
        .unwrap();
    let calling = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::CallingModel { .. }))
        .cloned()
        .unwrap();
    let completing = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Completing { .. }))
        .cloned()
        .unwrap();

    let planning_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    planning_store.seed(planning).unwrap();
    let planning_provider = continuation_provider("planning recovered");
    let planning_observer = RecordingObserver::shared();
    let planning_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(planning_provider.clone())
        .checkpoint_store(planning_store)
        .observer(planning_observer.clone())
        .build()
        .unwrap();
    planning_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&planning_observer).await;
    assert_eq!(planning_provider.requests().len(), 1);

    let calling_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    calling_store.seed(calling).unwrap();
    let calling_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let calling_observer = RecordingObserver::shared();
    let calling_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(calling_provider.clone())
        .checkpoint_store(calling_store)
        .observer(calling_observer.clone())
        .build()
        .unwrap();
    calling_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&calling_observer).await;
    assert!(calling_provider.requests().is_empty());
    assert!(calling_observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::Error { error }
            if error.message.contains("provider outcome is indeterminate")
    )));
    assert!(calling_observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    )));

    let completing_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    completing_store.seed(completing).unwrap();
    let completing_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let completing_observer = RecordingObserver::shared();
    let completing_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(completing_provider.clone())
        .checkpoint_store(completing_store)
        .observer(completing_observer.clone())
        .build()
        .unwrap();
    completing_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&completing_observer).await;
    assert!(completing_provider.requests().is_empty());
    assert_eq!(
        completing_observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn awaiting_approval_reauthorizes_exact_preparation_without_persisting_a_grant() {
    let id = SessionId::new("exact-approval-recovery");
    let source_prepares = Arc::new(AtomicUsize::new(0));
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-exact",
                "exact_prepared_write",
                json!({"path":"out.txt"}),
            )],
            "source done",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(ExactPreparedWriteTool {
            prepares: source_prepares.clone(),
            invocations: source_invocations,
        }))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("write")).await.unwrap();
    assert_eq!(source_prepares.load(Ordering::Acquire), 1);
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingApproval { .. }))
        .expect("approval boundary");

    let recovery_prepares = Arc::new(AtomicUsize::new(0));
    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_approval = Arc::new(OriginRecordingApproval::default());
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("recovered done"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(ExactPreparedWriteTool {
            prepares: recovery_prepares.clone(),
            invocations: recovery_invocations.clone(),
        }))
        .legacy_approval_authority()
        .approval(recovery_approval.clone())
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(
        recovery_prepares.load(Ordering::Acquire),
        0,
        "checkpointed prepared authority is never silently re-prepared"
    );
    assert_eq!(recovery_invocations.load(Ordering::Acquire), 1);
    assert_eq!(
        recovery_approval.origins.lock().unwrap().len(),
        1,
        "approval/grant state is not persisted; current policy is consulted again"
    );
}

#[tokio::test]
async fn executing_tools_recovery_keeps_committed_parallel_prefix_and_never_replays() {
    let id = SessionId::new("parallel-prefix-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[
                ("call-1", "parallel_count", json!({"n":1})),
                ("call-2", "parallel_count", json!({"n":2})),
            ],
            "source complete",
        )))
        .tool(Arc::new(CountingPureTool {
            name: "parallel_count",
            invocations: source_invocations.clone(),
        }))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("run both")).await.unwrap();
    assert_eq!(source_invocations.load(Ordering::Acquire), 2);
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.state,
                TurnState::ExecutingTools { completed, .. } if completed.len() == 1
            )
        })
        .expect("one-result committed prefix");

    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint.clone()).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("recovered complete"))
        .tool(Arc::new(CountingPureTool {
            name: "parallel_count",
            invocations: recovery_invocations.clone(),
        }))
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(
        recovery_invocations.load(Ordering::Acquire),
        0,
        "neither a committed nor an unknown in-flight side effect is replayed"
    );
    let recovered_history = recovered.history();
    let results = recovered_history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .map(|result| result.call_id.as_str())
            .collect::<Vec<_>>(),
        ["call-1", "call-2"]
    );
    assert!(!results[0].is_error);
    assert!(results[1].is_error);
    assert!(results[1].content.iter().any(|part| {
        part.as_text()
            .is_some_and(|text| text.contains("indeterminate"))
    }));

    let source_events = source_observer.events();
    let before_completion_event = source_events
        .iter()
        .filter(|event| event.seq < checkpoint.watermark.event_sequence)
        .cloned()
        .collect::<Vec<_>>();
    let recovery_events = observer.events();
    let crash_before_event =
        reconciled_payloads(&before_completion_event, &checkpoint, &recovery_events);
    let crash_after_event = reconciled_payloads(&source_events, &checkpoint, &recovery_events);
    assert_eq!(crash_before_event, crash_after_event);
    for call in ["call-1", "call-2"] {
        assert_eq!(
            crash_after_event
                .iter()
                .filter(|event| matches!(
                    event,
                    RuntimeEvent::ToolCallCompleted { call: completed, .. }
                        if completed.as_str() == call
                ))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn mixed_ready_denied_and_pure_batch_recovers_in_source_order() {
    let id = SessionId::new("mixed-tool-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[
                ("call-pure-1", "mixed_pure", json!({"n":1})),
                ("call-denied", "checkpoint_write", json!({})),
                ("call-pure-2", "mixed_pure", json!({"n":2})),
            ],
            "source complete",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CountingPureTool {
            name: "mixed_pure",
            invocations: source_invocations,
        }))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(Arc::new(DenyAll))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("mixed batch")).await.unwrap();
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.state,
                TurnState::ExecutingTools { completed, .. } if completed.is_empty()
            )
        })
        .expect("pre-invocation execution boundary");
    let TurnState::ExecutingTools {
        source_calls,
        slots,
        ..
    } = &checkpoint.state
    else {
        unreachable!()
    };
    assert_eq!(source_calls.len(), 3);
    assert_eq!(
        slots
            .iter()
            .map(|slot| slot.call_id().as_str())
            .collect::<Vec<_>>(),
        ["call-pure-1", "call-denied", "call-pure-2"],
        "every source slot has an exact prepared or canonical-result disposition"
    );
    assert!(matches!(
        &slots[1],
        ToolSlotCheckpoint::CanonicalResult(result)
            if result.call_id.as_str() == "call-denied"
    ));

    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("mixed recovered"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CountingPureTool {
            name: "mixed_pure",
            invocations: recovery_invocations.clone(),
        }))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(Arc::new(DenyAll))
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(recovery_invocations.load(Ordering::Acquire), 0);
    let recovered_history = recovered.history();
    let results = recovered_history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .map(|result| result.call_id.as_str())
            .collect::<Vec<_>>(),
        ["call-pure-1", "call-denied", "call-pure-2"]
    );
    assert!(results.iter().all(|result| result.is_error));
}

#[tokio::test]
async fn terminal_publication_recovers_before_or_after_the_event_exactly_once() {
    let id = SessionId::new("terminal-publication-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("complete")))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("finish")).await.unwrap();
    let history = source_checkpoints.history(&id);
    let publishing = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::PublishingTerminal { .. }))
        .cloned()
        .expect("publishing boundary");
    let terminal = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Terminal { .. }))
        .cloned()
        .expect("terminal barrier");

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(publishing.clone()).unwrap();
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(recovery_checkpoints)
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    recovery_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    let source_events = source_observer.events();
    let before_terminal_event = source_events
        .iter()
        .filter(|event| event.seq < publishing.watermark.event_sequence)
        .cloned()
        .collect::<Vec<_>>();
    let recovery_events = recovery_observer.events();
    let before = reconciled_payloads(&before_terminal_event, &publishing, &recovery_events);
    let after = reconciled_payloads(&source_events, &publishing, &recovery_events);
    assert_eq!(before, after);
    assert_eq!(
        after
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );

    let terminal_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    terminal_store.seed(terminal).unwrap();
    let terminal_observer = RecordingObserver::shared();
    let terminal_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(terminal_store)
        .observer(terminal_observer.clone())
        .build()
        .unwrap();
    terminal_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(
        terminal_observer
            .payloads()
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::TurnCompleted { .. })),
        "Terminal proves the existing journal event and must not republish it"
    );
    assert_eq!(
        source_events
            .iter()
            .filter(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn publishing_terminal_recovery_preserves_commit_hook_state_and_usage_without_rerun() {
    let id = SessionId::new("publishing-terminal-hook-recovery");
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let artifacts = Arc::new(ScenarioArtifactStore::default());
    let summary_model = Arc::new(CountingSummaryModel::default());
    let summary = Arc::new(
        SemanticSummaryCoordinator::new(
            artifacts,
            summary_model.clone(),
            SemanticSummaryPolicy {
                trigger_turns: 2,
                retain_turns: 1,
                ..SemanticSummaryPolicy::new(RegistryRevision::new("durable-summary-v1"))
            },
        )
        .unwrap(),
    );
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "first answer".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "second answer".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
            ],
        )))
        .checkpoint_store(checkpoints.clone())
        .history_projector(summary.clone())
        .turn_commit_hook(Arc::new(CountingSemanticSummaryHook {
            inner: summary.clone(),
            calls: hook_calls.clone(),
        }))
        .observer(observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("first request")).await.unwrap();
    source.run(UserInput::text("second request")).await.unwrap();

    assert_eq!(hook_calls.load(Ordering::Acquire), 2);
    assert_eq!(summary_model.calls.load(Ordering::Acquire), 1);
    let publishing = checkpoints
        .history(&id)
        .into_iter()
        .rev()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::PublishingTerminal { .. }))
        .expect("second turn has a publishing boundary");
    assert_eq!(
        publishing
            .snapshot
            .usage
            .records()
            .iter()
            .filter(|record| record.source == UsageSource::SemanticSummary)
            .count(),
        1,
        "PublishingTerminal protects post-hook usage"
    );
    assert!(
        publishing.snapshot.extension_state.values().any(|state| {
            state.sensitivity == agent_runtime_core::store::SessionStateSensitivity::Sensitive
        }),
        "PublishingTerminal protects the semantic summary state"
    );
    assert!(
        observer.events().iter().any(|event| {
            event.seq < publishing.watermark.event_sequence
                && matches!(
                    &event.payload,
                    RuntimeEvent::Usage { record }
                        if record.source == UsageSource::SemanticSummary
                )
        }),
        "the protected watermark follows the hook's usage event"
    );

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(publishing).unwrap();
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(recovery_checkpoints.clone())
        .history_projector(summary.clone())
        .turn_commit_hook(Arc::new(CountingSemanticSummaryHook {
            inner: summary,
            calls: hook_calls.clone(),
        }))
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    let recovered = recovery_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    assert_eq!(
        hook_calls.load(Ordering::Acquire),
        2,
        "PublishingTerminal recovery must not invoke turn-commit hooks again"
    );
    assert_eq!(
        summary_model.calls.load(Ordering::Acquire),
        1,
        "the idempotently keyed summary call is not repeated after its result is protected"
    );
    assert_eq!(
        recovered
            .snapshot()
            .usage
            .records()
            .iter()
            .filter(|record| record.source == UsageSource::SemanticSummary)
            .count(),
        1
    );
    assert!(
        recovery_observer.payloads().iter().all(|event| {
            !matches!(
                event,
                RuntimeEvent::Usage { record }
                    if record.source == UsageSource::SemanticSummary
            )
        }),
        "recovery does not duplicate the already protected usage event"
    );
    assert!(matches!(
        recovery_checkpoints
            .load_latest(recovered.id())
            .await
            .unwrap(),
        Some(TurnCheckpoint {
            state: TurnState::Terminal { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn raw_tool_outcome_checkpoint_failure_never_replays_the_invocation() {
    let id = SessionId::new("raw-tool-outcome-failure");
    let invocations = Arc::new(AtomicUsize::new(0));
    let checkpoints = Arc::new(FailOnceCheckpointStore::new(
        FailingCheckpointBoundary::ToolOutcomeReady,
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[("call-raw", "raw_count", json!({}))],
            "recovered after indeterminate result",
        )))
        .tool(Arc::new(CountingPureTool {
            name: "raw_count",
            invocations: invocations.clone(),
        }))
        .checkpoint_store(checkpoints.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let source = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("run once")).await.unwrap();

    assert_eq!(invocations.load(Ordering::Acquire), 1);
    assert!(
        observer
            .payloads()
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ToolCallCompleted { .. })),
        "a raw outcome that missed its checkpoint is not canonically committed"
    );
    let durable = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("pre-invocation checkpoint remains durable");
    assert!(matches!(
        durable.state,
        TurnState::ExecutingTools {
            ref completed,
            ..
        } if completed.is_empty()
    ));
    source.shutdown().await.unwrap();

    let terminals_before_resume = observer
        .payloads()
        .iter()
        .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
        .count();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if observer
                .payloads()
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count()
                > terminals_before_resume
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered turn reaches a terminal boundary");
    assert_eq!(
        invocations.load(Ordering::Acquire),
        1,
        "recovery must synthesize an indeterminate result instead of replaying"
    );
    let history = recovered.history();
    let result = history
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(result) if result.call_id.as_str() == "call-raw" => {
                Some(result)
            }
            _ => None,
        })
        .expect("recovery commits a canonical paired result");
    assert!(result.is_error);
    assert!(result.content.iter().any(|part| {
        part.as_text()
            .is_some_and(|text| text.contains("indeterminate"))
    }));
}

#[tokio::test]
async fn every_checkpoint_and_session_store_failure_has_one_live_terminal() {
    for boundary in [
        FailingCheckpointBoundary::Accepted,
        FailingCheckpointBoundary::Planning,
        FailingCheckpointBoundary::CallingModel,
        FailingCheckpointBoundary::ModelResponseReady,
        FailingCheckpointBoundary::Completing,
        FailingCheckpointBoundary::PublishingTerminal,
        FailingCheckpointBoundary::Terminal,
    ] {
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(Arc::new(scenarios::fake_text("answer")))
            .checkpoint_store(Arc::new(FailOnceCheckpointStore::new(boundary)))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(
                StartSession::new()
                    .with_id(SessionId::new(format!("checkpoint-failure-{boundary:?}"))),
            )
            .await
            .unwrap();
        session.run(UserInput::text("hello")).await.unwrap();
        let payloads = observer.payloads();
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnStarted))
                .count(),
            1,
            "{boundary:?}"
        );
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count(),
            1,
            "{boundary:?}"
        );
    }

    for boundary in [
        FailingCheckpointBoundary::AwaitingApproval,
        FailingCheckpointBoundary::ExecutingEmpty,
        FailingCheckpointBoundary::ExecutingCompleted,
    ] {
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(Arc::new(tool_batch_provider(
                &[("call-write", "checkpoint_write", json!({}))],
                "unused continuation",
            )))
            .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
            .tool(Arc::new(CheckpointWriteTool))
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .checkpoint_store(Arc::new(FailOnceCheckpointStore::new(boundary)))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::new().with_id(SessionId::new(format!(
                "tool-checkpoint-failure-{boundary:?}"
            ))))
            .await
            .unwrap();
        session.run(UserInput::text("write")).await.unwrap();
        let payloads = observer.payloads();
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnStarted))
                .count(),
            1,
            "{boundary:?}"
        );
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count(),
            1,
            "{boundary:?}"
        );
    }

    let observer = RecordingObserver::shared();
    let session_store = Arc::new(FailSessionStore::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("answer")))
        .session_store(session_store.clone())
        .checkpoint_store(Arc::new(
            agent_runtime_testkit::InMemoryCheckpointStore::new(),
        ))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-store-failure")))
        .await
        .unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    assert!(session_store.failed.load(Ordering::Acquire));
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn terminal_resume_prefers_non_regressing_canonical_session_snapshot() {
    let id = SessionId::new("terminal-session-precedence");
    let sessions = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("sensitive answer")))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let source = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    source.shutdown().await.unwrap();

    let terminal = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("terminal checkpoint");
    assert!(matches!(terminal.state, TurnState::Terminal { .. }));
    let mut canonical = sessions
        .load(&id)
        .await
        .unwrap()
        .expect("canonical session snapshot");
    assert!(
        canonical.identity.is_at_least(&terminal.snapshot.identity),
        "orderly shutdown legitimately advances identity after Terminal"
    );
    for message in &mut canonical.history {
        if message.role == Role::Assistant {
            *message = agent_runtime_core::content::Message::assistant(vec![ContentPart::text(
                "[canonical redacted]",
            )]);
        }
    }
    sessions.seed(canonical);

    let resumed = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert!(
        resumed
            .history()
            .iter()
            .any(|message| message.joined_text() == "[canonical redacted]")
    );
    assert!(
        resumed
            .history()
            .iter()
            .all(|message| !message.joined_text().contains("sensitive answer"))
    );
    resumed.shutdown().await.unwrap();

    let mut regressed = sessions.load(&id).await.unwrap().unwrap();
    regressed.identity.event_seq = terminal.snapshot.identity.event_seq.saturating_sub(1);
    sessions.seed(regressed);
    let error = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert!(
        error.message.contains("identity") && error.message.contains("terminal checkpoint"),
        "the conflict must identify the non-equivalent terminal boundary: {error:?}"
    );
}

#[tokio::test]
async fn resume_preserves_all_historical_manifests() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "answer-one".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "answer-two".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    );
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(first_provider))
        .session_store(store.clone())
        .build()
        .unwrap();
    let id = SessionId::new("manifest-round-trip");
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("turn one")).await.unwrap();
    first.run(UserInput::text("turn two")).await.unwrap();
    first.shutdown().await.unwrap();

    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("answer-three")))
        .session_store(store.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert_eq!(resumed.snapshot().manifests.len(), 2);
    resumed.run(UserInput::text("turn three")).await.unwrap();
    resumed.shutdown().await.unwrap();

    let final_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("unused")))
        .session_store(store)
        .build()
        .unwrap();
    let loaded = final_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    let manifests = loaded.snapshot().manifests;
    assert_eq!(manifests.len(), 3);
    assert_eq!(
        manifests
            .iter()
            .map(|manifest| manifest.turn.as_str())
            .collect::<Vec<_>>(),
        ["turn-1", "turn-2", "turn-3"]
    );
}

#[tokio::test]
async fn fresh_session_ids_do_not_collide_across_runtime_restarts() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    let first_id = first.id().clone();
    first.shutdown().await.unwrap();

    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .build()
        .unwrap();
    let second = second_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    assert_ne!(first_id, *second.id());
    assert!(second.history().is_empty());
}

#[tokio::test]
async fn resumed_session_continues_ids_and_event_sequences() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_observer = RecordingObserver::shared();
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .observer(first_observer.clone())
        .build()
        .unwrap();
    let id = SessionId::new("resume-counters");
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        first
            .run(UserInput::text("first turn"))
            .await
            .unwrap()
            .id()
            .as_str(),
        "turn-1"
    );
    first.shutdown().await.unwrap();
    let first_max_seq = first_observer
        .events()
        .iter()
        .map(|event| event.seq)
        .max()
        .unwrap();

    let second_observer = RecordingObserver::shared();
    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .observer(second_observer.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert_eq!(
        resumed
            .run(UserInput::text("second turn"))
            .await
            .unwrap()
            .id()
            .as_str(),
        "turn-2"
    );
    let resumed_events = second_observer.events();
    assert!(resumed_events.iter().all(|event| event.seq > first_max_seq));
    assert!(
        resumed_events
            .iter()
            .all(|event| event.id.as_str() != "evt-1")
    );
}

// consumer fixtures build and run the shared loop with distinct neutral policy.
#[tokio::test]
async fn all_consumer_fixtures_run_the_shared_loop() {
    for (label, payloads) in [
        ("smith", run_consumer_smith().await),
        ("nyx", run_consumer_nyx().await),
        ("forge", run_consumer_forge().await),
    ] {
        rt::assert_terminates(&payloads);
        assert!(
            rt::has_tool_completed(&payloads, "echo"),
            "{label} should complete the echo tool"
        );
    }
}

#[tokio::test]
async fn questionnaire_mixed_batch_is_sequential_bounded_and_metadata_only() {
    let broker = Arc::new(AnsweringInteractionBroker::default());
    let observer = RecordingObserver::shared();
    let pure_invocations = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(tool_batch_provider(
        &[
            (
                "call-question-1",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("secret-choice-one", "sensitive"),
            ),
            ("call-pure", "middle_pure", json!({})),
            (
                "call-question-2",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("secret-choice-two", "sensitive"),
            ),
        ],
        "clarified",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .tool(Arc::new(CountingPureTool {
            name: "middle_pure",
            invocations: pure_invocations.clone(),
        }))
        .interaction_broker(broker.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("clarify twice")).await.unwrap();

    let requests = broker.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.origin().call().as_str())
            .collect::<Vec<_>>(),
        ["call-question-1", "call-question-2"]
    );
    assert_ne!(requests[0].id(), requests[1].id());
    assert_eq!(pure_invocations.load(Ordering::Acquire), 1);
    assert_eq!(
        broker
            .closed
            .lock()
            .unwrap()
            .iter()
            .map(|(_, outcome)| *outcome)
            .collect::<Vec<_>>(),
        [
            InteractionOutcomeKind::Answered,
            InteractionOutcomeKind::Answered
        ]
    );

    let result_names = session
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.name),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        result_names,
        [
            QUESTIONNAIRE_TOOL_NAME,
            "middle_pure",
            QUESTIONNAIRE_TOOL_NAME
        ]
    );

    let event_json = serde_json::to_string(&observer.events()).unwrap();
    assert!(!event_json.contains("Which implementation"));
    assert!(!event_json.contains("secret-choice-one"));
    assert!(!event_json.contains("secret-choice-two"));
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::InteractionRequested { .. }))
            .count(),
        2
    );
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::InteractionResolved { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn unavailable_interaction_is_not_advertised_but_forced_calls_fail_fast() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(tool_batch_provider(
        &[(
            "call-question",
            QUESTIONNAIRE_TOOL_NAME,
            questionnaire_arguments("forced", "public"),
        )],
        "continued",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("forced ask")).await.unwrap();

    assert!(
        provider.requests()[0]
            .tools
            .iter()
            .all(|schema| schema.name != QUESTIONNAIRE_TOOL_NAME)
    );
    assert!(
        session
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .flat_map(|result| &result.content)
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("\"outcome\":\"unavailable\""))
    );
    assert!(matches!(
        observer
            .payloads()
            .iter()
            .find(|event| matches!(event, RuntimeEvent::InteractionResolved { .. })),
        Some(RuntimeEvent::InteractionResolved {
            outcome: InteractionOutcomeKind::Unavailable,
            ..
        })
    ));
}

#[tokio::test]
async fn interaction_response_cannot_authorize_or_invoke_an_effectful_action() {
    let broker = Arc::new(AnsweringInteractionBroker::default());
    let invocations = Arc::new(AtomicUsize::new(0));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[("call-adversarial", "authority_bearing_question", json!({}))],
            "continued safely",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(AuthorityBearingInteractionTool {
            invocations: invocations.clone(),
        }))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .interaction_broker(broker.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("adversarial ask"))
        .await
        .unwrap();

    assert!(broker.requests.lock().unwrap().is_empty());
    assert_eq!(invocations.load(Ordering::Acquire), 0);
    let blocks = session
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].is_error);
    assert!(
        blocks[0]
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("permission- and effect-free"))
    );
}

#[tokio::test]
async fn interaction_timeout_and_cancellation_close_the_broker() {
    let timeout_broker = Arc::new(HangingInteractionBroker::default());
    let timeout_provider = Arc::new(tool_batch_provider(
        &[(
            "call-timeout",
            QUESTIONNAIRE_TOOL_NAME,
            questionnaire_arguments("timeout", "public"),
        )],
        "unused",
    ));
    let timeout_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(timeout_provider)
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(timeout_broker.clone())
        .turn_time_limit_ms(25)
        .build()
        .unwrap();
    let timeout_session = timeout_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    timeout_session
        .run(UserInput::text("timeout"))
        .await
        .unwrap();
    assert_eq!(
        timeout_broker.closed.lock().unwrap()[0].1,
        InteractionOutcomeKind::TimedOut
    );

    let cancel_broker = Arc::new(HangingInteractionBroker::default());
    let cancel_observer = RecordingObserver::shared();
    let cancel_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-cancel",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("cancel", "public"),
            )],
            "unused",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(cancel_broker.clone())
        .observer(cancel_observer.clone())
        .build()
        .unwrap();
    let cancel_session = cancel_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    let turn = cancel_session.send(UserInput::text("cancel")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !cancel_broker.requests.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;
    assert_eq!(
        cancel_broker.closed.lock().unwrap()[0].1,
        InteractionOutcomeKind::Cancelled
    );
}

#[tokio::test]
async fn pending_interaction_recovers_from_both_pre_barrier_boundaries() {
    let id = SessionId::new("interaction-recovery-session");
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_broker = Arc::new(HangingInteractionBroker::default());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-recover",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("recover", "sensitive"),
            )],
            "unused",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(source_broker.clone())
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    let source_turn = source.send(UserInput::text("recover ask")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if source_store.history(&id).iter().any(|checkpoint| {
                matches!(
                    checkpoint.state,
                    TurnState::AwaitingInteraction { response: None, .. }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let history = source_store.history(&id);
    let executing = history
        .iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::ExecutingTools { ref completed, .. }
                    if completed.is_empty()
            )
        })
        .unwrap()
        .clone();
    let awaiting = history
        .iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::AwaitingInteraction { response: None, .. }
            )
        })
        .unwrap()
        .clone();
    let expected_request = match &awaiting.state {
        TurnState::AwaitingInteraction { request, .. } => request.clone(),
        _ => unreachable!(),
    };
    source_turn.interrupt(CancelReason::UserRequested);
    source_turn.completed().await;

    let deferred_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    deferred_store.seed(awaiting.clone()).unwrap();
    let deferred_broker = Arc::new(AnsweringInteractionBroker::default());
    let deferred_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("must remain dormant"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(deferred_broker.clone())
        .checkpoint_store(deferred_store.clone())
        .build()
        .unwrap();
    let deferred = deferred_runtime
        .start_session(
            StartSession::new()
                .with_id(id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::DeferPendingInteraction),
        )
        .await
        .unwrap();
    assert!(deferred.send(UserInput::text("must reject")).is_err());
    assert!(deferred_broker.requests.lock().unwrap().is_empty());
    let before_shutdown = deferred_store.load_latest(&id).await.unwrap().unwrap();
    deferred.shutdown().await.unwrap();
    let after_shutdown = deferred_store.load_latest(&id).await.unwrap().unwrap();
    assert_eq!(after_shutdown, before_shutdown);
    assert!(deferred_broker.requests.lock().unwrap().is_empty());

    let resumed_broker = Arc::new(AnsweringInteractionBroker::default());
    let resumed_observer = RecordingObserver::shared();
    let resumed_provider = continuation_provider("resumed after defer");
    let resumed_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(resumed_provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(resumed_broker.clone())
        .checkpoint_store(deferred_store)
        .observer(resumed_observer.clone())
        .build()
        .unwrap();
    let resumed_after_defer = resumed_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&resumed_observer).await;
    assert_eq!(resumed_broker.requests.lock().unwrap().len(), 1);
    assert_eq!(
        resumed_broker.requests.lock().unwrap()[0].id(),
        expected_request.id()
    );
    assert_eq!(resumed_provider.requests().len(), 1);
    assert!(
        resumed_after_defer
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .count()
            >= 1
    );

    for checkpoint in [executing] {
        let recovery_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
        recovery_store.seed(checkpoint).unwrap();
        let broker = Arc::new(AnsweringInteractionBroker::default());
        let observer = RecordingObserver::shared();
        let provider = continuation_provider("recovered");
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(provider.clone())
            .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
            .tool(Arc::new(QuestionnaireTool::new()))
            .interaction_broker(broker.clone())
            .checkpoint_store(recovery_store)
            .observer(observer.clone())
            .build()
            .unwrap();
        let resumed = runtime
            .start_session(StartSession::new().with_id(id.clone()))
            .await
            .unwrap();
        wait_for_terminal(&observer).await;

        let requests = broker.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].id(), expected_request.id());
        assert_eq!(requests[0].fingerprint(), expected_request.fingerprint());
        assert_eq!(provider.requests().len(), 1);
        assert!(
            resumed
                .history()
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|part| match part {
                    ContentPart::ToolResult(result) => Some(result),
                    _ => None,
                })
                .flat_map(|result| &result.content)
                .filter_map(ContentPart::as_text)
                .any(|text| text.contains("\"outcome\":\"answered\""))
        );
    }
}

#[tokio::test]
async fn answered_interaction_checkpoint_commits_without_representing() {
    let id = SessionId::new("answered-interaction-recovery");
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_broker = Arc::new(AnsweringInteractionBroker::default());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-answered",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("answered", "sensitive"),
            )],
            "source complete",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(source_broker)
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source
        .run(UserInput::text("answer then crash"))
        .await
        .unwrap();

    let answered_checkpoint = source_store
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::AwaitingInteraction {
                    response: Some(_),
                    ..
                }
            )
        })
        .expect("answer is durable before canonical tool-result commit");
    let expected_response = match &answered_checkpoint.state {
        TurnState::AwaitingInteraction {
            response: Some(response),
            ..
        } => response.clone(),
        _ => unreachable!(),
    };

    let recovery_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_store.seed(answered_checkpoint).unwrap();
    let recovery_broker = Arc::new(AnsweringInteractionBroker::default());
    let observer = RecordingObserver::shared();
    let provider = continuation_provider("answer recovered");
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(recovery_broker.clone())
        .checkpoint_store(recovery_store)
        .observer(observer.clone())
        .build()
        .unwrap();
    let resumed = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert!(recovery_broker.requests.lock().unwrap().is_empty());
    assert!(recovery_broker.closed.lock().unwrap().is_empty());
    assert_eq!(provider.requests().len(), 1);
    let results = resumed
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) if result.name == QUESTIONNAIRE_TOOL_NAME => {
                Some(result)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    let rendered = results[0]
        .content
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<String>();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
        serde_json::to_value(expected_response).unwrap()
    );
}

async fn run_consumer_smith() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::smith::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    observer.payloads()
}

async fn run_consumer_nyx() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::nyx::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    observer.payloads()
}

async fn run_consumer_forge() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::open_forge::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    observer.payloads()
}
