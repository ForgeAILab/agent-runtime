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
use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpTransport};
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

struct AuthRejectingHttpTransport {
    requests: Mutex<Vec<HttpRequest>>,
}

impl AuthRejectingHttpTransport {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl std::fmt::Debug for AuthRejectingHttpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthRejectingHttpTransport")
            .field("request_count", &self.requests.lock().unwrap().len())
            .finish()
    }
}

#[async_trait]
impl HttpTransport for AuthRejectingHttpTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.requests.lock().unwrap().push(request);
        Err(ProviderError::new(
            agent_runtime_core::provider::ProviderErrorKind::Auth,
            "sensitive-auth-body-canary",
        ))
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
