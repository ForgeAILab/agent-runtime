//! Delegation conformance: lifecycle ordering, depth rejection, capacity
//! behavior, scoped child views, and cancellation propagation.
//!
//! The harness composes a parent runtime (with authoritative coverage for the
//! `agent.delegate` permission unless a suite withholds it) and a scripted
//! child factory, then asserts the `agent-delegation` capability contract.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Notify;

use agent_runtime::delegation::{
    CapacityPolicy, ChildRuntimeFactory, ChildState, ChildTaskOutcome, ChildTaskResult,
    DELEGATION_PERMISSION, DelegationConfig, DelegationCoordinator, DelegationLimits, SpawnOutcome,
};
use agent_runtime::harness::{ArtifactOffloader, QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::registry::Permission;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, SessionHandle, StartSession};
use agent_runtime_core::artifact::{
    ArtifactChunk, ArtifactDigest, ArtifactError, ArtifactId, ArtifactRead, ArtifactRef,
    ArtifactStore, ArtifactWrite, MAX_ARTIFACT_READ_BYTES,
};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::check_set::ActionClass;
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::delegation::{
    ChildLimits, ChildModelSelection, ChildSpec, ToolViewScope, WorkspacePolicy,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{ChildPhase, RuntimeEvent};
use agent_runtime_core::grant::{
    GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
    SecurityCheckRevision,
};
use agent_runtime_core::interaction::{InteractionOrigin, InteractionRequest, InteractionResponse};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
    ProviderError, ProviderStream, ProviderStreamEvent,
};
use agent_runtime_core::security::{AuthorizationRequest, PermissionSet};
use agent_runtime_core::tool::{
    InvocationContext, LegacyTool, PreparedToolCall, Tool, ToolEffects, ToolOutcome, ToolSpec,
};

use crate::tools::{EchoTool, WriteTool};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// An authoritative check that allows everything it covers.
#[derive(Debug)]
struct AllowAllCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
}

#[async_trait]
impl SecurityCheck for AllowAllCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
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

/// A parent runtime and session. `covered` controls whether the composed
/// check set has authoritative coverage for the delegation permission —
/// withholding it proves the default-deny posture.
pub async fn parent_session(covered: bool) -> (Runtime, SessionHandle) {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile());
    if covered {
        builder = builder.security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        );
    }
    let runtime = builder.build().expect("parent runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("parent session starts");
    (runtime, session)
}

/// A factory serving one scripted provider per child, in order, registering
/// `tools` on every child builder. Keeps each child's provider so suites can
/// assert what its scoped view advertised.
#[derive(Debug)]
pub struct ScriptedChildFactory {
    scripts: Mutex<VecDeque<Vec<ScriptedStream>>>,
    providers: Mutex<Vec<Arc<FakeProvider>>>,
    tools: Vec<Arc<dyn Tool>>,
    event_buffer: usize,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl ScriptedChildFactory {
    /// A factory with one script per expected child.
    pub fn new(scripts: Vec<Vec<ScriptedStream>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            providers: Mutex::new(Vec::new()),
            tools: Vec::new(),
            event_buffer: 1024,
            artifact_store: None,
        }
    }

    /// Registers `tools` on every child builder.
    pub fn with_tools(mut self, tools: Vec<Arc<dyn Tool>>) -> Self {
        self.tools = tools;
        self
    }

    /// Sets the child event buffer. Control-path conformance uses a capacity
    /// of one to prove returned input does not depend on observer delivery.
    pub fn with_event_buffer(mut self, event_buffer: usize) -> Self {
        self.event_buffer = event_buffer;
        self
    }

    /// Enables standard oversized-output offloading and explicit child-result
    /// ownership transfer through one protected store.
    pub fn with_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
    }

    /// The provider handed to child `index`, for request inspection.
    pub fn provider(&self, index: usize) -> Arc<FakeProvider> {
        self.providers.lock().expect("providers poisoned")[index].clone()
    }
}

impl ChildRuntimeFactory for ScriptedChildFactory {
    fn child_builder(&self, _spec: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError> {
        let script = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .ok_or_else(|| RuntimeError::config("no script left for another child"))?;
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            script,
        ));
        self.providers
            .lock()
            .expect("providers poisoned")
            .push(provider.clone());
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider)
            .model_profile(profile())
            .event_buffer(self.event_buffer);
        for tool in &self.tools {
            builder = builder.tool(tool.clone());
        }
        // Effectful test tools rely on the compatibility authority so the
        // child builder can seal; scoping happens after this returns.
        if self
            .tools
            .iter()
            .any(|tool| !tool.spec().permission_upper_bound.is_empty())
        {
            builder = builder.legacy_approval_authority();
        }
        if let Some(store) = &self.artifact_store {
            let offloader = ArtifactOffloader::new(store.clone())
                .with_threshold_bytes(256)?
                .with_preview_chars(128)?;
            builder = builder.tool_output_processor(Arc::new(offloader));
        }
        Ok(builder)
    }

    fn artifact_store(&self) -> Option<Arc<dyn ArtifactStore>> {
        self.artifact_store.clone()
    }
}

#[derive(Debug, Default)]
struct DelegationArtifactState {
    next_id: usize,
    values: BTreeMap<ArtifactId, (ArtifactRef, Vec<u8>)>,
    idempotency: BTreeMap<String, ArtifactId>,
}

/// Protected in-memory artifact store used to exercise the default bounded
/// transfer implementation rather than a test-only ownership shortcut.
#[derive(Debug, Default)]
struct DelegationArtifactStore {
    state: Mutex<DelegationArtifactState>,
}

#[async_trait]
impl ArtifactStore for DelegationArtifactStore {
    async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
        let mut state = self.state.lock().expect("artifact store poisoned");
        if let Some(id) = state.idempotency.get(&write.idempotency_key) {
            let (reference, bytes) = state
                .values
                .get(id)
                .expect("idempotency index points at stored artifact");
            if bytes == &write.bytes
                && reference.media_type == write.media_type
                && reference.sensitivity == write.sensitivity
                && reference.retention == write.retention
                && reference.provenance == write.provenance
            {
                return Ok(reference.clone());
            }
            return Err(ArtifactError::Integrity {
                detail: "artifact idempotency key was reused for different content".into(),
            });
        }

        state.next_id = state.next_id.saturating_add(1);
        let id = ArtifactId::new(format!("delegation-artifact-{}", state.next_id))?;
        let reference = ArtifactRef {
            id: id.clone(),
            digest: ArtifactDigest::new("sha256", format!("{:064x}", write.bytes.len()))?,
            media_type: write.media_type,
            byte_length: write.bytes.len() as u64,
            sensitivity: write.sensitivity,
            retention: write.retention,
            provenance: write.provenance,
        };
        state.idempotency.insert(write.idempotency_key, id.clone());
        state.values.insert(id, (reference.clone(), write.bytes));
        Ok(reference)
    }

    async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
        let state = self.state.lock().expect("artifact store poisoned");
        let (reference, bytes) = state.values.get(&read.id).ok_or(ArtifactError::NotFound)?;
        if read.session != reference.provenance.session {
            return Err(ArtifactError::AccessDenied);
        }
        let start = usize::try_from(read.offset).map_err(|_| ArtifactError::InvalidRange {
            detail: "artifact offset does not fit this platform".into(),
        })?;
        if start > bytes.len() {
            return Err(ArtifactError::InvalidRange {
                detail: "artifact offset exceeds content".into(),
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

#[derive(Debug)]
struct ChildArtifactTool;

#[async_trait]
impl LegacyTool for ChildArtifactTool {
    fn name(&self) -> &str {
        "produce_child_artifact"
    }

    fn description(&self) -> &str {
        "Produce a large delegated result that the parent must recover"
    }

    fn input_schema(&self) -> Value {
        json!({"type":"object","additionalProperties":false})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(format!(
            "{}CHILD_ARTIFACT_SENTINEL{}",
            "delegated-head-".repeat(5_000),
            "-delegated-tail".repeat(5_000),
        )))
    }
}

/// A one-task, inherit-model child spec.
pub fn child_spec(task: &str) -> ChildSpec {
    ChildSpec {
        task: UserInput::text(task),
        model: ChildModelSelection::Inherit,
        limits: ChildLimits::turns(2),
        tools: ToolViewScope::All,
        workspace: WorkspacePolicy::SharedProject,
    }
}

/// A child script that answers `text` and stops.
pub fn text_child_script(text: &str) -> Vec<ScriptedStream> {
    vec![ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        usage_event(5, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])]
}

/// A child script that streams a delta then blocks until cancelled.
pub fn blocking_child_script() -> Vec<ScriptedStream> {
    vec![ScriptedStream::blocking(vec![
        ProviderStreamEvent::TextDelta {
            text: "working…".into(),
        },
    ])]
}

/// A child script whose entire answer is non-redacted reasoning — the shape
/// OpenAI-compatible thinking models (e.g. GLM) can produce.
pub fn reasoning_only_child_script(text: &str) -> Vec<ScriptedStream> {
    vec![ScriptedStream::new(vec![
        ProviderStreamEvent::ReasoningDelta {
            text: text.into(),
            redacted: false,
        },
        usage_event(5, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])]
}

#[derive(Debug)]
struct CountingEditTool {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl LegacyTool for CountingEditTool {
    fn name(&self) -> &str {
        "edit"
    }

    fn description(&self) -> &str {
        "Counts edit invocations without mutating a real workspace."
    }

    fn input_schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        Ok(ToolOutcome::text("edited"))
    }
}

fn questionnaire_arguments() -> Value {
    json!({
        "questions": [{
            "id": "implementation",
            "header": "Implementation",
            "prompt": "Which implementation should be used?",
            "choices": [
                {
                    "id": "recommended",
                    "label": "Recommended",
                    "description": "Use the recommended implementation"
                },
                {
                    "id": "alternate",
                    "label": "Alternate"
                }
            ],
            "allow_free_form": true
        }],
        "sensitivity": "sensitive"
    })
}

const RENAMED_QUESTIONNAIRE_TOOL_NAME: &str = "request_task_details";

/// Proves the child-return seam is defined by the typed interaction contract,
/// not by the standard questionnaire tool's well-known name.
#[derive(Debug)]
struct RenamedQuestionnaireTool;

#[async_trait]
impl Tool for RenamedQuestionnaireTool {
    fn spec(&self) -> ToolSpec {
        let mut spec = QuestionnaireTool::new().spec();
        spec.name = RENAMED_QUESTIONNAIRE_TOOL_NAME.to_owned();
        spec
    }

    fn supports_interaction(&self) -> bool {
        true
    }

    fn interaction_request(
        &self,
        prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        QuestionnaireTool::new().interaction_request(prepared, origin, deadline)
    }

    fn resolve_interaction(
        &self,
        prepared: &PreparedToolCall,
        response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        QuestionnaireTool::new().resolve_interaction(prepared, response)
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        QuestionnaireTool::new().invoke(prepared, ctx).await
    }
}

fn read_ask_edit_script() -> Vec<ScriptedStream> {
    let mut events = Vec::new();
    events.extend(tool_call_fragments(0, "call-read", "echo", "{}"));
    events.extend(tool_call_fragments(
        1,
        "call-question",
        RENAMED_QUESTIONNAIRE_TOOL_NAME,
        &questionnaire_arguments().to_string(),
    ));
    events.extend(tool_call_fragments(2, "call-edit", "edit", "{}"));
    events.push(usage_event(12, 2));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    vec![ScriptedStream::new(events)]
}

#[derive(Debug)]
struct GatedAskProvider {
    gate: Option<Arc<Notify>>,
    entered: Option<Arc<AtomicBool>>,
}

#[async_trait]
impl Provider for GatedAskProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        Vec::new()
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        _request: agent_runtime_core::provider::ProviderRequest,
        _ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        if let Some(entered) = &self.entered {
            entered.store(true, Ordering::Release);
        }
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        let mut events = tool_call_fragments(
            0,
            "call-question",
            QUESTIONNAIRE_TOOL_NAME,
            &questionnaire_arguments().to_string(),
        );
        events.push(ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        });
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}

#[derive(Debug)]
struct ReverseArrivalFactory {
    next: AtomicUsize,
    first_gate: Arc<Notify>,
    first_entered: Arc<AtomicBool>,
}

impl ChildRuntimeFactory for ReverseArrivalFactory {
    fn child_builder(&self, _spec: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError> {
        let index = self.next.fetch_add(1, Ordering::AcqRel);
        let provider = if index == 0 {
            GatedAskProvider {
                gate: Some(self.first_gate.clone()),
                entered: Some(self.first_entered.clone()),
            }
        } else {
            GatedAskProvider {
                gate: None,
                entered: None,
            }
        };
        Ok(RuntimeBuilder::new(ModelId::new("fake"))
            .provider(Arc::new(provider))
            .model_profile(profile())
            .event_buffer(1)
            .tool(Arc::new(QuestionnaireTool::new())))
    }
}

/// A child `[read, typed-interaction, edit]` parallel batch completes one fully paired
/// exchange, returns exact input without a root broker, and never invokes the
/// suffix edit. The outcome remains available to idempotent host waiters while
/// automatic delivery drains exactly once, even with a one-event observer
/// buffer.
pub async fn assert_returned_input_pairs_and_is_lossless() {
    let (_runtime, parent) = parent_session(true).await;
    let edit_invocations = Arc::new(AtomicUsize::new(0));
    let mut child_script = read_ask_edit_script();
    child_script.push(text_child_script("continued after explicit follow-up").remove(0));
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![child_script])
            .with_event_buffer(1)
            .with_tools(vec![
                Arc::new(EchoTool),
                Arc::new(RenamedQuestionnaireTool),
                Arc::new(CountingEditTool {
                    invocations: edit_invocations.clone(),
                }),
            ]),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let outcome = coordinator
        .spawn(child_spec("inspect, clarify, then edit"))
        .await
        .unwrap();
    let (child, handle) = match outcome {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let first = coordinator.wait_task_outcome(&child).await.unwrap();
    let request = match first {
        ChildTaskOutcome::NeedsInput {
            child: outcome_child,
            request,
        } => {
            assert_eq!(outcome_child, child);
            request
        }
        other => panic!("expected returned child input, got {other:?}"),
    };
    assert_eq!(request.origin().session(), handle.id());
    assert_eq!(
        coordinator.wait_task_outcome(&child).await.unwrap(),
        ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        },
        "host waits are idempotent and cannot race automatic delivery"
    );

    let delivered = coordinator.take_ready_task_outcomes();
    assert_eq!(
        delivered,
        [ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        }]
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert!(matches!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::NeedsInput { .. })
    ));

    let blocks = handle
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            agent_runtime_core::content::ContentPart::ToolResult(block) => Some(block),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.name.as_str())
            .collect::<Vec<_>>(),
        ["echo", RENAMED_QUESTIONNAIRE_TOOL_NAME, "edit"]
    );
    assert!(!blocks[0].is_error);
    assert!(!blocks[1].is_error);
    assert!(blocks[2].is_error);
    assert!(
        blocks[2]
            .content
            .iter()
            .filter_map(agent_runtime_core::content::ContentPart::as_text)
            .any(|text| text.contains("skipped"))
    );
    assert_eq!(edit_invocations.load(Ordering::Acquire), 0);
    assert_eq!(
        factory.provider(0).requests().len(),
        1,
        "NeedsInput must not issue a second child provider request"
    );

    coordinator
        .follow_up(
            &child,
            UserInput::text("Use the recommended implementation"),
        )
        .await
        .unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(
        status.last_result.as_deref(),
        Some("continued after explicit follow-up")
    );
    assert!(matches!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::Completed { ref result, .. })
            if result.text == "continued after explicit follow-up"
    ));
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::Completed {
            child: child.clone(),
            result: ChildTaskResult {
                text: "continued after explicit follow-up".to_owned(),
                artifacts: Vec::new(),
            },
        }],
        "explicit follow-up must clear stale input and deliver its own completion once"
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert_eq!(factory.provider(0).requests().len(), 2);
}

/// Concurrent returned-input arrivals are delivered in canonical
/// `(child_id, request_id)` order even when child two arrives first, and a
/// simultaneous host waiter cannot consume the automatic delivery.
pub async fn assert_returned_input_reverse_arrival_is_canonical() {
    let (_runtime, parent) = parent_session(true).await;
    let first_gate = Arc::new(Notify::new());
    let first_entered = Arc::new(AtomicBool::new(false));
    let factory = Arc::new(ReverseArrivalFactory {
        next: AtomicUsize::new(0),
        first_gate: first_gate.clone(),
        first_entered: first_entered.clone(),
    });
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let first = coordinator.spawn(child_spec("first")).await.unwrap();
    let first_child = match first {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected first child, got {other:?}"),
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !first_entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let second = coordinator.spawn(child_spec("second")).await.unwrap();
    let second_child = match second {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected second child, got {other:?}"),
    };
    let second_outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        coordinator.wait_task_outcome(&second_child),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        second_outcome,
        ChildTaskOutcome::NeedsInput { .. }
    ));

    first_gate.notify_one();
    let first_outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        coordinator.wait_task_outcome(&first_child),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(first_outcome, ChildTaskOutcome::NeedsInput { .. }));

    let (automatic, first_read, second_read) = tokio::join!(
        coordinator.wait_ready_task_outcomes(),
        coordinator.wait_task_outcome(&first_child),
        coordinator.wait_task_outcome(&second_child),
    );
    let automatic = automatic.unwrap();
    assert_eq!(
        automatic
            .iter()
            .map(|outcome| match outcome {
                ChildTaskOutcome::NeedsInput { child, .. }
                | ChildTaskOutcome::Completed { child, .. } => child.as_str(),
            })
            .collect::<Vec<_>>(),
        [first_child.as_str(), second_child.as_str()]
    );
    assert!(matches!(
        first_read.unwrap(),
        ChildTaskOutcome::NeedsInput { .. }
    ));
    assert!(matches!(
        second_read.unwrap(),
        ChildTaskOutcome::NeedsInput { .. }
    ));
}

/// Spawn one child and assert the parent stream carries the ordered,
/// attributed lifecycle — spawned, turn-started progress, turn-finished
/// progress, completed — with the final result intact on the completed event.
pub async fn assert_spawn_lifecycle_and_result() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "child answer",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let mut phases = Vec::new();
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildSpawned {
                child: id,
                workspace,
                max_turns,
                ..
            } => {
                assert_eq!(id, child);
                assert_eq!(workspace, WorkspacePolicy::SharedProject);
                assert_eq!(max_turns, 2);
                phases.push("spawned");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnStarted,
            } => {
                assert_eq!(id, child);
                phases.push("turn_started");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnFinished,
            } => {
                assert_eq!(id, child);
                phases.push("turn_finished");
            }
            RuntimeEvent::ChildCompleted { child: id, result } => {
                assert_eq!(id, child);
                assert_eq!(
                    result, "child answer",
                    "the final result must ride the event"
                );
                phases.push("completed");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        phases,
        ["spawned", "turn_started", "turn_finished", "completed"],
        "child lifecycle events must arrive attributed and in order"
    );

    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(status.last_result.as_deref(), Some("child answer"));
    assert_eq!(
        coordinator.result(&child).unwrap().as_deref(),
        Some("child answer")
    );
}

/// A child-produced artifact is copied explicitly into parent ownership,
/// retains source lineage, and remains recoverable only under the new owner.
pub async fn assert_child_artifact_result_transfers_to_parent() {
    let (_runtime, parent) = parent_session(true).await;
    let mut call = tool_call_fragments(0, "call-child-artifact", "produce_child_artifact", "{}");
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let store = Arc::new(DelegationArtifactStore::default());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![
            ScriptedStream::new(call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "artifact ready".into(),
                },
                usage_event(8, 2),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ]])
        .with_tools(vec![Arc::new(ChildArtifactTool)])
        .with_artifact_store(store.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let (child, handle) = match coordinator
        .spawn(child_spec("produce a large result"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let result = match coordinator.wait_task_outcome(&child).await.unwrap() {
        ChildTaskOutcome::Completed {
            child: outcome_child,
            result,
        } => {
            assert_eq!(outcome_child, child);
            result
        }
        other => panic!("expected a completed artifact result, got {other:?}"),
    };
    assert_eq!(result.text, "artifact ready");
    assert_eq!(result.artifacts.len(), 1);

    let transferred = &result.artifacts[0];
    assert_eq!(transferred.provenance.session, *parent.id());
    assert_eq!(transferred.provenance.purpose, "delegation.child-result");
    let lineage = transferred
        .provenance
        .derived_from
        .as_ref()
        .expect("parent reference preserves child lineage");
    assert_eq!(lineage.session, *handle.id());
    assert_ne!(lineage.id, transferred.id);
    assert_eq!(lineage.digest, transferred.digest);

    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_artifacts, result.artifacts);
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::Completed {
            child: child.clone(),
            result: result.clone(),
        }]
    );

    let mut bytes = Vec::new();
    let mut offset = 0u64;
    while offset < transferred.byte_length {
        let chunk = store
            .read(ArtifactRead {
                session: parent.id().clone(),
                id: transferred.id.clone(),
                offset,
                limit: MAX_ARTIFACT_READ_BYTES,
            })
            .await
            .unwrap();
        bytes.extend_from_slice(&chunk.bytes);
        offset = chunk.next_offset.unwrap_or(transferred.byte_length);
    }
    assert_eq!(bytes.len() as u64, transferred.byte_length);
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains("CHILD_ARTIFACT_SENTINEL")
    );
    assert_eq!(
        store
            .read(ArtifactRead {
                session: handle.id().clone(),
                id: transferred.id.clone(),
                offset: 0,
                limit: 1,
            })
            .await
            .unwrap_err(),
        ArtifactError::AccessDenied,
        "the copied parent reference grants no authority back to the child"
    );
}

/// A child whose provider classified its whole answer as reasoning still
/// completes with a non-empty result carrying that reasoning text.
pub async fn assert_reasoning_only_result_survives() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        reasoning_only_child_script("the diff is sound"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    while let Some(env) = parent_events.next().await {
        if let RuntimeEvent::ChildCompleted { child: id, result } = env.payload {
            assert_eq!(id, child);
            assert_eq!(
                result, "the diff is sound",
                "a reasoning-only answer must not become an empty result"
            );
            break;
        }
    }
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("the diff is sound"));
}

/// An approval-gated spawn shows the deciding surface what it is deciding:
/// the child task summary and the narrowing it would run under.
pub async fn assert_approval_sees_the_spawn_detail() {
    use agent_runtime_core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};

    /// Allows and captures every request it is shown.
    #[derive(Debug)]
    struct CapturingApproval {
        seen: Mutex<Vec<ApprovalRequest>>,
    }

    #[async_trait]
    impl ApprovalPolicy for CapturingApproval {
        async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
            self.seen
                .lock()
                .expect("seen poisoned")
                .push(request.clone());
            ApprovalDecision::Allow
        }
    }

    /// Answers `RequireApproval` for everything it covers, like a host's
    /// delegation authority routing through its approval surface.
    #[derive(Debug)]
    struct RequireApprovalCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
    }

    #[async_trait]
    impl SecurityCheck for RequireApprovalCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            _request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            }
        }
    }

    let approval = Arc::new(CapturingApproval {
        seen: Mutex::new(Vec::new()),
    });
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .approval(approval.clone())
        .security_check(
            Arc::new(RequireApprovalCheck {
                id: SecurityCheckId::new("require-approval"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("parent runtime builds");
    let parent = runtime
        .start_session(StartSession::new())
        .await
        .expect("parent session starts");

    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("done")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let outcome = coordinator
        .spawn(child_spec("summarize the auth module"))
        .await
        .expect("an approved spawn");
    assert!(matches!(outcome, SpawnOutcome::Spawned { .. }));

    let seen = approval.seen.lock().expect("seen poisoned").clone();
    let request = seen
        .iter()
        .find(|request| request.prepared().tool() == "delegation.spawn")
        .expect("the spawn was routed through approval");
    let rendered = request.prepared().arguments().to_string();
    assert!(
        rendered.contains("summarize the auth module"),
        "approval must see the child task: {rendered}"
    );
    assert!(
        rendered.contains("workspace") && rendered.contains("tools"),
        "approval must see the child's narrowing: {rendered}"
    );
}

/// A coordinator cannot be created for a child session, so a spawn-shaped
/// call from a child is rejected as a depth violation and no grandchild
/// exists.
pub async fn assert_depth_violation() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        text_child_script("done"),
        text_child_script("never runs"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let (child, handle) = match outcome {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();

    let err = DelegationCoordinator::new(&handle, factory, DelegationConfig::default())
        .expect_err("a child session must not be able to manage children");
    assert!(
        err.message.contains("depth"),
        "the rejection must identify a depth violation: {}",
        err.message
    );
}

/// Without authoritative coverage for `agent.delegate`, spawn is denied
/// fail-closed and no child session or lifecycle event is created.
pub async fn assert_spawn_denied_without_coverage() {
    let (_runtime, parent) = parent_session(false).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let err = coordinator
        .spawn(child_spec("task"))
        .await
        .expect_err("delegation without coverage must be denied");
    assert!(
        err.message.contains("denied"),
        "the denial must be structured: {}",
        err.message
    );
    assert!(
        coordinator.list().is_empty(),
        "a denied spawn must not create a child"
    );
}

/// A structurally invalid spec is rejected with no side effects.
pub async fn assert_invalid_spec_rejected() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut spec = child_spec("task");
    spec.limits.max_turns = 0;
    let err = coordinator.spawn(spec).await.expect_err("invalid spec");
    assert!(err.message.contains("turn"), "{}", err.message);
    assert!(coordinator.list().is_empty());
}

/// At the per-parent cap under the reject policy, spawn returns a structured
/// capacity result and the cap is not exceeded.
pub async fn assert_capacity_reject() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        blocking_child_script(),
        text_child_script("never"),
    ]));
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory,
        DelegationConfig {
            limits: DelegationLimits {
                max_running_children: 1,
            },
            capacity_policy: CapacityPolicy::Reject,
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let first = coordinator.spawn(child_spec("long task")).await.unwrap();
    assert!(matches!(first, SpawnOutcome::Spawned { .. }));

    let second = coordinator.spawn(child_spec("one too many")).await.unwrap();
    match second {
        SpawnOutcome::AtCapacity { running, limit } => {
            assert_eq!(running, 1);
            assert_eq!(limit, 1);
        }
        other => panic!("expected a capacity result, got {other:?}"),
    }
    assert_eq!(
        coordinator.list().len(),
        1,
        "the capacity result must not have created a child"
    );
}

/// Stopping a child mid-stream propagates cancellation into its provider
/// stream and produces exactly one terminal stopped event.
pub async fn assert_stop_cancels_running_child() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let status = coordinator.stop(&child).await.unwrap();
    assert!(
        matches!(status.state, ChildState::Stopped { .. }),
        "stop must resolve a terminal stopped state, got {:?}",
        status.state
    );

    // Exactly one terminal stopped event for this child on the parent stream.
    let mut stopped = 0;
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildStopped { child: id, .. } if id == child => {
                stopped += 1;
                // Drain briefly: any duplicate would already be queued.
                let drain = async {
                    while let Some(env) = parent_events.next().await {
                        if matches!(
                            &env.payload,
                            RuntimeEvent::ChildStopped { child: id, .. } if *id == child
                        ) {
                            return true;
                        }
                    }
                    false
                };
                let duplicate = tokio::time::timeout(std::time::Duration::from_millis(200), drain)
                    .await
                    .unwrap_or(false);
                assert!(!duplicate, "a child must emit exactly one terminal event");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(stopped, 1);
}

/// A read-only tool-view scope (and read-only workspace posture) leaves the
/// child's advertised tools without write-capable entries, and the host's
/// delegation-facing tools never reach a child view.
pub async fn assert_scoped_view_excludes_write_and_delegation_tools() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("scoped")]).with_tools(vec![
            Arc::new(EchoTool),
            Arc::new(WriteTool::new("/ws/out")),
            Arc::new(crate::tools::named_echo("delegate_task")),
        ]),
    );
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory.clone(),
        DelegationConfig {
            delegation_tool_names: vec!["delegate_task".to_string()],
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let mut spec = child_spec("read-only review");
    spec.tools = ToolViewScope::ReadOnly;
    spec.workspace = WorkspacePolicy::ReadOnlyView;
    let outcome = coordinator.spawn(spec).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);

    let requests = factory.provider(0).requests();
    assert!(!requests.is_empty());
    let names: Vec<&str> = requests[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        ["echo"],
        "the child view must retain only scoped read tools"
    );
}

/// A completed child accepts a follow-up under its original limits, and the
/// turn cap is enforced with a structured limit error.
pub async fn assert_follow_up_and_turn_limit() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![vec![
        text_child_script("first").remove(0),
        text_child_script("second").remove(0),
    ]]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("first"));
    assert_eq!(status.turns_used, 1);

    let (first_follow_up, competing_follow_up) = tokio::join!(
        coordinator.follow_up(&child, UserInput::text("continue")),
        coordinator.follow_up(&child, UserInput::text("competing continuation")),
    );
    assert_eq!(
        usize::from(first_follow_up.is_ok()) + usize::from(competing_follow_up.is_ok()),
        1,
        "the final child-turn slot must be reserved atomically"
    );
    let rejected = first_follow_up
        .err()
        .or_else(|| competing_follow_up.err())
        .expect("one concurrent follow-up is rejected");
    assert!(rejected.message.contains("turn limit"), "{rejected:?}");
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("second"));
    assert_eq!(status.turns_used, 2);

    let err = coordinator
        .follow_up(&child, UserInput::text("a third task"))
        .await
        .expect_err("the turn cap must reject a third task");
    assert!(err.message.contains("turn limit"), "{}", err.message);
}

/// Children stop when the parent session shuts down and never restart.
pub async fn assert_parent_teardown_stops_children() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    parent.shutdown().await.unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert!(
        status.state.is_terminal(),
        "children must stop with their parent, got {:?}",
        status.state
    );
}
