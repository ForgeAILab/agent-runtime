use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::{Barrier, Notify};

use agent_runtime::delegation::{
    CHILD_CATALOG_NAMESPACE, CapacityPolicy, ChildCompletionAdmission,
    ChildCompletionAdmissionRequest, ChildDurability, ChildRuntimeFactory, ChildSessionRecord,
    ChildState, ChildStatus, ChildTaskOutcome, ChildTaskResult, DEFAULT_DELEGATION_WAIT,
    DELEGATION_PERMISSION, DelegationConfig, DelegationCoordinator, DelegationLimits,
    DurableChildCatalog, DurableChildSpec, HARD_MAX_DELEGATION_WAIT, SpawnOutcome,
};
use agent_runtime::harness::{ArtifactOffloader, QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::registry::{Fingerprint, Permission};
use agent_runtime::runtime::{Runtime, RuntimeBuilder, SessionHandle, StartSession};
use agent_runtime_core::artifact::{
    ArtifactChunk, ArtifactDigest, ArtifactError, ArtifactId, ArtifactLineage, ArtifactProvenance,
    ArtifactRead, ArtifactRef, ArtifactRetention, ArtifactSensitivity, ArtifactStore,
    ArtifactTransfer, ArtifactWrite, MAX_ARTIFACT_READ_BYTES,
};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::check_set::ActionClass;
use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::clock::{Clock, Deadline, SystemClock, Timestamp};
use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::delegation::{
    ChildLimits, ChildModelSelection, ChildSpec, ToolViewScope, WorkspacePolicy,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{ChildPhase, RuntimeEvent};
use agent_runtime_core::grant::{
    GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
    SecurityCheckRevision,
};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::interaction::{InteractionOrigin, InteractionRequest, InteractionResponse};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
    ProviderError, ProviderRequest, ProviderStream, ProviderStreamEvent,
};
use agent_runtime_core::security::{AuthorizationRequest, PermissionSet};
use agent_runtime_core::store::{
    SessionIdentityState, SessionSnapshot, SessionStore, VersionedSessionState,
};
use agent_runtime_core::tool::{
    InvocationContext, LegacyTool, PreparedToolCall, Tool, ToolContent, ToolEffects, ToolOutcome,
    ToolSpec,
};
use agent_runtime_core::usage::UsageLedger;

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
    parent_session_with_provider_and_clock(
        covered,
        Arc::new(FakeProvider::text_reply("parent")),
        Arc::new(SystemClock),
    )
    .await
}

/// A parent runtime and session using an injected clock.  Delegation wait
/// tests use this seam so a configured timeout can be advanced without
/// sleeping for the production default.
pub async fn parent_session_with_clock(
    covered: bool,
    clock: Arc<dyn Clock>,
) -> (Runtime, SessionHandle) {
    parent_session_with_provider_and_clock(
        covered,
        Arc::new(FakeProvider::text_reply("parent")),
        clock,
    )
    .await
}

/// A parent runtime and session using a test-supplied provider.  Admission
/// race fixtures use a blocking provider so the winning user turn remains
/// active while the lower-priority child continuation evaluates the boundary.
pub async fn parent_session_with_provider(
    covered: bool,
    provider: Arc<dyn Provider>,
) -> (Runtime, SessionHandle) {
    parent_session_with_provider_and_clock(covered, provider, Arc::new(SystemClock)).await
}

async fn parent_session_with_provider_and_clock(
    covered: bool,
    provider: Arc<dyn Provider>,
    clock: Arc<dyn Clock>,
) -> (Runtime, SessionHandle) {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock);
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

async fn durable_parent_session(
    id: &str,
    session_store: Arc<dyn SessionStore>,
    checkpoint_store: Arc<dyn CheckpointStore>,
) -> (Runtime, SessionHandle) {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .session_store(session_store)
        .checkpoint_store(checkpoint_store)
        .security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("durable parent runtime builds");
    let session = runtime
        .start_session(StartSession::new().with_id(agent_runtime_core::ids::SessionId::new(id)))
        .await
        .expect("durable parent session starts");
    (runtime, session)
}

/// Parent-scoped SessionStore failure injection used by bind rollback tests.
/// Child-session writes pass through normally, so the failure lands on the
/// parent catalog save after a provider/runtime has been reconstructed.
#[derive(Debug)]
pub struct FailNextParentSessionStore {
    pub inner: Arc<crate::InMemorySessionStore>,
    parent: agent_runtime_core::ids::SessionId,
    remaining_failures: AtomicUsize,
}

impl FailNextParentSessionStore {
    pub fn new(
        inner: Arc<crate::InMemorySessionStore>,
        parent: agent_runtime_core::ids::SessionId,
    ) -> Self {
        Self {
            inner,
            parent,
            remaining_failures: AtomicUsize::new(0),
        }
    }

    pub fn fail_next_parent_save(&self) {
        self.fail_parent_saves(1);
    }

    pub fn fail_parent_saves(&self, count: usize) {
        self.remaining_failures.store(count, Ordering::Release);
    }

    pub fn clear_failures(&self) {
        self.remaining_failures.store(0, Ordering::Release);
    }
}

#[async_trait]
impl SessionStore for FailNextParentSessionStore {
    async fn load(
        &self,
        id: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let should_fail = snapshot.id == self.parent
            && self
                .remaining_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    if remaining > 0 {
                        Some(remaining - 1)
                    } else {
                        None
                    }
                })
                .is_ok();
        if should_fail {
            return Err(RuntimeError::conflict(
                "injected parent SessionStore save failure",
            ));
        }
        self.inner.save(snapshot).await
    }
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
    session_store: Option<Arc<dyn SessionStore>>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    policy_salt: String,
    policy_fingerprint_failures: AtomicUsize,
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
            session_store: None,
            checkpoint_store: None,
            policy_salt: "test-child-policy-v1".to_owned(),
            policy_fingerprint_failures: AtomicUsize::new(0),
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

    /// Enables durable child snapshots and exact checkpoints.
    pub fn with_durable_stores(
        mut self,
        session_store: Arc<dyn SessionStore>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Self {
        self.session_store = Some(session_store);
        self.checkpoint_store = Some(checkpoint_store);
        self
    }

    /// Changes the host reconstruction fingerprint for incompatibility tests.
    pub fn with_policy_salt(mut self, salt: impl Into<String>) -> Self {
        self.policy_salt = salt.into();
        self
    }

    /// Fails the next `count` policy fingerprint calls before allowing normal
    /// child construction. Used to prove admission capacity is released on a
    /// pre-construction policy error.
    pub fn with_policy_fingerprint_failures(self, count: usize) -> Self {
        self.policy_fingerprint_failures
            .store(count, Ordering::Release);
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
        if let Some(store) = &self.session_store {
            builder = builder.session_store(store.clone());
        }
        if let Some(store) = &self.checkpoint_store {
            builder = builder.checkpoint_store(store.clone());
        }
        Ok(builder)
    }

    fn artifact_store(&self) -> Option<Arc<dyn ArtifactStore>> {
        self.artifact_store.clone()
    }

    fn session_store(&self) -> Option<Arc<dyn SessionStore>> {
        self.session_store.clone()
    }

    fn checkpoint_store(&self) -> Option<Arc<dyn CheckpointStore>> {
        self.checkpoint_store.clone()
    }

    fn policy_fingerprint(
        &self,
        spec: &agent_runtime::delegation::DurableChildSpec,
    ) -> Result<Fingerprint, RuntimeError> {
        if self
            .policy_fingerprint_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                if remaining > 0 {
                    Some(remaining - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(RuntimeError::conflict("test policy fingerprint failure"));
        }
        let encoded = serde_json::to_vec(&(self.policy_salt.as_str(), spec)).unwrap();
        Ok(Fingerprint::of_fields([encoded.as_slice()]))
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

impl DelegationArtifactStore {
    fn artifact_count(&self) -> usize {
        self.state
            .lock()
            .expect("artifact store poisoned")
            .values
            .len()
    }
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

#[derive(Debug, Clone, Copy)]
enum MaliciousTransferMutation {
    Digest,
    ByteLength,
    MediaType,
    Sensitivity,
    Retention,
    InvalidMetadata,
    MissingLineage,
    WrongLineage,
    WrongOwner,
}

impl MaliciousTransferMutation {
    const ALL: [Self; 9] = [
        Self::Digest,
        Self::ByteLength,
        Self::MediaType,
        Self::Sensitivity,
        Self::Retention,
        Self::InvalidMetadata,
        Self::MissingLineage,
        Self::WrongLineage,
        Self::WrongOwner,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Digest => "digest",
            Self::ByteLength => "byte length",
            Self::MediaType => "media type",
            Self::Sensitivity => "sensitivity",
            Self::Retention => "retention",
            Self::InvalidMetadata => "validity",
            Self::MissingLineage => "missing lineage",
            Self::WrongLineage => "wrong lineage",
            Self::WrongOwner => "ownership",
        }
    }
}

/// Store whose custom transfer override returns metadata that the default
/// transfer implementation would reject. Delegation must validate the
/// returned reference before persisting or publishing the child outcome.
#[derive(Debug)]
struct MaliciousTransferArtifactStore {
    inner: DelegationArtifactStore,
    mutation: MaliciousTransferMutation,
}

impl MaliciousTransferArtifactStore {
    fn new(mutation: MaliciousTransferMutation) -> Self {
        Self {
            inner: DelegationArtifactStore::default(),
            mutation,
        }
    }
}

#[async_trait]
impl ArtifactStore for MaliciousTransferArtifactStore {
    async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
        self.inner.put(write).await
    }

    async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
        self.inner.read(read).await
    }

    async fn transfer(&self, transfer: ArtifactTransfer) -> Result<ArtifactRef, ArtifactError> {
        let mut reference = self.inner.transfer(transfer).await?;
        match self.mutation {
            MaliciousTransferMutation::Digest => {
                reference.digest.hex = "f".repeat(64);
            }
            MaliciousTransferMutation::ByteLength => {
                reference.byte_length = reference.byte_length.saturating_add(1);
            }
            MaliciousTransferMutation::MediaType => {
                reference.media_type = "text/malicious".into();
            }
            MaliciousTransferMutation::Sensitivity => {
                reference.sensitivity = ArtifactSensitivity::Public;
            }
            MaliciousTransferMutation::Retention => {
                reference.retention = ArtifactRetention::HostPolicy;
            }
            MaliciousTransferMutation::InvalidMetadata => {
                reference.media_type.clear();
            }
            MaliciousTransferMutation::MissingLineage => {
                reference.provenance.derived_from = None;
            }
            MaliciousTransferMutation::WrongLineage => {
                reference.provenance.derived_from = Some(ArtifactLineage {
                    session: SessionId::new("attacker-session"),
                    id: reference.id.clone(),
                    digest: reference.digest.clone(),
                });
            }
            MaliciousTransferMutation::WrongOwner => {
                reference.provenance.session = SessionId::new("attacker-session");
            }
        }
        Ok(reference)
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

/// A tool-supplied artifact reference from another session.  The runtime must
/// reject it before the child can publish a completed result or ready outcome.
#[derive(Debug)]
struct ForeignArtifactTool;

#[async_trait]
impl LegacyTool for ForeignArtifactTool {
    fn name(&self) -> &str {
        "produce_foreign_artifact"
    }

    fn description(&self) -> &str {
        "Return an artifact reference owned by another session"
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
        let reference = ArtifactRef {
            id: ArtifactId::new("foreign-artifact").expect("fixture artifact id"),
            digest: ArtifactDigest::new("sha256", "00").expect("fixture digest"),
            media_type: "text/plain".into(),
            byte_length: 1,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: ArtifactProvenance::new(SessionId::new("foreign-session"), "tool-output")
                .with_turn(TurnId::new("turn-1")),
        };
        Ok(ToolOutcome {
            value: json!({"artifact": reference.id.as_str()}),
            content: ToolContent::Artifact {
                preview: vec![ContentPart::text("foreign artifact")],
                reference,
                media_type: "text/plain".into(),
                byte_length: 1,
            },
            is_error: false,
        })
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
            signature: None,
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
