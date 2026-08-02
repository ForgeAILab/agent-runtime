use super::*;

pub trait ChildRuntimeFactory: Send + Sync + fmt::Debug {
    /// A builder for the child described by `spec`.
    fn child_builder(&self, spec: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError>;

    /// Protected artifact store shared by the parent/child composition.
    ///
    /// Returning a store does not widen `artifact.read`; the coordinator uses
    /// it only for explicit child-to-parent ownership transfer after a typed
    /// reference is observed from the exact child turn.
    fn artifact_store(&self) -> Option<Arc<dyn ArtifactStore>> {
        None
    }

    /// Redacted canonical child snapshots, when child continuity is durable.
    fn session_store(&self) -> Option<Arc<dyn SessionStore>> {
        None
    }

    /// Protected exact child checkpoints, when interrupted turns can resume.
    fn checkpoint_store(&self) -> Option<Arc<dyn CheckpointStore>> {
        None
    }

    /// Stable fingerprint of the host policy that reconstructs `spec`.
    ///
    /// Hosts should include provider/model, workspace identity, tool upper
    /// bounds, trust/profile revisions, and any other value whose change could
    /// widen or materially alter a recovered child. The default is the
    /// serialized durable child specification.
    fn policy_fingerprint(&self, spec: &DurableChildSpec) -> Result<Fingerprint, RuntimeError> {
        let encoded = serde_json::to_vec(spec).map_err(|error| {
            RuntimeError::new(
                ErrorKind::Serialization,
                format!("child policy could not be fingerprinted: {error}"),
            )
        })?;
        Ok(Fingerprint::of_fields([encoded.as_slice()]))
    }

    /// Whether this composition can restore both canonical and exact child
    /// state. Hosts missing either store remain explicitly ephemeral.
    fn durability(&self) -> ChildDurability {
        if self.session_store().is_some() && self.checkpoint_store().is_some() {
            ChildDurability::Durable
        } else {
            ChildDurability::Ephemeral
        }
    }
}

/// Deterministic caps on delegated children.
#[derive(Debug, Clone)]
pub struct DelegationLimits {
    /// The maximum children of this parent running (or idle-but-alive) at
    /// once.
    pub max_running_children: usize,
    /// Maximum durable child records retained for one parent.
    pub max_retained_children: usize,
    /// Optional retention age for idle/interrupted durable children.
    pub retention_ms: Option<u64>,
}

/// What happens when a spawn arrives at capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CapacityPolicy {
    /// Return a structured capacity result (the default).
    #[default]
    Reject,
    /// Queue up to `max_pending` spawns and start them as slots free.
    Queue {
        /// The maximum queued spawns.
        max_pending: usize,
    },
}

/// A shared capacity pool. Hosts that want one process-wide cap across
/// several coordinators (parents) share one of these.
#[derive(Debug)]
pub struct DelegationCapacity {
    limit: usize,
    running: AtomicU64,
}

impl DelegationCapacity {
    /// A pool admitting at most `limit` concurrent children.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            running: AtomicU64::new(0),
        })
    }

    pub(super) fn try_acquire(&self) -> bool {
        let mut current = self.running.load(Ordering::SeqCst);
        loop {
            if current as usize >= self.limit {
                return false;
            }
            match self.running.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    pub(super) fn release(&self) {
        // Saturating: release is only called after a successful acquire.
        let _ = self
            .running
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

/// Coordinator configuration.
#[derive(Debug, Clone, Default)]
pub struct DelegationConfig {
    /// Per-parent caps.
    pub limits: DelegationLimits,
    /// What happens at capacity.
    pub capacity_policy: CapacityPolicy,
    /// An optional shared (e.g. process-wide) capacity pool.
    pub shared_capacity: Option<Arc<DelegationCapacity>>,
    /// The names of the host's delegation-facing tools. Always excluded from
    /// child tool views, whatever the spec's scope, so a child can never see
    /// spawn/stop operations.
    pub delegation_tool_names: Vec<String>,
}

impl Default for DelegationLimits {
    fn default() -> Self {
        Self {
            max_running_children: 4,
            max_retained_children: 64,
            retention_ms: None,
        }
    }
}

/// Whether one child can survive loss of its owning process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildDurability {
    /// The child exists only while its current coordinator/process is alive.
    Ephemeral,
    /// Canonical state and exact checkpoints can be rebound after restart.
    Durable,
}

/// The immutable child composition needed to rebuild an existing session.
///
/// The initial task is deliberately absent: it already belongs to canonical
/// child history and must not be duplicated into the parent catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableChildSpec {
    /// Provider/model selection.
    pub model: agent_runtime_core::delegation::ChildModelSelection,
    /// Cumulative limits.
    pub limits: agent_runtime_core::delegation::ChildLimits,
    /// Narrowed tool view.
    pub tools: ToolViewScope,
    /// Declared workspace posture.
    pub workspace: WorkspacePolicy,
}

impl DurableChildSpec {
    pub(super) fn from_spawn(spec: &ChildSpec) -> Self {
        Self {
            model: spec.model.clone(),
            limits: spec.limits,
            tools: spec.tools.clone(),
            workspace: spec.workspace.clone(),
        }
    }

    pub(super) fn rebuild_spec(&self) -> ChildSpec {
        ChildSpec {
            task: UserInput::text("resume existing child session"),
            model: self.model.clone(),
            limits: self.limits,
            tools: self.tools.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

/// The lifecycle state of one child.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ChildState {
    /// Executing a task.
    Running,
    /// Completed at least one task and available for follow-ups.
    Idle,
    /// Its process-owned execution ended while durable state remained.
    Interrupted {
        /// Whether a compatible exact checkpoint was recorded.
        resumable: bool,
    },
    /// Stopped (terminal).
    Stopped {
        /// Why.
        reason: CancelReason,
    },
    /// Failed (terminal).
    Failed,
    /// Retention or absolute lifetime expired.
    Expired,
}

impl ChildState {
    /// Whether the child can do no further work.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ChildState::Stopped { .. } | ChildState::Failed | ChildState::Expired
        )
    }
}

pub(super) fn checkpoint_can_resume(state: &TurnState) -> bool {
    !matches!(
        state,
        TurnState::CallingModel { .. } | TurnState::Terminal { .. }
    )
}

/// A structured snapshot of one child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildStatus {
    /// The stable child id.
    pub child: ChildId,
    /// The parent session.
    pub parent: SessionId,
    /// The stable runtime session that owns this child's conversation.
    pub session: SessionId,
    /// Whether this child can be rebound after process loss.
    pub durability: ChildDurability,
    /// The lifecycle state.
    pub state: ChildState,
    /// The declared workspace posture.
    pub workspace: WorkspacePolicy,
    /// Tasks consumed (spawn plus follow-ups).
    pub turns_used: u32,
    /// The task cap.
    pub max_turns: u32,
    /// Cumulative provider tokens attributed to this child.
    pub tokens_used: u64,
    /// The latest completed task's final visible answer, if any.
    pub last_result: Option<String>,
    /// Parent-owned artifact references returned with the latest completed
    /// task.
    pub last_artifacts: Vec<ArtifactRef>,
    /// Last durable lifecycle update.
    pub updated_at: Timestamp,
    /// Bounded compatibility reason when recovery cannot proceed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incompatibility: Option<String>,
}

impl ChildStatus {
    /// Whether an explicit exact-turn resume is currently available.
    pub fn resumable(&self) -> bool {
        matches!(self.state, ChildState::Interrupted { resumable: true })
            && self.incompatibility.is_none()
    }
}

/// One versioned parent-owned durable child record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildSessionRecord {
    /// Record schema.
    pub schema_version: u32,
    /// Stable parent-facing identity.
    pub child: ChildId,
    /// Stable child runtime session.
    pub child_session: SessionId,
    /// Exact owning parent.
    pub parent_session: SessionId,
    /// Immutable reconstruction inputs excluding raw task content.
    pub spec: DurableChildSpec,
    /// Host policy fingerprint captured at spawn.
    pub policy_fingerprint: Fingerprint,
    /// Durable lifecycle snapshot.
    pub status: ChildStatus,
    /// Latest exact checkpoint boundary referenced by the catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_watermark: Option<CheckpointWatermark>,
    /// Whether the recorded exact boundary is safe to continue without
    /// replaying indeterminate provider I/O or a terminal turn.
    #[serde(default)]
    pub checkpoint_resumable: bool,
    /// Monotonic record revision.
    pub revision: u64,
    /// Absolute child lifetime deadline, if configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DurableChildCatalog {
    /// Catalog schema.
    pub schema_version: u32,
    /// Last allocated numeric child suffix.
    pub next_child: u64,
    /// Parent-owned records in stable child-id order.
    pub children: Vec<ChildSessionRecord>,
}

impl DurableChildCatalog {
    /// Creates a current-version catalog.
    pub fn new(next_child: u64, children: Vec<ChildSessionRecord>) -> Self {
        Self {
            schema_version: CHILD_CATALOG_SCHEMA_VERSION,
            next_child,
            children,
        }
    }

    /// Runtime-owned extension-state revision for this catalog schema.
    pub fn revision() -> RegistryRevision {
        RegistryRevision::new(CHILD_CATALOG_REVISION)
    }
}

/// Exact typed result of one completed delegated child task.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildTaskResult {
    /// Final visible answer.
    pub text: String,
    /// Parent-owned artifacts explicitly copied from this child turn.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}

impl fmt::Debug for ChildTaskResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildTaskResult")
            .field("text_chars", &self.text.chars().count())
            .field("artifact_count", &self.artifacts.len())
            .finish()
    }
}

/// Exact typed result of one delegated child task.
///
/// `NeedsInput` retains the protected interaction request; callers must not
/// stringify it into ordinary conversation or plaintext persistence. Use
/// [`ChildTaskOutcome::model_projection`] for the bounded delivery shape.
#[derive(Clone, PartialEq)]
pub enum ChildTaskOutcome {
    /// The child completed its current task.
    Completed {
        /// Stable child identity.
        child: ChildId,
        /// Final visible result and parent-owned artifact references.
        result: ChildTaskResult,
    },
    /// The child is blocked on attributed task information.
    NeedsInput {
        /// Stable child identity.
        child: ChildId,
        /// Exact protected interaction request.
        request: InteractionRequest,
    },
}

impl fmt::Debug for ChildTaskOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed { child, result } => formatter
                .debug_struct("ChildTaskOutcome::Completed")
                .field("child", child)
                .field("result", result)
                .finish(),
            Self::NeedsInput { child, request } => formatter
                .debug_struct("ChildTaskOutcome::NeedsInput")
                .field("child", child)
                .field("request", request.id())
                .field(
                    "question_count",
                    &request.questionnaire_payload().questions().len(),
                )
                .field("sensitivity", &request.sensitivity())
                .finish(),
        }
    }
}

impl ChildTaskOutcome {
    /// Redaction-safe model delivery. Public questionnaires retain their
    /// prompts; sensitive questionnaires carry attribution and question ids
    /// only and must be rendered by the trusted root interaction host.
    pub fn model_projection(&self) -> Option<ChildNeedsInputProjection> {
        let Self::NeedsInput { child, request } = self else {
            return None;
        };
        Some(ChildNeedsInputProjection::from_request(
            child.clone(),
            request,
        ))
    }
}

/// Bounded, sensitivity-aware projection of a delegated interaction.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildNeedsInputProjection {
    /// Stable child identity.
    pub child: ChildId,
    /// Exact child session.
    pub child_session: SessionId,
    /// Child turn.
    pub turn: TurnId,
    /// Originating call.
    pub call: ToolCallId,
    /// Interaction request identity.
    pub request: InteractionRequestId,
    /// Question identities in canonical order.
    pub question_ids: Vec<QuestionId>,
    /// Content-handling requirement.
    pub sensitivity: InteractionSensitivity,
    /// Exact prompts only when explicitly public.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_questionnaire: Option<Questionnaire>,
}

impl ChildNeedsInputProjection {
    fn from_request(child: ChildId, request: &InteractionRequest) -> Self {
        Self {
            child,
            child_session: request.origin().session().clone(),
            turn: request.origin().turn().clone(),
            call: request.origin().call().clone(),
            request: request.id().clone(),
            question_ids: request
                .questionnaire_payload()
                .questions()
                .iter()
                .map(|question| question.id().clone())
                .collect(),
            sensitivity: request.sensitivity(),
            public_questionnaire: (request.sensitivity() == InteractionSensitivity::Public)
                .then(|| request.questionnaire_payload().clone()),
        }
    }
}

impl fmt::Debug for ChildNeedsInputProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildNeedsInputProjection")
            .field("child", &self.child)
            .field("child_session", &self.child_session)
            .field("turn", &self.turn)
            .field("call", &self.call)
            .field("request", &self.request)
            .field("question_count", &self.question_ids.len())
            .field("sensitivity", &self.sensitivity)
            .field(
                "has_public_questionnaire",
                &self.public_questionnaire.is_some(),
            )
            .finish()
    }
}

/// The structured outcome of a spawn request.
#[derive(Debug)]
pub enum SpawnOutcome {
    /// The child started. The handle allows direct host subscription to the
    /// child's full event stream (presentation), while canonical lifecycle
    /// events flow on the parent stream.
    Spawned {
        /// The stable child id.
        child: ChildId,
        /// The child's session handle.
        handle: SessionHandle,
    },
    /// The spawn was queued under an explicit queue policy.
    Queued {
        /// The reserved child id.
        child: ChildId,
    },
    /// Capacity was reached under the reject policy: a structured result,
    /// not an error, and no child was created.
    AtCapacity {
        /// Children currently alive for this parent.
        running: usize,
        /// The configured cap.
        limit: usize,
    },
}

pub(super) enum ChildBinding {
    /// Durable metadata is loaded but no provider/runtime has been started.
    Dormant,
    /// Process-owned execution/session handle currently bound to the record.
    Live {
        handle: SessionHandle,
        // Keeps the child's runtime composition alive for the binding.
        _runtime: Runtime,
    },
}

pub(super) struct ChildEntry {
    pub(super) binding: ChildBinding,
    pub(super) status: watch::Sender<ChildStatus>,
    pub(super) spec: DurableChildSpec,
    pub(super) policy_fingerprint: Fingerprint,
    pub(super) checkpoint_watermark: Option<CheckpointWatermark>,
    pub(super) checkpoint_resumable: bool,
    pub(super) revision: u64,
    pub(super) deadline_at: Option<Timestamp>,
    pub(super) max_turns: u32,
    pub(super) uses_shared_capacity: bool,
}

impl fmt::Debug for ChildEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildEntry")
            .field("session", &self.status.borrow().session)
            .field("bound", &matches!(self.binding, ChildBinding::Live { .. }))
            .finish_non_exhaustive()
    }
}

impl ChildEntry {
    pub(super) fn handle(&self) -> Option<SessionHandle> {
        match &self.binding {
            ChildBinding::Dormant => None,
            ChildBinding::Live { handle, .. } => Some(handle.clone()),
        }
    }

    pub(super) fn record(&self) -> ChildSessionRecord {
        ChildSessionRecord {
            schema_version: CHILD_CATALOG_SCHEMA_VERSION,
            child: self.status.borrow().child.clone(),
            child_session: self.status.borrow().session.clone(),
            parent_session: self.status.borrow().parent.clone(),
            spec: self.spec.clone(),
            policy_fingerprint: self.policy_fingerprint.clone(),
            status: self.status.borrow().clone(),
            checkpoint_watermark: self.checkpoint_watermark,
            checkpoint_resumable: self.checkpoint_resumable,
            revision: self.revision,
            deadline_at: self.deadline_at,
        }
    }
}

pub(super) struct QueuedSpawn {
    pub(super) child: ChildId,
    pub(super) spec: ChildSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum TaskOutcomeKey {
    Completed(TurnId),
    NeedsInput(InteractionRequestId),
}

pub(super) struct CoordinatorInner {
    pub(super) parent: SessionHandle,
    pub(super) factory: Arc<dyn ChildRuntimeFactory>,
    pub(super) config: DelegationConfig,
    pub(super) children: Mutex<BTreeMap<ChildId, ChildEntry>>,
    pub(super) queue: Mutex<Vec<QueuedSpawn>>,
    pub(super) spawn_reservations: Mutex<usize>,
    pub(super) returned_inputs:
        Mutex<BTreeMap<(ChildId, InteractionRequestId), InteractionRequest>>,
    pub(super) ready_task_outcomes: Mutex<BTreeMap<(ChildId, TaskOutcomeKey), ChildTaskOutcome>>,
    pub(super) returned_inputs_changed: Notify,
    pub(super) next_child: AtomicU64,
    pub(super) bind_gate: tokio::sync::Mutex<()>,
    pub(super) catalog_save_gate: tokio::sync::Mutex<()>,
}

impl Drop for CoordinatorInner {
    fn drop(&mut self) {
        self.parent.release_delegation_coordinator();
    }
}

/// Root-session delegation operations. Cheap to clone.
#[derive(Clone)]
pub struct DelegationCoordinator {
    pub(super) inner: Arc<CoordinatorInner>,
}

impl fmt::Debug for DelegationCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelegationCoordinator")
            .field("parent", self.inner.parent.id())
            .finish_non_exhaustive()
    }
}
