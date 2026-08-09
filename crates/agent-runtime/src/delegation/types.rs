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
    released: Notify,
}

impl DelegationCapacity {
    /// A pool admitting at most `limit` concurrent children.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            running: AtomicU64::new(0),
            released: Notify::new(),
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
        self.released.notify_one();
    }

    pub(super) async fn wait_for_release(&self) {
        self.released.notified().await;
    }
}

/// Per-call child wait bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DelegationWaitOptions {
    /// Optional wait duration. `None` uses the coordinator's configured
    /// default (five seconds by default).
    pub timeout: Option<Duration>,
}

impl DelegationWaitOptions {
    /// Uses the coordinator default wait.
    pub const fn default_wait() -> Self {
        Self { timeout: None }
    }

    /// Requests one bounded per-call wait.
    pub const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
        }
    }
}

/// Opaque stable identity for one protected child task outcome.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChildOutcomeIdentity {
    /// A normal completed child turn.
    Completed(TurnId),
    /// A child turn returned a protected interaction request.
    NeedsInput(InteractionRequestId),
}

/// Opaque child/outcome key used by the protected parent cursor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChildOutcomeKey {
    child: ChildId,
    outcome: ChildOutcomeIdentity,
}

impl fmt::Debug for ChildOutcomeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildOutcomeKey")
            .field("child", &self.child)
            .field("outcome", &self.outcome)
            .finish()
    }
}

impl ChildOutcomeKey {
    pub(super) fn new(child: ChildId, outcome: ChildOutcomeIdentity) -> Self {
        Self { child, outcome }
    }

    /// Stable child identity without exposing protected outcome content.
    pub fn child(&self) -> &ChildId {
        &self.child
    }

    /// Stable task-outcome identity.
    pub fn outcome(&self) -> &ChildOutcomeIdentity {
        &self.outcome
    }
}

/// Opaque parent-scoped cursor for automatic child-outcome consumption.
///
/// The cursor contains only stable identities and a monotonic revision; the
/// exact result/request remains in protected child state.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildOutcomeCursor {
    parent: SessionId,
    revision: u64,
    consumed: Vec<ChildOutcomeKey>,
}

const MAX_CURSOR_IDENTITIES: usize = 256;

impl fmt::Debug for ChildOutcomeCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildOutcomeCursor")
            .field("parent", &self.parent)
            .field("revision", &self.revision)
            .field("consumed_count", &self.consumed.len())
            .finish()
    }
}

impl ChildOutcomeCursor {
    pub(super) fn initial(parent: SessionId) -> Self {
        Self {
            parent,
            revision: 0,
            consumed: Vec::new(),
        }
    }

    pub(super) fn validate(&self, parent: &SessionId) -> Result<(), RuntimeError> {
        if &self.parent != parent {
            return Err(RuntimeError::conflict(
                "child outcome cursor belongs to another parent",
            ));
        }
        if self.consumed.len() > MAX_CURSOR_IDENTITIES
            || self.consumed.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(RuntimeError::conflict(
                "child outcome cursor identities must be sorted and unique",
            ));
        }
        Ok(())
    }

    pub(super) fn next(&self, keys: impl IntoIterator<Item = ChildOutcomeKey>) -> Self {
        let mut consumed = self.consumed.clone();
        consumed.extend(keys);
        consumed.sort();
        consumed.dedup();
        Self {
            parent: self.parent.clone(),
            revision: self.revision.saturating_add(1),
            consumed,
        }
    }

    pub(super) fn contains(&self, key: &ChildOutcomeKey) -> bool {
        self.consumed.binary_search(key).is_ok()
    }

    pub(super) fn consumed(&self) -> &[ChildOutcomeKey] {
        &self.consumed
    }

    pub(super) fn prune_to(&mut self, retained: impl IntoIterator<Item = ChildOutcomeKey>) {
        let retained = retained
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let keep_recent_from = self.consumed.len().saturating_sub(MAX_CURSOR_IDENTITIES);
        let mut consumed: Vec<ChildOutcomeKey> = self
            .consumed
            .iter()
            .enumerate()
            .filter(|(index, key)| *index >= keep_recent_from || retained.contains(*key))
            .map(|(_, key)| key.clone())
            .collect();
        // Retained identities are a preference, not a way to exceed the
        // bounded cursor contract. Keep the lexically latest canonical
        // identities when the retained projection itself is larger than the
        // cap; the delivery ledger remains authoritative for duplicate
        // suppression after an old cursor identity is pruned.
        if consumed.len() > MAX_CURSOR_IDENTITIES {
            let drop_count = consumed.len() - MAX_CURSOR_IDENTITIES;
            consumed.drain(..drop_count);
        }
        self.consumed = consumed;
    }

    pub(super) fn belongs_to(&self, parent: &SessionId) -> bool {
        &self.parent == parent
    }

    /// Parent identity bound to this cursor.
    pub fn parent(&self) -> &SessionId {
        &self.parent
    }

    /// Monotonic cursor revision.
    pub fn revision(&self) -> u64 {
        self.revision
    }
}

/// Provenance-bearing request for one atomic child-completion admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildCompletionAdmissionRequest {
    parent: SessionId,
    expected_cursor: ChildOutcomeCursor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    child: Option<ChildId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<ChildOutcomeIdentity>,
}

impl ChildCompletionAdmissionRequest {
    /// Requests admission against an expected parent cursor revision.
    pub fn new(parent: SessionId, expected_cursor: ChildOutcomeCursor) -> Self {
        Self {
            parent,
            expected_cursor,
            child: None,
            outcome: None,
        }
    }

    /// Requests admission while naming one triggering child outcome. Runtime
    /// still consumes the complete canonical ready batch at the boundary.
    pub fn for_outcome(
        parent: SessionId,
        expected_cursor: ChildOutcomeCursor,
        child: ChildId,
        outcome: ChildOutcomeIdentity,
    ) -> Self {
        Self {
            parent,
            expected_cursor,
            child: Some(child),
            outcome: Some(outcome),
        }
    }

    pub fn parent(&self) -> &SessionId {
        &self.parent
    }

    pub fn expected_cursor(&self) -> &ChildOutcomeCursor {
        &self.expected_cursor
    }

    pub fn named_outcome(&self) -> Option<ChildOutcomeKey> {
        self.child
            .clone()
            .zip(self.outcome.clone())
            .map(|(child, outcome)| ChildOutcomeKey::new(child, outcome))
    }

    pub(super) fn has_partial_named_outcome(&self) -> bool {
        self.child.is_some() != self.outcome.is_some()
    }
}

/// Versioned protected delivery state persisted in the parent snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct ProtectedChildOutcomeState {
    pub(super) schema_version: u32,
    pub(super) parent: SessionId,
    /// Monotonic protected-state revision. This advances for every durable
    /// outcome/ready projection mutation, including a follow-up that clears a
    /// superseded result while leaving the cursor revision unchanged.
    #[serde(default)]
    pub(super) revision: u64,
    pub(super) cursor: ChildOutcomeCursor,
    /// All durable host-inspection outcomes, including cursor-consumed ones.
    pub(super) outcomes: Vec<(ChildOutcomeKey, ChildTaskOutcome)>,
    /// Explicit automatic-delivery projection. `None` preserves the legacy
    /// interpretation for snapshots written before this field existed.
    #[serde(default)]
    pub(super) ready: Option<Vec<ChildOutcomeKey>>,
}

/// Result of the serialized child-completion admission boundary.
#[derive(Debug)]
pub enum ChildCompletionAdmission {
    /// The child-completion internal turn was accepted after its checkpoint
    /// barrier and carries the cursor committed with that turn.
    Accepted {
        /// The ordinary attributed internal turn handle.
        turn: TurnHandle,
        /// Cursor revision staged with the acceptance checkpoint.
        cursor: ChildOutcomeCursor,
    },
    /// User, goal, local action, or another internal turn won the boundary.
    Busy,
    /// The supplied cursor or named outcome is no longer current.
    Stale,
    /// The parent session is shutting down.
    Shutdown,
    /// The request is structurally inconsistent or no ready protected outcome
    /// exists for it.
    Conflict { reason: String },
}

/// Coordinator configuration.
#[derive(Debug, Clone)]
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
    /// Default bounded child wait.
    pub wait_default: Duration,
    /// Host-narrowed maximum child wait, never above the runtime hard cap.
    pub wait_max: Duration,
}

impl Default for DelegationConfig {
    fn default() -> Self {
        Self {
            limits: DelegationLimits::default(),
            capacity_policy: CapacityPolicy::default(),
            shared_capacity: None,
            delegation_tool_names: Vec::new(),
            wait_default: DEFAULT_DELEGATION_WAIT,
            wait_max: HARD_MAX_DELEGATION_WAIT,
        }
    }
}

impl DelegationConfig {
    pub(super) fn validate_wait_options(
        &self,
        options: DelegationWaitOptions,
    ) -> Result<Duration, RuntimeError> {
        if self.wait_max.is_zero() || self.wait_max > HARD_MAX_DELEGATION_WAIT {
            return Err(RuntimeError::config(
                "delegation wait maximum must be non-zero and no greater than thirty seconds",
            ));
        }
        if self.wait_default > self.wait_max {
            return Err(RuntimeError::config(
                "delegation wait default cannot exceed its configured maximum",
            ));
        }
        let timeout = options.timeout.unwrap_or(self.wait_default);
        if timeout > self.wait_max || timeout > HARD_MAX_DELEGATION_WAIT {
            return Err(RuntimeError::config(
                "delegation wait timeout exceeds the configured hard maximum",
            ));
        }
        Ok(timeout)
    }
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
    // A publishing/terminal checkpoint already crossed the child turn's
    // provider boundary.  It must be reconciled into the parent catalog and
    // protected outcome ledger; treating it as resumable would either repeat
    // a provider/tool turn or lose the terminal result after a crash.
    !matches!(
        state,
        TurnState::CallingModel { .. }
            | TurnState::PublishingTerminal { .. }
            | TurnState::Terminal { .. }
            | TurnState::CacheOperationTerminal { .. }
    ) && !state.is_terminal()
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
    /// Exact terminal child turn that produced this result.
    ///
    /// The turn is part of the protected outcome value as well as the
    /// [`ChildOutcomeKey`]. Keeping both copies lets recovery reject a
    /// persisted key/value splice instead of treating any two completed
    /// results as interchangeable.
    pub turn: TurnId,
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
            .field("turn", &self.turn)
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
#[derive(Clone, PartialEq, Serialize, Deserialize)]
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
        self.record_with_status(self.status.borrow().clone())
    }

    pub(super) fn record_with_status(&self, status: ChildStatus) -> ChildSessionRecord {
        ChildSessionRecord {
            schema_version: CHILD_CATALOG_SCHEMA_VERSION,
            child: status.child.clone(),
            child_session: status.session.clone(),
            parent_session: status.parent.clone(),
            spec: self.spec.clone(),
            policy_fingerprint: self.policy_fingerprint.clone(),
            status,
            checkpoint_watermark: self.checkpoint_watermark.clone(),
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

pub(super) type TaskOutcomeKey = ChildOutcomeIdentity;

pub(super) struct CoordinatorInner {
    pub(super) parent: SessionHandle,
    pub(super) factory: Arc<dyn ChildRuntimeFactory>,
    pub(super) config: DelegationConfig,
    pub(super) children: Mutex<BTreeMap<ChildId, ChildEntry>>,
    pub(super) queue: Mutex<Vec<QueuedSpawn>>,
    pub(super) spawn_reservations: Mutex<usize>,
    /// Retained-record reservations cover the interval between spawn
    /// admission and insertion into `children`.  Checking `children.len()`
    /// alone lets concurrent spawns all pass a max-retained-children cap.
    pub(super) retained_reservations: Mutex<usize>,
    pub(super) returned_inputs:
        Mutex<BTreeMap<(ChildId, InteractionRequestId), InteractionRequest>>,
    pub(super) ready_task_outcomes: Mutex<BTreeMap<(ChildId, TaskOutcomeKey), ChildTaskOutcome>>,
    /// Protected outcomes that have crossed the parent session-store barrier.
    /// A terminal result may be staged in `ready_task_outcomes` before that
    /// barrier, but no host inspection or automatic admission may expose it
    /// until this set contains its opaque identity.
    pub(super) durable_task_outcomes: Mutex<std::collections::BTreeSet<ChildOutcomeKey>>,
    /// Durable host-inspection ledger. Delivery readiness is tracked
    /// separately so consuming an automatic projection never erases the
    /// exact result returned by `task_outcome` after a restart.
    pub(super) task_outcome_ledger: Mutex<BTreeMap<(ChildId, TaskOutcomeKey), ChildTaskOutcome>>,
    /// Completion/status transitions awaiting the parent snapshot barrier.
    pub(super) pending_terminal_statuses: Mutex<BTreeMap<ChildId, ChildStatus>>,
    pub(super) pending_terminal_outcomes: Mutex<std::collections::BTreeSet<ChildOutcomeKey>>,
    /// Monotonic revision for protected outcome ledger/readiness mutations.
    pub(super) outcome_state_revision: AtomicU64,
    pub(super) outcome_cursor: Mutex<ChildOutcomeCursor>,
    /// Serializes cursor validation, parent idle arbitration, and staging.
    pub(super) outcome_admission_gate: Mutex<()>,
    /// Prevents asynchronous catalog persistence from replacing a cursor
    /// extension value staged for an in-flight parent acceptance checkpoint.
    pub(super) outcome_admission_in_flight: AtomicBool,
    /// Wakes persistence callers waiting for an in-flight parent acceptance
    /// barrier to resolve before they snapshot protected outcomes.
    pub(super) outcome_admission_changed: Notify,
    /// The last protected-outcome persistence failure. Waiters receive this
    /// error instead of silently waiting forever for an outcome that was not
    /// made durable; staging a later distinct outcome begins a fresh barrier.
    pub(super) outcome_persistence_error: Mutex<Option<RuntimeError>>,
    /// Successful ordinary catalog saves must not erase an unobserved
    /// protected-outcome failure: a background monitor save could otherwise
    /// make `wait_ready_task_outcomes` wait forever. `recover` sets this flag
    /// when it is explicitly retrying a failed terminal reduction.
    pub(super) outcome_persistence_retry: AtomicBool,
    /// Whether a waiter has observed the current persistence error. Once the
    /// error is observable, a later successful save may clear it normally.
    pub(super) outcome_persistence_error_observed: AtomicBool,
    /// Terminal checkpoint recoveries whose parent/catalog transaction has
    /// not crossed its persistence barrier yet.  This is process-local
    /// retry state: it is intentionally not persisted as a second source of
    /// truth, and is cleared only after the catalog/outcome transaction
    /// succeeds.
    pub(super) pending_terminal_recoveries: Mutex<std::collections::BTreeSet<ChildId>>,
    /// Terminal checkpoint watermarks whose recovery projection was already
    /// persisted and published in this coordinator.  Re-running `recover`
    /// is otherwise liable to emit duplicate public lifecycle events for the
    /// same authoritative checkpoint.  A later checkpoint watermark naturally
    /// makes the child eligible again.
    pub(super) published_recoveries: Mutex<BTreeMap<ChildId, Option<CheckpointWatermark>>>,
    /// At most one wake-up waiter retries a queue item blocked by a shared
    /// process capacity pool.  The pool notification is edge-triggered, so
    /// duplicate waiters would otherwise create unbounded retry tasks.
    pub(super) shared_capacity_retry_waiting: AtomicBool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pruning_keeps_a_sorted_restart_valid_cursor_at_the_cap() {
        let parent = SessionId::new("cursor-cap-parent");
        let child = ChildId::new("child-1");
        let mut cursor = ChildOutcomeCursor::initial(parent.clone());
        let keys = (0..300)
            .map(|turn| {
                ChildOutcomeKey::new(
                    child.clone(),
                    ChildOutcomeIdentity::Completed(TurnId::new(format!("turn-{turn}"))),
                )
            })
            .collect::<Vec<_>>();
        cursor = cursor.next(keys);
        cursor.prune_to(std::iter::empty());
        assert_eq!(cursor.consumed.len(), MAX_CURSOR_IDENTITIES);
        assert!(cursor.validate(&parent).is_ok());
        assert!(cursor.consumed.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
