//! Neutral child-session delegation.
//!
//! A [`DelegationCoordinator`] is created by a host for one parent session.
//! It spawns children as full runtime sessions — built by the host's
//! [`ChildRuntimeFactory`], scoped by the runtime — and exposes the
//! spec-contracted lifecycle operations: spawn, list, follow up, wait, fetch
//! result, and stop, addressed by stable [`ChildId`].
//!
//! Guarantees, per the `agent-delegation` capability spec:
//! - Depth-one by default: a coordinator cannot be built for a child session,
//!   child views never retain delegation-management tools, and every
//!   operation re-checks the requesting session's parent link fail-closed.
//! - Spawn, follow-up, and stop pass the same composed authorization path
//!   tool invocation uses, fail-closed when no authorizer covers them.
//! - Attributed lifecycle events are emitted on the parent session's stream,
//!   and a final child result is never dropped by progress coalescing.
//! - Concurrency caps are enforced with reject-by-default capacity results.
//!   Live child execution stops with its parent/process; durable child
//!   sessions remain dormant and require explicit follow-up or resume.
//! - A durable host calls [`DelegationCoordinator::recover`] after rebuilding
//!   the parent. That provider-free pass reconciles exact checkpoint metadata
//!   and returned interactions before delegation commands are accepted.
//!
//! The delegation surface is host-facing API, not a built-in tool: hosts
//! register their own delegation tool (name, prompt text, schema) and call
//! into this module, so the runtime stays product-neutral.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};

use agent_runtime_core::approval::{
    ApprovalDecision, ApprovalOrigin, ApprovalPolicy, ApprovalRequest,
};
use agent_runtime_core::artifact::{ArtifactRef, ArtifactStore, ArtifactTransfer};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{CheckpointStore, CheckpointWatermark, TurnState};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::UserInput;
use agent_runtime_core::delegation::{ChildSpec, ToolViewScope, WorkspacePolicy};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{ChildPhase, ChildRecoveryState, RuntimeEvent, TurnFinish};
use agent_runtime_core::grant::AuthorizationDecision;
use agent_runtime_core::ids::{ChildId, SessionId, ToolCallId};
use agent_runtime_core::ids::{InteractionRequestId, QuestionId, TurnId};
use agent_runtime_core::interaction::{InteractionRequest, InteractionSensitivity, Questionnaire};
use agent_runtime_core::security::{
    AuthorizationRequest, SecurityAction, SecurityContext, SecurityEvidence,
};
use agent_runtime_core::store::{SessionStore, VersionedSessionState};
use agent_runtime_core::tool::{PreparedToolCall, ToolCallDisplay, ToolEffects};
use agent_runtime_core::usage::CounterKind;
use agent_runtime_registry::{Fingerprint, Permission, RegistryRevision, TrustClass};

use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::command::CheckpointRecoveryPolicy;
use crate::runtime::engine::Runtime;
use crate::runtime::session::{SessionHandle, TurnHandle};
use crate::runtime::state::returned_interaction_from_state;
use crate::tool::SecurityConfig;

/// The host-defined permission delegation operations request from the
/// composed authorization path. Default-deny: a host that never covers it
/// with an authoritative check cannot delegate.
pub const DELEGATION_PERMISSION: &str = "agent.delegate";

/// Parent session extension-state namespace containing durable child records.
pub const CHILD_CATALOG_NAMESPACE: &str = "agent-runtime.delegation.children";
const CHILD_CATALOG_REVISION: &str = "resumable-child-catalog-1";
const CHILD_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Builds the runtime a child session runs on.
///
/// The host owns provider/model routing, tool registration, workspace
/// adapters, and policy composition — the coordinator then applies the
/// spec's tool-view scope and strips delegation-management tools.
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

    fn try_acquire(&self) -> bool {
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

    fn release(&self) {
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
    fn from_spawn(spec: &ChildSpec) -> Self {
        Self {
            model: spec.model.clone(),
            limits: spec.limits,
            tools: spec.tools.clone(),
            workspace: spec.workspace.clone(),
        }
    }

    fn rebuild_spec(&self) -> ChildSpec {
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

fn checkpoint_can_resume(state: &TurnState) -> bool {
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

enum ChildBinding {
    /// Durable metadata is loaded but no provider/runtime has been started.
    Dormant,
    /// Process-owned execution/session handle currently bound to the record.
    Live {
        handle: SessionHandle,
        // Keeps the child's runtime composition alive for the binding.
        _runtime: Runtime,
    },
}

struct ChildEntry {
    binding: ChildBinding,
    status: watch::Sender<ChildStatus>,
    spec: DurableChildSpec,
    policy_fingerprint: Fingerprint,
    checkpoint_watermark: Option<CheckpointWatermark>,
    checkpoint_resumable: bool,
    revision: u64,
    deadline_at: Option<Timestamp>,
    max_turns: u32,
    uses_shared_capacity: bool,
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
    fn handle(&self) -> Option<SessionHandle> {
        match &self.binding {
            ChildBinding::Dormant => None,
            ChildBinding::Live { handle, .. } => Some(handle.clone()),
        }
    }

    fn record(&self) -> ChildSessionRecord {
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

struct QueuedSpawn {
    child: ChildId,
    spec: ChildSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum TaskOutcomeKey {
    Completed(TurnId),
    NeedsInput(InteractionRequestId),
}

struct CoordinatorInner {
    parent: SessionHandle,
    factory: Arc<dyn ChildRuntimeFactory>,
    config: DelegationConfig,
    children: Mutex<BTreeMap<ChildId, ChildEntry>>,
    queue: Mutex<Vec<QueuedSpawn>>,
    spawn_reservations: Mutex<usize>,
    returned_inputs: Mutex<BTreeMap<(ChildId, InteractionRequestId), InteractionRequest>>,
    ready_task_outcomes: Mutex<BTreeMap<(ChildId, TaskOutcomeKey), ChildTaskOutcome>>,
    returned_inputs_changed: Notify,
    next_child: AtomicU64,
    bind_gate: tokio::sync::Mutex<()>,
    catalog_save_gate: tokio::sync::Mutex<()>,
}

impl Drop for CoordinatorInner {
    fn drop(&mut self) {
        self.parent.release_delegation_coordinator();
    }
}

/// Root-session delegation operations. Cheap to clone.
#[derive(Clone)]
pub struct DelegationCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl fmt::Debug for DelegationCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelegationCoordinator")
            .field("parent", self.inner.parent.id())
            .finish_non_exhaustive()
    }
}

impl DelegationCoordinator {
    /// A coordinator for `parent`. Fails with a depth violation when `parent`
    /// is itself a delegated child — only a root session may manage children.
    pub fn new(
        parent: &SessionHandle,
        factory: Arc<dyn ChildRuntimeFactory>,
        config: DelegationConfig,
    ) -> Result<Self, RuntimeError> {
        if parent.parent().is_some() {
            return Err(depth_violation());
        }
        let mut restored = BTreeMap::new();
        let mut next_child = 0_u64;
        if let Some(state) = parent.extension_state(CHILD_CATALOG_NAMESPACE) {
            if state.revision != RegistryRevision::new(CHILD_CATALOG_REVISION) {
                return Err(RuntimeError::conflict(format!(
                    "unsupported durable child catalog revision `{}`",
                    state.revision
                )));
            }
            let catalog: DurableChildCatalog =
                serde_json::from_value(state.value).map_err(|error| {
                    RuntimeError::new(
                        ErrorKind::Serialization,
                        format!("durable child catalog could not be restored: {error}"),
                    )
                })?;
            if catalog.schema_version != CHILD_CATALOG_SCHEMA_VERSION {
                return Err(RuntimeError::conflict(format!(
                    "unsupported durable child catalog schema {}; expected {}",
                    catalog.schema_version, CHILD_CATALOG_SCHEMA_VERSION
                )));
            }
            next_child = catalog.next_child;
            let now = parent.inner().shared.clock.now();
            for mut record in catalog.children {
                if record.schema_version != CHILD_CATALOG_SCHEMA_VERSION
                    || record.parent_session != *parent.id()
                    || record.status.parent != *parent.id()
                    || record.status.child != record.child
                    || record.status.session != record.child_session
                {
                    return Err(RuntimeError::conflict(
                        "durable child catalog contains inconsistent ownership or identity",
                    ));
                }
                let retention_expired = config.limits.retention_ms.is_some_and(|retention_ms| {
                    now.as_millis()
                        .saturating_sub(record.status.updated_at.as_millis())
                        >= retention_ms
                });
                if retention_expired {
                    record.status.state = ChildState::Expired;
                    record.status.incompatibility = Some("retention expired".to_owned());
                } else if record.deadline_at.is_some_and(|deadline| now >= deadline) {
                    record.status.state = ChildState::Expired;
                    record.status.incompatibility =
                        Some("child lifetime deadline expired".to_owned());
                } else if record.status.state == ChildState::Running {
                    record.status.state = ChildState::Interrupted {
                        resumable: record.checkpoint_resumable,
                    };
                    record.status.updated_at = now;
                }

                let current_fingerprint = factory.policy_fingerprint(&record.spec)?;
                if factory.durability() != ChildDurability::Durable {
                    record.status.incompatibility =
                        Some("durable child stores are unavailable".to_owned());
                    if matches!(record.status.state, ChildState::Interrupted { .. }) {
                        record.status.state = ChildState::Interrupted { resumable: false };
                    }
                } else if current_fingerprint != record.policy_fingerprint {
                    record.status.incompatibility =
                        Some("child reconstruction policy changed".to_owned());
                    if matches!(record.status.state, ChildState::Interrupted { .. }) {
                        record.status.state = ChildState::Interrupted { resumable: false };
                    }
                }
                let (status, _) = watch::channel(record.status.clone());
                restored.insert(
                    record.child.clone(),
                    ChildEntry {
                        binding: ChildBinding::Dormant,
                        status,
                        spec: record.spec,
                        policy_fingerprint: record.policy_fingerprint,
                        checkpoint_watermark: record.checkpoint_watermark,
                        checkpoint_resumable: record.checkpoint_resumable,
                        revision: record.revision.saturating_add(1),
                        deadline_at: record.deadline_at,
                        max_turns: record.status.max_turns,
                        uses_shared_capacity: false,
                    },
                );
            }
        }
        // The protected catalog is parent-session state. Two coordinators for
        // one live parent could otherwise reserve the same child revision and
        // start competing continuations. Hosts provide the cross-process
        // parent-session lease; this closes the equivalent in-process race.
        parent.acquire_delegation_coordinator()?;
        let coordinator = Self {
            inner: Arc::new(CoordinatorInner {
                parent: parent.clone(),
                factory,
                config,
                children: Mutex::new(restored),
                queue: Mutex::new(Vec::new()),
                spawn_reservations: Mutex::new(0),
                returned_inputs: Mutex::new(BTreeMap::new()),
                ready_task_outcomes: Mutex::new(BTreeMap::new()),
                returned_inputs_changed: Notify::new(),
                next_child: AtomicU64::new(next_child),
                bind_gate: tokio::sync::Mutex::new(()),
                catalog_save_gate: tokio::sync::Mutex::new(()),
            }),
        };
        coordinator.watch_parent_shutdown();
        let recovered = coordinator.list();
        for status in &recovered {
            // Interrupted records require an asynchronous read of their exact
            // protected checkpoint. `recover()` emits their one authoritative
            // recovery transition after that reconciliation, so do not first
            // publish a provisional catalog-only answer here.
            if matches!(status.state, ChildState::Interrupted { .. }) {
                continue;
            }
            let state = if status.incompatibility.is_some() {
                ChildRecoveryState::Blocked
            } else {
                match &status.state {
                    ChildState::Idle => ChildRecoveryState::Idle,
                    ChildState::Interrupted { .. } | ChildState::Running => {
                        ChildRecoveryState::Interrupted
                    }
                    ChildState::Expired => ChildRecoveryState::Expired,
                    ChildState::Stopped { .. } | ChildState::Failed => ChildRecoveryState::Terminal,
                }
            };
            coordinator.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildProgress {
                    child: status.child.clone(),
                    phase: ChildPhase::Recovered {
                        child_session: status.session.clone(),
                        state,
                        resumable: status.resumable(),
                    },
                },
            );
        }
        if !recovered.is_empty() {
            coordinator.spawn_catalog_persist();
        }
        Ok(coordinator)
    }

    /// Spawns a child from `spec`.
    ///
    /// Order of enforcement: depth, structural validation, composed
    /// authorization, capacity — a rejected spec or denied operation creates
    /// no child session and emits no lifecycle event.
    pub async fn spawn(&self, spec: ChildSpec) -> Result<SpawnOutcome, RuntimeError> {
        self.check_depth()?;
        spec.validate()?;
        self.authorize("delegation.spawn", spawn_detail(&spec))
            .await?;

        let at_capacity = {
            let mut reservations = self
                .inner
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            let occupied = self.alive_children().saturating_add(*reservations);
            if occupied >= self.inner.config.limits.max_running_children {
                Some(occupied)
            } else {
                *reservations = (*reservations).saturating_add(1);
                None
            }
        };
        if let Some(occupied) = at_capacity {
            return match self.inner.config.capacity_policy {
                CapacityPolicy::Reject => Ok(SpawnOutcome::AtCapacity {
                    running: occupied,
                    limit: self.inner.config.limits.max_running_children,
                }),
                CapacityPolicy::Queue { max_pending } => {
                    let mut queue = self.inner.queue.lock().expect("delegation queue poisoned");
                    if queue.len() >= max_pending {
                        return Ok(SpawnOutcome::AtCapacity {
                            running: occupied,
                            limit: self.inner.config.limits.max_running_children,
                        });
                    }
                    let child = self.mint_child_id();
                    queue.push(QueuedSpawn {
                        child: child.clone(),
                        spec,
                    });
                    Ok(SpawnOutcome::Queued { child })
                }
            };
        }

        let child = self.mint_child_id();
        let started = self.start_child(child.clone(), spec).await;
        {
            let mut reservations = self
                .inner
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            *reservations = (*reservations).saturating_sub(1);
        }
        let handle = started?;
        Ok(SpawnOutcome::Spawned { child, handle })
    }

    /// Structured snapshots of every known child, in child-id order.
    pub fn list(&self) -> Vec<ChildStatus> {
        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .map(|entry| entry.status.borrow().clone())
            .collect()
    }

    /// Flushes the latest durable child checkpoints and parent-owned catalog.
    /// Ephemeral coordinators treat this as a no-op.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for child in children {
            self.refresh_checkpoint_watermark(&child).await?;
        }
        self.persist_catalog().await
    }

    /// Reconciles dormant durable children against their authoritative exact
    /// checkpoints without constructing a child runtime or provider.
    ///
    /// The parent catalog is committed independently from each child's turn
    /// checkpoint. An abrupt process exit can therefore leave a running
    /// catalog record whose watermark predates a newer safe checkpoint. Hosts
    /// call this once after constructing a coordinator and before accepting
    /// delegation commands. Missing, regressed, terminal, or indeterminate
    /// checkpoints fail closed in metadata; safe checkpoints become available
    /// only through an explicit [`Self::resume`]. Returned child interactions
    /// are restored in the same protected recovery pass.
    pub async fn recover(&self) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let candidates = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .iter()
                .filter_map(|(child, entry)| {
                    let status = entry.status.borrow();
                    (status.durability == ChildDurability::Durable
                        && matches!(status.state, ChildState::Interrupted { .. })
                        && status.incompatibility.is_none())
                    .then(|| {
                        (
                            child.clone(),
                            status.session.clone(),
                            entry.checkpoint_watermark,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (child, session, expected_watermark) in candidates {
            let checkpoint = store.load_latest(&session).await?;
            let (watermark, resumable, incompatibility) = match checkpoint {
                Some(checkpoint) => {
                    checkpoint.validate()?;
                    if checkpoint.session != session {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` checkpoint belongs to another session"
                        )));
                    }
                    if expected_watermark.as_ref().is_some_and(|expected| {
                        checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
                    }) {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` checkpoint regressed behind its catalog watermark"
                        )));
                    }
                    let incompatibility = match checkpoint.state {
                        TurnState::CallingModel { .. } => Some(
                            "provider outcome was indeterminate at process exit; exact replay is refused"
                                .to_owned(),
                        ),
                        TurnState::Terminal { .. } => Some(
                            "child checkpoint is terminal but its catalog transition was not committed"
                                .to_owned(),
                        ),
                        _ => None,
                    };
                    (
                        Some(checkpoint.watermark),
                        checkpoint_can_resume(&checkpoint.state),
                        incompatibility,
                    )
                }
                None => (
                    None,
                    false,
                    Some("exact child checkpoint is unavailable".to_owned()),
                ),
            };

            {
                let mut children = self
                    .inner
                    .children
                    .lock()
                    .expect("delegation children poisoned");
                let entry = children
                    .get_mut(&child)
                    .ok_or_else(|| unknown_child(&child))?;
                entry.checkpoint_watermark = watermark;
                entry.checkpoint_resumable = resumable;
                entry.revision = entry.revision.saturating_add(1);
                entry.status.send_modify(|status| {
                    status.state = ChildState::Interrupted { resumable };
                    status.incompatibility = incompatibility.clone();
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
            }

            let state = if incompatibility.is_some() {
                ChildRecoveryState::Blocked
            } else {
                ChildRecoveryState::Interrupted
            };
            self.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildProgress {
                    child: child.clone(),
                    phase: ChildPhase::Recovered {
                        child_session: session,
                        state,
                        resumable,
                    },
                },
            );
        }

        if !self.list().is_empty() {
            self.persist_catalog().await?;
        }
        self.recover_returned_interactions().await
    }

    /// Restores exact child task-information requests from protected terminal
    /// checkpoints without constructing child runtimes or providers.
    ///
    /// Hosts call this once after rebuilding a parent coordinator and before
    /// accepting new child operations. Ordinary catalog/list recovery remains
    /// metadata-only; this separate protected pass is what makes an
    /// unconsumed child questionnaire survive a process restart.
    pub async fn recover_returned_interactions(&self) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let candidates = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .iter()
                .filter_map(|(child, entry)| {
                    let status = entry.status.borrow();
                    (status.durability == ChildDurability::Durable
                        && status.state == ChildState::Idle
                        && status.incompatibility.is_none())
                    .then(|| {
                        (
                            child.clone(),
                            status.session.clone(),
                            entry.checkpoint_watermark,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        for (child, session, expected_watermark) in candidates {
            let Some(checkpoint) = store.load_latest(&session).await? else {
                continue;
            };
            if checkpoint.session != session {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint belongs to another session"
                )));
            }
            if expected_watermark.as_ref().is_some_and(|expected| {
                checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
            }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint regressed behind its catalog watermark"
                )));
            }
            let Some(request) =
                returned_interaction_from_state(&checkpoint.snapshot.extension_state)?
            else {
                continue;
            };
            match &checkpoint.state {
                TurnState::Terminal {
                    finish: TurnFinish::NeedsInput { request: expected },
                    ..
                } if expected == request.id() => {}
                _ => {
                    return Err(RuntimeError::conflict(format!(
                        "child `{child}` returned interaction is not bound to its terminal checkpoint"
                    )));
                }
            }
            record_returned_input_for_session(&self.inner, &child, &session, request)?;
        }
        Ok(())
    }

    /// The current snapshot of one child.
    pub fn status(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned");
        children
            .get(child)
            .map(|entry| entry.status.borrow().clone())
            .ok_or_else(|| unknown_child(child))
    }

    /// The latest completed task result of one child.
    pub fn result(&self, child: &ChildId) -> Result<Option<String>, RuntimeError> {
        Ok(self.status(child)?.last_result)
    }

    /// Observes the current exact task outcome for `child` without consuming
    /// either host-waiter or automatic model-delivery readiness.
    pub fn task_outcome(&self, child: &ChildId) -> Result<Option<ChildTaskOutcome>, RuntimeError> {
        let status = self.status(child)?;
        let request = self
            .inner
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned")
            .iter()
            .find(|((candidate, _), _)| candidate == child)
            .map(|(_, request)| request.clone());
        if let Some(request) = request {
            return Ok(Some(ChildTaskOutcome::NeedsInput {
                child: child.clone(),
                request,
            }));
        }
        if status.state == ChildState::Idle {
            return Ok(Some(ChildTaskOutcome::Completed {
                child: child.clone(),
                result: ChildTaskResult {
                    text: status.last_result.unwrap_or_default(),
                    artifacts: status.last_artifacts,
                },
            }));
        }
        Ok(None)
    }

    /// Compatibility alias for [`Self::task_outcome`].
    ///
    /// Host wait/status reads are intentionally idempotent. Automatic parent
    /// injection has a separate exact-once ordered delivery queue.
    pub fn take_task_outcome(
        &self,
        child: &ChildId,
    ) -> Result<Option<ChildTaskOutcome>, RuntimeError> {
        self.task_outcome(child)
    }

    /// Takes the once-delivery projection of every currently returned
    /// interaction in canonical
    /// `(child_id, request_id)` order.
    ///
    /// The exact protected outcomes remain retained for host inspection and
    /// explicit follow-up. Only their automatic delivery markers are
    /// consumed.
    pub fn take_ready_task_outcomes(&self) -> Vec<ChildTaskOutcome> {
        let ready = {
            let mut ready = self
                .inner
                .ready_task_outcomes
                .lock()
                .expect("ready child task outcomes poisoned");
            std::mem::take(&mut *ready)
        };
        ready.into_values().collect()
    }

    /// Waits for and drains the next non-empty canonical batch of returned
    /// child task outcomes.
    ///
    /// Both normal completion and returned input use this lossless path.
    /// It is independent of bounded event observers and ends when the parent
    /// session is cancelled or shut down.
    pub async fn wait_ready_task_outcomes(&self) -> Result<Vec<ChildTaskOutcome>, RuntimeError> {
        loop {
            let changed = self.inner.returned_inputs_changed.notified();
            let outcomes = self.take_ready_task_outcomes();
            if !outcomes.is_empty() {
                return Ok(outcomes);
            }
            tokio::select! {
                _ = changed => {}
                _ = self.inner.parent.inner().cancel.cancelled() => {
                    return Err(RuntimeError::cancelled(
                        "parent session ended while waiting for child task outcomes",
                    ));
                }
            }
        }
    }

    /// Waits until the child completes normally or returns exact task input.
    pub async fn wait_task_outcome(
        &self,
        child: &ChildId,
    ) -> Result<ChildTaskOutcome, RuntimeError> {
        let mut status_rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .get(child)
                .ok_or_else(|| unknown_child(child))?
                .status
                .subscribe()
        };
        loop {
            if let Some(outcome) = self.take_task_outcome(child)? {
                return Ok(outcome);
            }
            let status = status_rx.borrow().clone();
            if status.state.is_terminal() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` terminated before producing a task outcome"
                )));
            }
            tokio::select! {
                changed = status_rx.changed() => {
                    if changed.is_err() {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` outcome channel closed"
                        )));
                    }
                }
                _ = self.inner.returned_inputs_changed.notified() => {}
            }
        }
    }

    /// Sends a follow-up task to an existing child under its original
    /// specification and limits.
    pub async fn follow_up(&self, child: &ChildId, input: UserInput) -> Result<(), RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.follow_up",
            serde_json::json!({
                "child_id": child.as_str(),
                "task": clip_text(&joined_input_text(&input)),
            }),
        )
        .await?;
        // Refuse incompatible lifecycle states before lazily constructing a
        // provider/runtime. In particular, an interrupted child is never
        // rebound as an idle session merely because the caller used the
        // follow-up operation instead of explicit checkpoint resume.
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be recovered: {reason}"
                )));
            }
            if status.state.is_terminal() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` has stopped and cannot accept follow-ups"
                )));
            }
            if status.turns_used >= entry.max_turns {
                return Err(RuntimeError::new(
                    ErrorKind::Limit,
                    format!(
                        "child `{child}` reached its turn limit of {}",
                        entry.max_turns
                    ),
                ));
            }
            if status.state != ChildState::Idle {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is not idle; interrupted work requires explicit resume"
                )));
            }
        }
        let handle = self.bind_child(child, false).await?.0;
        let (handle, status_tx, previous_status) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be recovered: {reason}"
                )));
            }
            if status.state.is_terminal() {
                return Err(RuntimeError::new(
                    ErrorKind::Conflict,
                    format!("child `{child}` has stopped and cannot accept follow-ups"),
                ));
            }
            if status.turns_used >= entry.max_turns {
                return Err(RuntimeError::new(
                    ErrorKind::Limit,
                    format!(
                        "child `{child}` reached its turn limit of {}",
                        entry.max_turns
                    ),
                ));
            }
            if status.state != ChildState::Idle {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is not idle; interrupted work requires explicit resume"
                )));
            }
            let previous = status.clone();
            entry.status.send_modify(|status| {
                status.turns_used += 1;
                status.state = ChildState::Running;
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            (handle, entry.status.clone(), previous)
        };
        if let Err(error) = self.persist_catalog().await {
            status_tx.send_replace(previous_status);
            return Err(error);
        }
        let cleared = clear_returned_inputs_for_child(&self.inner, child, &handle);
        let turn = match handle.send(input) {
            Ok(turn) => turn,
            Err(error) => {
                restore_returned_inputs_for_child(&self.inner, child, &handle, cleared)?;
                status_tx.send_replace(previous_status);
                let _ = self.persist_catalog().await;
                return Err(error);
            }
        };
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::TurnStarted,
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle, turn);
        Ok(())
    }

    /// Explicitly resumes the exact checkpoint of an interrupted durable
    /// child. This never creates a new task or falls back to spawning another
    /// child identity.
    pub async fn resume(&self, child: &ChildId) -> Result<(), RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.resume",
            serde_json::json!({ "child_id": child.as_str() }),
        )
        .await?;
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be resumed: {reason}"
                )));
            }
            if !status.resumable() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` has no compatible interrupted checkpoint"
                )));
            }
        }
        let (handle, turn) = self.bind_child(child, true).await?;
        let turn = turn.ok_or_else(|| {
            RuntimeError::internal("durable child resume did not return a tracked turn")
        })?;
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::ResumeStarted {
                    child_session: handle.id().clone(),
                },
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle, turn);
        self.persist_catalog().await
    }

    /// Waits until `child` is not running (idle after completing a task, or
    /// terminal) and returns its snapshot.
    pub async fn wait(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        let mut rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.status.subscribe()
        };
        loop {
            let status = rx.borrow().clone();
            if status.state != ChildState::Running {
                return Ok(status);
            }
            if rx.changed().await.is_err() {
                return Ok(rx.borrow().clone());
            }
        }
    }

    /// Stops a child: cancellation reaches its tools and provider stream, and
    /// exactly one terminal stopped event is emitted for it.
    pub async fn stop(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.stop",
            serde_json::json!({ "child_id": child.as_str() }),
        )
        .await?;
        let handle = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.handle()
        };
        let reason = CancelReason::UserRequested;
        if mark_child_stopped(&self.inner, child, reason.clone()) {
            self.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildStopped {
                    child: child.clone(),
                    reason: reason.clone(),
                },
            );
        }
        if let Some(handle) = handle {
            handle.cancel(CancelReason::UserRequested);
            let _ = handle.shutdown().await;
            clear_returned_inputs_for_child(&self.inner, child, &handle);
        }
        self.persist_catalog().await?;

        // Wait for the *terminal* snapshot, not merely non-running: an idle
        // child is stopped through its monitor observing the shutdown, and
        // returning the stale idle state here would race it.
        let mut rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.status.subscribe()
        };
        loop {
            let status = rx.borrow().clone();
            if status.state.is_terminal() {
                return Ok(status);
            }
            if rx.changed().await.is_err() {
                return Ok(rx.borrow().clone());
            }
        }
    }

    /// Stops every non-terminal child (used on parent teardown).
    async fn stop_all(&self, reason: CancelReason) {
        let handles: Vec<(ChildId, SessionHandle)> = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .iter()
                .filter(|(_, entry)| !entry.status.borrow().state.is_terminal())
                .filter_map(|(id, entry)| entry.handle().map(|handle| (id.clone(), handle)))
                .collect()
        };
        for (_, handle) in &handles {
            handle.cancel(reason.clone());
        }
        for (child, handle) in &handles {
            let _ = handle.shutdown().await;
            clear_returned_inputs_for_child(&self.inner, child, handle);
        }
        let _ = self.persist_catalog().await;
    }

    async fn bind_child(
        &self,
        child: &ChildId,
        resume_checkpoint: bool,
    ) -> Result<(SessionHandle, Option<TurnHandle>), RuntimeError> {
        let _gate = self.inner.bind_gate.lock().await;
        if let Some(handle) = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .get(child)
            .and_then(ChildEntry::handle)
        {
            if resume_checkpoint {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is already bound and cannot resume twice"
                )));
            }
            return Ok((handle, None));
        }

        let (spec, session, expected_policy, expected_watermark, deadline_at) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow();
            if status.durability != ChildDurability::Durable {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` was process-ephemeral and cannot be rebound"
                )));
            }
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be rebound: {reason}"
                )));
            }
            (
                entry.spec.clone(),
                status.session.clone(),
                entry.policy_fingerprint.clone(),
                entry.checkpoint_watermark,
                entry.deadline_at,
            )
        };

        if deadline_at
            .is_some_and(|deadline| self.inner.parent.inner().shared.clock.now() >= deadline)
        {
            update_status(&self.inner, child, |status| {
                status.state = ChildState::Expired;
                status.incompatibility = Some("child lifetime deadline expired".to_owned());
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            self.persist_catalog().await?;
            return Err(RuntimeError::new(
                ErrorKind::Limit,
                format!("child `{child}` lifetime deadline expired"),
            ));
        }

        let current_policy = self.inner.factory.policy_fingerprint(&spec)?;
        if current_policy != expected_policy {
            update_status(&self.inner, child, |status| {
                status.incompatibility = Some("child reconstruction policy changed".to_owned());
                if matches!(status.state, ChildState::Interrupted { .. }) {
                    status.state = ChildState::Interrupted { resumable: false };
                }
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            self.persist_catalog().await?;
            return Err(RuntimeError::conflict(format!(
                "child `{child}` reconstruction policy is incompatible"
            )));
        }

        // Validate the exact checkpoint before acquiring process capacity or
        // constructing a child runtime. A missing, terminal, or regressed
        // checkpoint therefore cannot leave a dormant record accidentally
        // bound to a live provider composition.
        let checkpoint = if resume_checkpoint {
            let store = self.inner.factory.checkpoint_store().ok_or_else(|| {
                RuntimeError::conflict("durable child runtime has no checkpoint store")
            })?;
            let checkpoint = store.load_latest(&session).await?.ok_or_else(|| {
                RuntimeError::conflict(format!("child `{child}` has no exact checkpoint to resume"))
            })?;
            if matches!(checkpoint.state, TurnState::Terminal { .. }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint is terminal and cannot be resumed"
                )));
            }
            if !checkpoint_can_resume(&checkpoint.state) {
                update_status(&self.inner, child, |status| {
                    status.state = ChildState::Interrupted { resumable: false };
                    status.incompatibility = Some(
                        "provider outcome was indeterminate at process exit; exact replay is refused"
                            .to_owned(),
                    );
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
                self.persist_catalog().await?;
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint cannot be resumed without risking duplicate provider work"
                )));
            }
            if expected_watermark.as_ref().is_some_and(|expected| {
                checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
            }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint regressed behind its catalog watermark"
                )));
            }
            Some(checkpoint)
        } else {
            None
        };

        let uses_shared_capacity = match &self.inner.config.shared_capacity {
            Some(pool) => {
                if !pool.try_acquire() {
                    return Err(RuntimeError::new(
                        ErrorKind::Limit,
                        "shared delegation capacity is exhausted",
                    ));
                }
                true
            }
            None => false,
        };
        let (runtime, handle) = match self.build_and_start(child, &spec, Some(&session)).await {
            Ok(value) => value,
            Err(error) => {
                if let (true, Some(pool)) =
                    (uses_shared_capacity, &self.inner.config.shared_capacity)
                {
                    pool.release();
                }
                return Err(error);
            }
        };

        let events = handle.subscribe();
        {
            let mut children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children
                .get_mut(child)
                .ok_or_else(|| unknown_child(child))?;
            entry.binding = ChildBinding::Live {
                handle: handle.clone(),
                _runtime: runtime,
            };
            entry.uses_shared_capacity = uses_shared_capacity;
            entry.revision = entry.revision.saturating_add(1);
            if let Some(checkpoint) = &checkpoint {
                entry.checkpoint_watermark = Some(checkpoint.watermark);
                entry.checkpoint_resumable = checkpoint_can_resume(&checkpoint.state);
                entry.status.send_modify(|status| {
                    status.state = ChildState::Running;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
            }
        }
        self.spawn_monitor(
            child.clone(),
            handle.clone(),
            events,
            &spec,
            ChildDurability::Durable,
        );
        if let Some(deadline_at) = deadline_at {
            self.spawn_deadline_watchdog(handle.clone(), deadline_at);
        }
        let turn = match checkpoint {
            Some(checkpoint) => Some(handle.spawn_checkpoint_resume(checkpoint)?),
            None => None,
        };
        self.persist_catalog().await?;
        Ok((handle, turn))
    }

    async fn refresh_checkpoint_watermark(&self, child: &ChildId) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let session = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            if entry.status.borrow().durability != ChildDurability::Durable {
                return Ok(());
            }
            entry.status.borrow().session.clone()
        };
        if let Some(checkpoint) = store.load_latest(&session).await? {
            let mut children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            if let Some(entry) = children.get_mut(child) {
                entry.checkpoint_watermark = Some(checkpoint.watermark);
                entry.checkpoint_resumable = checkpoint_can_resume(&checkpoint.state);
                entry.revision = entry.revision.saturating_add(1);
            }
        }
        Ok(())
    }

    async fn persist_child(&self, child: &ChildId) -> Result<(), RuntimeError> {
        self.refresh_checkpoint_watermark(child).await?;
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            match children.get(child) {
                Some(entry)
                    if matches!(entry.status.borrow().state, ChildState::Interrupted { .. }) =>
                {
                    let resumable = entry.checkpoint_resumable;
                    entry.status.send_modify(|status| {
                        status.state = ChildState::Interrupted { resumable };
                    });
                }
                _ => {}
            }
        }
        self.persist_catalog().await
    }

    async fn persist_catalog(&self) -> Result<(), RuntimeError> {
        let _gate = self.inner.catalog_save_gate.lock().await;
        if self.inner.factory.durability() != ChildDurability::Durable {
            return Ok(());
        }
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| entry.status.borrow().durability == ChildDurability::Durable)
            .map(ChildEntry::record)
            .collect::<Vec<_>>();
        let catalog =
            DurableChildCatalog::new(self.inner.next_child.load(Ordering::SeqCst), children);
        let value = serde_json::to_value(catalog).map_err(|error| {
            RuntimeError::new(
                ErrorKind::Serialization,
                format!("durable child catalog could not be serialized: {error}"),
            )
        })?;
        self.inner.parent.set_extension_state(
            CHILD_CATALOG_NAMESPACE,
            VersionedSessionState::new(DurableChildCatalog::revision(), value).redaction_safe(),
        );
        self.inner.parent.persist().await
    }

    fn spawn_catalog_persist(&self) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _ = coordinator.persist_catalog().await;
        });
    }

    fn spawn_child_persist(&self, child: ChildId) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _ = coordinator.persist_child(&child).await;
        });
    }

    fn check_depth(&self) -> Result<(), RuntimeError> {
        if self.inner.parent.parent().is_some() {
            return Err(depth_violation());
        }
        Ok(())
    }

    fn alive_children(&self) -> usize {
        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| !entry.status.borrow().state.is_terminal())
            .count()
    }

    fn mint_child_id(&self) -> ChildId {
        let n = self.inner.next_child.fetch_add(1, Ordering::SeqCst) + 1;
        ChildId::new(format!("child-{n}"))
    }

    /// Evaluates a delegation operation through the parent runtime's composed
    /// authorization path — the same check set and approval policy tool
    /// invocation uses — failing closed on denial or missing coverage.
    ///
    /// `detail` is what an approval surface shows the person deciding: the
    /// child task summary, scope, or target child id. An uninformed approval
    /// is a rubber stamp, so every operation supplies one.
    async fn authorize(
        &self,
        operation: &str,
        detail: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let parent_inner = self.inner.parent.inner();
        let executor = parent_inner.shared.driver.executor();
        let security: &SecurityConfig = executor.security();
        let approval: &Arc<dyn ApprovalPolicy> = executor.approval_policy();

        let context = SecurityContext::new(
            security.subject.clone(),
            self.inner.parent.id().clone(),
            security.tenant.clone(),
            security.check_set.revision().clone(),
        );
        // Conservative evidence, mirroring the tool executor: the
        // least-trusted non-extension class and an operation fingerprint,
        // until a content-guard system is wired in.
        let evidence =
            SecurityEvidence::new(TrustClass::ExternalContent, Fingerprint::of(operation));
        let request = AuthorizationRequest::new(
            context,
            SecurityAction::new(operation),
            agent_runtime_core::security::SecurityResource::Other {
                kind: "child-agent".to_string(),
                id: self.inner.parent.id().to_string(),
            },
            agent_runtime_core::security::PermissionSet::single(Permission::other(
                DELEGATION_PERMISSION.to_string(),
            )),
            Deadline::never(),
            evidence,
        );
        let cancel = Cancellation::new();
        let outcome = security.check_set.authorize(&request, &cancel).await;
        match outcome.decision {
            AuthorizationDecision::Allow { .. } => Ok(()),
            AuthorizationDecision::Deny { code } => Err(RuntimeError::new(
                ErrorKind::Approval,
                format!("delegation authorization denied: {code}"),
            )),
            AuthorizationDecision::RequireApproval { eligible } => {
                let prepared = PreparedToolCall::new(
                    ToolCallId::new(format!("{operation}@{}", self.inner.parent.id())),
                    operation,
                    detail,
                    agent_runtime_core::security::PermissionSet::single(Permission::other(
                        DELEGATION_PERMISSION.to_string(),
                    )),
                    agent_runtime_core::security::SecurityResource::other(
                        "child-agent",
                        self.inner.parent.id().to_string(),
                    ),
                    ToolEffects::new(vec![]),
                    ToolCallDisplay::new("Authorize child-agent operation"),
                );
                let approval_request = ApprovalRequest::new(
                    prepared,
                    Deadline::never(),
                    ApprovalOrigin::new(
                        self.inner.parent.id().clone(),
                        agent_runtime_core::ids::RequestId::new(format!(
                            "{operation}@{}",
                            self.inner.parent.id()
                        )),
                    ),
                );
                let decision = approval.decide(&approval_request).await;
                let allowed = decision.is_allowed();
                let resolved = security.check_set.resolve_approval(eligible, allowed);
                if allowed && matches!(resolved, AuthorizationDecision::Allow { .. }) {
                    Ok(())
                } else {
                    let reason = match decision {
                        ApprovalDecision::Deny { reason } => reason,
                        ApprovalDecision::Allow => "approval could not be resolved".to_string(),
                        ApprovalDecision::Edit { .. } => {
                            "delegation approval cannot edit the prepared action".to_string()
                        }
                        ApprovalDecision::TimedOut => "approval timed out".to_string(),
                        ApprovalDecision::Cancelled => "approval was cancelled".to_string(),
                        ApprovalDecision::Unavailable { reason } => {
                            format!("approval unavailable: {reason}")
                        }
                    };
                    Err(RuntimeError::new(
                        ErrorKind::Approval,
                        format!("delegation approval denied: {reason}"),
                    ))
                }
            }
        }
    }

    /// Builds and starts one child session, emits `ChildSpawned`, sends the
    /// task, and installs the monitor that mirrors the child's lifecycle onto
    /// the parent stream.
    async fn start_child(
        &self,
        child: ChildId,
        spec: ChildSpec,
    ) -> Result<SessionHandle, RuntimeError> {
        if self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .len()
            >= self.inner.config.limits.max_retained_children
        {
            return Err(RuntimeError::new(
                ErrorKind::Limit,
                "retained child limit is exhausted",
            ));
        }
        let uses_shared_capacity = match &self.inner.config.shared_capacity {
            Some(pool) => {
                if !pool.try_acquire() {
                    return Err(RuntimeError::new(
                        ErrorKind::Limit,
                        "shared delegation capacity is exhausted",
                    ));
                }
                true
            }
            None => false,
        };

        let durable_spec = DurableChildSpec::from_spawn(&spec);
        let durability = self.inner.factory.durability();
        let requested_session = (durability == ChildDurability::Durable)
            .then(|| SessionId::new(format!("child-session-{}", uuid::Uuid::new_v4())));
        let policy_fingerprint = self.inner.factory.policy_fingerprint(&durable_spec)?;
        let started = self
            .build_and_start(&child, &durable_spec, requested_session.as_ref())
            .await;
        let (runtime, handle) = match started {
            Ok(pair) => pair,
            Err(err) => {
                if uses_shared_capacity {
                    if let Some(pool) = &self.inner.config.shared_capacity {
                        pool.release();
                    }
                }
                return Err(err);
            }
        };

        let now = self.inner.parent.inner().shared.clock.now();
        let deadline_at = spec
            .limits
            .deadline_ms
            .map(|duration| now.plus_millis(duration));
        let (status_tx, _) = watch::channel(ChildStatus {
            child: child.clone(),
            parent: self.inner.parent.id().clone(),
            session: handle.id().clone(),
            durability,
            state: ChildState::Running,
            workspace: spec.workspace.clone(),
            turns_used: 1,
            max_turns: spec.limits.max_turns,
            tokens_used: 0,
            last_result: None,
            last_artifacts: Vec::new(),
            updated_at: now,
            incompatibility: None,
        });

        // Subscribe before sending the task so no lifecycle event is missed.
        let events = handle.subscribe();
        self.spawn_monitor(
            child.clone(),
            handle.clone(),
            events,
            &durable_spec,
            durability,
        );
        if let Some(deadline_at) = deadline_at {
            self.spawn_deadline_watchdog(handle.clone(), deadline_at);
        }

        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .insert(
                child.clone(),
                ChildEntry {
                    binding: ChildBinding::Live {
                        handle: handle.clone(),
                        _runtime: runtime,
                    },
                    status: status_tx,
                    spec: durable_spec,
                    policy_fingerprint,
                    checkpoint_watermark: None,
                    checkpoint_resumable: false,
                    revision: 1,
                    deadline_at,
                    max_turns: spec.limits.max_turns,
                    uses_shared_capacity,
                },
            );

        let initial_persist = if durability == ChildDurability::Durable {
            self.persist_catalog().await
        } else {
            Ok(())
        };
        if let Err(error) = initial_persist {
            self.inner
                .children
                .lock()
                .expect("delegation children poisoned")
                .remove(&child);
            handle.cancel_session(CancelReason::Shutdown);
            let _ = handle.shutdown().await;
            if let (true, Some(pool)) = (uses_shared_capacity, &self.inner.config.shared_capacity) {
                pool.release();
            }
            return Err(error);
        }

        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildSpawned {
                child: child.clone(),
                workspace: spec.workspace.clone(),
                max_turns: spec.limits.max_turns,
                max_tokens: spec.limits.max_tokens,
                deadline_ms: spec.limits.deadline_ms,
            },
        );

        let turn = match handle.send(spec.task) {
            Ok(turn) => turn,
            Err(error) => {
                update_status(&self.inner, &child, |status| {
                    status.state = ChildState::Failed;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
                let _ = self.persist_catalog().await;
                return Err(error);
            }
        };
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::TurnStarted,
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle.clone(), turn);
        Ok(handle)
    }

    async fn build_and_start(
        &self,
        child: &ChildId,
        durable_spec: &DurableChildSpec,
        session: Option<&SessionId>,
    ) -> Result<(Runtime, SessionHandle), RuntimeError> {
        let spec = durable_spec.rebuild_spec();
        let mut builder = self.inner.factory.child_builder(&spec)?;

        // Delegation-management tools never reach a child view, whatever the
        // requested scope.
        let delegation_names = self.inner.config.delegation_tool_names.clone();
        builder.scope_tools(|tool| {
            let name = tool.spec().name;
            !delegation_names.iter().any(|candidate| candidate == &name)
        });

        // Apply the spec's scope. A read-only workspace posture also forces
        // the read-only tool filter, so a child that must not mutate cannot
        // hold write-capable tools regardless of the requested scope.
        let read_only_posture = spec.workspace == WorkspacePolicy::ReadOnlyView;
        match &spec.tools {
            ToolViewScope::All => {}
            ToolViewScope::ReadOnly => {
                builder.scope_tools(tool_is_read_only);
            }
            ToolViewScope::Named { names } => {
                let names = names.clone();
                builder.scope_tools(|tool| {
                    let name = tool.spec().name;
                    names.iter().any(|candidate| candidate == &name)
                });
            }
        }
        if read_only_posture {
            builder.scope_tools(tool_is_read_only);
        }

        // Child interactions are never presented directly through the root
        // host broker. The runtime completes the exchange and returns the
        // exact request through this coordinator's protected outcome path.
        builder.return_child_interactions_to_parent();

        if self.inner.factory.durability() == ChildDurability::Ephemeral {
            builder.clear_session_store();
        }

        let runtime = builder.build()?;
        let mut start = crate::runtime::command::StartSession::new()
            .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer);
        if let Some(session) = session {
            start = start.with_id(session.clone());
        }
        let handle = runtime
            .start_child_session(start, self.inner.parent.id().clone())
            .await
            .map_err(|err| {
                RuntimeError::new(
                    err.kind,
                    format!("failed to start child `{child}`: {}", err.message),
                )
            })?;
        Ok((runtime, handle))
    }

    /// Lossless control path for a child turn's protected returned
    /// interaction. This is deliberately independent of the bounded
    /// observability broadcast used by [`Self::spawn_monitor`].
    fn spawn_returned_input_collector(
        &self,
        child: ChildId,
        handle: SessionHandle,
        turn: TurnHandle,
    ) {
        let coordinator = self.inner.clone();
        tokio::spawn(async move {
            let turn_id = turn.id().clone();
            let (finish, returned) = turn.outcome().await;
            if finish.is_some() {
                coordinator.parent.inner().emitter.emit(
                    None,
                    RuntimeEvent::ChildProgress {
                        child: child.clone(),
                        phase: ChildPhase::TurnFinished,
                    },
                );
            }
            let result = match (finish, returned) {
                (Some(TurnFinish::NeedsInput { request }), Some(exact))
                    if exact.id() == &request =>
                {
                    record_returned_input(&coordinator, &child, &handle, exact)
                }
                (Some(TurnFinish::NeedsInput { .. }), _) => Err(RuntimeError::conflict(
                    "child completed with needs_input but its protected request was unavailable",
                )),
                (Some(TurnFinish::Completed | TurnFinish::LimitReached { .. }), None) => {
                    match transfer_completed_result(
                        &coordinator,
                        &child,
                        &handle,
                        &turn_id,
                        last_assistant_text(&handle),
                    )
                    .await
                    {
                        Ok(result) => {
                            record_completed_outcome(&coordinator, &child, turn_id, result)
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => return,
            };
            if let Err(error) = result {
                update_status(&coordinator, &child, |status| {
                    status.state = ChildState::Failed;
                    status.updated_at = coordinator.parent.inner().shared.clock.now();
                });
                DelegationCoordinator {
                    inner: coordinator.clone(),
                }
                .spawn_child_persist(child.clone());
                coordinator
                    .parent
                    .inner()
                    .emitter
                    .emit(None, RuntimeEvent::ChildFailed { child, error });
            }
        });
    }

    /// Mirrors one child's canonical events onto the parent stream as
    /// attributed child lifecycle events, enforces the token budget, and
    /// resolves the terminal state exactly once.
    fn spawn_monitor(
        &self,
        child: ChildId,
        handle: SessionHandle,
        mut events: crate::runtime::emitter::RuntimeEventStream,
        spec: &DurableChildSpec,
        durability: ChildDurability,
    ) {
        let coordinator = self.inner.clone();
        let max_tokens = spec.limits.max_tokens;
        tokio::spawn(async move {
            let parent_emitter = coordinator.parent.inner().emitter.clone();
            let mut tokens_used = coordinator
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(&child)
                .map(|entry| entry.status.borrow().tokens_used)
                .unwrap_or(0);
            let mut terminal = false;
            while let Some(envelope) = events.next().await {
                match envelope.payload {
                    RuntimeEvent::TurnStarted => {}
                    RuntimeEvent::ToolCallCompleted { name, .. } => {
                        parent_emitter.emit(
                            None,
                            RuntimeEvent::ChildProgress {
                                child: child.clone(),
                                phase: ChildPhase::ToolCall { name },
                            },
                        );
                    }
                    RuntimeEvent::Usage { record } => {
                        tokens_used = tokens_used
                            .saturating_add(record.delta.get(CounterKind::InputUncached))
                            .saturating_add(record.delta.get(CounterKind::InputCached))
                            .saturating_add(record.delta.get(CounterKind::Output))
                            .saturating_add(record.delta.get(CounterKind::Reasoning));
                        update_status(&coordinator, &child, |status| {
                            status.tokens_used = tokens_used;
                            status.updated_at = coordinator.parent.inner().shared.clock.now();
                        });
                        if let Some(budget) = max_tokens {
                            if tokens_used > budget {
                                handle.cancel(CancelReason::LimitReached);
                            }
                        }
                    }
                    RuntimeEvent::TurnCompleted { finish, .. } => {
                        match finish {
                            // Normal and returned-input task outcomes use the
                            // lossless TurnHandle completion cell. This
                            // bounded broadcast is observability only.
                            TurnFinish::Completed
                            | TurnFinish::LimitReached { .. }
                            // The protected NeedsInput control path is the
                            // lossless turn-completion cell. This bounded
                            // broadcast is observability only and may lag.
                            | TurnFinish::NeedsInput { .. } => {}
                            TurnFinish::Cancelled { reason } => {
                                if durability == ChildDurability::Durable
                                    && reason == CancelReason::Shutdown
                                {
                                    update_status(&coordinator, &child, |status| {
                                        status.state = ChildState::Interrupted {
                                            resumable: false,
                                        };
                                        status.updated_at =
                                            coordinator.parent.inner().shared.clock.now();
                                    });
                                } else {
                                    terminal = true;
                                    if mark_child_stopped(
                                        &coordinator,
                                        &child,
                                        reason.clone(),
                                    ) {
                                        parent_emitter.emit(
                                            None,
                                            RuntimeEvent::ChildStopped {
                                                child: child.clone(),
                                                reason,
                                            },
                                        );
                                    }
                                }
                                break;
                            }
                            TurnFinish::Failed => {
                                terminal = true;
                                update_status(&coordinator, &child, |status| {
                                    status.state = ChildState::Failed;
                                    status.updated_at =
                                        coordinator.parent.inner().shared.clock.now();
                                });
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildFailed {
                                        child: child.clone(),
                                        error: RuntimeError::new(
                                            ErrorKind::Internal,
                                            "child turn failed",
                                        ),
                                    },
                                );
                                break;
                            }
                        }
                    }
                    RuntimeEvent::SessionShutdown => {
                        if !terminal && durability == ChildDurability::Durable {
                            update_status(&coordinator, &child, |status| {
                                if status.state == ChildState::Running {
                                    status.state = ChildState::Interrupted { resumable: false };
                                }
                                status.updated_at = coordinator.parent.inner().shared.clock.now();
                            });
                        } else if !terminal {
                            terminal = true;
                            let reason = handle
                                .inner()
                                .cancel
                                .reason()
                                .unwrap_or(CancelReason::Shutdown);
                            if mark_child_stopped(&coordinator, &child, reason.clone()) {
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildStopped {
                                        child: child.clone(),
                                        reason,
                                    },
                                );
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }
            // The stream ended (child dropped or shut down). Resolve a
            // terminal state exactly once even without a SessionShutdown.
            if !terminal {
                if durability == ChildDurability::Durable {
                    update_status(&coordinator, &child, |status| {
                        if status.state == ChildState::Running {
                            status.state = ChildState::Interrupted { resumable: false };
                        }
                        status.updated_at = coordinator.parent.inner().shared.clock.now();
                    });
                } else {
                    let reason = handle
                        .inner()
                        .cancel
                        .reason()
                        .unwrap_or(CancelReason::Shutdown);
                    if mark_child_stopped(&coordinator, &child, reason.clone()) {
                        parent_emitter.emit(
                            None,
                            RuntimeEvent::ChildStopped {
                                child: child.clone(),
                                reason,
                            },
                        );
                    }
                }
            }
            {
                let mut children = coordinator
                    .children
                    .lock()
                    .expect("delegation children poisoned");
                if let Some(entry) = children.get_mut(&child) {
                    entry.binding = ChildBinding::Dormant;
                    entry.revision = entry.revision.saturating_add(1);
                }
            }
            let durable = DelegationCoordinator {
                inner: coordinator.clone(),
            };
            let _ = durable.persist_child(&child).await;
            let interrupted = coordinator
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(&child)
                .map(|entry| entry.status.borrow().clone())
                .filter(|status| matches!(status.state, ChildState::Interrupted { .. }));
            if let Some(status) = interrupted {
                let resumable = status.resumable();
                parent_emitter.emit(
                    None,
                    RuntimeEvent::ChildProgress {
                        child: child.clone(),
                        phase: ChildPhase::Interrupted {
                            child_session: status.session,
                            resumable,
                        },
                    },
                );
            }
            release_capacity(&coordinator, &child);
            start_queued(&coordinator).await;
        });
    }

    fn spawn_deadline_watchdog(&self, handle: SessionHandle, deadline_at: Timestamp) {
        let clock = self.inner.parent.inner().shared.clock.clone();
        tokio::spawn(async move {
            let remaining = deadline_at
                .as_millis()
                .saturating_sub(clock.now().as_millis());
            tokio::time::sleep(std::time::Duration::from_millis(remaining)).await;
            handle.cancel(CancelReason::Timeout);
            let _ = handle.shutdown().await;
        });
    }

    /// Watches the parent session and stops every live execution when it shuts
    /// down. Durable child sessions remain dormant for explicit recovery; an
    /// ephemeral child cannot outlive or restart after its parent process.
    fn watch_parent_shutdown(&self) {
        let coordinator = self.clone();
        let mut events = self.inner.parent.subscribe();
        tokio::spawn(async move {
            while let Some(envelope) = events.next().await {
                if matches!(envelope.payload, RuntimeEvent::SessionShutdown) {
                    break;
                }
            }
            coordinator.stop_all(CancelReason::Shutdown).await;
        });
    }
}

fn tool_is_read_only(tool: &Arc<dyn agent_runtime_core::tool::Tool>) -> bool {
    tool.spec().permission_upper_bound.iter().all(|permission| {
        matches!(
            permission,
            Permission::FsRead | Permission::ClockRead | Permission::RandomRead
        )
    })
}

fn update_status(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    apply: impl FnOnce(&mut ChildStatus),
) {
    let children = coordinator
        .children
        .lock()
        .expect("delegation children poisoned");
    if let Some(entry) = children.get(child) {
        entry.status.send_modify(apply);
    }
}

/// Applies the one terminal stopped transition and reports whether the caller
/// owns publication of the corresponding terminal event.
fn mark_child_stopped(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    reason: CancelReason,
) -> bool {
    let children = coordinator
        .children
        .lock()
        .expect("delegation children poisoned");
    let Some(entry) = children.get(child) else {
        return false;
    };
    let mut transitioned = false;
    entry.status.send_modify(|status| {
        if !status.state.is_terminal() {
            status.state = ChildState::Stopped {
                reason: reason.clone(),
            };
            status.updated_at = coordinator.parent.inner().shared.clock.now();
            transitioned = true;
        }
    });
    transitioned
}

fn record_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    record_returned_input_for_session(coordinator, child, handle.id(), request)
}

fn record_returned_input_for_session(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    child_session: &SessionId,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    request.validate()?;
    if request.origin().session() != child_session {
        return Err(RuntimeError::conflict(
            "returned child interaction did not preserve exact session attribution",
        ));
    }
    let key = (child.clone(), request.id().clone());
    let outcome_key = (
        child.clone(),
        TaskOutcomeKey::NeedsInput(request.id().clone()),
    );
    {
        let mut returned = coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned");
        if let Some(existing) = returned.get(&key) {
            if existing == &request {
                return Ok(());
            }
            return Err(RuntimeError::conflict(
                "duplicate returned child interaction identity has different protected content",
            ));
        }
        returned.insert(key.clone(), request.clone());
        coordinator
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned")
            .insert(
                outcome_key,
                ChildTaskOutcome::NeedsInput {
                    child: child.clone(),
                    request: request.clone(),
                },
            );
    }

    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = None;
        status.last_artifacts.clear();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildNeedsInput {
            child: child.clone(),
            child_session: child_session.clone(),
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
        },
    );
    coordinator.returned_inputs_changed.notify_waiters();
    DelegationCoordinator {
        inner: coordinator.clone(),
    }
    .spawn_child_persist(child.clone());
    Ok(())
}

fn record_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    turn: TurnId,
    result: ChildTaskResult,
) -> Result<(), RuntimeError> {
    let outcome = ChildTaskOutcome::Completed {
        child: child.clone(),
        result: result.clone(),
    };
    let key = (child.clone(), TaskOutcomeKey::Completed(turn));
    if coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned")
        .insert(key, outcome)
        .is_some()
    {
        return Err(RuntimeError::conflict(
            "duplicate completed child task outcome identity",
        ));
    }
    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = Some(result.text.clone());
        status.last_artifacts = result.artifacts.clone();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildCompleted {
            child: child.clone(),
            result: result.text,
        },
    );
    coordinator.returned_inputs_changed.notify_waiters();
    DelegationCoordinator {
        inner: coordinator.clone(),
    }
    .spawn_child_persist(child.clone());
    Ok(())
}

async fn transfer_completed_result(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    turn: &TurnId,
    text: String,
) -> Result<ChildTaskResult, RuntimeError> {
    let sources = handle.artifacts_for_turn(turn);
    if sources.is_empty() {
        return Ok(ChildTaskResult {
            text,
            artifacts: Vec::new(),
        });
    }
    let store = coordinator.factory.artifact_store().ok_or_else(|| {
        RuntimeError::conflict(
            "child produced artifact references but its host exposed no ownership-transfer store",
        )
    })?;
    let mut artifacts = Vec::with_capacity(sources.len());
    for source in sources {
        if source.provenance.session != *handle.id() {
            return Err(RuntimeError::conflict(
                "child result contained an artifact owned by another session",
            ));
        }
        let idempotency_key = Fingerprint::of_fields([
            b"delegation-child-artifact-transfer".as_slice(),
            coordinator.parent.id().as_str().as_bytes(),
            handle.id().as_str().as_bytes(),
            child.as_str().as_bytes(),
            turn.as_str().as_bytes(),
            source.id.as_str().as_bytes(),
            source.digest.algorithm.as_bytes(),
            source.digest.hex.as_bytes(),
        ]);
        let transferred = store
            .transfer(ArtifactTransfer {
                source: source.clone(),
                target_session: coordinator.parent.id().clone(),
                purpose: "delegation.child-result".into(),
                idempotency_key: idempotency_key.as_str().to_owned(),
            })
            .await
            .map_err(|error| {
                RuntimeError::tool(format!(
                    "failed to transfer child `{child}` artifact `{}`: {error}",
                    source.id
                ))
            })?;
        if transferred.provenance.session != *coordinator.parent.id()
            || transferred
                .provenance
                .derived_from
                .as_ref()
                .is_none_or(|lineage| {
                    lineage.session != *handle.id()
                        || lineage.id != source.id
                        || lineage.digest != source.digest
                })
        {
            return Err(RuntimeError::internal(
                "child artifact transfer returned invalid ownership lineage",
            ));
        }
        artifacts.push(transferred);
    }
    Ok(ChildTaskResult { text, artifacts })
}

fn clear_returned_inputs_for_child(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
) -> Vec<(InteractionRequest, Option<ChildTaskOutcome>)> {
    let cleared = {
        let mut returned = coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned");
        let keys = returned
            .keys()
            .filter(|(candidate, _)| candidate == child)
            .cloned()
            .collect::<Vec<_>>();
        let mut ready = coordinator
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned");
        keys.into_iter()
            .filter_map(|key| {
                let ready_key = (key.0.clone(), TaskOutcomeKey::NeedsInput(key.1.clone()));
                let pending = ready.remove(&ready_key);
                returned.remove(&key).map(|request| (request, pending))
            })
            .collect::<Vec<_>>()
    };
    for (request, _) in &cleared {
        handle
            .inner()
            .execution
            .clear_returned_interaction(request.id());
    }
    cleared
}

fn restore_returned_inputs_for_child(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    cleared: Vec<(InteractionRequest, Option<ChildTaskOutcome>)>,
) -> Result<(), RuntimeError> {
    let mut returned = coordinator
        .returned_inputs
        .lock()
        .expect("returned child inputs poisoned");
    let mut ready = coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned");
    for (request, pending) in &cleared {
        let key = (child.clone(), request.id().clone());
        if returned.insert(key.clone(), request.clone()).is_some() {
            return Err(RuntimeError::conflict(
                "could not roll back returned child interaction transaction",
            ));
        }
        if let Some(outcome) = pending {
            ready.insert(
                (
                    child.clone(),
                    TaskOutcomeKey::NeedsInput(request.id().clone()),
                ),
                outcome.clone(),
            );
        }
    }
    drop(ready);
    drop(returned);
    for (request, _) in cleared {
        handle.inner().execution.return_interaction(request)?;
    }
    Ok(())
}

fn release_capacity(coordinator: &Arc<CoordinatorInner>, child: &ChildId) {
    let uses_shared = {
        let mut children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        children
            .get_mut(child)
            .map(|entry| {
                let used = entry.uses_shared_capacity;
                entry.uses_shared_capacity = false;
                used
            })
            .unwrap_or(false)
    };
    if uses_shared {
        if let Some(pool) = &coordinator.config.shared_capacity {
            pool.release();
        }
    }
}

/// Starts the next queued spawn if a slot is free (queue policy only).
async fn start_queued(coordinator: &Arc<CoordinatorInner>) {
    let next = {
        let mut reservations = coordinator
            .spawn_reservations
            .lock()
            .expect("delegation spawn reservations poisoned");
        let alive = coordinator
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| !entry.status.borrow().state.is_terminal())
            .count();
        if alive.saturating_add(*reservations) >= coordinator.config.limits.max_running_children {
            return;
        }
        let mut queue = coordinator.queue.lock().expect("delegation queue poisoned");
        if queue.is_empty() {
            return;
        }
        *reservations = (*reservations).saturating_add(1);
        queue.remove(0)
    };
    let handle = DelegationCoordinator {
        inner: coordinator.clone(),
    };
    // A queued spawn was validated and authorized at submission; a failure to
    // start it now surfaces as a ChildFailed event so it is not silently lost.
    let started = handle.start_child(next.child.clone(), next.spec).await;
    let mut reservations = coordinator
        .spawn_reservations
        .lock()
        .expect("delegation spawn reservations poisoned");
    *reservations = (*reservations).saturating_sub(1);
    drop(reservations);
    if let Err(err) = started {
        coordinator.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildFailed {
                child: next.child,
                error: err,
            },
        );
    }
}

/// The child's final answer: the last assistant message's visible text, or —
/// when a provider classified the entire answer as reasoning (observed with
/// OpenAI-compatible thinking models such as GLM) — its non-redacted
/// reasoning text. An empty result for a child that plainly answered would
/// let the parent conclude the child found nothing.
fn last_assistant_text(handle: &SessionHandle) -> String {
    let history = handle.history();
    let Some(message) = history
        .iter()
        .rev()
        .find(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant))
    else {
        return String::new();
    };
    let visible = message.joined_text();
    if !visible.is_empty() {
        return visible;
    }
    let mut reasoning = String::new();
    for part in &message.content {
        if let agent_runtime_core::content::ContentPart::Reasoning {
            text,
            redacted: false,
            ..
        } = part
        {
            if !reasoning.is_empty() {
                reasoning.push('\n');
            }
            reasoning.push_str(text);
        }
    }
    reasoning
}

/// A bounded, single-line-ish summary of host/model text for approval detail.
fn clip_text(text: &str) -> String {
    const LIMIT: usize = 200;
    if text.chars().count() <= LIMIT {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(LIMIT).collect();
    clipped.push('…');
    clipped
}

/// The concatenated text parts of a task input.
fn joined_input_text(input: &UserInput) -> String {
    let mut out = String::new();
    for part in &input.parts {
        if let Some(text) = part.as_text() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(text);
        }
    }
    out
}

/// What an approval surface shows for a spawn: the task summary and the
/// narrowing the child would run under. Never the full task verbatim past the
/// clip bound, and never anything the host did not already author or accept.
fn spawn_detail(spec: &ChildSpec) -> serde_json::Value {
    serde_json::json!({
        "task": clip_text(&joined_input_text(&spec.task)),
        "tools": serde_json::to_value(&spec.tools).unwrap_or(serde_json::Value::Null),
        "workspace": serde_json::to_value(&spec.workspace).unwrap_or(serde_json::Value::Null),
        "max_turns": spec.limits.max_turns,
        "max_tokens": spec.limits.max_tokens,
        "deadline_ms": spec.limits.deadline_ms,
    })
}

fn depth_violation() -> RuntimeError {
    RuntimeError::new(
        ErrorKind::Approval,
        "delegation depth violation: a child session cannot manage children",
    )
}

fn unknown_child(child: &ChildId) -> RuntimeError {
    RuntimeError::new(ErrorKind::NotFound, format!("unknown child `{child}`"))
}
