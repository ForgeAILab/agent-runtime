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
//! - Concurrency caps are enforced with reject-by-default capacity results;
//!   children stop with their parent or the process and never restart on
//!   resume.
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
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::delegation::{ChildSpec, ToolViewScope, WorkspacePolicy};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{ChildPhase, RuntimeEvent, TurnFinish};
use agent_runtime_core::grant::AuthorizationDecision;
use agent_runtime_core::ids::{ChildId, SessionId, ToolCallId};
use agent_runtime_core::ids::{InteractionRequestId, QuestionId, TurnId};
use agent_runtime_core::interaction::{InteractionRequest, InteractionSensitivity, Questionnaire};
use agent_runtime_core::security::{
    AuthorizationRequest, SecurityAction, SecurityContext, SecurityEvidence,
};
use agent_runtime_core::tool::{PreparedToolCall, ToolCallDisplay, ToolEffects};
use agent_runtime_core::usage::CounterKind;
use agent_runtime_registry::{Fingerprint, Permission, TrustClass};

use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::engine::Runtime;
use crate::runtime::session::{SessionHandle, TurnHandle};
use crate::tool::SecurityConfig;

/// The host-defined permission delegation operations request from the
/// composed authorization path. Default-deny: a host that never covers it
/// with an authoritative check cannot delegate.
pub const DELEGATION_PERMISSION: &str = "agent.delegate";

/// Builds the runtime a child session runs on.
///
/// The host owns provider/model routing, tool registration, workspace
/// adapters, and policy composition — the coordinator then applies the
/// spec's tool-view scope, strips delegation-management tools, and clears
/// any session store so children stay ephemeral.
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
}

/// Deterministic caps on delegated children.
#[derive(Debug, Clone)]
pub struct DelegationLimits {
    /// The maximum children of this parent running (or idle-but-alive) at
    /// once.
    pub max_running_children: usize,
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
        }
    }
}

/// The lifecycle state of one child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildState {
    /// Executing a task.
    Running,
    /// Completed at least one task and available for follow-ups.
    Idle,
    /// Stopped (terminal).
    Stopped {
        /// Why.
        reason: CancelReason,
    },
    /// Failed (terminal).
    Failed,
}

impl ChildState {
    /// Whether the child can do no further work.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ChildState::Stopped { .. } | ChildState::Failed)
    }
}

/// A structured snapshot of one child.
#[derive(Debug, Clone, PartialEq)]
pub struct ChildStatus {
    /// The stable child id.
    pub child: ChildId,
    /// The parent session.
    pub parent: SessionId,
    /// The lifecycle state.
    pub state: ChildState,
    /// The declared workspace posture.
    pub workspace: WorkspacePolicy,
    /// Tasks consumed (spawn plus follow-ups).
    pub turns_used: u32,
    /// The task cap.
    pub max_turns: u32,
    /// The latest completed task's final visible answer, if any.
    pub last_result: Option<String>,
    /// Parent-owned artifact references returned with the latest completed
    /// task.
    pub last_artifacts: Vec<ArtifactRef>,
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

struct ChildEntry {
    handle: SessionHandle,
    // Keeps the child's runtime composition alive for the child's lifetime.
    _runtime: Runtime,
    status: watch::Sender<ChildStatus>,
    max_turns: u32,
    uses_shared_capacity: bool,
}

impl fmt::Debug for ChildEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildEntry")
            .field("session", self.handle.id())
            .finish_non_exhaustive()
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
        let coordinator = Self {
            inner: Arc::new(CoordinatorInner {
                parent: parent.clone(),
                factory,
                config,
                children: Mutex::new(BTreeMap::new()),
                queue: Mutex::new(Vec::new()),
                spawn_reservations: Mutex::new(0),
                returned_inputs: Mutex::new(BTreeMap::new()),
                ready_task_outcomes: Mutex::new(BTreeMap::new()),
                returned_inputs_changed: Notify::new(),
                next_child: AtomicU64::new(0),
            }),
        };
        coordinator.watch_parent_shutdown();
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
        let (handle, status_tx, previous_status) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
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
            let previous = status.clone();
            entry.status.send_modify(|status| {
                status.turns_used += 1;
                status.state = ChildState::Running;
            });
            (entry.handle.clone(), entry.status.clone(), previous)
        };
        let cleared = clear_returned_inputs_for_child(&self.inner, child, &handle);
        let turn = match handle.send(input) {
            Ok(turn) => turn,
            Err(error) => {
                restore_returned_inputs_for_child(&self.inner, child, &handle, cleared)?;
                status_tx.send_replace(previous_status);
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
            entry.handle.clone()
        };
        handle.cancel(CancelReason::UserRequested);
        let _ = handle.shutdown().await;
        clear_returned_inputs_for_child(&self.inner, child, &handle);

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
                .map(|(id, entry)| (id.clone(), entry.handle.clone()))
                .collect()
        };
        for (_, handle) in &handles {
            handle.cancel(reason.clone());
        }
        for (child, handle) in &handles {
            let _ = handle.shutdown().await;
            clear_returned_inputs_for_child(&self.inner, child, handle);
        }
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

        let started = self.build_and_start(&child, &spec).await;
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

        let parent_inner = self.inner.parent.inner();
        parent_inner.emitter.emit(
            None,
            RuntimeEvent::ChildSpawned {
                child: child.clone(),
                workspace: spec.workspace.clone(),
                max_turns: spec.limits.max_turns,
                max_tokens: spec.limits.max_tokens,
                deadline_ms: spec.limits.deadline_ms,
            },
        );

        let (status_tx, _) = watch::channel(ChildStatus {
            child: child.clone(),
            parent: self.inner.parent.id().clone(),
            state: ChildState::Running,
            workspace: spec.workspace.clone(),
            turns_used: 1,
            max_turns: spec.limits.max_turns,
            last_result: None,
            last_artifacts: Vec::new(),
        });

        // Subscribe before sending the task so no lifecycle event is missed.
        let events = handle.subscribe();
        self.spawn_monitor(child.clone(), handle.clone(), events, &spec);
        if let Some(deadline_ms) = spec.limits.deadline_ms {
            self.spawn_deadline_watchdog(handle.clone(), deadline_ms);
        }

        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .insert(
                child.clone(),
                ChildEntry {
                    handle: handle.clone(),
                    _runtime: runtime,
                    status: status_tx,
                    max_turns: spec.limits.max_turns,
                    uses_shared_capacity,
                },
            );

        let turn = handle.send(spec.task)?;
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
        spec: &ChildSpec,
    ) -> Result<(Runtime, SessionHandle), RuntimeError> {
        let mut builder = self.inner.factory.child_builder(spec)?;

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

        // Children are ephemeral: no persistence, no resume.
        builder.clear_session_store();

        let runtime = builder.build()?;
        let handle = runtime
            .start_child_session(
                crate::runtime::command::StartSession::new(),
                self.inner.parent.id().clone(),
            )
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
                });
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
        spec: &ChildSpec,
    ) {
        let coordinator = self.inner.clone();
        let max_tokens = spec.limits.max_tokens;
        tokio::spawn(async move {
            let parent_emitter = coordinator.parent.inner().emitter.clone();
            let mut tokens_used: u64 = 0;
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
                                terminal = true;
                                update_status(&coordinator, &child, |status| {
                                    status.state = ChildState::Stopped {
                                        reason: reason.clone(),
                                    };
                                });
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildStopped {
                                        child: child.clone(),
                                        reason,
                                    },
                                );
                                break;
                            }
                            TurnFinish::Failed => {
                                terminal = true;
                                update_status(&coordinator, &child, |status| {
                                    status.state = ChildState::Failed;
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
                        if !terminal {
                            terminal = true;
                            let reason = handle
                                .inner()
                                .cancel
                                .reason()
                                .unwrap_or(CancelReason::Shutdown);
                            update_status(&coordinator, &child, |status| {
                                status.state = ChildState::Stopped {
                                    reason: reason.clone(),
                                };
                            });
                            parent_emitter.emit(
                                None,
                                RuntimeEvent::ChildStopped {
                                    child: child.clone(),
                                    reason,
                                },
                            );
                        }
                        break;
                    }
                    _ => {}
                }
            }
            // The stream ended (child dropped or shut down). Resolve a
            // terminal state exactly once even without a SessionShutdown.
            if !terminal {
                let reason = handle
                    .inner()
                    .cancel
                    .reason()
                    .unwrap_or(CancelReason::Shutdown);
                update_status(&coordinator, &child, |status| {
                    if !status.state.is_terminal() {
                        status.state = ChildState::Stopped {
                            reason: reason.clone(),
                        };
                    }
                });
                parent_emitter.emit(
                    None,
                    RuntimeEvent::ChildStopped {
                        child: child.clone(),
                        reason,
                    },
                );
            }
            release_capacity(&coordinator, &child);
            start_queued(&coordinator).await;
        });
    }

    fn spawn_deadline_watchdog(&self, handle: SessionHandle, deadline_ms: u64) {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(deadline_ms)).await;
            handle.cancel(CancelReason::Timeout);
            let _ = handle.shutdown().await;
        });
    }

    /// Watches the parent session and stops every child when it shuts down —
    /// children never outlive their parent, and a later resume of the parent
    /// session never restarts them.
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

fn record_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    request.validate()?;
    if request.origin().session() != handle.id() {
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
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildNeedsInput {
            child: child.clone(),
            child_session: handle.id().clone(),
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
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildCompleted {
            child: child.clone(),
            result: result.text,
        },
    );
    coordinator.returned_inputs_changed.notify_waiters();
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
        let children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        children
            .get(child)
            .map(|entry| entry.uses_shared_capacity)
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
