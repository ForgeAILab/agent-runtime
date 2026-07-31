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
use tokio::sync::watch;

use agent_runtime_core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::delegation::{ChildSpec, ToolViewScope, WorkspacePolicy};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{ChildPhase, RuntimeEvent, TurnFinish};
use agent_runtime_core::grant::AuthorizationDecision;
use agent_runtime_core::ids::{ChildId, SessionId, ToolCallId};
use agent_runtime_core::security::{
    AuthorizationRequest, SecurityAction, SecurityContext, SecurityEvidence,
};
use agent_runtime_core::tool::ToolEffects;
use agent_runtime_core::usage::CounterKind;
use agent_runtime_registry::{Fingerprint, Permission, TrustClass};

use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::engine::Runtime;
use crate::runtime::session::SessionHandle;
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

struct CoordinatorInner {
    parent: SessionHandle,
    factory: Arc<dyn ChildRuntimeFactory>,
    config: DelegationConfig,
    children: Mutex<BTreeMap<ChildId, ChildEntry>>,
    queue: Mutex<Vec<QueuedSpawn>>,
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

        let alive = self.alive_children();
        if alive >= self.inner.config.limits.max_running_children {
            return match self.inner.config.capacity_policy {
                CapacityPolicy::Reject => Ok(SpawnOutcome::AtCapacity {
                    running: alive,
                    limit: self.inner.config.limits.max_running_children,
                }),
                CapacityPolicy::Queue { max_pending } => {
                    let mut queue = self.inner.queue.lock().expect("delegation queue poisoned");
                    if queue.len() >= max_pending {
                        return Ok(SpawnOutcome::AtCapacity {
                            running: alive,
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
        let handle = self.start_child(child.clone(), spec).await?;
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
        let (handle, status_tx) = {
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
            (entry.handle.clone(), entry.status.clone())
        };
        status_tx.send_modify(|status| {
            status.turns_used += 1;
            status.state = ChildState::Running;
        });
        handle.send(input);
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
        for (_, handle) in &handles {
            let _ = handle.shutdown().await;
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
                let approval_request = ApprovalRequest {
                    call_id: ToolCallId::new(format!("{operation}@{}", self.inner.parent.id())),
                    tool: operation.to_string(),
                    arguments: detail,
                    effects: ToolEffects::read_only(),
                };
                let decision = approval.decide(&approval_request).await;
                let allowed = decision.is_allowed();
                let resolved = security.check_set.resolve_approval(eligible, allowed);
                if allowed && matches!(resolved, AuthorizationDecision::Allow { .. }) {
                    Ok(())
                } else {
                    let reason = match decision {
                        ApprovalDecision::Deny { reason } => reason,
                        ApprovalDecision::Allow => "approval could not be resolved".to_string(),
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
                child,
                ChildEntry {
                    handle: handle.clone(),
                    _runtime: runtime,
                    status: status_tx,
                    max_turns: spec.limits.max_turns,
                    uses_shared_capacity,
                },
            );

        handle.send(spec.task);
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
        builder.scope_tools(|tool| !delegation_names.iter().any(|n| n == tool.name()));

        // Apply the spec's scope. A read-only workspace posture also forces
        // the read-only tool filter, so a child that must not mutate cannot
        // hold write-capable tools regardless of the requested scope.
        let read_only_posture = spec.workspace == WorkspacePolicy::ReadOnlyView;
        match &spec.tools {
            ToolViewScope::All => {}
            ToolViewScope::ReadOnly => {
                builder.scope_tools(|tool| !tool.effects().requires_authorization());
            }
            ToolViewScope::Named { names } => {
                let names = names.clone();
                builder.scope_tools(|tool| names.iter().any(|n| n == tool.name()));
            }
        }
        if read_only_posture {
            builder.scope_tools(|tool| !tool.effects().requires_authorization());
        }

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
                    RuntimeEvent::TurnStarted => {
                        parent_emitter.emit(
                            None,
                            RuntimeEvent::ChildProgress {
                                child: child.clone(),
                                phase: ChildPhase::TurnStarted,
                            },
                        );
                    }
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
                        parent_emitter.emit(
                            None,
                            RuntimeEvent::ChildProgress {
                                child: child.clone(),
                                phase: ChildPhase::TurnFinished,
                            },
                        );
                        match finish {
                            TurnFinish::Completed | TurnFinish::LimitReached { .. } => {
                                let result = last_assistant_text(&handle);
                                update_status(&coordinator, &child, |status| {
                                    status.state = ChildState::Idle;
                                    status.last_result = Some(result.clone());
                                });
                                // The final result rides the event itself, so
                                // coalescing progress can never lose it.
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildCompleted {
                                        child: child.clone(),
                                        result,
                                    },
                                );
                            }
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
        let alive = coordinator
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| !entry.status.borrow().state.is_terminal())
            .count();
        if alive >= coordinator.config.limits.max_running_children {
            return;
        }
        let mut queue = coordinator.queue.lock().expect("delegation queue poisoned");
        if queue.is_empty() {
            return;
        }
        queue.remove(0)
    };
    let handle = DelegationCoordinator {
        inner: coordinator.clone(),
    };
    // A queued spawn was validated and authorized at submission; a failure to
    // start it now surfaces as a ChildFailed event so it is not silently lost.
    if let Err(err) = handle.start_child(next.child.clone(), next.spec).await {
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
