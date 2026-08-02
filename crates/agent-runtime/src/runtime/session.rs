//! The session handle: send input, subscribe to events, cancel, and shut down.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::AbortHandle;

use agent_runtime_core::artifact::ArtifactRef;
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::TurnCheckpoint;
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{
    InternalTurnInput, Message, ToolCall, ToolResultBlock, UserInput,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::goal::{GoalCommand, GoalCommandResult, GoalProjection, GoalStatus};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::interaction::InteractionRequest;
use agent_runtime_core::store::{SessionSnapshot, VersionedSessionState};
use serde_json::Value;

use crate::capability::ActivationEpoch;
use crate::harness::GoalComponent;
use crate::ids::IdMinter;
use crate::runtime::emitter::{EventEmitter, RuntimeEventStream};
use crate::runtime::engine::{ActiveSessionLease, RuntimeShared};
use crate::runtime::inject::{InjectedContent, InjectionQueue};
use crate::runtime::state::{SessionExecutionContext, SessionState};

/// The shared inner state of a session.
#[derive(Debug)]
pub struct SessionInner {
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) id: SessionId,
    /// The parent session, when this session is a delegated child. A child
    /// session must never spawn children of its own (depth-one enforcement).
    pub(crate) parent: Option<SessionId>,
    pub(crate) cancel: Cancellation,
    pub(crate) emitter: Arc<EventEmitter>,
    pub(crate) minter: Arc<IdMinter>,
    pub(crate) state: Arc<Mutex<SessionState>>,
    pub(crate) execution: Arc<SessionExecutionContext>,
    pub(crate) inbox: Arc<Mutex<InjectionQueue>>,
    pub(crate) turn_gate: AsyncMutex<()>,
    pub(crate) turns: Mutex<ActiveTurns>,
    pub(crate) turn_ready: Notify,
    pub(crate) turns_changed: Notify,
    pub(crate) shutdown_lock: AsyncMutex<bool>,
    pub(crate) active_session_lease: ActiveSessionLease,
    /// Ensures one delegation coordinator owns this parent session's child
    /// catalog and execution bindings at a time.
    pub(crate) delegation_coordinator_active: AtomicBool,
    /// Ensures one process-scoped goal controller owns continuation admission.
    pub(crate) goal_controller_active: AtomicBool,
    /// An unanswered interaction checkpoint was intentionally left dormant.
    pub(crate) recovery_deferred: bool,
}

/// Active turn bookkeeping shared with shutdown.
#[derive(Debug, Default)]
pub(crate) struct ActiveTurns {
    shutting_down: bool,
    count: usize,
    aborts: Vec<AbortHandle>,
    cancellations: BTreeMap<TurnId, Cancellation>,
    internal_goals: BTreeMap<TurnId, agent_runtime_core::content::InternalGoalBinding>,
    current: Option<TurnId>,
    next_ticket: u64,
    serving_ticket: u64,
}

#[derive(Debug, Default)]
struct TurnCompletionState {
    done: bool,
    finish: Option<TurnFinish>,
    returned_interaction: Option<InteractionRequest>,
}

#[derive(Debug, Default)]
struct TurnCompletion {
    state: Mutex<TurnCompletionState>,
    notify: Notify,
}

impl TurnCompletion {
    fn finish(&self, finish: Option<TurnFinish>, returned_interaction: Option<InteractionRequest>) {
        let should_notify = {
            let mut state = self.state.lock().expect("turn completion poisoned");
            if state.done {
                false
            } else {
                state.finish = finish;
                state.returned_interaction = returned_interaction;
                state.done = true;
                true
            }
        };
        if should_notify {
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            if self.state.lock().expect("turn completion poisoned").done {
                return;
            }
            let notified = self.notify.notified();
            if self.state.lock().expect("turn completion poisoned").done {
                return;
            }
            notified.await;
        }
    }

    async fn outcome(&self) -> (Option<TurnFinish>, Option<InteractionRequest>) {
        self.wait().await;
        let state = self.state.lock().expect("turn completion poisoned");
        (state.finish.clone(), state.returned_interaction.clone())
    }
}

/// A handle to one accepted turn.
#[derive(Debug, Clone)]
pub struct TurnHandle {
    id: TurnId,
    cancel: Cancellation,
    completion: Arc<TurnCompletion>,
}

/// Atomic no-queue result for an attributed internal turn.
#[derive(Debug, Clone)]
pub enum InternalTurnAdmission {
    /// The internal turn won idle admission and started a tracked task.
    Accepted(TurnHandle),
    /// A real user turn, local action, control, or deferred recovery owns the
    /// session. Internal work was not queued.
    Busy,
    /// The expected goal identity/generation is no longer canonical.
    Stale {
        /// Current bounded projection, when a goal still exists.
        goal: Option<GoalProjection>,
    },
    /// The session is terminal and accepts no more work.
    Shutdown,
}

impl TurnHandle {
    /// The accepted turn id.
    pub fn id(&self) -> &TurnId {
        &self.id
    }

    /// Waits until the turn task has reached a terminal boundary.
    pub async fn completed(&self) {
        self.completion.wait().await;
    }

    pub(crate) async fn outcome(&self) -> (Option<TurnFinish>, Option<InteractionRequest>) {
        self.completion.outcome().await
    }

    /// Interrupts only this turn, including while it is queued.
    pub fn interrupt(&self, reason: CancelReason) {
        self.cancel.cancel(reason);
    }
}

/// A handle to one active or resumable session.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    inner: Arc<SessionInner>,
}

impl SessionHandle {
    pub(crate) fn acquire_delegation_coordinator(&self) -> Result<(), RuntimeError> {
        self.inner
            .delegation_coordinator_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                RuntimeError::conflict(format!(
                    "session `{}` already has an active delegation coordinator",
                    self.id()
                ))
            })
    }

    pub(crate) fn release_delegation_coordinator(&self) {
        self.inner
            .delegation_coordinator_active
            .store(false, Ordering::Release);
    }

    pub(crate) fn new(inner: Arc<SessionInner>) -> Self {
        Self { inner }
    }

    /// The session id.
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    /// The parent session id, when this session is a delegated child.
    pub fn parent(&self) -> Option<&SessionId> {
        self.inner.parent.as_ref()
    }

    /// Returns the session's current frozen activation epoch when live
    /// capability routing is enabled.
    ///
    /// This read-only projection lets hosts recover composition emitted
    /// before they subscribed without exposing mutable activation state.
    pub fn activation_epoch(&self) -> Option<ActivationEpoch> {
        self.inner.execution.activation_epoch()
    }

    pub(crate) fn inner(&self) -> &Arc<SessionInner> {
        &self.inner
    }

    /// Enqueues host content for this session, introduced to the model only
    /// at the next safe provider/tool boundary — never by mutating an
    /// in-flight provider stream. Coalescable content past the configured
    /// queue bound returns a structured overflow error; content marked
    /// must-deliver is always accepted.
    pub fn inject(&self, content: InjectedContent) -> Result<(), RuntimeError> {
        self.inner
            .inbox
            .lock()
            .expect("session inbox poisoned")
            .push(content)
    }

    /// Subscribes to events emitted after this call. Multiple concurrent
    /// subscribers receive the same live sequence; the persisted journal is
    /// authoritative for earlier events and any detected delivery gaps.
    pub fn subscribe(&self) -> RuntimeEventStream {
        self.inner.emitter.subscribe()
    }

    /// Queues input for this session and returns a turn-local handle.
    /// Turns execute serially in submission order while events flow through
    /// [`SessionHandle::subscribe`].
    pub fn send(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        self.spawn_turn(input)
    }

    /// Queues a turn, waits for its tracked task to complete, and returns its
    /// handle. Convenient for headless hosts that consume events through an
    /// observer.
    pub async fn run(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        let handle = self.spawn_turn(input)?;
        handle.completed().await;
        Ok(handle)
    }

    /// Starts attributed internal work only if the session is idle at the
    /// same serialized admission lock used by ordinary turns. It never queues
    /// behind real user work.
    pub fn try_send_internal_if_idle(
        &self,
        input: InternalTurnInput,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        input.validate()?;
        if self.inner.recovery_deferred {
            return Ok(InternalTurnAdmission::Busy);
        }
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Ok(InternalTurnAdmission::Shutdown);
        }
        if turns.count != 0 {
            return Ok(InternalTurnAdmission::Busy);
        }
        if let Some(expected) = &input.source.goal {
            let current = self
                .extension_state(GoalComponent::namespace())
                .as_ref()
                .map(|state| GoalComponent::sensitive().decode_state(state))
                .transpose()?;
            let matches = current.as_ref().is_some_and(|goal| {
                goal.status == GoalStatus::Active
                    && goal.id == expected.id
                    && goal.generation == expected.generation
            });
            if !matches {
                return Ok(InternalTurnAdmission::Stale {
                    goal: current.as_ref().map(|goal| goal.projection()),
                });
            }
        }

        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count = 1;
        let ticket = turns.next_ticket;
        debug_assert_eq!(ticket, turns.serving_ticket);
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());
        if let Some(goal) = input.source.goal.clone() {
            turns.internal_goals.insert(turn_id.clone(), goal);
        }

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
            }
            inner
                .shared
                .driver
                .run_internal_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    tid.clone(),
                    input,
                )
                .await;
            let finish = inner.execution.take_turn_finish(&tid);
            let returned_interaction = inner.execution.returned_interaction_value();
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(InternalTurnAdmission::Accepted(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
        }))
    }

    /// Returns the current validated persistent-goal projection.
    pub fn goal(&self, component: &GoalComponent) -> Result<Option<GoalProjection>, RuntimeError> {
        self.extension_state(GoalComponent::namespace())
            .as_ref()
            .map(|state| component.decode_state(state).map(|goal| goal.projection()))
            .transpose()
    }

    /// Applies one host-owned goal command at a serialized, durable session
    /// boundary. Mutating commands require an idle session. A pause may also
    /// interrupt the currently serving turn; its commit hook performs the
    /// single canonical active-to-paused transition after accounting usage.
    pub async fn control_goal(
        &self,
        component: &GoalComponent,
        command: GoalCommand,
    ) -> Result<GoalCommandResult, RuntimeError> {
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot mutate its goal",
            ));
        }

        let pause_target = match &command {
            GoalCommand::Pause { id, generation } => Some((id.clone(), *generation)),
            _ => None,
        };
        let serving_cancel = {
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down {
                return Err(RuntimeError::conflict(
                    "session is shutting down and no longer accepts goal controls",
                ));
            }
            if turns.count == 0 {
                None
            } else if let Some((id, generation)) = &pause_target {
                let current_goal = self
                    .extension_state(GoalComponent::namespace())
                    .as_ref()
                    .map(|state| component.decode_state(state))
                    .transpose()?
                    .ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                current_goal.validate_identity(id, *generation)?;
                if current_goal.status != GoalStatus::Active {
                    return Err(RuntimeError::conflict("only an active goal can be paused"));
                }
                let current = turns.current.as_ref().ok_or_else(|| {
                    RuntimeError::conflict(
                        "goal pause cannot overtake a queued turn that is not yet serving",
                    )
                })?;
                if turns
                    .internal_goals
                    .get(current)
                    .is_none_or(|goal| goal.id != *id)
                {
                    return Err(RuntimeError::conflict(
                        "busy goal pause requires the currently serving goal continuation",
                    ));
                }
                Some(turns.cancellations.get(current).cloned().ok_or_else(|| {
                    RuntimeError::internal(
                        "active turn cancellation handle is missing during goal pause",
                    )
                })?)
            } else {
                return Err(RuntimeError::conflict(
                    "goal mutation requires an idle session",
                ));
            }
        };

        if let Some(cancel) = serving_cancel {
            cancel.cancel(CancelReason::UserRequested);
            let (id, generation) = pause_target.expect("serving pause has a target");
            loop {
                let changed = self.inner.turns_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(goal) = self.goal(component)? {
                    if goal.id == id
                        && goal.generation > generation
                        && goal.status == GoalStatus::Paused
                    {
                        return Ok(GoalCommandResult { goal: Some(goal) });
                    }
                }
                let serving = self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .current
                    .is_some();
                if !serving {
                    return Err(RuntimeError::conflict(
                        "serving turn ended without committing the requested goal pause",
                    ));
                }
                changed.await;
            }
        }

        {
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.count != 0 {
                return Err(RuntimeError::conflict(
                    "goal mutation lost the idle-session admission race",
                ));
            }
            turns.count = 1;
        }
        let _control = GoalControlGuard {
            inner: self.inner.clone(),
        };
        let _turn_gate = self.inner.turn_gate.lock().await;

        let current = self
            .extension_state(GoalComponent::namespace())
            .as_ref()
            .map(|state| component.decode_state(state))
            .transpose()?;
        let now = self.inner.shared.clock.now();
        let usage_cursor = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .records()
            .len();
        let created_id = agent_runtime_core::ids::GoalId::new(format!(
            "goal-host-{}",
            self.inner.minter.tool_call().as_str()
        ));
        let next = component.apply_host_command(current, command, created_id, now, usage_cursor)?;

        let mut snapshot = self.snapshot();
        snapshot.updated = now;
        match &next {
            Some(goal) => {
                snapshot.extension_state.insert(
                    GoalComponent::namespace().to_owned(),
                    component.state_patch(goal)?.into_state(),
                );
            }
            None => {
                snapshot.extension_state.remove(GoalComponent::namespace());
            }
        }
        if let Some(store) = &self.inner.shared.session_store {
            store.save(&snapshot).await?;
        }

        {
            let mut extension = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            match &next {
                Some(goal) => {
                    extension.insert(
                        GoalComponent::namespace().to_owned(),
                        component.state_patch(goal)?.into_state(),
                    );
                }
                None => {
                    extension.remove(GoalComponent::namespace());
                }
            }
        }
        let event = match &next {
            Some(goal) => component.event(
                agent_runtime_core::event::GoalUpdateCause::HostControl,
                goal,
            ),
            None => component.cleared_event(),
        };
        self.inner.emitter.emit(None, event.into_runtime_event());
        Ok(GoalCommandResult::from_state(next.as_ref()))
    }

    /// Runs one explicit host-requested tool action without making a provider
    /// request.
    ///
    /// The call is serialized with ordinary turns and passes through the same
    /// schema validation, preparation, exact-resource authorization, approval,
    /// workspace enforcement, cancellation, deadline, scheduling, and output
    /// bound as a model-requested tool call. It is intentionally unavailable
    /// while another turn or local action is active.
    pub async fn run_local_tool(
        &self,
        name: impl Into<String>,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<ToolResultBlock, RuntimeError> {
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot accept a local action",
            ));
        }

        let turn = self.inner.minter.turn();
        let cancel = self.inner.cancel.child();
        {
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down {
                return Err(RuntimeError::conflict(
                    "session is shutting down and no longer accepts local actions",
                ));
            }
            if turns.count != 0 {
                return Err(RuntimeError::conflict(
                    "a local tool action requires an idle session",
                ));
            }
            turns.count = 1;
            turns.current = Some(turn.clone());
            turns.cancellations.insert(turn.clone(), cancel.clone());
        }
        let _active = LocalToolGuard {
            inner: self.inner.clone(),
            turn: turn.clone(),
        };
        let _turn_gate = self.inner.turn_gate.lock().await;

        let call = ToolCall {
            id: self.inner.minter.tool_call(),
            name: name.into(),
            arguments,
        };
        let deadline = Deadline::after(self.inner.shared.clock.as_ref(), timeout_ms.max(1));
        self.inner
            .shared
            .driver
            .run_local_tool(
                self.inner.state.clone(),
                self.inner.execution.clone(),
                self.inner.emitter.clone(),
                self.inner.minter.clone(),
                cancel,
                self.inner.inbox.clone(),
                turn,
                call,
                deadline,
            )
            .await
    }

    fn spawn_turn(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot accept a new turn",
            ));
        }
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and no longer accepts turns",
            ));
        }
        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count += 1;
        let ticket = turns.next_ticket;
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            loop {
                let ready = inner.turn_ready.notified();
                if inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .serving_ticket
                    == ticket
                {
                    break;
                }
                ready.await;
            }
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
            }
            inner
                .shared
                .driver
                .run_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    tid.clone(),
                    input,
                )
                .await;
            let finish = inner.execution.take_turn_finish(&tid);
            let returned_interaction = inner.execution.returned_interaction_value();
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
        })
    }

    pub(crate) fn spawn_checkpoint_resume(
        &self,
        checkpoint: TurnCheckpoint,
    ) -> Result<TurnHandle, RuntimeError> {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and cannot resume a turn",
            ));
        }
        if checkpoint.session != self.inner.id {
            return Err(RuntimeError::conflict(
                "cannot resume a checkpoint from another session",
            ));
        }
        checkpoint.validate()?;

        let turn_id = checkpoint.turn.clone();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count += 1;
        let ticket = turns.next_ticket;
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let completion_turn = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            loop {
                let ready = inner.turn_ready.notified();
                if inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .serving_ticket
                    == ticket
                {
                    break;
                }
                ready.await;
            }
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid);
            }
            inner
                .shared
                .driver
                .resume_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    checkpoint,
                )
                .await;
            let returned_interaction = inner.execution.returned_interaction_value();
            let finish = inner.execution.take_turn_finish(&completion_turn);
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
        })
    }

    /// Interrupts the currently serving turn without cancelling the session.
    pub fn interrupt_current_turn(&self, reason: CancelReason) -> Result<(), RuntimeError> {
        let cancel = {
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            let current = turns.current.as_ref().ok_or_else(|| {
                RuntimeError::not_found("there is no currently serving turn to interrupt")
            })?;
            turns.cancellations.get(current).cloned().ok_or_else(|| {
                RuntimeError::internal("active turn cancellation handle is missing")
            })?
        };
        cancel.cancel(reason);
        Ok(())
    }

    /// Permanently cancels this session. Cancellation propagates to active and
    /// future child tokens.
    pub fn cancel_session(&self, reason: CancelReason) {
        self.inner
            .turns
            .lock()
            .expect("session turns poisoned")
            .shutting_down = true;
        self.inner.cancel.cancel(reason);
    }

    /// Compatibility alias for terminal session cancellation.
    pub fn cancel(&self, reason: CancelReason) {
        self.cancel_session(reason);
    }

    /// The current conversation history.
    pub fn history(&self) -> Vec<Message> {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .clone()
    }

    /// A snapshot of the session's canonical state.
    pub fn snapshot(&self) -> SessionSnapshot {
        let state = self.inner.state.lock().expect("session state poisoned");
        SessionSnapshot {
            id: self.inner.id.clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self
                .inner
                .minter
                .snapshot(self.inner.emitter.next_sequence()),
            extension_state: self.inner.execution.snapshot_extension_state(),
            updated: self.inner.shared.clock.now(),
        }
    }

    /// Persists the current canonical snapshot when a session store exists.
    ///
    /// Runtime-owned background components use this at their own committed
    /// lifecycle boundaries (for example, a child completing while its parent
    /// is idle). It never changes canonical state and is a no-op for an
    /// explicitly ephemeral session.
    pub async fn persist(&self) -> Result<(), RuntimeError> {
        match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        }
    }

    pub(crate) fn extension_state(&self, namespace: &str) -> Option<VersionedSessionState> {
        self.inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .get(namespace)
            .cloned()
    }

    pub(crate) fn set_extension_state(
        &self,
        namespace: impl Into<String>,
        state: VersionedSessionState,
    ) {
        self.inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .insert(namespace.into(), state);
    }

    /// Typed artifact references produced by one turn.
    ///
    /// This protected result path is used by delegation and host UIs; it does
    /// not parse model-facing preview markers or the bounded event stream.
    pub fn artifacts_for_turn(&self, turn: &TurnId) -> Vec<ArtifactRef> {
        self.inner.execution.artifacts_for_turn(turn)
    }

    /// Cancels and drains active turns within the bounded shutdown timeout,
    /// persists the session if a store is configured, and emits a terminal
    /// [`RuntimeEvent::SessionShutdown`].
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let mut shutdown_complete = self.inner.shutdown_lock.lock().await;
        if *shutdown_complete {
            return Ok(());
        }

        {
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            turns.shutting_down = true;
        }
        self.inner.cancel.cancel(CancelReason::Shutdown);

        let timeout = Duration::from_millis(self.inner.shared.shutdown_timeout_ms);
        let drained = tokio::time::timeout(timeout, async {
            loop {
                let changed = self.inner.turns_changed.notified();
                if self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .count
                    == 0
                {
                    break;
                }
                changed.await;
            }
        })
        .await
        .is_ok();
        if !drained {
            let aborts = {
                let mut turns = self.inner.turns.lock().expect("session turns poisoned");
                std::mem::take(&mut turns.aborts)
            };
            for abort in aborts {
                abort.abort();
            }
            tokio::task::yield_now().await;
        }

        self.inner.emitter.emit(None, RuntimeEvent::SessionShutdown);
        let save_result = match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        };
        *shutdown_complete = true;
        self.inner.active_session_lease.release();
        save_result
    }
}

struct LocalToolGuard {
    inner: Arc<SessionInner>,
    turn: TurnId,
}

struct GoalControlGuard {
    inner: Arc<SessionInner>,
}

impl Drop for GoalControlGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        drop(turns);
        self.inner.turns_changed.notify_waiters();
    }
}

impl Drop for LocalToolGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        turns.cancellations.remove(&self.turn);
        turns.internal_goals.remove(&self.turn);
        if turns.current.as_ref() == Some(&self.turn) {
            turns.current = None;
        }
        drop(turns);
        self.inner.execution.clear_turn(&self.turn);
        self.inner.turns_changed.notify_waiters();
    }
}

struct ActiveTurnGuard {
    inner: Arc<SessionInner>,
    ticket: u64,
    turn: TurnId,
    completion: Arc<TurnCompletion>,
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        turns.cancellations.remove(&self.turn);
        if turns.current.as_ref() == Some(&self.turn) {
            turns.current = None;
        }
        if self.ticket >= turns.serving_ticket {
            turns.serving_ticket = self.ticket + 1;
        }
        drop(turns);
        self.inner.execution.clear_turn(&self.turn);
        self.completion.finish(None, None);
        self.inner.turn_ready.notify_waiters();
        self.inner.turns_changed.notify_waiters();
    }
}
