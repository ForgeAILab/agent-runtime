//! The session handle: send input, subscribe to events, cancel, and shut down.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify};
use tokio::task::AbortHandle;

use agent_runtime_core::artifact::ArtifactRef;
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{
    CacheOperationCheckpoint, CacheOperationResultCheckpoint, TurnCheckpoint, TurnState,
};
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{
    InternalTurnInput, Message, Role, ToolCall, ToolResultBlock, UserInput,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::goal::{GoalCommand, GoalCommandResult, GoalProjection, GoalStatus};
use agent_runtime_core::ids::{CacheOperationId, SessionId, TurnId};
use agent_runtime_core::interaction::InteractionRequest;
use agent_runtime_core::steer::{SteerReceipt, SteerRejection, SteerRejectionReason};
use agent_runtime_core::store::{SessionSnapshot, VersionedSessionState};
use agent_runtime_core::usage::{Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource};
use agent_runtime_registry::Fingerprint;
use serde_json::Value;

use crate::cache::{
    CacheOperationRequest, CacheOperationResult, CacheResourceDispatchRequest, cache_operation_turn,
};
use crate::capability::ActivationEpoch;
use crate::harness::{
    GoalComponent, HarnessEvent, ProtectedSemanticSummary, SEMANTIC_SUMMARY_COMPONENT_ID,
    TurnCommitView, protected_semantic_summary_from_state, protected_summary_from_patch,
};
use crate::ids::IdMinter;
use crate::runtime::emitter::{CacheEventBatch, EventEmitter, RuntimeEventStream};
use crate::runtime::engine::{ActiveSessionLease, RuntimeShared};
use crate::runtime::inject::{InjectedContent, InjectionQueue};
use crate::runtime::state::{SessionExecutionContext, SessionState};
use crate::runtime::steer::SteerMailbox;

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
    /// Serializes the idle-admission decision across user turns, internal
    /// continuations, goal controls, and local actions.  The `turns` mutex
    /// protects the resulting bookkeeping; this gate protects the
    /// check-then-reserve boundary so a user cannot arrive between an
    /// internal idle check and its turn reservation.
    pub(crate) admission_gate: Mutex<()>,
    /// Serializes cache admission, provider execution, and the two snapshot
    /// boundaries for a session. This prevents concurrent last-write-wins
    /// saves from dropping an operation reservation or terminal result.
    pub(crate) cache_gate: AsyncMutex<()>,
    /// Serializes every SessionStore snapshot write, including ordinary turn
    /// and shutdown persistence, so a snapshot is captured only after its
    /// predecessor write has completed.
    pub(crate) persist_gate: Arc<AsyncMutex<()>>,
    /// Cache dispatches participate in shutdown draining even though they do
    /// not create an ordinary turn handle.
    pub(crate) cache_active: AtomicUsize,
    /// A protected Started save reported an error before this live dispatch
    /// was allowed to poll the provider. Only these same-handle retries may
    /// reuse a durable Started checkpoint; an aborted provider future leaves
    /// no such marker and therefore fails closed instead of replaying I/O.
    pub(crate) cache_start_repairable: Mutex<BTreeSet<CacheOperationId>>,
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
    /// Number of real-user submissions currently entering the serialized
    /// admission boundary. Child/goal continuations yield to this marker so
    /// a user that is already submitting cannot be overtaken by an internal
    /// turn at the same idle boundary.
    pub(crate) user_submission_pending: AtomicUsize,
    /// Claims one explicit idle compaction call at a time. The claim is
    /// released when the async operation ends, including cancellation.
    pub(crate) idle_compaction_inflight: AtomicBool,
    /// Consumes the one idle-compaction attempt until a new real turn begins.
    /// A failed summary therefore cannot be retried at the same boundary.
    pub(crate) idle_compaction_attempted: AtomicBool,
    /// An unanswered interaction checkpoint was intentionally left dormant.
    pub(crate) recovery_deferred: bool,
}

/// Protected boundary invoked by the cache mechanism immediately after its
/// final dispatch preflight and immediately before polling provider I/O.
/// Implementations must durably save the Started checkpoint before returning.
#[async_trait]
pub(crate) trait CacheStartBarrier: Send + Sync {
    async fn cross(&self, operation: CacheOperationCheckpoint) -> Result<(), RuntimeError>;
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
    steering: Option<ServingSteer>,
    next_ticket: u64,
    serving_ticket: u64,
}

#[derive(Debug)]
struct ServingSteer {
    turn: TurnId,
    mailbox: Arc<SteerMailbox>,
}

struct UserSubmissionGuard<'a> {
    pending: &'a AtomicUsize,
}

/// A read-only lease proving that one exact cache identity is still the last
/// provider-committed plan while the lease is held.
///
/// The lease serializes against ordinary provider-turn admission. Consumers
/// may use it to commit an identity-bound host projection after a synthetic
/// operation returns. They must drop it before starting another turn or cache
/// operation.
#[must_use = "dropping the lease releases provider-turn admission"]
#[derive(Debug)]
pub struct CurrentCacheIdentityLease<'a> {
    _turn_gate: AsyncMutexGuard<'a, ()>,
}

impl<'a> UserSubmissionGuard<'a> {
    fn enter(pending: &'a AtomicUsize) -> Self {
        pending.fetch_add(1, Ordering::AcqRel);
        Self { pending }
    }
}

impl Drop for UserSubmissionGuard<'_> {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }
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

type TurnAcceptanceHook = Box<dyn FnOnce(Result<(), RuntimeError>) + Send + 'static>;

/// The protected admission barrier for an internal turn.
///
/// `try_send_internal_if_idle` returns before the spawned task has reached its
/// first durable checkpoint.  Delegation uses this small barrier to stage
/// protected cursor state alongside that checkpoint, and only considers the
/// child outcome consumed after the checkpoint store has accepted it.
pub(crate) struct TurnAcceptance {
    state: Mutex<Option<Result<(), RuntimeError>>>,
    hook: Mutex<Option<TurnAcceptanceHook>>,
    notify: Notify,
}

impl std::fmt::Debug for TurnAcceptance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnAcceptance")
            .field(
                "resolved",
                &self
                    .state
                    .lock()
                    .expect("turn acceptance poisoned")
                    .is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl TurnAcceptance {
    pub(crate) fn pending_with_hook(hook: Option<TurnAcceptanceHook>) -> Self {
        Self {
            state: Mutex::new(None),
            hook: Mutex::new(hook),
            notify: Notify::new(),
        }
    }

    fn accepted() -> Self {
        Self {
            state: Mutex::new(Some(Ok(()))),
            hook: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub(crate) fn resolve(&self, result: Result<(), RuntimeError>) {
        let hook = {
            let mut state = self.state.lock().expect("turn acceptance poisoned");
            if state.is_some() {
                return;
            } else {
                *state = Some(result.clone());
            }
            self.hook
                .lock()
                .expect("turn acceptance hook poisoned")
                .take()
        };
        if let Some(hook) = hook {
            hook(result);
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.state
            .lock()
            .expect("turn acceptance poisoned")
            .is_none()
    }

    pub(crate) async fn wait(&self) -> Result<(), RuntimeError> {
        loop {
            if let Some(result) = self.state.lock().expect("turn acceptance poisoned").clone() {
                return result;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.state.lock().expect("turn acceptance poisoned").clone() {
                return result;
            }
            notified.await;
        }
    }
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
            tokio::pin!(notified);
            notified.as_mut().enable();
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
    pub(crate) acceptance: Arc<TurnAcceptance>,
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

/// Outcome of one explicit idle semantic-compaction boundary.
///
/// The accepted branch intentionally carries the protected result by value so
/// callers can consume its metadata/body without an extra allocation; the
/// larger enum branch is an API tradeoff for this infrequent boundary result.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, PartialEq, Eq)]
pub enum IdleCompactionAdmission {
    /// The idle boundary was claimed. A summary is present when the
    /// configured coordinator committed one; a fallback reason means the
    /// bounded attempt completed without changing summary state.
    Accepted {
        /// Protected summary metadata and body, when committed.
        summary: Option<ProtectedSemanticSummary>,
        /// Redaction-safe fallback category, when the attempt made no state
        /// change.
        fallback_reason: Option<String>,
        /// Disjoint usage committed by this idle attempt.
        usage: UsageDelta,
    },
    /// User/admission/active work already owns the boundary, or this idle
    /// interval has consumed its one attempt.
    Busy,
    /// Shutdown or cancellation won the protected boundary.
    Shutdown,
}

impl std::fmt::Debug for IdleCompactionAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted {
                summary,
                fallback_reason,
                usage,
            } => formatter
                .debug_struct("IdleCompactionAdmission::Accepted")
                .field("has_summary", &summary.is_some())
                .field("fallback_reason", fallback_reason)
                .field("usage", usage)
                .finish(),
            Self::Busy => formatter.write_str("IdleCompactionAdmission::Busy"),
            Self::Shutdown => formatter.write_str("IdleCompactionAdmission::Shutdown"),
        }
    }
}

/// Protected result metadata returned by an accepted idle compaction.
pub type IdleCompactionSummary = ProtectedSemanticSummary;

/// Compatibility name for hosts that model the method as a result rather than
/// an admission operation.
pub type IdleCompactionResult = IdleCompactionAdmission;

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

    /// Waits for the initial protected acceptance checkpoint.  This is used
    /// by atomic admission paths; ordinary hosts only need [`Self::completed`].
    pub(crate) async fn accepted(&self) -> Result<(), RuntimeError> {
        self.acceptance.wait().await
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

#[derive(Debug)]
struct SessionCacheStartBarrier {
    session: SessionHandle,
    checkpoint: Arc<AsyncMutex<Option<TurnCheckpoint>>>,
}

struct CacheActivityGuard {
    inner: Arc<SessionInner>,
}

struct IdleCompactionGuard {
    inner: Arc<SessionInner>,
    _cache_activity: CacheActivityGuard,
}

impl Drop for IdleCompactionGuard {
    fn drop(&mut self) {
        self.inner
            .idle_compaction_inflight
            .store(false, Ordering::Release);
    }
}

impl CacheActivityGuard {
    fn enter(inner: &Arc<SessionInner>) -> Result<Self, RuntimeError> {
        let turns = inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and no longer accepts cache operations",
            ));
        }
        // Increment while holding the same lock shutdown uses to publish its
        // stopping decision. This closes the admission-vs-shutdown race.
        inner.cache_active.fetch_add(1, Ordering::AcqRel);
        drop(turns);
        Ok(Self {
            inner: inner.clone(),
        })
    }
}

impl SessionHandle {
    fn mark_cache_start_repairable(&self, operation: CacheOperationId) {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .insert(operation);
    }

    fn clear_cache_start_repairable(&self, operation: &CacheOperationId) {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .remove(operation);
    }

    fn cache_start_is_repairable(&self, operation: &CacheOperationId) -> bool {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .contains(operation)
    }
}

impl Drop for CacheActivityGuard {
    fn drop(&mut self) {
        self.inner.cache_active.fetch_sub(1, Ordering::AcqRel);
        self.inner.turns_changed.notify_waiters();
    }
}

/// A cache finalization failure carries the protected phase it reached. A
/// ResultReady write failure requires rolling back the in-memory reduction;
/// a later Terminal write failure must retain it because ResultReady and its
/// event watermark are already durable and recovery is now authoritative.
#[derive(Debug)]
enum CacheFinalizeError {
    ResultReady(RuntimeError),
    Terminal(RuntimeError),
}

#[async_trait]
impl CacheStartBarrier for SessionCacheStartBarrier {
    async fn cross(&self, operation: CacheOperationCheckpoint) -> Result<(), RuntimeError> {
        let checkpoint = self
            .checkpoint
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::conflict("cache start barrier was crossed twice"))?;
        if let TurnState::CacheOperationStarted {
            operation: protected,
        } = &checkpoint.state
        {
            // A store is allowed to durably commit and then report a
            // transient error. The first invocation has not polled the
            // provider yet (the event is emitted only after this boundary),
            // so an exact retry can reuse that protected Started state and
            // publish its one lifecycle event before proceeding.
            if protected.operation != operation.operation
                || protected.identity != operation.identity
                || protected.purpose != operation.purpose
                || protected.fingerprint != operation.fingerprint
            {
                *self.checkpoint.lock().await = Some(checkpoint);
                return Err(RuntimeError::conflict(
                    "cache retry does not match its protected Started checkpoint",
                ));
            }
            self.session
                .clear_cache_start_repairable(&operation.operation);
            self.session.inner.emitter.emit(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationStarted {
                    operation: protected.operation.clone(),
                    request: protected.request.clone(),
                    attempt: protected.attempt.clone(),
                    identity: protected.identity.clone(),
                    purpose: protected.purpose,
                },
            );
            *self.checkpoint.lock().await = Some(checkpoint);
            return Ok(());
        }
        let next = match self
            .session
            .advance_cache_checkpoint_locked(
                checkpoint.clone(),
                TurnState::CacheOperationStarted {
                    operation: operation.clone(),
                },
            )
            .await
        {
            Ok(next) => next,
            Err(error) => {
                // The provider future has not been polled until this
                // barrier returns.  Preserve Prepared locally so an exact
                // retry can repair a transient checkpoint-store fault and
                // then cross the start boundary once, without replaying any
                // provider work.
                self.session
                    .mark_cache_start_repairable(operation.operation.clone());
                *self.checkpoint.lock().await = Some(checkpoint);
                return Err(error);
            }
        };
        self.session
            .clear_cache_start_repairable(&operation.operation);
        // The Started event is published only after its protected checkpoint
        // is durable and before the provider future is first polled.
        self.session.inner.emitter.emit(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationStarted {
                operation: operation.operation,
                request: operation.request,
                attempt: operation.attempt,
                identity: operation.identity,
                purpose: operation.purpose,
            },
        );
        *self.checkpoint.lock().await = Some(next);
        Ok(())
    }
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

    /// Checks the exact opaque identity at the serialized provider boundary.
    /// The operation may have been derived earlier; an intervening ordinary
    /// turn can retire that identity before the maintenance call is admitted.
    fn cache_identity_matches_last_plan(
        &self,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> bool {
        self.inner
            .execution
            .planner
            .last_committed_plan()
            .is_some_and(|plan| {
                plan.cache_plan()
                    .and_then(|cache| cache.cache_identity())
                    .is_some_and(|current| current == identity)
            })
    }

    /// Acquires a read-only lease when `identity` is still the exact last
    /// provider-committed cache identity.
    ///
    /// The identity is checked after acquiring the ordinary provider-turn
    /// gate, closing the post-dispatch race where a new real turn could commit
    /// a different plan before a host persists identity-bound metadata.
    pub async fn lock_current_cache_identity(
        &self,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> Option<CurrentCacheIdentityLease<'_>> {
        let turn_gate = self.inner.turn_gate.lock().await;
        if !self.cache_identity_matches_last_plan(identity) {
            return None;
        }
        Some(CurrentCacheIdentityLease {
            _turn_gate: turn_gate,
        })
    }

    /// Saves the prepared cache checkpoint while the caller already owns the
    /// session persistence gate. Cache dispatch uses this variant so the
    /// reservation, protected checkpoint, and ordinary SessionStore snapshot
    /// cannot observe different projections.
    async fn begin_cache_checkpoint_locked(
        &self,
        turn: agent_runtime_core::ids::TurnId,
        operation: agent_runtime_core::checkpoint::CacheOperationCheckpoint,
        deadline: Deadline,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let Some(store) = self.inner.shared.checkpoint_store.as_ref() else {
            return Ok(None);
        };
        let checkpoint_sequence = match store.load_latest(&self.inner.id).await? {
            None => 1,
            Some(previous) if previous.state.is_terminal() => {
                previous.watermark.checkpoint_sequence.saturating_add(1)
            }
            Some(_) => {
                return Err(RuntimeError::conflict(
                    "cannot admit a cache operation over a non-terminal checkpoint",
                ));
            }
        };
        let event_sequence = self.inner.emitter.begin_checkpoint_barrier();
        let checkpoint = TurnCheckpoint::cache_operation(
            turn,
            operation,
            self.snapshot(),
            deadline,
            checkpoint_sequence,
            event_sequence,
            self.inner.shared.clock.now(),
        );
        let checkpoint = match checkpoint {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.inner.emitter.end_checkpoint_barrier();
                return Err(error);
            }
        };
        let save = store.save(&checkpoint).await;
        self.inner.emitter.end_checkpoint_barrier();
        save?;
        Ok(Some(checkpoint))
    }

    async fn advance_cache_checkpoint(
        &self,
        checkpoint: TurnCheckpoint,
        state: TurnState,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        let _persist_gate = self.inner.persist_gate.lock().await;
        self.advance_cache_checkpoint_locked(checkpoint, state)
            .await
    }

    /// Advances a cache checkpoint while the caller already owns the shared
    /// persistence gate. This is the only form used by an in-flight cache
    /// dispatch; it keeps checkpoint and SessionStore projections atomic with
    /// respect to ordinary persistence.
    async fn advance_cache_checkpoint_locked(
        &self,
        checkpoint: TurnCheckpoint,
        state: TurnState,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        let event_sequence = self.inner.emitter.begin_checkpoint_barrier();
        let next = match checkpoint.transition(
            state,
            self.snapshot(),
            event_sequence,
            self.inner.shared.clock.now(),
        ) {
            Ok(next) => next,
            Err(error) => {
                self.inner.emitter.end_checkpoint_barrier();
                return Err(error);
            }
        };
        if let Some(store) = self.inner.shared.checkpoint_store.as_ref() {
            let save = store.save(&next).await;
            self.inner.emitter.end_checkpoint_barrier();
            save?;
        } else {
            self.inner.emitter.end_checkpoint_barrier();
        }
        Ok(next)
    }

    /// Completes the ResultReady -> Terminal boundary while the caller holds
    /// the persistence gate for the whole cache operation. Result reduction is
    /// therefore never visible to an ordinary SessionStore snapshot before
    /// ResultReady is protected.
    async fn finalize_cache_checkpoint_locked(
        &self,
        checkpoint: TurnCheckpoint,
        result: &CacheOperationResult,
        operation: &agent_runtime_core::checkpoint::CacheOperationCheckpoint,
        cache_events: Option<CacheEventBatch>,
    ) -> Result<(), CacheFinalizeError> {
        // A rejection is itself the protected preflight decision. Preserve
        // that reason on the reservation through ResultReady and Terminal so
        // recovery can return the exact value without re-running mutable
        // capability checks.
        let mut operation = operation.clone();
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            operation.preflight_rejection = result.rejection_reason;
        }
        let result_state = TurnState::CacheOperationResultReady {
            operation: operation.clone(),
            result: result.checkpoint_result(),
        };
        // A protected store write may fail transiently after the provider
        // result is known. Retry the exact same ResultReady transition once
        // while the persistence gate and cache event batch are still held;
        // this repairs a same-process fault without replaying provider I/O.
        // If the second write also fails, discard the volatile tail and let
        // the caller roll back the unprotected in-memory projection. The
        // durable Prepared/Started checkpoint remains the recovery authority.
        let mut cache_events = cache_events;
        if let Some(cache_events) = cache_events.as_mut() {
            cache_events.mark_result_ready();
        }
        let checkpoint_before_result = checkpoint.clone();
        let checkpoint = match self
            .advance_cache_checkpoint_locked(checkpoint, result_state.clone())
            .await
        {
            Ok(checkpoint) => checkpoint,
            Err(_first_error) => match self
                .advance_cache_checkpoint_locked(checkpoint_before_result, result_state)
                .await
            {
                Ok(checkpoint) => checkpoint,
                Err(second_error) => {
                    return Err(CacheFinalizeError::ResultReady(second_error));
                }
            },
        };
        // The result checkpoint's watermark precedes every deferred lifecycle,
        // evidence, suspension, and usage event. A crash after this save
        // therefore truncates the tail and recovery can republish it once.
        if let Some(cache_events) = cache_events {
            cache_events.flush();
        }
        let cache_turn = cache_operation_turn(&operation.operation);
        let terminal_state = TurnState::CacheOperationTerminal {
            operation,
            result: result.checkpoint_result(),
        };
        self.advance_cache_checkpoint_locked(checkpoint, terminal_state)
            .await
            .map_err(CacheFinalizeError::Terminal)?;
        self.inner.emitter.clear_cache_tail(&cache_turn);
        Ok(())
    }

    /// Repairs a cache operation from a protected checkpoint/result pair.
    ///
    /// This is used both by the same-handle retry after a ResultReady save
    /// fault and by the completed-result fast path after a Terminal save
    /// fault.  It never constructs or polls a provider request.  A Prepared
    /// or Started checkpoint is advanced through the exact protected result
    /// and terminal states; a ResultReady checkpoint only needs its terminal
    /// successor.
    async fn repair_cache_checkpoint_result(
        &self,
        result: &CacheOperationResult,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.shared.checkpoint_store.as_ref() else {
            return Ok(());
        };
        let Some(mut checkpoint) = store.load_latest(&self.inner.id).await? else {
            return Err(RuntimeError::conflict(
                "cache result has no protected checkpoint to repair",
            ));
        };
        checkpoint.validate()?;
        if checkpoint.session != self.inner.id
            || checkpoint.turn != cache_operation_turn(&result.operation)
        {
            return Err(RuntimeError::conflict(
                "cache result does not match its protected checkpoint",
            ));
        }
        match checkpoint.state.clone() {
            TurnState::CacheOperationTerminal { .. } => {}
            TurnState::CacheOperationResultReady { operation, .. } => {
                if operation.operation != result.operation
                    || operation.identity != result.identity
                    || operation.purpose != result.purpose
                {
                    return Err(RuntimeError::conflict(
                        "cache result does not match its ResultReady checkpoint",
                    ));
                }
                let cache_turn = cache_operation_turn(&operation.operation);
                if !self.inner.emitter.cache_tail_published(&cache_turn) {
                    let cache_events = self
                        .inner
                        .emitter
                        .begin_cache_events_for_turn(cache_turn.clone());
                    let checkpoint_result = result.checkpoint_result();
                    let usage = self.cache_usage_for_checkpoint(
                        &checkpoint.snapshot.usage,
                        &operation,
                        &checkpoint_result,
                    )?;
                    self.replay_cache_checkpoint_events(
                        &operation,
                        &checkpoint_result,
                        false,
                        false,
                        usage.as_ref(),
                    );
                    cache_events.flush();
                }
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationTerminal {
                            operation,
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
                self.inner.emitter.clear_cache_tail(&cache_turn);
            }
            TurnState::CacheOperationPrepared { operation }
            | TurnState::CacheOperationStarted { operation } => {
                if operation.operation != result.operation
                    || operation.identity != result.identity
                    || operation.purpose != result.purpose
                    || (result.outcome
                        != agent_runtime_core::event::CacheOperationOutcome::Rejected
                        && operation.attempt != result.attempt)
                {
                    return Err(RuntimeError::conflict(
                        "cache result does not match its in-flight checkpoint",
                    ));
                }
                let cache_events = self
                    .inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(&operation.operation));
                let usage_ledger = self
                    .inner
                    .state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .clone();
                let usage = self.cache_usage_for_checkpoint(
                    &usage_ledger,
                    &operation,
                    &result.checkpoint_result(),
                )?;
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationResultReady {
                            operation: operation.clone(),
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
                self.replay_cache_checkpoint_events(
                    &operation,
                    &result.checkpoint_result(),
                    false,
                    false,
                    usage.as_ref(),
                );
                cache_events.flush();
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationTerminal {
                            operation,
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
            }
            _ => {
                return Err(RuntimeError::conflict(
                    "cache result repair requested for a non-cache checkpoint",
                ));
            }
        }
        let checkpoint_operation = match &checkpoint.state {
            TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
            _ => {
                return Err(RuntimeError::conflict(
                    "cache checkpoint repair did not reach Terminal",
                ));
            }
        };
        self.inner
            .shared
            .cache
            .commit_recovered_result_with_checkpoint(
                &self.inner.id,
                &checkpoint_operation,
                result,
            )?;
        self.inner
            .emitter
            .clear_cache_tail(&cache_operation_turn(&checkpoint_operation.operation));
        Ok(())
    }

    fn emit_cache_prepared(&self, operation: &CacheOperationCheckpoint) {
        self.inner.emitter.emit(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationPrepared {
                operation: operation.operation.clone(),
                request: operation.request.clone(),
                identity: operation.identity.clone(),
                purpose: operation.purpose,
            },
        );
    }

    /// Keeps the post-provider ledger beside a live-only result while a
    /// protected checkpoint save is retried. The ordinary SessionStore
    /// projection is rolled back on that failure, so the exact usage record
    /// must travel with the same-process repair capability.
    fn retain_pending_cache_repair(&self, result: &CacheOperationResult) {
        let usage = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        self.inner
            .shared
            .cache
            .retain_pending_repair(&self.inner.id, result, usage);
    }

    fn restore_pending_cache_usage(&self, usage: UsageLedger) {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage = usage;
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

    /// Attempts one semantic compaction at the current idle turn boundary.
    ///
    /// The operation claims the same admission boundary as user and internal
    /// turns, invokes the configured semantic-summary hook through the normal
    /// driver pipeline, and commits its extension state and usage under the
    /// ordinary persistence gate. A failed model attempt is represented by an
    /// accepted result with a fallback reason and consumes the attempt; it is
    /// never retried automatically. The canonical history remains unchanged.
    pub async fn try_idle_semantic_compaction(
        &self,
    ) -> Result<IdleCompactionAdmission, RuntimeError> {
        // Claim both admission layers before publishing the idle attempt.  In
        // particular, do not release `admission_gate` and then await
        // `turn_gate`: a user can otherwise win admission in that gap and the
        // idle operation would run against the new interval's history.
        let turn_gate = {
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            if self.inner.recovery_deferred {
                return Ok(IdleCompactionAdmission::Busy);
            }
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down || self.inner.cancel.is_cancelled() {
                return Ok(IdleCompactionAdmission::Shutdown);
            }
            if self.inner.user_submission_pending.load(Ordering::Acquire) != 0
                || turns.count != 0
                || self.inner.idle_compaction_attempted.load(Ordering::Acquire)
            {
                return Ok(IdleCompactionAdmission::Busy);
            }
            let turn_gate = match self.inner.turn_gate.try_lock() {
                Ok(turn_gate) => turn_gate,
                Err(_) => return Ok(IdleCompactionAdmission::Busy),
            };
            if self
                .inner
                .idle_compaction_inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Ok(IdleCompactionAdmission::Busy);
            }
            self.inner
                .idle_compaction_attempted
                .store(true, Ordering::Release);
            turn_gate
        };
        // Keep the successful gate guard through snapshot, hook, and
        // persistence.  This prevents a turn admitted after the check from
        // changing the canonical boundary while compaction is in flight.
        let _turn_gate = turn_gate;

        let cache_activity = match CacheActivityGuard::enter(&self.inner) {
            Ok(activity) => activity,
            Err(_) => {
                self.inner
                    .idle_compaction_inflight
                    .store(false, Ordering::Release);
                return Ok(IdleCompactionAdmission::Shutdown);
            }
        };
        let _idle = IdleCompactionGuard {
            inner: self.inner.clone(),
            _cache_activity: cache_activity,
        };
        if self.inner.cancel.is_cancelled() {
            return Ok(IdleCompactionAdmission::Shutdown);
        }

        let (history, usage, boundary_turn) = {
            let state = self.inner.state.lock().expect("session state poisoned");
            let boundary_turn = state
                .manifests
                .last()
                .map(|manifest| manifest.turn.clone())
                .unwrap_or_else(|| TurnId::new("idle-compaction-boundary"));
            (
                Arc::from(state.history.clone().into_boxed_slice()),
                Arc::from(state.usage.records().to_vec().into_boxed_slice()),
                boundary_turn,
            )
        };
        let extension_state = self
            .inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .clone();
        let committed_at = self.inner.shared.clock.now();
        let view = TurnCommitView {
            session: self.inner.id.clone(),
            turn: boundary_turn.clone(),
            finish: TurnFinish::Completed,
            provider_error_kind: None,
            visible_output: false,
            history,
            state: None,
            usage,
            started_at: committed_at,
            committed_at,
        };
        let patches = match self
            .inner
            .shared
            .driver
            .run_idle_compaction_hooks(&view, &extension_state, &self.inner.cancel)
            .await
        {
            Ok(patches) => patches,
            Err(_error) if self.inner.cancel.is_cancelled() => {
                return Ok(IdleCompactionAdmission::Shutdown);
            }
            Err(error) => return Err(error),
        };

        let mut updates = Vec::new();
        let mut usage_records = Vec::new();
        let mut events = Vec::new();
        let mut summary = None;
        let mut fallback_reason = None;
        let mut usage = UsageDelta::new();
        for (descriptor, patch) in patches {
            if descriptor.id().as_str() == SEMANTIC_SUMMARY_COMPONENT_ID {
                summary = protected_summary_from_patch(&patch)?;
            }
            if fallback_reason.is_none() {
                fallback_reason = patch.events.iter().find_map(|event| match event {
                    HarnessEvent::SemanticSummaryFallback { reason } => Some(reason.clone()),
                    _ => None,
                });
            }
            if let Some(state) = patch.state {
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
            for record in &patch.usage {
                usage.merge(&record.delta);
            }
            usage_records.extend(patch.usage);
            events.extend(patch.events);
        }

        let _persist_gate = self.inner.persist_gate.lock().await;
        if self.inner.cancel.is_cancelled() {
            return Ok(IdleCompactionAdmission::Shutdown);
        }
        let previous_extensions = {
            let extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .iter()
                .map(|(namespace, _)| (namespace.clone(), extensions.get(namespace).cloned()))
                .collect::<Vec<_>>()
        };
        let previous_usage = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in &updates {
                extensions.insert(namespace.clone(), state.clone());
            }
        }
        {
            let mut state = self.inner.state.lock().expect("session state poisoned");
            for record in &usage_records {
                state.usage.record(record.clone());
            }
        }
        let save_result = match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        };
        if let Err(error) = save_result {
            {
                let mut extensions = self
                    .inner
                    .execution
                    .extension_state
                    .lock()
                    .expect("session extension state poisoned");
                for (namespace, previous) in previous_extensions {
                    match previous {
                        Some(state) => {
                            extensions.insert(namespace, state);
                        }
                        None => {
                            extensions.remove(&namespace);
                        }
                    }
                }
            }
            self.inner
                .state
                .lock()
                .expect("session state poisoned")
                .usage = previous_usage;
            return Err(error);
        }

        for record in usage_records {
            self.inner
                .emitter
                .emit(Some(boundary_turn.clone()), RuntimeEvent::Usage { record });
        }
        for event in events {
            self.inner
                .emitter
                .emit(Some(boundary_turn.clone()), event.into_runtime_event());
        }
        Ok(IdleCompactionAdmission::Accepted {
            summary,
            fallback_reason,
            usage,
        })
    }

    /// Alias emphasizing that this is the single idle-compaction attempt.
    pub async fn try_idle_compaction(&self) -> Result<IdleCompactionAdmission, RuntimeError> {
        self.try_idle_semantic_compaction().await
    }

    /// Dispatches one conformance-gated synthetic cache operation through the
    /// Runtime mechanism. The operation is attributed with fresh request and
    /// attempt identities and is never retried or routed to the tool executor.
    pub async fn dispatch_cache_operation(
        &self,
        operation: CacheOperationRequest,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let _cache_activity = CacheActivityGuard::enter(&self.inner)?;
        let _cache_gate = self.inner.cache_gate.lock().await;
        // Serialize against ordinary provider turns. This is an admission
        // boundary, not a scheduling policy: if a turn was already serving,
        // it completes first and the exact identity is then rechecked below.
        let _turn_gate = self.inner.turn_gate.lock().await;
        // Keep ordinary persistence out of the result-reduction window. The
        // cache mechanism updates its in-memory projection before returning;
        // holding this gate through ResultReady prevents a concurrent
        // SessionStore snapshot from publishing an unprotected completion.
        let _persist_gate = self.inner.persist_gate.lock().await;
        let cache_snapshot = self
            .inner
            .shared
            .cache
            .snapshot_for_dispatch(&self.inner.id);
        let usage_snapshot = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        let request_id = self.inner.minter.request();
        let attempt_id = self.inner.minter.attempt();
        let operation_fingerprint = operation.fingerprint();
        match self.inner.shared.cache.pending_repair(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some((result, usage))) => {
                self.restore_pending_cache_usage(usage);
                self.inner
                    .shared
                    .cache
                    .restore_pending_repair_state(&self.inner.id, operation.operation());
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        match self.inner.shared.cache.completed_result(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some(result)) => {
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        let reserved_existing = self
            .inner
            .shared
            .cache
            .operation_reserved(&self.inner.id, operation.operation());
        let prepared_retry_checkpoint = if self.inner.shared.checkpoint_store.is_some() {
            match self.inner.shared.checkpoint_store.as_ref() {
                Some(store) => store.load_latest(&self.inner.id).await?,
                None => None,
            }
        } else {
            None
        };
        let prepared_retry_checkpoint = match prepared_retry_checkpoint {
            Some(checkpoint) => match &checkpoint.state {
                TurnState::CacheOperationPrepared {
                    operation: checkpoint_operation,
                }
                | TurnState::CacheOperationStarted {
                    operation: checkpoint_operation,
                } if operation.matches_checkpoint(checkpoint_operation)
                    && (matches!(&checkpoint.state, TurnState::CacheOperationPrepared { .. })
                        || self.cache_start_is_repairable(operation.operation())) =>
                {
                    Some(checkpoint)
                }
                _ => None,
            },
            None => None,
        };
        if let Some(checkpoint) = prepared_retry_checkpoint.as_ref()
            && let TurnState::CacheOperationPrepared {
                operation: checkpoint_operation,
            } = &checkpoint.state
            && let Some(reason) = checkpoint_operation.preflight_rejection
        {
            let result = self.cache_result_from_checkpoint(
                checkpoint_operation,
                &CacheOperationResultCheckpoint {
                    outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                    state: self
                        .inner
                        .shared
                        .cache
                        .current_state(&self.inner.id, &checkpoint_operation.identity),
                    evidence: None,
                    metrics: BTreeMap::new(),
                    rejection_reason: Some(reason),
                    terminal_reason: None,
                },
            );
            self.repair_cache_checkpoint_result(&result).await?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if reserved_existing
            && prepared_retry_checkpoint.is_none()
            && self.inner.shared.checkpoint_store.is_some()
        {
            // A duplicate arriving while a protected cache checkpoint is
            // non-terminal must not try to allocate another revision-zero
            // checkpoint for the same synthetic turn. Resolve it as a
            // structured conflict; recovery owns the existing boundary.
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::Conflict,
                &self.inner.emitter,
            )?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if !reserved_existing
            && prepared_retry_checkpoint.is_none()
            && !self.cache_identity_matches_last_plan(operation.synthetic().identity())
        {
            let checkpoint = if self.inner.shared.checkpoint_store.is_some() {
                let prepared = operation.checkpoint_metadata_with_rejection(
                    Some(request_id.clone()),
                    agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                );
                match self
                    .begin_cache_checkpoint_locked(
                        cache_operation_turn(operation.operation()),
                        prepared,
                        operation.synthetic().deadline(),
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let cache_events = checkpoint.as_ref().map(|_| {
                self.inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
            });
            let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                &self.inner.emitter,
            )?;
            if let Some(checkpoint) = checkpoint {
                let operation_checkpoint = match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation }
                    | TurnState::CacheOperationStarted { operation }
                    | TurnState::CacheOperationResultReady { operation, .. }
                    | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                    _ => operation
                        .checkpoint_metadata(result.request.clone(), result.attempt.clone()),
                };
                match self
                    .finalize_cache_checkpoint_locked(
                        checkpoint,
                        &result,
                        &operation_checkpoint,
                        cache_events,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CacheFinalizeError::ResultReady(error)) => {
                        self.retain_pending_cache_repair(&result);
                        self.inner.shared.cache.rollback_unprotected_result(
                            &self.inner.id,
                            operation.operation(),
                            cache_snapshot.clone(),
                        );
                        self.inner
                            .state
                            .lock()
                            .expect("session state poisoned")
                            .usage = usage_snapshot.clone();
                        return Err(error);
                    }
                    Err(CacheFinalizeError::Terminal(error)) => {
                        self.retain_pending_cache_repair(&result);
                        return Err(error);
                    }
                }
            }
            self.persist_locked().await?;
            return Ok(result);
        }
        let request_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation } => operation.request.clone(),
                _ => None,
            })
            .unwrap_or(request_id);
        let attempt_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationStarted { operation } => operation.attempt.clone(),
                _ => None,
            })
            .unwrap_or(attempt_id);
        let protected_retry = prepared_retry_checkpoint.is_some();
        let reserved = self
            .inner
            .shared
            .cache
            .reserve_synthetic_for_dispatch(&self.inner.id, &operation, &self.inner.cancel)
            .is_ok()
            || (reserved_existing && protected_retry);
        if reserved && self.inner.shared.checkpoint_store.is_none() {
            // The operation id is now durable before any provider future is
            // polled. A crash after Started cannot replay the same action on
            // restart. A SessionStore error is intentionally ambiguous: keep
            // the live reservation rather than reopening provider admission
            // when the store may have committed it before returning the
            // error.
            self.persist_locked().await?;
        }
        let checkpoint = if let Some(checkpoint) = prepared_retry_checkpoint {
            Some(checkpoint)
        } else if self.inner.shared.checkpoint_store.is_some() {
            let prepared = match if self.inner.cancel.is_cancelled() {
                Some(agent_runtime_core::event::CacheOperationReason::Shutdown)
            } else {
                self.inner
                    .shared
                    .cache
                    .preflight_synthetic_reason(&self.inner.id, &operation)
                    .err()
            } {
                Some(reason) => {
                    operation.checkpoint_metadata_with_rejection(Some(request_id.clone()), reason)
                }
                None => operation.checkpoint_metadata(Some(request_id.clone()), None),
            };
            let checkpoint = match self
                .begin_cache_checkpoint_locked(
                    cache_operation_turn(operation.operation()),
                    prepared.clone(),
                    operation.synthetic().deadline(),
                )
                .await
            {
                Ok(Some(checkpoint)) => checkpoint,
                Ok(None) => unreachable!("checkpoint store disappeared during dispatch"),
                Err(error) => {
                    if reserved {
                        self.inner
                            .shared
                            .cache
                            .release_operation(&self.inner.id, operation.operation());
                    }
                    return Err(error);
                }
            };
            self.emit_cache_prepared(&prepared);
            Some(checkpoint)
        } else {
            None
        };
        if reserved && self.inner.shared.checkpoint_store.is_some() {
            self.persist_locked().await?;
        }
        let checkpoint_slot =
            checkpoint.map(|checkpoint| Arc::new(AsyncMutex::new(Some(checkpoint))));
        let start_barrier = checkpoint_slot
            .as_ref()
            .map(|checkpoint| SessionCacheStartBarrier {
                session: self.clone(),
                checkpoint: checkpoint.clone(),
            });
        // Only result-tail events are deferred. Prepared/Started remain
        // visible at their own protected phase boundaries.  The guard also
        // discards the batch if this future is aborted at any later await.
        let cache_events = checkpoint_slot.as_ref().map(|_| {
            self.inner
                .emitter
                .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
        });
        let result = match self
            .inner
            .shared
            .cache
            .dispatch_synthetic(
                self.inner.id.clone(),
                request_id,
                attempt_id,
                operation.clone(),
                &self.inner.emitter,
                self.inner.state.clone(),
                self.inner.cancel.clone(),
                reserved,
                start_barrier
                    .as_ref()
                    .map(|barrier| barrier as &dyn CacheStartBarrier),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(error),
        };
        let checkpoint = if let Some(slot) = checkpoint_slot {
            slot.lock().await.take()
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint {
            let operation_checkpoint = match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation }
                | TurnState::CacheOperationResultReady { operation, .. }
                | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                _ => operation.checkpoint_metadata(result.request.clone(), result.attempt.clone()),
            };
            match self
                .finalize_cache_checkpoint_locked(
                    checkpoint,
                    &result,
                    &operation_checkpoint,
                    cache_events,
                )
                .await
            {
                Ok(()) => {}
                Err(CacheFinalizeError::ResultReady(error)) => {
                    self.retain_pending_cache_repair(&result);
                    self.inner.shared.cache.rollback_unprotected_result(
                        &self.inner.id,
                        operation.operation(),
                        cache_snapshot,
                    );
                    self.inner
                        .state
                        .lock()
                        .expect("session state poisoned")
                        .usage = usage_snapshot;
                    return Err(error);
                }
                Err(CacheFinalizeError::Terminal(error)) => {
                    self.retain_pending_cache_repair(&result);
                    return Err(error);
                }
            }
        }
        self.persist_locked().await?;
        Ok(result)
    }

    /// Derives a maintenance operation from the exact immutable plan that
    /// last crossed this session's provider-start boundary. This is the
    /// consumer-safe path for Runtime extensions: callers cannot rebuild a
    /// prompt or inject an independent model/cache identity.
    pub fn cache_operation_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        purpose: agent_runtime_core::provider::ProviderAttemptPurpose,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheOperationRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheOperationRequest::from_plan(
            operation, &plan, purpose, authority, budget, cancel, deadline,
        )
    }

    /// Derives a bounded cache-handoff operation from the exact last
    /// provider-committed plan. The suffix is appended after the immutable
    /// provider cache boundary and is never persisted or emitted.
    pub fn cache_handoff_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        suffix: crate::cache::CacheHandoffSuffix,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheOperationRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheOperationRequest::from_plan_with_handoff_suffix(
            operation, &plan, suffix, authority, budget, cancel, deadline,
        )
    }

    /// Derives an explicit resource operation from the exact last committed
    /// context plan; resource identity and model remain Runtime-owned.
    pub fn cache_resource_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        kind: agent_runtime_core::provider::CacheResourceOperationKind,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheResourceDispatchRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheResourceDispatchRequest::from_plan(
            operation, &plan, kind, authority, budget, cancel, deadline,
        )
    }

    /// Dispatches one typed explicit-resource cache operation through the
    /// optional provider companion capability.
    pub async fn dispatch_cache_resource(
        &self,
        operation: CacheResourceDispatchRequest,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let _cache_activity = CacheActivityGuard::enter(&self.inner)?;
        let _cache_gate = self.inner.cache_gate.lock().await;
        let _turn_gate = self.inner.turn_gate.lock().await;
        // Keep ordinary persistence out of the result-reduction window; see
        // the synthetic dispatch for the protected-boundary rationale.
        let _persist_gate = self.inner.persist_gate.lock().await;
        let cache_snapshot = self
            .inner
            .shared
            .cache
            .snapshot_for_dispatch(&self.inner.id);
        let usage_snapshot = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        let request_id = self.inner.minter.request();
        let attempt_id = self.inner.minter.attempt();
        let operation_fingerprint = operation.fingerprint();
        match self.inner.shared.cache.pending_repair(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some((result, usage))) => {
                self.restore_pending_cache_usage(usage);
                self.inner
                    .shared
                    .cache
                    .restore_pending_repair_state(&self.inner.id, operation.operation());
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_resource_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        match self.inner.shared.cache.completed_result(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some(result)) => {
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_resource_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        let reserved_existing = self
            .inner
            .shared
            .cache
            .operation_reserved(&self.inner.id, operation.operation());
        let prepared_retry_checkpoint = if self.inner.shared.checkpoint_store.is_some() {
            match self.inner.shared.checkpoint_store.as_ref() {
                Some(store) => store.load_latest(&self.inner.id).await?,
                None => None,
            }
        } else {
            None
        };
        let prepared_retry_checkpoint = match prepared_retry_checkpoint {
            Some(checkpoint) => match &checkpoint.state {
                TurnState::CacheOperationPrepared {
                    operation: checkpoint_operation,
                }
                | TurnState::CacheOperationStarted {
                    operation: checkpoint_operation,
                } if operation.matches_checkpoint(checkpoint_operation)
                    && (matches!(&checkpoint.state, TurnState::CacheOperationPrepared { .. })
                        || self.cache_start_is_repairable(operation.operation())) =>
                {
                    Some(checkpoint)
                }
                _ => None,
            },
            None => None,
        };
        if let Some(checkpoint) = prepared_retry_checkpoint.as_ref()
            && let TurnState::CacheOperationPrepared {
                operation: checkpoint_operation,
            } = &checkpoint.state
            && let Some(reason) = checkpoint_operation.preflight_rejection
        {
            let result = self.cache_result_from_checkpoint(
                checkpoint_operation,
                &CacheOperationResultCheckpoint {
                    outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                    state: self
                        .inner
                        .shared
                        .cache
                        .current_state(&self.inner.id, &checkpoint_operation.identity),
                    evidence: None,
                    metrics: BTreeMap::new(),
                    rejection_reason: Some(reason),
                    terminal_reason: None,
                },
            );
            self.repair_cache_checkpoint_result(&result).await?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if reserved_existing
            && prepared_retry_checkpoint.is_none()
            && self.inner.shared.checkpoint_store.is_some()
        {
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let result = self.inner.shared.cache.reject_resource_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::Conflict,
                &self.inner.emitter,
            )?;
            self.persist_locked().await?;
            return Ok(result);
        }
        let request_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation } => operation.request.clone(),
                _ => None,
            })
            .unwrap_or(request_id);
        let attempt_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationStarted { operation } => operation.attempt.clone(),
                _ => None,
            })
            .unwrap_or(attempt_id);
        if !reserved_existing
            && prepared_retry_checkpoint.is_none()
            && !self.cache_identity_matches_last_plan(operation.identity())
        {
            let checkpoint = if self.inner.shared.checkpoint_store.is_some() {
                let prepared = operation.checkpoint_metadata_with_rejection(
                    Some(request_id.clone()),
                    agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                );
                match self
                    .begin_cache_checkpoint_locked(
                        cache_operation_turn(operation.operation()),
                        prepared,
                        operation.deadline(),
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let cache_events = checkpoint.as_ref().map(|_| {
                self.inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
            });
            let result = self.inner.shared.cache.reject_resource_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                &self.inner.emitter,
            )?;
            if let Some(checkpoint) = checkpoint {
                let operation_checkpoint = match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation }
                    | TurnState::CacheOperationStarted { operation }
                    | TurnState::CacheOperationResultReady { operation, .. }
                    | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                    _ => operation
                        .checkpoint_metadata(result.request.clone(), result.attempt.clone()),
                };
                match self
                    .finalize_cache_checkpoint_locked(
                        checkpoint,
                        &result,
                        &operation_checkpoint,
                        cache_events,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CacheFinalizeError::ResultReady(error)) => {
                        self.retain_pending_cache_repair(&result);
                        self.inner.shared.cache.rollback_unprotected_result(
                            &self.inner.id,
                            operation.operation(),
                            cache_snapshot.clone(),
                        );
                        self.inner
                            .state
                            .lock()
                            .expect("session state poisoned")
                            .usage = usage_snapshot.clone();
                        return Err(error);
                    }
                    Err(CacheFinalizeError::Terminal(error)) => {
                        self.retain_pending_cache_repair(&result);
                        return Err(error);
                    }
                }
            }
            self.persist_locked().await?;
            return Ok(result);
        }
        let protected_retry = prepared_retry_checkpoint.is_some();
        let reserved = self
            .inner
            .shared
            .cache
            .reserve_resource_for_dispatch(&self.inner.id, &operation, &self.inner.cancel)
            .is_ok()
            || (reserved_existing && protected_retry);
        if reserved && self.inner.shared.checkpoint_store.is_none() {
            self.persist_locked().await?;
        }
        let checkpoint = if let Some(checkpoint) = prepared_retry_checkpoint {
            Some(checkpoint)
        } else if self.inner.shared.checkpoint_store.is_some() {
            let prepared = match if self.inner.cancel.is_cancelled() {
                Some(agent_runtime_core::event::CacheOperationReason::Shutdown)
            } else {
                self.inner
                    .shared
                    .cache
                    .preflight_resource_reason(&self.inner.id, &operation)
                    .err()
            } {
                Some(reason) => {
                    operation.checkpoint_metadata_with_rejection(Some(request_id.clone()), reason)
                }
                None => operation.checkpoint_metadata(Some(request_id.clone()), None),
            };
            let checkpoint = match self
                .begin_cache_checkpoint_locked(
                    cache_operation_turn(operation.operation()),
                    prepared.clone(),
                    operation.deadline(),
                )
                .await
            {
                Ok(Some(checkpoint)) => checkpoint,
                Ok(None) => unreachable!("checkpoint store disappeared during dispatch"),
                Err(error) => {
                    if reserved {
                        self.inner
                            .shared
                            .cache
                            .release_operation(&self.inner.id, operation.operation());
                    }
                    return Err(error);
                }
            };
            self.emit_cache_prepared(&prepared);
            Some(checkpoint)
        } else {
            None
        };
        if reserved && self.inner.shared.checkpoint_store.is_some() {
            self.persist_locked().await?;
        }
        let checkpoint_slot =
            checkpoint.map(|checkpoint| Arc::new(AsyncMutex::new(Some(checkpoint))));
        let start_barrier = checkpoint_slot
            .as_ref()
            .map(|checkpoint| SessionCacheStartBarrier {
                session: self.clone(),
                checkpoint: checkpoint.clone(),
            });
        let cache_events = checkpoint_slot.as_ref().map(|_| {
            self.inner
                .emitter
                .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
        });
        let result = match self
            .inner
            .shared
            .cache
            .dispatch_resource(
                self.inner.id.clone(),
                request_id,
                attempt_id,
                operation.clone(),
                &self.inner.emitter,
                self.inner.state.clone(),
                self.inner.cancel.clone(),
                reserved,
                start_barrier
                    .as_ref()
                    .map(|barrier| barrier as &dyn CacheStartBarrier),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(error),
        };
        let checkpoint = if let Some(slot) = checkpoint_slot {
            slot.lock().await.take()
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint {
            let operation_checkpoint = match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation }
                | TurnState::CacheOperationResultReady { operation, .. }
                | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                _ => operation.checkpoint_metadata(result.request.clone(), result.attempt.clone()),
            };
            match self
                .finalize_cache_checkpoint_locked(
                    checkpoint,
                    &result,
                    &operation_checkpoint,
                    cache_events,
                )
                .await
            {
                Ok(()) => {}
                Err(CacheFinalizeError::ResultReady(error)) => {
                    self.retain_pending_cache_repair(&result);
                    self.inner.shared.cache.rollback_unprotected_result(
                        &self.inner.id,
                        operation.operation(),
                        cache_snapshot,
                    );
                    self.inner
                        .state
                        .lock()
                        .expect("session state poisoned")
                        .usage = usage_snapshot;
                    return Err(error);
                }
                Err(CacheFinalizeError::Terminal(error)) => {
                    self.retain_pending_cache_repair(&result);
                    return Err(error);
                }
            }
        }
        self.persist_locked().await?;
        Ok(result)
    }

    /// Queues input for this session and returns a turn-local handle.
    /// Turns execute serially in submission order while events flow through
    /// [`SessionHandle::subscribe`].
    pub fn send(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        self.spawn_turn(input)
    }

    /// Targets additional real-user input to the eligible provider-backed
    /// turn that is currently serving.
    ///
    /// Acceptance is process-local until a matching
    /// [`RuntimeEvent::TurnSteerCommitted`] event. Rejection retains exact
    /// caller ownership of `input` and never queues a later whole turn.
    pub fn steer_current_turn(
        &self,
        expected_turn: Option<&TurnId>,
        input: UserInput,
    ) -> Result<SteerReceipt, SteerRejection> {
        let turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(SteerRejection::new(SteerRejectionReason::Shutdown, input));
        }
        let Some(current) = turns.current.as_ref() else {
            return Err(SteerRejection::new(
                SteerRejectionReason::NoActiveTurn,
                input,
            ));
        };
        let serving = turns
            .steering
            .as_ref()
            .filter(|serving| &serving.turn == current);
        if let Some(expected) = expected_turn {
            if expected != current {
                return Err(SteerRejection::new(
                    SteerRejectionReason::TurnMismatch {
                        expected: expected.clone(),
                        active_turn: current.clone(),
                        steerable: serving.is_some_and(|serving| serving.mailbox.is_open()),
                    },
                    input,
                ));
            }
        }
        let Some(serving) = serving else {
            return Err(SteerRejection::new(
                SteerRejectionReason::NonSteerable {
                    active_turn: current.clone(),
                },
                input,
            ));
        };
        serving.mailbox.admit(input, || self.inner.minter.steer())
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
        self.try_send_internal_if_idle_with_state(input, Vec::new())
    }

    /// Internal admission variant that stages extension state before the
    /// spawned turn creates its first checkpoint.  The state is visible to
    /// the checkpoint snapshot and no state is changed when admission loses
    /// the idle race.
    pub(crate) fn try_send_internal_if_idle_with_state(
        &self,
        input: InternalTurnInput,
        extension_updates: Vec<(String, VersionedSessionState)>,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        self.try_send_internal_if_idle_with_state_and_hook(input, extension_updates, None)
    }

    /// Internal admission variant with a one-shot resolution hook. The hook
    /// runs after the first acceptance checkpoint has committed (or failed)
    /// and before waiters are notified. This lets a caller bind its own
    /// protected state transition to the checkpoint barrier without an
    /// abortable future having to guess whether rollback is still safe.
    pub(crate) fn try_send_internal_if_idle_with_state_and_hook(
        &self,
        input: InternalTurnInput,
        extension_updates: Vec<(String, VersionedSessionState)>,
        acceptance_hook: Option<TurnAcceptanceHook>,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        input.validate()?;
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        if self.inner.recovery_deferred {
            return Ok(InternalTurnAdmission::Busy);
        }
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Ok(InternalTurnAdmission::Shutdown);
        }
        if self.inner.user_submission_pending.load(Ordering::Acquire) != 0 {
            return Ok(InternalTurnAdmission::Busy);
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

        self.inner
            .idle_compaction_attempted
            .store(false, Ordering::Release);
        if !extension_updates.is_empty() {
            if acceptance_hook.is_some() {
                self.inner
                    .execution
                    .stage_extension_state(extension_updates);
            } else {
                self.inner
                    .execution
                    .extension_state
                    .lock()
                    .expect("session extension state poisoned")
                    .extend(extension_updates);
            }
        }

        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        let acceptance = Arc::new(TurnAcceptance::pending_with_hook(acceptance_hook));
        let steer_mailbox = Arc::new(SteerMailbox::new(
            turn_id.clone(),
            self.inner.shared.driver.steer_limits(),
        ));
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
        let task_acceptance = acceptance.clone();
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: acceptance.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
                turns.steering = Some(ServingSteer {
                    turn: tid.clone(),
                    mailbox: task_steer_mailbox.clone(),
                });
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
                    task_steer_mailbox,
                    tid.clone(),
                    input,
                    task_acceptance,
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
            acceptance,
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
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down {
                return Err(RuntimeError::conflict(
                    "session is shutting down and no longer accepts goal controls",
                ));
            }
            if turns.count == 0 {
                turns.count = 1;
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

        let _persist_gate = self.inner.persist_gate.lock().await;
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

        let (turn, cancel) = {
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            let turn = self.inner.minter.turn();
            let cancel = self.inner.cancel.child();
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
            turns.steering = None;
            turns.cancellations.insert(turn.clone(), cancel.clone());
            (turn, cancel)
        };
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
        // Publish user intent before waiting for the shared admission gate.
        // An internal completion that already owns the gate must observe this
        // marker and yield rather than winning the idle check while the user
        // call is queued behind it.
        let _user_submission = UserSubmissionGuard::enter(&self.inner.user_submission_pending);
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
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
        // A real user turn starts a new idle interval. This reset is made
        // while the serialized admission gate is held, so an idle attempt
        // cannot be re-enabled by a stale boundary after user work wins.
        self.inner
            .idle_compaction_attempted
            .store(false, Ordering::Release);
        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        let acceptance = Arc::new(TurnAcceptance::accepted());
        let steer_mailbox = Arc::new(SteerMailbox::new(
            turn_id.clone(),
            self.inner.shared.driver.steer_limits(),
        ));
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
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: acceptance.clone(),
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
                turns.steering = Some(ServingSteer {
                    turn: tid.clone(),
                    mailbox: task_steer_mailbox.clone(),
                });
            }
            inner
                .shared
                .driver
                .run_serving_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    task_steer_mailbox,
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
            acceptance,
        })
    }

    pub(crate) fn spawn_checkpoint_resume(
        &self,
        checkpoint: TurnCheckpoint,
    ) -> Result<TurnHandle, RuntimeError> {
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
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
        let steer_mailbox = checkpoint_is_steerable(&checkpoint).then(|| {
            Arc::new(SteerMailbox::new(
                turn_id.clone(),
                self.inner.shared.driver.steer_limits(),
            ))
        });
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
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: Arc::new(TurnAcceptance::accepted()),
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
                turns.steering = task_steer_mailbox.as_ref().map(|mailbox| ServingSteer {
                    turn: tid,
                    mailbox: mailbox.clone(),
                });
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
                    task_steer_mailbox,
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
            acceptance: Arc::new(TurnAcceptance::accepted()),
        })
    }

    /// Resolves a non-terminal cache checkpoint after restart without replaying
    /// provider I/O. Result/evidence metadata is restored from the protected
    /// checkpoint, lifecycle events are republished after ResultReady, and a
    /// terminal cache checkpoint then permits later direct turns.
    pub(crate) async fn recover_cache_checkpoint(
        &self,
        mut checkpoint: TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        let _cache_gate = self.inner.cache_gate.lock().await;
        let _turn_gate = self.inner.turn_gate.lock().await;
        checkpoint.validate()?;
        let recovering_started =
            matches!(&checkpoint.state, TurnState::CacheOperationStarted { .. });
        let (operation, result_checkpoint, replay_prepared, replay_started) =
            match checkpoint.state.clone() {
                TurnState::CacheOperationPrepared { mut operation } => {
                    // Prepared proves provider I/O had not crossed its durable
                    // start boundary. If the original preflight admitted the
                    // operation, recovery converts that ambiguous unfinished
                    // reservation into one exact Conflict rejection. Protect
                    // the chosen reason on the successor operation metadata so
                    // ResultReady/Terminal validation and later retries never
                    // have to recompute it.
                    let rejection_reason = operation
                        .preflight_rejection
                        .unwrap_or(agent_runtime_core::event::CacheOperationReason::Conflict);
                    operation.preflight_rejection = Some(rejection_reason);
                    (
                        operation.clone(),
                        CacheOperationResultCheckpoint {
                            outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                            state: self
                                .inner
                                .shared
                                .cache
                                .current_state(&self.inner.id, &operation.identity),
                            evidence: None,
                            metrics: BTreeMap::new(),
                            rejection_reason: Some(rejection_reason),
                            terminal_reason: None,
                        },
                        true,
                        false,
                    )
                }
                TurnState::CacheOperationStarted { operation } => (
                    operation.clone(),
                    CacheOperationResultCheckpoint {
                        outcome: agent_runtime_core::event::CacheOperationOutcome::Failed,
                        state: self
                            .inner
                            .shared
                            .cache
                            .current_state(&self.inner.id, &operation.identity),
                        evidence: None,
                        metrics: BTreeMap::new(),
                        rejection_reason: None,
                        terminal_reason: Some(
                            agent_runtime_core::event::CacheOperationReason::Conflict,
                        ),
                    },
                    false,
                    true,
                ),
                TurnState::CacheOperationResultReady { operation, result } => {
                    let usage = self.cache_usage_for_checkpoint(
                        &checkpoint.snapshot.usage,
                        &operation,
                        &result,
                    )?;
                    let result_value = self.cache_result_from_checkpoint(&operation, &result);
                    self.inner
                        .shared
                        .cache
                        .commit_recovered_result_with_checkpoint(
                            &self.inner.id,
                            &operation,
                            &result_value,
                        )?;
                    self.replay_cache_checkpoint_events(
                        &operation,
                        &result,
                        false,
                        false,
                        usage.as_ref(),
                    );
                    let terminal = TurnState::CacheOperationTerminal { operation, result };
                    self.advance_cache_checkpoint(checkpoint, terminal).await?;
                    return self.persist().await;
                }
                TurnState::CacheOperationTerminal { operation, result } => {
                    // Terminal's post-event watermark proves its lifecycle
                    // tail already crossed the protected barrier; restoring
                    // it is state-only and never republishes events.
                    let result_value = self.cache_result_from_checkpoint(&operation, &result);
                    self.inner
                        .shared
                        .cache
                        .commit_recovered_result_with_checkpoint(
                            &self.inner.id,
                            &operation,
                            &result_value,
                        )?;
                    return self.persist().await;
                }
                _ => {
                    return Err(RuntimeError::conflict(
                        "requested cache recovery for a non-cache checkpoint",
                    ));
                }
            };
        let usage = if recovering_started {
            let record = self.append_recovered_cache_usage(&operation)?;
            Some(record)
        } else {
            self.cache_usage_for_checkpoint(
                &checkpoint.snapshot.usage,
                &operation,
                &result_checkpoint,
            )?
        };
        let result_state = TurnState::CacheOperationResultReady {
            operation: operation.clone(),
            result: result_checkpoint.clone(),
        };
        checkpoint = self
            .advance_cache_checkpoint(checkpoint, result_state)
            .await?;
        self.replay_cache_checkpoint_events(
            &operation,
            &result_checkpoint,
            replay_prepared,
            replay_started,
            usage.as_ref(),
        );
        let terminal = TurnState::CacheOperationTerminal {
            operation: operation.clone(),
            result: result_checkpoint.clone(),
        };
        let recovered_result = self.cache_result_from_checkpoint(&operation, &result_checkpoint);
        self.inner
            .shared
            .cache
            .commit_recovered_result_with_checkpoint(
                &self.inner.id,
                &operation,
                &recovered_result,
            )?;
        self.advance_cache_checkpoint(checkpoint, terminal).await?;
        self.persist().await
    }

    fn cache_result_from_checkpoint(
        &self,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
    ) -> CacheOperationResult {
        CacheOperationResult {
            operation: operation.operation.clone(),
            request: operation.request.clone(),
            attempt: operation.attempt.clone(),
            identity: operation.identity.clone(),
            purpose: operation.purpose,
            outcome: result.outcome,
            state: result.state,
            evidence: result.evidence.clone(),
            metrics: result.metrics.clone(),
            rejection_reason: result.rejection_reason,
            terminal_reason: result.terminal_reason,
            captured_output: None,
        }
    }

    /// Finds the one usage record correlated with an admitted cache
    /// operation. ResultReady snapshots retain the complete ledger, so the
    /// record itself need not be duplicated in redaction-safe result
    /// metadata. Requiring uniqueness prevents replay from selecting an
    /// unrelated provider attempt after a journal splice.
    fn cache_usage_for_checkpoint(
        &self,
        ledger: &agent_runtime_core::usage::UsageLedger,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
    ) -> Result<Option<UsageRecord>, RuntimeError> {
        let matches = ledger
            .records()
            .iter()
            .filter(|record| {
                record.source == UsageSource::ProviderAttempt
                    && record.provenance.request == operation.request
                    && record.provenance.attempt == operation.attempt
                    && record.provenance.attempt_purpose == Some(operation.purpose)
                    && record.provenance.cache_identity.as_ref() == Some(&operation.identity)
            })
            .cloned()
            .collect::<Vec<_>>();
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            if !matches.is_empty() {
                return Err(RuntimeError::conflict(
                    "rejected cache checkpoint unexpectedly carries provider usage",
                ));
            }
            return Ok(None);
        }
        match matches.as_slice() {
            [record] => Ok(Some(record.clone())),
            [] => Err(RuntimeError::conflict(
                "admitted cache checkpoint is missing its correlated usage record",
            )),
            _ => Err(RuntimeError::conflict(
                "cache checkpoint has multiple correlated usage records",
            )),
        }
    }

    /// Started recovery crossed the provider barrier but has no provider
    /// response to account for. Append one sparse failed-attempt record so a
    /// resumed terminal state remains provenance-complete without inventing
    /// any billed token count.
    fn append_recovered_cache_usage(
        &self,
        operation: &CacheOperationCheckpoint,
    ) -> Result<UsageRecord, RuntimeError> {
        let request = operation.request.clone().ok_or_else(|| {
            RuntimeError::conflict("started cache checkpoint is missing request attribution")
        })?;
        let attempt = operation.attempt.clone().ok_or_else(|| {
            RuntimeError::conflict("started cache checkpoint is missing attempt attribution")
        })?;
        let record = UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance {
                request: Some(request),
                attempt: Some(attempt),
                tool_call: None,
                purpose: None,
                attempt_purpose: Some(operation.purpose),
                cache_identity: Some(operation.identity.clone()),
                failed: true,
            },
            delta: UsageDelta::new(),
        };
        let mut state = self.inner.state.lock().expect("session state poisoned");
        if state.usage.records().iter().any(|existing| {
            existing.source == record.source
                && existing.provenance.request == record.provenance.request
                && existing.provenance.attempt == record.provenance.attempt
        }) {
            return Err(RuntimeError::conflict(
                "started cache checkpoint already has an attempted usage record",
            ));
        }
        state.usage.record(record.clone());
        Ok(record)
    }

    fn replay_cache_checkpoint_events(
        &self,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
        replay_prepared: bool,
        replay_started: bool,
        usage: Option<&UsageRecord>,
    ) {
        if replay_prepared {
            self.inner.emitter.emit_cache(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationPrepared {
                    operation: operation.operation.clone(),
                    request: operation.request.clone(),
                    identity: operation.identity.clone(),
                    purpose: operation.purpose,
                },
            );
        }
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            self.inner.emitter.emit_cache(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationRejected {
                    operation: operation.operation.clone(),
                    request: operation.request.clone(),
                    attempt: operation.attempt.clone(),
                    identity: operation.identity.clone(),
                    purpose: operation.purpose,
                    reason: result
                        .rejection_reason
                        .unwrap_or(agent_runtime_core::event::CacheOperationReason::Conflict),
                },
            );
        } else {
            if replay_started {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheOperationStarted {
                        operation: operation.operation.clone(),
                        request: operation.request.clone(),
                        attempt: operation.attempt.clone(),
                        identity: operation.identity.clone(),
                        purpose: operation.purpose,
                    },
                );
            }
            if let Some(evidence) = &result.evidence {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                        evidence: evidence.clone(),
                    },
                );
                if evidence.suspends_maintenance() {
                    self.inner.emitter.emit_cache(
                        Some(cache_operation_turn(&operation.operation)),
                        RuntimeEvent::CacheOperationSuspended {
                            request: operation.request.clone(),
                            attempt: operation.attempt.clone(),
                            identity: operation.identity.clone(),
                            operation: Some(operation.operation.clone()),
                            reason: result.terminal_reason.unwrap_or(
                                agent_runtime_core::event::CacheOperationReason::CacheMiss,
                            ),
                        },
                    );
                }
            }
            if let Some(record) = usage {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::Usage {
                        record: record.clone(),
                    },
                );
            }
        }
        self.inner.emitter.emit_cache(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationCompleted {
                operation: operation.operation.clone(),
                request: operation.request.clone(),
                attempt: operation.attempt.clone(),
                identity: operation.identity.clone(),
                purpose: operation.purpose,
                outcome: result.outcome,
                reason: result.terminal_reason,
                metrics: result.metrics.clone(),
            },
        );
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

    /// Runs `f` over the current conversation history without cloning it.
    ///
    /// The session state lock is held while `f` runs, so `f` must stay a
    /// short synchronous projection — a tail scan, not a blocking wait.
    pub fn with_history<R>(&self, f: impl FnOnce(&[Message]) -> R) -> R {
        let state = self.inner.state.lock().expect("session state poisoned");
        f(&state.history)
    }

    /// Restores a protected semantic-summary extension only when canonical
    /// and protected startup state did not already provide one.
    ///
    /// This narrow cold-resume seam is intended for hosts whose ordinary
    /// session store deliberately omits Sensitive extension namespaces and
    /// retains them in a separate protected artifact. It is fail-closed: the
    /// current summary component revision, source session, canonical history
    /// prefix, and content-derived summary revision must all match, and no
    /// turn or cache operation may be active. Existing Runtime-restored state
    /// always wins and is never overwritten.
    pub fn restore_semantic_summary_if_absent(
        &self,
        persisted: VersionedSessionState,
    ) -> Result<bool, RuntimeError> {
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        {
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down
                || turns.count != 0
                || self.inner.cancel.is_cancelled()
                || self.inner.user_submission_pending.load(Ordering::Acquire) != 0
                || self.inner.cache_active.load(Ordering::Acquire) != 0
            {
                return Err(RuntimeError::conflict(
                    "semantic summary restore requires an idle live session",
                ));
            }
        }
        let expected_revision = self
            .inner
            .shared
            .driver
            .semantic_summary_revision()
            .ok_or_else(|| {
                RuntimeError::config(
                    "semantic summary restore requires a configured summary component",
                )
            })?;
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(
                "semantic summary restore revision does not match the active component",
            ));
        }
        {
            let extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            if extensions.contains_key(SEMANTIC_SUMMARY_COMPONENT_ID) {
                return Ok(false);
            }
        }

        let summary = protected_semantic_summary_from_state(&persisted, UsageDelta::new())?;
        if summary.source_artifact.provenance.session != self.inner.id {
            return Err(RuntimeError::conflict(
                "semantic summary restore artifact belongs to another session",
            ));
        }
        {
            let state = self.inner.state.lock().expect("session state poisoned");
            if summary.omit_prefix > state.history.len()
                || (summary.omit_prefix < state.history.len()
                    && state.history[summary.omit_prefix].role != Role::User)
            {
                return Err(RuntimeError::conflict(
                    "semantic summary restore would split or exceed canonical history",
                ));
            }
            let encoded =
                serde_json::to_vec(&state.history[..summary.omit_prefix]).map_err(|error| {
                    RuntimeError::internal(format!(
                        "failed to verify restored semantic summary source: {error}"
                    ))
                })?;
            if Fingerprint::of(encoded) != summary.source_fingerprint {
                return Err(RuntimeError::conflict(
                    "semantic summary restore source no longer matches canonical history",
                ));
            }
        }
        self.inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .insert(SEMANTIC_SUMMARY_COMPONENT_ID.to_owned(), persisted);
        Ok(true)
    }

    /// A snapshot of the session's canonical state.
    pub fn snapshot(&self) -> SessionSnapshot {
        let state = self.inner.state.lock().expect("session state poisoned");
        let mut extension_state = self.inner.execution.snapshot_extension_state();
        if let Some(cache) = self.inner.shared.cache.persisted_session(&self.inner.id) {
            extension_state.insert(
                crate::cache::CACHE_MECHANISM_STATE_NAMESPACE.to_owned(),
                cache,
            );
        }
        SessionSnapshot {
            id: self.inner.id.clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self
                .inner
                .minter
                .snapshot(self.inner.emitter.next_sequence()),
            extension_state,
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
        let _persist_gate = self.inner.persist_gate.lock().await;
        self.persist_locked().await
    }

    /// Persists without taking `persist_gate`. Callers must already own the
    /// gate. Cache dispatch uses this to keep reservation, result reduction,
    /// and protected checkpoint publication in one serialized interval.
    async fn persist_locked(&self) -> Result<(), RuntimeError> {
        match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        }
    }

    /// Applies runtime-owned extension updates and persists one snapshot as a
    /// single transaction against ordinary session persistence. A failed
    /// store write restores the exact in-memory extension values that were
    /// present before the transaction, while the persistence gate prevents a
    /// concurrent ordinary save from observing the uncommitted updates.
    pub(crate) async fn persist_with_extension_state(
        &self,
        updates: impl IntoIterator<Item = (String, VersionedSessionState)>,
    ) -> Result<(), RuntimeError> {
        let _persist_gate = self.inner.persist_gate.lock().await;
        let Some(store) = self.inner.shared.session_store.as_ref() else {
            return Ok(());
        };
        let updates = updates.into_iter().collect::<Vec<_>>();
        let previous = {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            let previous = updates
                .iter()
                .map(|(namespace, _)| (namespace.clone(), extensions.get(namespace).cloned()))
                .collect::<Vec<_>>();
            for (namespace, state) in &updates {
                extensions.insert(namespace.clone(), state.clone());
            }
            previous
        };
        let result = store.save(&self.snapshot()).await;
        if result.is_err() {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in previous {
                match state {
                    Some(state) => {
                        extensions.insert(namespace, state);
                    }
                    None => {
                        extensions.remove(&namespace);
                    }
                }
            }
        }
        result
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
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .count
                    == 0
                    && self.inner.cache_active.load(Ordering::Acquire) == 0
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
            let cache_drained = tokio::time::timeout(timeout, async {
                loop {
                    let changed = self.inner.turns_changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if self.inner.cache_active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .is_ok();
            if !cache_drained {
                return Err(RuntimeError::internal(
                    "cache operation did not drain before session shutdown timeout",
                ));
            }
        }

        self.inner.emitter.emit(None, RuntimeEvent::SessionShutdown);
        let _persist_gate = self.inner.persist_gate.lock().await;
        let save_result = match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        };
        *shutdown_complete = true;
        self.inner.active_session_lease.release();
        save_result
    }
}

fn checkpoint_is_steerable(checkpoint: &TurnCheckpoint) -> bool {
    !matches!(
        checkpoint.state,
        TurnState::LocalActionAccepted { .. }
            | TurnState::LocalActionPrepared { .. }
            | TurnState::LocalActionExecuting { .. }
            | TurnState::LocalActionOutcomeReady { .. }
            | TurnState::LocalActionResultReady { .. }
            | TurnState::Completing { .. }
            | TurnState::PublishingTerminal { .. }
            | TurnState::Terminal { .. }
            | TurnState::CacheOperationPrepared { .. }
            | TurnState::CacheOperationStarted { .. }
            | TurnState::CacheOperationResultReady { .. }
            | TurnState::CacheOperationTerminal { .. }
    )
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
        if turns
            .steering
            .as_ref()
            .is_some_and(|serving| serving.turn == self.turn)
        {
            turns.steering = None;
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
    acceptance: Arc<TurnAcceptance>,
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        self.acceptance.resolve(Err(RuntimeError::cancelled(
            "turn ended before its acceptance checkpoint committed",
        )));
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        turns.cancellations.remove(&self.turn);
        turns.internal_goals.remove(&self.turn);
        if turns.current.as_ref() == Some(&self.turn) {
            turns.current = None;
        }
        if turns
            .steering
            .as_ref()
            .is_some_and(|serving| serving.turn == self.turn)
        {
            turns.steering = None;
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
