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

mod cache;
mod lifecycle;
mod recovery;
mod turns;

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
    /// Startup interrupted this turn after re-authorizing changed abilities.
    pub(crate) interrupted_on_resume: Option<TurnId>,
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

/// Protected result metadata returned by an accepted idle compaction.
pub type IdleCompactionSummary = ProtectedSemanticSummary;

/// Compatibility name for hosts that model the method as a result rather than
/// an admission operation.
pub type IdleCompactionResult = IdleCompactionAdmission;

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

/// A cache finalization failure carries the protected phase it reached. A
/// ResultReady write failure requires rolling back the in-memory reduction;
/// a later Terminal write failure must retain it because ResultReady and its
/// event watermark are already durable and recovery is now authoritative.
#[derive(Debug)]
enum CacheFinalizeError {
    ResultReady(RuntimeError),
    Terminal(RuntimeError),
}

struct LocalToolGuard {
    inner: Arc<SessionInner>,
    turn: TurnId,
}

struct GoalControlGuard {
    inner: Arc<SessionInner>,
}

struct ActiveTurnGuard {
    inner: Arc<SessionInner>,
    ticket: u64,
    turn: TurnId,
    completion: Arc<TurnCompletion>,
    acceptance: Arc<TurnAcceptance>,
}
