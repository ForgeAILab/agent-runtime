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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};

use agent_runtime_core::approval::{
    ApprovalDecision, ApprovalOrigin, ApprovalPolicy, ApprovalRequest,
};
use agent_runtime_core::artifact::{
    ArtifactLineage, ArtifactProvenance, ArtifactRef, ArtifactStore, ArtifactTransfer,
};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{
    CheckpointStore, CheckpointWatermark, TurnCheckpoint, TurnState,
};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{
    InternalTurnInput, InternalTurnSensitivity, InternalTurnSource, MAX_INTERNAL_TURN_CHARS,
    Message, UserInput,
};
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
use crate::runtime::emitter::RuntimeEventStream;
use crate::runtime::engine::Runtime;
use crate::runtime::session::{InternalTurnAdmission, SessionHandle, TurnHandle};
use crate::runtime::state::{artifact_references_for_turn, returned_interaction_from_state};
use crate::tool::SecurityConfig;

/// The host-defined permission delegation operations request from the
/// composed authorization path. Default-deny: a host that never covers it
/// with an authoritative check cannot delegate.
pub const DELEGATION_PERMISSION: &str = "agent.delegate";

/// Parent session extension-state namespace containing durable child records.
pub const CHILD_CATALOG_NAMESPACE: &str = "agent-runtime.delegation.children";
const CHILD_CATALOG_REVISION: &str = "resumable-child-catalog-1";
const CHILD_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Parent-owned protected automatic child-outcome cursor state.
pub const CHILD_OUTCOME_CURSOR_NAMESPACE: &str = "agent-runtime.delegation.child-outcomes";
// Version two binds completed outcome values to their originating turn. Old
// protected ledgers do not carry enough identity to validate a key/value pair,
// so they fail closed at restore instead of being silently upgraded.
const CHILD_OUTCOME_CURSOR_REVISION: &str = "child-outcome-cursor-2";
const CHILD_OUTCOME_CURSOR_SCHEMA_VERSION: u32 = 2;

/// Default maximum time a host waits for a child before receiving a running
/// projection.  The timeout never cancels the child.
pub const DEFAULT_DELEGATION_WAIT: Duration = Duration::from_secs(5);
/// Absolute runtime hard cap for one delegation wait.
pub const HARD_MAX_DELEGATION_WAIT: Duration = Duration::from_secs(30);

/// Builds the runtime a child session runs on.
///
/// The host owns provider/model routing, tool registration, workspace
/// adapters, and policy composition — the coordinator then applies the
/// spec's tool-view scope and strips delegation-management tools.
mod coordinator;
mod lifecycle;
mod monitor;
mod persistence;
mod types;

use monitor::*;
use persistence::*;
pub use types::{
    CapacityPolicy, ChildCompletionAdmission, ChildCompletionAdmissionRequest, ChildDurability,
    ChildNeedsInputProjection, ChildOutcomeCursor, ChildOutcomeIdentity, ChildOutcomeKey,
    ChildRuntimeFactory, ChildSessionRecord, ChildState, ChildStatus, ChildTaskOutcome,
    ChildTaskResult, DelegationCapacity, DelegationConfig, DelegationCoordinator, DelegationLimits,
    DelegationWaitOptions, DurableChildCatalog, DurableChildSpec, SpawnOutcome,
};
use types::{
    ChildBinding, ChildEntry, CoordinatorInner, ProtectedChildOutcomeState, QueuedSpawn,
    TaskOutcomeKey, checkpoint_can_resume,
};
