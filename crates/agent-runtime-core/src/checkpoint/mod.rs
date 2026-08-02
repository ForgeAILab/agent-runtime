//! Protected, exact turn checkpoints.
//!
//! Checkpoints are deliberately separate from observability events. Events are
//! bounded and may redact sensitive values; a checkpoint contains the exact
//! provider request, prepared invocation, and committed results needed to
//! resume without repeating an external operation. Hosts therefore persist
//! this contract under their protected-state policy rather than in an audit
//! journal.

use std::collections::BTreeSet;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_registry::{Fingerprint, FingerprintHasher};

use crate::clock::{Deadline, Timestamp};
use crate::content::{ContentPart, InternalTurnInput, ToolCall, ToolResultBlock, UserInput};
use crate::error::RuntimeError;
use crate::event::TurnFinish;
use crate::ids::{AttemptId, RequestId, SessionId, ToolCallId, TurnId};
use crate::interaction::{InteractionRequest, InteractionResponse};
use crate::provider::{FinishReason, ProviderErrorKind, ProviderRequest};
use crate::store::SessionSnapshot;
use crate::tool::{PreparedToolCall, ToolOutcome};

/// The protected-checkpoint wire schema.
///
/// Version 2 adds exact attributed internal-turn input and terminal provider
/// error classification to the same unreleased protected contract.
pub const CHECKPOINT_SCHEMA_VERSION: u32 = 2;

/// The direct turn-machine transition-table revision.
///
/// A runtime MUST reject a checkpoint with a transition revision it cannot
/// execute equivalently. Silent best-effort recovery could repeat a provider
/// call or tool side effect. Revision 1 is the initial transition table from
/// the same unreleased change and includes interaction barriers.
pub const TURN_TRANSITION_REVISION: u32 = 3;

/// Event/checkpoint progress connecting protected state to the redacted
/// observability stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointWatermark {
    /// Monotonic checkpoint sequence within one session, starting at one.
    pub checkpoint_sequence: u64,
    /// The next event-envelope sequence when this checkpoint was written.
    ///
    /// For a non-terminal recovery, a host with a durable redacted journal
    /// truncates records whose sequence is greater than or equal to this
    /// value before starting the runtime. Events before the value are known
    /// to precede this protected state; events at/after it are a crash-window
    /// tail that recovery deterministically republishes or discards.
    pub event_sequence: u64,
}

impl CheckpointWatermark {
    /// Creates a watermark.
    pub fn new(checkpoint_sequence: u64, event_sequence: u64) -> Self {
        Self {
            checkpoint_sequence,
            event_sequence,
        }
    }

    /// Advances to the next checkpoint at the supplied event boundary.
    pub fn next(self, event_sequence: u64) -> Self {
        Self {
            checkpoint_sequence: self.checkpoint_sequence.saturating_add(1),
            event_sequence,
        }
    }
}

/// A fully assembled, successful provider-attempt response.
///
/// This is the first durable state after provider I/O. Restoring it reuses the
/// response rather than calling the provider again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssembledModelResponse {
    /// Successful provider-attempt identity.
    pub attempt: AttemptId,
    /// Committed visible text.
    pub text: String,
    /// Committed reasoning parts retained for same-turn continuation.
    pub reasoning: Vec<ContentPart>,
    /// Fully assembled and schema-validated tool calls.
    pub tool_calls: Vec<ToolCall>,
    /// Tool names advertised by the exact frozen request that produced this
    /// response. Recovery and execution reject registered-but-inactive calls
    /// against this boundary rather than the runtime-wide implementation map.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advertised_tools: Vec<String>,
    /// The successful attempt's terminal reason.
    pub finish: FinishReason,
}

/// Exact disposition of every source-call slot while a mixed batch is
/// suspended for host interaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum ToolSlotCheckpoint {
    /// Exact prepared action, including an interaction action or a later
    /// ordinary action that has not executed yet.
    Prepared(PreparedToolCall),
    /// Deterministic preparation/authorization/approval result already known
    /// before any invocation began.
    CanonicalResult(ToolResultBlock),
}

impl ToolSlotCheckpoint {
    /// Source call identity represented by this slot.
    pub fn call_id(&self) -> &ToolCallId {
        match self {
            Self::Prepared(prepared) => prepared.call_id(),
            Self::CanonicalResult(result) => &result.call_id,
        }
    }

    /// Source tool name represented by this slot.
    pub fn tool_name(&self) -> &str {
        match self {
            Self::Prepared(prepared) => prepared.tool(),
            Self::CanonicalResult(result) => &result.name,
        }
    }
}

/// Serializable state of the one canonical direct turn machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TurnState {
    /// User input was accepted and appended to canonical history.
    Accepted {
        /// Exact accepted input.
        input: UserInput,
    },
    /// Attributed internal input was accepted without appending a fabricated
    /// user-role message to canonical history.
    InternalAccepted {
        /// Exact bounded input, also retained on the checkpoint while the
        /// state machine advances beyond this initial state.
        input: InternalTurnInput,
    },
    /// An explicit host-requested tool action was accepted without appending
    /// model-facing conversation history.
    LocalActionAccepted {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
    },
    /// A local action has an exact prepared invocation durably recorded before
    /// approval or execution.
    LocalActionPrepared {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Canonical prepared action; recovery reauthorizes it before use.
        prepared: PreparedToolCall,
    },
    /// A local action crossed its pre-invocation durability barrier.
    ///
    /// Recovery never replays this state because the external outcome is
    /// indeterminate until a subsequent raw-outcome checkpoint exists.
    LocalActionExecuting {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Exact prepared action that may have executed.
        prepared: PreparedToolCall,
    },
    /// A local action returned an exact raw outcome before fallible harness
    /// processing or output bounding.
    LocalActionOutcomeReady {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Exact unbounded serializable outcome.
        outcome: ToolOutcome,
    },
    /// A local action's canonical bounded result and component state are
    /// durable and ready for terminal publication.
    LocalActionResultReady {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Canonical committed local result.
        result: ToolResultBlock,
    },
    /// Context planning is about to run.
    Planning {
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// A fully planned provider request is ready to be called.
    CallingModel {
        /// Logical request identity.
        request_id: RequestId,
        /// Exact request derived from the authoritative context plan.
        request: ProviderRequest,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Provider I/O finished and the successful response is assembled.
    ModelResponseReady {
        /// Logical request identity.
        request_id: RequestId,
        /// Exact assembled response.
        response: AssembledModelResponse,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Immutable prepared calls are waiting for security approval.
    AwaitingApproval {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in their canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// An authority-free task interaction is waiting on its host.
    AwaitingInteraction {
        /// Provider request that produced the mixed tool batch.
        request_id: RequestId,
        /// Exact provider calls in canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared/rejected disposition of every source slot.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed before the exclusive interaction slot.
        completed: Vec<ToolResultBlock>,
        /// Index of the interaction call in `source_calls`/`slots`.
        interaction_index: usize,
        /// Exact protected interaction request.
        request: InteractionRequest,
        /// Exact accepted outcome, once the broker resolves. Persisted before
        /// it becomes the canonical interaction tool result.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<InteractionResponse>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// A tool invocation (or resolved interaction) returned an exact raw
    /// outcome. This checkpoint is written before any fallible harness
    /// processor or irreversible model-facing output bound is applied.
    ToolOutcomeReady {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed before this outcome.
        completed: Vec<ToolResultBlock>,
        /// Source slot whose raw outcome is durable.
        outcome_index: usize,
        /// Exact unbounded serializable tool outcome.
        outcome: ToolOutcome,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Prepared calls are executing, with an ordered committed prefix.
    ExecutingTools {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in their canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed to canonical history.
        completed: Vec<ToolResultBlock>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Canonical state is complete and is being durably committed.
    Completing {
        /// Terminal result to commit.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
        /// Typed provider failure that caused this terminal result, when one
        /// exists. Persisted so post-commit policy is identical after restart.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_error_kind: Option<ProviderErrorKind>,
    },
    /// Protected post-hook terminal state is durable and its one terminal
    /// event is ready to be published.
    ///
    /// The snapshot already contains turn-commit component state and usage,
    /// and the watermark follows their projected events. Recovery truncates
    /// the journal at this state's watermark and republishes
    /// `TurnCompleted` without re-running hooks; only the subsequent
    /// `Terminal` checkpoint proves that publication crossed the host's
    /// durability barrier.
    PublishingTerminal {
        /// Terminal result being published.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
    },
    /// The completed turn is durably committed.
    Terminal {
        /// Terminal result.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
    },
}

/// One protected checkpoint of a direct turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnCheckpoint {
    /// Protected checkpoint wire schema.
    pub schema_version: u32,
    /// Transition-table revision required to resume equivalently.
    pub transition_revision: u32,
    /// Session identity.
    pub session: SessionId,
    /// Turn identity.
    pub turn: TurnId,
    /// Monotonic state revision within this turn, starting at zero.
    pub state_revision: u64,
    /// Fingerprint binding the exact operation represented by `state`.
    pub operation_fingerprint: Fingerprint,
    /// Exact canonical-history index owned by the accepted input.
    pub active_history_start: usize,
    /// Exact attributed input for an internal turn. Retained after the
    /// initial state so any later checkpoint resumes with identical context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_input: Option<InternalTurnInput>,
    /// Whether committed provider output in this turn contains visible text.
    pub visible_output: bool,
    /// Current direct-machine state.
    pub state: TurnState,
    /// Exact canonical session state at this boundary.
    pub snapshot: SessionSnapshot,
    /// Absolute turn deadline retained across restart.
    pub deadline: Deadline,
    /// Link to checkpoint and observability progress.
    pub watermark: CheckpointWatermark,
    /// Host-clock timestamp of this write.
    pub updated: Timestamp,
}

mod store;
mod transition;
mod validation;

pub use store::CheckpointStore;
use validation::*;

#[cfg(test)]
mod tests;
