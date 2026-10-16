//! The external agent boundary.
//!
//! An [`ExternalAgentBackend`] executes one whole turn outside the runtime's
//! provider loop. It exists for installed coding agents — a CLI that owns
//! its own conversation, runs its own tools under its own policy, and streams
//! its own events. Such an agent is not a model, and presenting it as one
//! would create two owners for history, tools, retries, and authority.
//!
//! The runtime keeps what it already owns: turn identity, admission,
//! cancellation, the canonical event stream, usage records, and persistence.
//! The backend owns only what happens between a turn starting and its terminal
//! event.
//!
//! The backend's own tools remain observations. Host tools requested through
//! [`ExternalToolDispatch`] are dispatched by the runtime with its ordinary
//! authorization, approval, output bounds, history, and checkpoints. Handle
//! calls run one at a time and commit in order around the backend's text.
//! The optional MCP [`ExternalToolBridge`] lets the CLI call runtime tools
//! itself, with up to eight concurrent calls and no canonical history writes.
//! Both paths can be used in one turn. Internal turns also run on the backend.
//! Recovery interrupts unfinished external turns without re-entering the
//! backend or replaying a dispatched call.
//!
//! External turns do not plan context, call providers, maintain prompt caches,
//! or retry. Backend observations are available in a bounded, volatile log
//! through the session view; they never become canonical runtime tool calls.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::content::{ToolResultBlock, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::TurnId;
use agent_runtime_core::tool::ToolSpec;
use agent_runtime_core::usage::UsageDelta;
use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

pub use crate::agent::external_capabilities::{
    AllowedTool, ExternalCapabilities, ExternalMcpServer, ExternalMcpTransport, ExternalToolBridge,
    ExternalToolPolicy, RUNTIME_BRIDGE_SERVER_NAME, SkillBundle,
};

/// Extension-state namespace holding the continuation identity.
pub const EXTERNAL_AGENT_STATE_NAMESPACE: &str = "agent.external";

/// Schema version for the persisted continuation identity.
pub const EXTERNAL_AGENT_STATE_SCHEMA_VERSION: u32 = 1;

/// Longest backend session identity the runtime will store or offer.
///
/// Backends report opaque ids (a UUID today, for both known CLIs). The bound
/// exists so a misbehaving backend cannot grow session state without limit.
pub const MAX_EXTERNAL_SESSION_ID_BYTES: usize = 256;

/// A backend's own conversation identity, opaque to the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExternalSessionId(String);

impl ExternalSessionId {
    /// Accepts an identity within the stored bound.
    ///
    /// Rejects empty, oversized, and NUL-bearing values rather than storing
    /// something that cannot be handed back on a later turn.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RuntimeError::config(
                "external agent session identity cannot be empty",
            ));
        }
        if value.len() > MAX_EXTERNAL_SESSION_ID_BYTES {
            return Err(RuntimeError::config(
                "external agent session identity is outside the stored bound",
            ));
        }
        if value.contains('\0') {
            return Err(RuntimeError::config(
                "external agent session identity cannot contain NUL",
            ));
        }
        Ok(Self(value))
    }

    /// The identity as the backend reported it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A turn-scoped channel into the runtime's ordinary tool path.
#[derive(Clone, Debug)]
pub struct ExternalToolDispatch {
    specs: Arc<Vec<ToolSpec>>,
    sender: Option<mpsc::UnboundedSender<ExternalToolCall>>,
    closed: Cancellation,
}

impl ExternalToolDispatch {
    /// A handle that lists nothing and refuses every call.
    pub fn disabled() -> Self {
        let closed = Cancellation::new();
        closed.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        Self {
            specs: Arc::new(Vec::new()),
            sender: None,
            closed,
        }
    }

    /// The tool specifications offered for this turn.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.specs.as_ref().clone()
    }

    /// Runs one call through the runtime's tool path. Dropping the
    /// returned future abandons the call.
    pub async fn call(
        &self,
        name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let name = name.into();
        if self.closed.is_cancelled() {
            return Err(RuntimeError::tool("external tool dispatch is closed"));
        }
        if !self.specs.iter().any(|spec| spec.name == name) {
            return Err(RuntimeError::tool(format!(
                "tool `{name}` is not offered by this handle"
            )));
        }
        let (reply, receive) = oneshot::channel();
        let cancel = Cancellation::new();
        let _abandon = AbandonCall(cancel.clone());
        self.sender
            .as_ref()
            .ok_or_else(|| RuntimeError::tool("external tool dispatch is closed"))?
            .send(ExternalToolCall {
                name,
                arguments,
                reply,
                cancel,
            })
            .map_err(|_| RuntimeError::tool("external tool turn has ended"))?;
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => Err(RuntimeError::tool("external tool turn has ended")),
            result = receive => result.unwrap_or_else(|_| Err(RuntimeError::tool("external tool turn has ended"))),
        }
    }

    pub(crate) fn channel(
        specs: Vec<ToolSpec>,
    ) -> (Self, mpsc::UnboundedReceiver<ExternalToolCall>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                specs: Arc::new(specs),
                sender: Some(sender),
                closed: Cancellation::new(),
            },
            receiver,
        )
    }

    pub(crate) fn close(&self) {
        self.closed
            .cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
    }
}

struct AbandonCall(Cancellation);

impl Drop for AbandonCall {
    fn drop(&mut self) {
        self.0
            .cancel(agent_runtime_core::cancel::CancelReason::Host(
                "call abandoned".to_owned(),
            ));
    }
}

#[derive(Debug)]
pub(crate) struct ExternalToolCall {
    pub(crate) name: String,
    pub(crate) arguments: serde_json::Value,
    pub(crate) reply: oneshot::Sender<Result<ToolResultBlock, RuntimeError>>,
    pub(crate) cancel: Cancellation,
}

/// An entry in the volatile log of tools the BACKEND ran, not runtime tool calls.
/// `DispatchedCall` is only a position marker for a runtime call in canonical history.
#[derive(Debug, Clone, PartialEq)]
pub struct BackendToolObservation {
    /// Backend call id, or the canonical runtime call id for a dispatch marker.
    pub id: String,
    /// Backend tool name; empty for an outcome whose invocation was dropped.
    pub name: String,
    /// Backend detail, bounded to 8 KiB of JSON with an explicit truncation marker.
    pub detail: serde_json::Value,
    /// Invocation, outcome, or canonical-history dispatch position.
    pub kind: BackendToolObservationKind,
}

/// Distinguishes tools the BACKEND ran from markers for runtime-dispatched calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendToolObservationKind {
    /// The backend invoked its own tool.
    Invocation,
    /// The backend reported its own tool's outcome.
    Outcome {
        /// Whether the backend considered the tool successful.
        ok: bool,
    },
    /// A runtime dispatched call at this position; its round lives in canonical history.
    DispatchedCall,
}

/// Owned snapshot of tools the BACKEND ran, not runtime tool-call history.
/// Volatile and turn-scoped: never persisted, and empty outside an external turn.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BackendToolObservations {
    /// Most recent entries in observation order, including dispatch position markers.
    pub entries: Vec<BackendToolObservation>,
    /// Number of oldest entries discarded by the entry or serialized-detail bounds.
    pub dropped: usize,
}

#[derive(Debug, Default)]
pub(crate) struct BackendToolObservationLog {
    entries: std::collections::VecDeque<BackendToolObservation>,
    bytes: usize,
    dropped: usize,
}

impl BackendToolObservationLog {
    pub(crate) fn snapshot(&self) -> BackendToolObservations {
        BackendToolObservations {
            entries: self.entries.iter().cloned().collect(),
            dropped: self.dropped,
        }
    }

    pub(crate) fn name(&self, id: &str) -> String {
        self.entries
            .iter()
            .rev()
            .find(|entry| {
                entry.id == id && entry.kind != BackendToolObservationKind::DispatchedCall
            })
            .map(|entry| entry.name.clone())
            .unwrap_or_default()
    }

    pub(crate) fn push(&mut self, mut entry: BackendToolObservation) {
        const DETAIL_BYTES: usize = 8 * 1024;
        let encoded = serde_json::to_string(&entry.detail).expect("JSON detail serializes");
        if encoded.len() > DETAIL_BYTES {
            let mut end = DETAIL_BYTES;
            loop {
                while !encoded.is_char_boundary(end) {
                    end -= 1;
                }
                entry.detail =
                    serde_json::Value::String(format!("{}…[truncated]", &encoded[..end]));
                let size = serde_json::to_vec(&entry.detail)
                    .expect("JSON detail serializes")
                    .len();
                if size <= DETAIL_BYTES {
                    break;
                }
                end = end.saturating_sub(size - DETAIL_BYTES);
            }
        }
        self.bytes += serde_json::to_vec(&entry.detail)
            .expect("JSON detail serializes")
            .len();
        self.entries.push_back(entry);
        while self.entries.len() > 200 || self.bytes > 256 * 1024 {
            let oldest = self.entries.pop_front().expect("nonempty bounded log");
            self.bytes -= serde_json::to_vec(&oldest.detail)
                .expect("JSON detail serializes")
                .len();
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

/// One turn handed to a backend.
#[non_exhaustive]
#[derive(Debug)]
pub struct ExternalTurnRequest {
    /// Instructions added after the backend's configured instructions for this turn.
    pub instructions: Option<String>,
    /// The user input for this turn, already resolved by the host.
    pub input: UserInput,
    /// An identity the backend reported earlier, offered for continuation.
    ///
    /// Advisory: a backend that cannot resume it reports a different one and
    /// the turn proceeds.
    pub resume: Option<ExternalSessionId>,
    /// This turn's identity, for correlation in backend-side logs.
    pub turn: TurnId,
    /// Cancelled when the turn is cancelled or the session shuts down.
    pub cancel: Cancellation,
    /// What the session injects into this turn. Already validated; a backend
    /// that supports injection re-applies all of it on every turn.
    pub capabilities: Arc<ExternalCapabilities>,
    /// The runtime MCP endpoint when `runtime_tools` and the
    /// `external-agent-bridge` feature are enabled. Calls emit tool events
    /// without entering canonical history. Valid until this turn's terminal event.
    pub bridge: Option<ExternalToolBridge>,
    /// Runtime-owned host tools offered for this turn.
    pub tools: ExternalToolDispatch,
}

impl ExternalTurnRequest {
    /// Creates a fresh turn without continuation, injected capabilities, or host tools.
    /// Optional fields remain assignable so hosts can opt into only what they need.
    pub fn new(input: UserInput, turn: TurnId, cancel: Cancellation) -> Self {
        Self {
            input,
            turn,
            cancel,
            instructions: None,
            resume: None,
            capabilities: Arc::default(),
            bridge: None,
            tools: ExternalToolDispatch::disabled(),
        }
    }
}

/// Normalized events a backend streams while its turn runs.
///
/// Ordering contract: any number of non-terminal events, then exactly one of
/// [`ExternalAgentEvent::Completed`] or [`ExternalAgentEvent::Failed`].
/// Anything after a terminal event is dropped.
#[derive(Debug, Clone, PartialEq)]
pub enum ExternalAgentEvent {
    /// The backend's conversation identity for this turn.
    SessionStarted {
        /// Identity to offer back on the next turn.
        session: ExternalSessionId,
    },
    /// Assistant prose, incremental or whole.
    Text {
        /// The text fragment.
        text: String,
    },
    /// Backend-visible reasoning, where the backend reports any.
    Reasoning {
        /// The reasoning fragment.
        text: String,
    },
    /// A tool the backend ran itself. Observation only: the runtime never
    /// dispatched it and cannot approve it.
    ToolInvoked {
        /// Backend-assigned call identity, correlating with its outcome.
        id: String,
        /// Backend-reported tool name.
        name: String,
        /// Backend-reported invocation detail.
        detail: serde_json::Value,
    },
    /// The outcome of a tool the backend ran itself.
    ToolCompleted {
        /// The identity reported by the matching invocation.
        id: String,
        /// Whether the backend considered the tool successful.
        ok: bool,
        /// Backend-reported outcome detail.
        detail: serde_json::Value,
    },
    /// Token counts for this turn, with whatever cache breakdown is known.
    Usage {
        /// Disjoint counters, following the ordinary usage contract.
        usage: UsageDelta,
    },
    /// Non-terminal information the host should surface without failing the turn.
    Diagnostic {
        /// Message, projected to at most 1,000 characters by the runtime.
        message: String,
    },
    /// Terminal: the turn finished, and accumulated text is its answer.
    Completed,
    /// Terminal: the turn failed.
    Failed {
        /// A reason safe to show a host.
        message: String,
    },
}

impl ExternalAgentEvent {
    /// Whether this event ends the turn.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed { .. })
    }
}

/// The stream a backend returns for one turn.
pub type ExternalTurnStream = Pin<Box<dyn Stream<Item = ExternalAgentEvent> + Send>>;

/// Executes one whole turn outside the runtime's provider loop.
#[async_trait]
pub trait ExternalAgentBackend: Send + Sync + fmt::Debug {
    /// Runs one turn, streaming normalized events until a terminal one.
    ///
    /// Returning `Err` fails the turn before it produces any event; a failure
    /// discovered while running is reported as
    /// [`ExternalAgentEvent::Failed`] instead, so a partially observed turn
    /// still completes through the ordinary path.
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError>;
}

/// The persisted continuation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalAgentState {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// The identity last reported by the backend.
    pub session: ExternalSessionId,
}

impl ExternalAgentState {
    /// Wraps an identity for storage at the current schema version.
    pub fn new(session: ExternalSessionId) -> Self {
        Self {
            schema_version: EXTERNAL_AGENT_STATE_SCHEMA_VERSION,
            session,
        }
    }
}

/// A shared backend handle.
pub type SharedExternalAgentBackend = Arc<dyn ExternalAgentBackend>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_identity_rejects_values_it_could_not_hand_back() {
        assert!(ExternalSessionId::new("").is_err());
        assert!(ExternalSessionId::new("a\0b").is_err());
        assert!(ExternalSessionId::new("x".repeat(MAX_EXTERNAL_SESSION_ID_BYTES + 1)).is_err());

        let ok = ExternalSessionId::new("01a070f5-5458-7323-8a60-8458d779041e")
            .expect("a bounded identity");
        assert_eq!(ok.as_str(), "01a070f5-5458-7323-8a60-8458d779041e");
        assert_eq!(ok.to_string(), "01a070f5-5458-7323-8a60-8458d779041e");
    }

    #[test]
    fn only_completion_and_failure_end_a_turn() {
        assert!(ExternalAgentEvent::Completed.is_terminal());
        assert!(
            ExternalAgentEvent::Failed {
                message: "boom".to_owned()
            }
            .is_terminal()
        );
        assert!(
            !ExternalAgentEvent::Text {
                text: "hello".to_owned()
            }
            .is_terminal()
        );
        assert!(
            !ExternalAgentEvent::Usage {
                usage: UsageDelta::new()
            }
            .is_terminal()
        );
    }

    #[test]
    fn stored_state_carries_its_schema_version() {
        let state = ExternalAgentState::new(
            ExternalSessionId::new("session-1").expect("a bounded identity"),
        );
        assert_eq!(state.schema_version, EXTERNAL_AGENT_STATE_SCHEMA_VERSION);

        let round_tripped: ExternalAgentState =
            serde_json::from_str(&serde_json::to_string(&state).expect("serializes"))
                .expect("deserializes");
        assert_eq!(round_tripped, state);
    }
}
