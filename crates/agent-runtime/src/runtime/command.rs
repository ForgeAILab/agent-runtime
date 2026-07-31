//! Runtime commands.
//!
//! Commands carry an explicit payload schema version independent of the crate's
//! semantic version. `StartSession` is the only command needed to begin work;
//! further interaction happens through the returned session handle.

use agent_runtime_core::content::Message;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::store::SessionIdentityState;
use serde::{Deserialize, Serialize};

/// The schema version of runtime command payloads.
///
/// Version 1 permits additive optional fields whose default preserves the
/// original wire bytes and behavior. `checkpoint_recovery` follows that rule:
/// `Resume` is omitted, while the explicit defer opt-in is serialized.
pub const COMMAND_SCHEMA_VERSION: u32 = 1;

const fn command_schema_version() -> u32 {
    COMMAND_SCHEMA_VERSION
}

/// Host policy for one protected non-terminal checkpoint found on start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointRecoveryPolicy {
    /// Resume every supported non-terminal checkpoint immediately.
    #[default]
    Resume,
    /// Leave only an unanswered `AwaitingInteraction` checkpoint dormant.
    ///
    /// Every other checkpoint resumes normally. This narrow mode lets a
    /// non-interactive host inspect/hand off a pending question without racing
    /// a broker; it does not change live interaction behavior.
    DeferPendingInteraction,
}

fn checkpoint_recovery_is_resume(policy: &CheckpointRecoveryPolicy) -> bool {
    *policy == CheckpointRecoveryPolicy::Resume
}

/// A request to start (or resume) a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartSession {
    /// The command payload schema version.
    #[serde(default = "command_schema_version")]
    pub schema_version: u32,
    /// An explicit session id (for resuming a persisted session). When absent
    /// the runtime mints a fresh id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    /// An initial history used when no persisted snapshot is found.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_history: Vec<Message>,
    /// Optional monotonic floor derived from a separately durable observer
    /// journal when resuming after a crash.
    ///
    /// Protected checkpoints bind exact resumable state, but the redacted
    /// event stream may have advanced after the last checkpoint write. A host
    /// that persists that tail supplies its last-known identity counters here
    /// so the runtime never reuses an event sequence or minted ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_identity_floor: Option<SessionIdentityState>,
    /// Policy for a protected non-terminal checkpoint found during resume.
    #[serde(default, skip_serializing_if = "checkpoint_recovery_is_resume")]
    pub checkpoint_recovery: CheckpointRecoveryPolicy,
}

impl Default for StartSession {
    fn default() -> Self {
        Self {
            schema_version: COMMAND_SCHEMA_VERSION,
            session_id: None,
            initial_history: Vec::new(),
            resume_identity_floor: None,
            checkpoint_recovery: CheckpointRecoveryPolicy::Resume,
        }
    }
}

impl StartSession {
    /// A new, empty start request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an explicit session id.
    pub fn with_id(mut self, id: SessionId) -> Self {
        self.session_id = Some(id);
        self
    }

    /// Seeds the initial history.
    pub fn with_history(mut self, history: Vec<Message>) -> Self {
        self.initial_history = history;
        self
    }

    /// Sets the monotonic identity floor recovered from durable observer
    /// state. This is valid only together with an explicit session id.
    pub fn with_resume_identity_floor(mut self, floor: SessionIdentityState) -> Self {
        self.resume_identity_floor = Some(floor);
        self
    }

    /// Sets the protected-checkpoint recovery policy.
    pub fn with_checkpoint_recovery(mut self, policy: CheckpointRecoveryPolicy) -> Self {
        self.checkpoint_recovery = policy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_session_serializes_an_explicit_schema_version() {
        let command = StartSession::new();
        let json = serde_json::to_value(&command).unwrap();
        assert_eq!(json["schema_version"], COMMAND_SCHEMA_VERSION);
        assert!(json.get("checkpoint_recovery").is_none());
        let restored: StartSession = serde_json::from_value(json).unwrap();
        assert_eq!(restored, command);

        let deferred = StartSession::new()
            .with_checkpoint_recovery(CheckpointRecoveryPolicy::DeferPendingInteraction);
        let json = serde_json::to_value(&deferred).unwrap();
        assert_eq!(json["checkpoint_recovery"], "defer_pending_interaction");
        assert_eq!(
            serde_json::from_value::<StartSession>(json).unwrap(),
            deferred
        );
    }
}
