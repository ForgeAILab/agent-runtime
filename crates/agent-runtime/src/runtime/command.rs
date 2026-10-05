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
/// Version 2 requires explicit create, resume or ephemeral intent.
pub const COMMAND_SCHEMA_VERSION: u32 = 2;

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
    /// Resume compatible checkpoints, but stop an unfinished ordinary turn
    /// without replay when its saved activation scope has changed.
    ///
    /// The conversation and accounting survive; available abilities are
    /// re-authorized against the current registry. No saved approval, tool
    /// invocation, provider call, or turn-commit hook is replayed on this
    /// fallback. Invalid state and cache-operation checkpoints remain strict.
    /// Hosts must report `SessionHandle::interrupted_on_resume` to the user.
    ResumeOrInterrupt,
    /// Leave only an unanswered `AwaitingInteraction` checkpoint dormant.
    ///
    /// Every other checkpoint resumes normally. This narrow mode lets a
    /// non-interactive host inspect/hand off a pending question without racing
    /// a broker; it does not change live interaction behavior.
    DeferPendingInteraction,
    /// Load the compatible snapshot and exact checkpoint but leave every
    /// non-terminal checkpoint dormant.
    ///
    /// The delegation coordinator uses this policy while rebinding a durable
    /// child so merely listing or inspecting it cannot execute provider/tool
    /// work. An explicit child `resume` later continues the returned exact
    /// checkpoint through the ordinary turn machine.
    Defer,
}

fn checkpoint_recovery_is_resume(policy: &CheckpointRecoveryPolicy) -> bool {
    *policy == CheckpointRecoveryPolicy::Resume
}

/// Explicit session persistence intent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartSessionMode {
    /// Start fresh; an existing snapshot or checkpoint is a conflict.
    #[default]
    Create,
    /// Load an existing session; supplying seed history is a conflict.
    Resume,
    /// Start fresh without reading or writing session/checkpoint stores.
    Ephemeral,
}

/// A request to start (or resume) a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartSession {
    /// The command payload schema version.
    #[serde(default = "command_schema_version")]
    pub schema_version: u32,
    /// Required explicit intent on the wire.
    pub mode: StartSessionMode,
    /// Session identity. Create may mint an id; resume requires one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    /// Seed history for create/ephemeral; any resume seed is a Conflict.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed: Vec<Message>,
    /// Explicit recovery choice for populated LCM timelines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lcm_policy: Option<crate::harness::LcmRecoveryPolicy>,
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
            mode: StartSessionMode::Create,
            session_id: None,
            seed: Vec::new(),
            lcm_policy: None,
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

    /// Creates one fresh, named session with host seed history.
    pub fn create(id: SessionId, seed: Vec<Message>) -> Self {
        Self {
            session_id: Some(id),
            seed,
            ..Self::default()
        }
    }

    /// Resumes an existing named session without seed history.
    pub fn resume(id: SessionId) -> Self {
        Self {
            mode: StartSessionMode::Resume,
            session_id: Some(id),
            ..Self::default()
        }
    }

    /// Starts history without using configured session/checkpoint persistence.
    pub fn ephemeral(history: Vec<Message>) -> Self {
        Self {
            mode: StartSessionMode::Ephemeral,
            seed: history,
            ..Self::default()
        }
    }

    /// Chooses explicit LCM recovery at this construction boundary.
    pub fn with_lcm_policy(mut self, policy: crate::harness::LcmRecoveryPolicy) -> Self {
        self.lcm_policy = Some(policy);
        self
    }

    /// Sets an explicit session id without changing the selected intent.
    pub fn with_id(mut self, id: SessionId) -> Self {
        self.session_id = Some(id);
        self
    }

    /// Seeds the initial history.
    pub fn with_history(mut self, history: Vec<Message>) -> Self {
        self.seed = history;
        self
    }

    /// Sets the monotonic identity floor recovered from durable observer
    /// state. This is valid only for explicit resume.
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
