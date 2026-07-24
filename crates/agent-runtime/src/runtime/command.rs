//! Runtime commands.
//!
//! Commands carry an explicit payload schema version independent of the crate's
//! semantic version. `StartSession` is the only command needed to begin work;
//! further interaction happens through the returned session handle.

use agent_runtime_core::content::Message;
use agent_runtime_core::ids::SessionId;
use serde::{Deserialize, Serialize};

/// The schema version of runtime command payloads.
pub const COMMAND_SCHEMA_VERSION: u32 = 1;

const fn command_schema_version() -> u32 {
    COMMAND_SCHEMA_VERSION
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
}

impl Default for StartSession {
    fn default() -> Self {
        Self {
            schema_version: COMMAND_SCHEMA_VERSION,
            session_id: None,
            initial_history: Vec::new(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_session_serializes_an_explicit_schema_version() {
        let command = StartSession::new();
        let json = serde_json::to_value(&command).unwrap();
        assert_eq!(json["schema_version"], COMMAND_SCHEMA_VERSION);
        let restored: StartSession = serde_json::from_value(json).unwrap();
        assert_eq!(restored, command);
    }
}
