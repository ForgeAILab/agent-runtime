//! Why talking to a server failed.
//!
//! Every variant is safe to log. Server names, tool names, credential *names*,
//! and protocol versions appear; environment values, headers, and bearer
//! tokens never do. A server's own error text is bounded before it lands in
//! [`McpError::Protocol`] or [`McpError::ToolFailed`], because that text is
//! written by the party whose behavior is in question.

use std::time::Duration;

use agent_runtime_core::error::RuntimeError;

/// The bound applied to any message a server authored before it enters an
/// error. Long enough to diagnose, short enough that a hostile server cannot
/// flood a log through a failure path.
pub(crate) const MAX_SERVER_MESSAGE_BYTES: usize = 2048;

/// A failure while connecting to, listing, or calling an MCP server.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum McpError {
    /// The server process or endpoint could not be reached at all.
    #[error("mcp server `{server}` did not start: {reason}")]
    Startup {
        /// The configured server name.
        server: String,
        /// Why it could not start.
        reason: String,
    },

    /// The server accepted a connection but did not finish initializing in
    /// time.
    #[error("mcp server `{server}` did not become ready within {}ms", timeout.as_millis())]
    StartupTimeout {
        /// The configured server name.
        server: String,
        /// The deadline that expired.
        timeout: Duration,
    },

    /// The server speaks no protocol version this client also speaks.
    ///
    /// Both sides' supported versions are named, because "incompatible" without
    /// them tells an operator nothing about which end to upgrade.
    #[error(
        "mcp server `{server}` supports protocol {server_supported}, \
         this client supports {client_supported}"
    )]
    IncompatibleVersion {
        /// The configured server name.
        server: String,
        /// What the server said it speaks.
        server_supported: String,
        /// What this client speaks.
        client_supported: String,
    },

    /// The server sent something that is not a valid message, or violated the
    /// protocol's shape.
    #[error("mcp server `{server}` sent an invalid message: {reason}")]
    Protocol {
        /// The configured server name.
        server: String,
        /// A bounded description, safe to log.
        reason: String,
    },

    /// The connection is gone: the process exited, or the endpoint closed.
    #[error("mcp server `{server}` is no longer connected")]
    Disconnected {
        /// The configured server name.
        server: String,
    },

    /// A call did not return before its deadline.
    #[error("call to `{tool}` on mcp server `{server}` exceeded its deadline")]
    CallTimeout {
        /// The configured server name.
        server: String,
        /// The tool that was called.
        tool: String,
    },

    /// The server advertised a tool this client will not register.
    #[error("mcp server `{server}` advertised an unusable tool: {reason}")]
    UnusableTool {
        /// The configured server name.
        server: String,
        /// Why the tool was rejected.
        reason: String,
    },

    /// The configured transport was not compiled in.
    #[error("mcp server `{server}` needs the `{feature}` feature, which is not enabled")]
    TransportUnavailable {
        /// The configured server name.
        server: String,
        /// The Cargo feature that would provide it.
        feature: &'static str,
    },
}

impl McpError {
    /// The server this failure belongs to.
    pub fn server(&self) -> &str {
        match self {
            Self::Startup { server, .. }
            | Self::StartupTimeout { server, .. }
            | Self::IncompatibleVersion { server, .. }
            | Self::Protocol { server, .. }
            | Self::Disconnected { server }
            | Self::CallTimeout { server, .. }
            | Self::UnusableTool { server, .. }
            | Self::TransportUnavailable { server, .. } => server,
        }
    }

    /// Whether this failure means the server can no longer serve calls.
    ///
    /// A connection-level fault retires the server's tools at the next safe
    /// boundary; a single tool's failure does not.
    pub fn is_fatal_to_connection(&self) -> bool {
        matches!(
            self,
            Self::Startup { .. }
                | Self::StartupTimeout { .. }
                | Self::IncompatibleVersion { .. }
                | Self::Disconnected { .. }
                | Self::TransportUnavailable { .. }
        )
    }
}

impl From<McpError> for RuntimeError {
    fn from(error: McpError) -> Self {
        RuntimeError::tool(error.to_string())
    }
}

/// Bounds a message a server authored, on a character boundary.
pub(crate) fn bound_server_message(message: impl AsRef<str>) -> String {
    let message = message.as_ref();
    if message.len() <= MAX_SERVER_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_SERVER_MESSAGE_BYTES;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (truncated)", &message[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounding_keeps_a_short_message_intact() {
        assert_eq!(bound_server_message("boom"), "boom");
    }

    #[test]
    fn bounding_truncates_a_flood_on_a_char_boundary() {
        let flood = "é".repeat(MAX_SERVER_MESSAGE_BYTES);
        let bounded = bound_server_message(&flood);
        assert!(bounded.len() < flood.len());
        assert!(bounded.ends_with("… (truncated)"));
    }

    #[test]
    fn one_bad_call_does_not_retire_the_connection() {
        // A server-reported tool failure never becomes an `McpError` at all —
        // it is a `ToolOutcome` the model can see. The nearest call-scoped
        // fault is a timeout, and it too leaves the server usable.
        let failed = McpError::CallTimeout {
            server: "github".to_owned(),
            tool: "create_issue".to_owned(),
        };
        assert!(!failed.is_fatal_to_connection());
        assert_eq!(failed.server(), "github");

        let gone = McpError::Disconnected {
            server: "github".to_owned(),
        };
        assert!(gone.is_fatal_to_connection());
    }
}
