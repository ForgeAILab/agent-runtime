//! Structured, redaction-safe errors.
//!
//! The donor code had per-crate error enums with no shared `kind`, no
//! `retryable` flag, and no redaction. This unified [`RuntimeError`] carries a
//! coarse [`ErrorKind`] discriminant, an explicit retryability flag, and a
//! redaction-safe [`Metadata`] bag so errors can be emitted in events safely.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::metadata::Metadata;

/// A coarse, stable classification of a runtime error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// A provider (LLM backend) failure.
    Provider,
    /// A tool invocation failure.
    Tool,
    /// The action was denied by the approval policy.
    Approval,
    /// A workspace boundary violation.
    Workspace,
    /// Work was cancelled.
    Cancelled,
    /// A configured limit was reached.
    Limit,
    /// A deadline elapsed.
    Timeout,
    /// Invalid configuration or request.
    Config,
    /// Serialization / deserialization failure.
    Serialization,
    /// A referenced entity was not found.
    NotFound,
    /// A conflicting state (e.g. a duplicate registration).
    Conflict,
    /// An unexpected internal error.
    Internal,
}

/// The canonical error type of the runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeError {
    /// The coarse classification.
    pub kind: ErrorKind,
    /// A human-readable, redaction-safe message.
    pub message: String,
    /// Whether retrying the operation might succeed.
    pub retryable: bool,
    /// Redaction-safe structured context.
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
}

impl RuntimeError {
    /// Builds an error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable: false,
            metadata: Metadata::new(),
        }
    }

    /// Marks the error retryable.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// Attaches metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Whether retrying might succeed.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    // Convenience constructors for the common kinds.

    /// A [`ErrorKind::Config`] error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message)
    }
    /// A [`ErrorKind::Tool`] error.
    pub fn tool(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Tool, message)
    }
    /// A [`ErrorKind::Approval`] denial.
    pub fn approval(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Approval, message)
    }
    /// A [`ErrorKind::Workspace`] violation.
    pub fn workspace(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Workspace, message)
    }
    /// A [`ErrorKind::Cancelled`] error.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message)
    }
    /// A [`ErrorKind::Limit`] error.
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Limit, message)
    }
    /// A [`ErrorKind::NotFound`] error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }
    /// A [`ErrorKind::Conflict`] error.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }
    /// A [`ErrorKind::Internal`] error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for RuntimeError {}

impl From<serde_json::Error> for RuntimeError {
    fn from(err: serde_json::Error) -> Self {
        RuntimeError::new(ErrorKind::Serialization, err.to_string())
    }
}

/// The runtime's result alias.
pub type Result<T, E = RuntimeError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_flag_and_display() {
        let e = RuntimeError::tool("boom").retryable();
        assert!(e.is_retryable());
        assert_eq!(e.kind, ErrorKind::Tool);
        assert!(format!("{e}").contains("boom"));
    }

    #[test]
    fn serializes_without_empty_metadata() {
        let e = RuntimeError::config("bad");
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("metadata").is_none());
    }
}
