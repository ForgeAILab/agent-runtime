//! The observability error type.

use thiserror::Error;

/// An error produced while writing an event to a sink.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ObsError {
    /// A sink I/O failure (writing a line, opening a file).
    #[error("sink io error: {0}")]
    Io(#[from] std::io::Error),
    /// The event could not be serialized for a sink.
    #[error("event serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    /// A backend-specific failure (e.g. a SQL insert), stringified so the error
    /// type stays neutral and does not leak a backend crate into the public API.
    #[error("sink error: {0}")]
    Sink(String),
}
