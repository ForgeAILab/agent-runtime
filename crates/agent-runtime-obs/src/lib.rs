//! Observability facade for the shared agent runtime.
//!
//! `agent-runtime-obs` turns the runtime's neutral event stream into log output
//! without the runtime picking a logging backend. Runtime code emits
//! [`agent_runtime_core::event::EventEnvelope`]s; this crate provides the
//! [`EventSink`] trait and small building blocks to route them:
//!
//! - [`FanoutSink`] — write one event to many sinks (e.g. CLI + SQLite).
//! - [`SinkObserver`] — plug a sink into `RuntimeBuilder::observer`; forwards
//!   events off the synchronous emit path to a background drain task.
//! - [`drive`] — pump the runtime's async `subscribe()` stream into a sink with
//!   real back-pressure (lossless) instead of the drop-on-full observer bridge.
//! - [`ObsRow`] — a flat, SQL-ready projection so a consumer can persist events
//!   with its own database.
//!
//! Concrete sinks are feature-gated so the default dependency graph stays lean:
//!
//! - `cli` (default) — [`CliSink`], one human log line per event.
//! - `file` — [`FileSink`], append-only JSONL.
//! - `sqlite` — [`SqliteSink`], a bundled SQLite table (opt-in; pulls
//!   `rusqlite`). Consumers with their own store use [`ObsRow`] and leave this
//!   off.
//!
//! It depends only on [`agent_runtime_core`] and contains no consumer domain
//! type: the *routing mechanism* is shared; each host keeps its own log format
//! and storage *policy*.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use agent_runtime_obs::{CliSink, EventSink, FanoutSink, SinkObserver};
//!
//! # async fn wire() {
//! // Fan the CLI sink out alongside any others, then bridge to the runtime.
//! let sink: Arc<dyn EventSink> = Arc::new(FanoutSink::new(vec![Arc::new(CliSink::stdout())]));
//! let observer = SinkObserver::spawn(sink);
//! // RuntimeBuilder::new(model).observer(observer)...
//! # let _ = observer;
//! # }
//! ```
#![forbid(unsafe_code)]

use std::fmt;

use async_trait::async_trait;

use agent_runtime_core::event::EventEnvelope;

mod drive;
mod error;
mod fanout;
mod observer;
mod render;
mod row;
pub mod testing;

#[cfg(feature = "cli")]
mod cli;
#[cfg(feature = "file")]
mod file;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use drive::drive;
pub use error::ObsError;
pub use fanout::FanoutSink;
pub use observer::{DEFAULT_CAPACITY, SinkObserver};
pub use render::{event_type, log_line};
pub use row::ObsRow;

#[cfg(feature = "cli")]
pub use cli::CliSink;
#[cfg(feature = "file")]
pub use file::FileSink;
#[cfg(feature = "sqlite")]
pub use sqlite::{DEFAULT_TABLE, SqliteSink};

/// A re-export of the neutral core contracts these sinks operate on.
pub use agent_runtime_core as core;

/// An asynchronous destination for runtime events.
///
/// Implementations write, index, or forward an [`EventEnvelope`]. `emit` should
/// return a structured [`ObsError`] on failure rather than panicking;
/// [`FanoutSink`] and [`drive`] both tolerate a single sink's error. `flush` is
/// called when a drain loop ends and defaults to a no-op.
#[async_trait]
pub trait EventSink: Send + Sync + fmt::Debug {
    /// Handles one event.
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError>;

    /// Flushes any buffered state. Defaults to a no-op.
    async fn flush(&self) -> Result<(), ObsError> {
        Ok(())
    }
}
