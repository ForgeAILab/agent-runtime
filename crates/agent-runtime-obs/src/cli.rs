//! A terminal sink that writes one human log line per event.

use std::io::{self, Write};

use async_trait::async_trait;

use agent_runtime_core::event::EventEnvelope;

use crate::render::log_line;
use crate::{EventSink, ObsError};

/// Which standard stream a [`CliSink`] writes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Stdout,
    Stderr,
}

/// Writes each event as a compact [`log_line`] to stdout or stderr.
///
/// Locking is per-call, so interleaving with other writers to the same stream
/// stays line-atomic.
#[derive(Debug, Clone)]
pub struct CliSink {
    target: Target,
}

impl CliSink {
    /// A sink writing to standard output.
    pub fn stdout() -> Self {
        Self {
            target: Target::Stdout,
        }
    }

    /// A sink writing to standard error.
    pub fn stderr() -> Self {
        Self {
            target: Target::Stderr,
        }
    }
}

impl Default for CliSink {
    fn default() -> Self {
        Self::stdout()
    }
}

#[async_trait]
impl EventSink for CliSink {
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError> {
        let line = log_line(event);
        match self.target {
            Target::Stdout => {
                let out = io::stdout();
                let mut lock = out.lock();
                writeln!(lock, "{line}")?;
            }
            Target::Stderr => {
                let err = io::stderr();
                let mut lock = err.lock();
                writeln!(lock, "{line}")?;
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ObsError> {
        match self.target {
            Target::Stdout => io::stdout().flush()?,
            Target::Stderr => io::stderr().flush()?,
        }
        Ok(())
    }
}
