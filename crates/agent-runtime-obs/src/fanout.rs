//! A sink that fans one event out to many sinks.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use agent_runtime_core::event::EventEnvelope;

use crate::{EventSink, ObsError};

/// Broadcasts every event to a set of sinks, e.g. a [`crate::CliSink`] and a
/// [`crate::SqliteSink`] at once.
///
/// Every sink is attempted for every event; the first error is returned after
/// the remaining sinks have been given the event, so one failing sink never
/// starves the others. [`EventSink::flush`] follows the same all-then-report
/// rule.
#[derive(Default, Clone)]
pub struct FanoutSink {
    sinks: Vec<Arc<dyn EventSink>>,
}

impl FanoutSink {
    /// A fanout over the given sinks.
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self { sinks }
    }

    /// Adds a sink to the fanout.
    pub fn with(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// The sinks in this fanout.
    pub fn sinks(&self) -> &[Arc<dyn EventSink>] {
        &self.sinks
    }
}

impl fmt::Debug for FanoutSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FanoutSink")
            .field("sinks", &self.sinks.len())
            .finish()
    }
}

#[async_trait]
impl EventSink for FanoutSink {
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError> {
        // Attempt every sink; keep the first error but never short-circuit, so
        // one failing sink cannot starve the others. Avoids let-chains to hold
        // the MSRV 1.86 line.
        let mut first_error: Option<ObsError> = None;
        for sink in &self.sinks {
            if let Err(err) = sink.emit(event).await {
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn flush(&self) -> Result<(), ObsError> {
        let mut first_error: Option<ObsError> = None;
        for sink in &self.sinks {
            if let Err(err) = sink.flush().await {
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::CapturingSink;

    #[tokio::test]
    async fn fans_out_to_every_sink() {
        let a = Arc::new(CapturingSink::new());
        let b = Arc::new(CapturingSink::new());
        let fanout = FanoutSink::new(vec![a.clone(), b.clone()]);

        let env = crate::testing::sample_event(1);
        fanout.emit(&env).await.unwrap();

        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
    }
}
