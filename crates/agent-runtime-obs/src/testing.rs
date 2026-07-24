//! Test helpers: an in-memory capturing sink and a sample event factory.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime_core::ids::{EventId, SessionId};

use crate::{EventSink, ObsError};

/// An [`EventSink`] that records every event it receives, for assertions.
#[derive(Debug, Default, Clone)]
pub struct CapturingSink {
    events: Arc<Mutex<Vec<EventEnvelope>>>,
    flushes: Arc<Mutex<usize>>,
}

impl CapturingSink {
    /// A fresh, empty capturing sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// A clone of the captured events, in arrival order.
    pub fn events(&self) -> Vec<EventEnvelope> {
        self.events.lock().expect("poisoned").clone()
    }

    /// The number of captured events.
    pub fn len(&self) -> usize {
        self.events.lock().expect("poisoned").len()
    }

    /// Whether no events have been captured.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of times [`EventSink::flush`] was called.
    pub fn flush_count(&self) -> usize {
        *self.flushes.lock().expect("poisoned")
    }
}

#[async_trait]
impl EventSink for CapturingSink {
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError> {
        self.events.lock().expect("poisoned").push(event.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<(), ObsError> {
        *self.flushes.lock().expect("poisoned") += 1;
        Ok(())
    }
}

/// Builds a deterministic sample envelope with the given sequence number.
pub fn sample_event(seq: u64) -> EventEnvelope {
    EventEnvelope::new(
        seq,
        EventId::new(format!("evt-{seq}")),
        SessionId::new("s-test"),
        None,
        Timestamp(seq),
        RuntimeEvent::TextDelta {
            text: format!("chunk-{seq}"),
        },
    )
}
