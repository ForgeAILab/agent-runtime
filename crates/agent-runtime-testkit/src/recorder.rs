//! An event observer that records everything it sees.

use std::sync::{Arc, Mutex};

use agent_runtime_core::event::{EventEnvelope, RuntimeEvent, canonical_payloads};
use agent_runtime_core::observer::EventObserver;

/// Records every emitted event for later assertions.
#[derive(Debug, Default)]
pub struct RecordingObserver {
    events: Mutex<Vec<EventEnvelope>>,
}

impl RecordingObserver {
    /// A new, empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps the recorder in an `Arc` for injection.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// A snapshot of the recorded envelopes.
    pub fn events(&self) -> Vec<EventEnvelope> {
        self.events.lock().expect("recorder poisoned").clone()
    }

    /// The canonical payload sequence (identity, time, and presentation
    /// metadata dropped) — suitable for cross-host equivalence checks.
    pub fn payloads(&self) -> Vec<RuntimeEvent> {
        canonical_payloads(&self.events())
    }
}

impl EventObserver for RecordingObserver {
    fn observe(&self, event: &EventEnvelope) {
        self.events
            .lock()
            .expect("recorder poisoned")
            .push(event.clone());
    }
}
