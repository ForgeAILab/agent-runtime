//! The event observation contract.
//!
//! Hosts inject an [`EventObserver`] to receive every emitted [`EventEnvelope`]
//! synchronously (e.g. for logging or metrics). Observation is fire-and-forget
//! and must not block; subscribers that need back-pressure should use the
//! runtime's event stream instead.

use std::fmt;

use crate::event::EventEnvelope;

/// A host-injected synchronous event sink.
pub trait EventObserver: Send + Sync + fmt::Debug {
    /// Observes an emitted event. Implementations must not block.
    fn observe(&self, event: &EventEnvelope);
}

/// An observer that ignores every event.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullObserver;

impl EventObserver for NullObserver {
    fn observe(&self, _event: &EventEnvelope) {}
}
