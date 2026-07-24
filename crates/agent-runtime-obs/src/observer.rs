//! Bridges the runtime's synchronous observer hook onto async sinks.
//!
//! The runtime delivers events through the synchronous, must-not-block
//! [`EventObserver`] hook. Async sinks (a SQLite insert, a file append) cannot
//! run there directly, so [`SinkObserver`] forwards each event over a bounded
//! channel to a background task that drains it into the sink. This keeps the
//! emit path non-blocking; if the sink falls badly behind and the channel
//! fills, events are dropped and counted rather than blocking the runtime.
//!
//! When you need lossless delivery with real back-pressure instead, subscribe
//! to the runtime's event stream and pump it with [`crate::drive`].

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use agent_runtime_core::event::EventEnvelope;
use agent_runtime_core::observer::EventObserver;

use crate::EventSink;

/// The default bounded-channel capacity between the emit path and the drain
/// task.
pub const DEFAULT_CAPACITY: usize = 1024;

/// A fire-and-forget [`EventObserver`] that forwards to an async sink.
#[derive(Clone)]
pub struct SinkObserver {
    tx: mpsc::Sender<EventEnvelope>,
    dropped: Arc<AtomicU64>,
}

impl SinkObserver {
    /// Spawns a drain task for `sink` on the current Tokio runtime and returns
    /// an observer to hand to `RuntimeBuilder::observer`.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn spawn(sink: Arc<dyn EventSink>) -> Arc<Self> {
        Self::spawn_with_capacity(sink, DEFAULT_CAPACITY)
    }

    /// Like [`SinkObserver::spawn`] with an explicit channel capacity.
    pub fn spawn_with_capacity(sink: Arc<dyn EventSink>, capacity: usize) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let _ = sink.emit(&event).await;
            }
            let _ = sink.flush().await;
        });
        Arc::new(Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        })
    }

    /// The number of events dropped because the drain channel was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for SinkObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkObserver")
            .field("dropped", &self.dropped())
            .finish()
    }
}

impl EventObserver for SinkObserver {
    fn observe(&self, event: &EventEnvelope) {
        if self.tx.try_send(event.clone()).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CapturingSink, sample_event};

    #[tokio::test]
    async fn forwards_events_to_the_sink() {
        let sink = Arc::new(CapturingSink::new());
        let observer = SinkObserver::spawn(sink.clone());
        for seq in 0..3 {
            observer.observe(&sample_event(seq));
        }
        // Let the drain task run.
        for _ in 0..50 {
            if sink.len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(sink.len(), 3);
        assert_eq!(observer.dropped(), 0);
    }
}
