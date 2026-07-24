//! Pumps a runtime event stream into a sink with real back-pressure.

use std::sync::Arc;

use futures_core::Stream;
use futures_util::StreamExt;

use agent_runtime_core::event::EventEnvelope;

use crate::EventSink;

/// Drains an event stream into `sink` until the stream ends, then flushes.
///
/// Unlike [`crate::SinkObserver`], this awaits each `emit`, so a slow sink slows
/// the pump instead of dropping events — the caller chooses lossless delivery by
/// using this over the observer bridge. Per-event `emit` errors are ignored so a
/// single failure never tears down the pump; wrap sinks in a
/// [`crate::FanoutSink`] to isolate one sink's failures from the others.
///
/// Spawn it on a task fed by the runtime's `subscribe()` stream:
///
/// ```ignore
/// let stream = session.subscribe();
/// tokio::spawn(agent_runtime_obs::drive(stream, sink));
/// ```
pub async fn drive<S>(stream: S, sink: Arc<dyn EventSink>)
where
    S: Stream<Item = EventEnvelope>,
{
    futures_util::pin_mut!(stream);
    while let Some(event) = stream.next().await {
        let _ = sink.emit(&event).await;
    }
    let _ = sink.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CapturingSink, sample_event};

    #[tokio::test]
    async fn drains_the_whole_stream_then_flushes() {
        let sink = Arc::new(CapturingSink::new());
        let events = vec![sample_event(0), sample_event(1), sample_event(2)];
        let stream = futures_util::stream::iter(events);
        drive(stream, sink.clone()).await;
        assert_eq!(sink.len(), 3);
    }
}
