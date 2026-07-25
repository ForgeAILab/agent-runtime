//! The per-session event emitter and subscription stream.
//!
//! Events are stamped with a monotonic per-session sequence number and a
//! versioned envelope, delivered synchronously to injected observers, and
//! broadcast to any number of concurrent subscribers. Subscribers that fall
//! behind skip lagged events rather than blocking the runtime.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_stream::stream;
use futures_core::Stream;
use tokio::sync::broadcast;

use agent_runtime_core::clock::Clock;
use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::observer::EventObserver;

use crate::ids::IdMinter;

/// A stream of runtime events for one subscriber.
pub type RuntimeEventStream = Pin<Box<dyn Stream<Item = EventEnvelope> + Send>>;

/// Emits and fans out events for one session.
#[derive(Debug)]
pub struct EventEmitter {
    session: SessionId,
    minter: Arc<IdMinter>,
    clock: Arc<dyn Clock>,
    seq: AtomicU64,
    sender: broadcast::Sender<EventEnvelope>,
    observers: Arc<[Arc<dyn EventObserver>]>,
}

impl EventEmitter {
    /// Builds an emitter with the given broadcast buffer capacity.
    pub fn new(
        session: SessionId,
        minter: Arc<IdMinter>,
        clock: Arc<dyn Clock>,
        observers: Arc<[Arc<dyn EventObserver>]>,
        buffer: usize,
        initial_seq: u64,
    ) -> Self {
        let (sender, _) = broadcast::channel(buffer.max(1));
        Self {
            session,
            minter,
            clock,
            seq: AtomicU64::new(initial_seq),
            sender,
            observers,
        }
    }

    /// The sequence number that will be assigned to the next event.
    pub fn next_sequence(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// The session this emitter belongs to.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// Emits an event under `turn`, delivering it to observers and subscribers.
    pub fn emit(&self, turn: Option<TurnId>, payload: RuntimeEvent) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let envelope = EventEnvelope::new(
            seq,
            self.minter.event(),
            self.session.clone(),
            turn,
            self.clock.now(),
            payload,
        );
        for observer in self.observers.iter() {
            observer.observe(&envelope);
        }
        // Ignore the error when there are no live subscribers.
        let _ = self.sender.send(envelope);
    }

    /// Subscribes to the event stream. Lagged events are skipped.
    pub fn subscribe(&self) -> RuntimeEventStream {
        let mut rx = self.sender.subscribe();
        Box::pin(stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => yield event,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}
