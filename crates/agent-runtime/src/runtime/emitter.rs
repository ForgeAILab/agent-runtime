//! The per-session event emitter and subscription stream.
//!
//! Events are stamped with a monotonic per-session sequence number and a
//! versioned envelope, delivered synchronously to injected observers, and
//! broadcast to any number of concurrent subscribers. Subscribers that fall
//! behind skip lagged events rather than blocking the runtime.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_stream::stream;
use futures_core::Stream;
use tokio::sync::broadcast;

use agent_runtime_core::clock::Clock;
use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime_core::ids::{AttemptId, SessionId, TurnId};
use agent_runtime_core::observer::EventObserver;

use crate::ids::IdMinter;

/// A stream of runtime events for one subscriber.
pub type RuntimeEventStream = Pin<Box<dyn Stream<Item = EventEnvelope> + Send>>;

type DeferredCacheEvent = (Option<TurnId>, RuntimeEvent);
type DeferredCacheEvents = Vec<DeferredCacheEvent>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheTailStatus {
    /// ResultReady is protected, but the deferred tail was not published.
    NeedsReplay,
    /// The deferred tail crossed the synchronous publication boundary.
    Published,
}

#[derive(Debug, Default)]
struct CacheEventState {
    active: Option<DeferredCacheEvents>,
    tail_status: BTreeMap<TurnId, CacheTailStatus>,
}

#[derive(Debug, Default)]
struct PendingPresentation {
    next_sequence: u64,
    attempts: BTreeMap<AttemptId, Vec<EventEnvelope>>,
    external: Vec<EventEnvelope>,
    retained: super::transcript::RetainedHistoryObservations,
}

/// An RAII guard for one deferred cache-event batch.
///
/// A cache dispatch can be aborted at any await point, including while the
/// provider future or a protected checkpoint save is pending.  Owning this
/// guard keeps the emitter from being left permanently in a batching state:
/// an uncommitted batch is discarded automatically, while an explicit flush
/// marks it committed before the guard is dropped.
#[derive(Debug)]
pub(crate) struct CacheEventBatch {
    emitter: Arc<EventEmitter>,
    active: bool,
    turn: Option<TurnId>,
    result_ready: bool,
}

impl CacheEventBatch {
    /// Publishes the batch in its original order and consumes the guard.
    pub(crate) fn flush(mut self) {
        if self.active {
            self.emitter.flush_cache_events(self.turn.as_ref());
            self.active = false;
        }
    }

    /// Marks the ResultReady boundary as in flight. If this future is
    /// cancelled after the protected save commits, Drop records that the
    /// deferred tail must be reconstructed before Terminal.
    pub(crate) fn mark_result_ready(&mut self) {
        if self.active {
            self.result_ready = true;
            if let Some(turn) = &self.turn {
                self.emitter.mark_cache_tail_needs_replay(turn);
            }
        }
    }
}

impl Drop for CacheEventBatch {
    fn drop(&mut self) {
        if self.active {
            // Dropping an aborted dispatch is a normal cleanup path.  Never
            // panic from a destructor (especially during unwinding); the
            // explicit flush/discard APIs retain assertions for programmer
            // errors while Drop remains best-effort cleanup.
            let _ = self
                .emitter
                .discard_cache_events_checked(self.turn.as_ref(), self.result_ready);
            self.active = false;
        }
    }
}

/// Emits and fans out events for one session.
#[derive(Debug)]
pub struct EventEmitter {
    session: SessionId,
    minter: Arc<IdMinter>,
    clock: Arc<dyn Clock>,
    /// Serializes the complete publication transaction. Sequence minting,
    /// synchronous observer delivery, and broadcast therefore have one
    /// physical order, while `next_sequence` remains independently readable
    /// by reentrant observers taking a session snapshot.
    delivery: Mutex<()>,
    next_sequence: AtomicU64,
    sender: broadcast::Sender<EventEnvelope>,
    observers: Arc<[Arc<dyn EventObserver>]>,
    /// Cache lifecycle/evidence events are staged here while the cache
    /// mechanism writes its ResultReady checkpoint. Ordinary events never
    /// enter this queue: they are delivered synchronously by `emit` and are
    /// therefore never hidden behind, or discarded with, a cache batch.
    cache_events: Mutex<CacheEventState>,
    pending_presentation: Mutex<PendingPresentation>,
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
            delivery: Mutex::new(()),
            next_sequence: AtomicU64::new(initial_seq),
            sender,
            observers,
            cache_events: Mutex::new(CacheEventState::default()),
            pending_presentation: Mutex::new(PendingPresentation {
                next_sequence: initial_seq,
                ..PendingPresentation::default()
            }),
        }
    }

    /// The sequence number that will be assigned to the next event.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    /// The session this emitter belongs to.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// Emits an event under `turn`, delivering it to observers and subscribers.
    pub fn emit(&self, turn: Option<TurnId>, payload: RuntimeEvent) {
        self.emit_now(turn, payload);
    }

    fn emit_now(&self, turn: Option<TurnId>, payload: RuntimeEvent) {
        self.emit_with_history_position(turn, payload, None);
    }

    pub(crate) fn emit_retained(
        &self,
        turn: Option<TurnId>,
        payload: RuntimeEvent,
        history_index: usize,
    ) {
        self.emit_with_history_position(turn, payload, Some(history_index));
    }

    fn emit_with_history_position(
        &self,
        turn: Option<TurnId>,
        payload: RuntimeEvent,
        history_index: Option<usize>,
    ) {
        // Sequence allocation, observer delivery, and broadcast are one
        // publication transaction. In particular, returning from this
        // function is the host-visible delivery acknowledgement: a
        // concurrent checkpoint cannot observe an allocated-but-undelivered
        // envelope and later truncate it as an invisible tail.
        let _delivery = self
            .delivery
            .lock()
            .expect("event publication state poisoned");
        let sequence = self.next_sequence.fetch_add(1, Ordering::AcqRel);
        let envelope = EventEnvelope::new(
            sequence,
            self.minter.event(),
            self.session.clone(),
            turn,
            self.clock.now(),
            payload,
        );
        {
            let mut pending = self
                .pending_presentation
                .lock()
                .expect("pending presentation poisoned");
            pending.next_sequence = sequence + 1;
            if let Some(history_index) = history_index {
                pending
                    .retained
                    .events
                    .push(super::transcript::ChildHistoryObservation {
                        history_index,
                        event: envelope.clone(),
                    });
            }
            match &envelope.payload {
                RuntimeEvent::ProviderAttemptStarted { attempt, .. }
                | RuntimeEvent::TextDelta { attempt, .. }
                | RuntimeEvent::ReasoningDelta { attempt, .. } => {
                    pending
                        .attempts
                        .entry(attempt.clone())
                        .or_default()
                        .push(envelope.clone());
                }
                RuntimeEvent::ProviderAttemptOutputDiscarded { attempt, .. }
                | RuntimeEvent::ProviderAttemptOutputCommitted { attempt, .. } => {
                    pending.attempts.remove(attempt);
                }
                RuntimeEvent::ExternalText { .. } | RuntimeEvent::ExternalReasoning { .. } => {
                    pending.external.push(envelope.clone())
                }
                RuntimeEvent::TurnCompleted { .. } => {
                    pending.attempts.clear();
                    pending.external.clear();
                }
                _ => {}
            }
        }
        for observer in self.observers.iter() {
            observer.observe(&envelope);
        }
        // Ignore the error when there are no live subscribers.
        let _ = self.sender.send(envelope);
    }

    /// Called under the session state lock when this attempt's message is appended.
    pub(crate) fn commit_presentation(&self, attempt: &AttemptId) {
        self.pending_presentation
            .lock()
            .expect("pending presentation poisoned")
            .attempts
            .remove(attempt);
    }

    #[cfg(feature = "external-agent")]
    pub(crate) fn commit_external_presentation(&self) {
        self.pending_presentation
            .lock()
            .expect("pending presentation poisoned")
            .external
            .clear();
    }

    #[cfg(feature = "external-agent")]
    pub(crate) fn take_external_presentation(&self) -> Vec<EventEnvelope> {
        std::mem::take(
            &mut self
                .pending_presentation
                .lock()
                .expect("pending presentation poisoned")
                .external,
        )
    }

    #[cfg(feature = "external-agent")]
    pub(crate) fn restore_external_presentation(&self, events: Vec<EventEnvelope>) {
        let mut pending = self
            .pending_presentation
            .lock()
            .expect("pending presentation poisoned");
        pending.external.extend(events);
        pending.external.sort_by_key(|event| event.seq);
    }

    pub(crate) fn presentation_snapshot(
        &self,
    ) -> (
        u64,
        Vec<EventEnvelope>,
        super::transcript::RetainedHistoryObservations,
    ) {
        let pending = self
            .pending_presentation
            .lock()
            .expect("pending presentation poisoned");
        let mut events = pending
            .attempts
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        events.extend(pending.external.iter().cloned());
        events.sort_by_key(|event| event.seq);
        (pending.next_sequence, events, pending.retained.clone())
    }

    pub(crate) fn restore_history_observations(
        &self,
        observations: super::transcript::RetainedHistoryObservations,
    ) {
        self.pending_presentation
            .lock()
            .expect("pending presentation poisoned")
            .retained = observations;
    }

    pub(crate) fn history_observation_state(
        &self,
    ) -> agent_runtime_core::store::VersionedSessionState {
        self.pending_presentation
            .lock()
            .expect("pending presentation poisoned")
            .retained
            .state()
    }

    /// Establishes a cache checkpoint watermark after all prior synchronous
    /// publications have completed. `emit_now` holds the same mutex through
    /// delivery, so taking it here is also the publication acknowledgement
    /// barrier. Events emitted while an async checkpoint save is in progress
    /// receive ordinary sequences and are reconciled by the cache operation's
    /// turn-scoped journal boundary rather than a volatile in-memory queue.
    pub(crate) fn begin_checkpoint_barrier(&self) -> u64 {
        let _delivery = self
            .delivery
            .lock()
            .expect("event publication state poisoned");
        self.next_sequence.load(Ordering::Acquire)
    }

    /// Completes the compatibility boundary around an async checkpoint save.
    /// There is no deferred ordinary-event queue to flush: every ordinary
    /// event was already synchronously delivered by `emit`.
    pub(crate) fn end_checkpoint_barrier(&self) {
        // Intentionally empty. Kept as a paired API so checkpoint save error
        // paths remain structurally obvious at call sites.
    }

    /// Starts a deferred cache-event batch. Cache dispatch is serialized by
    /// the session cache/turn gates, so nesting is a programmer error.
    #[cfg(test)]
    pub(crate) fn begin_cache_events(self: &Arc<Self>) -> CacheEventBatch {
        self.begin_cache_events_with_turn(None)
    }

    /// Starts a deferred cache-event batch associated with one protected
    /// synthetic turn. The association lets an aborted ResultReady save be
    /// repaired without guessing whether its tail was already published.
    pub(crate) fn begin_cache_events_for_turn(self: &Arc<Self>, turn: TurnId) -> CacheEventBatch {
        self.begin_cache_events_with_turn(Some(turn))
    }

    fn begin_cache_events_with_turn(self: &Arc<Self>, turn: Option<TurnId>) -> CacheEventBatch {
        let mut state = self
            .cache_events
            .lock()
            .expect("cache event queue poisoned");
        assert!(state.active.is_none(), "cache event batch already active");
        if let Some(turn) = &turn {
            state.tail_status.remove(turn);
        }
        state.active = Some(Vec::new());
        CacheEventBatch {
            emitter: self.clone(),
            active: true,
            turn,
            result_ready: false,
        }
    }

    /// Queues one cache lifecycle/evidence/usage event while a result
    /// checkpoint is being prepared. Outside a batch it behaves like `emit`.
    pub(crate) fn emit_cache(&self, turn: Option<TurnId>, payload: RuntimeEvent) {
        let mut state = self
            .cache_events
            .lock()
            .expect("cache event queue poisoned");
        if let Some(events) = state.active.as_mut() {
            events.push((turn, payload));
            return;
        }
        drop(state);
        self.emit_now(turn, payload);
    }

    /// Publishes a committed deferred cache batch in its original order.
    pub(crate) fn flush_cache_events(&self, turn: Option<&TurnId>) {
        let mut state = self
            .cache_events
            .lock()
            .expect("cache event queue poisoned");
        let events = state
            .active
            .take()
            .expect("cache event batch is not active");
        // Hold the publication mutex across the whole cache tail so its
        // sequence range is contiguous and observers cannot observe an
        // ordinary event interleaved between two correlated cache events.
        let _delivery = self
            .delivery
            .lock()
            .expect("event publication state poisoned");
        for (turn, payload) in events {
            let sequence = self.next_sequence.fetch_add(1, Ordering::AcqRel);
            let envelope = EventEnvelope::new(
                sequence,
                self.minter.event(),
                self.session.clone(),
                turn,
                self.clock.now(),
                payload,
            );
            for observer in self.observers.iter() {
                observer.observe(&envelope);
            }
            let _ = self.sender.send(envelope);
        }
        if let Some(turn) = turn {
            state
                .tail_status
                .insert(turn.clone(), CacheTailStatus::Published);
        }
    }

    fn discard_cache_events_checked(
        &self,
        turn: Option<&TurnId>,
        result_ready: bool,
    ) -> Result<(), ()> {
        // This helper is also called from `Drop`; recover the poisoned guard
        // and clear the batch rather than allowing an unwinding cleanup path
        // to panic a second time. Explicit begin/flush paths retain their
        // programmer-error assertions.
        let mut state = match self.cache_events.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.active.is_none() {
            return Err(());
        }
        state.active.take();
        if let Some(turn) = turn {
            if result_ready {
                state
                    .tail_status
                    .insert(turn.clone(), CacheTailStatus::NeedsReplay);
            } else {
                state.tail_status.remove(turn);
            }
        }
        Ok(())
    }

    /// Returns whether this process has already published the protected cache
    /// tail. An absent status is deliberately treated as unpublished so a
    /// fresh process reconstructs ResultReady events from protected metadata.
    pub(crate) fn cache_tail_published(&self, turn: &TurnId) -> bool {
        self.cache_events
            .lock()
            .expect("cache event queue poisoned")
            .tail_status
            .get(turn)
            .is_some_and(|status| *status == CacheTailStatus::Published)
    }

    pub(crate) fn clear_cache_tail(&self, turn: &TurnId) {
        self.cache_events
            .lock()
            .expect("cache event queue poisoned")
            .tail_status
            .remove(turn);
    }

    fn mark_cache_tail_needs_replay(&self, turn: &TurnId) {
        self.cache_events
            .lock()
            .expect("cache event queue poisoned")
            .tail_status
            .insert(turn.clone(), CacheTailStatus::NeedsReplay);
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::Weak;

    #[tokio::test]
    async fn presentation_seed_and_subscribed_tail_cover_each_fragment_once() {
        use agent_runtime_core::ids::RequestId;
        let emitter = EventEmitter::new(
            SessionId::new("presentation-boundary"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn EventObserver>>::new()),
            32,
            0,
        );
        let attempt = AttemptId::new("attempt-1");
        let request = RequestId::new("request-1");
        emitter.emit(
            None,
            RuntimeEvent::ProviderAttemptStarted {
                request: request.clone(),
                attempt: attempt.clone(),
                index: 0,
                model: "fake".into(),
            },
        );
        let emit = |text: &str| {
            emitter.emit(
                None,
                RuntimeEvent::TextDelta {
                    request: request.clone(),
                    attempt: attempt.clone(),
                    text: text.into(),
                },
            )
        };
        emit("before subscription ");
        let mut events = emitter.subscribe();
        emit("during hydration ");
        let (watermark, seed, _) = emitter.presentation_snapshot();
        emit("after snapshot");
        let mut observed = seed;
        while let Some(event) = events.next().await {
            let terminal = matches!(&event.payload, RuntimeEvent::TextDelta { text, .. } if text == "after snapshot");
            if event.seq >= watermark {
                observed.push(event);
            }
            if terminal {
                break;
            }
        }
        let text = observed
            .iter()
            .filter_map(|event| match &event.payload {
                RuntimeEvent::TextDelta { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(text, "before subscription during hydration after snapshot");
        emitter.commit_presentation(&attempt);
        assert!(emitter.presentation_snapshot().1.is_empty());
        emit("discarded");
        emitter.emit(
            None,
            RuntimeEvent::ProviderAttemptOutputDiscarded { request, attempt },
        );
        assert!(emitter.presentation_snapshot().1.is_empty());
    }

    #[derive(Debug, Default)]
    struct ReentrantSequenceObserver {
        emitter: Mutex<Option<Weak<EventEmitter>>>,
    }

    impl EventObserver for ReentrantSequenceObserver {
        fn observe(&self, _event: &EventEnvelope) {
            if let Some(emitter) = self
                .emitter
                .lock()
                .expect("reentrant observer poisoned")
                .as_ref()
                .and_then(Weak::upgrade)
            {
                // Session snapshots read next_sequence from the same
                // observer callback. This must not deadlock publication.
                let _ = emitter.next_sequence();
            }
        }
    }

    #[tokio::test]
    async fn observer_can_read_sequence_during_publication() {
        let observer = Arc::new(ReentrantSequenceObserver::default());
        let emitter = Arc::new(EventEmitter::new(
            SessionId::new("emitter-reentrant-sequence"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(vec![observer.clone() as Arc<dyn EventObserver>]),
            8,
            0,
        ));
        *observer
            .emitter
            .lock()
            .expect("reentrant observer poisoned") = Some(Arc::downgrade(&emitter));
        emitter.emit(None, RuntimeEvent::SessionShutdown);
        assert_eq!(emitter.next_sequence(), 1);
    }

    #[tokio::test]
    async fn ordinary_events_bypass_cache_batch_and_survive_discard() {
        let emitter = Arc::new(EventEmitter::new(
            SessionId::new("emitter-cache-batch"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn EventObserver>>::new()),
            8,
            0,
        ));
        let mut events = emitter.subscribe();
        let cache_events = emitter.begin_cache_events();

        // Session/shutdown and child/session producers use the ordinary
        // emitter path. They must not be held behind a provider cache call or
        // disappear when the protected ResultReady save fails.
        emitter.emit(None, RuntimeEvent::SessionShutdown);
        let envelope = events
            .next()
            .await
            .expect("ordinary event remains observable during cache batch");
        assert!(matches!(envelope.payload, RuntimeEvent::SessionShutdown));
        assert_eq!(emitter.next_sequence(), 1);

        drop(cache_events);
        assert_eq!(emitter.next_sequence(), 1);
    }

    #[tokio::test]
    async fn checkpoint_barrier_observes_prior_publications_before_watermark() {
        let emitter = Arc::new(EventEmitter::new(
            SessionId::new("emitter-checkpoint-barrier"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn EventObserver>>::new()),
            8,
            7,
        ));
        let mut events = emitter.subscribe();
        let watermark = emitter.begin_checkpoint_barrier();
        assert_eq!(watermark, 7);
        emitter.emit(None, RuntimeEvent::SessionShutdown);
        assert_eq!(emitter.next_sequence(), watermark + 1);
        emitter.end_checkpoint_barrier();
        let envelope = events
            .next()
            .await
            .expect("ordinary event is published synchronously");
        assert_eq!(envelope.seq, watermark);
        assert!(matches!(envelope.payload, RuntimeEvent::SessionShutdown));
        assert_eq!(emitter.next_sequence(), watermark + 1);
    }

    #[tokio::test]
    async fn scoped_cache_reconciliation_preserves_interleaved_ordinary_event() {
        let emitter = Arc::new(EventEmitter::new(
            SessionId::new("emitter-scoped-journal"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn EventObserver>>::new()),
            8,
            0,
        ));
        let mut events = emitter.subscribe();
        let cache_turn = TurnId::new("cache-operation:interleaved");

        // A prior cache phase is already settled before the next protected
        // save captures its watermark.
        emitter.emit(Some(cache_turn.clone()), RuntimeEvent::SessionShutdown);
        let watermark = emitter.begin_checkpoint_barrier();
        assert_eq!(watermark, 1);

        // This models a child/session producer running while the async
        // checkpoint store write is pending. It is delivered immediately and
        // therefore cannot disappear into a volatile barrier queue.
        emitter.emit(None, RuntimeEvent::SessionShutdown);
        emitter.end_checkpoint_barrier();

        let cache_events = emitter.begin_cache_events();
        emitter.emit_cache(Some(cache_turn.clone()), RuntimeEvent::SessionShutdown);
        cache_events.flush();

        let before = [
            events.next().await.expect("prior cache event"),
            events.next().await.expect("interleaved ordinary event"),
            events.next().await.expect("cache tail event"),
        ];
        let scope = agent_runtime_core::checkpoint::JournalTruncationScope {
            event_sequence: watermark,
            turn: Some(cache_turn.clone()),
        };
        let retained = before
            .iter()
            .filter(|event| {
                event.seq < scope.event_sequence || event.turn.as_ref() != scope.turn.as_ref()
            })
            .collect::<Vec<_>>();
        assert_eq!(retained.len(), 2);
        assert_eq!(
            retained.iter().filter(|event| event.turn.is_none()).count(),
            1,
            "unrelated ordinary event survives scoped truncation exactly once"
        );
        assert_eq!(
            before
                .iter()
                .filter(|event| event.turn.as_ref() == Some(&cache_turn))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn dropped_cache_batch_recovers_for_the_next_dispatch() {
        let emitter = Arc::new(EventEmitter::new(
            SessionId::new("emitter-cache-batch-drop"),
            Arc::new(IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn EventObserver>>::new()),
            8,
            0,
        ));
        let mut events = emitter.subscribe();
        let first = emitter.begin_cache_events();
        emitter.emit_cache(None, RuntimeEvent::SessionShutdown);
        drop(first);
        assert_eq!(emitter.next_sequence(), 0);

        let second = emitter.begin_cache_events();
        emitter.emit_cache(None, RuntimeEvent::SessionShutdown);
        second.flush();
        let event = events.next().await.expect("flushed batch event");
        assert_eq!(event.seq, 0);
        assert!(matches!(event.payload, RuntimeEvent::SessionShutdown));
    }
}
