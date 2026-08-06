//! Presentation-delta coalescing.
//!
//! `provider.rs` batches `TextDelta`/`ReasoningDelta` provider events into
//! fewer `RuntimeEvent`s before they reach the bounded broadcast channel (see
//! `DeltaCoalescer` in `agent/driver/provider.rs`). These tests exercise the
//! four load-bearing properties from the outside, through a real session: a
//! burst coalesces without losing or reordering bytes, interleaved kinds keep
//! exact relative order, a slow trickle is never delayed, and pending text
//! survives every stream exit path including mid-burst cancellation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::Notify;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::runtime::{RuntimeBuilder, RuntimeEventStream, StartSession};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// A clock the test advances from inside the provider stream, so delta
/// timing relative to the coalescing window is deterministic instead of
/// racing real wall-clock time.
#[derive(Debug, Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

/// One scripted provider event, paired with how far to advance the shared
/// clock immediately before it is yielded.
fn step(advance_ms: u64, event: ProviderStreamEvent) -> (u64, ProviderStreamEvent) {
    (advance_ms, event)
}

/// A provider whose stream advances a shared [`TestClock`] before yielding
/// each scripted event, then optionally blocks until cancelled. The blocking
/// generator notifies `blocked` immediately before it awaits cancellation —
/// since every step up to that point runs synchronously (no `.await` inside
/// the loop), a test that awaits that notification is guaranteed the driver
/// has already consumed and accumulated the entire script.
#[derive(Debug)]
struct ClockScriptedProvider {
    clock: Arc<TestClock>,
    script: Mutex<VecDeque<(u64, ProviderStreamEvent)>>,
    block_until_cancel: bool,
    blocked: Arc<Notify>,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl ClockScriptedProvider {
    fn new(clock: Arc<TestClock>, script: Vec<(u64, ProviderStreamEvent)>) -> Self {
        Self {
            clock,
            script: Mutex::new(script.into()),
            block_until_cancel: false,
            blocked: Arc::new(Notify::new()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn blocking(
        clock: Arc<TestClock>,
        script: Vec<(u64, ProviderStreamEvent)>,
        blocked: Arc<Notify>,
    ) -> Self {
        Self {
            clock,
            script: Mutex::new(script.into()),
            block_until_cancel: true,
            blocked,
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Provider for ClockScriptedProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: ModelId::new("fake"),
            display_name: "fake".into(),
            vendor: "test".into(),
            capabilities: Capabilities::basic_streaming(),
        }]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.requests
            .lock()
            .expect("requests poisoned")
            .push(request);
        let script: Vec<_> = self
            .script
            .lock()
            .expect("script poisoned")
            .drain(..)
            .collect();
        let clock = self.clock.clone();
        let cancel = ctx.cancel.clone();
        let block = self.block_until_cancel;
        let blocked = self.blocked.clone();
        let out = stream! {
            for (advance_ms, event) in script {
                clock.advance(advance_ms);
                yield event;
            }
            if block {
                blocked.notify_waiters();
                cancel.cancelled().await;
                yield ProviderStreamEvent::Error {
                    error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                };
            }
        };
        Ok(Box::pin(out))
    }
}

/// Collects `TextDelta`/`ReasoningDelta` payloads from a session's event
/// stream up to (and not including) `TurnCompleted`.
async fn collect_deltas(events: &mut RuntimeEventStream) -> Vec<(&'static str, String)> {
    let mut collected = Vec::new();
    while let Some(env) = events.next().await {
        match env.payload {
            RuntimeEvent::TextDelta { text, .. } => collected.push(("text", text)),
            RuntimeEvent::ReasoningDelta { text, .. } => collected.push(("reasoning", text)),
            RuntimeEvent::TurnCompleted { .. } => break,
            _ => {}
        }
    }
    collected
}

/// A burst of many tiny deltas delivered without intervening time coalesces
/// into far fewer emitted events, and the emitted text concatenates back to
/// exactly what the deltas carried — coalescing must never lose or reorder a
/// byte.
#[tokio::test]
async fn a_burst_of_tiny_deltas_coalesces_into_far_fewer_events() {
    let clock = TestClock::shared();
    let deltas: Vec<String> = (0..300).map(|_| "abcd".to_string()).collect();
    let mut script: Vec<(u64, ProviderStreamEvent)> = deltas
        .iter()
        .map(|d| step(0, ProviderStreamEvent::TextDelta { text: d.clone() }))
        .collect();
    script.push(step(
        0,
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ));
    let provider = Arc::new(ClockScriptedProvider::new(clock.clone(), script));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock)
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();

    let emitted = collect_deltas(&mut events).await;
    let reconstructed: String = emitted
        .iter()
        .map(|(_, text)| text.as_str())
        .collect::<Vec<_>>()
        .concat();
    assert_eq!(
        reconstructed,
        deltas.concat(),
        "the coalesced text must be byte-identical to the concatenated input deltas"
    );
    assert!(
        emitted.len() < deltas.len() / 10,
        "{} deltas must coalesce into far fewer than {} events, got {}",
        deltas.len(),
        deltas.len() / 10,
        emitted.len()
    );
}

/// Interleaved text and reasoning deltas keep their exact relative order
/// after coalescing: each kind switch flushes the other kind first, so a
/// text/reasoning/text/reasoning stream reaches subscribers in that same
/// order, merged only within each contiguous same-kind run.
#[tokio::test]
async fn interleaved_deltas_preserve_exact_relative_order() {
    let clock = TestClock::shared();
    let reasoning_delta = |text: &str| ProviderStreamEvent::ReasoningDelta {
        text: text.into(),
        redacted: false,
        signature: None,
    };
    let script = vec![
        step(0, ProviderStreamEvent::TextDelta { text: "A".into() }),
        step(0, ProviderStreamEvent::TextDelta { text: "B".into() }),
        step(0, reasoning_delta("R1")),
        step(0, reasoning_delta("R2")),
        step(0, ProviderStreamEvent::TextDelta { text: "C".into() }),
        step(0, ProviderStreamEvent::TextDelta { text: "D".into() }),
        step(0, reasoning_delta("R3")),
        step(
            0,
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ),
    ];
    let provider = Arc::new(ClockScriptedProvider::new(clock.clone(), script));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock)
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();

    let emitted = collect_deltas(&mut events).await;
    assert_eq!(
        emitted,
        vec![
            ("text", "AB".to_string()),
            ("reasoning", "R1R2".to_string()),
            ("text", "CD".to_string()),
            ("reasoning", "R3".to_string()),
        ],
        "each kind switch must flush the other kind first, preserving arrival order"
    );
}

/// Deltas separated by more than the coalescing window each emit promptly,
/// one `RuntimeEvent` per delta — a slow trickle must never wait on a byte
/// threshold or a delta that never comes.
#[tokio::test]
async fn a_trickle_of_deltas_emits_promptly_without_batching() {
    let clock = TestClock::shared();
    let window_ms = 20;
    let gap_ms = window_ms + 5;
    let words = ["alpha", "bravo", "charlie", "delta", "echo"];
    let mut script: Vec<(u64, ProviderStreamEvent)> = words
        .iter()
        .map(|word| {
            step(
                gap_ms,
                ProviderStreamEvent::TextDelta {
                    text: (*word).into(),
                },
            )
        })
        .collect();
    script.push(step(
        gap_ms,
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ));
    let provider = Arc::new(ClockScriptedProvider::new(clock.clone(), script));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock)
        .delta_coalesce_window_ms(window_ms)
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();

    let emitted = collect_deltas(&mut events).await;
    let texts: Vec<String> = emitted.into_iter().map(|(_, text)| text).collect();
    assert_eq!(
        texts,
        words.iter().map(|w| w.to_string()).collect::<Vec<_>>(),
        "a delta arriving after the window has already elapsed must flush the \
         prior delta on its own, not batch it with what arrives next"
    );
}

/// Pending text below the byte threshold is still flushed — never lost —
/// once the stream ends without another delta or non-delta event to trigger
/// it.
#[tokio::test]
async fn pending_text_flushes_when_the_stream_ends_below_threshold() {
    let clock = TestClock::shared();
    let script = vec![
        step(0, ProviderStreamEvent::TextDelta { text: "un".into() }),
        step(0, ProviderStreamEvent::TextDelta { text: "der".into() }),
        step(
            0,
            ProviderStreamEvent::TextDelta {
                text: "sized".into(),
            },
        ),
        step(
            0,
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ),
    ];
    let provider = Arc::new(ClockScriptedProvider::new(clock.clone(), script));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock)
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();

    let emitted = collect_deltas(&mut events).await;
    assert_eq!(
        emitted,
        vec![("text", "undersized".to_string())],
        "well under the byte threshold, so only the stream-end flush emits it"
    );
}

/// Pending text is flushed — never silently dropped — when the turn is
/// cancelled mid-burst, before any threshold or window would otherwise have
/// triggered a flush.
#[tokio::test]
async fn pending_text_flushes_when_the_turn_is_cancelled_mid_burst() {
    let clock = TestClock::shared();
    let blocked = Arc::new(Notify::new());
    // Well under the default 512-byte threshold, and the clock never
    // advances, so nothing but cancellation could flush this buffer.
    let deltas: Vec<String> = (0..50).map(|_| "chunk".to_string()).collect();
    let script: Vec<(u64, ProviderStreamEvent)> = deltas
        .iter()
        .map(|d| step(0, ProviderStreamEvent::TextDelta { text: d.clone() }))
        .collect();
    let provider = Arc::new(ClockScriptedProvider::blocking(
        clock.clone(),
        script,
        blocked.clone(),
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .clock(clock)
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();

    let ready = blocked.notified();
    let turn = session.send(UserInput::text("go")).unwrap();
    ready.await;
    session
        .interrupt_current_turn(CancelReason::UserRequested)
        .expect("the running turn has a cancellation handle");
    tokio::time::timeout(Duration::from_secs(5), turn.completed())
        .await
        .expect("the cancelled turn must terminate");

    let mut emitted = String::new();
    let mut saw_cancelled_completion = false;
    while let Some(env) = events.next().await {
        match env.payload {
            RuntimeEvent::TextDelta { text, .. } => emitted.push_str(&text),
            RuntimeEvent::TurnCompleted { finish, .. } => {
                saw_cancelled_completion = matches!(finish, TurnFinish::Cancelled { .. });
                break;
            }
            _ => {}
        }
    }
    assert!(saw_cancelled_completion, "the turn must end cancelled");
    assert_eq!(
        emitted,
        deltas.concat(),
        "every accumulated byte must still reach the event stream even though \
         the turn was cancelled before any threshold or window flush"
    );
}
