//! End-to-end wiring of the split-out facades through the live runtime.
//!
//! Proves two of the sibling crates compose with the core loop using only
//! neutral contracts: `agent-runtime-context`'s folded-in `SystemPromptBuilder`
//! assembles the system prompt that reaches the provider, and
//! `agent-runtime-obs` sinks the runtime's event stream via the synchronous
//! observer hook.

use std::sync::Arc;
use std::time::Duration;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_obs::testing::CapturingSink;
use agent_runtime_obs::{EventSink, FanoutSink, SinkObserver, log_line};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prompt_reaches_provider_and_events_reach_a_sink() {
    // 1. Assemble the system prompt with the context crate's prompt-section
    //    mechanism (host-supplied text).
    let mut prompt = SystemPromptBuilder::new();
    prompt
        .section("HARNESS", "You are a terminal coding assistant.")
        .section("WORKSPACE", "/repo");
    let system_prompt = prompt.build().expect("prompt renders");

    // 2. Wire an obs sink into the runtime's observer hook.
    let sink = Arc::new(CapturingSink::new());
    let fanout: Arc<dyn EventSink> = Arc::new(FanoutSink::new(vec![sink.clone()]));
    let observer = SinkObserver::spawn(fanout);

    let provider = Arc::new(FakeProvider::text_reply("done"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .provider(provider.clone())
        .system_prompt(system_prompt.clone())
        .observer(observer.clone())
        .build()
        .expect("runtime builds");

    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("hi")).await;

    // 3. The assembled prompt reached the provider as the first system message.
    let requests = provider.requests();
    assert_eq!(requests.len(), 1, "one provider request for one turn");
    let first_system = requests[0].messages.first().expect("a system message");
    assert!(
        first_system
            .joined_text()
            .contains("terminal coding assistant"),
        "system prompt from the context crate's prompt builder reached the provider"
    );

    // 4. The obs sink observed the turn. The observer bridge drains off the hot
    //    path, so poll briefly for the terminal event.
    let mut saw_completed = false;
    for _ in 0..100 {
        let events = sink.events();
        if events
            .iter()
            .any(|e| matches!(e.payload, RuntimeEvent::TurnCompleted { .. }))
        {
            saw_completed = true;
            // The neutral renderer must format a real event without panicking.
            let line = log_line(&events[0]);
            assert!(line.starts_with('#'));
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(saw_completed, "obs sink received the turn's events");
    assert_eq!(observer.dropped(), 0, "no events dropped for a short turn");
}
