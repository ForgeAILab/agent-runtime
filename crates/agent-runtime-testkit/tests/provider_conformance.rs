//! Provider conformance: fake and OpenAI-compatible adapters produce the same
//! normalized event contract.

use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, usage_event};
use agent_runtime::provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime_core::content::Message;
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelId, ProviderRequest, ProviderStreamEvent,
};
use agent_runtime_testkit::ReplayTransport;
use agent_runtime_testkit::conformance::provider as pc;

fn kind(event: &ProviderStreamEvent) -> &'static str {
    match event {
        ProviderStreamEvent::TextDelta { .. } => "text",
        ProviderStreamEvent::ReasoningDelta { .. } => "reasoning",
        ProviderStreamEvent::ToolCallDelta { .. } => "tool_call",
        ProviderStreamEvent::Finish { .. } => "finish",
        ProviderStreamEvent::Error { .. } => "error",
        ProviderStreamEvent::Usage { .. } => "usage",
        ProviderStreamEvent::CacheObservation { .. } => "cache",
        ProviderStreamEvent::Downgrade { .. } => "downgrade",
        ProviderStreamEvent::VendorMetadata { .. } => "vendor",
    }
}

fn fake_two_chunk_text() -> FakeProvider {
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta { text: "Hel".into() },
            ProviderStreamEvent::TextDelta { text: "lo".into() },
            usage_event(10, 2),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    )
}

fn openai_two_chunk_text() -> OpenAiProvider<ReplayTransport> {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],",
        "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n",
        "data: [DONE]\n\n",
    );
    OpenAiProvider::new(
        ReplayTransport::single(sse),
        OpenAiConfig::new("http://local/v1", "gpt-x"),
    )
}

#[tokio::test]
async fn fake_adapter_meets_normalized_contract() {
    let provider = fake_two_chunk_text();
    pc::assert_normalized_text_stream(&provider, &ModelId::new("fake")).await;
}

#[tokio::test]
async fn openai_adapter_meets_normalized_contract() {
    let provider = openai_two_chunk_text();
    pc::assert_normalized_text_stream(&provider, &ModelId::new("gpt-x")).await;
}

// provider-runtime: "Compare fake and production adapter contracts" — equivalent
// fixtures produce the same normalized event-kind sequence.
#[tokio::test]
async fn fake_and_openai_produce_the_same_event_kinds() {
    let fake = fake_two_chunk_text();
    let fake_events = pc::collect(
        &fake,
        ProviderRequest::new(ModelId::new("fake"), vec![Message::user("hi")]),
    )
    .await;

    let openai = openai_two_chunk_text();
    let openai_events = pc::collect(
        &openai,
        ProviderRequest::new(ModelId::new("gpt-x"), vec![Message::user("hi")]),
    )
    .await;

    let fake_kinds: Vec<&str> = fake_events.iter().map(kind).collect();
    let openai_kinds: Vec<&str> = openai_events.iter().map(kind).collect();
    assert_eq!(
        fake_kinds, openai_kinds,
        "normalized event kinds must match"
    );
}

// provider-runtime: cancellation is observed by a streaming provider.
#[tokio::test]
async fn provider_observes_cancellation() {
    let provider = agent_runtime_testkit::scenarios::fake_blocking();
    pc::assert_cancellation_stops_stream(&provider, &ModelId::new("fake")).await;
}
