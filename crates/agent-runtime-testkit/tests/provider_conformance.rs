//! Provider conformance: fake and OpenAI-compatible adapters produce the same
//! normalized event contract.

use std::sync::Arc;

use agent_runtime::provider::ProviderCredentialTarget;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, usage_event};
use agent_runtime::provider::gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider};
use agent_runtime::provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::Message;
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelId, ProviderRequest, ProviderStreamEvent, ReasoningSupport,
};
use agent_runtime_core::store::Secret;
use agent_runtime_testkit::conformance::provider as pc;
use agent_runtime_testkit::{
    CredentialLeaseFixture, ManualClock, RenewableProviderCredentialSource, ReplayTransport,
};

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

fn gemini_config() -> GeminiInteractionsConfig {
    let mut config = GeminiInteractionsConfig::new(
        "https://generativelanguage.googleapis.com/v1beta",
        "gemini-x",
    )
    .with_supported_thinking_levels(["low", "medium", "high"]);
    config.api_key = Some(Secret::new("fixture-key"));
    config
}

fn gemini_two_chunk_text() -> GeminiInteractionsProvider<ReplayTransport> {
    GeminiInteractionsProvider::new(
        ReplayTransport::single(include_str!("fixtures/gemini-text.sse")),
        gemini_config(),
    )
    .expect("Gemini fixture config")
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

#[tokio::test]
async fn gemini_adapter_meets_normalized_contract() {
    let provider = gemini_two_chunk_text();
    pc::assert_normalized_text_stream(&provider, &ModelId::new("gemini-x")).await;
}

#[tokio::test]
async fn openai_adapter_requests_and_injects_a_proactively_refreshed_lease() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let clock = ManualClock::shared(100);
    let source = Arc::new(RenewableProviderCredentialSource::new(
        clock.clone(),
        CredentialLeaseFixture::expiring("old-canary", Timestamp(120), "r1").unwrap(),
        [CredentialLeaseFixture::expiring("new-canary", Timestamp(1_000), "r2").unwrap()],
    ));
    let provider = OpenAiProvider::with_credential_source(
        ReplayTransport::single(sse),
        OpenAiConfig::new("http://local/v1", "gpt-x"),
        ProviderCredentialTarget::new("openrouter").unwrap(),
        source.clone(),
    )
    .unwrap()
    .with_clock(clock)
    .with_credential_minimum_validity_ms(30);

    let events = pc::collect(
        &provider,
        ProviderRequest::new(ModelId::new("gpt-x"), vec![Message::user("hi")]),
    )
    .await;

    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Finish {
            reason: FinishReason::Stop
        })
    ));
    assert_eq!(source.acquisitions().len(), 1);
    assert!(
        provider.transport().requests()[0]
            .headers
            .iter()
            .any(|(name, value)| name == "authorization" && value == "Bearer new-canary")
    );
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

#[tokio::test]
async fn fake_and_gemini_produce_the_same_text_event_kinds() {
    let fake_events = pc::collect(
        &fake_two_chunk_text(),
        ProviderRequest::new(ModelId::new("fake"), vec![Message::user("hi")]),
    )
    .await;
    let gemini_events = pc::collect(
        &gemini_two_chunk_text(),
        ProviderRequest::new(ModelId::new("gemini-x"), vec![Message::user("hi")]),
    )
    .await;

    assert_eq!(
        fake_events.iter().map(kind).collect::<Vec<_>>(),
        gemini_events.iter().map(kind).collect::<Vec<_>>()
    );
}

// provider-runtime: cancellation is observed by a streaming provider.
#[tokio::test]
async fn provider_observes_cancellation() {
    let provider = agent_runtime_testkit::scenarios::fake_blocking();
    pc::assert_cancellation_stops_stream(&provider, &ModelId::new("fake")).await;
}

fn fake_reasoning_then_text() -> FakeProvider {
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::ReasoningDelta {
                text: "thinking".into(),
                redacted: false,
                signature: None,
            },
            ProviderStreamEvent::TextDelta { text: "Hi".into() },
            usage_event(10, 2),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    )
}

fn openai_reasoning_then_text() -> OpenAiProvider<ReplayTransport> {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],",
        "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n",
        "data: [DONE]\n\n",
    );
    OpenAiProvider::new(
        ReplayTransport::single(sse),
        OpenAiConfig::new("http://local/v1", "gpt-x"),
    )
}

/// A continuation-shaped history: the assistant already produced reasoning
/// and now the provider is asked to keep going.
fn reasoning_continuation(model: &str) -> ProviderRequest {
    use agent_runtime_core::content::{ContentPart, Role};
    ProviderRequest::new(
        ModelId::new(model),
        vec![
            Message::user("think it through"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentPart::Reasoning {
                        text: "prior thought".into(),
                        redacted: false,
                        signature: None,
                    },
                    ContentPart::text("so far so good"),
                ],
            },
            Message::user("continue"),
        ],
    )
}

// provider-runtime: streamed reasoning normalizes identically across
// adapters, and a continuation carrying reasoning back is accepted.
#[tokio::test]
async fn fake_adapter_normalizes_reasoning() {
    let provider = fake_reasoning_then_text();
    pc::assert_normalized_reasoning_stream(&provider, reasoning_continuation("fake")).await;
}

#[tokio::test]
async fn openai_adapter_normalizes_reasoning() {
    let provider = openai_reasoning_then_text();
    pc::assert_normalized_reasoning_stream(&provider, reasoning_continuation("gpt-x")).await;
}

#[tokio::test]
async fn gemini_adapter_normalizes_signed_reasoning() {
    let mut config = gemini_config();
    config.capabilities.reasoning = ReasoningSupport::Controllable;
    let provider = GeminiInteractionsProvider::new(
        ReplayTransport::single(include_str!("fixtures/gemini-reasoning.sse")),
        config,
    )
    .expect("Gemini fixture config");
    pc::assert_normalized_reasoning_stream(
        &provider,
        ProviderRequest::new(ModelId::new("gemini-x"), vec![Message::user("think")]),
    )
    .await;
}

#[tokio::test]
async fn fake_and_openai_produce_the_same_reasoning_event_kinds() {
    let fake_events =
        pc::collect(&fake_reasoning_then_text(), reasoning_continuation("fake")).await;
    let openai_events = pc::collect(
        &openai_reasoning_then_text(),
        reasoning_continuation("gpt-x"),
    )
    .await;

    let fake_kinds: Vec<&str> = fake_events.iter().map(kind).collect();
    let openai_kinds: Vec<&str> = openai_events.iter().map(kind).collect();
    assert_eq!(
        fake_kinds, openai_kinds,
        "normalized reasoning event kinds must match"
    );
}

// provider-runtime: the OpenAI adapter echoes non-redacted history reasoning
// as `reasoning_content` on the continuation wire request.
#[tokio::test]
async fn openai_wire_request_carries_reasoning_content_back() {
    let provider = openai_reasoning_then_text();
    let _ = pc::collect(&provider, reasoning_continuation("gpt-x")).await;

    let requests = provider.transport().requests();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(&requests[0].body).expect("wire body is JSON");
    let assistant = body["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("assistant message on the wire");
    assert_eq!(assistant["reasoning_content"], "prior thought");
}
