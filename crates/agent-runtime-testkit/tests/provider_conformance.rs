//! Provider conformance: fake and OpenAI-compatible adapters produce the same
//! normalized event contract.

use std::sync::Arc;

use agent_runtime::provider::ProviderCredentialTarget;
use agent_runtime::provider::anthropic::{AnthropicConfig, AnthropicProvider};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, cache_observation, usage_event};
use agent_runtime::provider::gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider};
use agent_runtime::provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime::provider::responses::{ResponsesConfig, ResponsesProvider};
use agent_runtime_core::catalog::{
    CatalogSource, LayeredModelCatalog, ModelCatalog, ModelLimits, ModelRecord, ProfileField,
    StaticSource,
};
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
        ProviderStreamEvent::RateLimit { .. } => "rate_limit",
        ProviderStreamEvent::Downgrade { .. } => "downgrade",
        ProviderStreamEvent::VendorMetadata { .. } => "vendor",
    }
}

fn cache_observations(events: &[ProviderStreamEvent]) -> Vec<(Option<u64>, Option<u64>)> {
    events
        .iter()
        .filter_map(|event| match event {
            ProviderStreamEvent::CacheObservation {
                read_tokens,
                write_tokens,
            } => Some((*read_tokens, *write_tokens)),
            _ => None,
        })
        .collect()
}

fn assert_cache_observation(
    events: &[ProviderStreamEvent],
    expected: &[(Option<u64>, Option<u64>)],
) {
    assert_eq!(cache_observations(events), expected);
    let cache_index = events
        .iter()
        .position(|event| matches!(event, ProviderStreamEvent::CacheObservation { .. }));
    let finish_index = events
        .iter()
        .position(|event| matches!(event, ProviderStreamEvent::Finish { .. }));
    if expected.is_empty() {
        assert!(cache_index.is_none());
    } else {
        assert!(cache_index < finish_index);
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
            cache_observation(Some(0), None).expect("explicit zero is evidence"),
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
        "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":0}}}\n\n",
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

fn responses_config() -> ResponsesConfig {
    let mut config = ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5");
    config.api_key = Some(Secret::new("fixture-key"));
    config
}

fn responses_two_chunk_text() -> ResponsesProvider<ReplayTransport> {
    ResponsesProvider::new(
        ReplayTransport::single(include_str!("fixtures/responses-text.sse")),
        responses_config(),
    )
    .expect("Responses fixture config")
}

fn anthropic_config() -> AnthropicConfig {
    let mut config = AnthropicConfig::new("https://api.anthropic.com/v1", "claude-x");
    config.api_key = Some(Secret::new("fixture-key"));
    config
}

#[test]
fn responses_model_policy_is_resolved_from_the_host_catalog() {
    let config = responses_config();
    assert_eq!(config.capabilities.max_output_tokens, None);

    let catalog = LayeredModelCatalog::new().with_source(Arc::new(
        StaticSource::new("xai-host", CatalogSource::ProviderLocal)
            .for_provider("responses")
            .with_model(
                "grok-4.5",
                ModelRecord::new()
                    .with_limits(ModelLimits::new(500_000, 499_000, 32_000))
                    .with_capabilities(config.capabilities.clone()),
            ),
    ));
    let profile = catalog
        .resolve("responses", &ModelId::new("grok-4.5"))
        .expect("host catalog metadata resolves");
    assert_eq!(profile.limits.max_output_tokens, 32_000);
    assert_eq!(
        profile
            .provenance_of(ProfileField::MaxOutputTokens)
            .expect("limit provenance")
            .source,
        CatalogSource::ProviderLocal
    );
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
async fn responses_adapter_meets_normalized_contract() {
    let provider = responses_two_chunk_text();
    pc::assert_normalized_text_stream(&provider, &ModelId::new("grok-4.5")).await;
}

#[tokio::test]
async fn cache_presence_and_read_write_separation_conform_across_adapters() {
    let fake = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            usage_event(10, 2),
            cache_observation(Some(0), None).expect("explicit zero is evidence"),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    );
    let fake_events = pc::collect(
        &fake,
        ProviderRequest::new(ModelId::new("fake"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&fake_events, &[(Some(0), None)]);

    let openai = OpenAiProvider::new(
        ReplayTransport::single(concat!(
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":7,\"cache_write_tokens\":3}}}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        )),
        OpenAiConfig::new("http://local/v1", "gpt-x"),
    );
    let openai_events = pc::collect(
        &openai,
        ProviderRequest::new(ModelId::new("gpt-x"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&openai_events, &[(Some(7), Some(3))]);
    let openai_usage = openai_events
        .iter()
        .find_map(|event| match event {
            ProviderStreamEvent::Usage { delta } => Some(delta),
            _ => None,
        })
        .expect("OpenAI usage");
    assert_eq!(
        openai_usage.get(agent_runtime_core::usage::CounterKind::InputCached),
        7
    );
    assert_eq!(
        openai_usage.get(agent_runtime_core::usage::CounterKind::CacheWrite),
        3
    );

    let responses = ResponsesProvider::new(
        ReplayTransport::single(concat!(
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":100,\"input_tokens_details\":{\"cached_tokens\":7,\"cache_write_tokens\":3},\"output_tokens\":2}}}\n\n",
            "data: [DONE]\n\n",
        )),
        responses_config(),
    )
    .expect("Responses config");
    let responses_events = pc::collect(
        &responses,
        ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&responses_events, &[(Some(7), Some(3))]);

    let anthropic = AnthropicProvider::new(
        ReplayTransport::single(concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":5}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )),
        anthropic_config(),
    );
    let anthropic_events = pc::collect(
        &anthropic,
        ProviderRequest::new(ModelId::new("claude-x"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&anthropic_events, &[(Some(0), Some(5))]);

    let gemini = GeminiInteractionsProvider::new(
        ReplayTransport::single(concat!(
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\",\"usage\":{\"total_input_tokens\":10,\"total_output_tokens\":2,\"total_cached_tokens\":0}}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n",
        )),
        gemini_config(),
    )
    .expect("Gemini config");
    let gemini_events = pc::collect(
        &gemini,
        ProviderRequest::new(ModelId::new("gemini-x"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&gemini_events, &[(Some(0), None)]);

    let omitted = OpenAiProvider::new(
        ReplayTransport::single(concat!(
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        )),
        OpenAiConfig::new("http://local/v1", "gpt-x"),
    );
    let omitted_events = pc::collect(
        &omitted,
        ProviderRequest::new(ModelId::new("gpt-x"), vec![Message::user("hi")]),
    )
    .await;
    assert_cache_observation(&omitted_events, &[]);
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
async fn responses_adapter_normalizes_encrypted_reasoning() {
    let provider = ResponsesProvider::new(
        ReplayTransport::single(include_str!("fixtures/responses-reasoning.sse")),
        responses_config(),
    )
    .expect("Responses fixture config");
    pc::assert_normalized_reasoning_stream(
        &provider,
        ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("think")]),
    )
    .await;
}

#[tokio::test]
async fn responses_adapter_preserves_unsigned_signed_and_encrypted_only_reasoning() {
    let provider = ResponsesProvider::new(
        ReplayTransport::single(include_str!("fixtures/responses-reasoning-signatures.sse")),
        responses_config(),
    )
    .expect("Responses fixture config");
    let events = pc::collect(
        &provider,
        ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("think")]),
    )
    .await;

    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::ReasoningDelta {
            text,
            redacted: false,
            signature: None,
        } if text == "plain"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::ReasoningDelta {
            text,
            redacted: false,
            signature: Some(signature),
        } if text.is_empty() && signature == "sig-signed"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::ReasoningDelta {
            text,
            redacted: true,
            signature: Some(signature),
        } if text.is_empty() && signature == "sig-redacted"
    )));
}

#[tokio::test]
async fn responses_adapter_normalizes_parallel_function_calls() {
    let provider = ResponsesProvider::new(
        ReplayTransport::single(include_str!("fixtures/responses-tools.sse")),
        responses_config(),
    )
    .expect("Responses fixture config");
    let mut request = ProviderRequest::new(
        ModelId::new("grok-4.5"),
        vec![Message::user("use both tools")],
    );
    request.tools = vec![
        agent_runtime_core::provider::ToolSchema {
            name: "read".into(),
            description: "read".into(),
            input_schema: serde_json::json!({"type":"object"}),
        },
        agent_runtime_core::provider::ToolSchema {
            name: "write".into(),
            description: "write".into(),
            input_schema: serde_json::json!({"type":"object"}),
        },
    ];
    let events = pc::collect(&provider, request).await;

    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some(id),
            name: Some(name),
            ..
        } if id == "call_1" && name == "read"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::ToolCallDelta {
            index: 1,
            id: Some(id),
            name: Some(name),
            ..
        } if id == "call_2" && name == "write"
    )));
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls
        })
    ));
}

#[tokio::test]
async fn responses_adapter_redacts_auth_failure_details() {
    let provider = ResponsesProvider::new(
        ReplayTransport::single(include_str!("fixtures/responses-auth.sse")),
        responses_config(),
    )
    .expect("Responses fixture config");
    let events = pc::collect(
        &provider,
        ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]),
    )
    .await;

    match events.last() {
        Some(ProviderStreamEvent::Error { error }) => {
            assert_eq!(
                error.kind,
                agent_runtime_core::provider::ProviderErrorKind::Auth
            );
            assert!(!error.message.contains("api-key-canary"));
        }
        other => panic!("expected redacted auth error, got {other:?}"),
    }
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
