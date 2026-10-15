//! Hosted-search history remains portable across provider protocols.

use std::sync::{Arc, Mutex};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::provider::ProviderAttemptPurpose;
use agent_runtime::prelude::*;
use agent_runtime::provider::anthropic::{AnthropicConfig, AnthropicProvider};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime::provider::responses::{ResponsesConfig, ResponsesProvider};
use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpTransport};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

#[derive(Debug, Clone)]
struct ReplayTransport {
    body: String,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl ReplayTransport {
    fn new(body: String) -> Self {
        Self {
            body,
            requests: Arc::default(),
        }
    }

    fn payload(&self) -> Value {
        serde_json::from_slice(&self.requests.lock().unwrap()[0].body).unwrap()
    }
}

#[async_trait]
impl HttpTransport for ReplayTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.requests.lock().unwrap().push(request);
        Ok(Box::pin(futures_util::stream::iter([Ok(self
            .body
            .as_bytes()
            .to_vec())])))
    }
}

fn profile(model: &str) -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fixture",
        ModelId::new(model),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

fn context() -> ProviderCallContext {
    ProviderCallContext {
        session: SessionId::new("s"),
        request_id: RequestId::new("r"),
        attempt_id: AttemptId::new("a"),
        cache_identity: None,
        purpose: ProviderAttemptPurpose::Ordinary,
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
    }
}

#[tokio::test]
async fn anthropic_search_history_serializes_for_responses_and_chat_completions() {
    let frames = [
        json!({"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"hosted-id-canary","name":"web_search","input":{"query":"rust docs"}}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"web_search_tool_result","tool_use_id":"hosted-id-canary","content":[{"type":"web_search_result","url":"https://rust.test","title":"Rust","encrypted_content":"encrypted-canary"}]}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":"Rust works.","citations":[{"type":"web_search_result_location","url":"https://rust.test","title":"Rust","cited_text":"Rust"}]}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3,"server_tool_use":{"web_search_requests":2}}}),
        json!({"type":"message_stop"}),
    ];
    let body = frames
        .iter()
        .map(|frame| format!("data: {frame}\n\n"))
        .collect();
    let provider = AnthropicProvider::new(
        ReplayTransport::new(body),
        AnthropicConfig::new("https://anthropic.test/v1", "claude-test"),
    );
    let runtime = RuntimeBuilder::new(ModelId::new("claude-test"))
        .provider(Arc::new(provider))
        .model_profile(profile("claude-test"))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("search")).await.unwrap();
    let mut searches = 0;
    while let Some(envelope) = events.next().await {
        match envelope.payload {
            RuntimeEvent::HostedSearch {
                action,
                query,
                sources,
            } => {
                assert!(action.is_none());
                searches += 1;
                assert_eq!(query.as_deref(), Some("rust docs"));
                assert_eq!(sources[0].url, "https://rust.test");
            }
            RuntimeEvent::TurnCompleted { finish, .. } => {
                assert_eq!(finish, TurnFinish::Completed);
                break;
            }
            _ => {}
        }
    }
    assert_eq!(searches, 1);
    let snapshot = session.snapshot();
    assert_eq!(
        snapshot.usage.total().get(CounterKind::WebSearchRequests),
        2
    );
    assert_eq!(
        snapshot.history[1].content,
        vec![ContentPart::text("Rust [Rust](https://rust.test) works.")]
    );
    let history: Vec<Message> =
        serde_json::from_value(serde_json::to_value(&snapshot.history).unwrap()).unwrap();
    let responses_transport = ReplayTransport::new(String::new());
    let responses = ResponsesProvider::new(
        responses_transport.clone(),
        ResponsesConfig::new("https://api.openai.com/v1", "responses-test"),
    )
    .unwrap();
    let chat_transport = ReplayTransport::new(String::new());
    let chat = OpenAiProvider::new(
        chat_transport.clone(),
        OpenAiConfig::new("https://glm.test/v1", "glm-test"),
    );
    drop(
        responses
            .stream(
                ProviderRequest::new(ModelId::new("responses-test"), history.clone()),
                context(),
            )
            .await
            .unwrap(),
    );
    drop(
        chat.stream(
            ProviderRequest::new(ModelId::new("glm-test"), history),
            context(),
        )
        .await
        .unwrap(),
    );
    for payload in [
        serde_json::to_value(snapshot.history).unwrap(),
        responses_transport.payload(),
        chat_transport.payload(),
    ] {
        let serialized = serde_json::to_string(&payload).unwrap();
        assert!(serialized.contains("Rust [Rust](https://rust.test) works."));
        for opaque in [
            "server_tool_use",
            "web_search_tool_result",
            "encrypted-canary",
            "hosted-id-canary",
        ] {
            assert!(
                !serialized.contains(opaque),
                "opaque hosted data leaked: {opaque}"
            );
        }
    }
}

#[tokio::test]
async fn driver_counts_search_events_only_when_no_search_counter_was_reported() {
    for reported in [None, Some(0), Some(4)] {
        let mut script = vec![ProviderStreamEvent::HostedSearch {
            action: None,
            query: None,
            sources: Vec::new(),
        }];
        if let Some(count) = reported {
            script.push(ProviderStreamEvent::Usage {
                delta: UsageDelta::new().with(CounterKind::WebSearchRequests, count),
            });
        }
        script.push(ProviderStreamEvent::TextDelta {
            text: "answer".into(),
        });
        script.push(ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        });
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(script)],
        );
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(Arc::new(provider))
            .model_profile(profile("fake"))
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("search")).await.unwrap();
        assert_eq!(
            session
                .snapshot()
                .usage
                .total()
                .get(CounterKind::WebSearchRequests),
            reported.unwrap_or(1)
        );
        assert_eq!(
            session.history()[1].content,
            vec![ContentPart::text("answer")]
        );
    }
}
