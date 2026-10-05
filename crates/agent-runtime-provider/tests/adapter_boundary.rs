use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{ContentPart, Message, ToolCall, ToolResultBlock};
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId, ToolCallId};
use agent_runtime_core::provider::{
    ModelId, Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderError,
    ProviderErrorKind, ProviderRequest, ProviderStreamEvent, ToolChoice, ToolSchema,
};
use agent_runtime_provider::anthropic::{AnthropicConfig, AnthropicProvider};
use agent_runtime_provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime_provider::responses::{ResponsesConfig, ResponsesProvider};
use agent_runtime_provider::transport::{ByteStream, HttpRequest, HttpTransport};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
struct Capture {
    body: String,
    error: Option<ProviderError>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

#[async_trait]
impl HttpTransport for Capture {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.requests.lock().unwrap().push(request);
        let chunk = self
            .error
            .clone()
            .map_or_else(|| Ok(self.body.as_bytes().to_vec()), Err);
        Ok(Box::pin(stream::iter([chunk])))
    }
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

fn adapter(kind: &str, capture: Capture) -> Box<dyn Provider> {
    match kind {
        "anthropic" => Box::new(AnthropicProvider::new(
            capture,
            AnthropicConfig::new("https://example.test/v1", "model"),
        )),
        "openai" => Box::new(OpenAiProvider::new(
            capture,
            OpenAiConfig::new("https://example.test/v1", "model"),
        )),
        "responses" => Box::new(
            ResponsesProvider::new(
                capture,
                ResponsesConfig::new("https://example.test/v1", "model"),
            )
            .unwrap(),
        ),
        _ => unreachable!(),
    }
}

async fn exchange(
    kind: &str,
    request: ProviderRequest,
    body: String,
) -> (Value, Vec<ProviderStreamEvent>) {
    let capture = Capture {
        body,
        error: None,
        requests: Arc::default(),
    };
    let provider = adapter(kind, capture.clone());
    let events = provider
        .stream(request, context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ProviderStreamEvent::Error { .. })),
        "{kind}: {events:?}"
    );
    let payload = serde_json::from_slice(&capture.requests.lock().unwrap()[0].body).unwrap();
    (payload, events)
}

fn tool_request(names: &[String]) -> ProviderRequest {
    let mut request = ProviderRequest::new(ModelId::new("model"), vec![Message::user("hi")]);
    request.tools = names
        .iter()
        .map(|name| ToolSchema {
            name: name.clone(),
            description: "tool".into(),
            input_schema: json!({"type":"object"}),
        })
        .collect();
    request
}

fn wire_names(kind: &str, payload: &Value) -> Vec<String> {
    payload["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| {
            (if kind == "openai" {
                &tool["function"]["name"]
            } else {
                &tool["name"]
            })
            .as_str()
            .unwrap()
            .to_owned()
        })
        .collect()
}

fn model_call(kind: &str, wire: &str) -> String {
    let frames = match kind {
        "anthropic" => vec![
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"c1","name":wire,"input":{}}}),
            json!({"type":"message_stop"}),
        ],
        "openai" => vec![
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":wire,"arguments":"{}"}}]},"finish_reason":"tool_calls"}]}),
        ],
        "responses" => vec![
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"item1","call_id":"c1","name":wire,"arguments":"{}"}}),
            json!({"type":"response.completed","response":{"output":[],"usage":{}}}),
        ],
        _ => unreachable!(),
    };
    frames
        .iter()
        .map(|value| format!("data: {value}\n\n"))
        .collect()
}

async fn dotted_and_overlong_names_round_trip_across_adapter_boundaries(kind: &str) {
    for name in [
        "memory.search".to_owned(),
        format!("mcp__server__{}", "tool".repeat(40)),
    ] {
        let mut request = tool_request(std::slice::from_ref(&name));
        request.tool_choice = ToolChoice::Named(name.clone());
        let (payload, _) = exchange(kind, request.clone(), model_call(kind, "unknown_tool")).await;
        let wire = wire_names(kind, &payload).remove(0);
        assert!(
            wire.len() <= 64
                && wire
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "{kind}: {wire}"
        );
        let choice = if kind == "openai" {
            &payload["tool_choice"]["function"]["name"]
        } else {
            &payload["tool_choice"]["name"]
        };
        assert_eq!(choice, &wire);
        let (_, events) = exchange(kind, request.clone(), model_call(kind, &wire)).await;
        assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::ToolCallDelta { name: Some(n), .. } if n == &name)), "{kind}: {events:?}");
        request
            .messages
            .push(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("c1"),
                name: name.clone(),
                arguments: json!({}),
            })]));
        request.messages.push(Message::tool_result(ToolResultBlock {
            call_id: ToolCallId::new("c1"),
            name: name.clone(),
            content: vec![ContentPart::text("ok")],
            is_error: false,
        }));
        let (replay, _) = exchange(kind, request.clone(), model_call(kind, "unknown_tool")).await;
        assert_eq!(wire_names(kind, &replay)[0], wire);
        let replay_name = match kind {
            "anthropic" => &replay["messages"][1]["content"][0]["name"],
            "openai" => &replay["messages"][1]["tool_calls"][0]["function"]["name"],
            _ => &replay["input"][1]["name"],
        };
        assert_eq!(replay_name, &wire);
        assert_eq!(request.tools[0].name, name);
    }
}

#[tokio::test]
async fn anthropic_dotted_and_overlong_names_round_trip_across_adapter_boundaries() {
    dotted_and_overlong_names_round_trip_across_adapter_boundaries("anthropic").await;
}

#[tokio::test]
async fn openai_dotted_and_overlong_names_round_trip_across_adapter_boundaries() {
    dotted_and_overlong_names_round_trip_across_adapter_boundaries("openai").await;
}

#[tokio::test]
async fn responses_dotted_and_overlong_names_round_trip_across_adapter_boundaries() {
    dotted_and_overlong_names_round_trip_across_adapter_boundaries("responses").await;
}

async fn collision_disambiguation_is_deterministic_and_valid_names_are_unchanged(kind: &str) {
    let names = vec![
        "memory.search".into(),
        "memory/search".into(),
        "memory_search".into(),
        "valid-Tool_9".into(),
        "x".repeat(64),
        "x".repeat(150),
        format!("{}y", "x".repeat(149)),
    ];
    let (payload, _) = exchange(kind, tool_request(&names), model_call(kind, "unknown_tool")).await;
    let wire = wire_names(kind, &payload);
    assert_eq!(wire.iter().collect::<BTreeSet<_>>().len(), names.len());
    assert!(wire.iter().all(|name| {
        name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }));
    for i in [2, 3, 4] {
        assert_eq!(wire[i], names[i]);
    }
    let reversed: Vec<_> = names.iter().rev().cloned().collect();
    let (payload, _) = exchange(
        kind,
        tool_request(&reversed),
        model_call(kind, "unknown_tool"),
    )
    .await;
    assert_eq!(
        wire_names(kind, &payload),
        wire.into_iter().rev().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn anthropic_collision_disambiguation_is_deterministic_and_valid_names_are_unchanged() {
    collision_disambiguation_is_deterministic_and_valid_names_are_unchanged("anthropic").await;
}

#[tokio::test]
async fn openai_collision_disambiguation_is_deterministic_and_valid_names_are_unchanged() {
    collision_disambiguation_is_deterministic_and_valid_names_are_unchanged("openai").await;
}

#[tokio::test]
async fn responses_collision_disambiguation_is_deterministic_and_valid_names_are_unchanged() {
    collision_disambiguation_is_deterministic_and_valid_names_are_unchanged("responses").await;
}

async fn unknown_wire_name_passes_through_unchanged(kind: &str) {
    let (_, events) = exchange(
        kind,
        tool_request(&["memory.search".into()]),
        model_call(kind, "unknown_tool"),
    )
    .await;
    assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::ToolCallDelta { name: Some(n), .. } if n == "unknown_tool")));
}

#[tokio::test]
async fn anthropic_unknown_wire_name_passes_through_unchanged() {
    unknown_wire_name_passes_through_unchanged("anthropic").await;
}

#[tokio::test]
async fn openai_unknown_wire_name_passes_through_unchanged() {
    unknown_wire_name_passes_through_unchanged("openai").await;
}

#[tokio::test]
async fn responses_unknown_wire_name_passes_through_unchanged() {
    unknown_wire_name_passes_through_unchanged("responses").await;
}

async fn echoed_credential_in_transport_stream_error_is_absent_from_all_renderings(kind: &str) {
    let mut error = ProviderError::new(
        ProviderErrorKind::Server,
        "sk-echoed-credential private prompt",
    )
    .retry_after(123);
    error
        .metadata
        .insert("provider.detail", "sk-echoed-credential");
    let capture = Capture {
        body: String::new(),
        error: Some(error),
        requests: Arc::default(),
    };
    let events = adapter(kind, capture)
        .stream(tool_request(&[]), context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    let ProviderStreamEvent::Error { error } = &events[0] else {
        panic!("error expected")
    };
    for rendering in [
        error.to_string(),
        format!("{error:?}"),
        format!("{events:?}"),
        serde_json::to_string(&events).unwrap(),
    ] {
        assert!(
            !rendering.contains("sk-echoed-credential"),
            "{kind}: {rendering}"
        );
    }
    assert_eq!(error.retry_after_ms, Some(123));
}

#[tokio::test]
async fn anthropic_echoed_credential_in_transport_stream_error_is_absent_from_all_renderings() {
    echoed_credential_in_transport_stream_error_is_absent_from_all_renderings("anthropic").await;
}

#[tokio::test]
async fn openai_echoed_credential_in_transport_stream_error_is_absent_from_all_renderings() {
    echoed_credential_in_transport_stream_error_is_absent_from_all_renderings("openai").await;
}

#[tokio::test]
async fn responses_echoed_credential_in_transport_stream_error_is_absent_from_all_renderings() {
    echoed_credential_in_transport_stream_error_is_absent_from_all_renderings("responses").await;
}

#[tokio::test]
async fn openai_fragmented_wire_names_emit_only_complete_canonical_names() {
    let request = tool_request(&["memory.search".into()]);
    let (payload, _) = exchange(
        "openai",
        request.clone(),
        model_call("openai", "unknown_tool"),
    )
    .await;
    let wire = wire_names("openai", &payload).remove(0);
    let frames = [
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":&wire[..5],"arguments":"{"}}]}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":&wire[5..],"arguments":"}"}}]},"finish_reason":"tool_calls"}]}),
    ];
    let body: String = frames
        .iter()
        .map(|frame| format!("data: {frame}\n\n"))
        .collect();
    let (_, events) = exchange("openai", request, body).await;
    let names: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ProviderStreamEvent::ToolCallDelta {
                name: Some(name), ..
            } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["memory.search"]);
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Finish { .. })
    ));
}

#[tokio::test]
async fn openai_repeated_complete_wire_name_still_maps_to_one_canonical_name() {
    let request = tool_request(&["memory.search".into()]);
    let (payload, _) = exchange(
        "openai",
        request.clone(),
        model_call("openai", "unknown_tool"),
    )
    .await;
    let wire = wire_names("openai", &payload).remove(0);
    let frames = [
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":wire,"arguments":"{"}}]}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":wire,"arguments":"}"}}]},"finish_reason":"tool_calls"}]}),
    ];
    let body: String = frames
        .iter()
        .map(|frame| format!("data: {frame}\n\n"))
        .collect();
    let (_, events) = exchange("openai", request, body).await;
    let names: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ProviderStreamEvent::ToolCallDelta {
                name: Some(name), ..
            } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["memory.search"]);
}
