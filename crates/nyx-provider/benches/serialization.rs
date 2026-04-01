use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use nyx_provider::{
    JsonDirectiveParser, ProviderContent, ProviderMessage, ProviderRole, ToolDefinition,
    XmlDirectiveParser,
};

const OPENAI_IMAGE_DETAIL: Option<&str> = Some("high");

#[derive(Serialize)]
#[serde(tag = "role", rename_all = "lowercase")]
enum OpenAiRequestMessage {
    System {
        content: OpenAiRequestContent,
    },
    User {
        content: OpenAiRequestContent,
    },
    Assistant {
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<OpenAiToolCallRequest>,
    },
    #[serde(rename = "tool")]
    ToolResult {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
enum OpenAiRequestContent {
    Text(String),
    Blocks(Vec<OpenAiContentBlock>),
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpenAiContentBlock {
    Text {
        text: String,
    },
    ImageUrl {
        image_url: OpenAiImageUrl,
    },
}

#[derive(Serialize)]
struct OpenAiImageUrl {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Serialize)]
struct OpenAiFunctionTool {
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiFunctionDefinition,
}

#[derive(Serialize)]
struct OpenAiFunctionDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Serialize)]
struct OpenAiToolCallRequest {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiToolCallFunction,
}

#[derive(Serialize)]
struct OpenAiToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct OpenAiCompletionPayload {
    model: String,
    messages: Vec<OpenAiRequestMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<OpenAiFunctionTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_tokens: Option<u32>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiCompletionResponse {
    model: String,
    choices: Vec<OpenAiChoice>,
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiResponseMessage {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAiResponseToolCall>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiResponseToolCall {
    id: String,
    function: OpenAiResponseFunction,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiResponseFunction {
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
}

fn make_request(messages: usize, tool_count: usize) -> nyx_provider::CompletionRequest {
    let mut body = Vec::with_capacity(messages);
    for index in 0..messages {
        let role = match index % 4 {
            0 => ProviderRole::System,
            1 => ProviderRole::User,
            2 => ProviderRole::Assistant,
            _ => ProviderRole::Tool,
        };
        let content = match role {
            ProviderRole::System => {
                vec![ProviderContent::text(format!("system setup {index}")), ProviderContent::text("context")]
            }
            ProviderRole::User => vec![
                ProviderContent::text(format!("user prompt {index}")),
                ProviderContent::Image {
                    url: format!("https://example.com/{index}.png"),
                    detail: OPENAI_IMAGE_DETAIL.map(str::to_string),
                },
            ],
            ProviderRole::Assistant => vec![ProviderContent::text(format!("assistant text {index}"))],
            ProviderRole::Tool => {
                vec![ProviderContent::text(format!("tool response {index}"))]
            }
        };
        body.push(nyx_provider::ProviderMessage {
            role,
            content,
            tool_call_id: if role == ProviderRole::Tool {
                Some(format!("tool-result-{index}"))
            } else {
                None
            },
        });
    }

    let tools = (0..tool_count)
        .map(|index| ToolDefinition {
            name: format!("search_{index}"),
            description: format!("Tool {index} for benchmarking request payloads"),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "q": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["q"]
            }),
        })
        .collect();

    nyx_provider::CompletionRequest {
        model: "bench-model".to_string(),
        messages: body,
        tools,
        max_tokens: Some(1024),
        temperature: Some(0.2),
        thinking_tokens: None,
    }
}

fn transform_messages(messages: &[ProviderMessage]) -> Vec<OpenAiRequestMessage> {
    messages
        .iter()
        .map(|message| {
            match message.role {
                ProviderRole::System => OpenAiRequestMessage::System {
                    content: provider_content_to_openai(message.content.clone()),
                },
                ProviderRole::User => OpenAiRequestMessage::User {
                    content: provider_content_to_openai(message.content.clone()),
                },
                ProviderRole::Assistant => {
                    let (content, tool_calls) = decode_assistant_blocks(&message.content);
                    OpenAiRequestMessage::Assistant {
                        content: if content.is_empty() { None } else { Some(content) },
                        tool_calls,
                    }
                }
                ProviderRole::Tool => {
                    if let Some(tool_call_id) = &message.tool_call_id {
                        OpenAiRequestMessage::ToolResult {
                            tool_call_id: tool_call_id.clone(),
                            content: concat_text(&message.content),
                        }
                    } else {
                        OpenAiRequestMessage::User {
                            content: OpenAiRequestContent::Text(format!(
                                "[Tool Result]\n{}",
                                concat_text(&message.content)
                            )),
                        }
                    }
                }
            }
        })
        .collect()
}

fn build_completion_payload(req: &nyx_provider::CompletionRequest) -> OpenAiCompletionPayload {
    OpenAiCompletionPayload {
        model: req.model.clone(),
        messages: transform_messages(&req.messages),
        tools: req
            .tools
            .iter()
            .map(|tool| OpenAiFunctionTool {
                tool_type: "function".to_string(),
                function: OpenAiFunctionDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.input_schema.clone(),
                },
            })
            .collect(),
        max_tokens: req.max_tokens,
        temperature: req.temperature,
        thinking_tokens: req.thinking_tokens,
    }
}

fn benchmark_tool_definitions() -> Vec<String> {
    vec![
        r#"{"tool":"search","input":{"query":"weather","limit":5}}"#.to_string(),
        r#"<tool_call name="search"><payload>{"query":"weather","limit":5}</payload></tool_call>"#
            .to_string(),
        r#"{"tool":"summarize","input":{"text":"quick update","format":"short"}}"#.to_string(),
        r#"<tool_call name="summarize"><payload>{"text":"quick update","format":"short"}</payload></tool_call>"#
            .to_string(),
    ]
}

fn decode_assistant_blocks(content: &[ProviderContent]) -> (Option<String>, Vec<OpenAiToolCallRequest>) {
    let raw = concat_text(content);
    let Ok(blocks) = serde_json::from_str::<Vec<Value>>(&raw) else {
        return (Some(raw), vec![]);
    };
    if !blocks.iter().all(|block| block.get("type").is_some()) {
        return (Some(raw), vec![]);
    }

    let mut text = None;
    let mut tool_calls = Vec::new();
    for block in blocks {
        if let Some(block_type) = block.get("type").and_then(Value::as_str) {
            match block_type {
                "text" => {
                    if let Some(block_text) = block.get("text").and_then(Value::as_str) {
                        text = Some(block_text.to_string());
                    }
                }
                "tool_use" => {
                    let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                    let name = block.get("name").and_then(Value::as_str).unwrap_or_default();
                    let input = serde_json::to_string(block.get("input").unwrap_or(&Value::Null))
                        .unwrap_or_else(|_| "{}".to_string());
                    tool_calls.push(OpenAiToolCallRequest {
                        id: id.to_string(),
                        kind: "function".to_string(),
                        function: OpenAiToolCallFunction {
                            name: name.to_string(),
                            arguments: input,
                        },
                    });
                }
                _ => {}
            }
        }
    }
    (text, tool_calls)
}

fn provider_content_to_openai(content: Vec<ProviderContent>) -> OpenAiRequestContent {
    let mut blocks = Vec::new();
    for block in content {
        match block {
            ProviderContent::Text { text } => {
                if !text.is_empty() {
                    blocks.push(OpenAiContentBlock::Text { text });
                }
            }
            ProviderContent::Image { url, detail } => {
                blocks.push(OpenAiContentBlock::ImageUrl {
                    image_url: OpenAiImageUrl { url, detail },
                });
            }
        }
    }

    if blocks.len() == 1 {
        match &blocks[0] {
            OpenAiContentBlock::Text { text } => OpenAiRequestContent::Text(text.clone()),
            OpenAiContentBlock::ImageUrl { .. } => OpenAiRequestContent::Blocks(blocks),
        }
    } else {
        OpenAiRequestContent::Blocks(blocks)
    }
}

fn concat_text(content: &[ProviderContent]) -> String {
    content.iter().filter_map(|block| match block {
        ProviderContent::Text { text } => Some(text.as_str()),
        ProviderContent::Image { .. } => None,
    }).collect()
}

fn response_payload(message_count: usize) -> String {
    let mut choices = Vec::with_capacity(message_count);
    for index in 0..message_count {
        choices.push(json!({
            "index": index,
            "message": {
                "content": format!("answer #{index}"),
                "tool_calls": []
            },
            "finish_reason": "stop"
        }));
    }

    let body = json!({
        "id": "cmpl-bench-01",
        "object": "chat.completion",
        "model": "bench-model",
        "choices": choices,
        "usage": {
            "prompt_tokens": message_count as u32 * 8,
            "completion_tokens": message_count as u32 * 2
        }
    });
    body.to_string()
}

fn benchmark_request_serialization(c: &mut Criterion) {
    let small = make_request(5, 10);
    let large = make_request(100, 50);
    let mut group = c.benchmark_group("nyx_provider_completion_request");
    group.throughput(Throughput::Elements(1));

    group.bench_function("serialize_small", |b| {
        b.iter(|| {
            let payload = build_completion_payload(&small);
            black_box(serde_json::to_vec(&payload).expect("serialize small request"));
        })
    });

    group.bench_function("serialize_large", |b| {
        b.iter(|| {
            let payload = build_completion_payload(black_box(&large));
            black_box(serde_json::to_vec(&payload).expect("serialize large request"));
        })
    });
    group.finish();
}

fn benchmark_message_transformation(c: &mut Criterion) {
    let small = make_request(5, 10);
    let large = make_request(100, 50);
    let mut group = c.benchmark_group("nyx_provider_message_transformation");
    group.bench_function("small", |b| {
        b.iter(|| {
            black_box(transform_messages(black_box(&small.messages)));
        })
    });
    group.bench_with_input(
        BenchmarkId::new("large", "messages"),
        &large.messages,
        |b, messages| {
            b.iter(|| black_box(transform_messages(black_box(messages))));
        },
    );
    group.finish();
}

fn benchmark_tool_definition_collection(c: &mut Criterion) {
    let small = make_request(5, 10);
    let large = make_request(5, 50);

    c.bench_function("nyx_provider_tool_definition_collection/small_10", |b| {
        b.iter(|| {
            let mut total = 0usize;
            let tools = small
                .tools
                .iter()
                .map(|tool| {
                    OpenAiFunctionTool {
                        tool_type: "function".to_string(),
                        function: OpenAiFunctionDefinition {
                            name: tool.name.clone(),
                            description: tool.description.clone(),
                            parameters: tool.input_schema.clone(),
                        },
                    }
                })
                .collect::<Vec<_>>();
            for item in tools {
                total = total.saturating_add(serde_json::to_vec(&item).expect("serialize tool").len());
            }
            black_box(total);
        });
    });

    c.bench_function("nyx_provider_tool_definition_collection/large_50", |b| {
        b.iter(|| {
            let mut total = 0usize;
            let tools = large
                .tools
                .iter()
                .map(|tool| {
                    OpenAiFunctionTool {
                        tool_type: "function".to_string(),
                        function: OpenAiFunctionDefinition {
                            name: tool.name.clone(),
                            description: tool.description.clone(),
                            parameters: tool.input_schema.clone(),
                        },
                    }
                })
                .collect::<Vec<_>>();
            for item in tools {
                total = total.saturating_add(serde_json::to_vec(&item).expect("serialize tool").len());
            }
            black_box(total);
        });
    });
}

fn benchmark_response_deserialization(c: &mut Criterion) {
    let response = response_payload(100);
    c.bench_function("nyx_provider_response_deserialization", |b| {
        b.iter(|| {
            let parsed = serde_json::from_str::<OpenAiCompletionResponse>(black_box(&response))
                .expect("parse response");
            black_box(parsed);
        });
    });
}

fn benchmark_tool_call_parsing(c: &mut Criterion) {
    let fixtures = benchmark_tool_definitions();
    let mut group = c.benchmark_group("nyx_provider_tool_call_parsing");
    group.bench_with_input(BenchmarkId::new("json", "tool_call"), &fixtures[0], |b, payload| {
        let parser = JsonDirectiveParser;
        b.iter(|| {
            let calls = parser.parse(black_box(payload));
            black_box(calls.len());
        });
    });
    group.bench_with_input(BenchmarkId::new("xml", "tool_call"), &fixtures[1], |b, payload| {
        let parser = XmlDirectiveParser;
        b.iter(|| {
            let calls = parser.parse(black_box(payload));
            black_box(calls.len());
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_request_serialization,
    benchmark_message_transformation,
    benchmark_tool_definition_collection,
    benchmark_tool_call_parsing,
    benchmark_response_deserialization
);
criterion_main!(benches);
