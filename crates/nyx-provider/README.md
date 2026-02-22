# nyx-provider

LLM provider abstraction for the Nyx agent runtime. Unified interface to Claude, OpenAI, and 30+ OpenAI-compatible APIs with automatic retries and fallback chains.

## Overview

`nyx-provider` lets the agent talk to any supported LLM through a single `LlmProvider` trait. It handles streaming, tool-call parsing, token usage tracking, retry with exponential backoff, and multi-provider fallback.

## Key Types

| Type | Description |
|---|---|
| `LlmProvider` | Trait: `complete()`, `complete_stream()`, `health()` |
| `CompletionRequest` | Request envelope: messages, tools, temperature, max tokens |
| `CompletionResponse` | Response: content, tool calls, usage metadata |
| `ProviderMessage` | Message with role and content blocks |
| `ProviderContent` | Content variant: text, image, tool call, tool result |
| `ToolDefinition` | Tool schema for LLM function calling |
| `ToolCall` | Parsed tool invocation (name + arguments) |
| `UsageMetadata` | Input/output token counts |
| `ProviderError` | Error types (rate limit, auth, timeout, etc.) |

## Providers

| Provider | Feature | Description |
|---|---|---|
| `ClaudeProvider` | `claude` | Anthropic Claude API |
| `OpenAiProvider` | `openai` | OpenAI API |
| `OpenAiCompatProvider` | `compat` | Any OpenAI-compatible endpoint |
| `RetryProvider` | always | Wraps a provider with exponential backoff |
| `FallbackProvider` | always | Chains providers; falls through on failure |

The `compat` feature covers Ollama, Groq, Mistral, xAI, DeepSeek, Together, Fireworks, and many more.

## Tool-Call Parsers

- `JsonDirectiveParser` - Extracts JSON tool calls from model output.
- `XmlDirectiveParser` - Extracts XML-formatted tool calls.

## Configuration

```rust
use nyx_provider::{build_provider_chain, ProvidersConfig};

let config: ProvidersConfig = load_config();
let provider = build_provider_chain(&config)?;

let response = provider.complete(request).await?;
```

## Feature Flags

| Feature | Default | Description |
|---|---|---|
| `claude` | no | Anthropic Claude provider |
| `openai` | no | OpenAI provider |
| `compat` | yes | OpenAI-compatible providers |

## Dependencies

Core: `async-trait`, `reqwest`, `tokio`, `tokio-stream`, `serde`, `serde_json`, `thiserror`, `nyx-security`
