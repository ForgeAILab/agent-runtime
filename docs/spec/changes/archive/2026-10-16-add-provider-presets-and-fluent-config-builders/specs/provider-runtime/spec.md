# provider-runtime Specification Delta

## ADDED Requirements

### Requirement: Well-known provider presets and fluent config builders

`agent-runtime-provider` SHALL provide well-known provider presets and fluent config builders on provider configuration structs (`OpenAiConfig`, `AnthropicConfig`, `ResponsesConfig`, and `GeminiInteractionsConfig`).

The configuration structs MUST support fluent chaining via:
- `with_api_key`
- `with_extra_header`
- `with_capabilities`
- `with_prompt_cache`

`OpenAiConfig` SHALL offer presets for OpenAI, OpenRouter, Groq, DeepSeek, DeepInfra, Together AI, Fireworks AI, Cerebras, Perplexity, Mistral, Baseten, Nvidia NIM, Kilo AI, ZenMux, LLM Gateway, Cloudflare AI Gateway, Cloudflare Workers AI, and Azure OpenAI, correctly setting base URLs and required default headers.

`AnthropicConfig`, `ResponsesConfig`, and `GeminiInteractionsConfig` SHALL offer presets for Anthropic, xAI, and Google AI Studio respectively.

#### Scenario: OpenRouter preset configures custom headers and base URL

- **GIVEN** a host creates an `OpenAiConfig::openrouter("anthropic/claude-3.7-sonnet")`
- **WHEN** the config is inspected
- **THEN** the base URL is `"https://openrouter.ai/api/v1"`
- **AND** the headers include `"HTTP-Referer"` and `"X-Title"`

#### Scenario: Fluent config builders chain parameters

- **GIVEN** a host builds a provider config using fluent methods `.with_api_key(...)` and `.with_extra_header(...)`
- **WHEN** the config is evaluated
- **THEN** the secret key and extra headers are set on the configuration struct
