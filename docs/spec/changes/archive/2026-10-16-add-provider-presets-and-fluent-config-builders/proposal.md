# Proposal: Add Provider Presets and Fluent Config Builders

## Why

In `@opencode`, provider configurations support a broad array of well-known AI providers (OpenAI, Anthropic, Google, xAI, OpenRouter, Groq, DeepSeek, DeepInfra, Together AI, Cerebras, Fireworks, Perplexity, Mistral, Baseten, Nvidia NIM, Kilo, ZenMux, LLM Gateway, Cloudflare, Azure, etc.).

Currently, `agent-runtime-provider` requires hosts to manually specify base URLs, custom headers, and capability details for each provider when constructing `OpenAiConfig`, `AnthropicConfig`, `ResponsesConfig`, or `GeminiInteractionsConfig`. Furthermore, builder methods for attaching API keys, capabilities, prompt caching, or custom headers fluently do not exist on these config structs.

Adding a rich collection of well-known provider presets and ergonomic fluent configuration builder methods will make `agent-runtime-provider` comprehensive, easier to configure, and fully aligned with provider options learned from `@opencode`.

## What

1. **Fluent Builder Methods**:
   - `OpenAiConfig`, `AnthropicConfig`, `ResponsesConfig`, and `GeminiInteractionsConfig` receive fluent builder methods: `.with_api_key(...)`, `.with_extra_header(...)`, `.with_capabilities(...)`, `.with_prompt_cache(...)`.
   - `AnthropicConfig` receives `.with_interleaved_thinking()`.

2. **Well-Known Provider Presets on `OpenAiConfig`**:
   - `openai(model)`
   - `openrouter(model)`
   - `groq(model)`
   - `deepseek(model)`
   - `deepinfra(model)`
   - `togetherai(model)`
   - `fireworks(model)`
   - `cerebras(model)`
   - `perplexity(model)`
   - `mistral(model)`
   - `baseten(model)`
   - `nvidia(model)`
   - `kilo(model)`
   - `zenmux(model)`
   - `llmgateway(model)`
   - `cloudflare_ai_gateway(account_id, gateway_id, model)`
   - `cloudflare_workers_ai(account_id, model)`
   - `azure(resource_name, model)`

3. **Well-Known Provider Presets for Native Adapters**:
   - `AnthropicConfig::anthropic(model)`
   - `ResponsesConfig::xai(model)`
   - `GeminiInteractionsConfig::google(model)`

## Impact

- Non-breaking addition of constructors and builder methods to `agent-runtime-provider`.
- Enables straightforward setup of 18+ provider presets learned from `@opencode`.
- All tests and MSRV remain 100% compliant.
