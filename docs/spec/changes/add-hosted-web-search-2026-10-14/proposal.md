---
created_at: 2026-10-15T01:06:46Z
updated_at: 2026-10-15T01:06:46Z
---

## Why

Smith's `add-web-tools-2026-10-14` uses the model provider's own web search
where it exists. Today every adapter rejects or drops hosted-tool output:
Responses returns `Unsupported` for `web_search_call`, Anthropic does not
parse `server_tool_use` or `web_search_tool_result`, Gemini ignores grounding
metadata, and no adapter keeps citations.

## What Changes

- **A canonical hosted web search declaration** on `ProviderRequest`
  (optional per-response cap) and a capability flag, so a host can tell before
  sending whether an adapter supports it.
- **Adapters map it:**

  | Adapter | Hosted tool |
  |---|---|
  | Responses, including xAI | `web_search` |
  | Anthropic | `web_search_20250305` |
  | Gemini | `google_search` |
  | Chat Completions | reports it unsupported |

- **Normalized output.** A hosted-search stream event and a runtime event
  carry the query and the sources found. Citations are rendered into
  assistant text as Markdown links. Usage records the search count.
  Provider-opaque blocks never enter canonical history, so continuing a
  session on another provider is safe.
- **Classified rejection.** An Anthropic organization without web search
  produces a classified error that a host can fall back on.

## Impact

- Affected specs: `provider-runtime`
- Affected code: `agent-runtime-core` (`provider.rs` request, capabilities,
  stream events; `event.rs`), `agent-runtime-provider` (`responses.rs`,
  `anthropic.rs`, `gemini.rs`, `openai.rs` capability), `agent-runtime`
  driver (forward the events and usage), and `agent-runtime-obs` render.
- Compatibility: additive; new serde fields default. No checkpoint revision
  change.
