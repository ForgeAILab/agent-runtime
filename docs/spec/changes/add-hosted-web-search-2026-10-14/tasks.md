---
created_at: 2026-10-15T01:06:46Z
updated_at: 2026-10-15T01:06:46Z
completed_at:
---

## 1. Hosted web search

- [x] 1.1 Canonical declaration on `ProviderRequest`, capability flag,
      `ProviderStreamEvent` hosted-search event, `RuntimeEvent` forwarding,
      and a usage count.
- [x] 1.2 Responses adapter (OpenAI, xAI): `web_search` tool, `web_search_call`
      items, `url_citation` annotations rendered as links.
- [x] 1.3 Anthropic adapter: `web_search_20250305` server tool,
      `server_tool_use` and `web_search_tool_result`, text-block citations,
      `usage.server_tool_use.web_search_requests`, and the
      organization-disabled error classification.
- [x] 1.4 Gemini adapter: `google_search` tool and `groundingMetadata`.
- [x] 1.5 Chat Completions and other adapters report it unsupported.
- [x] 1.6 Tests: per-adapter serialization and parsing; no opaque block in
      canonical history; cross-provider continuation after hosted search.
- [x] 1.7 Gate: fmt, clippy `-D warnings` with all features, workspace tests.
