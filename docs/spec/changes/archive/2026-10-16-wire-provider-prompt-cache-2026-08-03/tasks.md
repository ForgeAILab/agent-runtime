---
created_at: 2026-08-03T23:40:00Z
updated_at: 2026-08-03T23:40:00Z
completed_at: null
---

## 1. Core

- [x] 1.1 Add `PromptCacheControl` and `Capabilities::prompt_cache`.
- [x] 1.2 Add `ProviderCallContext::session`; update every construction site.

## 2. Mapping

- [x] 2.1 `ProviderCacheCapability::from_control`.

## 3. Adapters

- [x] 3.1 OpenAI Responses: `prompt_cache_key` from session; declare Implicit.
- [x] 3.2 Anthropic: `cache_control` on tools and trailing system; declare Explicit.
- [x] 3.3 Gemini: declare Implicit.

## 4. Tests

- [x] 4.1 Two turns share a key; separate sessions differ.
- [x] 4.2 Anthropic marks tools and system, and omits marks when neither exists.
- [x] 4.3 Capability mapping reports stable support honestly.

## 5. Validation

- [x] 5.1 fmt, clippy warnings-as-errors, workspace tests, and a re-run benchmark.
