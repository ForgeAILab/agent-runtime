---
created_at: 2026-08-04T00:31:01Z
updated_at: 2026-08-04T00:31:01Z
---

## Context

Agent Runtime's provider boundary is transport-injected and host-neutral.
`ProviderRequest` already carries ordered canonical messages, tools, reasoning,
structured output, output limits, and bounded vendor extensions. Provider
streams already expose text, signed reasoning, fragmented tool calls, usage,
cache observations, finish state, and classified errors.

The OpenAI Responses API represents one request as ordered input items and one
response as ordered output items, streamed as typed SSE events
(`response.output_text.delta`, `response.reasoning_text.delta`,
`response.function_call_arguments.delta`, `response.output_item.added`, …)
ending in exactly one of `response.completed`, `response.incomplete`, or
`response.failed`. In stateless mode (`store=false`, no
`previous_response_id`), later requests resend the complete history, and
reasoning context returns only if the client requested
`include: ["reasoning.encrypted_content"]` and replays the encrypted items.

Upstream evidence (xai-org/grok-build @ `e5478eff`): `grok-4.5` defaults to
the Responses backend with a 500k context window and low/medium/high
reasoning effort; the sampler forces `store: false`, always includes
encrypted reasoning content, treats a stream without a terminal as failed,
and forwards `prompt_cache_key` only on the Responses backend. Its Messages
dialect is already reachable through the existing Anthropic adapter, and its
Chat Completions dialect through the existing OpenAI adapter, by base-url
configuration.

## Goals / Non-Goals

### Goals

- Add a native, reusable, offline-testable Responses adapter whose first
  fixture-verified deployment is xAI serving Grok.
- Preserve canonical runtime ownership of attempts, tools, history, and retry.
- Use stateless requests so consumers do not depend on provider retention.
- Replay encrypted reasoning items verbatim, reusing the signed
  `ContentPart::Reasoning` contract.
- Normalize Responses stream events and terminals without provider-specific
  public event variants.
- Conform to the session-keyed implicit prompt-cache contract.

### Non-Goals

- Grok Build's product surface: OIDC/session auth against
  `cli-chat-proxy.grok.com`, prompts, hosted search/X-search/code execution,
  compaction thresholds, telemetry headers, TUI, agent orchestration.
- OpenAI-served Responses conformance (hosted tools, service tiers,
  ChatGPT-specific auth). Base-url configurability keeps the door open; the
  fixtures of this change target the xAI deployment only.
- Provider-side conversation state (`store=true`, `previous_response_id`),
  background responses, or any hidden tool loop.
- An xAI/OpenAI SDK dependency; the adapter stays on the injected
  `HttpTransport` and shared SSE normalization.

## Decisions

### One protocol-named adapter, not a per-vendor fork

New module `responses.rs` with `ResponsesProvider<T: HttpTransport>` and
`ResponsesConfig { base_url, model, capabilities, api_key, extra_headers }`,
mirroring `OpenAiConfig`. The protocol is OpenAI's and is served by more than
one vendor, so the module is named for the wire protocol (like `sse`), not a
vendor. xAI-specific product headers (`x-grok-*`) are not adapter behavior; a
host that wants them uses `extra_headers`.

### Always operate statelessly

Serialize `store: false` on every request and reject vendor extensions that
request storage, `previous_response_id`, background execution, or hosted
tools before credential acquisition or network I/O — the same refusal shape
the Gemini adapter uses for Interactions storage. Canonical local history is
the only conversation state.

### Encrypted reasoning rides the existing signature contract

Request `include: ["reasoning.encrypted_content"]` unconditionally. A
reasoning output item maps to canonical reasoning content: streamed
summary/text deltas become visible reasoning text; `encrypted_content`
becomes the opaque signature; an item with encrypted content and no text
becomes a signature-bearing redacted block (the Gemini signature-only path,
already required to survive assembly, persistence, and replay). On
continuation, preserved reasoning items are replayed verbatim in their
original order relative to function calls; per the existing contract a
signature is dropped whenever its text is altered, and the adapter never
invents or reorders encrypted items. Unlike Gemini, a missing encrypted item
does not hard-fail the request: the Responses API accepts degraded
continuations, so the adapter sends what was preserved.

### Terminal mapping is total and single-shot

- `response.completed` → committed output with the natural finish state
  (tool-call finish when function calls are present).
- `response.incomplete` → committed output whose finish state carries the
  `incomplete_details` truncation reason (e.g. max output tokens); never
  silent success.
- `response.failed` → structured provider error with redaction-safe detail.
- Stream end without a terminal, or a second conflicting terminal →
  malformed-stream error; nothing after the first terminal is committed.

### Fragmented function calls assemble under provider call IDs

`response.output_item.added` supplies call identity (`call_id`, name);
`response.function_call_arguments.delta` supplies indexed fragments; the
existing normalized-event contract assembles and validates one typed tool
call and preserves the provider call ID for the function result item on
continuation.

### Capabilities and cache declaration

Per-model `Capabilities`: tools, reasoning (named efforts `low`/`medium`/
`high`; reasoning cannot be disabled on `grok-4.5`, so a no-reasoning request
is served with provider defaults rather than failed), structured output via
`text.format` JSON schema, usage with cached and reasoning token detail,
`cache: true`, `prompt_cache: Implicit`, `auth: ApiKey` (bearer),
`continuation: false`. The adapter sends a session-derived `prompt_cache_key`
exactly as `wire-provider-prompt-cache-2026-08-03` specifies; this change
adds no cache requirement of its own.

### Model policy stays outside the adapter

Grok Build embeds `default_models.json` (context window, default efforts,
compaction thresholds) as product configuration. The runtime instead resolves
`grok-4.5` limits and efforts through the layered model catalog and explicit
host overrides; conservative unknown-model handling already refuses to guess
a context window for an unregistered Grok variant.

## Risks / Trade-offs

- The Responses event vocabulary is larger than what the adapter consumes.
  Unknown *semantic* structures fail deterministically; unknown *additive*
  event types that carry no committed content are skipped, bounded by the
  malformed-stream rules, to avoid breaking on additive upstream changes.
- xAI's serving of the protocol may drift from OpenAI's. Fixtures are
  recorded against the xAI deployment; divergence handling for other
  deployments is explicitly follow-up work.
- Encrypted reasoning size is provider-controlled and can be large; replay is
  bounded by the existing context-planning budget, not by the adapter.

## Migration Plan

Additive. New module and exports; no existing adapter changes shape. Hosts
opt in by constructing `ResponsesProvider` with an xAI base URL and a
credential source. No data migration.

## Open Questions

- Whether `grok-4.5` enforces structured output and function tools in the
  same request; fixtures will pin the observed behavior before the capability
  matrix is finalized.
