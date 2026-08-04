---
created_at: 2026-08-04T00:31:01Z
updated_at: 2026-08-04T01:07:09Z
---

## Why

Consumers need a reusable adapter for xAI's Grok models. xAI's current frontier
model, `grok-4.5` (500k context, configurable low/medium/high reasoning
effort), is served primarily over the OpenAI Responses API — verified against
upstream `xai-org/grok-build` at `e5478eff`, whose sampler defaults to
`api_backend: "responses"` for `grok-4.5`, forces `store: false`, always
requests `include: ["reasoning.encrypted_content"]`, and forwards a prompt
cache key only on that backend. The existing `OpenAiProvider` speaks only Chat
Completions: it cannot express Responses input items, encrypted reasoning
replay, or the `completed`/`incomplete`/`failed` terminal vocabulary. The
Messages dialect xAI also serves is already reachable through the existing
Anthropic adapter by base-url configuration; the Responses dialect is the
missing piece.

## What Changes

- Add a transport-injected `ResponsesProvider` and configuration type to
  `agent-runtime-provider` speaking the OpenAI Responses wire protocol over
  the shared `Provider`, credential-source, cancellation, deadline,
  capability, error, and event contracts. Base URL is configurable; the first
  supported and fixture-verified deployment is xAI (`api.x.ai/v1/responses`)
  serving Grok.
- Encode stateless streaming requests with `stream=true` and `store=false`,
  translating complete canonical local history into Responses input items.
  Provider-side conversation state (`store=true`, `previous_response_id`),
  background responses, and hosted tools (web search, X search, code
  execution) are rejected before credential or network I/O.
- Map canonical messages, images, function tool declarations/choice, function
  calls and results, structured output (`text.format` JSON schema), output
  limits, and named reasoning effort (`low`/`medium`/`high`) onto the
  Responses schema with pre-I/O capability validation.
- Always request `include: ["reasoning.encrypted_content"]` and reuse the
  signed `ContentPart::Reasoning` contract for encrypted reasoning items:
  summary/text deltas stream as reasoning, encrypted-only items survive as
  signature-bearing non-rendered blocks, and continuations replay preserved
  reasoning items verbatim in source order.
- Normalize streamed output-text deltas, reasoning deltas, fragmented
  function-call arguments, usage (including cached and reasoning tokens), and
  exactly one `completed`/`incomplete`/`failed` terminal into the existing
  provider event vocabulary. `incomplete` surfaces its truncation reason as a
  finish state, and a stream ending without a terminal is a structured
  malformed-stream error.
- Declare `PromptCacheControl::Implicit` and send a session-derived
  `prompt_cache_key`, conforming to the session-keyed cache requirements of
  `wire-provider-prompt-cache-2026-08-03`.
- Resolve Grok model metadata (context window, output limits, reasoning
  efforts) through the existing model catalog and host overrides. Grok Build's
  product configuration (`default_models.json`, compaction thresholds, UI
  labels) is not embedded in the runtime.
- Add deterministic adapter and shared conformance fixtures for text,
  reasoning, encrypted-reasoning replay, parallel/sequential tools, structured
  output, usage/cache, cancellation, authentication, terminal mapping, and
  malformed streams.

## Impact

- Affected specs: `provider-runtime`.
- Affected code: `agent-runtime-provider` new `responses` module and exports;
  `agent-runtime-testkit` recorded fixtures/conformance; facade docs,
  changelog, and compatibility gates. No new `agent-runtime-core` contract is
  expected: encrypted reasoning reuses the signed-reasoning preservation
  contract, and cache declaration reuses `PromptCacheControl` from the
  in-flight prompt-cache change.
- Public compatibility: additive provider/config exports. Existing adapters
  retain their wire shape.
- Not in scope: OpenAI-served Responses conformance (`api.openai.com`). The
  adapter is base-url configurable, but only the xAI deployment is
  fixture-verified by this change; OpenAI/ChatGPT-specific divergences are a
  follow-up change.

## Active Change Coordination

- `wire-provider-prompt-cache-2026-08-03` remains authoritative for cache
  capability declaration, session-keyed `prompt_cache_key`, and plan
  reporting. This adapter declares `Implicit` and adds no cache requirement of
  its own.
- `add-renewable-provider-credentials-2026-08-02` (archived) remains
  authoritative for per-attempt credential acquisition, invalidation, and
  bounded auth recovery. Grok Build's OIDC/session flow against
  `cli-chat-proxy.grok.com` is host product work implemented as a renewable
  `ProviderCredentialSource`; the adapter only consumes leases as bearer
  tokens.
- `add-reasoning-preservation-2026-07-26` and the Gemini signed-thought work
  remain authoritative for signature-bearing reasoning survival through
  assembly, persistence, and replay. This change adds Responses
  encrypted-content fixtures on top of that contract.

## Approval Boundary

Approval authorizes the reusable native stateless Responses adapter, encrypted
reasoning replay requirements, catalog-resolved Grok model metadata, and
deterministic conformance fixtures. It does not authorize an xAI or OpenAI SDK
dependency, Grok Build's OIDC/session authentication or proxy endpoints,
hosted tools, background or stored responses, `x-grok-*` product telemetry
headers as runtime behavior, product prompts/UI/orchestration,
provider-specific secret persistence, or live provider spend.
