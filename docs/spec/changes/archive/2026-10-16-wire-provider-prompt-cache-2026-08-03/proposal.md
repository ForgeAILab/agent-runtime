---
created_at: 2026-08-03T23:40:00Z
updated_at: 2026-08-03T23:40:00Z
---

## Why

The context planner computes a full cache plan — a stable prefix, a declared
`CacheClass` per segment, a `ProviderCachePlan` — and then throws it away at the
provider boundary. No adapter declares a `ProviderCacheCapability`; the type is
referenced only by the context crate that defines it and by the conformance
testkit. `ProviderRequest` carries no cache hint, and `ProviderCallContext`
carries no stable identity an adapter could key a provider-side cache to.

The consequences are measurable. The Anthropic adapter reads
`cache_read_input_tokens` back but never emits a single `cache_control`
breakpoint, so every request pays full price for a prefix the provider would
happily have cached. On the Responses API, a benchmark of the same task on the
same model showed 86% of input served from cache for a client that sends a
stable `prompt_cache_key` against 16% for one that does not — the same
conversation, an order of magnitude apart in cost, decided entirely by a field
nobody was sending.

Keeping the prefix byte-identical, which the planner already guarantees, is
necessary but not sufficient. Something has to tell the provider to cache it.

## What Changes

- `Capabilities` gains `prompt_cache: PromptCacheControl`, declaring how an
  adapter drives a prompt cache: `None`, `Implicit` (the provider matches a
  prefix on its own and the adapter only keeps it stable and keyed), or
  `Explicit { max_breakpoints }` (the adapter marks segments itself).
- `ProviderCacheCapability::from_control` maps that declaration onto the neutral
  cache classes, so a plan reports what the serving adapter can actually honor
  instead of assuming nothing.
- `ProviderCallContext` gains `session`. A request id changes every turn and is
  useless as a cache key; session identity is what a provider-side prompt cache
  must be routed by, and an adapter had no access to it.
- The OpenAI and ChatGPT Responses adapters send `prompt_cache_key` derived from
  the session, and declare `Implicit`.
- The Anthropic adapter marks the tool block and the trailing system block with
  `cache_control`, the two parts of the request the planner already classifies
  `CacheClass::Stable`, and declares `Explicit`.
- The Gemini adapter declares `Implicit`; it caches a matching prefix without
  adapter-side markers.

## Impact

- Affected specs: `provider-runtime`
- Affected code: `agent-runtime-core/src/provider.rs`,
  `agent-runtime-context/src/cache.rs`,
  `agent-runtime-provider/src/{openai,anthropic,gemini}.rs`, and every
  `ProviderCallContext` construction site.
- Breaking: `ProviderCallContext` gains a required field and `Capabilities`
  gains one with a conservative `None` default.
- Not in scope: threading the plan's exact stable-prefix message index into
  `ProviderRequest` so an explicit adapter can place a breakpoint mid-history.
  Tools and system instructions are the stable, high-value part and are
  addressable without it.
