## Context

Agent Runtime's provider boundary is transport-injected and host-neutral.
`ProviderRequest` already carries ordered canonical messages, tools, reasoning,
structured output, output limits, and bounded vendor extensions. Provider
streams already expose text, signed reasoning, fragmented tool calls, usage,
cache observations, finish state, and classified errors.

The Gemini Interactions API represents one request as ordered input/output
steps. In stateless mode, later requests resend the complete history. Thought
steps contain opaque signatures; tool continuation fails or degrades when
required signed steps are missing or reordered. The existing canonical
`Reasoning { text, redacted, signature }`, tool-call ID/name/arguments, and
tool-result content can represent the required first-release step set, provided
signature-only reasoning blocks are not discarded.

## Goals / Non-Goals

### Goals

- Add a native, reusable, offline-testable Gemini Interactions adapter.
- Preserve canonical runtime ownership of attempts, tools, history, and retry.
- Use stateless requests so consumers do not depend on provider retention.
- Replay signed thought and function-call state exactly.
- Normalize native usage and stream events without provider-specific public
  event variants.
- Keep authentication, prompts, signatures, and raw provider failures
  non-disclosing.

### Non-Goals

- Add the Google Gen AI SDK or any language runtime.
- Support `generateContent`, OpenAI compatibility, Vertex AI, managed agents,
  or provider-hosted tools.
- Use `store=true`, background interactions, retrieval, deletion, or
  `previous_interaction_id`.
- Define product provider names, default endpoints, model catalogs, setup, or
  credential persistence.
- Add live-network tests to the shared runtime.

## Decisions

### Implement native REST/SSE over the injected transport

`GeminiInteractionsConfig` contains a reviewed base endpoint, model identity,
resolved model capabilities, and bounded adapter options. Provider
construction validates an absolute HTTPS endpoint without credentials, query,
or fragment. Hosts may enforce a stricter fixed endpoint.

The adapter appends only its reviewed Interactions path, acquires one provider
credential lease after request/endpoint validation, and attaches the key as
`x-goog-api-key` immediately before transport. It uses the same renewable
credential invalidation and one-replay contract as other direct adapters.

No Google client library is added. Serialization and SSE normalization remain
small Rust-owned mechanism over `HttpTransport`, so tests inject exact request
and response bytes.

### Always operate statelessly

Every request sets `stream=true` and `store=false`; the adapter rejects host
vendor extensions that attempt to enable storage, background execution,
provider-managed continuation, endpoint changes, or hosted tools.

Canonical history is translated to Interactions steps in order:

- system content becomes the interaction system instruction;
- user text/images become user-input content;
- assistant text becomes model-output content;
- signed reasoning becomes a thought step with the opaque signature and
  optional redacted summary;
- canonical tool calls become function-call steps with their stable call ID;
  and
- canonical tool results become correlated function-result steps.

The adapter emits signed thought steps as `ReasoningDelta` and MUST preserve a
signature-only block even when the provider sends no summary text. Existing
reasoning assembly/persistence is strengthened only where needed to keep that
block ordered in assistant content. Missing signatures on a current-turn
Gemini function-call continuation fail locally before transport.

Alternative considered: introduce a general opaque JSON content block. Rejected
for the first release because the supported Interactions step set maps to
existing signed reasoning and typed tool content; an unbounded escape hatch
would weaken canonical-history guarantees.

### Keep model policy outside the adapter

The host supplies the selected model and resolved capabilities. The adapter
does not fetch Models.dev, guess limits from model names, or choose a default.
It uses capabilities to validate tools, reasoning, structured output,
modalities, usage, and output limits before transport.

Named Agent Runtime reasoning effort maps to native `thinking_level`. Unknown
or unsupported efforts fail before I/O. Sampling and stop controls are sent
only when the reviewed Interactions schema and resolved model capability
permit them; unsupported controls fail explicitly rather than being dropped.

### Normalize the typed stream loss-aware

The adapter accepts the reviewed SSE event sequence:

- interaction lifecycle events establish identity/status but do not create a
  second session abstraction;
- model-output deltas emit visible text;
- thought-summary/signature deltas emit one ordered signed reasoning block;
- function-call start/deltas emit indexed `ToolCallDelta` fragments;
- completion usage emits disjoint input, output, reasoning, tool-use, and cache
  observations where shared fields exist; and
- completed/requires-action/incomplete/failed/cancelled statuses map to the
  existing finish or error vocabulary.

Unknown additive event fields are ignored only when the enclosing known event
remains unambiguous. Unknown event types, invalid indices, duplicate terminal
events, bad JSON fragments, missing tool identity, or truncated signed thought
steps fail as malformed streams.

### Bound and redact every provider-owned value

Model output and argument fragments use existing runtime bounds. Interaction
IDs, event IDs, raw error messages, annotations, and signatures never enter
default metadata or diagnostics. Signatures are stored only in signed
reasoning content for exact replay and keep opaque debug/display behavior.

Authentication status is classified without retaining the response body.
Cancellation drops the transport stream; no background task, provider cancel
request, or hidden retry is spawned by the adapter.

## Risks / Trade-offs

- Interactions is a newer schema. A pinned adapter revision and recorded
  request/stream fixtures make drift visible.
- Stateless history can be larger than provider-side continuation. The runtime
  retains deterministic local replay and avoids remote retention dependency.
- Signed thought ordering is correctness-critical. Sequential/parallel tool
  and clean-resume conformance are mandatory release gates.
- Some native Gemini capabilities remain deliberately unsupported until the
  shared contract can represent them without provider-specific execution paths.

## Migration Plan

1. Prove signature-only reasoning survives provider stream assembly,
   canonical history serialization, and replay.
2. Add request/response wire types and stateless history translation.
3. Add credential-aware native provider construction and SSE normalization.
4. Add deterministic unit and shared conformance fixtures.
5. Export the adapter, update docs/changelog, and run all runtime gates plus
   Smith consumer conformance.
6. Publish a compatible runtime revision for Smith to pin separately.

## Open Questions

None for approval. Exact public type names may follow crate conventions without
weakening stateless operation, signed continuation, cancellation, redaction,
and capability-validation requirements.

## References

- https://ai.google.dev/gemini-api/docs/interactions-overview
- https://ai.google.dev/gemini-api/docs/streaming
- https://ai.google.dev/gemini-api/docs/function-calling
- https://ai.google.dev/gemini-api/docs/thought-signatures
