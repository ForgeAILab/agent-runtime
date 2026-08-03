---
created_at: 2026-08-02T21:39:09Z
updated_at: 2026-08-02T23:57:21Z
---

## Why

Consumers need a reusable native Google Gemini provider rather than routing
Gemini through OpenAI compatibility or implementing consumer-local adapters.
Google's current Interactions API exposes typed streamed model output,
thinking, function calls/results, usage, and structured output, but its
stateless mode requires exact replay of signed thought steps.

## What Changes

- Add a transport-injected `GeminiInteractionsProvider` and configuration type
  to `agent-runtime-provider` using the shared `Provider`, credential-source,
  cancellation, deadline, capability, error, and event contracts.
- Encode native stateless requests with `stream=true` and `store=false`, using
  complete canonical local history instead of provider-side conversation
  storage or `previous_interaction_id`.
- Map canonical messages, images, tool declarations/choice, function calls and
  results, structured output, output limits, and named reasoning effort to the
  reviewed Interactions schema.
- Normalize streamed model-output, thought summary/signature, function-call
  argument fragments, usage/cache observations, finish states, and failures
  without losing source order.
- Reuse signed `ContentPart::Reasoning` for Gemini thought steps and strengthen
  stream assembly/replay so signature-only blocks survive. Function call IDs,
  names, arguments, and results remain first-class canonical content.
- Reject missing signed thought continuation, invalid model capability, unsafe
  endpoint construction, unsupported request controls, and malformed streams
  before or at the documented provider boundary with redaction-safe errors.
- Add deterministic adapter and shared conformance fixtures for text,
  reasoning, parallel/sequential tools, structured output, multimodal content,
  usage/cache, cancellation, authentication, malformed streams, and replay.

## Impact

- Affected specs: `provider-runtime`, `runtime-reproducibility`.
- Affected code: `agent-runtime-core` signed-reasoning preservation contracts;
  `agent-runtime-provider` native adapter and exports; `agent-runtime-testkit`
  recorded fixtures/conformance; facade docs, changelog, and compatibility
  gates.
- Public compatibility: additive provider/config exports and stricter
  preservation of already-optional signed reasoning blocks. Existing providers
  and unsigned content retain their wire shape.
- Consumer: coordinated Smith behavior is specified by
  `../tui/docs/spec/changes/add-google-gemini-provider-2026-08-02/`.

## Active Change Coordination

- `add-renewable-provider-credentials-2026-08-02` remains authoritative for
  per-attempt credential acquisition, exact invalidation, and bounded auth
  recovery. Gemini reuses it and adds no hidden credential request.
- `add-reasoning-preservation-2026-07-26` remains authoritative for signed
  reasoning content. This change proves signature-only Gemini thought steps
  survive assembly, persistence, and request replay.
- `stabilize-session-harness-pipeline-2026-07-31` remains authoritative for
  attempt lifecycle, speculative output, retries, checkpoints, and canonical
  history.

## Approval Boundary

Approval authorizes the reusable native stateless Gemini Interactions adapter,
signed-thought replay requirements, and deterministic conformance fixtures. It
does not authorize a Google SDK dependency, Vertex AI, Google-hosted tools,
server-side conversation storage, product configuration/UX, provider-specific
secret persistence, or live provider spend.
