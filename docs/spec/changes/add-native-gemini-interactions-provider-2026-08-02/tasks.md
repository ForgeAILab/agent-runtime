---
created_at: 2026-08-02T21:39:09Z
updated_at: 2026-08-02T23:57:21Z
completed_at: 2026-08-02T23:57:21Z
---

## 0. Approval and coordination

- [x] 0.1 Approve the native stateless Interactions adapter, signed-thought
  replay, and conformance boundary.
- [x] 0.2 Reconcile final types with renewable credentials, reasoning
  preservation, stabilized attempt lifecycle, and Smith consumer requirements.

## 1. Signed continuation contract

- [x] 1.1 Preserve signature-only reasoning blocks through stream assembly,
  canonical assistant content, serialization, persistence, and replay.
- [x] 1.2 Reject missing or reordered signed thought state before a Gemini
  function-result continuation reaches transport.
- [x] 1.3 Add backward-compatible schema and replay fixtures for unsigned,
  signed-summary, and signature-only reasoning content.

## 2. Native request encoding

- [x] 2.1 Add bounded Gemini Interactions request/step wire types and exact
  stateless canonical-history translation.
- [x] 2.2 Encode tools, choice, function results, images, structured output,
  output limits, and supported thinking levels with pre-I/O validation.
- [x] 2.3 Enforce `store=false`, streaming, safe endpoint/path construction,
  and refusal of hosted-tool or provider-state overrides.

## 3. Streaming provider

- [x] 3.1 Add credential-source-aware `GeminiInteractionsProvider`
  construction and `x-goog-api-key` injection over `HttpTransport`.
- [x] 3.2 Normalize model output, thought/signature, fragmented function calls,
  usage/cache, finish state, and errors into existing shared events.
- [x] 3.3 Enforce cancellation, deadline, bounded fragments, single terminal,
  malformed-stream classification, and non-disclosing diagnostics.

## 4. Conformance and release surface

- [x] 4.1 Add deterministic request/stream fixtures for text, signed reasoning,
  sequential/parallel tools, multimodal results, structured output, usage,
  cache, auth, cancellation, and malformed streams.
- [x] 4.2 Add the native adapter to reusable provider/runtime conformance and
  Smith consumer compatibility gates.
- [x] 4.3 Export the provider/config types and update README, changelog,
  migration notes, and crate documentation.
- [x] 4.4 Run formatting, Clippy with warnings denied, focused tests, workspace
  tests, MSRV, schema compatibility, redaction, and consumer gates.
