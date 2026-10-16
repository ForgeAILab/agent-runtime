---
created_at: 2026-08-04T00:31:01Z
updated_at: 2026-08-04T01:07:09Z
completed_at: 2026-08-04T01:07:09Z
---

## 0. Approval and coordination

- [x] 0.1 Approve the native stateless Responses adapter, encrypted-reasoning
  replay, and conformance boundary.
- [x] 0.2 Reconcile final types with `wire-provider-prompt-cache-2026-08-03`
  (`PromptCacheControl::Implicit`, session-derived `prompt_cache_key`) and the
  shipped renewable-credential and signed-reasoning contracts.

## 1. Native request encoding

- [x] 1.1 Add bounded Responses request/input-item wire types and exact
  stateless canonical-history translation (`stream=true`, `store=false`,
  `include: ["reasoning.encrypted_content"]`).
- [x] 1.2 Encode tools, tool choice, function results, images, structured
  output (`text.format` JSON schema), output limits, and named reasoning
  effort (`low`/`medium`/`high`) with pre-I/O capability validation.
- [x] 1.3 Enforce safe endpoint construction and refusal of storage,
  `previous_response_id`, background execution, and hosted-tool overrides
  before credential or network I/O.

## 2. Encrypted reasoning continuation

- [x] 2.1 Map reasoning output items onto signed `ContentPart::Reasoning`:
  summary/text deltas as visible reasoning, `encrypted_content` as the opaque
  signature, encrypted-only items as signature-bearing redacted blocks.
- [x] 2.2 Replay preserved reasoning items verbatim in original order relative
  to function calls on continuation; never invent or reorder encrypted
  content; drop signatures whose text was altered.
- [x] 2.3 Add replay fixtures for unsigned, summary-plus-encrypted, and
  encrypted-only reasoning items.

## 3. Streaming provider

- [x] 3.1 Add credential-source-aware `ResponsesProvider` construction with
  bearer injection over `HttpTransport`, reusing shared SSE normalization and
  the authentication-recovery contract.
- [x] 3.2 Normalize output-text deltas, reasoning deltas, fragmented
  function-call arguments with preserved call IDs, usage with cached and
  reasoning token detail, and the `completed`/`incomplete`/`failed` terminals
  into existing shared events.
- [x] 3.3 Enforce cancellation, deadline, bounded fragments, single terminal,
  missing-terminal and conflicting-terminal classification, truncation finish
  for `incomplete`, and non-disclosing diagnostics.
- [x] 3.4 Declare per-model capabilities (`prompt_cache: Implicit`,
  `continuation: false`, `auth: ApiKey`) and send the session-derived
  `prompt_cache_key`.

## 4. Conformance and release surface

- [x] 4.1 Add deterministic request/stream fixtures recorded against the xAI
  deployment for text, reasoning, encrypted replay, sequential/parallel
  tools, structured output, usage/cache, auth, cancellation, terminal
  mapping, and malformed streams.
- [x] 4.2 Add the adapter to reusable provider/runtime conformance and
  consumer compatibility gates; verify `grok-4.5` metadata resolves through
  the model catalog with no embedded product defaults.
- [x] 4.3 Export the provider/config types and update README, changelog,
  migration notes, and crate documentation.
- [x] 4.4 Run formatting, Clippy with warnings denied, focused tests,
  workspace tests, MSRV, schema compatibility, redaction, and consumer gates.
