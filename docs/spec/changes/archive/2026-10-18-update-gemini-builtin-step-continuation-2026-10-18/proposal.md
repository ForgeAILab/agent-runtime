---
created_at: 2026-10-18T00:00:00Z
updated_at: 2026-10-18T00:00:00Z
---

## Why

A production Smith session on `google/gemini-4-argon` failed with `Gemini
signed continuation is incomplete or out of order`. One Interactions response
carried its only continuation signatures on Google Search call/result steps,
then emitted function calls without a thought step. The adapter discarded the
search steps, so the next stateless request could not satisfy its signed
continuation guard.

Google's [Interactions thinking
documentation](https://ai.google.dev/gemini-api/docs/interactions/thinking.md.txt)
states that built-in tool call/result blocks may carry distinct signatures and
that stateless clients must resend tool-result signatures in later turns.

Live checks against `gemini-4-argon` on 2026-10-18 showed the shape and the
server's rules. A response streamed `google_search_call`, then
`function_call`, then `google_search_result`, with no thought step. Replaying
that history without the search steps is rejected (`invalid_request`).
Replaying them with the result ahead of the function call is also rejected,
and replaying them in stream order is accepted. Canonical history keeps
reasoning parts ahead of tool calls, so the envelope records the function call
each step followed.

## What Changes

- Preserve complete Gemini `google_search_call` and `google_search_result`
  steps as adapter-private, versioned envelopes inside redacted canonical
  reasoning parts.
- Each envelope records `after_call`, the function call that preceded the step
  in the stream. Replay sends valid envelopes verbatim to the same Gemini
  provider and model, each right after its `after_call` function call (or in
  canonical position when it has none), restoring Gemini's stream order. A
  non-empty built-in-step signature satisfies the active-turn signed
  continuation guard for the calls replayed after it.
- Bound envelope JSON independently and reject malformed, unknown-type, or
  oversized continuation state without exposing it in rendered output,
  diagnostics, hosted-search events, or errors.
- Keep existing hosted-search normalization unchanged. The runtime's reasoning
  producer projection removes the opaque Gemini parts for other providers or
  models.

## Impact

- Affected specs: `provider-runtime`
- Affected code: `crates/agent-runtime-provider/src/gemini.rs`, reasoning
  projection tests in `crates/agent-runtime-core/src/content.rs`, and
  `CHANGELOG.md`
- No core schema, checkpoint schema version, or turn transition revision
  changes.
