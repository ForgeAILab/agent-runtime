---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-04T00:15:16Z
completed_at:
---

Approved 2026-10-03 by the Smith owner ("continue with the road map ... good
ui ux. and strong backend") after the cross-provider matrix in proposal.md.
Branch `fix/smith-cross-provider-reasoning` from Smith's pin `9bbfdd1`.

## 1. Provenance

- [x] 1.1 Add an optional producer (provider and model) to
  `ContentPart::Reasoning`, serde-defaulted and skipped when absent. Older
  snapshots, journals, and checkpoints deserialize unchanged.
- [x] 1.2 The driver stamps the producer on every reasoning part when it
  commits assistant output, from the turn's provider and model (the values
  the run manifest records). Adapters still decode reasoning without it.

## 2. Request projection

- [x] 2.1 Where canonical history becomes `ProviderRequest.messages`, omit
  reasoning parts whose producer differs from the request's provider or
  model, before context and cache planning. Parts without a producer pass
  through.
- [x] 2.2 When nothing is omitted, the messages and the serialized request
  are byte-identical to today's (test with every adapter).
- [x] 2.3 Per adapter (OpenAI-compatible, Responses, Gemini, Anthropic), a
  test renders a history holding another producer's signed and redacted
  reasoning and asserts the request is accepted by the adapter's own
  validation and contains none of it.
- [x] 2.4 A test switches A → B → A and asserts A's reasoning and signature
  return on the third request.
- [x] 2.5 The Gemini adapter requires a signed thought only before tool calls
  in the active continuation (assistant messages after the last user input).
  Tool calls in earlier turns may be unsigned: once another provider's
  reasoning is omitted, its tool calls have no Gemini signature, and the
  adapter rejected them locally ("signed continuation is incomplete or out
  of order"). Probed live 2026-10-03: with this rule, Z.AI, xAI, and
  Anthropic turns that called tools continue on Gemini 3.8 Flash.

## 3. Verification

- [x] 3.1 `cargo fmt --all -- --check`, Clippy with `-D warnings`, workspace
  tests.
- [ ] 3.2 Push the branch; Smith bumps its pin and runs its live
  cross-provider matrix and cache comparison.
