---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-04T00:15:16Z
completed_at:
---

Approved 2026-10-03 by the Smith owner ("continue with the road map ... good
ui ux. and strong backend") after the cross-provider matrix in proposal.md.
Branch `fix/smith-cross-provider-reasoning` from Smith's pin `9bbfdd1`.

## 1. Provenance

- [ ] 1.1 Add an optional producer (provider and model) to
  `ContentPart::Reasoning`, serde-defaulted and skipped when absent. Older
  snapshots, journals, and checkpoints deserialize unchanged.
- [ ] 1.2 The driver stamps the producer on every reasoning part when it
  commits assistant output, from the turn's provider and model (the values
  the run manifest records). Adapters still decode reasoning without it.

## 2. Request projection

- [ ] 2.1 Where canonical history becomes `ProviderRequest.messages`, omit
  reasoning parts whose producer differs from the request's provider or
  model, before context and cache planning. Parts without a producer pass
  through.
- [ ] 2.2 When nothing is omitted, the messages and the serialized request
  are byte-identical to today's (test with every adapter).
- [ ] 2.3 Per adapter (OpenAI-compatible, Responses, Gemini, Anthropic), a
  test renders a history holding another producer's signed and redacted
  reasoning and asserts the request is accepted by the adapter's own
  validation and contains none of it.
- [ ] 2.4 A test switches A → B → A and asserts A's reasoning and signature
  return on the third request.

## 3. Verification

- [ ] 3.1 `cargo fmt --all -- --check`, Clippy with `-D warnings`, workspace
  tests.
- [ ] 3.2 Push the branch; Smith bumps its pin and runs its live
  cross-provider matrix and cache comparison.
