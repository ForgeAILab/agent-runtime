---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-04T02:19:56Z
---

## Why

A session that changes provider or model mid-conversation sends the earlier
provider's reasoning to the new one. Reasoning parts carry provider-issued
signatures or opaque redacted payloads, but nothing records who issued them,
so every adapter replays them as its own. Measured with Smith on the pinned
runtime (2026-10-03), one turn on the first provider then a resumed turn on
the second:

| From → to | Result |
| --- | --- |
| Gemini → OpenAI-compatible (Z.AI) | rejected locally: "cannot represent one or more message content parts" |
| Gemini → xAI Responses | rejected by the provider |
| xAI → Gemini | rejected: "Corrupted thought signature" |
| Anthropic Messages → Gemini | rejected by the provider |
| Gemini → Anthropic Messages | Gemini's signature sent as `redacted_thinking` |
| Z.AI → any, any → Z.AI except Gemini | works (no signed reasoning) |

## What Changes

- Committed reasoning parts record the provider and model that produced
  them.
- When a request is built, reasoning produced by a different provider or
  model is left out of the request. Canonical history keeps it, so switching
  back sends it again.
- Reasoning with no recorded producer (history written before this change)
  is sent exactly as today.
- A request whose reasoning all comes from its own provider and model is
  byte-identical to today's, so same-model sessions keep their prompt-cache
  prefix.

## Impact

- Affected specs: provider-runtime.
- Affected code: `agent-runtime-core` `ContentPart::Reasoning` (new optional
  field, serde-defaulted); the driver's commit of assistant output and the
  path that turns canonical history into `ProviderRequest.messages`; tests in
  `agent-runtime` and the provider adapters.
- Consumer: Smith (`ForgeAILab/smith`) pins this runtime by revision and
  constructs `ContentPart::Reasoning` in tests and in its ChatGPT binding; it
  bumps its pin and adds the field where it builds parts.
