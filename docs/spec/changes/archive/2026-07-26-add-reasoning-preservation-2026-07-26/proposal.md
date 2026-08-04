---
created_at: 2026-07-26T20:56:34Z
updated_at: 2026-07-26T20:56:34Z
---

# Proposal: Preserve streamed reasoning and correct context telemetry

## Why

Live testing against Z.AI GLM-4.7 (Coding Plan, thinking mode) showed the
runtime discards streamed reasoning after emitting `ReasoningDelta` events.
OpenAI-compatible thinking models require the reasoning streamed with a
tool-call answer to be echoed back (`reasoning_content`) on the assistant
message during the same turn's continuation; without it the provider's
thinking contract is broken. Separately, the `ContextPlanned` event documents
`input_budget_tokens` as the enforced budget but actually reports consumed
input tokens, so budget dashboards read consumption as the limit.

## What

1. The driver accumulates streamed reasoning into `ContentPart::Reasoning`
   history parts (merging consecutive same-`redacted` deltas), placed ahead of
   visible text and tool calls on the assistant message.
2. When a new user turn starts, the driver strips prior-turn reasoning from
   the canonical history and drops assistant messages left empty by the strip
   (reasoning-only completions).
3. The OpenAI adapter serializes non-redacted reasoning parts as
   `reasoning_content` on assistant wire messages; redacted reasoning never
   reaches the wire.
4. `ContextPlanned` reports both `input_tokens` (counted consumption, new
   field, `serde(default)` for old journals) and `input_budget_tokens` (now
   actually the enforced budget).
5. Compaction treats prior-turn reasoning as the cheapest reclaim: a new
   first stage removes reasoning parts from message fragments before the last
   user message, and bounded truncation handles `Reasoning` parts like text.

## Impact

- Affected specs: agent-execution, provider-runtime, context-management.
- Affected crates: agent-runtime (driver), agent-runtime-core (event),
  agent-runtime-provider (openai), agent-runtime-context (plan accessor,
  compaction), agent-runtime-obs (render).
- Wire/schema: `ContextPlanned` gains `input_tokens` (backwards-compatible via
  `serde(default)`); the meaning of `input_budget_tokens` is corrected to
  match its documentation. Hosts that treated it as consumption must switch
  to `input_tokens`.
- Smith (../tui) consumes events generically via serde and needs no change;
  its live Z.AI test exercises the new round-trip end to end.
