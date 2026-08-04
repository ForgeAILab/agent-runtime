---
created_at: 2026-07-26T20:56:34Z
updated_at: 2026-07-26T20:56:34Z
completed_at:
---

# Tasks: add-reasoning-preservation

## 1. Driver reasoning retention

- [x] 1.1 Accumulate `ReasoningDelta` events into merged `ContentPart::Reasoning` parts (`ReasoningAccumulator` in `crates/agent-runtime/src/agent/driver.rs`)
- [x] 1.2 Carry reasoning through `ProviderTurnOutcome::Success` and prepend it to the assistant history message
- [x] 1.3 Strip prior-turn reasoning at turn start; drop assistant messages left empty (`strip_stale_reasoning`)
- [x] 1.4 Integration tests: round-trip within a turn, redacted separation, cross-turn strip (`crates/agent-runtime/tests/reasoning_preservation.rs`)

## 2. OpenAI adapter round-trip

- [x] 2.1 Serialize non-redacted reasoning as `reasoning_content` on assistant wire messages (`crates/agent-runtime-provider/src/openai.rs`)
- [x] 2.2 Unit tests: echo, redacted omission, absent key

## 3. Context telemetry

- [x] 3.1 `ContextPlan::input_budget()` accessor (`crates/agent-runtime-context/src/plan.rs`)
- [x] 3.2 `ContextPlanned` gains `input_tokens` (serde-default); `input_budget_tokens` now carries the enforced budget (`crates/agent-runtime-core/src/event.rs`, `crates/agent-runtime/src/agent/driver.rs`)
- [x] 3.3 Obs renderer includes both counters (`crates/agent-runtime-obs/src/render.rs`)

## 4. Compaction

- [x] 4.1 First-stage removal of prior-turn reasoning parts from message fragments (`crates/agent-runtime-context/src/compaction.rs`)
- [x] 4.2 `truncate_fragment` handles `Reasoning` parts like text
- [x] 4.3 Unit tests: prior-turn dropped, current-turn preserved, under-target no-op, pairing validation

## 5. Verification

- [x] 5.1 Workspace fmt, clippy `-D warnings`, full test suite
- [x] 5.2 Live Z.AI GLM-4.7 run via Smith's `live_provider` test confirming `reasoning_content` continuation succeeds

## 6. Follow-on hardening

- [x] 6.1 `visible_output` on `TurnCompleted` (serde default-true, absent on the wire for ordinary turns; obs renderer flags `visible_output=false`; Smith shows a "reasoning only" notice)
- [x] 6.2 Optional `signature` on `ContentPart::Reasoning` (serde-defaulted, wire-stable when absent; tool-output truncation drops a signature when the signed text changes)
- [x] 6.3 Provider conformance: reasoning normalization across fake/OpenAI adapters, continuation acceptance, and wire-level `reasoning_content` echo (`agent-runtime-testkit`)
- [x] 6.4 Re-verify: both workspaces green, MSRV 1.86 check, live Z.AI rerun
