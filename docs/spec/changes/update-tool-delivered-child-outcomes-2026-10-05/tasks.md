---
created_at: 2026-10-05T00:00:00Z
updated_at: 2026-10-05T00:00:00Z
---

Approved 2026-10-05 by the Smith owner ("we need to fix the double delivery").
Branch `fix/smith-agent-result-delivery` from Smith's pin `b1d1974`
(`smith-baseline-v0.3.3`).

## 1. Runtime

- [x] 1.1 `SessionExecutionContext` holds commit hooks keyed by turn and tool
  call. Registering against a turn that is not serving fails. `clear_turn`
  drops the turn's hooks.
- [x] 1.2 `commit_tool_result` takes the call's hooks after the checkpoint
  transition succeeds and awaits them when the block is not an error.
- [x] 1.3 `acknowledge_task_outcome_on_tool_result` validates the outcome
  against the durable ledger and registers a hook that removes that exact
  outcome from the ready projection and saves the parent snapshot.

## 2. Conformance

- [x] 2.1 A committed tool result withdraws the outcome: no ready outcome,
  admission finds nothing to deliver, `task_outcome` still returns it, and a
  restarted parent agrees.
- [x] 2.2 An error tool result keeps automatic delivery.
- [x] 2.3 A turn interrupted before the result commits keeps automatic
  delivery.
- [x] 2.4 Acknowledging against a turn that is not serving fails.

## 3. Verification

- [x] 3.1 `cargo fmt --all -- --check`, Clippy with `-D warnings`, workspace
  tests.
- [ ] 3.2 Push the branch; Smith bumps its pin and runs its delegation and
  headless tests.
