---
created_at: 2026-10-05T00:00:00Z
updated_at: 2026-10-05T00:00:00Z
---

## Why

A parent model that reads a child's result itself gets the same result again.
In Smith (2026-10-05) the main agent spawned a child, called the agent tool's
wait action, received "pong", and answered "pong". When the turn ended, the
automatic child-completion turn delivered the same outcome, and the model
answered "pong" a second time ("I already replied 'pong'"). Every task where
the parent waits for a child costs an extra provider call and a duplicate
answer.

Host inspection (`task_outcome`, `wait`) is idempotent by design, so the
runtime cannot tell that the outcome it returned reached the model. Only the
automatic admission consumes an outcome.

## What Changes

- New `DelegationCoordinator::acknowledge_task_outcome_on_tool_result(turn,
  call, outcome)`. A host delegation tool that returns an outcome to the
  model calls it from its invocation.
- When that call's result commits to the parent's canonical history without an
  error, the outcome leaves the automatic-delivery projection, the same way an
  explicit follow-up or stop supersedes it, and the parent snapshot is saved
  before the turn continues. The ledger keeps it, so `task_outcome` still
  returns it. The protected cursor is unchanged.
- If the result is an error or the turn ends before the result commits, the
  outcome stays ready and is delivered automatically. A crash between the tool
  result checkpoint and the snapshot save repeats a delivery; it never loses
  one.
- The turn driver gains a per-call commit hook, run after a non-error tool
  result's checkpoint commits and dropped when the turn ends first.

## Impact

- Affected specs: agent-delegation.
- Affected code: `delegation/coordinator.rs` (new method), `runtime/state.rs`
  (tool-result commit hooks), `agent/driver/turn.rs` (`commit_tool_result`
  runs them), conformance tests in `agent-runtime-testkit`.
- Consumer: Smith calls the new method from its agent tool's `wait` and
  `result` actions, and its headless mode stops waiting for a delivery turn
  that no longer comes.
