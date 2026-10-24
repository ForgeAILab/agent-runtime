---
created_at: 2026-10-24T12:00:00Z
updated_at: 2026-10-24T10:34:37Z
---

## Why

A delegated child whose turn ended at a limit (`TurnFinish::LimitReached`) was
reported to its parent exactly like a completed task. The monitor and the
recovery path both built a `Completed` outcome from the child's last assistant
message. After an output limit the runtime discards the response that ran
over, so that message is an earlier preamble ("One quick check…, then I'll
write the document."). In a Smith session four children each lost about 29
minutes of work this way. The parent received `child_completed` with the
preamble as the result, learned nothing about the limit, and found out only by
paging the child's transcript.

## What Changes

- `ChildTaskOutcome::LimitReached { child, turn, limit, artifacts }` is the
  protected outcome of a child task that stopped at a limit. It carries no
  text. Artifacts the stopped turn produced are copied to the parent as for a
  completed task. Its cursor identity is the turn's, the same identity a
  completed outcome uses, since a turn has one terminal outcome.
- The child stays `Idle` and available for follow-ups. Its status drops
  `last_result` and sets `last_error` to a `Limit` error naming the limit
  (metadata `limit`). The error clears when the next task starts.
- The parent stream gets `RuntimeEvent::ChildLimitReached { child, limit }`
  instead of `ChildCompleted`. Event schema version 17.
- Automatic child-completion delivery says the child stopped at the named
  limit, gave no final answer, and can continue by follow-up.
- Recovery reduces a `LimitReached` terminal checkpoint to the same outcome.
  An outcome an older build already recorded as completed for that turn is
  kept. It is never contradicted by a second outcome, which the ledger would
  reject as a conflicting duplicate.

## Impact

- Affected specs: `agent-delegation`.
- Affected code: `agent-runtime-core` (`RuntimeEvent`, `LimitKind::as_str`),
  `agent-runtime` delegation (monitor, persistence, coordinator, types),
  `agent-runtime-obs` rendering, and the delegation conformance suites.
- Breaking for exhaustive matches on `ChildTaskOutcome` and `RuntimeEvent`.
  Checkpoint constants are unchanged. Persisted catalogs, ledgers and cursors
  written before this change still load. A ledger written after it cannot be
  read by an older build.
