---
created_at: 2026-10-24T12:00:00Z
updated_at: 2026-10-26T05:08:53Z
completed_at: 2026-10-24T10:34:37Z
---

## 1. Outcome

- [x] 1.1 Add `ChildTaskOutcome::LimitReached` and `RuntimeEvent::ChildLimitReached`;
  bump the event schema to 17.
- [x] 1.2 Monitor path records the limit outcome with the stopped turn's
  artifacts and no text.
- [x] 1.3 Recovery reduces a limit terminal checkpoint to the same outcome and
  keeps a legacy completed record for the turn.
- [x] 1.4 Status: idle, no `last_result`, `last_error` names the limit; a later
  task (or a lagging `TurnStarted`) clears it only while the child runs.
- [x] 1.5 Child-completion delivery names the limit.

## 2. Tests

- [x] 2.1 Unit: delivery text, key validation, status projection, serde shape.
- [x] 2.2 Conformance: live limit outcome and follow-up, recovery after a failed
  outcome save, legacy completed record still recovers.

## Completion evidence (2026-10-26)

[PR #76](https://github.com/ForgeAILab/agent-runtime/pull/76) merged as `b00024a`. Release
[PR #78](https://github.com/ForgeAILab/agent-runtime/pull/78) merged as `9cdb356`, and the
[v0.11.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.11.0) records `ChildTaskOutcome::LimitReached` and
event schema 17 in its CHANGELOG.
