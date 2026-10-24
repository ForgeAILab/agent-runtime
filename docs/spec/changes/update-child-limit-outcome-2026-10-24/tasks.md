---
created_at: 2026-10-24T12:00:00Z
updated_at: 2026-10-24T10:34:37Z
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
