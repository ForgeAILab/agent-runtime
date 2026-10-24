---
created_at: 2026-10-24T19:54:52Z
updated_at: 2026-10-24T19:54:52Z
---

## Why

`SessionHandle::try_idle_semantic_compaction` permits one attempt per idle
interval, and only a new turn starts another interval. Smith's `/compact`
calls it, so after an automatic idle attempt, or an earlier `/compact`, ran
in the same interval, an explicit request is refused as busy. A user whose
summary failed or fell back cannot retry without sending a turn first, which
is the work compaction was meant to make cheaper.

## What Changes

- `SessionHandle::try_manual_semantic_compaction` runs the same flat semantic
  compaction for an explicit host request. An earlier attempt in the current
  idle interval does not refuse it. Every other admission rule still applies:
  no running or pending turn, no compaction in flight, a safe checkpoint, the
  configured flat summary, and the same persistence and rollback.
- A manual attempt leaves the interval attempted, so automatic callers of
  `try_idle_semantic_compaction` still get one attempt per interval.
- `try_idle_semantic_compaction` and LCM's `try_idle_compaction` are
  unchanged.

## Impact

- Affected specs: `context-management`.
- Affected code: `agent-runtime` session idle compaction
  (`runtime/session/turns.rs`) and its semantic-summary tests.
- Additive API. No event, checkpoint or persisted-state change.
