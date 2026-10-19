---
created_at: 2026-10-19T01:40:00Z
updated_at: 2026-10-19T01:40:00Z
completed_at:
---

## 1. Implementation

- [x] 1.1 Per-source internal-turn bound: `CHILD_COMPLETION_SOURCE_KIND`,
  `MAX_CHILD_COMPLETION_TURN_CHARS`, `InternalTurnInput::max_content_chars`.
- [x] 1.2 `DelegationConfig::delivery_inline_bytes`, validated at
  coordinator construction.
- [x] 1.3 Store long replies as parent-owned artifacts on completion and on
  recovery; `ChildTaskResult::reply`.
- [x] 1.4 Budgeted delivery builder with excerpts, artifact listing, and the
  compact fallback; admission no longer refuses for size.
- [x] 1.5 Unit tests: existing delivery text unchanged for short replies; a
  reply within the budget is whole; over-budget excerpt with and without a
  stored copy; in-order inlining; artifact listing; compact fallback; the
  per-source bound.
- [ ] 1.6 Workspace tests, clippy, and fmt green.
- [ ] 1.7 CHANGELOG entry under Unreleased.
- [ ] 1.8 PR to `main`; Smith pins the merged revision and runs its
  delegation suite.
