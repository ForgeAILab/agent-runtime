---
created_at: 2026-10-19T01:40:00Z
updated_at: 2026-10-20T01:02:11Z
completed_at: 2026-10-20T01:02:11Z
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
- [x] 1.6 Workspace tests, clippy, and fmt green.
- [x] 1.7 CHANGELOG entry under Unreleased.
- [x] 1.8 PR to `main`; Smith pins the merged revision and runs its
  delegation suite.

## Completion evidence (2026-10-19)

[PR #55](https://github.com/ForgeAILab/agent-runtime/pull/55) merged as
`70c4f96ce8060c00d6176bc0724ee6d252b1426d`. Its
[main CI](https://github.com/ForgeAILab/agent-runtime/actions/runs/39196249148)
passed workspace and doc tests on Linux and macOS, formatting, Clippy,
consumer conformance, dependency policy, and the declared MSRV builds.
The reported local run passed 2,007 tests. The PR's macOS
`task_2_6_crash_matrix_accepted` failure also occurred on main and passed on
rerun. CHANGELOG's Unreleased section records both delivery fixes and the
new public fields.

Smith pinned that exact revision without a committed path override; all 30
delegation tests passed, including exact readback of an over-budget reply.
Smith commit `847e1c8` landed the integration, passed all four
[release-commit CI jobs](https://github.com/ForgeAILab/smith/actions/runs/39200661920),
and shipped it in [v0.7.5](https://github.com/ForgeAILab/smith/releases/tag/v0.7.5).
Smith's coordinated spec is already archived under
`docs/spec/changes/archive/2026-10-19-update-child-result-delivery-2026-10-18/`.
