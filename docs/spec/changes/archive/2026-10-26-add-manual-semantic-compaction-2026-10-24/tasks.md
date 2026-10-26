---
created_at: 2026-10-24T19:54:52Z
updated_at: 2026-10-26T05:08:52Z
completed_at: 2026-10-26T05:08:52Z
---

## 1. Runtime

- [x] 1.1 Add `SessionHandle::try_manual_semantic_compaction`, sharing the idle
  compaction path but skipping only the once-per-interval gate.
- [x] 1.2 Keep the interval attempted after a manual attempt.

## 2. Tests

- [x] 2.1 Unit: after an idle attempt, the automatic call is busy, the manual
  call is admitted, and the automatic call stays busy afterwards.
- [x] 2.2 Unit: a manual retry after a failed summary calls the model again; a
  manual call while a compaction is in flight is busy.
- [x] 2.3 Workspace gate: fmt, clippy `-D warnings`, `cargo test --workspace
  --all-features` (2,140 passed, 0 failed).

## Completion evidence (2026-10-26)

[PR #77](https://github.com/ForgeAILab/agent-runtime/pull/77) merged as `34c619a`. Release
[PR #78](https://github.com/ForgeAILab/agent-runtime/pull/78) merged as `9cdb356`, and the
[v0.11.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.11.0) records
`SessionHandle::try_manual_semantic_compaction` in its CHANGELOG.
