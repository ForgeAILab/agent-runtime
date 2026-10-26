---
created_at: 2026-10-23T10:03:12Z
updated_at: 2026-10-26T05:08:34Z
completed_at: 2026-10-23T13:28:40Z
---

## 1. Implementation
- [x] 1.1 Implement route snapshots, builder validation, model-bound planner activation, and canonical events.
- [x] 1.2 Implement first-response budgets, cancellation, retry integration, and conservative replay fences.
- [x] 1.3 Preserve request/tool history and route identity across turn/checkpoint boundaries.
- [x] 1.4 Run deterministic fallback and regression tests, formatting, Clippy, and Smith consumer conformance.

## Completion evidence (2026-10-26)

[PR #76](https://github.com/ForgeAILab/agent-runtime/pull/76) merged as `b00024a`. Release
[PR #78](https://github.com/ForgeAILab/agent-runtime/pull/78) merged as `9cdb356`, and the
[v0.11.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.11.0) records ordered request-level model fallback in
its CHANGELOG.
