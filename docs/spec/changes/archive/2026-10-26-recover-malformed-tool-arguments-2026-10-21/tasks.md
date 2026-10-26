---
created_at: 2026-10-21T04:39:57Z
updated_at: 2026-10-26T07:08:47Z
completed_at: 2026-10-21T04:50:53Z
---

## 1. Implementation
- [x] 1.1 Privately distinguish invalid JSON from structural assembly failures.
- [x] 1.2 Admit safe, bounded correction requests with input sizing, normal
  attempt accounting, cancellation/deadlines, and cache retirement.
- [x] 1.3 Keep terminal finish reasons authoritative over incomplete arguments.
- [x] 1.4 Cover correction, repeated invalid output, structural errors, limits,
  cancellation, usage, pairing, and secret-safe feedback.
- [x] 1.5 Exercise Smith with a local override, add its recovery regression,
  and run formatting, relevant tests, and Clippy.

## Completion evidence (2026-10-26)

[PR #71](https://github.com/ForgeAILab/agent-runtime/pull/71) merged as `b791341`. Release
[PR #73](https://github.com/ForgeAILab/agent-runtime/pull/73) merged as `c036af7`, and the
[v0.10.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.10.0) records bounded correction of malformed or
truncated tool arguments in its CHANGELOG.
