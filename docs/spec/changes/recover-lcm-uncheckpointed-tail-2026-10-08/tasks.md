---
created_at: 2026-10-08T00:00:00Z
updated_at: 2026-10-08T00:00:00Z
completed_at: 2026-10-08T00:00:00Z
---

## 1. Implementation
- [x] 1.1 Add tail recovery after the append-successor check in resume validation.
- [x] 1.2 Truncate with the owner-fenced binding recorded by the claim recheck.

## 2. Verification and Delivery
- [x] 2.1 Integration test: a session-store-only host killed mid-turn resumes and accepts a turn.
- [x] 2.2 Unit test: a store that cannot truncate keeps the original conflict.
- [x] 2.3 Resume a copy of the stuck Nyx production session with the patched runtime.
- [x] 2.4 Workspace tests, clippy and changelog entry.
