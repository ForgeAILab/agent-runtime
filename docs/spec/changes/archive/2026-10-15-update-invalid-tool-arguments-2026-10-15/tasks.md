---
created_at: 2026-10-15T04:23:39Z
updated_at: 2026-10-15T04:23:39Z
completed_at:
---

## 1. Implementation

- [x] 1.1 Driver: schema-invalid assembled calls become tool-error results
      instead of `MalformedStream`; they are never prepared, approved, or
      invoked.
- [x] 1.2 Tests: missing required argument, corrected retry, unparseable
      arguments still a stream error, and no approval for an invalid call.
- [x] 1.3 Gate: fmt, clippy `-D warnings` with all features, workspace tests.
