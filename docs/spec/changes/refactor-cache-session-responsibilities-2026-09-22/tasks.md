---
created_at: 2026-09-22T20:49:36Z
updated_at: 2026-09-22T21:10:24Z
completed_at: 2026-09-22T21:10:24Z
---

# Tasks: Refactor cache and session source responsibilities

## 1. Cache mechanism
- [x] 1.1 Move request construction and public request types into responsibility-focused modules.
- [x] 1.2 Extract fingerprint, persisted state, dispatch, and validation ownership.
- [x] 1.3 Keep `agent_runtime::cache` public exports and all existing cache tests.

## 2. Session runtime
- [x] 2.1 Extract cache coordination and checkpoint transitions.
- [x] 2.2 Extract checkpoint recovery and recovered cache accounting/replay.
- [x] 2.3 Extract turn admission, execution, and lifecycle code while preserving `runtime::session` exports.

## 3. Provider identity types
- [x] 3.1 Move the cohesive cache-identity types, builder, custom deserializer, and validation helpers behind the existing provider root.
- [x] 3.2 Keep all provider conformance tests and current public type paths.

## 4. Compatibility and verification
- [x] 4.1 Run focused formatting and relevant crate checks/tests; confirm public re-exports, persisted shapes, event order, and existing conformance inventories.
