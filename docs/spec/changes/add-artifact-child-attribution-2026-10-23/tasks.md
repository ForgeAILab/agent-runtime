---
created_at: 2026-10-23T22:20:00Z
updated_at: 2026-10-23T22:20:00Z
completed_at:
---

## 1. Implementation

- [x] 1.1 Optional `ArtifactProvenance::child` with `with_child`, skipped when
  absent.
- [x] 1.2 Attribute stored child replies; tag the reply idempotency key `v2`.
- [x] 1.3 Accept the field in the semantic-summary wire mirror.

## 2. Tests

- [x] 2.1 Provenance without a child round-trips and omits the field.
- [x] 2.2 Conformance: a long child reply is stored for the parent and names
  its child.

## 3. Verification

- [x] 3.1 Runtime gate: fmt, Clippy `--all-features -D warnings`,
  `cargo test --workspace --all-features`.
- [ ] 3.2 Smith pins the change and lists replies by child.
