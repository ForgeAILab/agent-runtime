---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
completed_at:
---

# Tasks: Cache diagnostics and argument normalization

## 1. Prefix Diagnostics

- [ ] 1.1 Add optional plan/event diagnostics without a core-to-context dependency or fingerprint/wire changes.
- [ ] 1.2 Compare committed predecessor IDs/hashes/classes; cover insertion, deletion, reorder, stable-class change, unchanged prefix, appended/promoted tail, partition-only/mixed changes, and suppressed/unsupported baselines.
- [ ] 1.3 Add legacy plan/event fixtures and unsafe-ID omission/redaction tests.

## 2. Tool Seam

- [ ] 2.1 Add the pure identity-default object-safe hook and document host ownership and bounded synchronous work.
- [ ] 2.2 Use normalize-before-full-validation in both executor paths and approval edits; reuse checkpointed prepared actions without normalization on resume.
- [ ] 2.3 Cache exact frozen-schema validators privately in runtime; preserve registration failure, sealed ordering, clone behavior, and ability/registry dependency boundaries.
- [ ] 2.4 Cover hook failure, invalid normalized output, identity rejection, a legitimate parameters property, edited authority, deadlines/cancellation, and crash recovery without repeated side effects.

## 3. Gates

- [ ] 3.1 Implement the Smith contributor/approval-edit, Forge opt-in canonical-schema/policy-denial, and Nyx identity/seeded-history fixtures listed in proposal.md.
- [ ] 3.2 Compile all consumers and coordinate field-exact cache event/plan constructors; keep exhaustive variant matches unchanged.
- [ ] 3.3 Run cache/schema/prepared-tool tests, all named consumer gates, workspace/doc tests, formatting, all-feature Clippy, dependency/license and MSRV gates; document immutable release pins.
