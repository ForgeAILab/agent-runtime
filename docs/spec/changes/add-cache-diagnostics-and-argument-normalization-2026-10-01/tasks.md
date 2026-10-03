---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-03T05:00:57Z
completed_at: 2026-10-03T05:00:57Z
---

# Tasks: Cache diagnostics and argument normalization

## 1. Prefix Diagnostics

- [x] 1.1 Add optional plan/event diagnostics without a core-to-context dependency or fingerprint/wire changes.
- [x] 1.2 Compare committed predecessor IDs/hashes/classes; cover insertion, deletion, reorder, stable-class change, unchanged prefix, appended/promoted tail, partition-only/mixed changes, and suppressed/unsupported baselines.
- [x] 1.3 Add legacy plan/event fixtures and unsafe-ID omission/redaction tests.

## 2. Tool Seam

- [x] 2.1 Add the pure identity-default object-safe hook and document host ownership and bounded synchronous work.
- [x] 2.2 Use normalize-before-full-validation in both executor paths and approval edits; reuse checkpointed prepared actions without normalization on resume.
- [x] 2.3 Cache exact frozen-schema validators privately in runtime; preserve registration failure, sealed ordering, clone behavior, and ability/registry dependency boundaries.
- [x] 2.4 Cover hook failure, invalid normalized output, identity rejection, a legitimate parameters property, edited authority, deadlines/cancellation, and crash recovery without repeated side effects.

## 3. Gates

- [x] 3.1 Implement the Smith contributor/approval-edit, Forge opt-in canonical-schema/policy-denial, and Nyx identity/seeded-history fixtures listed in proposal.md.
- [x] 3.2 Compile the workspace and all three named testkit consumer targets; document exact field additions and constructor/destructuring updates; keep exhaustive variant matches unchanged.
  - Completed using the implementation brief's local compile scope. implementation-report.md lists both exact field additions and the required constructor/pattern edits. Actual product-tree builds remain required external release gates.
- [x] 3.3 Run cache/schema/prepared-tool tests, all named consumer gates, workspace/doc tests, formatting, all-feature Clippy, dependency/license and MSRV gates; document immutable release pins.

Local implementation and requested gates are complete. All compilation, focused/consumer/workspace/doc tests, formatting, and Clippy pass. Native-target bans/licenses/sources pass; full multi-target audit is blocked by uncached windows v0.61.3 and the advisory audit by a read-only database lock. Actual product builds and those audits remain release follow-ups; see implementation-report.md.
