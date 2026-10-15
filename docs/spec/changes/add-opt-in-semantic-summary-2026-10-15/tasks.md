---
created_at: 2026-10-15T22:46:05Z
updated_at: 2026-10-15T22:46:05Z
completed_at:
---

# Tasks: Opt-in flat semantic summary beside LCM

## 1. Coordinator
- [ ] 1.1 Restore `harness/semantic_summary.rs` from `b037801` with its unit
      tests, and export the pin line's public names from `harness/mod.rs`.
- [ ] 1.2 Adapt it to `main`: `SummaryProvenance { lossless: None, .. }`, and
      `after_idle_compaction` returning `IdleCompactionResult`.
- [ ] 1.3 Share the schema-v1 state definition with
      `legacy_semantic_summary.rs` (D5).
- [ ] 1.4 Re-add `HarnessEvent::SemanticSummaryFallback` and its `Downgrade`
      mapping.

## 2. Composition
- [ ] 2.1 Add `RuntimeBuilder::semantic_summary(Arc<SemanticSummaryCoordinator>)`.
- [ ] 2.2 Make `build` reject LCM together with a flat summary (by method or
      component id), and the flat summary with `soft_on_turn_boundary(true)`.

## 3. Session seams
- [ ] 3.1 Add `SessionHandle::try_idle_semantic_compaction()` and accept the
      configured strategy's idle usage in the driver.
- [ ] 3.2 Re-add `SessionHandle::restore_semantic_summary_if_absent`,
      available only when the flat summary is configured.
- [ ] 3.3 Resume and fork: when the flat summary is configured, keep the
      namespace as live state, matching the pin line's fork behavior. LCM
      and unconfigured paths stay unchanged.

## 4. Tests
- [ ] 4.1 Build-exclusivity tests (method, component id,
      `soft_on_turn_boundary`).
- [ ] 4.2 Resume and fork for each of: flat, LCM, neither, both namespaces.
- [ ] 4.3 LCM imports state the restored coordinator wrote.
- [ ] 4.4 Idle compaction returns the protected summary and records
      `SemanticSummary` usage. The restore seam's fail-closed cases.
- [ ] 4.5 Restore the coordinator-backed recovery conformance case.

## 5. Docs and release
- [ ] 5.1 `CHANGELOG.md` Unreleased; scope the "no summary body crosses the
      facade" rule to LCM in `CHANGELOG.md` and `docs/migration-0.1.md`.
- [ ] 5.2 Gate: fmt, clippy `-D warnings`, `cargo test --workspace
      --all-features`, the feature combinations, Rust 1.86, consumer-compat.
- [ ] 5.3 Before tagging, build Smith against this branch through a local
      override and run its semantic-summary and cache-controller suites.
- [ ] 5.4 Release v0.4.0.
