---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
completed_at:
---

# Tasks: Share history and reduce repeated accounting/test work

## 1. Private History

- [ ] 1.1 Record public field/signature/serde baselines and optional copy/allocation measurements; preserve existing history and with_history surfaces.
- [ ] 1.2 Share private immutable generations through planning/projector/staging phases; materialize owned Message values only where current public contracts require them.
- [ ] 1.3 Prove held snapshots/views/frozen requests and signed continuation remain immutable across append, compaction, and recovery.

## 2. LCM Accounting

- [ ] 2.1 Implement a process-local authorized/revision-bound sidecar with checked per-entry totals and no persisted schema/descriptor revision change.
- [ ] 2.2 Use existing range APIs for append deltas and committed summary accounting; preserve strict projection/source checks, revocation, CAS, and partial-commit recovery.
- [ ] 2.3 Make append-aware fingerprint output byte-identical to main; test all content parts/escaping and invalidate on untrusted/replaced/truncated generations.
- [ ] 2.4 Add counting store/sizer oracle fixtures for unchanged/append/compaction/cold resume, overflow, changed revisions and grants; distinguish accounting from other O(history) work.

## 3. Test Organization and Consumer Seams

- [ ] 3.1 Inventory all 20 runtime and seven testkit integration roots and qualified cases before moving; consolidate runtime to one and testkit conformance to one while retaining its three consumer target names.
- [ ] 3.2 Verify scenario counts, feature/ignored coverage, isolation, deterministic fixtures and existing consumer CI commands after consolidation.
- [ ] 3.3 Add/reuse the Smith contributor/store/held-view, Forge protected-LCM/counting-authority, and Nyx ephemeral-seeded/held-history fixtures listed in proposal.md without consumer dependencies.

## 4. Gates

- [ ] 4.1 Compare public Rust/serde/revision/fingerprint baselines and execution events with current main or the separately landed U1/U2/U3 contracts.
- [ ] 4.2 Pass all actual consumers and named testkit targets, workspace/schema/doc tests, formatting, all-feature Clippy, dependency/license and declared MSRV checks.
- [ ] 4.3 Report remaining copies and optional allocation/link measurements; publish only after the compatible-release gate passes and immutable pins are available.
