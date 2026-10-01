---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T21:32:43Z
completed_at:
---

# Tasks: Bound session manifests

## 1. Window and Reads

- [ ] 1.1 Approve finite retention/default and consumer archival decisions; add the positive window builder setting and recent_manifests accessor without changing public vector/store types.
  - Setting/default/accessor implemented; actual consumer archival decisions require consumer owners outside this repository.
- [x] 1.2 Bound append, validated live resume, child/internal records, and ordinary snapshot writes; preserve raw legacy deserialization.
- [x] 1.3 Add a constant-size RedactionSafe planned-step boundary record in existing extension state; bootstrap legacy counts from full lists before pruning and reject overflow/malformed evidence.

## 2. Protected Recovery

- [x] 2.1 Exclude manifests from every newly produced checkpoint snapshot; preserve exact execution state and old checkpoint records on duplicate saves.
- [x] 2.2 Replace diagnostic-list equality with validated planned-step boundary equality while preserving identity/history/usage/revision/security checks; handle legacy/mixed pairs before pruning and keep diagnostics separate from nonterminal execution authority.
- [x] 2.3 Normalize same-state refresh comparisons without artificial revisions; verify cache/child/internal recovery paths and checkpoint-only recovery.
- [x] 2.4 Keep historical manifest/revision unavailability explicit for equivalent replay; document recent-window reads and old-binary downgrade limitations.

## 3. Proof and Gates

- [x] 3.1 Add absent/unbounded legacy snapshot and checkpoint JSON fixtures, retained-window serialization tests, and duplicate-save/same-state-refresh tests in testkit.
- [x] 3.2 Add Smith file-backed/redacting-versus-exact stores, Forge protected-store/LCM crash boundaries, and Nyx storeless over-window cases listed in proposal.md.
- [x] 3.3 Prove checkpoint schema/transition revisions remain executable equivalently; test differing planned-step boundaries, repeated migration restarts, missing/malformed markers, mismatched usage, incompatible extensions, and unsupported revision rejection.
- [ ] 3.4 Pass all supported consumer retention/source gates plus workspace/schema/doc, formatting, all-feature Clippy, dependency/license, and MSRV checks before an immutable compatible release.
  - Local gates passed except the three socket-dependent bridge tests and full multi-target/advisory audits; actual product gates and release eligibility remain external (see implementation-report.md).

## 4. Audit Revisions

- [x] 4.1 Repair a boundary record under-count exposed by a longer legacy list, accept future additive record fields, and export the existing namespace constant.
- [x] 4.2 Carry lagging ordinary diagnostics through manifest-free nonterminal recovery without letting an incompatible diagnostic pair fail protected execution recovery.
- [x] 4.3 Avoid redundant checkpoint/manifest clones, document rollback and first-resume retention limits, and cover rollback, K/K+1, and lagging-crash regressions.
