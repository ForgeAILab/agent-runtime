---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-03T04:57:19Z
completed_at:
---

# Tasks: Share history and reduce repeated accounting/test work

## 1. Private History

- [x] 1.1 Record public field/signature/serde baselines and optional copy/allocation measurements; preserve existing history and with_history surfaces.
- [x] 1.2 Share private immutable generations through planning/projector/staging phases; materialize owned Message values only where current public contracts require them.
- [x] 1.3 Prove held snapshots/views/frozen requests and signed continuation remain immutable across append, compaction, and recovery.

## 2. LCM Accounting

- [x] 2.1 Implement a process-local authorized/revision-bound sidecar with checked per-entry totals and no persisted schema/descriptor revision change.
- [x] 2.2 Use existing range APIs for append deltas and committed summary accounting; preserve strict projection/source checks, revocation, CAS, and partial-commit recovery.
- [x] 2.3 Make append-aware fingerprint output byte-identical to main; test all content parts/escaping and invalidate on untrusted/replaced/truncated generations.
- [x] 2.4 Add counting store/sizer oracle fixtures for unchanged/append/compaction/cold resume, overflow, changed revisions and grants; distinguish accounting from other O(history) work.

## 3. Test Organization and Consumer Seams

- [ ] 3.1 Inventory all 20 runtime and seven testkit integration roots and qualified cases before moving; consolidate runtime to one and testkit conformance to one while retaining its three consumer target names.
- [ ] 3.2 Verify scenario counts, feature/ignored coverage, isolation, deterministic fixtures and existing consumer CI commands after consolidation.
- [x] 3.3 Add/reuse the Smith contributor/store/held-view, Forge protected-LCM/counting-authority, and Nyx ephemeral-seeded/held-history fixtures listed in proposal.md without consumer dependencies.

## 4. Gates

- [x] 4.1 Compare public Rust/serde/revision/fingerprint baselines and execution events with current main or the separately landed U1/U2/U3 contracts.
- [ ] 4.2 Pass all actual consumers and named testkit targets, workspace/schema/doc tests, formatting, all-feature Clippy, dependency/license and declared MSRV checks.
- [ ] 4.3 Report remaining copies and optional allocation/link measurements; publish only after the compatible-release gate passes and immutable pins are available.

## Scoped implementation evidence

This job implements groups 1/2, the history/LCM portions of 3.3, and 4.1 on
`11ca2bd` (the separately landed U1/U3 contracts). All 20 runtime and seven
testkit integration roots are retained; 3.1/3.2 remain deferred until U2 merges.
No provider wire extraction or Tool/registry/executor/cache-plan diagnostic
changes are included.

- Existing public Vec/Arc fields, `history()`/`with_history()`, core message,
  provider, store, event and checkpoint files, LCM persisted fields, descriptor
  calculation and strict revision decoder match the baseline. Incremental
  fingerprints match the original JSON-array/FNV path for every content part,
  escaping, empty arrays, signed reasoning, reordered/truncated/replaced history,
  and floating signed-zero arguments. No schema or revision was bumped.
- Public `SessionState.history: Vec<Message>` is retained. Private captures
  reuse one contiguous immutable Arc per generation; changed generations still
  materialize Message values once and verify their exact prefix. Public owned
  snapshots/history, context fragments, provider messages, CallingModel events,
  checkpoint serialization and pinned generations still require copies.
- The sidecar is process-local, opaque-grant and component/DAG revision-bound,
  uses checked per-entry prefix totals and cached active-node coverage/totals,
  and is removed when its session generation owner drops. Only exact local
  append/CAS successors advance reusable revision evidence. Every accounting
  lookup reauthorizes and fences revisions; externally supplied slices remain
  cold. Strict projection/source validation, protected pending-summary commits
  and startup/recovery rules remain authoritative.
- Counting fixtures compare unchanged/append/paged append/leaf/condensation/cache loss/
  cold resume against the independent original full-recompute oracle, with
  overflow, changed binding/store/sizer/classifier/guard/DAG evidence, forged
  grants and revocation. Consumer fixtures retain the named CI targets and
  reuse U1/U3's event/protected-boundary checks. Smith's cold recovery follows a
  completed post-compaction turn, retaining the existing idle-usage versus
  terminal-checkpoint fence.
- Measurement for four private captures of 256 messages with 1 KiB text bodies:
  baseline 1,024 Message copies, 2,055 allocation/reallocation calls and
  1,196,080 requested bytes; shared generation 256 copies, 515 calls and
  292,976 bytes; already materialized generation 0 copies/allocations/bytes.
  This isolates private captures, not full turn cost, peak/live memory, provider
  planning or checkpoint copies. The small counting-allocator harness and gate
  logs were kept outside the repository; no link-speed claim is made.

Task 4.3's measurement/report portion is recorded above; its compatible-release
publication/pin requirement and 4.2's actual external-consumer release gates
remain outstanding. This job leaves changes uncommitted and does not publish.

Final local gates: focused history 3/3, accounting 8/8, consumer seam additions
3/3, frozen-request regression 1/1, LCM hooks 28/28, history/recovery 43/43,
core/context/LCM schema suites 387/387, and named Smith/Nyx/Forge targets 6/6,
5/5 and 4/4. Workspace all-feature tests passed 1,455 cases and workspace doc
checks passed 7. Formatting and workspace all-target/all-feature Clippy exited
0, with no base-lint exceptions. All-feature production builds passed on Rust
1.86 and MCP/CLI builds on Rust 1.88. The strict spec validator passed.

The full offline `cargo deny check` exited 1 because `windows v0.61.3` is not in
the local cache; a native advisory check also cannot acquire the read-only
advisory database lock. Native all-feature bans/licenses/sources checks passed
using filtered local metadata. No packages or advisory data were downloaded.
Actual external consumer compile/contract gates, the full dependency/advisory
gate and release pins/publication are still required for a compatible release.
