---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T18:26:00Z
---

# Tasks: Storage-generic journal engine and physical disk reduction

Each task is one Codex job of at most ~3 hours. Run only the focused tests
named in the task plus the existing suites it lists; full suites run in CI.
"Gate" marks tasks that wait for measured results; all others can start once
their predecessor lands. Groups 1 and 2 have no dependency on each other and
may run in parallel.

## 1. Measurement harness (first; no lever is built before 1.4)

- [x] 1.1 Add the non-default core feature `journal-measurement` with
  `InMemorySessionJournal::measurement_export()` (retained objects with kind
  and bytes, current heads per session, operation-record count). No default
  API change. Test: a two-commit protected and ordinary session exports
  exactly `object_count()` objects totaling `object_bytes()`.
- [x] 1.2 Create `crates/agent-runtime-journal-bench` (`publish = false`;
  core, runtime, testkit, `rusqlite` 0.32 `bundled`, `zstd`). Implement
  fixtures A (task-2.7 shape, plain and tool), B (same shape, seeded realistic
  content generator) and C (B grown by turns to N=1000), and the legacy layout
  with Forge's row shape and save rules (`FORGE-SESSION\0`, zstd 3, 40-byte
  seal model, checkpoint NULLs snapshot, digest skip). Measure
  `page_count × page_size` after `wal_checkpoint(TRUNCATE)` + `VACUUM` minus
  the empty-schema file. Report whether the idle row holds one or two blobs.
  Test: non-ignored N=100 smoke for A and B.
- [x] 1.3 Add layout simulators from the exported objects: Forge D4 per-object
  rows (its DDL), S1 commit packs, S1+S2 session repack with cross-domain
  sharing, S1+S2+S3 in-pack compact index. Per-kind breakdown (count,
  logical, compressed, row/index overhead), pre- and post-collection bytes,
  operation-record bytes per commit. Explain the ~4,000 extra objects per
  retained manifest seen at N=4000 (object kinds of each manifest's closure).
  Test: simulator byte totals for N=100 equal a direct sum of the rows it
  wrote (model validated against the real SQLite file).
- [x] 1.4 Run the `#[ignore]` release report for N=100/1000/4000 (A, B) and
  N=1000 (C). Record the tables in `design.md` D8.4 (replacing the estimates)
  and the raw JSON under `/Volumes/Data/tmp/refactor/`. Apply the D8.5
  decision rule and record which levers groups 4–5 build.

Measurement decision (1.4, 2026-10-19): B/N=4000 S1–S5 measures
3.08750× L1 plain and 3.44375× tool, above the 2.0× gate.
Group 4 builds S1–S4 plus owner-approved S5; task 4.5 still rechecks the real
storage. Group 5 is gated in on current evidence: F1, re-measure, then F2,
with F3 accompanying schema 5. A does not gate; the 1.5× aim adds no mandatory
scope. Idle legacy fixtures use L2, while the comparison remains the stricter
L1. Tables and scope limits are in design D8.4; raw JSON and verification logs
are in `/Volumes/Data/tmp/refactor/rt-e1/`. No group-2–7 work is implemented by
this measurement job.

## 2. Engine extraction (no behavior change; all existing suites unchanged)

- [ ] 2.1 Move the private engine pieces out of `memory.rs` into
  `journal/engine/` modules (assembler, draft reader and staged closure,
  object links and closures, dependency roots, encoding cache, projection
  policy). Pure moves; public paths unchanged. Tests: core journal unit
  tests, testkit `journal` and `journal_gc` suites.
- [ ] 2.2 Add `journal::storage` (`JournalStorage` and the D2 DTOs) and
  `VolatileJournalStorage` (copy-on-write object layers, records, operation
  records, pins, catalog, per-(domain, session) membership, `inject`
  handling, `dirty_sessions`). Engine not yet using it. Tests: storage unit
  tests for all-or-nothing `apply` at each `inject` point, `expect` fences,
  membership-restricted reads, deletion only at last membership across
  domains and sessions.
- [ ] 2.3 Add `JournalEngine<S>` with the D3 registry and write gate; port
  `open`, `read`, `commit`, `renew`, `close` and `revoke_writer` to storage
  reads plus one `apply` per write, with the same root-epoch points. Make
  `InMemorySessionJournal` a newtype over `JournalEngine<VolatileJournalStorage>`.
  Tests: core journal tests, runtime `session_journal_equivalence`, import and
  crash matrices, testkit `assert_session_journal_reference_conformance`, a
  new test that `close` completes under `now_or_never` while another task
  holds the write gate.
- [ ] 2.4 Port `retire`, `authorize_retirement`, `pin_transfer`,
  `retain_root`, `release_pin`, `stage_leaves` and `compact`, keeping the
  newtype's synchronous signatures (D3). Tests: every testkit collection suite
  and `journal_gc_runtime`, unchanged.
- [ ] 2.5 Replace the domain-wide trace with session-scoped incremental
  collection (D4): `collection_dirty`, per-session live sets, membership
  release, catalog pruning. Tests: all collection suites unchanged; new tests
  for a shared object across two sessions and across both domains, a dirty
  session swept while ten clean sessions are never read (storage call count),
  and partial sweeps under `max_objects`.

## 3. Storage conformance

- [ ] 3.1 Add testkit `conformance::journal_storage` with
  `assert_journal_storage_conformance(factory)`: the protected, ordinary and
  collection suites over `JournalEngine<S>`, the storage-level cases from 2.2,
  and a restart case (fresh engine, same storage, sees exactly acknowledged
  state). Add `JournalCollectionHost for JournalEngine<S>`. Run it on
  `VolatileJournalStorage`.
- [ ] 3.2 Add the five broken storages (torn transaction, lost write,
  reordered commit, stale epoch, shared object deleted while referenced) and
  require each to fail a named case. Tests: one test per broken storage.

## 4. R7 storage-level levers (gate: after 1.4 and 3.1)

- [ ] 4.1 Reference packed storage in the bench crate implementing
  `JournalStorage` over `rusqlite`: S1 one sealed zstd pack per write, a pack
  directory per session, per-domain membership bitmaps, S3 in-pack prefix
  index with id verification on read. Tests: `assert_journal_storage_conformance`.
- [ ] 4.2 S2 session repack on collection (kind-ordered base pack, geometric
  merge of small packs) and the post-terminal trigger hook. Tests: repack
  preserves every member's bytes and membership bits; storage suite again.
- [ ] 4.3 S4 packed operation records; run the harness end to end through
  the packed storage (A, B, C) and replace the 1.3 simulator numbers; delete
  the `journal-measurement` feature. Tests: report smoke at N=100.
- [ ] 4.4 Only if the owner approves Q1: S5 replay window (prune in
  collection, latest 64 revisions plus current writer epoch). Tests:
  `assert_journal_gc_replay_fence_conformance` unchanged; new window-edge case
  (revision −64 kept, −65 pruned and an identical replay conflicts without
  applying). CHANGELOG Breaking entry.
- [ ] 4.5 Gate: record native/L1 at N=4000 for B (and A, C). If B ≤ 2.0× and
  the owner did not require more (Q2), group 5 is skipped.

## 5. R7 format levers (gate: only if 4.5 fails or Q2 requires them)

- [ ] 5.1 Journal schema 5 readers for chunked JSON leaves (F1), with frozen
  golden fixtures; schema-4 readers unchanged. Tests: core fixture and
  round-trip tests at chunk boundaries (63/64/65, 8 KiB ±1).
- [ ] 5.2 F1 writer and engine assembler behind `JournalEngineConfig::format`;
  v4 heads keep v4 writes. Tests: cold-encoding oracle and equivalence
  harness in both formats; prefix-identical arrays share chunks.
- [ ] 5.3 F2 chunked history with virtual message ids for `history_chain`,
  and F3 minimum-height generic sequences. Tests: equivalence harness both
  formats; FromIndex fork cases; spine-sized histories.
- [ ] 5.4 Explicit v4→v5 upgrade operation (one counted full replacement);
  re-run the harness. Tests: upgrade of every task-2.6 checkpoint state
  materializes byte-identical schema-3 checkpoints.

## 6. R4–R6 (after 2.3; independent of groups 4–5)

- [ ] 6.1 R4 validated-root memo (D5) replacing the writer-changed full
  validation. Tests: a new writer on an unchanged head performs zero full
  validations and zero object reads at open (counters); eviction falls back
  to one full validation; cold-encoding checks unchanged. Check
  `the_protected_fast_path_reseeds_after_a_writer_change` and
  `protected_commits_with_an_unreplayable_delta_fall_back_to_full_validation`
  (`memory.rs` tests) first: if either asserts that a writer change on a
  validated head forces full validation, report it as the one intended
  expectation change before editing it.
- [ ] 6.2 R5 native-first open (D6). Update the group-D tests that record
  legacy load counts to the lower counts and list each changed expectation
  in the task report. Tests: imported session reopened N times loads legacy
  zero times; partial-import repair still loads once.
- [ ] 6.3 R6 `JournalReader::extension` (D7). Tests: read count is
  head + batch + map height + 1 at 10 and 10,000 namespaces; Sensitive and
  missing namespaces; structured namespace rejected or materialized as
  documented.

## 7. Documentation and release notes

- [ ] 7.1 Update `docs/development.md` (engine/storage split, storage
  conformance, harness commands, measured tables) and the CHANGELOG
  `Unreleased` entries from design D12. Record final verification evidence in
  `design.md`.
