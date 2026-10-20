---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-20T06:44:24Z
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

Measurement decision (1.4, 2026-10-19): B/N=4000 simulated S1–S5
measures 3.08750× L1 plain and 3.44375× L1 tool. The later Owner Decision
in design.md supersedes that stricter baseline: the gate is **≤2.0× L2**,
aiming at 1.5×. Simulated S1–S5 meets L2 (1.49× / 1.62×), so group 4
builds storage levers and group 5 stays gated out. Task 4.5 still rechecks
real storage after S5. Task 4.3 records measured S1–S4 and an explicitly
projected S5 result; S5 itself remains task 4.4. Task-1 raw JSON and evidence
remain in `/Volumes/Data/tmp/refactor/rt-e1/`.

## 2. Engine extraction (no behavior change; all existing suites unchanged)

- [x] 2.1 Move the private engine pieces out of `memory.rs` into
  `journal/engine/` modules (assembler, draft reader and staged closure,
  object links and closures, dependency roots, encoding cache, projection
  policy). Pure moves; public paths unchanged. Tests: core journal unit
  tests, testkit `journal` and `journal_gc` suites.
- [x] 2.2 Add `journal::storage` (`JournalStorage` and the D2 DTOs) and
  `VolatileJournalStorage` (copy-on-write object layers, records, operation
  records, pins, catalog, per-(domain, session) membership, `inject`
  handling, `dirty_sessions`). Engine not yet using it. Tests: storage unit
  tests for all-or-nothing `apply` at each `inject` point, `expect` fences,
  membership-restricted reads, deletion only at last membership across
  domains and sessions.
- [x] 2.3 Add `JournalEngine<S>` with the D3 registry and write gate; port
  `open`, `read`, `commit`, `renew`, `close` and `revoke_writer` to storage
  reads plus one `apply` per write, with the same root-epoch points. Make
  `InMemorySessionJournal` a newtype over `JournalEngine<VolatileJournalStorage>`.
  Tests: core journal tests, runtime `session_journal_equivalence`, import and
  crash matrices, testkit `assert_session_journal_reference_conformance`, a
  new test that `close` completes under `now_or_never` while another task
  holds the write gate.
- [x] 2.4 Port `retire`, `authorize_retirement`, `pin_transfer`,
  `retain_root`, `release_pin`, `stage_leaves` and `compact`, keeping the
  newtype's synchronous signatures (D3). Tests: every testkit collection suite
  and `journal_gc_runtime`, unchanged.
- [x] 2.5 Replace the domain-wide trace with session-scoped incremental
  collection (D4): `collection_dirty`, per-session live sets, membership
  release, catalog pruning. Tests: all collection suites unchanged; new tests
  for a shared object across two sessions and across both domains, a dirty
  session swept while ten clean sessions are never read (storage call count),
  and partial sweeps under `max_objects`.

Review follow-ups (N3/N1/N5): retire reads stored dependency/pin records,
including clean sources after restart, through candidate reverse references
persisted before a source's dirty flag clears; completed-fork children are
loaded directly. Domain diagnostics no longer repeatedly clone the registry.
Pins contain head/leaf roots and minimal dependency/epoch metadata; catalog
predecessors preserve pinned writer history without commit-time pin writes,
and controls write only changed pins. `dirty` is live after 2.5: collection
drains/restores local hints for I/O-free close/revoke and read-only opens, so
N5 requires retaining it. Regression and byte-count evidence is recorded in
`/Volumes/Data/tmp/refactor/rt-e5/report.md`.

## 3. Storage conformance

- [x] 3.1 Add testkit `conformance::journal_storage` with
  `assert_journal_storage_conformance(factory)`: the protected, ordinary and
  collection suites over `JournalEngine<S>`, the storage-level cases from 2.2,
  and a restart case (fresh engine, same storage, sees exactly acknowledged
  state). Add `JournalCollectionHost for JournalEngine<S>`. Run it on
  `VolatileJournalStorage`.
- [x] 3.2 Add the five broken storages (torn transaction, lost write,
  reordered commit, stale epoch, shared object deleted while referenced) and
  require each to fail a named case. Tests: one test per broken storage.

## 4. R7 storage-level levers (gate: after 1.4 and 3.1)

- [x] 4.1 Reference packed storage in the bench crate implementing
  `JournalStorage` over `rusqlite`: S1 one sealed zstd pack per write, a pack
  directory per session, per-domain membership bitmaps, S3 in-pack prefix
  index with id verification on read. Tests: `assert_journal_storage_conformance`.
- [x] 4.2 S2 session repack on collection (kind-ordered base pack, geometric
  merge of small packs) and the post-terminal trigger hook. Tests: repack
  preserves every member's bytes and membership bits; storage suite again.
- [x] 4.3 S4 packed operation records; run the harness end to end through
  the packed storage (A, B, C) and replace the 1.3 simulator numbers; delete
  the `journal-measurement` feature. Tests: report smoke at N=100.
- [x] 4.4 Only if the owner approves Q1: S5 replay window (prune in
  collection, inclusive current −64 floor plus current writer epoch's own last
  operation). Tests:
  `assert_journal_gc_replay_fence_conformance` unchanged; new window-edge case
  (revision −64 kept, −65 pruned and an identical replay conflicts without
  applying). CHANGELOG Breaking entry.

  Verified in step 5: both domains keep revision −64, prune −65, and reject
  an identical pruned replay without changing the stored head, membership or
  root epoch. The unchanged replay-fence and storage/collection conformance
  tests pass. Step 6 additionally verifies the packed adapter (repacking off,
  on, and connection restart), including transactional pruning of operation
  packs and preservation of the current writer epoch’s last operation. D8.6 records the explicit inclusive-edge/last-operation owner
  instruction; CHANGELOG has the owner-approved pre-1.0 Breaking entry.
- [x] 4.5 Gate: record native/L2 at N=4000 for B (and A, C). If B ≤ 2.0× and
  the owner did not require more (Q2), group 5 is skipped.

4.5 step-7 evidence (Q4-aligned source): B/N=4000 passes the required **≤2.0× L2** gate: collect-at-end = **1.60542× plain / 1.79118× tool**; per-turn = **1.60542× plain / 1.79118× tool**. All four miss the optional 1.5× aim. Group 5 stays unbuilt; journal schema 4 is unchanged.

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| B | 1000 | plain | 335872 | 577536 | 1.71951 | 845627 | 33297 |
| B | 1000 | tool | 339968 | 622592 | 1.83133 | 1491335 | 288771 |
| B | 4000 | plain | 1359872 | 2183168 | 1.60542 | 3057083 | 313815 |
| B | 4000 | tool | 1392640 | 2494464 | 1.79118 | 3540392 | 323123 |
| C | 1000 | tool | 942080 | 1204224 | 1.27826 | 144922993 | 368800 |

Cumulative per-turn B pack writes grow **3.615× plain / 2.374× tool** when seeded N grows 4× (1000 → 4000). C/1000 contributes 250 turn samples in the raw JSON. **Write verdict: O(N) in admitted bytes for these bounded turn shapes; no O(N²) whole-history pack rewrites.** Fixed 1 MiB packs bound merge tiers; stable large packs stay sealed. At a 25% dead threshold, survivor rewrites are charged to at most three live bytes per dead byte. Replay-window maintenance is bounded. This verdict concerns pack payload/nonce writes; the chosen metric excludes metadata, SQLite page overhead and VACUUM.

Supplemental C/N=4000, collection at the end (one release run):

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| C | 4000 | tool | 3977216 | 4886528 | 1.22863 | 487612606 | 9208323 |

Its native/L2 ratio is 1.22863×. It is reported without gating.
Raw JSON: `/Volumes/Data/tmp/refactor/rt-e7/measurements.json`.

4.5 step-8 repeat status: **C/N=4000 NOT MET / not run** because incremental
sizing was withdrawn after two runtime worker stack overflows. The step-7
C/N=4000 row remains historical evidence; no step-8 native/L2 value is inferred.
The remaining **18** release cases pass. All **4,032 size fields across
252 size records** equal step 7; only pack-write meters/samples change.
Current raw report: `rt-e8/measurements.json`; exhaustive comparison:
`rt-e8/physical-comparison.json`. Every final requested verification gate
passes (1,523 normal package tests, plus release reports and the future gate).
The retained C/N=1000 release CPU is 18.351893 ms/commit versus 17.771257
before; complete debug timing is NOT MET (900-second limits, epochs 120/111).

## 5. R7 format levers (gate: only if 4.5 fails or Q2 requires them)

- [ ] 5.1 (not built: measured 4.5 B/N=4000 passes ≤2.0× L2) Journal schema 5 readers for chunked JSON leaves (F1), with frozen
  golden fixtures; schema-4 readers unchanged. Tests: core fixture and
  round-trip tests at chunk boundaries (63/64/65, 8 KiB ±1).
- [ ] 5.2 (not built: measured 4.5 B/N=4000 passes ≤2.0× L2) F1 writer and engine assembler behind `JournalEngineConfig::format`;
  v4 heads keep v4 writes. Tests: cold-encoding oracle and equivalence
  harness in both formats; prefix-identical arrays share chunks.
- [ ] 5.3 (not built: measured 4.5 B/N=4000 passes ≤2.0× L2) F2 chunked history with virtual message ids for `history_chain`,
  and F3 minimum-height generic sequences. Tests: equivalence harness both
  formats; FromIndex fork cases; spine-sized histories.
- [ ] 5.4 (not built: measured 4.5 B/N=4000 passes ≤2.0× L2) Explicit v4→v5 upgrade operation (one counted full replacement);
  re-run the harness. Tests: upgrade of every task-2.6 checkpoint state
  materializes byte-identical schema-3 checkpoints.

## 6. R4–R6 (after 2.3; independent of groups 4–5)

- [x] 6.1 R4 validated-root memo (D5) replacing the writer-changed full
  validation. Tests: a new writer on an unchanged head performs zero full
  validations and zero object reads at open (counters); eviction falls back
  to one full validation; cold-encoding checks unchanged. Check
  `the_protected_fast_path_reseeds_after_a_writer_change` and
  `protected_commits_with_an_unreplayable_delta_fall_back_to_full_validation`
  (`memory.rs` tests) first: if either asserts that a writer change on a
  validated head forces full validation, report it as the one intended
  expectation change before editing it.
  Step-6 verification: core, runtime and testkit all-feature suites pass;
  packed adapter conformance and smoke pass. LRU key, 256 MiB default budget,
  collection-aware trimming and N2 zero-object-read counters are recorded in D5
  and `rt-e6/r4-core-final.log`. Neither named existing test required an
  assertion change.
- [x] 6.2 R5 native-first open (D6). Update the group-D tests that record
  legacy load counts to the lower counts and list each changed expectation
  in the task report. Tests: imported session reopened N times loads legacy
  zero times; partial-import repair still loads once.
  Step-6 verification: the new runtime reopen/repair regression and the full
  testkit suite pass. The sole existing assertion change is the shared consumer
  recovery load count, `loads + 2` → `loads` (all three consumer fixtures).
- [x] 6.3 R6 `JournalReader::extension` (D7). Tests: read count is
  head + batch + map height + 1 at 10 and 10,000 namespaces; Sensitive and
  missing namespaces; structured namespace rejected or materialized as
  documented.

  Step-6 verification: all 12 reader tests and the complete core all-feature
  suite pass. Opaque read counts are 4 / 6 at 10 / 10,000 namespaces with
  4,000 messages; map height is counted in visited levels (D7). No existing
  expectation changes and no storage trait changes.

## 7. Documentation and release notes

- [x] 7.1 Update `docs/development.md` (engine/storage split, storage
  conformance, harness commands, measured tables) and the CHANGELOG
  `Unreleased` entries from design D12. Record final verification evidence in
  `design.md`.

## 8. Q4 alignment and bounded rewrites (step 7 owner instruction)

- [x] 8.1 Declare additive Global/PerSession physical sharing; align D2,
  storage docs and release notes with Q4. Default Global preserves adapters.
- [x] 8.2 Allow bounded deferred deletion below a 25% dead-byte share; hide
  released memberships immediately and account at physical removal.
- [x] 8.3 Parameterize storage conformance by sharing; run volatile in both
  modes and reject premature shared deletion and serving dead bytes.
- [x] 8.4 Make packed admission/session indexes independent across sessions;
  cap packs at 1 MiB, bound base merges and enforce dead-byte threshold.
- [x] 8.5 Measure collect-at-end and per-turn collection; record final bytes,
  L2 ratio, cumulative pack writes and largest collection write in D8.4/4.5.

## 9. Memo review follow-ups (step 8 owner instruction)

- [x] 9.1 D-1 storage authority check once per memo-served read/materialization;
  cold fallback, current and historical repros, call counts, D5 decision.
- [x] 9.2 Drop unheld predecessor proofs on successful commits; session fairness
  regression; retain documented heap/charge calibration rather than claim an upper bound.
- [ ] 9.3 Constant-time LRU touches and incremental exact memo sizing; compare
  with full sizing and measure C/N=1000 commit CPU in debug/release.
- [x] 9.4 Base SessionJournal conformance reads the pinned head itself.
- [x] 9.5 S5 conformance covers AfterCommit RAM pruning, domain isolation and
  last-writer persistence after Reopen; run/revert all three mutations.
- [x] 9.6 Rewrite only changed operation packs; correct operation-pack comment.
- [ ] 9.7 Re-measure C/N=4000 in release within the remaining budget (90-minute
  maximum); record result or timeout epoch/time trend in D8.4 and 4.5.
- [x] 9.8 Full requested verification, every physical size compared with step 7,
  changed-expectation audit and final rt-e8 report.

Step-8 sizing status: 9.3 is partial. Constant-time LRU touches pass; the
incremental sizing attempt was withdrawn after two runtime worker stack
overflows. Full sizing is restored. The debug C/N=1000 baseline timed out at
900 seconds, writer epoch 120. Step 9.7's prerequisite (completed sizing/time
work) is therefore NOT MET; no new C/N=4000 claim supersedes step 7's measured
result. Detailed measurements, mutations and verification: `rt-e8/report.md`.

## 10. Control-generation fence (step 9 / review R3 owner instruction)

- [x] 10.1 Replace the two-field fence with revision/epoch/generation; every
  engine session write supplies a required record and advances generation.
- [x] 10.2 Check full fences and successor generations atomically in volatile
  and packed storage, including packed connection restart; add a named
  generation conformance case and a broken adapter that fails it.
- [x] 10.3 Preserve revision-mismatch conflict propagation without retry loops;
  prove shared load/apply atomicity with concurrent controls through clones.
- [x] 10.4 Prove all four two-engine R3 regressions fail with the original
  two-field fence and pass with the generation fence restored.
- [x] 10.5 Update D2/D5, development docs, existing Added changelog entry and
  begin_read wrapper documentation; run all requested gates, compare physical
  sizes with rt-e8 and record changed expectations and final rt-e9 evidence.
