---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-07T00:00:00Z
---

# Tasks: SessionJournal and referenced checkpoints

All boxes are future work. Documents only are authorized now. Groups can merge
independently; release requires all applicable gates, not merely contract merge.

## 1. Merge group A — Contracts and dual readers (no native writer activation)

- [ ] 1.1 Approve design decisions and record pre-1.0 break/migration contract; define every DTO and exact/default trait method listed in design.md without removing old methods.
- [ ] 1.2 Freeze journal-json-1/SHA-256 domain/type/version fixtures, signed reasoning and negative-zero round trips; add bounded persistent sequence/map reference encoding.
- [ ] 1.3 Add schema-4 head/reference checkpoint readers, retain v3/unversioned snapshot readers for the stated release window, and validate transition revision 4 equivalence rather than assume it.
- [ ] 1.4 Preserve materialized public SessionSnapshot/TurnCheckpoint/TurnState, all exactness/redaction checks, and explicit unsupported capability errors; add unchanged legacy store implementation compile fixtures.
- [ ] 1.5 Add SnapshotJournal materialization adapter over separate policy stores; make its O(history) cost, single-writer requirement and lack of native CAS/GC explicit.

## 2. Merge group B — Native writer and migration (independent of U10)

- [ ] 2.1 Implement reference-counted-by-reachability roots, atomic commit/idempotency index, pinned reads and monotonic writer fences in neutral reference backends; reject different payload reuse and stale revisions.
- [ ] 2.2 Route all transition, acceptance, child, internal, local-action, cache, idle-compaction, fork/seed and ordinary-only hard-admission barriers through deltas; unchanged boundaries submit no old bodies.
- [ ] 2.3 Freeze final planned requests as ordered object references without replanning; deduplicate history, requests, usage prefixes, diagnostics and extension leaves, preserving opaque replacement costs.
- [ ] 2.4 Preserve distinct exact/projection domains and existing terminal/nonterminal overlay rules, U3 boundary counters, hard-admission exact-successor proof, U7 authority and U8 durable fork ordering.
- [ ] 2.5 Implement idempotent one-time v3 import and mixed native/legacy detection; cut crashes at each import write, including pending approval, CallingModel, model/tool results, cache phases and pending summary.
- [ ] 2.6 Exhaustively inject crash/ambiguous write outcomes between physical object, batch, protected head, ordinary projection and publication writes; test torn tails and committed corruption fail-closed repair.
- [ ] 2.7 Count serialized submission/new-object bytes at N=100/1000/10000 and fixed deltas; prove no unchanged-prefix writes and quantify changing metadata, opaque extensions, bootstrap and legacy-path costs.

## 3. Merge group C — Retirement and safe collection (retention stays host-owned)

- [ ] 3.1 Implement expected-revision retirement, durable migration/fork pins, pin renewal/close, retained-root closure and transactional root/pin epoch checks; no automatic expiry defaults.
- [ ] 3.2 Interleave mark/delete with read-only resume, writer resume, Summary/Empty/FromIndex fork, Continue ownership transfer, pending child and expired process lease; demonstrate no dangling reachable reference.
- [ ] 3.3 Reclaim orphan staging and superseded checkpoint/request/extension objects, then retired-session content; retain live canonical history and every committed source required by LCM summaries.
- [ ] 3.4 Prove unsupported collection retains content safely and failed repair cannot clear pending intent pins; distinguish backend compaction from logical GC.

## 4. Merge group D — Coordinated consumers and release eligibility

- [ ] 4.1 Extend consumer_open_forge with assert_chunked_journal_recovery over separate redacting/exact neutral chunk stores, actual Forge row-3.7 adapter/schema checks and shared-tag adoption.
- [ ] 4.2 Extend consumer_smith with assert_file_journal_migration and check all actual file/capsule checkpoint implementations, direct manifests reads, contributors, idle compaction and event matches.
- [ ] 4.3 Extend consumer_nyx with assert_ephemeral_journal_isolation and assert_optional_durable_journal; verify zero durable activity for ephemeral starts and immutable dependency pinning.
- [ ] 4.4 Run three existing named consumer targets, workspace/schema/conformance/doc checks, formatting, Clippy, dependency/license/neutrality and MSRV gates during implementation; a failing consumer blocks compatible release.
- [ ] 4.5 Publish migration/downgrade/retention limitations and actual byte counts in CHANGELOG, coordinate pre-1.0 break, and release only an immutable landed revision. Do not remove v3 readers or legacy methods in this change.
