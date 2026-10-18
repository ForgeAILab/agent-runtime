---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-18T00:00:00Z
---

# Tasks: SessionJournal and referenced checkpoints

The owner approved merge group A only on 2026-10-08 (local date). Groups B, C,
D and U10 remain unapproved. Groups can merge independently; release requires
all applicable gates, not merely contract merge.

## 1. Merge group A — Contracts and dual readers (no native writer activation)

- [x] 1.1 Approve design decisions and record pre-1.0 break/migration contract; define every DTO and exact/default trait method listed in design.md without removing old methods.
- [x] 1.2 Freeze journal-json-1/SHA-256 domain/type/version fixtures, signed reasoning and negative-zero round trips; add bounded persistent sequence/map reference encoding.
- [x] 1.3 Add schema-4 head/reference checkpoint readers, retain v3/unversioned snapshot readers for the stated release window, and validate transition revision 4 equivalence rather than assume it. Materialized writes stay at schema 3 with unchanged bytes (review fix, 2026-10-08).
- [x] 1.4 Preserve materialized public SessionSnapshot/TurnCheckpoint/TurnState, all exactness/redaction checks, and explicit unsupported capability errors; add unchanged legacy store implementation compile fixtures.
- [x] 1.5 Add SnapshotJournal materialization adapter over separate policy stores; make its O(history) cost, single-writer requirement and lack of native CAS/GC explicit.

## 2. Merge group B — Native writer and migration (independent of U10)

- [x] 2.1 Implement reference-counted-by-reachability roots, atomic commit/idempotency index, pinned reads and monotonic writer fences in neutral reference backends; reject different payload reuse and stale revisions.
  Evidence: the reusable protected (`assert_session_journal_conformance`) and ordinary (`assert_ordinary_session_journal_conformance`) suites pass against `InMemorySessionJournal`, covering every injected commit cut, replay, pin isolation, fence takeover via host revocation, closure, materialization and revision case, and reject nine deliberately broken backends (review fix, 2026-10-18).
- [x] 2.2 Route all transition, acceptance, child, internal, local-action, cache, idle-compaction, fork/seed and ordinary-only hard-admission barriers through deltas; unchanged boundaries submit no old bodies.
  Evidence: native barrier/equivalence suites cover plain/tool/approval/local/internal/cache/child/idle/fork/hard-admission paths and unchanged ordinary commits submit 0 objects/0 bytes. Review (2026-10-18): `session_journal_equivalence` compares every legacy save with every published native head read back through `JournalReader`, then resumes from a fresh runtime and continues (plain, redacted, tools/local, internal, provider error, cancellation, crash at AwaitingApproval and at CallingModel (no replan, no replay), cache phases, fork Summary/Empty/FromIndex of a non-live parent, two-store idle compaction, lost acknowledgements, lost writer fence, ordinary-only hard-admission crash cuts, child rebind after restart); dropping a barrier, dropping one extension delta or reordering `history_append` each fails 9-11 of them. It found and fixed three native-only wedges: redacting hosts failed every turn after a terminal resume, dropped handles kept writer fences (fork of an idle parent, `abort_fork`), and sessions persisted before their first checkpoint could not resume.
- [x] 2.3 Freeze final planned requests as ordered object references without replanning; deduplicate history, requests, usage prefixes, diagnostics and extension leaves, preserving opaque replacement costs.
  Evidence: `native_delta_submission_is_independent_of_unchanged_history_bodies` proves referenced request/history bodies stay constant at N=100/1000 while changed leaves retain replacement bytes. Review measurement (`review_one_message_turn_submissions_do_not_grow_with_history`, N=100/1000/4000): Accepted (36 objects, 143 new body bytes, ~9.8-10.4 KB) and CallingModel (6 objects/1.7 KB; 39 objects/11 KB with a system prompt) commits are constant and unchanged boundaries submit 0 objects, but a whole one-message turn is still O(N): the runtime-owned `runtime.core.previous_cache` plan record (~113 bytes per history message) is replaced at every ModelResponseReady and ordinary terminal save, and the turn manifest is likewise O(N); ordinary commits carry each changed leaf twice (draft object and delta). At N=4000 one turn submits ~465 KB protected and ~1.9 MB ordinary. N=10000 is not measurable here: legacy and native both end that turn Failed before provider I/O. Task 2.7 must quantify and decide these costs.
- [x] 2.4 Preserve distinct exact/projection domains and existing terminal/nonterminal overlay rules, U3 boundary counters, hard-admission exact-successor proof, U7 authority and U8 durable fork ordering.
  Evidence: redacted terminal overlay, native hard-admission/idle recovery, and Summary/Empty/FromIndex fork equivalence tests pass with existing transition validation unchanged. Review: exact checkpoints after a terminal resume carry the host-redacted canonical prefix exactly as legacy does (protected `history_append` is advisory; backends verify roots); a rewritten history is never expressed as an append to the ordinary projection; an ordinary head published after an exact checkpoint still requires its protected head.
- [ ] 2.5 Implement idempotent one-time v3 import and mixed native/legacy detection; cut crashes at each import write, including pending approval, CallingModel, model/tool results, cache phases and pending summary.
- [ ] 2.6 Exhaustively inject crash/ambiguous write outcomes between physical object, batch, protected head, ordinary projection and publication writes; test torn tails and committed corruption fail-closed repair.
- [x] 2.7 Count serialized submission/new-object bytes at N=100/1000/4000 (and 10000 when the cache-identity limit permits) and fixed deltas; prove no unchanged-prefix writes and quantify changing metadata, opaque extensions, bootstrap and legacy-path costs.
  Evidence (implementation measurement, 2026-10-18; exact bytes, each cell is
  `serialized submission / new immutable-object bytes`): the pre-change
  serialized totals are reconstructed from the 2.2–2.4 review's component
  columns; that review did not count backend new-object bytes. A "turn" is the
  complete second turn after one bootstrap turn.

  | Path / boundary | N=100 | N=1000 | N=4000 |
  | --- | ---: | ---: | ---: |
  | Before: plain protected turn | 61,513 / n/a | 264,356 / n/a | 941,480 / n/a |
  | After: plain protected turn | 52,327 / 58,337 | 53,871 / 59,895 | 52,998 / 59,022 |
  | Before: plain ordinary terminal | 70,827 / n/a | 498,042 / n/a | 1,931,604 / n/a |
  | After: plain ordinary terminal | 29,620 / 34,177 | 30,259 / 36,430 | 30,265 / 40,412 |
  | After: tool protected turn | 127,974 / 139,853 | 131,154 / 143,061 | 129,408 / 141,315 |
  | After: tool ordinary terminal | 46,492 / 51,328 | 47,341 / 53,791 | 47,350 / 57,776 |
  | After: one unchanged ordinary boundary | 1,183 / 2,011 | 1,187 / 2,017 | 1,187 / 2,017 |

  The unchanged boundary submits 0 drafts, 0 draft bytes and 0 body bytes; its
  nonzero serialized/new-object totals are the revisioned delta/batch/head
  metadata measured here. Every second-turn commit submitted at most 512 bytes
  of new Message/Usage bodies, independent of N. The changed cache record's
  `ModelResponseReady` commit is 18,343/19,137 bytes at N=100,
  18,669/19,465 at N=1000 and 18,672/19,468 at N=4000, replacing the review's
  13,362/114,171/453,171 draft bytes. The ordinary terminal contains one cache
  reference and one (plain) or two (tool) manifest references; its only opaque
  replacement is `runtime.manifest_boundary`, 140 serialized delta bytes at
  every N. Other opaque namespaces retain full replacement cost, once rather
  than both draft and delta in the ordinary submission.

  | Control / bootstrap total | N=100 | N=1000 | N=4000 |
  | --- | ---: | ---: | ---: |
  | Native plain bootstrap | 285,872 / 607,811 | 2,318,762 / 5,107,642 | 9,114,257 / 20,124,203 |
  | Native tool bootstrap | 294,745 / 619,525 | 2,327,637 / 5,119,362 | 9,123,130 / 20,135,917 |
  | Legacy plain protected turn / ordinary terminal | 154,348 / 48,207 | 1,414,421 / 443,321 | 5,659,421 / 1,772,321 |
  | Legacy tool protected turn / ordinary terminal | 316,555 / 63,352 | 2,836,721 / 570,973 | 11,326,721 / 2,277,973 |

  Bootstrap and unchanged legacy saves remain intentionally O(N); legacy
  materialized bytes and schemas are unchanged. N=10,000 is not a successful
  turn in either mode because the existing 4,096-entry stable-history cache
  identity limit rejects planning before provider I/O. The regular test
  `task_2_7_native_turn_bytes_are_delta_bounded` covers plain/tool/unchanged
  cases and both byte counters at all three successful sizes; the pre-existing
  equivalence suite still compares every barrier and fresh-runtime resume.
  Detailed per-commit-kind rows are recorded in `docs/development.md` and
  `/Volumes/Data/tmp/refactor/u9b-3/task-2.7-measurement-1.log`.
  Verification: `fmt --all -- --check` and all-target/all-feature Clippy with
  warnings denied pass; agent-runtime-core passes 262 tests, agent-runtime
  passes 592 tests (including the unchanged 2.2–2.4 equivalence harness and
  both scale tests), agent-runtime-testkit passes 364 tests including all
  consumer fixtures, agent-runtime-lcm passes 62 tests, and the requested Rust
  1.86 build of agent-runtime-core plus agent-runtime succeeds. Logs are under
  `/Volumes/Data/tmp/refactor/u9b-3/`.
- [ ] 2.8 Decide whether materialized checkpoints need a new schema tag once the native writer exists. Group A keeps CHECKPOINT_SCHEMA_VERSION at 3 and writes unchanged bytes; if a new tag is needed, ship its reader one release before any writer and state the rollback limit in CHANGELOG.

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
