---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-18T00:00:00Z
---

# Tasks: SessionJournal and referenced checkpoints

The owner approved merge group A on 2026-10-08 (local date); group B has since
landed with the released native writer. The 2026-10-18 implementation request
authorizes group C (3.1–3.4). Group D and U10 remain outside this authorization.
Groups can merge independently; release requires all applicable gates, not
merely contract merge.

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
- [x] 2.5 Implement idempotent one-time v3 import and mixed native/legacy detection; cut crashes at each import write, including pending approval, CallingModel, model/tool results, cache phases and pending summary.
  Evidence (2026-10-18): fully capable pairs reserve absent heads before one
  v3 load, validate identity/operation/namespace/pair evidence, publish exact
  first under stable `runtime-import-v3-*` operations, repair a missing
  ordinary projection from the unchanged v3 pair, close the import pins only
  after both publications and rediscover the committed heads. Mixed pairs stay
  wholly legacy before a head; a detectable head plus a lost capability fails
  incompatible; ephemeral sessions still make zero store/journal calls. The
  authorized old selection test was split into
  `legacy_v3_records_with_both_capabilities_import_once` (native heads equal
  the v3 materializations, no legacy save, no second import) and the unchanged
  `mixed_pairs_keep_the_materialized_path` expectations. The equivalence
  harness's `v3_import_crash_cut_matrix_repairs_exactly_without_provider_io`
  covers 12 real states (idle, approval, interaction, CallingModel,
  ModelResponseReady, ExecutingTools, ToolOutcomeReady, local executing, and
  four cache phases) across all seven protected and seven ordinary commit cuts
  plus pin close before/after release; every reopened head equals v3 and its
  cold encoding and provider requests remain zero. The dedicated pending-LCM
  test imports a committed pending response without another summary-model or
  provider call. Redacted/exact separation, invalid evidence, v3 mixed pairs,
  lost capability and ephemeral isolation have focused integration tests.
  Review: the matrix resumes with `Defer` (no continuation);
  `v3_import_continuation_matches_legacy_resume_for_every_state` runs import,
  resume and continue for all 12 states on a 1.75 MiB worker and matches
  legacy provider requests, tool calls and barriers exactly. It exposed a
  debug-build stack fault in `TurnMachine::resume` (~1.8 MiB legacy, ~2 MiB
  native at ModelResponseReady; now ~1.25 MiB). Read-only views now reject a
  lost capability like open. Ordinary repair requires a v3 checkpoint equal
  to the exact head (`ordinary_repair_requires_the_exact_head_to_be_the_v3_import`).
  Added refusal-parity, raw-ordinary projection, racing-import and
  advanced-exact repair tests.
- [x] 2.6 Exhaustively inject crash/ambiguous write outcomes between physical object, batch, protected head, ordinary projection and publication writes; test torn tails and committed corruption fail-closed repair.
  Evidence (2026-10-18, revised in review):
  The crash matrix (`task_2_6_crash_matrix_<barrier>`, one test per barrier;
  `task_2_6_native_barrier_crash_and_ambiguous_write_matrix` is its inventory
  tripwire) covers all 22 all-feature `TurnState` checkpoint tags plus
  ordinary bootstrap, terminal projection, shutdown persist, fork intent/child
  seed/parent supersession and both idle-compaction publications: 30
  barriers, 293 distinct cases. Each barrier is crashed after every physical
  commit step of the reference backend (`AfterObjects`, checkpoint (protected
  only), batch, pin, head, idempotency, and `AfterCommit` with a lost
  acknowledgement), before submission, after an acknowledged commit, and after
  an acknowledged commit plus pin close. The reference backend stages every
  step on a private copy and publishes at one point, so every pre-visibility
  cut is observably "before" and is a regression guard for that atomicity
  (a backend publishing the head before its batch, or before its idempotency
  record, fails these rows). Every case reopens through a fresh reader,
  accepts only the complete before/after boundary with cold-encoded roots,
  continues on a 1.75-MiB worker against legacy resume (same provider
  requests, tool calls and later barriers) with an absolute no-replay check
  for `calling_model`/`cache_operation_started` (no provider request before
  the next input) and `executing_tools`/`local_action_executing` (no tool
  re-execution), and retries the same operation to the original head. The
  first version of this matrix counted 678 rows by crossing every cut with
  two "ambiguous" outcomes that ignored the cut (a plain commit and no
  commit), never reached the runtime's own commit path, and compared resume
  only against legacy, so a replay in shared resume code passed.

  Ambiguous outcomes seen by the live writer are
  `task_2_6_runtime_resolves_ambiguous_commits_{plain,internal,tools,interaction,cache,bootstrap,fork}`:
  at every commit of each scenario (125 commits, 250 cases) the running
  runtime receives a lost acknowledgement (the commit landed; its
  renew/head-advance/same-operation recommit must reach the original head
  and the whole run must equal undisturbed legacy) or a failed pre-visibility
  commit (the run, its published barriers, a fresh resume and continuation
  must equal legacy with the same save failing). This found a production
  defect (fixed): after a failed terminal publication followed by a saved
  input, terminal republication on resume had a shorter history than the
  ordinary head and the writer refused it as a non-append, wedging the
  session; it is now submitted as a complete replacement. The ordinary-only
  hard-admission path keeps its 18-row matrix with the real LCM store/model
  (6 distinct cuts plus the two cut-independent outcomes, renew blocked so
  the fresh process resolves). Durable child rebind remains covered by its
  existing fresh-process native/legacy conformance test, not by matrix rows;
  resume-time repaired-successor and start-time namespace-replacement saves
  (`runtime/engine.rs`) have no dedicated row.

  `task_2_6_committed_corruption_fails_closed_without_stale_fallback` covers
  truncated/garbled history objects and batches, a missing history object, a
  corrupt referenced checkpoint object, digest substitution and a corrupt
  ordinary projection beside a valid exact head. All eight return explicit
  `StateConflict` before provider I/O and refuse both a retained predecessor
  and a deliberately retained stale v3 pair; restoring verified backend bytes
  recovers the latest exact content with cold roots and no lost history,
  usage, extension or manifest data. No runtime path trims or rolls back. The
  unpublished torn-tail row only illustrates the `SessionJournal::read`
  rustdoc rule (trim only under a writer fence): the volatile backend has no
  physical frame tail, the adapter never exposes the tail bytes, and the
  fence check is the adapter's own; a file/database adapter must enforce it
  itself, and no conformance test can.
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
  Review (2026-10-18): the byte tables reproduce exactly. The empty-history
  equivalence scenarios never decoded the >64-item binary spine, so
  `review_spine_sized_histories_match_at_every_barrier_and_after_fresh_resume`
  now seeds 70/130/260 messages (identity and redacting policies, plus tools
  and a local action) and compares every barrier and a fresh resume;
  `review_spine_sized_forks_match_at_every_barrier` does the same for
  Summary and FromIndex forks of spine-sized parents. Core
  unit tests round-trip the persistent JSON at every 2^k/2^k±1 boundary up to
  4,097 items and for node-shaped values, and prove draft references never
  resolve across sessions, from unpinned objects, for host namespaces or a
  fenced writer. The ordinary adapter conformance suite now requires adapters
  to resolve structured references across N commits sharing a prefix, a
  collection attempt and a fresh writer. A protected head the writer cannot
  re-derive fails closed instead of being migrated (no release wrote one).
  The ~20 MB N=4,000 bootstrap (vs a 1.77 MB legacy snapshot) is the
  per-element cost of the JSON-node form: each small cache/manifest entry
  becomes ~8 objects with their own envelopes, single-reference leaves and
  spine branches; the fixed 32-level spine also adds ~20–25 unary branch
  nodes to every changed array. Both are representation choices, not
  unchanged-prefix writes.
  Refinement evidence (2026-10-18): persistent arrays now use their minimum
  height, compact JSON values use a 1,024-byte canonical threshold with typed
  inline/reference nodes, and cached record/path encodings are reused across
  commits in the runtime writer and ordinary reference backend. Exact `S/N`
  totals before/after this refinement are:

  | Path | N=100 before / after | N=1000 before / after | N=4000 before / after |
  | --- | ---: | ---: | ---: |
  | Plain bootstrap | 285,872/607,811 / 146,814/239,857 | 2,318,762/5,107,642 / 1,062,815/1,742,270 | 9,114,257/20,124,203 / 4,133,607/6,767,016 |
  | Plain protected turn | 52,327/58,337 / 42,041/48,051 | 53,871/59,895 / 43,740/49,764 | 52,998/59,022 / 43,088/49,112 |
  | Plain ordinary terminal | 29,620/34,177 / 10,867/28,134 | 30,259/36,430 / 11,815/29,671 | 30,265/40,412 / 12,263/29,681 |
  | Tool bootstrap | 294,745/619,525 / 154,239/247,610 | 2,327,637/5,119,362 / 1,070,240/1,750,023 | 9,123,130/20,135,917 / 4,141,032/6,774,769 |
  | Tool protected turn | 127,974/139,853 / 108,028/119,907 | 131,154/143,061 / 111,468/123,375 | 129,408/141,315 / 110,164/122,071 |
  | Tool ordinary terminal | 46,492/51,328 / 16,496/34,042 | 47,341/53,791 / 17,848/35,983 | 47,350/57,776 / 18,520/36,217 |

  N=4,000 plain bootstrap is 3.82× the 1,772,321-byte legacy snapshot and
  clears the ≤4× target. Each unchanged boundary remains zero drafts/zero body
  bytes at 1,183/2,015, 1,187/2,021 and 1,187/2,021 (plain) and
  1,180/2,011, 1,184/2,017 and 1,184/2,017 (tool).

  Debug wall time per native commit, measured as complete API-phase elapsed
  time divided by the phase's commit count, changed as follows (milliseconds):

  | Phase | N=100 before / after | N=1000 before / after | N=4000 before / after |
  | --- | ---: | ---: | ---: |
  | Plain bootstrap | 217.0 / 42.5 | 1,857.6 / 316.6 | 7,421.0 / 1,302.0 |
  | Plain turn | 310.2 / 27.9 | 2,693.7 / 159.3 | 10,761.8 / 593.6 |
  | Plain unchanged | 644.9 / 2.5 | 5,842.1 / 9.4 | 23,261.0 / 31.6 |
  | Tool bootstrap | 211.5 / 44.5 | 1,866.9 / 319.8 | 7,419.3 / 1,257.6 |
  | Tool turn | 295.9 / 28.9 | 2,510.6 / 129.9 | 10,004.4 / 469.1 |
  | Tool unchanged | 813.0 / 2.5 | 7,072.3 / 9.4 | 28,134.2 / 32.9 |

  The optimized after run measured N=100/1000/4000 plain turns at
  2.3/11.6/44.6 ms per commit and tool turns at 2.1/10.0/37.3 ms per commit;
  its raw log is `/Volumes/Data/tmp/refactor/u9b-4/task-2.7-current-final-release.log`.

  This is an 18–22× N=4,000 turn improvement, but complete phase time remains
  N-dependent. The requested “roughly flat” end-to-end CPU target is therefore
  recorded as NOT MET. Detailed commit-kind bytes are in `docs/development.md`;
  raw before/after logs are
  `/Volumes/Data/tmp/refactor/u9b-4/task-2.7-before.log` and
  `/Volumes/Data/tmp/refactor/u9b-4/task-2.7-current-final.log`.
  Because minimum-height arrays make a changed path O(log N) instead of a fixed
  32 levels, two existing assertions were relaxed from equality to bounds by
  this refinement: `native_delta_submission_is_independent_of_unchanged_history_bodies`
  (object count N=1000 ≤ N=100 + 8) and
  `review_one_message_turn_submissions_do_not_grow_with_history` (objects ≤ base
  + 16, reference bytes ≤ base + 2,048). Measured growth from N=100 to N=4000 is
  +5 objects for the cache-record commit and +10 for the ordinary terminal.
  Review fixes (2026-10-18): a warm path cache failed every commit once a cached
  array grew past one 64-item leaf ("JSON sequence cache is incomplete": any
  session whose cache record or manifest outgrew 64 entries by turns) and
  reused a two-item node for an array shrunk to an odd width; caches and the
  writer's delta treated `0.0`/`-0.0` as unchanged; the protected fast path
  rejected valid commits whose advisory delta did not replay and never re-armed
  after a writer change; reference-backend layers grew one per commit; the
  ordinary draft closure re-validated every pinned object. Tests: `a_warm_json_cache_encodes_reshaped_arrays_like_a_cold_one`,
  `review_arrays_growing_past_one_leaf_match_at_every_barrier`,
  `a_warm_json_cache_distinguishes_negative_zero`,
  `negative_zero_records_are_encoded_and_sent_like_any_change`,
  `a_negative_zero_rewrite_is_stored_like_a_cold_encoding`,
  `protected_commits_with_an_unreplayable_delta_fall_back_to_full_validation`,
  `the_protected_fast_path_reseeds_after_a_writer_change`,
  `many_commits_keep_logarithmic_layers_and_exact_accounting`, three-encoder
  boundary checks (1,023/1,024/1,025 bytes, multi-byte UTF-8, signed zero,
  64/65/66/130 items) in the runtime and testkit, and a cold-encoding oracle
  on every equivalence head. Byte totals are unchanged. Re-measured debug turn
  time at N=4000 is 388/379 ms per commit (plain/tool, was 594/469) and release
  32.1/31.1 ms (was 44.6/37.3); the debug ordinary terminal commit is 1.3/2.0 s
  (was 3.0/3.6 s). Remaining O(N), per backend commit kind at N=4000 (debug): the
  cache-record reference commit 628 ms (full decode of the referenced value),
  `calling_model` 261 ms (full request validation), ordinary terminal 1.34 s
  (decode for the host policy) versus 13 ms for a commit with no changed
  record; see `design.md`.
- [x] 2.8 Decide whether materialized checkpoints need a new schema tag once the native writer exists. Group A keeps CHECKPOINT_SCHEMA_VERSION at 3 and writes unchanged bytes; if a new tag is needed, ship its reader one release before any writer and state the rollback limit in CHANGELOG.
  Decision (2026-10-18, confirmed in review): no new materialized tag is
  needed. Native schema 4 is confined to journal heads, objects and reference
  envelopes; `JournalReader` reconstructs and validates the unchanged schema-3
  `TurnCheckpoint`. `task_2_8_materialized_checkpoint_bytes_equal_legacy_at_every_barrier`
  shows the materialized bytes equal the bytes a legacy `CheckpointStore`
  receives at every barrier of five scenarios (159 records, 20 checkpoint
  states; serialized JSON, not only `PartialEq`) and survive a typed round
  trip. The checkpoint DTO/validation has no persisted-contract diff between
  released `v0.5.0`, `v0.6.0`, `v0.6.1` and this decision, so no released
  reader sees a byte it could not already receive from the legacy path. The
  Smith, Nyx and Open Forge consumer rows apply *hypothetical* strict JSON
  guards (schema 3, inline `snapshot`, no `snapshot_roots`); they are not the
  consumers' code. Open Forge's actual adapter deserializes the typed
  `TurnCheckpoint` of a pinned pre-`v0.2.0` runtime revision, which predates
  this release window; that compatibility question is the same for legacy
  writes and is unaffected by the native writer. The existing warning
  against downgrading a migrated native store remains, but no reader-first
  materialized schema or additional CHANGELOG rollback limit is required.

## 3. Merge group C — Retirement and safe collection (retention stays host-owned)

- [x] 3.1 Implement expected-revision retirement, durable migration/fork pins, pin renewal/close, retained-root closure and transactional root/pin epoch checks; no automatic expiry defaults.
  Evidence: `InMemorySessionJournal` requires explicit `enable_collection` and
  revision-bound `authorize_retirement`. `pin_transfer`, `retain_root` and
  epoch-fenced `release_pin` preserve whole historical closures across process
  renew/close/revocation. All root/pin mutations serialize with mark/delete.
  Publication also indexes existing fork intents and child catalogs, including
  dependency metadata omitted by ordinary projection. Unknown index revisions
  return typed unsupported. Tombstones and replay fences stay in backend indexes;
  no released object/head/checkpoint format or required trait method changes.
- [x] 3.2 Interleave mark/delete with read-only resume, writer resume, Summary/Empty/FromIndex fork, Continue ownership transfer, pending child and expired process lease; demonstrate no dangling reachable reference.
  Evidence: eight named `JournalGcInterleaving` operations each run before mark,
  after mark/before deletion and after deletion (24 deterministic rows), reading
  every reachable candidate/head/batch/state object and materializing fresh
  roots. Each row includes aborted staging and asserts actual deletion at the
  first successful sweep. A retired retained-root open after deletion fails before dereference.
  Nine `journal_gc_runtime` tests run actual fork/resume, Continue with LCM leaf
  nodes, and a child explicitly paused before provider I/O under all three GC
  schedules on normal Tokio workers; the child rebinds after process expiry and
  GC without repeating its completed provider request. Design.md contains the
  operation-by-step result matrix.
- [x] 3.3 Reclaim orphan staging and superseded checkpoint/request/extension objects, then retired-session content; retain live canonical history and every committed source required by LCM summaries.
  Evidence: `long_session_collection_byte_evidence` publishes eight exact
  CallingModel checkpoints over N=1000 canonical messages with distinct
  requests/settings and 16 KB opaque replacements. Retained immutable bytes are
  570,121 before GC, 404,529 with the session live (165,592 reclaimed), and 0 after
  authorized retirement/pin release. Every canonical source and latest exact
  request materializes after sweeping; superseded checkpoints do not open.
  Summary/Empty/FromIndex cases assert that only the child's complete closure
  remains after parent retirement. Continue expands the actual committed LCM
  sources after the original parent is retired.
- [x] 3.4 Prove unsupported collection retains content safely and failed repair cannot clear pending intent pins; distinguish backend compaction from logical GC.
  Evidence: named third-party supported, ordinary and unsupported collector
  assertions; three broken adapters are rejected for losing a pinned object,
  ignoring the epoch and clearing intent pins after a failed repair, while the
  transparent wrapper passes. All six pre-visibility repair cuts retain durable
  intent pins; a separate committed-intent case checks both endpoints and a
  retained dependency root after failed repair. Physical backend `compact`
  retains orphan bytes, unlike logical `collect`; LCM compaction keeps canonical
  sources. Default unsupported tests and all earlier expectations remain intact.
  Verification (2026-10-18): fmt and all-target/all-feature Clippy clean; core
  276, runtime 656, testkit 405 (including 25 collection and 9 runtime GC tests),
  LCM 62; Rust 1.86 core/runtime build and strict change validation pass. The
  cross-domain host fixture registers protected child pins before publication,
  since the parent's actual child catalog is an ordinary-store publication.

## 4. Merge group D — Coordinated consumers and release eligibility

- [ ] 4.1 Extend consumer_open_forge with assert_chunked_journal_recovery over separate redacting/exact neutral chunk stores, actual Forge row-3.7 adapter/schema checks and shared-tag adoption.
- [ ] 4.2 Extend consumer_smith with assert_file_journal_migration and check all actual file/capsule checkpoint implementations, direct manifests reads, contributors, idle compaction and event matches.
- [ ] 4.3 Extend consumer_nyx with assert_ephemeral_journal_isolation and assert_optional_durable_journal; verify zero durable activity for ephemeral starts and immutable dependency pinning.
- [ ] 4.4 Run three existing named consumer targets, workspace/schema/conformance/doc checks, formatting, Clippy, dependency/license/neutrality and MSRV gates during implementation; a failing consumer blocks compatible release.
- [ ] 4.5 Publish migration/downgrade/retention limitations and actual byte counts in CHANGELOG, coordinate pre-1.0 break, and release only an immutable landed revision. Do not remove v3 readers or legacy methods in this change.
