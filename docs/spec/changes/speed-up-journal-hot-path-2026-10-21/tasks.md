---
created_at: 2026-10-22T00:49:18Z
updated_at: 2026-10-23T22:36:31Z
completed_at:
---

# Implementation tasks after proposal approval

Phase 0 is authorized by the owner. P1/P2/P4 are required future phases;
P3 is deferred and not required for the Forge gate.
Each phase requires its own before/after artifacts and acceptance; complete
phase acceptance does not imply Forge D10 adoption readiness.

## 0. Bench and Instrumentation Prerequisite

- [x] 0.1 Record the v0.9.0 fixture/source hashes and generated-content fingerprints; preserve all three seeds, clock 100, scripts, word list, cache assertions and original C tool cadence.
- [x] 0.2 Add non-default core/runtime test-support and bench journal-profile feature wiring without copying the temporary dependency override or adding production bench dependencies.
- [x] 0.3 Add scoped async-safe observers with every supplied patch label/alias, writer builders, final authority, gate wait/hold, storage units, complete drains and memory/work counters from bench.md.
- [x] 0.4 Add the named ignored release target, N=100 smoke, A/B N=1000/4000 plain/tool, original C tool and separately approved/labeled C-plain-supplement coverage with fresh baselines.
- [ ] 0.5 Capture frozen instrumented baselines and observer-off/calibration controls; directly attribute the 216/396 ms B writer remainder and import preparation before estimating their gains.
- [ ] 0.6 Preserve full-matrix and isolated C contexts plus 100 unrelated/dirty-session diagnostics; record every raw run, failure, call maximum and missing metric.

## 1. P1 Incremental Commit

- [ ] 1.1 Introduce immutable semantic snapshot and persistent sequence/map/JSON views shared across writer, transition and validated memo; preserve public DTOs and canonical schema-4 encoding.
- [ ] 1.2 Carry base-versioned runtime mutation hints into the writer, and reuse canonical subtree encodings and manifest/request/previous-cache prefixes; remove full-vector copies, repeated to_value and unchanged-prefix scans in supported writer and engine preparation.
- [x] 1.3 Add defaulted revisioned incremental projection capability with complete fallback; implement identity/conservative fixture opt-ins and differential global-policy/configuration cases.
- [x] 1.4 Maintain exact persistent closure evidence with correct typed-edge activation/removal and membership revalidation outside the base exact closure; retain complete tracing as the test oracle.
- [x] 1.5 Cache incremental canonical lengths/charge using iterative work; test negative zero, escaped strings, optional fields and normal 2 MiB Tokio debug stacks without reviving recursive sizing/drop failures.
- [x] 1.6 Validate protected checkpoint changed state and cross-field invariants against root-bound evidence; retain complete fallback for unprovable/advisory protected deltas and every recovery barrier.
- [x] 1.7 Add split-boundary, manifest eviction, multiple-parent, dropped/re-referenced-body, wrong-kind, missing-body and all failure-cut/replay regressions in both sharing modes and zero/tiny/oversize memo configurations.
- [ ] 1.8 Run preserved conformance plus P1 matrix/work gates; publish each-stage and additive writer measurements, 50% minimum reductions, fixed-delta scaling and all remaining O(N) costs.

## 2. P2 Proof Reuse in Collection

- [x] 2.1 Bind collector proof handles to engine/domain/session/root, authority source, full session fence and mark epoch; charge temporary proof retention under unchanged memo policy.
- [x] 2.2 Authenticate current heads and certified historical pins, resolve dynamic writer tips/staged/transfer/catalog dependencies, and reuse exact proofs without flattening them into a fresh full set.
- [x] 2.3 Keep cold/uncertified/evicted-root tracing, complete membership integrity checks, retirement dependency indexing, session isolation and acknowledged-release memo trimming.
- [x] 2.4 Add head/retirement/epoch races, release-and-recreate pin, writer-pin freeze, FromIndex/shared-object and zero-budget cases; prove pin-presence-only and ignored-epoch negative controls fail.
- [ ] 2.5 Run all preserved collection/storage cases and P2 matrix; require zero warm closure body reads/old-edge validation and B collection time at most 30% of pre-P2 while reporting inventory/apply/purge separately.

## 3. P3 Validated Writer-Open View (Deferred / Optional)

- [ ] 3.1 Add optional object-safe validated_writer_view with default decline, opaque immutable proof fields and a shared core canonical-encoding seam usable by the runtime writer.
- [ ] 3.2 Initialize NativeDomain from canonical semantic/checkpoint/encoding views without materialize-then-re-derive; preserve projected ordinary id separation and exact candidate replacement behavior.
- [ ] 3.3 Complete final D-1 head/certified-pin authority and lease checks before returning a view; preserve stale retry, custom/cold fallback, native-first import recovery and failed-open lease cleanup.
- [ ] 3.4 Add wrong-role/session/root, stale current/pin, retirement, checkpoint-free, ordinary mismatch and every import/recovery cut; prove a bypass-authority negative control fails warm.
- [ ] 3.5 Run full cold/warm Runtime resume and C writer-change measurements at both sizes/shapes; require zero re-derivation, 75% warm improvement and B/4000 150/200 ms targets without excluding owned restore costs.

## 4. P4 Bounded Collection Calls

- [x] 4.1 Finalize listed breaking request/report/progress/storage contracts and migration helpers; retain max_objects release semantics and explicit unsupported capability refusal.
- [ ] 4.2 Implement a charged engine-local cursor and budget checks for discovery, proof/cold mark, inventory, dependencies, release, S5, purge and S2, with bounded iterative work/drop and fair quiescent progress.
- [ ] 4.3 Implement fence/scope-bound metadata/pin/catalog/member/maintenance pages and incremental opaque-metadata decoding in volatile/packed storage; reject whole-result-then-slice implementations.
- [ ] 4.4 Add bounded read-only apply preflight, no-effect split requests and complete atomic collection batches; preserve threshold purge, shared holders, replay window and exact physical-copy receipts.
- [x] 4.5 Authenticate epoch/fence/authority across every yield/await; discard stale marks, update only acknowledged collector-owned progress, and keep durable/local dirty state until the session and its maintenance finish.
- [ ] 4.6 Add one-work-unit yields at every state boundary; interleave roots/pins/staging/close/revoke/retirement/dependencies, lost acknowledgements, restarts and session/domain failures; prove stale-page/fence, unbounded-page and partial-apply adapters fail.
- [ ] 4.7 Test huge no-garbage marks, pin/catalog chains, dirty/unreadable-session fairness, cursor cap, zero releases, all sharing/repack modes and deferred body/operation maintenance.
- [ ] 4.8 Run P4 full matrix/isolated C and 100-dirty-session acceptance; require every call and hold <1 s, target <250 ms, terminating equivalent drains and ≤15% full-drain time/pack-write overhead.

### P4 Receipt (partial)

Implemented on `feat/hotpath-p4`: 4.1 and 4.5 complete; 4.2, 4.3, 4.4, 4.6,
4.7 and 4.8 partial. Open items: whole-record metadata reads (no chunked
metadata pages or incremental opaque decoding), catalog point lookups instead
of catalog pages, no separate maintenance pages, S5/purge/S2 bounded only as
part of one atomic apply (release splitting bounds the packs a release
touches; retired forced purge and base repack are counted, not split), the
packed adapter's whole-session index build on first touch, no FromIndex or
storage-reconnect interleavings, and the full-matrix C context. Evidence and
the exact NOT MET list: `/Volumes/Data/tmp/refactor/rt-p4/report.md`.

Review fix round (`/Volumes/Data/tmp/refactor/rt-p4-review/report.md`): the
first-unit rule (a held gate always runs one unit; `zero_work_calls`
reported), restart only on the session's own fence or lease-set change with
restarted sessions rotated behind the pass, per-session over-capacity skip
(`over_capacity_sessions`, in-unit cap check), engine rejection of `Fits` over
`max_units`, receipts with `rewritten_units` checked by conformance, bounded
retired purge/base repack in the packed adapter, member continuations bound to
the session's own pack version, `has_more` after a partial release, cancel-safe
hints and one shared apply helper. Evidence: `/Volumes/Data/tmp/refactor/rt-p4/fix-report.md`.

Drain-cost round: the in-unit cap check walked the whole mark (quadratic
tracing); the charge is now maintained incrementally. C/4000 isolated full
drains: 874 s / 4,675 calls before, 369 s / 2,676 calls after (one-call
collector 396 s; P4 381 s), 1001/1001 drains with equal receipts and pack
writes, zero restarts. `journal-profile` runs record per-call `rt.gc.*`
diagnostics in `collect.jsonl`. Evidence:
`/Volumes/Data/tmp/refactor/rt-p4-drain/report.md`.

## 5. Combined Validation and Release Handoff

- [ ] 5.1 Run every existing core journal, testkit protected/ordinary/storage/collection/broken-adapter, packed configuration and runtime session-journal equivalence/import/crash suite without weakening cases.
- [ ] 5.2 Run appropriate formatting/lint, feature-off production/default builds, feature-on observer-off parity and normal-stack tests; record failures and all retries.
- [ ] 5.3 Run the combined paired release bench at both sizes/shapes and N=100 diagnostics; publish JSON/JSONL/Markdown, fixture equality, per-phase/cumulative effects, memory/work charge and uncorrected timer overhead.
- [ ] 5.4 Update runtime development/bench docs and CHANGELOG with exact P4 API/behavior breaks, any adapter metadata migration, remaining O(N) work and the measured limits; retain journal/checkpoint object schemas.
- [ ] 5.5 Coordinate Smith/Nyx/Forge compatibility updates and immutable released-runtime prerequisite; Forge owns its policy/view forwarding and bounded-storage/D8 integration.
- [ ] 5.6 Request a separate real Forge D10 rerun with unchanged seeds, production policy, L2, full cadence and all continuations; report every turn/write/disk/hold verdict and non-gating import diagnostic and leave failed adoption gates blocked.

## Phase 0 Receipt

Implemented 0.1–0.4. Tasks 0.5–0.6 remain open: raw A/B baselines and direct
attribution are recorded, but the final instrumentation capture overlaps an
uncancellable earlier diagnostic worker, and the required complete C replicate
sets do not fit this run. No phase/adoption performance acceptance is claimed.
See `/Volumes/Data/tmp/refactor/rt-hotpath-p0/report.md` for versioned evidence,
missing metrics, process-control limits and the exact NOT MET list.

## Spec Validation Receipt

The specification validation result and docs-only scope receipt are recorded in
`/Volumes/Data/tmp/refactor/rt-hotpath-spec/report.md`. Phase-0 implementation and verification are recorded in
`/Volumes/Data/tmp/refactor/rt-hotpath-p0/report.md`; later phase checks remain future work.

## P1 Receipt (partial)

Tasks 1.3–1.7 remain implemented. Round 2 adds Runtime-owned immutable live
views, mutation recording, shared internal checkpoints, identity-bound writer
hints, persistent JSON item/branch indexes, typed JSON construction proofs,
direct closure-evidence layers and shared request/component-state owners.
Public DTO fields and journal/checkpoint/transition formats are unchanged.

Tasks 1.1–1.2 remain unchecked for complete acceptance: JSON-array and request
source ingress still inspect unchanged positions, namespace key tables are
visited, and some runtime component export functions still create their full
payload. The full fixed-delta zero-prefix gate is not met. Task 1.8 remains
open: single-sample B diagnostics do not replace the complete matrix/work gates.

Round-2 implementation, final-source verification, per-step B/4000 diagnostics,
P0/fresh-round-1/final comparisons, fingerprint design note and the phase-4
representation seam are recorded in
`/Volumes/Data/tmp/refactor/rt-p1b2/report.md`. The phase-4 worktree was read
only. Collection optimization and P2/P4 acceptance are not claimed here.

## P2 Receipt (partial acceptance)

Tasks 2.1–2.4 are implemented over the integrated P1/P4 representation and
bounded cursor. Current and head-certified historical roots reuse immutable
exact closure references, authenticated by engine/domain/session/root,
authority source, full fence and mark epoch. Temporary cache-derived proof
retention shares the unchanged configured memo budget. Collector lease/proof
layers retain their original Arcs without member coalescing; membership pages
count each layer independently and perform one lookup per layer/member.
Cold, zero-budget, evicted, uncertified and invalid-root paths retain tracing.
A failed live-subset check skips the session instead of restarting a physical
page continuation indefinitely. Working-proof clearing also restarts marks
with otherwise unchanged fences and lease ids.

Final-source core journal tests: 133 passed, including 14 new P2 cases.
All 14 supplied P4 fail-without controls and three P2 controls (pin-presence-only,
ignored epoch and member coalescing) fail their targeted tests with the guard
removed. Source bytes are restored before final validation. Preserved runtime,
storage/conformance, packed, feature/parity, formatting and clippy checks pass.

Task 2.5 remains unchecked for complete acceptance: the requested release
B/1000/4000 plain/tool, N=100 and isolated C/1000 capture plus A warm-work
diagnostics do not replace the full spec replicate/context matrix. Numeric
thresholds, owner overhead/hold results and every remaining NOT MET item are
reported in `/Volumes/Data/tmp/refactor/rt-p2/report.md`; raw fixtures are
removed after summary extraction. No full P1/P4 or Forge adoption acceptance
is implied.

Round-2 review fixes (`/Volumes/Data/tmp/refactor/rt-p1b-review/report.md`):
`runtime::SessionState` fields are private (breaking, see CHANGELOG) and every
mutation records its changed suffix; debug/test builds compare each incremental
capture and each certified typed JSON composition with the complete path.
Reachable sets fold above 16 layers and durable pins keep closure membership
only. Results are in `/Volumes/Data/tmp/refactor/rt-p1b-fix/`. No measurement
was rerun for these fixes.

### P2 review repair (2026-10-23)

- [x] 2.6 Fix reference-only proof layers, delayed/addition-only lease checks, non-destructive memo admission, renew identity/LRU feedback and unresolved-summary fallback; report incomplete membership through the existing unreadable list.
- [x] 2.7 Add >16-root, lease churn/protection, failed reservation, evicted-renew convergence and reachable-body subset controls; retain existing assertions and document fixture edits.
- [x] 2.8 Run the requested focused merge targets, both lint configurations, fourteen P4 controls, repair fail-without controls and one B/4000 sample per shape; publish `/Volumes/Data/tmp/refactor/rt-p2-fix/report.md`.

Task 2.5 remains partial: this repair's quick sanity sample does not replace
its previously outstanding full performance acceptance matrix.

The single B/4000 plain/tool sanity capture preserves canonical state and drain
receipts, but its overhead and maximum collect timings exceed the prior P2
sample under the recorded higher host load. These comparisons are NOT MET; no
additional sample or timing-driven code change followed. All requested
correctness targets, both clippy configurations and 26 negative controls pass.
