# Runtime journal performance bench contract

## Target and Isolation

Extend the existing unpublished `agent-runtime-journal-bench` crate with a
named ignored release target, as `tests/hot_path.rs` /
`journal_hot_path_release_report`, plus a small non-ignored N=100 smoke and
deterministic work-count/conformance tests. The existing physical report stays
available. Phase 0 implements this prerequisite before P1/P2/P4.

Add non-default `test-support` features in core and runtime, with runtime
forwarding core's feature, and a non-default bench `journal-profile` feature
forwarding both. The named target requires `journal-profile`. Timer/counter
code and its observer surface compile out when the feature is off; when
compiled in, observation is disabled unless explicitly scoped to a bench run.
Keep published default graphs, canonical data, normal logging and production
metrics unchanged. Do not copy the supplied patch's temporary registry git
revision override or add a benchmark crate to a production dependency graph.

A prospective invocation, after implementation, is:

```sh
JOURNAL_MEASUREMENT_DIR=/tmp/rt-journal-hot-path \
  cargo test --offline -p agent-runtime-journal-bench --release \
  --features journal-profile --test hot_path \
  journal_hot_path_release_report -- --ignored --exact --nocapture
```

The release report rejects debug assertions and records compiler/package
revision, optimization settings, adapter, features, policy and budget settings.
Use available offline dependencies. Record only allowlisted build/bench
configuration, never a process environment or secrets. Artifact location is
an explicit parameter; no product telemetry or external service is required.

## Fixture Identity and Matrix

The source of truth is this tag's
`crates/agent-runtime-journal-bench/tests/support/fixture.rs`, also copied by
Forge. Freeze a file hash plus generated-content fingerprints before changing
instrumentation. Refactoring support code is allowed only with generated-byte,
script, cache-identity and ordering equality. Report differences as fixture
changes, never as a performance pass.

| Fixture | Required shapes at N=1000 and 4000 | Measurement |
| --- | --- | --- |
| A | Plain and tool, original tiny seeded messages, two turns | Same fixed bootstrap/tool replies; last turn plus complete maintenance. |
| B | Plain and tool, original realistic content, two turns | Profile-comparable primary component series; same cached provider setup. |
| C tool | Original N/4 tool turns, writer/Runtime replaced every turn; 32 retained manifests | 250/1000 turns; collect after every terminal plus shutdown drain. Record every call/turn and state-size milestone. |
| C plain supplement | Same realistic generator/seeds/plain reply script, two messages per turn, N/2 turns; new writer each turn and 32 retained manifests | 500/2000 turns to reach N actual messages. Explicitly a new supplemental variant; the original C generator supports only the tool N/4 expectation. Establish a new baseline before P1. |
| N=100 smoke | Original A/B plain/tool; no replacement of larger fixtures | Compile/identity/correctness and fixed-cost timing diagnostics for D10. |

Preserve xorshift64, the exact existing word list, history seed
`0xbadc0ffee1234567`, provider-script seed `0x12349876abcd5678`, input seed
`0x987612345678abcd`, fixed clock 100, input/response ordering, storage/session
ids, cache endpoint/implicit-prefix configuration, cache identity assertions,
provider scripts and the 32-manifest window. B uses 100–800 byte user text,
200–2000 byte assistant text, and 500–8000 byte tool payloads. Do not tune the
content or seeds. Instrumented/reference engines receive identical operation
identities when checking roots byte-for-byte.

Keep the original C tool case intact. The plain supplement changes only the
turn-count rule needed to reach N messages and the existing plain script
branch; label it `C-plain-supplement` in every output. Do not compare its timing
to a historical C tool number. The owner accepted this extension on 2026-10-21; it supplies the requested plain coverage without mislabeling
Forge's existing C evidence.

Use one shared engine per storage/domain and new Runtime instances at the
same boundaries as the original fixtures. Configure 128 MiB estimated memo
charge per engine in comparison runs, zero/tiny/oversize modes separately, and
collection enabled/disabled diagnostics. Report cursor charge independently;
accepted additional workspace cap is 16 MiB per engine.
The runtime reference adapter models sealing lengths and can share bodies
within the session; it is not Forge's sqlx/real-AEAD independent-domain layout.
Freeze and report the adapter/sharing/repack configuration for every comparison.

Measure identity and an explicit conservative ordinary policy preserving all
restore-critical/exact runtime data and omitting optional sensitive diagnostics.
The fixture's lack of sensitive optional extensions can make their bytes equal;
that is not permission to replace product policy. Policy-fallback and redaction
canaries are separate conformance cases, with unchanged generator bytes.

## Cadence and Timing Boundaries

Charge S2 after each terminal and both domain collections after every turn in
A/B/C for this target, plus a final shutdown sweep. For P4, drain all continuations
needed by that cadence and charge their sum; timing one yielded call as if it
completed collection is invalid. Keep the existing collect-at-end physical
report as a separately labeled diagnostic. Preserve the same retention/replay
state and compare stable final results, not the first cheap slice.

Record seed/bootstrap/import, initial writer open, run, writer preparation,
engine commit, post-terminal S2, each domain collection, other maintenance,
shutdown/drain, cold/warm read-only root selection, full cold/warm Runtime resume,
extension probe and first legacy import separately. Cold means fresh engine
and adapter caches, not a flushed OS page cache. Warm reuses the same engine
clones and validated root. Include the complete Runtime restore, public owned
snapshot materialization and final authority checks in resume time.

Keep forced WAL checkpoints, page accounting, final VACUUM, diagnostic exports
and initial fixture setup outside the last-turn metric, as in the supplied
profile. Retain their times separately. Keep SQLite durability/pack settings
identical in paired comparisons. The runtime legacy baseline is modeled by
this crate and is not the real Forge L2 implementation; Forge D10 must be rerun
in Forge with its real legacy/runtime paths and storage.

Run C in both an isolated native process and the full A/B/C matrix sequence
with the historical legacy snapshot holders retained. Pair gate-hold spans
with the exact collect call id. Do not infer the old 2.99 s cause from the
ratio of unrelated historical maxima. Add 100 unrelated sessions and 100 dirty
sessions, plus a no-garbage large live graph, so hydration and marking cannot
hide behind a release limit.

## Timers Equivalent to the Supplied Patch

Retain these labels or emit a documented one-to-one alias when refactoring
moves a function. Each records calls and inclusive elapsed nanoseconds;
event counters retain counts/bytes. Preserve parent/child ids and operation
context so nested spans cannot be summed as disjoint wall time.

| Area | Exact profile labels |
| --- | --- |
| Encoding | `rt.encoding.canonical_journal_json_len` |
| Assembler | `rt.assembler.structured_sequence`, `rt.assembler.manifest`, `rt.assembler.snapshot_cached` |
| Transition/draft | `rt.transition.commit`, `rt.transition.project_snapshot`, `rt.draft.manifest` |
| Links | `rt.links.memoized_closure`, `rt.links.draft_reachable_closure`, `rt.links.reachable_closure` |
| Staged validation | `rt.staged.materialize_staged_checkpoint`, `rt.staged.materialize_staged_snapshot` |
| Memo | `rt.memo.get`, `rt.memo.hit`, `rt.memo.miss`, `rt.memo.insert`, `rt.memo.release`, `rt.memo.memo_entry`, `rt.memo.writer_memo`, `rt.memo.seed_memo`, `rt.memo.validated_root`, `rt.memo.commit_memo_entry` |
| Public engine operations | `rt.mod.open`, `rt.mod.commit`, `rt.mod.renew`, `rt.mod.close`, `rt.mod.collect` |
| Persistence | `rt.persistence.hydrate_state`, `rt.persistence.closure_objects`, `rt.persistence.pin_roots`, `rt.persistence.pin_closure`, `rt.persistence.collection_live`, `rt.persistence.write`, `rt.persistence.publish`, `rt.persistence.commit_inner` |
| Controls | `rt.controls.collect_inner`, `rt.controls.collect_sessions` |

Add writer spans for `open_domain`, writer-view acquisition, reader snapshot /
checkpoint, writer `snapshot_cached`, manifest/request/previous-cache assembly,
semantic delta generation, known-candidate/id filtering, checkpoint assembly,
commit submission and candidate publication. Exclude the awaited engine commit
from an explicitly reported additive writer-preparation total. Separately time
public restore materialization and remaining Runtime/context preparation. This
resolves the profile's 216/396 ms subtraction and untimed import remainder.

Add spans/counters for authority load/pin certificate verification and final
lease check; collection gate wait, actual held interval(s), whole call,
discover/authenticate/mark/inventory/dependency/release/purge/S5/S2 phases,
preflight/atomic storage apply, yields/restarts and full drain. Observe bounded
storage trait calls with wall time, ids/bytes, page and transaction counts;
record maximum single non-preemptible read/decode/apply unit. Start with the
explicit discovery/member/metadata/edge/pack caps in design P4; record calibrated
limits with the 250 ms target so work-count comparisons use identical settings.
Never include
plaintext objects, user content, credentials or full root ids in normal output.
Use anonymous benchmark-local operation/session identifiers.

Test observers must be scoped/resettable and safe for concurrent async tests;
async span identity follows the operation across awaits, rather than a global
counter reset racing another case or thread-local ancestry across task moves.
Callbacks are non-reentrant, do not change execution, and do not panic into
runtime operations. Buffer output outside timed spans. Instrumentation errors
fail the bench, not a production journal operation.

## Work and Memory Counters

Expose test-only snapshots for semantic nodes/bytes copied or newly encoded,
unchanged prefix nodes visited, manifest nodes decoded, full candidate
materializations, canonical-length bytes traversed, closure edges checked/reused,
exact-set insert/remove counts, checkpoint fields/bytes validated, writer
root-derivation passes, authority checks, proof hits/fallbacks, body reads,
member pages and storage units. Separate new data from reused immutable data.

Report memo hits/misses/evictions/oversize rejection and estimated charge,
writer working state, persistent-layer/version depth, cursor/frontier charge,
shared proof retention and storage cache charge. A retained-heap or safe RSS
probe is optional supplementary evidence and must be labeled with its scope;
RSS is not memo heap. Unavailable allocator heap is null with a reason. Do not
claim the profile's lookup counters resolved Forge's missing allocator metrics.
Dropping evicted versions must use bounded stack depth; leaks of all historical
versions fail acceptance even if time improves.

## Phase Acceptance

Capture a frozen v0.9.0 baseline with instrumentation first, then paired
pre-phase/post-phase runs with the same fixture bytes, policy, budgets, storage
and cadence. Use at least five fresh A/B samples per shape; run at least three
complete C replicates in each context and retain every per-call maximum. Report
median and p95/range for repeated operation samples, and the maximum without
percentile substitution for P4. Repeat after a material failure/fix, not until
a favorable sample appears. Never subtract timer calibration from gate results.

All phases must pass existing protected/ordinary/storage/collection, import,
crash/recovery and broken-adapter conformance. Run the existing core, testkit,
runtime session-journal integration and packed adapter configurations. Report
canonical root/byte equality for identical commits, protected exact checkpoint
execution equality, ordinary policy equality, replay/retirement and cumulative
physical receipts. Total turn/open/import time must not regress more than 10%
against the immediate paired predecessor unless P4's declared full-drain 15%
limit applies to the collection portion; any exception needs an owner decision.

| Phase | Required measurement at both N values and plain/tool coverage | Numeric/work acceptance |
| --- | --- | --- |
| P1 | Candidate, manifest, memo proof/charge, checkpoint validation and additive writer preparation in A/B and C variants | In supported warm cases, each engine stage and the disjoint candidate+memo+checkpoint sum ≤50% of baseline; aim ≤25%. Attributed writer preparation ≤50%. Fallbacks are separately reported and excluded from delta-scaling claims, with no hidden correctness failures. |
| P2 | Warm certified-root collection and cold/uncertified fallback, both domains, every fixture | Certified roots cause zero old-body closure fetches and zero old-edge revalidation. B's combined collections ≤30% of pre-P2 time. Inventory/physical work remains measured; A/C use the same proof counters and report latency without inventing B-equivalent gains. |
| P3 (deferred/optional) | Full warm/cold Runtime resume and every C writer change, both domains | Warm writer root derivation and unchanged body reads are zero. Full warm resume ≤25% of pre-P3 median for each shape; B/4000 additionally ≤150 ms plain / ≤200 ms tool. Cold/import ≤110% of pre-P3. |
| P4 | Every collect call/gate interval, cold proof miss and warm hit, no-garbage graph, 100 dirty sessions, isolated/full-matrix C | Target every call/hold <250 ms; hard acceptance every call/hold <1,000 ms. All quiescent drains with positive budgets and sufficient capacity terminate with reference-equivalent state and receipts. Default-budget full-drain time and pack-write bytes ≤115% of pre-P4; report the entire drain in charged turns. |

A and C include phases without a qualifying manifest/checkpoint operation;
those spans report zero/not-applicable with a reason rather than divide by zero.
The policy used for incremental acceptance must explicitly opt in. Full-only
policies keep functional/regression coverage, not an O(delta) speed claim.

For the P1 fixed-delta scaling case, take unchanged A/B generators at N=1000
and 4000, warm a base, and replay a separately labeled identical bounded append
and scalar checkpoint update into each base. Keep the exact changed payload
constant and account for encoding-height boundary changes. At N=4000, targeted
visited/copied/encoded work must be ≤1.5× N=1000, with **zero walks over unchanged
history/usage/manifest prefixes** in targeted preparation. This is a deterministic
work gate, not a replacement for the full original two-turn workload. Genuine
full replacement/global policy cases must show the O(N) fallback explicitly.

Useful profile-calibration reference values, not portable pass thresholds:

| B shape | Candidate+memo+checkpoint baseline ms | P1 50% maximum ms | Combined collection baseline ms | P2 30% maximum ms | Full warm resume baseline ms | P3 25% maximum ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 plain | 143.61 | 71.81 | 159.76 | 47.93 | 241.90 | 60.48 |
| 1000 tool | 273.47 | 136.74 | 188.05 | 56.42 | 288.52 | 72.13 |
| 4000 plain | 601.75 | 300.88 | 620.46 | 186.14 | 971.11 | 242.78, additionally 150 |
| 4000 tool | 1,118.88 | 559.44 | 713.47 | 214.04 | 1,162.07 | 290.52, additionally 200 |

## Artifacts and Verification

Produce structured JSON summary, raw JSONL spans/per-collect samples and a
Markdown table per revision/phase. Record fixture hashes/seeds, build settings,
all runs and numeric pass/fail independently of whether the harness completed.
Save cold/warm/import and full drain boundaries, differential correctness,
work/memory counters, exact limits, yield/restart causes and missing-metric
reasons. Include a ledger of attempted runs/failures without process listings.

Calibrate an empty enabled timer, compiled-in disabled timer and callback sink
with comparable counter cardinality. The supplied host profile measured about
76 ns enabled/2 ns disabled, but this is context, not a bound or a correction.
Require an observer-off paired end-to-end check; unexplained median overhead
above 2% requires investigation and recording before interpreting phase gains.
Verify a production feature-off build has no observer export/hook/default
logging and no benchmark dependency. Feature-on/observer-off results must be
semantically identical. Run the ordinary development format/lint checks and
safe minimum-stack tests appropriate to the implementation.

Finally rerun Forge D10 only in its own consumer work: real sqlx/AEAD, unchanged
conservative projection, independent domains, fixed fixtures, legacy L2,
production cadence and full drains. A runtime bench pass is evidence about
mechanism; it is not a Forge adoption pass. Apply the owner-approved fixed budgets in
design.md; runtime import speed is non-gating with Forge background conversion.
