# Implementation report: Bound session manifests (U3)

Implemented on `feat/bound-session-manifests`; changes remain uncommitted.
The approved delta specs remain in this change directory; no archive or truth
spec merge was performed.

## Behavior and design choices

- `RuntimeBuilder::manifest_window(NonZeroUsize)` defaults to 32. Appends,
  ordinary snapshot captures/saves, and validated live adoption retain the
  newest K records in existing append order. Delegation applies the parent's
  setting to child builders; internal records share the same window. Retries
  retain one record for the frozen request.
- Consumers reading `SessionSnapshot.manifests` still read
  `Vec<TurnManifest>`. They now see the ordered recent suffix, at most K
  planned-step records, including internal turns. `recent_manifests()` returns
  that same owned suffix. This is not lifetime retention, and empty does not
  imply no provider activity. Eviction changes no history, usage, identities,
  signatures, or execution extension state. A larger future K cannot restore
  evicted evidence.
- The runtime-owned `runtime.manifest_boundary` extension is RedactionSafe,
  revision `manifest-boundary-1`, schema 1, and contains only the checked u64
  `planned_steps` frontier. Its namespace is publicly exported as
  `agent_runtime::runtime::MANIFEST_BOUNDARY_NAMESPACE`, and additive value
  fields are ignored by this reader. Stores must preserve it. Terminal overlay
  compares validated counters and all prior identity/history-role/usage/extension
  revision checks. Two legacy forms also require original full-list equality;
  mixed pairs compare the counter with the full legacy length before pruning.
  A record below its retained full-list length is repaired from that legacy
  evidence; malformed, stripped required, incompatible, and overflow evidence
  still fails explicitly.
- All newly produced direct/local-action/child/internal/cache checkpoints omit
  manifests in memory and JSON, preserving their execution fields. Protected
  recovery owns exact requests/actions/history/outcomes/extensions. A valid
  ordinary store at or behind the protected frontier supplies only its older,
  trimmed diagnostic suffix; stale diagnostics cannot attest execution, and an
  incompatible equal legacy pair is ignored rather than failing recovery. The
  in-flight request manifest cannot be recovered from a manifest-free
  checkpoint and is not invented. Without ordinary state, legacy checkpoint
  diagnostics provide a bounded fallback and new checkpoint-only recovery
  exposes an empty window.
- Generic deserialization stays lossless. Legacy checkpoints are never rewritten
  at their existing revision just to remove diagnostics. Same-state Planning
  comparisons normalize diagnostic lists and legacy/new markers on both sides;
  an unchanged boundary retains its original record. Genuine extension progress
  advances exactly one revision. Schema 3 and transition revision 4 remain.
- Runtime-produced checkpoint captures do not clone the retained manifest
  window, and transition comparison clones the current checkpoint only when it
  still has manifests or lacks the boundary record.
- Cache planning remains independent: the previous-cache extension and the
  planner's committed plan retain their existing ownership. The cache fixture
  makes more than K planned calls, derives maintenance from the committed plan,
  and recovers ResultReady without ordinary diagnostics or repeated I/O.
- Frozen absent/unbounded snapshot and checkpoint JSON forms carry the pre-U3
  wire representation. A frozen reader shape and terminal-overlay check model
  the pre-U3 behavior from the design's baseline. Both readers parse legacy and
  new forms; the old check rejects a new manifest-free checkpoint paired with
  nonempty ordinary diagnostics. This is a reader/check model, not an old
  runtime binary build. Supported products must pin the gated runtime and
  explicitly accept retention; unchanged schema numbers do not promise
  execution downgrade.

## Branch facts and deviations from design.md

The audit required two deviations from the originally approved design wording;
the design note now records both. First, a boundary record below a longer
retained list is rollback evidence to repair, rather than a regressed-counter
failure. The repair is exact only while the longer list exposes the under-count.
Second, nonterminal recovery may carry diagnostics from a valid ordinary
snapshot behind the protected frontier; an incompatible equal legacy pair is
ignored because diagnostics are not execution authority. Terminal overlay
remains strict. No checkpoint representation, schema/transition, store, error,
or security contract otherwise changed.

These implementation details use this branch's actual ownership and existing
surfaces:

1. Planning returns `ContextError`, not `RuntimeError`. Boundary validation and
   overflow use the existing `harness_context_error` conversion at the append
   owner; the conversion implementation and error definitions were not edited.
2. Equivalent replay here is an existing `RunManifest` evidence/revision check;
   there is no session historical-replay command or manifest lookup API. The
   host must locate a retained/archived record, return an existing structured
   Conflict if it is unavailable, and check revisions before I/O. The fixture
   proves this preflight and existing mismatch behavior. No archive or replay
   service was added.
3. LCM startup repair/import can itself write an ordinary snapshot. Those
   already-validated writes initialize the frontier and apply the window too.
   LCM error mappings were not changed.
4. Old-reader execution behavior is tested with the frozen serde shape and
   prior overlay predicate, rather than compiling an unavailable old binary.
   No old-binary execution compatibility is claimed.

## Public Rust source compatibility and merge scope

Source-breaking public item shape changes: **none**. No existing public fields,
enum variants, signatures, commands, event vocabulary, or store traits changed.
The additive runtime methods and
`agent_runtime::runtime::MANIFEST_BOUNDARY_NAMESPACE` constant require no
literal, destructuring, or exhaustive match migration. Consumers using lifetime
manifest retention must archive records or select an adequate finite window,
and redacting stores must preserve the boundary namespace.

The parallel failure-class job's error definitions, provider-error conversion,
and LCM error mappings are untouched. Merge-check
`crates/agent-runtime/src/agent/driver/provider.rs`: only the manifest append
owner changed, including calls to an existing context-error conversion. Also
merge-check `agent/driver/mod.rs` (private window field/constructor argument;
its existing context-error conversion is unchanged) and `agent/driver/turn.rs`
(checkpoint capture/transition and focused test; no error mapping changed).
`runtime/engine.rs` also gained startup retention/boundary logic and bounded
LCM repair saves, without modifying LCM error conversion.

## Validation

All Cargo builds/tests use:

```sh
export TMPDIR=/Volumes/Data/tmp CARGO_TARGET_DIR=$PWD/target CARGO_NET_OFFLINE=true
```

Scratch generators/logs live under `/Volumes/Data/tmp`. The configured cache
wrapper reported an unwritable cache and disabled caching. Later commands also
set `RUSTC_WRAPPER=/usr/bin/env`; this does not change the build/test contracts.

| Exact command | Result |
| --- | --- |
| `cargo test -p agent-runtime-testkit --test runtime_conformance` | Passed: 73 tests. |
| `cargo test -p agent-runtime-testkit --lib` | Passed: 81 tests before the final refresh fixture; the final workspace run passed all 82 library tests. |
| `cargo test -p agent-runtime manifest_tests` | Passed: direct private-owner same-state refresh regression. |
| `cargo test -p agent-runtime-testkit --test consumer_smith --test consumer_open_forge --test consumer_nyx` | Passed: Nyx 2, Forge 2, Smith 4 tests (8 total). |
| `cargo test -p agent-runtime-testkit --lib conformance::replay` | Passed: 3 replay/reproducibility tests. |
| `cargo test -p agent-runtime-testkit event_schema` | Passed: 13 event-schema compatibility tests. |
| `cargo fmt --all -- --check` | Passed; final repeat after the last test addition also passed. |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed; final repeat after the last test addition also passed. |
| `cargo test --workspace` | Exit 101: three existing TCP bridge tests failed under the socket restriction. Full gate remains unavailable. |
| `cargo test --workspace --doc` | Passed: 7 doc tests; 1 pre-existing ignored example. |
| `cargo deny --offline check` | Exit 1: all-target cargo metadata needs uncached `windows v0.61.3`; offline mode prevented its download. Full multi-target audit not completed. |
| `cargo +1.86.0 build -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm -p agent-runtime-obs -p agent-runtime --all-features` | Passed on installed Rust 1.86.0; no download. |
| `cargo +1.86.0 test -p agent-runtime-provider --features command-provider` | Passed on installed Rust 1.86.0, including command-provider architecture tests. |
| `cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli --all-features` | Passed on installed Rust 1.88.0; no download. |
| `cargo test -p agent-runtime-testkit --lib legacy_planning_refresh_recovers_equivalently_across_diagnostic_windows` | Passed: real legacy Planning recovery has identical execution checkpoints/revisions at K=2 and K=32. |
| `cargo check -p agent-runtime-core -p agent-runtime -p agent-runtime-testkit --all-targets` | Passed after the audit revisions. |
| `cargo test -p agent-runtime-testkit --lib conformance::manifests::tests` | Passed: 17 focused manifest tests, including rollback under-count, K/K+1, incompatible legacy diagnostics, and lagging-crash recovery. |
| `cargo test -p agent-runtime manifest_tests` | Passed: 1 private checkpoint normalization/refresh test; unrelated integration targets were filtered out. |
| `cargo clippy -p agent-runtime-core -p agent-runtime -p agent-runtime-testkit --all-targets --all-features -- -D warnings` | Passed after the audit revisions. |
| `cargo test --workspace --no-fail-fast -- --skip bridge_tests::bridge_denial_returns_an_error_result_and_emits_tool_events --skip bridge_tests::bridge_dispatches_success_and_keeps_tool_results_out_of_history --skip bridge_tests::bridge_rejects_wrong_tokens_and_closes_after_the_turn` | Passed: 1,387 tests, 1 existing ignored example, only the 3 TCP bridge cases filtered. |
| `cargo deny --offline --target aarch64-apple-darwin check licenses bans sources` | Passed: host-target licenses, dependency bans, and source checks; existing duplicate-version warnings remain. |
| `cargo deny --offline --target aarch64-apple-darwin check` | Exit 1: advisory database requires an exclusive lock under read-only `/Users/mai1015/.cargo/advisory-dbs`. Advisory check not completed; no network refresh attempted. |
| `python3 /Users/mai1015/.codex/skills/spec-toolkit/scripts/spec_toolkit.py validate bound-session-manifests-2026-10-01 --type change --strict` | Passed (`Valid`). |
| `git --no-optional-locks diff --check` | Passed. |
| `cargo fmt --all` | Passed during implementation. |
| `cargo run --offline --manifest-path /Volumes/Data/tmp/u3-fixture-generator/Cargo.toml` | Passed; generated the frozen pre-U3 wire forms under `/Volumes/Data/tmp` before copying the four JSON artifacts into testkit. |

The excluded workspace cases are:

- `bridge_tests::bridge_denial_returns_an_error_result_and_emits_tool_events`
- `bridge_tests::bridge_dispatches_success_and_keeps_tool_results_out_of_history`
- `bridge_tests::bridge_rejects_wrong_tokens_and_closes_after_the_turn`

They exercise the existing loopback listener in `agent/driver/external_bridge.rs`;
no bridge code or test was changed. The filtered rerun exercises every other
workspace test without permitting sockets. The full unfiltered gate, full
multi-target dependency audit, and advisory check must run in an environment
with the required socket/cache access before release.

Development iterations also ran
`cargo test -p agent-runtime-testkit --test runtime_conformance --no-fail-fast`
(initial compile failed while the new RuntimeError checks lacked the existing
ContextError conversion), and
`cargo test -p agent-runtime-testkit --lib conformance::manifests` (9 early
proofs passed; later iteration caught a private child entry point in a test,
which was replaced with the public session flow plus the delegation fixture).
The full `--lib` run caught the cache fixture's missing finite deadline; it was
corrected and all tests passed. Two queued intermediate `--lib` builds were
interrupted (exit 130) before the successful final run. Fixture generation
initially failed on a three-argument call to the branch's two-argument
`usage_event`; the successful generator uses the actual branch signature.


## Task status and follow-ups

Completed: 1.2, 1.3, 2.1–2.4, 3.1–3.3, and 4.1–4.3 (12 of 14 items).

- 1.1 remains unchecked only for the consumer owners' archival/retention
  decisions. The approved default, finite configuration, and accessor are
  implemented and the neutral consumer retention fixtures pass.
- 3.4 remains unchecked for external product/source/retention gates and
  immutable release eligibility, the three unavailable TCP tests, and the full
  multi-target/advisory audit limitations listed above.
  Actual Forge, Smith, and Nyx products were not compiled here.
- Run actual product projection/retention checks and decide archival policy;
  preserve the RedactionSafe marker in their stores, then pin a tag or exact
  landed runtime revision. Review execution downgrade limitations with those
  owners before a compatible release.
- Ordinary snapshots still contain history plus K history-sized diagnostics;
  checkpoints still contain exact history and CallingModel's full request.
  Delta persistence, journaling, and duplicate history/request optimization
  remain separate work (including U11); no percentage size reduction is claimed.

## Changed files

| File | Change |
| --- | --- |
| [CHANGELOG.md:14](/Volumes/Data/codes/ai/agent-runtime-u3/CHANGELOG.md:14) | Retention/downgrade migration entry. |
| [crates/agent-runtime-core/src/checkpoint/mod.rs:553](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-core/src/checkpoint/mod.rs:553) | Document diagnostic exception to exact snapshot state. |
| [crates/agent-runtime-core/src/store.rs:220](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-core/src/store.rs:220) | Document the unchanged public manifest vector. |
| [crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1944](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1944) | Protected-only over-window cache recovery fixture. |
| [crates/agent-runtime-testkit/src/conformance/delegation/durable_recovery.rs:1067](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/delegation/durable_recovery.rs:1067) | Inherited child-window fixture. |
| [crates/agent-runtime-testkit/src/conformance/fixtures/legacy-checkpoint-absent.json:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/fixtures/legacy-checkpoint-absent.json:1) | Frozen legacy JSON. |
| [crates/agent-runtime-testkit/src/conformance/fixtures/legacy-checkpoint-unbounded.json:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/fixtures/legacy-checkpoint-unbounded.json:1) | Frozen legacy JSON. |
| [crates/agent-runtime-testkit/src/conformance/fixtures/legacy-snapshot-absent.json:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/fixtures/legacy-snapshot-absent.json:1) | Frozen legacy JSON. |
| [crates/agent-runtime-testkit/src/conformance/fixtures/legacy-snapshot-unbounded.json:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/fixtures/legacy-snapshot-unbounded.json:1) | Frozen legacy JSON. |
| [crates/agent-runtime-testkit/src/conformance/manifests.rs:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/manifests.rs:1) | Reusable storeless, file-store, protected LCM crash, and exact K/K+1 fixtures. |
| [crates/agent-runtime-testkit/src/conformance/manifests/legacy_reader.rs:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/manifests/legacy_reader.rs:1) | Frozen old serde shape and terminal overlay predicate. |
| [crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:1) | Legacy, rollback, lagging-crash, mixed, malformed, overflow, replay, and retention proofs. |
| [crates/agent-runtime-testkit/src/conformance/mod.rs:17](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/src/conformance/mod.rs:17) | Conformance module registration. |
| [crates/agent-runtime-testkit/tests/consumer_nyx.rs:28](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/tests/consumer_nyx.rs:28) | Consumer retention/recovery gate. |
| [crates/agent-runtime-testkit/tests/consumer_open_forge.rs:29](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/tests/consumer_open_forge.rs:29) | Consumer retention/recovery gate. |
| [crates/agent-runtime-testkit/tests/consumer_smith.rs:85](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime-testkit/tests/consumer_smith.rs:85) | Consumer retention/recovery gate. |
| [crates/agent-runtime/src/agent/driver/mod.rs:461](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/agent/driver/mod.rs:461) | Private runtime window plumbing. |
| [crates/agent-runtime/src/agent/driver/provider.rs:656](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/agent/driver/provider.rs:656) | Checked frontier increment and bounded append. |
| [crates/agent-runtime/src/agent/driver/turn.rs:88](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/agent/driver/turn.rs:88) | Allocation-aware manifest-free checkpoints, refresh normalization, regression test. |
| [crates/agent-runtime/src/delegation/lifecycle.rs:1099](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/delegation/lifecycle.rs:1099) | Inherit the parent window. |
| [crates/agent-runtime/src/runtime/builder.rs:8](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/builder.rs:8) | Positive finite builder setting and default. |
| [crates/agent-runtime/src/runtime/engine.rs:28](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/engine.rs:28) | Boundary-aware recovery, lagging diagnostic carry, and bounded adoption/LCM repair saves. |
| [crates/agent-runtime/src/runtime/manifests.rs:1](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/manifests.rs:1) | Retention/boundary validation, rollback repair, and compatible diagnostic carry helpers. |
| [crates/agent-runtime/src/runtime/mod.rs:14](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/mod.rs:14) | Module registration and public boundary namespace export. |
| [crates/agent-runtime/src/runtime/session/cache.rs:89](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/session/cache.rs:89) | Manifest-free cache checkpoint captures. |
| [crates/agent-runtime/src/runtime/session/lifecycle.rs:132](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/session/lifecycle.rs:132) | Recent accessor and bounded ordinary captures. |
| [crates/agent-runtime/src/runtime/state.rs:519](/Volumes/Data/codes/ai/agent-runtime-u3/crates/agent-runtime/src/runtime/state.rs:519) | Correct retained-window field documentation. |
| [docs/development.md:175](/Volumes/Data/codes/ai/agent-runtime-u3/docs/development.md:175) | Focused gate commands. |
| [docs/migration-0.1.md:124](/Volumes/Data/codes/ai/agent-runtime-u3/docs/migration-0.1.md:124) | Retention, required marker, replay and downgrade migration. |
| [docs/spec/changes/bound-session-manifests-2026-10-01/implementation-report.md:1](/Volumes/Data/codes/ai/agent-runtime-u3/docs/spec/changes/bound-session-manifests-2026-10-01/implementation-report.md:1) | Implementation/compatibility/gate report. |
| [docs/spec/changes/bound-session-manifests-2026-10-01/tasks.md:3](/Volumes/Data/codes/ai/agent-runtime-u3/docs/spec/changes/bound-session-manifests-2026-10-01/tasks.md:3) | Completed items and external-gate reasons. |

Suggested commit: `feat(runtime): bound session manifest retention and exclude checkpoint diagnostics`
