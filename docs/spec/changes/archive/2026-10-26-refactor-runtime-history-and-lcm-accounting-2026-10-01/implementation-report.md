# U11 part B implementation report

Test consolidation and scoped accounting hardening are implemented on `feat/u11b-test-consolidation`. Changes remain uncommitted. Tasks 3.1/3.2 and audit items 2.5/2.6 are complete; the compatible-release gates in 4.2/4.3 remain open. No consumer files or CHANGELOG.md were edited.

## Test targets, coverage, and isolation

| Package | Integration targets before → after | Default cases | All-feature cases | Ignored moved cases |
| --- | --- | --- | --- | --- |
| agent-runtime | 20 → 1 (`integration`) | 126 → 126 | 135 → 135 | 0 → 0 |
| agent-runtime-testkit | 7 → 4 (`conformance` + three consumers) | 177 → 177 | 177 → 177 | 0 → 0 |

All 27 original roots and qualified cases were captured before the first move. The before inventory is `/Volumes/Data/tmp/rt/u11b/inventory-before.txt`; the after inventory is `inventory-after.txt` in that directory. Every moved name differs only by the original target name added as its module prefix. Consumer names remain exact, including their case names: `consumer_open_forge` 5, `consumer_smith` 8, `consumer_nyx` 6. Conformance has 158 cases.

Default, all-feature, `external-agent`, and `external-agent-bridge` inventories match. Source/fixture comparison passes all 101 checks: moved assertions, async runtime attributes, gates, support modules, and SSE data are unchanged. Only seven provider fixture include paths and one corresponding rustfmt wrap differ. The exact-body scan covered all 312 integration test functions and found zero duplicate groups; no test was deleted or deduplicated.

Runtime modules are private. The testkit nested runtime modules retain their existing `super::*` scope by moving their root and support directory together. Both manifests use automatic test discovery with no explicit `[[test]]` entries; both Cargo manifests and `.github/workflows/ci.yml` remain byte-identical. Every moved path/target reference elsewhere in the repository was updated.

The runtime process-wide OnceLock in `lcm_expansion` only initializes an immutable RegistryRevision. Steering/injection OnceLocks are fixture-owned. Transitive testkit `NEXT_FILE` / `NEXT_STORE` atomics allocate distinct directory names using different prefixes, PID and counters; each store removes its own directory on Drop. Providers/stores/recorders/clocks/barriers are instance-owned. No environment mutation or shared mutable directory was found; normal parallel harness execution passed.

Detailed evidence: `inventory-proof.txt`, `isolation-proof.txt`, `isolation-before.txt`, `isolation-transitive.txt`, `duplicates.json`, and before/after metadata JSON under `/Volumes/Data/tmp/rt/u11b/`.

## Accounting hardening and unchanged contracts

- Current main already rebuilds and stores totals when authorized binding/component/DAG evidence mismatches. `mismatched_accounting_evidence_rebuilds_once_then_stays_warm` now checks all three mismatches against the independent full oracle: each sizes/reads the full range once, then the next call performs no reads or sizing. Existing authorization denial and strict decoder behavior remain intact.
- Successful provisional-tail `truncate_from` now clears the session accounting totals immediately, including active coverage. It leaves the generation/fingerprint sidecar intact. `truncated_store_tail_discards_removed_entry_totals` proves invalidation before any later append and equivalent totals after a replacement canonical tail, including a second warm evaluation. The test failed before the three-line invalidation call was added and passes after it.
- No public field, signature, serde form, dependency, persisted revision, LCM policy, or provider wire module changed. All part A public baseline files match current main. The separately landed U2 differences in core event/checkpoint roots remain intact. All three recorded part A hashes for persisted LCM fields, descriptor calculation and strict decoding match exactly. See `baseline-after.txt` and `/Volumes/Data/tmp/rt/u11-baseline.txt`.

## Remaining copies and measured allocations

The existing `/Volumes/Data/tmp/rt/u11-measure` harness was rerun offline with the same 256 messages, 1 KiB text bodies and four private captures. Results exactly match part A; consolidation changes none of these allocation measurements.

| Capture path | Message copies | Allocation/reallocation calls | Requested bytes |
| --- | ---: | ---: | ---: |
| Original private-capture baseline | 1,024 | 2,055 | 1,196,080 |
| Current fresh shared generation | 256 | 515 | 292,976 |
| Current already-materialized generation | 0 | 0 | 0 |

These measurements isolate private captures, and do not measure a complete turn, peak/live memory or CI speed. New/changed generations still materialize owned Message values. Owned history/snapshots, context fragments, fragment-to-message planning, ProviderRequest messages, CallingModel events and checkpoint serialization retain their existing copy boundaries. Examples: `runtime/history.rs:59`, `runtime/session/lifecycle.rs:161`, `agent/driver/turn.rs:86`, `agent/planning.rs:558`, `agent-runtime-context/src/planner.rs:648`, and `agent-runtime-context/src/plan.rs:272`. The actual integration harness count is 27 → 5, with no measured link-speed claim.

## Exact final commands and results

Working directory was this repository. Every invocation used `TMPDIR=/Volumes/Data/tmp`, `CARGO_TARGET_DIR=$PWD/target`, `CARGO_NET_OFFLINE=true`; no package/tool was downloaded or installed. The commands below ran in the final gate pass; the later scratch probes attempted safe alternatives to locked external workspaces. Logs and structured results are in `/Volumes/Data/tmp/rt/u11b/`; `commands-results.txt` contains the exact commands and log paths.

| Gate | Exact command | Exit | Passed | FAILED names |
| --- | --- | ---: | ---: | --- |
| focused-mismatch | `cargo test -p agent-runtime --lib harness::lcm::accounting_tests::mismatched_accounting_evidence_rebuilds_once_then_stays_warm -- --exact` | 0 | 1 | none |
| focused-truncation | `cargo test -p agent-runtime --lib harness::lcm::accounting::tests::truncated_store_tail_discards_removed_entry_totals -- --exact` | 0 | 1 | none |
| history | `cargo test -p agent-runtime --lib runtime::history::tests` | 0 | 3 | none |
| accounting | `cargo test -p agent-runtime --lib harness::lcm::accounting` | 0 | 10 | none |
| lcm-hooks | `cargo test -p agent-runtime --lib harness::lcm::tests` | 0 | 28 | none |
| runtime-integration | `cargo test -p agent-runtime --test integration --all-features` | 0 | 135 | none |
| testkit-conformance | `cargo test -p agent-runtime-testkit --test conformance` | 0 | 158 | none |
| schema-context-lcm | `cargo test -p agent-runtime-core -p agent-runtime-context -p agent-runtime-lcm --all-features` | 0 | 394 | none |
| consumer-open-forge | `cargo test -p agent-runtime-testkit --test consumer_open_forge` | 0 | 5 | none |
| consumer-smith | `cargo test -p agent-runtime-testkit --test consumer_smith` | 0 | 8 | none |
| consumer-nyx | `cargo test -p agent-runtime-testkit --test consumer_nyx` | 0 | 6 | none |
| fmt | `cargo fmt --all -- --check` | 0 | — | none |
| clippy | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 | — | none |
| workspace | `cargo test --workspace --all-features` | 0 | 1479 | none |
| docs | `cargo test --workspace --doc` | 0 | 7 | none |
| msrv-default | `cargo +1.86.0 build -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm -p agent-runtime-obs -p agent-runtime` | 0 | — | none |
| msrv-all-features | `cargo +1.86.0 build --all-features -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm -p agent-runtime-obs -p agent-runtime` | 0 | — | none |
| msrv-mcp-cli | `cargo +1.88.0 build --all-features -p agent-runtime-mcp -p agent-runtime-cli` | 0 | — | none |
| dependency-license | `cargo deny --offline check` | 1 | — | none |
| native-metadata | `cargo metadata --offline --all-features --filter-platform aarch64-apple-darwin --format-version 1` | 0 | — | none |
| native-license-bans-sources | `cargo deny --offline --all-features --target aarch64-apple-darwin --metadata-path /Volumes/Data/tmp/rt/u11b/native-metadata.json check bans licenses sources` | 0 | — | none |
| native-advisories | `cargo deny --offline --all-features --target aarch64-apple-darwin --metadata-path /Volumes/Data/tmp/rt/u11b/native-metadata.json check advisories` | 1 | — | none |
| measure-before | `cargo run --offline --manifest-path /Volumes/Data/tmp/rt/u11-measure/Cargo.toml --bin before` | 0 | — | none |
| measure-after | `cargo run --offline --manifest-path /Volumes/Data/tmp/rt/u11-measure/Cargo.toml --bin u11-measure` | 0 | — | none |
| spec-validation | `python3 /Users/mai1015/.codex/skills/spec-toolkit/scripts/spec_toolkit.py validate refactor-runtime-history-and-lcm-accounting-2026-10-01 --type change --strict` | 0 | — | none |
| external-open-forge-compile | `cargo check --offline --locked --manifest-path /Volumes/Data/codes/ai/open-forge/Cargo.toml -p forge-agent-host --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-open-forge-contracts | `cargo test --offline --locked --manifest-path /Volumes/Data/codes/ai/open-forge/Cargo.toml -p forge-agent-host --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml --test lcm_store --test revised_authority` | 101 | — | none |
| external-smith-compile | `cargo check --offline --locked --manifest-path /Volumes/Data/codes/ai/tui/Cargo.toml -p smith-runtime --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-smith-contracts | `cargo test --offline --locked --manifest-path /Volumes/Data/codes/ai/tui/Cargo.toml -p smith-runtime --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml --test host_session --test persistence --test context_windows` | 101 | — | none |
| external-nyx-compile | `cargo check --offline --locked --manifest-path /Volumes/Data/codes/ai/nyx/Cargo.toml -p nyx-runtime --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-nyx-contracts | `cargo test --offline --locked --manifest-path /Volumes/Data/codes/ai/nyx/Cargo.toml -p nyx-runtime --all-features --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml --test shared_conformance` | 101 | — | none |
| external-open-forge-scratch-compile | `cargo check --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/open-forge/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-smith-scratch-compile | `cargo check --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/smith/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-nyx-scratch-compile | `cargo check --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/nyx/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-open-forge-scratch-locked-baseline | `cargo check --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/open-forge/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 0 | — | none |
| external-nyx-scratch-locked-baseline | `cargo check --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/nyx/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml` | 101 | — | none |
| external-open-forge-scratch-contracts | `cargo test --offline --all-features --manifest-path /Volumes/Data/tmp/rt/u11b/consumer-probes/open-forge/Cargo.toml --config /Volumes/Data/tmp/rt/u11b/consumer-patch.toml --test lcm_store --test revised_authority` | 0 | 22 | none |

All final runtime/testkit/workspace test cases passed. Workspace all-feature execution includes 4 ignored cases in other targets/docs; standalone workspace doc execution includes 1 ignored example. None of the consolidated integration cases were ignored before or after. Clippy exited 0, so there are no base-lint exceptions. The pre-fix red-test run is separately recorded in `hardening-before-fix.log`: only `harness::lcm::accounting::tests::truncated_store_tail_discards_removed_entry_totals` failed and is now fixed.

## Release blockers and consumer follow-ups

- Full `cargo deny --offline check` exits 1 because `windows v0.61.3` is not cached. Native all-feature bans/licenses/sources passed; native advisories exits 1 because the advisory database lock is outside the writable sandbox. Full dependency/advisory verification remains required in an environment with the dependencies and writable database available.
- Direct external compile/contract commands used `--locked` and command-local candidate patches, then exited 101 when patching required a lockfile update. Original consumer trees and lockfiles were untouched. Scratch manifests reference original sources and update only scratch lockfiles; no repository clone was created. Forge needed its existing lockfile copied to scratch to preserve its already-selected yanked `spin v0.9.8` dependency; then its adapter compile and all 22 original LCM/authority cases passed.
- The local Smith (`tui`) adapter compile exits 101 with six errors in old semantic-summary imports/constants: ProtectedSemanticSummary, SemanticSummaryCoordinator, SummaryModel, SEMANTIC_SUMMARY_COMPONENT_ID, SEMANTIC_SUMMARY_PURPOSE, protected_semantic_summary_from_state, SEMANTIC_SUMMARY_IDLE_COMPACTION_PURPOSE, SemanticSummaryPolicy, SummaryModelRequest and SummaryModelResponse. These symbols were already absent from current main; U11B does not edit public exports. This consumer must complete its existing LCM migration/align with the supported main contract before release validation. Its contract gate remains blocked by compilation.
- The local Nyx scratch compile with its copied lockfile exits 101 for uncached `addr2line v0.26.1`; its compile/contract gates need the offline dependency set provisioned elsewhere.
- U11B requires no API/store/serde migration in Forge, Smith or Nyx. Their three existing named testkit CI commands are unchanged. A caller selecting a former runtime target uses `cargo test -p agent-runtime --test integration <old-target>::`; a caller selecting a former testkit conformance target uses `cargo test -p agent-runtime-testkit --test conformance <old-target>::`. Preserve existing feature flags.
- Release publication remains blocked until all compatible-release gates pass and an immutable landed pin exists. This run leaves changes uncommitted and does not publish.

## Changed files (path:line)

| File | Change |
| --- | --- |
| `crates/agent-runtime/tests/integration.rs:1` | New root for all runtime scenario modules |
| `crates/agent-runtime-testkit/tests/conformance.rs:1` | New root for the four conformance modules |
| `crates/agent-runtime/src/harness/lcm.rs:1158` | Clear totals after successful truncation |
| `crates/agent-runtime/src/harness/lcm/accounting.rs:28` | Private invalidation helper; regression at line 293 |
| `crates/agent-runtime/src/harness/lcm/accounting_tests.rs:188` | Mismatch rebuild/warm regression |
| `docs/spec/changes/refactor-runtime-history-and-lcm-accounting-2026-10-01/tasks.md:1` | Completed task tracking and part B evidence |
| `docs/spec/changes/refactor-runtime-history-and-lcm-accounting-2026-10-01/implementation-report.md:1` | This report |
| `crates/agent-runtime/tests/integration/active_turn_steering.rs:1` | Moved from `crates/agent-runtime/tests/active_turn_steering.rs` |
| `crates/agent-runtime/tests/integration/cache_admission.rs:1` | Moved from `crates/agent-runtime/tests/cache_admission.rs` |
| `crates/agent-runtime/tests/integration/cache_evidence.rs:1` | Moved from `crates/agent-runtime/tests/cache_evidence.rs` |
| `crates/agent-runtime/tests/integration/delta_coalescing.rs:1` | Moved from `crates/agent-runtime/tests/delta_coalescing.rs` |
| `crates/agent-runtime/tests/integration/external_agent.rs:1` | Moved from `crates/agent-runtime/tests/external_agent.rs` |
| `crates/agent-runtime/tests/integration/fetch_tool_integration.rs:1` | Moved from `crates/agent-runtime/tests/fetch_tool_integration.rs` |
| `crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:1` | Moved from `crates/agent-runtime/tests/interrupted_turn_admission.rs` |
| `crates/agent-runtime/tests/integration/invalid_tool_arguments.rs:1` | Moved from `crates/agent-runtime/tests/invalid_tool_arguments.rs` |
| `crates/agent-runtime/tests/integration/lcm_expansion.rs:1` | Moved from `crates/agent-runtime/tests/lcm_expansion.rs` |
| `crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs:1` | Moved from `crates/agent-runtime/tests/lcm_failed_turn_recovery.rs` |
| `crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1` | Moved from `crates/agent-runtime/tests/lcm_legacy_resume_integration.rs` |
| `crates/agent-runtime/tests/integration/lcm_unsigned_reasoning.rs:1` | Moved from `crates/agent-runtime/tests/lcm_unsigned_reasoning.rs` |
| `crates/agent-runtime/tests/integration/local_tool_actions.rs:1` | Moved from `crates/agent-runtime/tests/local_tool_actions.rs` |
| `crates/agent-runtime/tests/integration/obs_context_integration.rs:1` | Moved from `crates/agent-runtime/tests/obs_context_integration.rs` |
| `crates/agent-runtime/tests/integration/parallel_tool_execution.rs:1` | Moved from `crates/agent-runtime/tests/parallel_tool_execution.rs` |
| `crates/agent-runtime/tests/integration/reasoning_preservation.rs:1` | Moved from `crates/agent-runtime/tests/reasoning_preservation.rs` |
| `crates/agent-runtime/tests/integration/replay_and_persistence.rs:1` | Moved from `crates/agent-runtime/tests/replay_and_persistence.rs` |
| `crates/agent-runtime/tests/integration/safe_boundary_injection.rs:1` | Moved from `crates/agent-runtime/tests/safe_boundary_injection.rs` |
| `crates/agent-runtime/tests/integration/structured_output.rs:1` | Moved from `crates/agent-runtime/tests/structured_output.rs` |
| `crates/agent-runtime/tests/integration/tool_argument_redaction.rs:1` | Moved from `crates/agent-runtime/tests/tool_argument_redaction.rs` |
| `crates/agent-runtime-testkit/tests/conformance/delegation_conformance.rs:1` | Moved from `crates/agent-runtime-testkit/tests/delegation_conformance.rs` |
| `crates/agent-runtime-testkit/tests/conformance/goal_conformance.rs:1` | Moved from `crates/agent-runtime-testkit/tests/goal_conformance.rs` |
| `crates/agent-runtime-testkit/tests/conformance/provider_conformance.rs:1` | Moved from `crates/agent-runtime-testkit/tests/provider_conformance.rs`; seven relative fixture includes adjusted |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/interaction.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/local_action.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/local_action.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/provider_loop.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/provider_loop.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/recovery.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/session.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/session.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance/support.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance/support.rs` |
| `crates/agent-runtime-testkit/tests/conformance/runtime_conformance.rs:1` | Moved from `crates/agent-runtime-testkit/tests/runtime_conformance.rs` |
| `docs/spec/changes/archive/2026-07-24-add-registry-driven-context-runtime-2026-07-24/tasks.md:248` | Update moved source path or target invocation |
| `docs/spec/changes/archive/2026-07-26-add-reasoning-preservation-2026-07-26/tasks.md:14` | Update moved source path or target invocation |
| `docs/spec/changes/archive/2026-08-02-add-active-turn-steering-2026-08-02/evidence.md:11` | Update moved source path or target invocation |
| `docs/spec/changes/archive/2026-08-02-refactor-runtime-module-boundaries-2026-08-02/evidence.md:25` | Update moved source path or target invocation |
| `docs/spec/changes/bound-session-manifests-2026-10-01/implementation-report.md:128` | Update moved source path or target invocation |
