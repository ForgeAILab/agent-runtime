---
created_at: 2026-08-02T16:32:19Z
updated_at: 2026-08-02T17:02:37Z
completed_at: 2026-08-02T17:02:37Z
---

## 0. Baseline and Coordination

- [x] 0.1 Approve this proposal before implementation and confirm no behavior
  change is concurrently editing the module selected for the current slice.
- [x] 0.2 Record supported exports/module paths, checkpoint and event schema
  fixtures, all existing runtime/delegation conformance scenario names, source
  line counts, and `too_many_arguments` suppressions.
- [x] 0.3 Run the focused driver, checkpoint, tool, ability, delegation,
  steering, goal, and runtime conformance suites to establish a green baseline.

## 1. Test-Only Extraction

- [x] 1.1 Move the embedded `check_set.rs` tests into a private
  `check_set/tests.rs` module without changing production code or coverage.
- [x] 1.2 Move the embedded `tool/executor.rs` tests into a private
  `tool/executor/tests.rs` module while retaining the centralized prepared
  execution pipeline.
- [x] 1.3 Move other embedded tests encountered in a selected production module
  into that module's private `tests.rs` as part of its slice.

## 2. P0 Direct Driver

- [x] 2.1 Convert `agent/driver.rs` into `agent/driver/mod.rs` as the stable
  construction/dispatch root and preserve all existing caller paths.
- [x] 2.2 Extract turn lifecycle, transition/checkpoint commit, terminal, and
  steering-boundary behavior into `driver/turn.rs`.
- [x] 2.3 Extract prepared/local tool execution and host interaction paths into
  `driver/tools.rs` without changing authorization or execution order.
- [x] 2.4 Extract checkpoint restoration and resume dispatch into
  `driver/recovery.rs` without repeating committed work.
- [x] 2.5 Extract context request construction, provider attempts, reasoning
  accumulation, retry disposition, and streamed output into
  `driver/provider.rs`.
- [x] 2.6 Introduce private cohesive context values where they remove existing
  long-parameter plumbing; add no new lint suppressions.
- [x] 2.7 Run driver, steering, goal, tool, interaction, checkpoint, retry,
  recovery, and runtime conformance gates before continuing.

## 3. P0 Delegation Runtime

- [x] 3.1 Move public contracts and private record/binding values into
  `delegation/types.rs`, re-exporting the supported surface from
  `delegation/mod.rs`.
- [x] 3.2 Move coordinator construction and public operation routing into
  `delegation/coordinator.rs`.
- [x] 3.3 Move catalog, child/checkpoint persistence, returned-input recovery,
  and artifact transfer into `delegation/persistence.rs`.
- [x] 3.4 Move spawn, bind, follow-up, resume, wait, stop, and authorization
  orchestration into `delegation/lifecycle.rs`.
- [x] 3.5 Move returned-input collectors, child monitors, deadline/parent
  watchdogs, capacity release, and queued starts into `delegation/monitor.rs`.
- [x] 3.6 Run delegation lifecycle, durability, authorization, depth/capacity,
  interaction return, artifact transfer, and shutdown conformance gates.

## 4. P0 Runtime Integration Conformance

- [x] 4.1 Turn `tests/runtime_conformance.rs` into a thin target harness and
  extract reusable fakes, fixtures, builders, clocks, barriers, and assertions
  into `tests/runtime_conformance/support.rs`.
- [x] 4.2 Group all existing scenarios into provider-loop, session,
  local-action, recovery, and interaction modules without renaming, deleting,
  ignoring, or weakening a scenario.
- [x] 4.3 Compare the integration test inventory with the baseline and run the
  complete `runtime_conformance` target.

## 5. P1 Checkpoint Core

- [x] 5.1 Convert `checkpoint.rs` into a stable `checkpoint/mod.rs` containing
  checkpoint data and re-exports.
- [x] 5.2 Move the complete exhaustive `TurnState` successor relation into
  `checkpoint/transition.rs`; keep the match together.
- [x] 5.3 Move state/checkpoint invariant checks, successor validation, and
  operation fingerprints into `checkpoint/validation.rs`.
- [x] 5.4 Move only the `CheckpointStore` contract into `checkpoint/store.rs`
  and embedded tests into `checkpoint/tests.rs`.
- [x] 5.5 Compare serialized fixtures/fingerprints with the baseline and run
  checkpoint, recovery, replay, and runtime conformance gates.

## 6. P1 Delegation Conformance

- [x] 6.1 Convert `conformance/delegation.rs` into a stable module root and
  move common stores, providers, factories, fixtures, and assertions into
  `conformance/delegation/support.rs`.
- [x] 6.2 Group implementations into lifecycle, returned-input,
  authorization, and durable-recovery modules while re-exporting every
  existing public scenario assertion from the root.
- [x] 6.3 Compare the public assertion inventory with the baseline and run all
  runtime and consumer delegation conformance targets.

## 7. P1 Live Abilities

- [x] 7.1 Convert `harness/live_abilities.rs` into a stable crate-visible root
  and separate session activation state/persisted projections into
  `live_abilities/session.rs`.
- [x] 7.2 Move restoration and rebase decisions into
  `live_abilities/rebase.rs`.
- [x] 7.3 Move selection, authorization, and materialization into
  `live_abilities/activation.rs`; move capability search and transactional
  staging into `live_abilities/search.rs`.
- [x] 7.4 Move embedded tests into `live_abilities/tests.rs` and run ability,
  registry, capability-routing, checkpoint/rebase, and runtime conformance.

## 8. Final Verification

- [x] 8.1 Confirm supported exports and module paths, serialized schemas,
  fingerprints, conformance scenario/assertion inventories, and observable
  event ordering match the baseline.
- [x] 8.2 Confirm no production behavior was duplicated, no new dependency or
  lint suppression was introduced, and unrelated modules remain unchanged.
- [x] 8.3 Run `cargo fmt --all -- --check`, warning-denied workspace Clippy,
  workspace all-feature tests, MSRV checks, schema compatibility, and all
  available consumer compatibility gates.
- [x] 8.4 Record verification evidence and final source line/suppression counts;
  mark complete only after every slice is green.
