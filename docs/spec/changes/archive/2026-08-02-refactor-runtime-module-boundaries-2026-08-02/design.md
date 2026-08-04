## Context

The affected files currently concentrate these responsibilities:

| Current file | Current size | Planned ownership |
| --- | ---: | --- |
| `agent/driver.rs` | 4,951 lines | driver facade, turn lifecycle, tools/interactions, recovery, provider loop |
| `delegation/mod.rs` | 2,953 lines | contracts, coordinator, persistence, lifecycle, monitoring/capacity |
| `tests/runtime_conformance.rs` | 4,983 lines | shared support plus scenario-family modules |
| `checkpoint.rs` | 1,991 lines | types, transition relation, validation, store, tests |
| `conformance/delegation.rs` | 2,130 lines | shared support plus focused public scenario families |
| `harness/live_abilities.rs` | 1,482 lines | runtime/session state, rebase, activation, search/staging, tests |

`check_set.rs` and `tool/executor.rs` are large primarily because each embeds
more than 1,300 lines of tests. Their production implementations are cohesive;
the production boundaries do not change in this proposal.

## Goals / Non-Goals

### Goals

- Give each production module one recognizable reason to change.
- Keep stable public and crate-visible paths at thin module roots through
  re-exports where necessary.
- Keep exhaustive transition and security-sensitive execution logic together
  rather than scattering individual match arms or pipeline stages.
- Make test fixtures reusable within a suite without turning helpers into new
  public testkit contracts.
- Reduce merge-conflict hotspots and avoid growing the existing
  `too_many_arguments` suppression set.
- Make every move independently verifiable and easy to revert.

### Non-Goals

- Change runtime, recovery, delegation, activation, or security semantics.
- Redesign the turn machine or delegation coordinator.
- Change serialized checkpoint, event, manifest, or persisted extension-state
  representations.
- Rename supported public types or testkit assertion functions.
- Introduce new crates, third-party dependencies, generic middleware, or a
  source-line-count lint.
- Split `harness/pipeline.rs` or `agent-runtime-core/src/tool.rs`.

## Decisions

### Preserve module identity with thin roots

File modules may become directory modules, but callers continue to use the
same Rust paths. Root modules own public re-exports and the minimum orchestration
needed to make the boundary legible. Extracted implementation details use the
narrowest practical visibility, normally `pub(super)` or private.

Planned production layout:

```text
agent/driver/
  mod.rs          Driver construction and turn dispatch
  turn.rs         TurnMachine lifecycle, transitions, commits, steering
  tools.rs        prepared/local tool paths and interaction resolution
  recovery.rs     checkpoint restoration and resume dispatch
  provider.rs     request planning, provider attempts, streamed output

delegation/
  mod.rs          stable exports and coordinator facade
  types.rs        public contracts and internal record/binding values
  coordinator.rs  construction and public operation routing
  persistence.rs  catalogs, checkpoints, returned input, artifact transfer
  lifecycle.rs    spawn, bind, follow-up, resume, wait, stop
  monitor.rs      collectors, watchdogs, capacity release and queued starts

checkpoint/
  mod.rs          stable exports and core checkpoint data
  transition.rs   exhaustive TurnState successor relation
  validation.rs   checkpoint/state invariant validation and fingerprints
  store.rs        CheckpointStore contract
  tests.rs        unit tests

harness/live_abilities/
  mod.rs          stable crate-visible facade
  session.rs      session activation state and persisted projections
  rebase.rs       restoration and rebase decisions
  activation.rs   selection, authorization, and materialization
  search.rs       capability search and transactional staging
  tests.rs        unit tests
```

The exact private placement MAY shift during implementation when Rust privacy
or borrow boundaries make another responsibility-aligned placement clearer.
Any change to the stable surface or the responsibility list requires proposal
reapproval.

### Keep cohesive invariants in one place

The full `TurnState::can_transition_to` match remains in one transition module;
it is not split by state. Checkpoint validation remains one validation module.
The prepared tool execution pipeline stays in `tool/executor.rs`; only its
tests move. `check_set.rs` likewise retains its production implementation.

### Split tests by scenario family

The integration target remains `runtime_conformance`; its top-level file
becomes a small module harness. Support code moves under
`tests/runtime_conformance/support.rs`, and scenarios are grouped into
provider-loop, session, local-action, recovery, and interaction modules.

Reusable delegation conformance retains every existing public assertion name
through the `conformance::delegation` root. Its implementation is grouped into
support, lifecycle, returned-input, authorization, and durable-recovery
modules. Helpers stay private to the conformance module.

### Use private context values only for cohesive dependencies

Where an extracted operation currently threads the same large set of borrowed
runtime/session dependencies through several private functions, it MAY receive
a private context struct. Context values do not own new mutable state, change
lifetime, alter ordering, or become public contracts. Existing lint
suppressions are removed when the context makes them unnecessary; none are
added merely to complete a move.

## Risks / Trade-offs

- Rust privacy and borrow-checker constraints can tempt widened visibility.
  Review each extracted item and default to private or `pub(super)`.
- Large mechanical moves obscure accidental edits. Move one responsibility at
  a time and compare focused test/schema output after every slice.
- Test helpers can accidentally become shared contracts. Keep suite support
  below its existing module/target and avoid facade exports.
- Concurrent work in the same hotspots can create conflict-heavy merges.
  Start a slice only from a clean baseline and avoid overlapping behavior work
  in that file until the slice is green.
- Moving modules can perturb rustdoc paths or macro resolution even when types
  are re-exported. Compile public examples and run existing compatibility
  suites in addition to unit tests.

## Migration Plan

1. Record the current exports, schema fixtures, test inventory, and lint
   suppression count.
2. Extract test-only modules first to reduce later move noise.
3. Apply each production split as a standalone source move with private import
   and visibility repair only.
4. Run focused tests after each move and the complete workspace gates after
   each priority tier.
5. Compare the final public/schema/conformance inventory with the baseline and
   record evidence in the change directory.

No persisted data or consumer migration is required because supported paths
and wire contracts remain unchanged.
