---
created_at: 2026-08-02T06:09:22Z
updated_at: 2026-08-02T07:57:20Z
completed_at:
---

## 0. Prerequisites and Compatibility Baseline

- [x] 0.1 Confirm Sections 1 through 8 of
  `stabilize-session-harness-pipeline-2026-07-31` remain complete and capture
  ordinary send/history/event/persistence fixtures before goal changes.
- [x] 0.2 Record the coordinated approved Smith change and keep consumer-owned
  commands, presentation, eligibility policy, and output formats outside the
  runtime.

## 1. Goal Contracts and Pure Lifecycle

- [x] 1.1 Add bounded versioned goal identity, state, status, stopped reason,
  usage provenance, token budget, active elapsed time, timestamps, and public
  projection types.
- [x] 1.2 Add pure validated create/replace/edit/budget/pause/resume/block/
  complete/limit/clear transitions with stale-generation and one-goal rules.
- [x] 1.3 Add typed host goal commands, results, busy/conflict/compatibility
  errors, and facade exports without product policy or presentation text.
- [x] 1.4 Add schema/golden fixtures and exhaustive state-machine tests,
  including budget-limited resume and malformed persisted state.

## 2. Standard Goal Ability and Component

- [x] 2.1 Implement descriptor-first `get_goal`, `create_goal`, and
  `update_goal` tools with bounded schemas, explicit-intent guidance, and no
  user-control transitions in the model contract.
- [x] 2.2 Implement the versioned goal harness component for exact tool-output
  processing, namespaced state patches, no-cache context contribution, and
  typed goal events.
- [x] 2.3 Implement host control/query through the same component transition
  path, with durability-aligned persistence and no raw parallel extension-state
  mutation.
- [x] 2.4 Add tool/component tests for ordinary no-goal turns, create/get/
  complete/block, invalid and stale mutations, context bounds, event ordering,
  and discarded mutations.

## 3. Internal Turn Admission

- [x] 3.1 Add bounded provenance-bearing internal turn input/source types to
  accepted turn/checkpoint/manifest schemas while retaining ordinary
  `UserInput` compatibility.
- [x] 3.2 Update planning so internal content is required attributed tail
  context and never appends a user-role canonical history message.
- [x] 3.3 Add serialized `try_send_internal_if_idle` with accepted, busy,
  stale, and shutdown results; never queue internal work behind user work.
- [x] 3.4 Add deterministic admission-race, history, policy-equivalence,
  interruption, checkpoint, and resume tests.

## 4. Goal Accounting and Controller

- [x] 4.1 Attribute provider-reported uncached-input/output usage to goal-owned
  attempts exactly once while excluding cached input and preserving unknown
  evidence.
- [x] 4.2 Track derived active serving time without idle, stopped, or process
  downtime and finalize it on every terminal/control boundary.
- [x] 4.3 Enforce observed budget-limited and accounting-unavailable stopped
  transitions before another continuation is admitted.
- [x] 4.4 Implement the reusable process-scoped goal controller with restored
  initial projection, identity/generation deduplication, one idle continuation,
  real-user priority, and bounded shutdown.
- [x] 4.5 Add deterministic completion, blocker, budget overshoot, missing
  usage, external usage limit, turn error, pause race, duplicate event, crash,
  resume, and shutdown controller tests.

## 5. Persistence, Events, and Conformance

- [x] 5.1 Persist goal state through canonical extension state, completed-turn
  snapshots, and protected checkpoints; reject incompatible required revisions
  without clearing state.
- [x] 5.2 Add versioned typed goal/internal-turn events and event-schema golden
  fixtures with bounded privacy-safe projections.
- [x] 5.3 Add reusable testkit goal conformance covering tool, lifecycle,
  accounting, admission, controller, persistence, and replay invariants.
- [ ] 5.4 Pass the coordinated Smith consumer suite against the exact runtime
  revision and record the compatibility evidence.
  - Local sibling-checkout conformance and the complete Smith workspace pass;
    an immutable revision remains pending the runtime commit/release.

## 6. Documentation and Verification

- [x] 6.1 Update README, changelog, migration/compatibility documentation, and
  examples with process-scoped goal semantics and explicit non-goals.
- [x] 6.2 Run formatting, warning-denied Clippy, workspace/all-feature tests,
  privacy/security suites, schema fixtures, MSRV checks, Smith consumer tests,
  strict change validation, and diff hygiene.
