---
created_at: 2026-07-31T08:34:33Z
updated_at: 2026-07-31T13:49:42Z
completed_at:
---

## 0. Coordination and Baseline

- [x] 0.1 Approve this proposal and the coordinated Smith proposal before
  implementation.
- [x] 0.2 Reconcile the prepared-invocation and typed-permission work with
  `add-runtime-security-boundary-2026-07-24`; retain one authorization path and
  mark any superseded tasks explicitly.
- [x] 0.3 Record public API/event/checkpoint schema baselines and add
  compatibility fixtures before changing them.
- [x] 0.4 Block MCP, new skill sources, nested agents, and release publication
  until Sections 1 through 4 pass.
- [x] 0.5 Archive or explicitly rebase the completed-but-unarchived delegation
  and reasoning-preservation changes before modifying their shared capability
  specs.

## 1. Release-Gate Tests

- [x] 1.1 Add provider-request capture to the fake provider and implement
  `tool_loop_preserves_exact_provider_message_order`.
- [x] 1.2 Add
  `parallel_tool_calls_and_results_form_one_atomic_exchange` and
  `current_turn_continuation_is_never_compacted`.
- [x] 1.3 Add `two_sessions_do_not_share_cache_or_compaction_state` and
  `non_compacted_plan_does_not_reuse_prior_compaction_outcome`.
- [x] 1.4 Add `resume_preserves_all_historical_manifests` with three turns
  across a resume boundary.
- [x] 1.5 Add `interrupting_one_turn_allows_a_later_turn_to_complete` and assert
  that submission during shutdown returns an error.
- [x] 1.6 Add `retryable_partial_stream_is_discarded_from_transcript` and
  verify failed-attempt text does not set committed `visible_output`.
- [x] 1.7 Add `prepared_edit_authorizes_the_exact_canonical_path`,
  `tool_ability_permissions_cover_every_runtime_invocation`, and
  `approval_observes_cancellation_and_deadline`.

## 2. Context Order and Session Isolation

- [x] 2.1 Add `ContextPosition`/lanes and preserve sequence within the
  conversation lane independently of `FragmentKind`.
- [x] 2.2 Extract complete turn groups and multi-call `ToolExchange` values;
  populate all call IDs and make the latest active-turn suffix required.
- [x] 2.3 Update validation and structural compaction to retain or remove a
  complete exchange atomically.
- [x] 2.4 Move `RunPlanner`, prior cache plan, compactor state, activation
  epochs, and extension state into `SessionExecutionContext`.
- [x] 2.5 Replace `SemanticCompactor::last_outcome` with an owned
  `CompactionResult`; rename the deterministic implementation
  `StructuralCompactor` with a compatibility alias only for a bounded
  migration.
- [x] 2.6 Restore `snapshot.manifests` and all session-scoped revision state on
  resume.
- [x] 2.7 Emit `ContextCompacted` only from the outcome associated with the
  current plan.

## 3. Turn Control and Provider Attempts

- [x] 3.1 Introduce `TurnHandle` and make `send`/`run` return structured
  results; reject submissions once shutdown begins.
- [x] 3.2 Store a cancellation handle for every active turn and expose
  `interrupt_current_turn` separately from terminal `cancel_session`.
- [x] 3.3 Add request/attempt identity to text and reasoning deltas plus
  committed/discarded attempt-output terminal events.
- [x] 3.4 Accumulate `visible_output`, assistant text, and reasoning only from a
  committed attempt while retaining failed-attempt usage/diagnostics.
- [x] 3.5 Update event renderers, observers, schema fixtures, and reducer
  conformance for speculative output.

## 4. Prepared Tool Authority

- [x] 4.1 Define `PreparationContext`, `PreparedToolCall`, canonical argument
  and resource rules, display metadata, and preparation fingerprints.
- [x] 4.2 Split `Tool` into `spec`, `prepare`, and exact `invoke`; provide a
  migration adapter that is conservative and cannot claim argument-specific
  authority.
- [x] 4.3 Schedule calls from prepared effects/resources and authorize the
  concrete prepared resource before approval.
- [x] 4.4 Re-run preparation, validation, and authorization after any edited
  approval input; reject fingerprint mismatch before execution.
- [x] 4.5 Make approval waiting cancellation- and deadline-aware and represent
  decline, timeout, cancellation, and unavailable policy distinctly.
- [x] 4.6 Remove the ability-local string permission type, use
  `agent_runtime_registry::Permission`, and require prepared permissions to be
  a subset of the descriptor upper bound.
- [x] 4.7 Migrate built-in/test tools and declare shell's conservative broad
  workspace/process/network upper bound.

## 5. Checkpointable Turn Machine

- [x] 5.1 Define the versioned `TurnState`, transition revision, operation
  fingerprints, and idempotency rules.
- [x] 5.2 Refactor `Driver::run_turn` into an explicit `TurnMachine` without
  adding a general graph engine.
- [x] 5.3 Add a protected `CheckpointStore` and journal/checkpoint watermark;
  keep raw resumable state out of default observability events.
- [x] 5.4 Persist after every completed turn before enabling mid-turn recovery.
- [x] 5.5 Checkpoint accepted input, assembled model response, prepared pending
  actions, each committed tool result, and terminal completion.
- [x] 5.6 Restore each non-terminal state without repeating committed provider
  calls or tool side effects.
- [x] 5.7 Add crash/restart fixtures at every checkpoint boundary, including
  parallel calls with partially committed results.

## 6. Host Interaction and Questionnaire

- [x] 6.1 Define bounded, versioned interaction request/response types with
  stable question/choice IDs, deadlines, cancellation, and sensitivity.
- [x] 6.2 Add `InteractionBroker` and the `AwaitingInteraction` turn state;
  checkpoint exact pending requests and resume the same turn from an answer.
- [x] 6.3 Implement a standard questionnaire ability supporting one to three
  questions, mutually exclusive choices, optional free-form input, decline,
  timeout, cancellation, and unavailable-host results.
- [x] 6.4 Prove questionnaire responses cannot resolve approval, widen grants,
  or mutate prepared actions.
- [x] 6.5 Gate activation on host readiness and make non-interactive absence a
  structured result rather than an indefinite wait.
- [x] 6.6 Add redaction/replay tests for sensitive answers and pending
  questionnaire recovery.
- [x] 6.7 Attribute requests to session/turn/call identity and require explicit
  host policy before a child session can activate user interaction.

## 7. Live Ability and Harness Integration

- [x] 7.1 Build a session-scoped registry snapshot/view/initial retrieval and
  authorized activation epoch during session creation.
- [x] 7.2 Materialize the frozen epoch's tool schemas and instruction
  fragments at each provider boundary and advance epochs only at safe
  boundaries.
- [x] 7.3 Emit every declared registry, retrieval, activation, compaction, and
  planning lifecycle event from the live path.
- [x] 7.4 Register the current fixed tool fixture as abilities with accurate
  affordances, permission upper bounds, risk, cost, and readiness; test
  read-only versus editing activation.
- [x] 7.5 Add protected capability search for intent misses with bounded,
  dependency-complete activation.
- [x] 7.6 Implement ordered phase-specific component traits, deterministic
  topological sorting, protected phases, pipeline fingerprinting, immutable
  views, and explicit patches.

## 8. Standard Harness Components and Artifacts

- [x] 8.1 Add typed, versioned todo state, pure mutation, checkpointing, and
  `PlanUpdated` events.
- [x] 8.2 Add descriptor-first skill loading and memory contributors while
  leaving source precedence/trust policy to hosts.
- [x] 8.3 Define session-private `ArtifactStore`, `ToolContent`, bounded
  `artifact.read`, content hashes, retention, and permission checks.
- [x] 8.4 Offload oversized tool results as head/tail previews plus retrievable
  references instead of irreversible truncation.
- [x] 8.5 Add the asynchronous semantic-summary coordinator with dedicated
  model-purpose attribution, stored originals, provenance validation, and
  deterministic planner revalidation.
- [x] 8.6 Add scenario-level evaluations for complete read-only, editing,
  clarification, artifact-offload, compaction, delegation, retry, approval,
  and crash-resume workflows.
  - Evidence: `runtime_conformance` covers 61 full-loop scenarios, including
    activated read/edit tool loops, questionnaire, artifact readback,
    compaction, retry, approval, provider-switch cache rebasing, and every
    recovery boundary; the 14-case `delegation_conformance` suite includes
    parent-owned artifact transfer.

## 9. Compatibility and Release

- [x] 9.1 Update docs, examples, changelog, schema fixtures, MSRV checks, and
  consumer compatibility matrices.
  - Evidence: README, migration guide, changelog, development matrix, v5-v8
    event fixtures, and examples are updated; all production crates build with
    `cargo +1.86.0 build ... --all-features`.
- [x] 9.2 Run fmt, warning-denied Clippy, unit/integration/conformance tests,
  affected-consumer tests, and privacy/security adversarial suites.
  - Evidence: `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
    and `cargo test --workspace --all-features` pass, including Smith, Nyx,
    Open Forge, checkpoint, redaction, authorization, and artifact suites.
- [ ] 9.3 Pin and test the coordinated Smith revision before releasing the
  breaking runtime version.
- [ ] 9.4 Publish only after Sections 1 through 8 are complete or explicitly
  split into separately approved follow-up changes with no false claims in the
  public surface.
