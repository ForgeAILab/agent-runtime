---
created_at: 2026-10-14T01:20:00Z
updated_at: 2026-10-14T22:46:52Z
completed_at: 2026-10-14T22:46:52Z
---

Each item lands with its conformance tests in
`crates/agent-runtime/tests/external_agent.rs` or its
`external_agent/extensions.rs` sibling, driven by the scripted backend there.

## 1. Bridge and dispatch

- [x] 1.1 `ExternalToolBridge` in `agent::external`: `specs()`, an async
  `call(name, arguments)`, `disabled()`, `Clone + Send + Sync + Debug`; a
  `tools` field on `ExternalTurnRequest`; facade re-export.
- [x] 1.2 Builder option for the names the host excludes. The bridge lists
  the session's authorized tools, never a scope-denied one, less those
  names, in registration order.
- [x] 1.3 `ExternalTurnMachine` multiplexes backend events with bridged
  calls, one call at a time in arrival order, through the ordinary prepare,
  authorize, approve, execute, process, and commit path, emitting the
  ordinary tool events. No provider attempt. A refusal, denial, or failure
  goes back to the caller and the turn continues.
- [x] 1.4 History: text reported so far is committed as the assistant
  message carrying the call, then the result, then later text. A failed or
  cancelled turn keeps committed rounds.
- [x] 1.5 Abandonment by phase (queued: never runs; awaiting approval:
  policy decision future dropped/cancelled with no interaction events; awaiting
  broker interaction: real request withdrawn with Cancelled resolution, a
  later answer is a no-op; executing: completes and commits, nothing returned).
  Turn cancellation (user, deadline, shutdown) is distinct: execution receives
  the turn token and settles with provider cancellation history and latency.
  The handle refuses calls after the terminal event.
- [x] 1.6 Conformance tests for 1.1 to 1.5, including approval event parity
  with provider turns and cancellation of real broker interactions, executing
  tool cancellation history/event parity, cancellation-ignoring tool latency,
  abandonment versus turn cancellation on the same tool, and deadlines during
  execution even after the backend stream ends. Checkpoint
  compatibility tests preserve the HEAD schema (3) and transition revision
  (4), validate checkpoints in every pre-existing state, and cover the new
  state with and without external-agent. Build/test verification is delegated
  to the orchestrator.

- [x] 1.7 Mark the external continuation identity redaction-safe; audit the
  extension-state writes used by sections 1–4; preserve legacy sensitive
  identities on restore. Add snapshot-only restart tests through a store that
  drops sensitive extension state, including a committed bridged tool round.
  Build/test verification is delegated to the orchestrator.

- [x] 1.8 Settle requested tool results before terminal publication when turn
  cancellation or a deadline interrupts output processing; bypass processors
  on cancelled execution results and preserve raw outcomes for processing-time
  races across provider, bridged, internal, and local-action paths. Add output
  processor cancellation/deadline conformance tests; orchestrator verifies.
  (Verified 2026-10-14: `external_agent::execution::output_processing`, 11
  tests; workspace gate 1,304 tests, clippy and fmt clean.)

## 2. Internal turns

- [x] 2.1 `run_internal_turn` on a session with a backend runs
  `ExternalTurnMachine`: the internal input becomes delivery text, no user
  message is appended, attribution and outcome consumption are unchanged.
- [x] 2.2 Tests: child completion runs on the backend and not the provider;
  history has no fabricated user message; replay does not invoke the
  backend again. Includes provider-session admission parity, acceptance
  failure rollback, and fail-closed interrupted internal recovery. Build/test
  verification is delegated to the orchestrator.

## 3. Observations and activation

- [x] 3.1 A bounded log of the in-flight external turn's own tool
  observations, readable through the session view a tool receives, ordered
  relative to bridged calls, cleared at the terminal event.
- [x] 3.2 `registry.activate` through the bridge returns the instruction
  text of each skill it activated; verification failures are unchanged.
- [x] 3.3 Observation, activation, provider-result compatibility, and bound
  tests in `tests/external_agent/extensions.rs`; builds/tests delegated to the
  orchestrator.
- [x] 3.4 List and dispatch the discovery bootstraps only with live routing
  and an authorized non-tool capability, respecting host exclusions; test
  discovery visibility and authorized search results.
- [x] 3.5 Add non-terminal backend diagnostics, projected onto the existing
  turn-attributed Conflict error event with a 1,000-character bound; test
  attribution, bounds, normal completion, and unchanged history.

## 4. Recovery

- [x] 4.1 An external turn interrupted by process exit is finalized as
  interrupted; the backend is not invoked again and no bridged call is
  replayed, in any phase.
- [x] 4.2 Checkpoint/journal recovery tests drop the executor before terminal
  publication during streaming, approval, execution, a committed bridged
  round, and child completion; also cover every earlier protected checkpoint
  and compare interrupted child completion with the provider contract.
  Builds/tests delegated to the orchestrator.

## 5. Close

- [x] 5.1 Update the module documentation of `agent/external.rs` and
  `agent/driver/external_turn.rs`, which say an external turn dispatches no
  tools, and add a CHANGELOG entry.
- [x] 5.2 `cargo fmt --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo test --workspace --all-features`.
  (2026-10-14, on `feat/smith-external-tool-bridge`: 1,304 tests pass.)

Implementation choices for sections 3–5.1:

- The log is volatile session execution metadata; existing backend observation
  events retain their established journal projection. No snapshot, dropped
  counter, or bridge-position marker is journaled. An outcome whose invocation
  has fallen out of the retained log has an empty name rather than an inferred
  tool name; bridge markers never supply backend names.
- External search reuses scoped descriptive retrieval without materializing or
  staging a provider-context bundle. External activation reuses the existing
  unbounded explicit selector and the exact authorization/materialization path;
  provider activation is unchanged and has a byte-pinned integration test.
- Interruption uses the existing ResumeOrInterrupt contract: Failed terminal,
  Conflict error, and interrupted_on_resume attribution. Already determinate
  Completing/PublishingTerminal barriers retain their finish. Explicit Defer
  remains inspection-only; an actual resume always interrupts external work.
- A durably accepted child-completion turn consumes delivery exactly once,
  including on interruption. Its outcome remains in the child ledger, and its
  delivery is not requeued. The recovery test compares this with a provider
  session at the same crash window.
- The implementer ran only formatting, diff checks, and spec validation;
  the orchestrator ran the 5.2 gate.
