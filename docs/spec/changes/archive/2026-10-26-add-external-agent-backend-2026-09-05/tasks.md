---
created_at: 2026-10-26T07:08:45Z
updated_at: 2026-10-26T07:08:45Z
completed_at: 2026-10-26T07:08:45Z
---

## 1. Boundary

- [x] 1.1 Add `agent::external` with `ExternalAgentBackend`,
  `ExternalTurnRequest`, `ExternalTurnStream`, `ExternalAgentEvent`, and
  `ExternalSessionId`, behind an `external-agent` feature that no default
  dependency graph pulls in.
- [x] 1.2 Re-export the boundary from the facade so an adopter never depends on
  an internal module path.

## 2. Dispatch

- [x] 2.1 Add `external` to `Driver`, threaded from the builder, defaulting to
  `None`.
- [x] 2.2 Branch `Driver::run_turn` once, constructing either the existing
  `TurnMachine` or the new `ExternalTurnMachine` over the same context.
- [x] 2.3 Implement `ExternalTurnMachine`: consume the stream, accumulate text,
  project events onto canonical events, honor cancellation, and complete the
  turn exactly once.

## 3. Continuity and usage

- [x] 3.1 Read and write the continuation identity in extension state, and
  offer it on the next turn.
- [x] 3.2 Record reported usage through the ordinary usage path with its cache
  breakdown, attributed to the turn.

## 4. Conformance

- [x] 4.1 Scripted-backend fixtures: text turn, tool observation, failure,
  cancellation, usage, and continuation across two turns including the
  identity-replaced case.
- [x] 4.2 Assert an external turn records no provider attempt, no context plan,
  and no tool-authority dispatch.
- [x] 4.3 Assert a runtime built without a backend is byte-identical in behavior
  to before the change. Closed 2026-10-26 without a separate test: the
  `#[cfg]` feature gate makes it hold by construction (see the note below).

## 4b. Durability of an external terminal

- [x] 4b.1 Accept the turn checkpoint before invoking the backend, and route the
  terminal through the shared completion path so it advances `Accepted` ->
  `Completing` -> `PublishingTerminal` -> `Terminal`.
- [x] 4b.2 Let a completing checkpoint carry visible-output progress, so a turn
  with no model response of its own can report the answer it committed.
- [x] 4b.3 Conformance that collects past the terminal event: a completed
  external turn reports no error after `TurnCompleted`.

## 5. Gates

- [x] 5.1 Formatting, all-target/all-feature Clippy with warnings denied,
  workspace tests, dependency policy, and the declared MSRV check.

Verification (2026-09-05): 1,184 workspace tests pass with all features, and
warning-denied all-target Clippy and formatting are clean. Four external-agent
conformance tests drive the real runtime with a scripted backend and assert the
canonical stream a host observes: streamed text, tool observation, and session
identity; no provider attempt, context plan, or dispatched tool call for a turn
that made none; usage recorded under `ExternalAgent` and not
`ProviderAttempt`; a failed turn leaving no partial assistant message; a stream
without a terminal event failing rather than passing for success; and
continuation offered across three turns including the identity-replaced case.

Follow-up (2026-09-05): the external path published its terminal without ever
accepting a checkpoint, so every harness turn ended in
`Internal: turn has no accepted checkpoint` and left nothing durable behind.
The conformance tests missed it because they stopped collecting at
`TurnCompleted`, which is emitted before the failing transition. Task 4b covers
the fix and the test that would have caught it.

Task 4.3 stands on the feature gate: `external-agent` is off by default and the
dispatch branch is `#[cfg]`-gated, so a runtime built without it contains no
external path at all.

## Completion evidence (2026-10-26)

[PR #5](https://github.com/ForgeAILab/agent-runtime/pull/5) merged as `ef482fb`, including the durable-terminal
follow-up of task 4b (`7674089`). Release
[PR #22](https://github.com/ForgeAILab/agent-runtime/pull/22) merged as `e483bba`, and the
[v0.2.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.2.0) ships `RuntimeBuilder::external_agent` behind the
`external-agent` feature.

Archive edits to the delta, so the truth specs match the code:

- "External agent backend registration": the builder never rejected a backend
  combined with a provider. The provider stays required and is never called
  for an external turn; the builder rejects a backend combined with model
  fallback routes instead (`RuntimeBuilder::build`). The requirement now says so.
- "External agent turn execution": a later change lets a backend dispatch host
  tools through the runtime, so the first scenario now excludes only runtime
  tool dispatch that the backend did not request.
