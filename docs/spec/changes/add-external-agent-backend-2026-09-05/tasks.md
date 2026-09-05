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
- [ ] 4.3 Assert a runtime built without a backend is byte-identical in behavior
  to before the change.

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

Task 4.3 stands on the feature gate: `external-agent` is off by default and the
dispatch branch is `#[cfg]`-gated, so a runtime built without it contains no
external path at all.
