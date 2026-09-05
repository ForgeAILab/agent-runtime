## 1. Boundary

- [ ] 1.1 Add `agent::external` with `ExternalAgentBackend`,
  `ExternalTurnRequest`, `ExternalTurnStream`, `ExternalAgentEvent`, and
  `ExternalSessionId`, behind an `external-agent` feature that no default
  dependency graph pulls in.
- [ ] 1.2 Re-export the boundary from the facade so an adopter never depends on
  an internal module path.

## 2. Dispatch

- [ ] 2.1 Add `external` to `Driver`, threaded from the builder, defaulting to
  `None`.
- [ ] 2.2 Branch `Driver::run_turn` once, constructing either the existing
  `TurnMachine` or the new `ExternalTurnMachine` over the same context.
- [ ] 2.3 Implement `ExternalTurnMachine`: consume the stream, accumulate text,
  project events onto canonical events, honor cancellation, and complete the
  turn exactly once.

## 3. Continuity and usage

- [ ] 3.1 Read and write the continuation identity in extension state, and
  offer it on the next turn.
- [ ] 3.2 Record reported usage through the ordinary usage path with its cache
  breakdown, attributed to the turn.

## 4. Conformance

- [ ] 4.1 Scripted-backend fixtures: text turn, tool observation, failure,
  cancellation, usage, and continuation across two turns including the
  identity-replaced case.
- [ ] 4.2 Assert an external turn records no provider attempt, no context plan,
  and no tool-authority dispatch.
- [ ] 4.3 Assert a runtime built without a backend is byte-identical in behavior
  to before the change.

## 5. Gates

- [ ] 5.1 Formatting, all-target/all-feature Clippy with warnings denied,
  workspace tests, dependency policy, and the declared MSRV check.
