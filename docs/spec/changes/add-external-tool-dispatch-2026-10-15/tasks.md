---
created_at: 2026-10-15T20:40:12Z
updated_at: 2026-10-15T21:06:27Z
completed_at:
---

Port of `db8dff2` (`add-external-tool-dispatch-2026-10-13`) onto main.
Use that commit as the reference implementation (`git show db8dff2 --
<path>`). Where main has moved, keep main's code and re-apply the commit's
intent. Do not take the compat line's version of a file wholesale.

## 1. Handle and dispatch

- [x] 1.1 `ExternalToolDispatch` (the compat line's `ExternalToolBridge`
  handle) in `agent::external`: `specs()`, async `call(name, arguments)`,
  `disabled()`, `Clone + Send + Sync + Debug`; `tools` field on
  `ExternalTurnRequest` beside main's `bridge`; facade re-export. Main's
  `ExternalToolBridge` descriptor and `RUNTIME_BRIDGE_SERVER_NAME` are
  unchanged.
- [x] 1.2 Builder option for host-excluded names; the handle lists the
  authorized view less exclusions, in registration order.
- [x] 1.3 `ExternalTurnMachine` multiplexes backend events with handle
  calls, one at a time, through the ordinary tool path, with the ordinary
  events and no provider attempt. It still starts and stops the MCP bridge
  server as on main.
- [x] 1.4 History commits for dispatched rounds, in order around backend
  text; failed or cancelled turns keep committed rounds. MCP-bridged calls
  add nothing to history.
- [x] 1.5 Abandonment by phase and turn cancellation, as in the original
  task 1.5. The handle refuses calls after the terminal event.
- [x] 1.6 Checkpoint: additive `ExternalToolCall` state, transition, and
  validation; schema stays 3 and revision stays 4; port the constant doc
  comments and checkpoint tests.
- [x] 1.7 Redaction-safe continuation identity, with legacy sensitive
  identities preserved on restore.
- [x] 1.8 Settle-before-terminal: port the output-processing
  cancellation/deadline tests first, then port only the `tool_output.rs` /
  `tools.rs` code needed to make them pass. Main's
  `failed_tool_step_pairing.rs` (`c20e928`) passes unchanged.
  (Assessed 2026-10-15: not redundant. On main, `await_harness_phase(..)?`
  in the tool-output loop of `driver/turn.rs` returns on cancellation before
  `commit_tool_result`, so the real result and `ToolCallCompleted` are lost.
  `c20e928` closes such a call later, with an error result. All 11
  output-processing tests pass with the port.)

## 2. Internal turns

- [x] 2.1 `run_internal_turn` on a session with a backend runs
  `ExternalTurnMachine`, with delivery text and no user message.
- [x] 2.2 Tests: child completion runs on the backend; no fabricated user
  message; no replay; recovery parity with provider sessions.

## 3. Observations, activation, diagnostics

- [x] 3.1 Bounded backend observation log in the session view, with
  dispatch-position markers, cleared at the terminal event, never persisted.
- [x] 3.2 `registry.activate` through the handle returns instruction text;
  activation through the provider loop and the MCP bridge is unchanged.
- [x] 3.3 Discovery bootstraps are listed on the handle only with live
  routing and an authorized non-tool capability.
- [x] 3.4 `Diagnostic` event projected to a turn-attributed Conflict error,
  bounded to 1,000 characters.

## 4. Recovery

- [x] 4.1 An interrupted external turn is finalized as interrupted, with no
  backend re-invocation and no replay of a dispatched or MCP-bridged call.
- [x] 4.2 Recovery tests from the original task 4.2.

## 5. Coexistence

- [x] 5.1 Test: one turn with both the MCP bridge (`runtime_tools`, feature
  `external-agent-bridge`) and the handle. The MCP call leaves no history,
  and the handle call commits.
- [x] 5.2 All of main's existing external-agent tests (including
  `agent-runtime-agent-cli/tests/{claude,codex}.rs`) pass unchanged except
  for adding `tools: ExternalToolDispatch::disabled()` where a request is
  built by hand.

## 6. Close

- [x] 6.1 Module docs in `agent/external.rs` and `driver/external_turn.rs`
  (which still say an external turn dispatches no tools), plus
  `docs/external-agents.md`: when to use the MCP bridge and when to use the
  handle. Add a CHANGELOG entry.
- [x] 6.2 Gate: `cargo fmt --all --check`, `cargo clippy --workspace
  --all-targets --all-features -- -D warnings`, `cargo test --workspace
  --all-features --no-fail-fast`, plus the CI `--no-default-features` and
  `external-agent`-without-`external-agent-bridge` builds.
  (2026-10-15: fmt, clippy, the three feature combinations, 1,800 tests,
  doc tests, and the 1.86 build pass. Under workspace feature unification,
  two testkit crash-boundary tests first overflowed their stacks. Eight
  port-added await sites are now boxed in `driver/{mod,recovery,tools,turn}.rs`.)

- [ ] 6.3 PR to main, CI green, merge. Archive this change.
  `add-external-tool-dispatch-2026-10-13` stays on the compat line.
