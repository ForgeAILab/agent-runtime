## Context

The reference implementation is commit `db8dff2` on `feat/smith-web-tools`.
It was written against `d2752c1`; main has 121 further commits. A trial
`git cherry-pick -n db8dff2` onto main `5fb0507` conflicts in 12 files:
`CHANGELOG.md`, `agent-runtime-core/src/checkpoint/mod.rs`,
`agent-runtime/Cargo.toml`, `driver/{external_turn, mod, recovery, tools,
turn}.rs`, `agent/external.rs`, `runtime/{builder, state}.rs`, and
`tests/external_agent.rs`. It applies `tool_output.rs`, the
`tests/external_agent/*.rs` modules, and the checkpoint transition,
validation, and tests cleanly.

On main, three earlier changes touch the same ground:

- `0c06379` (#11) serves runtime tools to the CLI over a loopback MCP
  server: `ExternalToolBridgeServer` in `driver/external_bridge.rs`, with
  the public endpoint descriptor `ExternalToolBridge { url, bearer_token,
  tools }` in `agent/external_capabilities.rs`. Each call builds its own
  `TurnMachine` and runs `run_external_bridge_action`, which covers
  prepare, authorize, approve, invoke, and publish, but commits nothing to
  history. Up to eight calls run at once, and the listing is
  `driver.registry.specs()`.
- `98a4baa` injects skills and MCP servers into the CLI natively.
- `c20e928` closes calls left unanswered when a tool step fails, at the
  failure or at the next admission.

## Goals / Non-Goals

- Goals: every behavior in the `add-external-tool-dispatch-2026-10-13`
  deltas exists on main, with the handle under a non-colliding name and the
  #11 MCP bridge unchanged.
- Non-Goals: unifying the two dispatch paths, changing #11's behavior, or
  moving Smith off the compat line.

## Decisions

- **Name.** The in-process handle becomes `ExternalToolDispatch`, exposed as
  `agent::external::ExternalToolDispatch`, re-exported from the facade, and
  carried in `ExternalTurnRequest::tools`. `ExternalToolBridge` keeps
  meaning the MCP endpoint descriptor. The term "bridged call" in the
  original deltas becomes "dispatched call".
  - Alternative considered: keep `ExternalToolBridge` for the handle and
    rename #11's descriptor. Rejected: #11 is on main already and its name
    matches its role, since it describes a bridge endpoint.
- **Two paths, two behaviors, one tool pipeline.** Handle calls go through
  `ExternalTurnMachine`'s serialized queue and commit to history. MCP calls
  keep `run_external_bridge_action`. Both use the executor's
  prepare/authorize/approve/invoke steps. Where `db8dff2` refactored
  `tools.rs` (cancellation settlement, `tool_output.rs`), the refactor
  SHALL be applied so that the MCP path keeps its observable behavior. That
  path's existing tests (`tests/external_agent.rs` on main) are the guard.
- **Feature gating.** The handle and its tests sit behind `external-agent`,
  as in `db8dff2`. They MUST NOT require `external-agent-bridge`. Code
  touching both paths compiles under each feature combination CI builds.
- **Overlap with `c20e928`.** Main's closure of unanswered calls after a
  failed step stays. `db8dff2`'s settle-before-terminal guarantee covers
  cancellation and deadlines during tool execution and output processing.
  Port its tests first. Where main already passes one, port no code for it.
  Where a test fails, port the code. `failed_tool_step_pairing.rs` must
  keep passing unchanged.
- **Overlap with `98a4baa`.** CLI-native skill injection is untouched.
  Activation through the handle returns instructions, and activation through
  the MCP bridge is unchanged.
- **Checkpoints.** `ExternalToolCall` is an additive `TurnState`. Main's
  constants are still schema 3 and revision 4, and they MUST NOT change. The
  doc comments `db8dff2` added beside the constants are ported.
- **Tests.** Main keeps its external-agent conformance tests in
  `tests/external_agent.rs` and `tests/integration/external_agent.rs`. The
  ported modules go under `tests/external_agent/` as in `db8dff2`, loaded
  from `tests/external_agent.rs`, with request construction updated to set
  both `bridge` and `tools`.

## Risks / Trade-offs

- Two dispatch paths with different history semantics can confuse a host
  that enables both. The runtime-api and agent-execution deltas state the
  difference, and the docs (`docs/external-agents.md`) say which path to
  use: MCP for a CLI that should call tools itself, the handle for a host
  that brokers calls.
- The `tools.rs` merge is the riskiest step, because main's bridge path and
  `db8dff2`'s cancellation refactor share helpers there.

## Migration Plan

Additive for hosts. A host or test that builds `ExternalTurnRequest` by
hand adds `tools: ExternalToolDispatch::disabled()`. Smith keeps pinning
the compat line and is unaffected.

## Follow-ups (not in this change)

- The MCP bridge lists `driver.registry.specs()`, including tools the
  session scope denies; calls to them are refused at authorization.
  Consider listing the authorized view, as the handle does.
- `registry.activate` through the MCP bridge stages instructions for a
  provider request that never comes. Consider applying the handle's
  instruction-returning behavior to any external turn.
- Re-implement the MCP bridge as a transport over the handle, so one path
  owns history.
- Optionally rename the compat line's handle to `ExternalToolDispatch` too,
  updating Smith's three import sites, so a future compat→main merge does
  not conflict on the name.

## Open Questions

None blocking.
