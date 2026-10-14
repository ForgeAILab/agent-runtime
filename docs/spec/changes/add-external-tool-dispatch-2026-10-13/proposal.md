---
created_at: 2026-10-14T01:20:00Z
updated_at: 2026-10-14T22:46:52Z
---

## Why

An external agent backend runs a whole turn outside the provider/tool loop,
and the runtime only observes it. The agent it drives therefore cannot use
anything the host registered: child delegation, an advisor, remote MCP
servers, module tools, skills. Smith wants its installed-CLI turns to reach
those capabilities, and it must not do so by invoking tool implementations
itself: that would skip validation, authorization, approval, workspace
enforcement, output bounds, events, history, and checkpoints, which is the
two-owners problem the external boundary exists to prevent.

`add-external-agent-backend` ruled out projecting the *backend's own* tools
through the runtime. That still holds. This change adds the opposite
direction: the backend asks the runtime to run one of the *host's* tools.

Two gaps make that impossible today. `Session::run_local_tool` refuses
unless the session is idle, so it cannot run during an external turn. And
`Driver::run_internal_turn` always builds the provider `TurnMachine`, so a
child-completion turn on an external session would call a provider that was
never meant to serve it.

## What Changes

- **A dispatch handle on the turn request.** `ExternalTurnRequest` carries an
  `ExternalToolBridge` that lists the tool specifications the host offers
  for the turn and runs one call through the runtime's own tool path.
- **Host exclusions.** The builder accepts tool names the host does not
  offer to an external backend (the tools the backend replaces). The handle
  lists the session's authorized tools, never a denied one, less those names.
- **A bridged call is a runtime tool call.** It passes the same validation,
  preparation, authorization, approval, execution, output processing, and
  commit as a model-requested call, emits the ordinary tool events, and is
  committed to canonical history in order with the text the backend
  reported. It resolves against the authorized view, not the activation
  epoch. Calls run one at a time in arrival order.
- **Abandonment is defined.** A call the backend stops waiting for never
  starts if it has not started, has its pending interaction withdrawn if it
  is waiting for one, and runs to completion with its result committed and
  discarded if it is already executing.
- **Internal turns run on the backend.** On a session with an external
  backend, an admitted internal turn is executed by the backend, continuing
  its conversation, with the internal input sent as delivery text.
- **Tools can see what the backend did.** A bounded log of the in-flight
  external turn's own tool observations is readable through the session
  view, so a tool that reviews the conversation is not blind to it.
- **Skill activation returns its instructions** when the turn is external,
  because nothing can be staged into a context the runtime does not own.
- **An interrupted external turn is not replayed.** Recovery finalizes it as
  interrupted and does not run a bridged call again.

## Impact

- Affected specs: `agent-execution`, `runtime-api`, `internal-turn-control`,
  `capability-routing`.
- Affected code: `agent::external`, `agent::driver::external_turn`,
  `agent::driver` (internal turns, recovery), the runtime builder,
  `harness::live_abilities` (activation result), the session view.
- Compatibility: `ExternalTurnRequest` gains a field, which breaks a backend
  test that builds the request by hand; `ExternalToolBridge::disabled()`
  exists for that. A session without a backend is unchanged, and a backend
  that never uses the handle behaves exactly as before. Everything stays
  behind the `external-agent` feature.
- Checkpoint compatibility: checkpoints written before this change remain
  valid; a checkpoint in the new `external_tool_call` state is rejected by
  a build without `external-agent` as an unknown variant; neither
  `CHECKPOINT_SCHEMA_VERSION` (3) nor `TURN_TRANSITION_REVISION` (4) changes.
- Security: nothing new is authorized. Every bridged call is checked exactly
  as a model-requested call is, and the handle refuses calls once its turn
  has ended.

## Non-Goals

- Approving or recording the backend's own tools as runtime tool calls.
- Context planning, compaction, or prompt-cache maintenance for external
  turns.
- Any transport. How a host exposes the handle to an external process (an
  MCP server, a socket) is the host's business.
- Parallel execution of bridged calls.
