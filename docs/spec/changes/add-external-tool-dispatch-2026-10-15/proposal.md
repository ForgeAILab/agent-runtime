---
created_at: 2026-10-15T20:40:12Z
updated_at: 2026-10-15T21:06:27Z
---

## Why

Smith's installed-CLI turns run on the `feat/smith-*` compat line, where
commit `db8dff2` (`add-external-tool-dispatch-2026-10-13`) gave an external
backend an in-process handle for running host tools through the runtime's
own tool path. Main took a different route for a related need: PR #11
(`0c06379`) serves the session's tools to the CLI as a loopback MCP server.
The owner's rule is that all runtime work lands on main, and `db8dff2` is the
one compat commit main still lacks. It cannot be merged as-is: both lines
define a public type named `ExternalToolBridge`, and main has moved
`external_turn.rs`, `tools.rs`, `turn.rs`, recovery, and the checkpoint module
since the commit's base.

The two designs are not duplicates. The MCP bridge gives a CLI a transport
and runs authorization, approval, and execution, but it lists the whole
registry, runs up to eight calls at once, and commits nothing to canonical
history. The dispatch handle has no transport. It lists only the session's
authorized tools less host exclusions, runs calls one at a time, commits them
to history in order with the backend's text, settles abandoned calls by
phase, and never replays an interrupted turn. It also brings fixes that main
lacks: internal turns on an external session reach the provider today, and a
skill activated from an external turn returns no instructions (both checked
on main `5fb0507`). It also guarantees that a tool cancelled with its turn
under an output processor still commits its result. Main's `c20e928` covers
the related failed-step case. Whether main also loses the cancelled result
is settled by porting the tests first.

## What Changes

- **Port the dispatch handle under a new name.** `ExternalTurnRequest` gains
  `tools: ExternalToolDispatch`, the handle `db8dff2` called
  `ExternalToolBridge`. Main's `bridge: Option<ExternalToolBridge>` MCP
  endpoint keeps its name, feature, and behavior. **BREAKING** for code that
  builds `ExternalTurnRequest` by hand: it must set `tools`, and
  `ExternalToolDispatch::disabled()` exists for that. On main, the only such
  code is the runtime itself and `agent-runtime-agent-cli/tests/{claude,codex}.rs`.
- **Host exclusions** on the builder, for the handle's listing.
- **Dispatched calls are runtime tool calls** committed to canonical history
  in order. Abandonment is settled by phase. Calls the CLI makes through the
  MCP bridge are unchanged.
- **Internal turns on an external session run on the backend.**
- **Tools can read the backend's own tool observations**, bounded and never
  persisted.
- **`registry.activate` through the handle returns skill instructions**, and
  the discovery bootstraps are listed only when useful.
- **Backend diagnostics** are non-terminal and use the existing Conflict
  error event.
- **The external continuation identity is redaction-safe.**
- **A requested tool call always settles before its turn ends**, alongside
  main's existing closure of calls left unanswered by a failed step
  (`c20e928`).
- **An interrupted external turn is not replayed.**

## Impact

- Affected specs: `agent-execution`, `runtime-api`, `internal-turn-control`,
  `capability-routing`. All deltas are ADDED.
- Affected code: `agent::external`, `agent::external_capabilities` (docs
  only), `agent::driver::{external_turn, external_bridge, tools, tool_output,
  turn, recovery, mod}`, `runtime::{builder, engine, state,
  session::lifecycle}`, `harness::live_abilities::search`,
  `agent-runtime-core::checkpoint`, `lib.rs` facade, tests under
  `crates/agent-runtime/tests/`, and request construction in
  `crates/agent-runtime-agent-cli/tests/{claude,codex}.rs`.
- Checkpoint compatibility: adds the additive `ExternalToolCall` state.
  `CHECKPOINT_SCHEMA_VERSION` stays 3 and `TURN_TRANSITION_REVISION` stays 4
  on main, as on the compat line. A build without `external-agent` rejects a
  checkpoint in the new state by name.
- Smith is unaffected: it pins the compat line, which keeps the name
  `ExternalToolBridge` for the handle.
- Security: nothing new is authorized. Every dispatched call is checked as a
  model-requested call is, and the handle refuses calls after its turn ends.

## Non-Goals

- Re-implementing the MCP bridge as a transport over the dispatch handle.
  That would change #11's concurrency and history semantics and is a
  separate change.
- Narrowing the MCP bridge's listing to the authorized view, or making MCP
  activation return instructions. Both are recorded as follow-ups in
  `design.md`.
- Moving Smith to runtime main. Smith stays on the compat line.
- Parallel execution of dispatched calls.
