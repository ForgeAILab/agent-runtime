---
created_at: 2026-09-05T10:30:00Z
updated_at: 2026-09-05T10:30:00Z
---

## Why

Adopters can already select a model behind the normalized `Provider` boundary,
including one reached through a bounded subprocess. They cannot select an
*agent*. Installed coding CLIs -- Claude Code, Codex -- are not models: each
owns a conversation session, runs its own tools under its own permission
policy, and streams its own agent events. The command-provider mechanism
deliberately excludes them, because presenting an agent's events as one
stateless model turn would create two owners for history, tools, retries, and
authority.

The result today is that an adopter who wants to delegate work to an installed
CLI must either write a shim that lies about being a model -- re-sending the
entire canonical history on every attempt, disabling the CLI's own tools, and
parsing prose for tool intent -- or fork the runtime. Both are worse than a
named boundary that says plainly: this turn is executed elsewhere.

## What Changes

- Add an opt-in `ExternalAgentBackend` boundary: a host-supplied executor that
  runs one turn end to end and streams normalized agent events back, in place
  of the runtime's provider/tool loop for sessions configured to use it.
- Dispatch to it from the existing turn entry point, so an external turn keeps
  the same turn identity, admission, cancellation, event stream, and
  persistence path as a direct turn. A session either has a backend or it does
  not; the two loops never interleave within one turn.
- Define the normalized external agent event set: session identity, assistant
  text, reasoning, tool invocation and its outcome, usage, and terminal
  completion or failure. These project onto the existing canonical events so
  hosts render an external turn with no new rendering path.
- Persist the backend's own session identity in extension state so a later turn
  can continue the same external conversation instead of replaying history the
  backend already holds. Continuation is advisory: a backend that cannot resume
  MUST be able to report that and start fresh without failing the turn.
- Account usage the backend reports through the ordinary usage path, including
  its cache breakdown, so an external turn is billed and displayed like any
  other.
- Keep the runtime the owner of canonical history, turn lifecycle, events,
  usage records, checkpoints, and cancellation. The backend owns only what
  happens inside its turn.

## Impact

- Affected specs: `agent-execution`, `runtime-api`.
- Affected code: a new `agent::external` module, the turn entry point in
  `agent::driver`, session extension state for continuation identity, and the
  builder surface that accepts a backend.
- Compatibility: additive. A runtime with no backend configured is unchanged,
  and the default dependency graph does not grow.
- Security: an external backend executes with the authority of the host
  process and receives the prompt. It is not sandboxed by the runtime, and the
  runtime cannot enforce approvals over tools it never sees. That boundary is
  stated rather than implied, and the backend is opt-in per session.

## Non-Goals

- No adapter for any specific CLI in this change. Claude Code and Codex
  adapters are the adopter's, built on this boundary.
- No attempt to project an external agent's internal tool calls through the
  runtime's approval, workspace, or tool-authority machinery. A tool the
  runtime never dispatched cannot be approved by it, and pretending otherwise
  would be the two-owners problem in a new place.
- No context planning, compaction, retry, or prompt-cache maintenance for
  external turns. The backend owns its own context.
- No change to the command-provider mechanism, which remains the way to reach a
  *model* through a subprocess.
