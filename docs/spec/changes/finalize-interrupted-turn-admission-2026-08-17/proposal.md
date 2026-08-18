---
created_at: 2026-08-17T00:00:00Z
updated_at: 2026-08-17T00:00:00Z
---

## Why

A turn that ends without a durable terminal boundary -- a failed protected
write during terminal publication, or a restart that deliberately left its
checkpoint dormant under the defer policy -- leaves the session's latest
protected checkpoint non-terminal. Every later admission (new turn, internal
turn, or local action) then fails closed with
`Conflict: cannot accept a new turn over a non-terminal checkpoint`, wedging
the session with no in-process recovery path: only a full restart under the
resume policy can clear it, and that path re-runs the interrupted turn's
provider/tool work.

This contradicts the existing guarantee that an interrupted turn must not
cancel the session ("Turn interruption is not session cancellation"). The
host's explicit new work is a strictly better disposition than an
unrecoverable wedge.

## What Changes

- Admission (new turn, internal turn, local action) reconciles a protected
  non-terminal checkpoint whose turn is no longer running before accepting:
  the interrupted turn is finalized as an explicit `TurnFinish::Failed`
  terminal through the ordinary terminal publication path (commit hooks,
  `TurnCompleted` event, session snapshot save, terminal checkpoint), with an
  attributed error event explaining the finalization.
- The interrupted turn's indeterminate provider/tool outcome is never
  replayed by this path; only its already-durable state is kept.
- Live turns are never finalized: checkpoint-resume recovery still serving,
  and cache-operation checkpoints (which retain their own repair path),
  keep failing closed exactly as before. The acceptance-time conflict remains
  as the fail-closed backstop whenever reconciliation cannot complete.

## Impact

- `agent-runtime` driver: new `TurnMachine::abandon` recovery disposition and
  `Driver::finalize_interrupted_turn`; `SessionInner::reconcile_interrupted_checkpoint`
  invoked at the three admission sites.
- No schema, wire, or store-contract changes: the finalized chain reuses the
  existing transition table (`Completing` is reachable from every
  non-terminal state) and successor watermark rules.
- Hosts observing events see one attributed error plus one `TurnCompleted
  { Failed }` for the interrupted turn, before the new turn's `TurnStarted`.
