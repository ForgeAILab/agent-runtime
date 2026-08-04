---
created_at: 2026-08-02T07:15:49Z
updated_at: 2026-08-02T16:29:54Z
---

## Why

The session facade can queue later whole turns and inject generic host content,
but it cannot target additional real-user input to the serving turn. Consumers
therefore either mislabel a later turn as immediate guidance or reimplement
turn identity, safe-boundary delivery, terminal races, and continuation around
the canonical provider/tool loop.

## What Changes

- Add a host-neutral typed API for steering one eligible serving turn with
  bounded `UserInput`, an optional expected `TurnId`, and a stable steer
  receipt.
- Add a turn-local bounded FIFO whose admission and terminal close decision are
  serialized so accepted input cannot disappear in a completion race.
- Commit accepted steers only at provider/tool safe boundaries and continue the
  same turn when pending user input exists, including after a provider response
  that would otherwise be final.
- Add privacy-safe committed/discarded steer disposition events so hosts can
  reconcile optimistic input without parsing logs or provider text.
- Preserve queued whole-turn `send(UserInput)`, generic injected content,
  cancellation, checkpoint, usage, limits, and provider/tool policy as distinct
  contracts.
- Add deterministic testkit conformance for ordering, races, interruption,
  bounds, context planning, and compatible consumer integration.

## Impact

- Affected specs: new `turn-steering`
- Affected code: `agent-runtime-core` IDs/events/schema fixtures,
  `agent-runtime` session facade/turn bookkeeping/direct driver loop,
  `agent-runtime-testkit`, documentation, changelog, and consumer conformance
- Public compatibility: additive session API, steer identity/admission/error
  types, runtime events, and driver bounds
- Consumer: coordinated Smith behavior is specified by
  `../tui/docs/spec/changes/add-turn-steering-and-input-queue-2026-08-02/`

## Active Change Coordination

- `stabilize-session-harness-pipeline-2026-07-31` remains authoritative for
  the direct turn machine, checkpoints, event durability, cancellation, and
  completed-turn persistence. Steering extends its serving-turn lifecycle.
- `add-persistent-session-goals-2026-08-02` remains authoritative for
  provenance-bearing internal turns and serialized idle-only admission.
  Real-user steering MAY target an eligible serving goal turn, while pending or
  queued real-user work continues to outrank a new automatic continuation.
- `add-agent-delegation-runtime-2026-07-26` remains authoritative for generic
  attributed child-result injection. A user steer is not a child mailbox item.

## Approval Boundary

Approval authorizes the neutral in-process steering mechanism, typed events,
and reusable conformance. It does not authorize consumer keybindings or queue
presentation, remote/app-server protocols, durable offline draft storage,
steering local-only actions, or changes to product-specific prompts and policy.
