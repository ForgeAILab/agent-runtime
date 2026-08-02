## Context

`SessionHandle::send(UserInput)` admits a complete later turn and the session
serves accepted turns FIFO. `SessionHandle::inject(InjectedContent)` contributes
generic user-role context at turn start or after a tool boundary, but it is
session-scoped, is not targeted to an expected serving turn, has no user-input
disposition, and does not force another provider pass when a response finishes
without tools.

Consumers need a third semantic operation: real-user input accepted into one
eligible serving turn, introduced only after the current provider/tool
operation is safe, and guaranteed either to commit once or receive an explicit
in-process discarded disposition. The canonical driver must own that behavior
because it alone controls request construction, history, checkpoints, limits,
terminal publication, and interruption.

## Goals / Non-Goals

### Goals

- Expose one consumer-neutral active-turn steering API with stable identity,
  bounded admission, and structured non-acceptance.
- Preserve FIFO real-user input and never mutate an in-flight provider request
  or tool invocation.
- Continue the same turn after safe-boundary steer commitment even when the
  preceding response would otherwise complete.
- Serialize steer admission with terminal close so every accepted steer has one
  committed or discarded in-process disposition.
- Make committed steer history, planning, usage, cancellation, and limits flow
  through the existing direct turn machine.
- Support steering eligible ordinary and internal provider-backed turns without
  giving user input additional tool authority or changing goal identity.

### Non-Goals

- Future-turn queue presentation or editing, consumer keymaps, or product
  fallback policy.
- Persisting an accepted but not yet committed steer across process exit in the
  first slice.
- Steering local-tool-only actions, returned-interaction terminals, completed
  turns, or consumer-defined review/compaction tasks without explicit runtime
  eligibility.
- Cancelling an in-flight provider request merely because a steer arrived.
- Replacing the generic injected-content or child-mailbox contracts.
- Adding app-server, daemon, network, analytics, or multi-session routing APIs.

## Decisions

### A serving turn owns a typed steer mailbox

Each provider-backed `TurnMachine` receives an `Arc<SteerMailbox>` registered
with the session only while that turn is serving. The mailbox contains a FIFO
of bounded entries with `SteerId`, target `TurnId`, and owned `UserInput`, plus
cumulative accepted-count/byte accounting and an open/closing state.

`SessionHandle::steer_current_turn` accepts optional expected turn identity,
validates non-empty bounded input and eligibility, then enqueues under the same
serving-turn lifecycle synchronization used by terminal close. It returns a
`SteerReceipt { id, turn }`. Structured rejection identifies no active turn,
expected-turn mismatch, non-steerable work, limit, or shutdown without placing
raw user content in an error message; caller-owned input remains available for
consumer fallback.

Alternative: append to the existing session `InjectionQueue`. Rejected because
generic injection may intentionally survive to a later turn, is not targeted,
and does not need a committed/discarded real-user disposition. The mailbox may
reuse bounded queue helpers internally but not erase the semantic boundary.

### Delivery occurs only through canonical history boundaries

The driver drains steering input before constructing a new provider request,
never while a provider stream or tool invocation is in flight. Initial turn
input is sampled first. Steers accepted during that request are drained after
the response is committed; steers accepted during tool work are drained after
canonical tool results commit. Each drained `UserInput` becomes its own ordered
user-role message in canonical history and participates normally in context
planning, compaction, cache policy, manifests, and token limits.

If a provider response has no tool calls but the mailbox contains input, the
driver commits the assistant response, drains and commits the steers, and
starts another provider step under the same `TurnId`. Tool-step limits continue
to count tool continuations only; steering has separate pending, per-input, and
cumulative per-turn bounds plus the existing turn deadline and context limits.

Alternative: cancel and start a new turn for every steer. Rejected because it
changes attribution, discards current-turn continuation, and duplicates
consumer interruption policy.

### Empty-check and terminal close are one atomic decision

Before publishing a terminal completion, the turn asks its mailbox to
`drain_or_close`. If input exists, the mailbox returns it and remains open. If
empty, it becomes closing atomically; later steer admission fails with a typed
no-active/closing result. Once closing begins, the driver completes through the
existing protected terminal path and unregisters the mailbox.

This fence applies to ordinary completion and terminal limits. Cancellation
closes the mailbox and produces discarded dispositions for every accepted but
uncommitted steer before `TurnCompleted`. A steer racing cancellation either
enters before close and is discarded exactly once or is rejected to its caller.

Alternative: check queue length and then complete under separate locks.
Rejected because an accepted steer can land between those operations and never
reach history or a disposition.

### Disposition events reconcile content without exposing it

The API receipt is the immediate process-local acceptance signal. The runtime
emits a typed `TurnSteerCommitted` event after the matching input is appended at
a protected safe boundary, and `TurnSteerDiscarded` when graceful terminal
closure drops an accepted entry. Events carry session/turn attribution,
`SteerId`, ordinal, and bounded reason, but no raw content or reversible
fingerprint.

Committed events are ordered with the checkpoint/history transition they
describe. In the first slice, acceptance before commitment is explicitly
process-local: an unclean process exit may leave no disposition and recovery
MUST NOT claim the steer committed. Durable pending steering requires a later
checkpoint-schema proposal.

Alternative: emit raw user text in lifecycle events. Rejected because event
sinks have different storage policies and existing user input is canonical
history rather than default progress telemetry.

### Internal goal turns remain eligible without policy elevation

An internal provider-backed goal turn may be steered by real user input. The
steer is appended as an ordinary user-role message under that serving turn and
may influence its next provider request, but it does not replace goal state,
change goal generation, bypass accounting, or grant permissions. The admitted
internal turn remains attributable to its original source for lifecycle and
goal accounting.

Idle-only internal admission and ordinary `send(UserInput)` share their planned
serialized admission boundary. A pending/queued real-user turn prevents a new
automatic continuation according to the goal contract; steering an already
serving internal turn is the explicit real-user path after that race has
resolved.

### Generic injection remains independent

Generic `InjectedContent` continues to drain at its documented safe boundaries
for monitor, child, and host context. Its presence does not create a steer
receipt or user disposition. At a boundary the driver orders canonical tool
results, eligible generic injections, and user steers deterministically; user
steer messages remain FIFO relative to one another. The exact cross-kind order
is frozen in conformance fixtures and documented for consumers.

## Risks / Trade-offs

- Turn lifecycle and mailbox synchronization are concurrency-sensitive.
  Deterministic barrier tests and loom-style or repeated race coverage are
  required around admission, completion, and cancellation.
- Continuing after a nominal final response can increase spend. Explicit user
  action, separate steer bounds, normal turn deadlines, and context budgets
  bound the behavior.
- Process-local accepted input can be lost on a crash before commitment. The
  API and events make that boundary honest; durable pending input is deferred.
- Runtime events and facade exports expand and require schema fixtures plus all
  consumer compatibility gates.
- Steering an internal goal turn joins two active changes. Implementation must
  use the final internal-turn/admission types rather than guessing an adapter.

## Migration Plan

1. Add neutral steer IDs, admission/result types, events, bounds, exports, and
   schema fixtures without enabling driver delivery.
2. Add the serving-turn mailbox and atomic registration/close fence with unit
   race tests.
3. Integrate safe-boundary drain and final-response continuation into the
   direct turn machine and protected checkpoints.
4. Add cancellation/discard handling, internal-turn compatibility, generic
   injection ordering, and testkit conformance.
5. Update changelog and run Smith plus other consumer compatibility gates.
6. Pin the exact runtime revision in the coordinated Smith change; publication
   remains a separate action.

## Open Questions

None for implementation. Durable pre-commit steering, remote protocols,
consumer queue persistence, and non-provider task kinds remain future changes.
