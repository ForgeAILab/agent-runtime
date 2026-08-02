## Context

The live runtime already has the necessary substrate: session-scoped extension
state, an ordered harness pipeline, standard todo tools/components,
durability-aligned events, provider usage records, structured turn handles,
turn-local interruption, completed-turn persistence, and protected
checkpoints. What is missing is the semantic bridge between a persistent
objective and a later turn that has no new user message.

Queued `SessionHandle::send(UserInput)` is deliberately unsuitable. It appends
canonical user history and accepts FIFO work that may get ahead of later real
user input. A host timer around that API would therefore create both incorrect
history and a race unique to each consumer.

## Goals / Non-Goals

### Goals

- Own one reusable, versioned goal state machine below product presentation.
- Make internal continuation a first-class attributed turn with ordinary
  runtime policy and no fabricated user role.
- Give concurrent real user work priority at one serialized admission point.
- Account observable provider usage and active serving time honestly and once.
- Persist and resume through the existing session/checkpoint contracts.
- Supply deterministic conformance that any host can reuse.

### Non-Goals

- Product commands, visual status, output formatting, or prompt voice.
- Multiple or nested goals, child inheritance, cross-session sharing, a daemon,
  or work after the hosting process stops.
- Attachment storage, analytics, fork semantics, or app-server protocols.
- Estimated budget enforcement when provider counters are unavailable.

## Decisions

### One namespaced goal envelope

`GoalState` contains a stable `GoalId`, monotonically increasing generation,
bounded objective, `GoalStatus`, optional positive token budget, token usage
with provider-reported/unknown provenance, derived active elapsed
milliseconds, timestamps, and optional bounded `GoalStoppedReason`.

Statuses are `active`, `paused`, `blocked`, `usage_limited`,
`budget_limited`, and `complete`. Only `active` is schedulable. A complete goal
may be replaced with a new identity; any unfinished goal conflicts with create.
Budget-limited state may resume only after its budget exceeds observed usage or
is removed. Clear removes the namespace without implying completion.

The state uses one stable component namespace and schema revision in
`SessionSnapshot::extension_state`. All decode paths validate revision and
invariants and fail closed rather than clear or reinterpret incompatible state.

Alternative: a dedicated goal store. Rejected because it would split
atomicity, identity, migration, deletion, and resume from canonical sessions.

### Standard tools produce exact component mutations

The standard ability exports:

- `get_goal`, which returns the current validated state;
- `create_goal`, which accepts objective and an optional explicitly requested
  positive token budget;
- `update_goal`, which permits only model-owned `complete` and `blocked`
  transitions.

Tool descriptions prohibit inferred goal creation and inferred budgets.
User-owned pause/resume/edit/budget/clear transitions use a typed host control
contract, not the model tool. As with todos, tools return a versioned bounded
payload and the component processes that exact result into a namespaced state
patch and durability-aligned typed event.

The component contributes a required no-cache context fragment only while a
goal exists. The fragment contains bounded identity, objective, status, usage,
budget evidence, and tool policy. It grants no permissions.

Alternative: an unrestricted `set_goal` tool. Rejected because the model could
undo a user pause or budget and replace unfinished work.

### Internal turn input is separate from canonical user input

The session turn machine accepts an internal variant carrying bounded content,
stable source kind/id/revision, sensitivity, goal identity/generation, and a
hard size limit. That input is persisted in protected turn/checkpoint state and
attributed in lifecycle events, but is not appended as a user-role message.
It becomes a required tail context contribution for that turn.

`try_send_internal_if_idle` checks session lifecycle, active turn, queued user
work, and expected goal identity/generation under the same serialized
admission lock used by ordinary send. It either returns an accepted
`TurnHandle` or a structured `Busy`, `Stale`, or `Shutdown` rejection. It never
queues an internal request. Ordinary user submission either wins the lock or
causes an already-admitted internal turn to be visible and interruptible; an
internal request cannot silently sit ahead of user work.

Internal turns use the same planning, provider, tool, interaction, approval,
workspace, cancellation, retries, limits, checkpoints, and terminal events as
ordinary turns. The event/manifest source identifies them without consumers
parsing prompt text.

Alternative: synthetic `UserInput`. Rejected because it corrupts history and
does not provide atomic no-queue admission.

### A controller observes durable state, not presentation replay

`GoalController` is a reusable process-scoped task attached to one eligible
root `SessionHandle`. It receives initial restored state, then observes typed
goal and attributed turn terminal events. For each active goal generation it
attempts at most one conditional internal continuation after the session is
idle. Duplicate events and presentation replay cannot schedule work.

The controller owns no second state machine. It calls the runtime's typed goal
control/query and internal-admission APIs, records the accepted turn identity,
and re-evaluates only after a distinct committed boundary. Dropping or shutting
down the controller cancels/drains its current work through existing bounds and
starts nothing afterward.

Alternative: consumer event-loop recursion or polling timers. Rejected because
it duplicates deduplication and creates replay/shutdown hazards.

### Usage is reported at safe boundaries

Goal charged tokens equal provider-reported uncached input plus output tokens
for attempts attributable after goal activation. Cached input and reasoning
subcounters are not double charged. Usage records retain their provenance;
turn-commit logic computes each attributable delta once by stable attempt/turn
identity.

Active elapsed time advances only while an active goal owns a serving turn in
the current process. Idle time, stopped time, process downtime, and time before
activation are excluded. Clock values are monotonic during serving and stored
as a derived duration.

An optional budget is checked only after observed usage boundaries, so one
provider call may overshoot it. When usage reaches or exceeds budget, the goal
becomes `budget_limited` and no later automatic turn begins. If a budgeted
boundary lacks trustworthy required counters, the goal becomes `blocked` with
`accounting_unavailable`; the runtime never guesses remaining budget.

Alternative: estimator fallback. Rejected because provider-reported budget
control cannot honestly be derived from an estimate.

### Host controls serialize with serving turns

Goal query is always read-only. Create, edit, budget, resume, and clear require
the session to be idle. Pause may target an active serving goal turn: the
runtime records the pending pause intent, interrupts that turn, finalizes its
usage/time, and commits one paused generation before any controller
continuation can be admitted. Hosts receive structured busy/conflict/invalid
transition results rather than racing raw extension-state writes.

The runtime exposes neutral state/control/result types. Consumers decide which
commands or UI actions map to them and whether a session is an eligible root;
the runtime does not infer product policy from names.

## Risks / Trade-offs

- The turn-input and event schemas expand. Golden fixtures and versioned decode
  paths are required, with explicit compatibility failure for unknown required
  revisions.
- Provider usage may arrive after spend, so budgets can overshoot one request.
  The API exposes requested and actual values rather than claiming a hard cap.
- A controller can generate substantial work. It is opt-in, tied to explicit
  goal state, interruptible, subject to normal limits, and process-scoped.
- Public objectives may include user-authored sensitive text. Sensitivity is
  explicit and hosts choose public versus protected presentation policy.

## Migration Plan

1. Add schemas, pure state transitions, events, fixtures, and public exports
   without enabling controllers in existing hosts.
2. Add the component/tools and prove persistence/context/accounting in runtime
   conformance.
3. Add internal input/admission and controller conformance, preserving ordinary
   `send(UserInput)` behavior.
4. Update and run the Smith consumer against the exact local runtime revision.
5. Update changelog/compatibility evidence; publication remains a separate
   release decision.

## Open Questions

None for implementation. Attachments, analytics, forks, child goals, multiple
goals, app-server APIs, and daemon execution require separate proposals.
