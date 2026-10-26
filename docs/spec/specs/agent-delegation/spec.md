# agent-delegation Specification

## Purpose
TBD - created by archiving change add-agent-delegation-runtime. Update Purpose after archive.
## Requirements
### Requirement: Neutral delegation lifecycle operations

The runtime SHALL expose host-invoked operations to spawn, list, follow up
with, wait for, fetch the result of, and stop child sessions. Operations MUST
be addressed by a stable child ID and MUST return structured lifecycle or
error results. The runtime MUST NOT hard-code any consumer's delegation tool
name, prompt text, or presentation.

#### Scenario: Spawn returns a stable child ID

- **GIVEN** a host submits a valid child specification with capacity available
- **WHEN** the runtime spawns the child
- **THEN** the operation returns a stable child ID attributed to the parent
  session
- **AND** subsequent list, wait, and result operations resolve that ID

#### Scenario: Stop reaches a running child

- **GIVEN** a child session is executing a cancellable tool
- **WHEN** the host stops that child by ID
- **THEN** cancellation reaches the child's tool and provider stream
- **AND** exactly one terminal stopped result is produced for that child

#### Scenario: Follow up with a completed child

- **GIVEN** a child completed a turn and remains within its limits
- **WHEN** the host sends a follow-up task to the same child ID
- **THEN** the child resumes under its existing specification and limits
- **AND** new activity remains attributed to the same child ID

### Requirement: Host-owned child specification

A child session SHALL be created only from a host-supplied specification
declaring the task content, provider/model selection, turn/token/deadline
limits, tool-view scope, and workspace policy. Workspace policy MUST be one of
shared project, explicit directory, isolated worktree, or read-only view; the
runtime validates and carries the policy but does not create workspaces. The
runtime MUST reject structurally invalid or incomplete specifications with a
structured error and no side effects.

#### Scenario: Read-only child receives no write tools

- **GIVEN** a specification whose tool-view scope excludes write and execute
  tools
- **WHEN** the child session starts
- **THEN** the child's tool view contains only the scoped read tools
- **AND** the declared read-only workspace policy appears in its lifecycle
  events

#### Scenario: Invalid specification is rejected

- **GIVEN** a specification missing limits or naming an unknown workspace
  policy
- **WHEN** the host submits it
- **THEN** the runtime returns a structured validation error
- **AND** no child session or lifecycle event is created

### Requirement: Configurable delegation depth

The runtime SHALL enforce a configurable maximum delegation depth with a
default of one. Child-session views MUST exclude delegation operations at the
maximum depth, and the runtime MUST reject a spawn, follow-up, or stop
operation whose requesting session is itself a child, even if a malformed or
injected call reaches the host.

#### Scenario: Child spawn attempt is rejected

- **GIVEN** a child session emits a call shaped like a spawn request
- **WHEN** the runtime authorizes the operation
- **THEN** it rejects the request as a depth violation
- **AND** no grandchild session is created

### Requirement: Delegation routes through composed authorization

Spawn, follow-up, and stop SHALL be evaluated as authority-bearing operations
through the same composed authorization path used for tool invocation, and
MUST fail closed when no authorizer is composed. Denials MUST be returned as
structured results without creating or mutating child sessions.

#### Scenario: Policy denies a spawn

- **GIVEN** the composed authorization path denies the delegation operation
- **WHEN** the host submits a spawn request
- **THEN** the runtime returns a structured denial
- **AND** no child session is created

### Requirement: Attributed child lifecycle events

The runtime SHALL emit normalized, ordered child lifecycle events — spawned,
progress, completed, stopped, and failed — attributed with the child ID, the
parent session ID, the declared workspace policy, and limit metadata. A final
child result MUST NOT be dropped by progress coalescing. When a provider
classifies a child's entire final answer as non-redacted reasoning, the
completed event SHALL carry that reasoning text as the result rather than an
empty result.

#### Scenario: Parent observer sees an ordered lifecycle

- **GIVEN** a host subscribes to the parent session's event stream
- **WHEN** a child spawns, streams progress, and completes
- **THEN** the observer receives spawned, progress, and completed events in
  order with child and parent attribution
- **AND** the completed event carries the child's final result

#### Scenario: Reasoning-only child answer survives

- **GIVEN** a child's provider streamed its entire final answer as
  non-redacted reasoning with no visible text
- **WHEN** the child's task completes
- **THEN** the completed event's result carries the reasoning text
- **AND** the result is not empty

### Requirement: Bounded concurrency and ephemeral children

The runtime SHALL enforce configurable process, session, and per-parent
running-child concurrency limits plus cumulative per-child turn, token, and
deadline limits through the existing deterministic limit machinery. By
default an over-capacity spawn returns a structured capacity result; queueing
is an explicit host policy. Live child executions MUST stop when their parent
session or process stops and MUST NOT restart automatically on session resume.
When durable stores are configured, the stopped execution's child-session
identity and committed state SHALL remain available for an explicit compatible
resume or later follow-up. Retained child records MUST be bounded by host
policy; terminal stop, expiry, or parent deletion MUST make them
non-executable.

#### Scenario: Concurrency limit is reached

- **GIVEN** the per-parent running-child limit is reached
- **WHEN** the host requests another child without a queue policy
- **THEN** the runtime returns a structured capacity result
- **AND** the running-child limit is not exceeded

#### Scenario: Parent process stops with a running child

- **GIVEN** a durable parent session has a running child
- **WHEN** the process tears down or loses its execution lease
- **THEN** the live child execution is cancelled or reconciled as interrupted
- **AND** resuming the parent does not automatically restart it
- **AND** its committed child-session state remains addressable according to
  retention policy

#### Scenario: Cumulative limit survives restart

- **GIVEN** a child consumed all but one of its allowed tasks before restart
- **WHEN** the recovered child completes one follow-up
- **THEN** another follow-up is rejected at the original cumulative limit
- **AND** restart did not reset usage or limits

### Requirement: Durable child-session identity and continuity

The runtime SHALL, when durable delegation, session, and checkpoint stores are
configured, bind each child ID to one stable child session ID and original
parent session. It MUST persist enough protected state to restore the child's
canonical history, manifests, cumulative usage, limits, specification
fingerprint, latest outcome, and lifecycle revision after process restart. A
follow-up after recovery MUST execute as a new turn in that same child session
and MUST NOT create a replacement child.

#### Scenario: Follow up after parent restart

- **GIVEN** a child completed a task and both parent and child state are durable
- **WHEN** the host restarts, resumes the parent, and follows up by child ID
- **THEN** the runtime resumes the same child session with its prior canonical
  conversation
- **AND** new events retain the original child and parent attribution
- **AND** no spawn lifecycle event or new child identity is created

#### Scenario: Host has no durable child stores

- **GIVEN** a host composes delegation without durable child stores
- **WHEN** it spawns and lists a child
- **THEN** the runtime labels that child as process-ephemeral
- **AND** does not claim it can be followed up after restart

### Requirement: Explicit interrupted-task resume

The runtime SHALL distinguish durable child-session continuity from a live
child execution. Process loss MUST reconcile an unleased running child to an
interrupted state without starting provider or tool work. A host-authorized
`resume` operation SHALL continue the exact compatible child checkpoint; it
MUST NOT create a new task, increment the task count again, or repeat committed
provider calls, interaction answers, approvals, or tool side effects.

#### Scenario: Process exits after one tool result commits

- **GIVEN** a child checkpoint records one committed tool result and a pending
  continuation
- **WHEN** the parent is resumed and the host lists the child
- **THEN** the child is reported interrupted and resumable without executing
  work
- **WHEN** the host explicitly resumes that child
- **THEN** execution continues after the committed result exactly once

#### Scenario: Exact checkpoint is unavailable

- **GIVEN** a durable child record is interrupted but its exact checkpoint is
  missing, corrupt, or incompatible
- **WHEN** the host requests resume
- **THEN** the runtime returns a structured non-resumable result
- **AND** leaves the original record intact and spawns no replacement

### Requirement: Durable child ownership and policy compatibility

Every recovered child operation SHALL resolve through the original
parent-scoped catalog and composed authorization path. Recovery MUST validate
the parent session, security scope, child session, specification fingerprint,
workspace identity, model/provider policy, tool-view upper bound, and relevant
revisions. It MAY apply a strictly narrower compatible policy, but MUST fail
closed rather than widen authority or silently change identity. A child whose
policy the host cannot compute while the parent's catalog is restored SHALL
be marked incompatible, and an interrupted one non-resumable, without
failing the parent's restore.

#### Scenario: Another parent knows the child ID

- **GIVEN** a child belongs to parent session A
- **WHEN** parent session B requests its result, follow-up, resume, or stop by
  the same child ID
- **THEN** the runtime returns a structured ownership denial
- **AND** exposes no child content or state mutation

#### Scenario: Workspace identity changed

- **GIVEN** a durable child was authorized for one canonical workspace
- **WHEN** recovery resolves a different or less-trusted workspace identity
- **THEN** recovery fails closed as incompatible
- **AND** does not rebuild the child with the new workspace

#### Scenario: The host can no longer build a child's policy

- **GIVEN** a parent's catalog records a child the host factory now fails to
  fingerprint, for example because the child's profile route is gone
- **WHEN** the parent session is restored
- **THEN** the parent restores
- **AND** that child reports the incompatibility and is not resumable
- **AND** the other children are unaffected

### Requirement: Bounded child wait

The delegation API SHALL accept DelegationWaitOptions with an optional
per-call timeout. DelegationConfig SHALL supply a five-second default and a
thirty-second hard maximum that hosts may narrow. Excessive values MUST fail
validation; timeout MUST return a successful Running projection rather than an
error or cancellation, and cancellation MUST remain a distinct result without
discarding or mutating the child's protected terminal outcome.

#### Scenario: Child exceeds the default wait

- **GIVEN** a child remains running beyond the default wait duration
- **WHEN** the host waits without an explicit timeout
- **THEN** the operation returns a structured running result successfully
- **AND** the child continues under its existing limits

#### Scenario: Host requests an excessive timeout

- **GIVEN** a host supplies a timeout greater than thirty seconds
- **WHEN** Runtime validates the wait request
- **THEN** it rejects the value because it exceeds the configured hard maximum
- **AND** it never waits beyond the hard cap

#### Scenario: Host narrows the configured maximum

- **GIVEN** DelegationConfig permits at most ten seconds for this host
- **AND** the global hard maximum is thirty seconds
- **WHEN** the host requests twenty seconds
- **THEN** Runtime rejects the request as excessive for that host
- **AND** the host's narrower maximum remains enforced

#### Scenario: Wait is cancelled

- **GIVEN** a wait is active and the parent cancellation token is cancelled
- **WHEN** Runtime resolves the wait
- **THEN** it returns a distinct cancellation result
- **AND** it does not report cancellation as a successful Running timeout

#### Scenario: Child completes during the timeout race

- **GIVEN** a child terminal outcome becomes protected while a wait expires
- **WHEN** Runtime resolves the wait boundary
- **THEN** the host receives either a terminal result or a successful running
  projection according to the serialized boundary
- **AND** the terminal outcome remains available for exact inspection and
  automatic delivery

### Requirement: Protected automatic outcome cursor

Runtime SHALL maintain a parent-scoped protected consumption cursor for
automatic child-task outcome delivery. The cursor MUST be keyed by stable child
and task-outcome identity, advance monotonically and idempotently, survive
restart through versioned extension state, and remain separate from
idempotent host inspection. Cursor advancement MUST be staged with
child-completion InternalTurnInput acceptance in one parent TurnCheckpoint
revision; the outcome remains protected until that revision commits. The
public opaque `ChildOutcomeCursor` and `ChildCompletionAdmissionRequest` MUST
carry the expected parent/cursor revision without exposing outcome content.

#### Scenario: Outcome is consumed once

- **GIVEN** two ready child outcomes are ordered canonically
- **WHEN** the parent consumes the ready batch
- **THEN** the cursor is staged with the accepted internal turn and commits once
  in one parent checkpoint revision
- **AND** a repeated delivery attempt returns no duplicate outcomes

#### Scenario: Crash before the staged revision commits

- **GIVEN** a child outcome is staged for delivery but the parent checkpoint
  does not commit
- **WHEN** the parent recovers
- **THEN** the cursor remains at its prior revision
- **AND** the protected outcome remains available for one later admission

#### Scenario: Process exits before delivery

- **GIVEN** a terminal child outcome is protected before the parent process
  exits
- **WHEN** the parent resumes
- **THEN** the outcome is still available after cursor recovery
- **AND** Runtime does not derive delivery from a lossy event observer

#### Scenario: Host inspection does not consume delivery

- **GIVEN** a host reads a child result by child ID
- **WHEN** it performs the idempotent inspection
- **THEN** the automatic outcome cursor is unchanged
- **AND** a later admitted child-completion turn can still consume the outcome

### Requirement: Deterministic ready-outcome batches

Automatic child-completion delivery SHALL consume all ready protected terminal
outcomes visible at one serialized boundary in canonical child/task identity
order. Reverse event arrival, observer gaps, and duplicate collector signals
MUST NOT change the batch or lose a terminal result.

#### Scenario: Results arrive in reverse order

- **GIVEN** child B completes before child A at the event observer
- **AND** the protected outcome keys sort as A then B
- **WHEN** Runtime forms the next automatic batch
- **THEN** the batch is ordered A then B
- **AND** both outcomes are delivered exactly once

#### Scenario: Duplicate collector notification

- **GIVEN** a child completion collector retries the same outcome identity
- **WHEN** Runtime records the second notification
- **THEN** it rejects or idempotently ignores the duplicate
- **AND** the protected result and cursor remain unchanged

### Requirement: Child-completion delivery is budgeted and never refused for size

Automatic child-completion delivery SHALL inline completed child replies in
canonical batch order while each fits the remaining
`DelegationConfig::delivery_inline_bytes`; a reply of at most 1,024 bytes
SHALL always be inlined. A reply that does not fit SHALL be delivered as its
opening 1,024 bytes plus the reference of its parent-owned stored copy, or
with a pointer to the child's result when no copy exists. Runtime MUST NOT
refuse, defer, or leave ready an admission because of the size of its
replies.

#### Scenario: A long reply is delivered by reference

- **GIVEN** a factory with an artifact store and a child reply over the budget
- **WHEN** the ready batch is admitted
- **THEN** the delivery carries the reply's opening bytes and the stored
  reference
- **AND** the stored copy is owned by the parent session and holds the exact
  reply

#### Scenario: A batch over the child-completion ceiling

- **GIVEN** a ready batch whose inline form exceeds
  `MAX_CHILD_COMPLETION_TURN_CHARS`
- **WHEN** the batch is admitted
- **THEN** each outcome is delivered as one compact line
- **AND** the admission is accepted

#### Scenario: A short reply keeps the existing text

- **GIVEN** a reply within the budget and no transferred artifacts
- **WHEN** it is delivered
- **THEN** the delivery text is unchanged from previous releases

### Requirement: Delivered outcomes name their transferred artifacts

Each completed outcome in an automatic delivery SHALL list the artifacts
transferred to the parent from that child turn, giving the parent-owned id,
the child-owned source id, media type, and byte length. At most 32 SHALL be
listed per outcome, followed by the count of the rest.

#### Scenario: A transferred tool-output artifact

- **GIVEN** a child turn whose offloaded tool output was transferred to the
  parent
- **WHEN** its outcome is delivered automatically
- **THEN** the delivery names the parent-owned copy and its child-owned source

### Requirement: Outcomes returned through a tool result are not delivered again

The runtime SHALL let a host delegation tool acknowledge that the child task
outcome it returns is the result of one tool call in the parent's serving
turn. When that call's result commits to the parent's canonical history
without an error, the runtime MUST remove that exact outcome from automatic
child-completion delivery and persist the removal before the turn continues.
The outcome MUST remain available to idempotent host inspection, and the
protected automatic outcome cursor MUST NOT change. When the result is an
error, or the turn ends before the result commits, the outcome MUST remain
ready for automatic delivery. Acknowledging against a turn that is not
serving, or an outcome that is not a durable recorded outcome of the parent,
SHALL fail without effect.

#### Scenario: The model read the result with a tool

- **GIVEN** a child completed and its outcome is ready for automatic delivery
- **WHEN** a parent tool call returns the outcome, acknowledges it, and its
  result commits
- **THEN** no ready outcome remains and admission delivers nothing
- **AND** host inspection still returns the outcome
- **AND** a parent restored from its stores has no ready outcome

#### Scenario: The tool result is an error

- **GIVEN** a parent tool call acknowledged a ready outcome
- **WHEN** the call's result commits as an error
- **THEN** the outcome is still delivered automatically

#### Scenario: The turn ends before the result commits

- **GIVEN** a parent tool call acknowledged a ready outcome
- **WHEN** the turn is interrupted before the call's result commits
- **THEN** the outcome is still delivered automatically

#### Scenario: Acknowledging outside the serving turn

- **GIVEN** a ready outcome
- **WHEN** a host acknowledges it against a turn that is not serving
- **THEN** the call fails and the outcome stays ready

### Requirement: Limit-stopped child tasks

When a delegated child's turn ends at a configured limit, Runtime SHALL record
a distinct limit outcome that names the limit. It MUST NOT be a completed
outcome and MUST NOT carry the child's last assistant message as a result.
The child SHALL remain available for follow-ups within its limits. Its status
SHALL carry no last result and a limit error naming the limit until its next
task starts. The parent stream SHALL receive a child-limit-reached event
instead of a completed event. Automatic child-completion delivery SHALL say
which limit stopped the task. Artifacts produced by the stopped turn SHALL
transfer to the parent as they do for a completed task. Recovery from a
limit-stopped terminal checkpoint SHALL produce the same outcome. It MUST keep
an outcome already recorded for that turn by an earlier build.

#### Scenario: Output limit after a preamble

- **GIVEN** a child says "One quick check, then I'll write the document",
  runs a tool, and its next response runs over the output limit
- **WHEN** the turn ends
- **THEN** the child's outcome is a limit outcome naming the output limit
- **AND** neither the outcome nor the parent stream reports a completion or
  the preamble
- **AND** the child is idle with a limit error and accepts a follow-up

#### Scenario: Limit outcome recovers after a failed parent save

- **GIVEN** a child task stopped at a limit and the parent's outcome save
  failed
- **WHEN** the coordinator recovers the terminal checkpoint
- **THEN** it records the same limit outcome once and publishes one limit
  event

#### Scenario: Earlier build recorded the limit as a completion

- **GIVEN** a parent ledger recorded a limit-stopped turn as completed
- **WHEN** an upgraded coordinator recovers that child
- **THEN** recovery succeeds and the recorded outcome stands

### Requirement: Host-reviewed tools stay in read-only child views

A child factory SHALL be able to name, on the builder it returns, the tools it
reviewed for a read-only child view. When a child's tool scope is read-only or
its workspace is the read-only view, the coordinator SHALL keep a tool that
only reads the filesystem, clock or random source, and SHALL also keep a named
tool whose permission upper bound includes none of `fs.write`, `fs.create`,
`fs.delete`, `host.fs.write`, `external.write`, or `process.spawn`. A named tool
whose bound includes any of those MUST NOT be kept. Delegation-management
tools MUST NOT reach a child view even when named.

#### Scenario: A reviewed network read reaches a read-only child

- **GIVEN** a factory that names `fetch` (bound `net.http`, `data.egress`)
  and `artifact.read` (a host-defined permission)
- **WHEN** a child is spawned with a read-only tool scope, and another with
  scope `all` in the read-only workspace view
- **THEN** both children's views hold `fetch`, `artifact.read` and the
  filesystem read tools

#### Scenario: An unnamed network tool stays out

- **GIVEN** the same factory and a tool `unreviewed_fetch` it does not name
- **WHEN** a read-only child is spawned
- **THEN** the child's view does not hold `unreviewed_fetch`

#### Scenario: A named tool that writes stays out

- **GIVEN** a factory that names tools bound by `fs.create`,
  `process.spawn`, and a workspace write, and the host's delegation tool
- **WHEN** a read-only child is spawned
- **THEN** the child's view holds none of them
