## ADDED Requirements

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
closed rather than widen authority or silently change identity.

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

## MODIFIED Requirements

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
