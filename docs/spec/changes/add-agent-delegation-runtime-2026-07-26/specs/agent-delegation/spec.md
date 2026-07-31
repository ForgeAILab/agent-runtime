## ADDED Requirements

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
child-concurrency limits plus each child's turn/token/deadline limits through
the existing deterministic limit machinery. By default an over-capacity spawn
returns a structured capacity result; queueing is an explicit host policy.
Children MUST stop when their parent session or the process stops and MUST NOT
restart on session resume.

#### Scenario: Concurrency limit is reached

- **GIVEN** the per-parent running-child limit is reached
- **WHEN** the host requests another child without a queue policy
- **THEN** the runtime returns a structured capacity result
- **AND** the running-child limit is not exceeded

#### Scenario: Children stop with the parent

- **GIVEN** a parent session with running children ends
- **WHEN** the runtime tears the parent down
- **THEN** every running child receives cancellation and reaches a terminal
  event
- **AND** resuming the parent session later does not restart any child
