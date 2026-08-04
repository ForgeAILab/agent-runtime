## ADDED Requirements

### Requirement: Structured turn submission and control

Turn submission SHALL return a structured result containing a turn handle or a
specific rejection. A turn handle MUST support completion and turn-local
interruption, while the session API separately exposes terminal session
cancellation.

#### Scenario: Submission occurs during shutdown
- **GIVEN** session shutdown has stopped accepting work
- **WHEN** a host submits new user input
- **THEN** submission returns a structured shutdown error
- **AND** no orphan turn identifier is minted

### Requirement: Per-session execution context

Starting or resuming a session SHALL create one execution context containing
all mutable planner, activation, extension, and active-turn state. The runtime
MAY share immutable providers, registries, policies, executors, and catalogs
across those contexts.

#### Scenario: Session resumes
- **GIVEN** a compatible persisted session and checkpoint
- **WHEN** the runtime resumes it
- **THEN** history, usage, manifests, identity, activation, planner, and
  pending-turn state are restored for that session
- **AND** no mutable state is borrowed from another live session
