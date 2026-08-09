# internal-turn-control Specification

## Purpose
TBD - created by archiving change add-persistent-session-goals. Update Purpose after archive.
## Requirements
### Requirement: Internal turns are attributed without user history

The session API SHALL support bounded internal turn input with stable source,
revision, sensitivity, and optional goal identity/generation. Internal content
MUST be checkpointable and required for its turn, but MUST NOT append a
user-role message to canonical conversation history.

#### Scenario: Internal goal turn completes

- **GIVEN** a controller submits a valid internal goal input
- **WHEN** the turn plans, calls the provider, and completes
- **THEN** normal lifecycle/checkpoint evidence identifies its internal source
- **AND** canonical history contains no fabricated user continuation message

#### Scenario: Internal turn invokes a tool

- **GIVEN** an admitted internal turn requests an effectful tool
- **WHEN** the runtime processes that request
- **THEN** ordinary preparation, authorization, approval, workspace,
  cancellation, retry, and limit policy applies
- **AND** internal provenance grants no additional authority

### Requirement: Conditional internal admission is idle-only

`try_send_internal_if_idle` SHALL serialize with ordinary user submission and
session lifecycle state. It MUST either return an accepted turn handle or a
structured busy, stale, or shutdown result and MUST NOT queue internal work
behind a serving or queued user turn.

#### Scenario: Real user and controller race

- **GIVEN** a session becomes idle while user and internal submissions compete
- **WHEN** the serialized admission boundary resolves them
- **THEN** user work wins or the internal caller receives busy/stale rather
  than queued acceptance
- **AND** no internal request silently runs ahead of already-submitted user work

#### Scenario: Expected goal generation changed

- **GIVEN** an internal request names an expected active goal generation
- **WHEN** that goal was paused, edited, replaced, or completed first
- **THEN** admission returns stale
- **AND** starts no provider or tool work

### Requirement: Internal input survives protected recovery

Accepted internal input SHALL be included in protected turn/checkpoint state
and attributed manifests. Equivalent recovery MUST resume at the recorded
boundary without duplicating accepted provider calls or tool side effects.

#### Scenario: Process exits after internal input acceptance

- **GIVEN** an internal turn was accepted and checkpointed before provider I/O
- **WHEN** a compatible host resumes its protected checkpoint
- **THEN** the same attributed turn may continue exactly once
- **AND** no user-role message is synthesized during recovery

### Requirement: Internal work ends with session shutdown

Internal turn admission and execution SHALL remain process- and session-scoped.
Terminal session shutdown MUST reject new internal work, cancel/drain serving
work through existing bounds, and leave no detached scheduler.

#### Scenario: Controller shuts down

- **GIVEN** an active internal turn or pending continuation exists
- **WHEN** the host shuts down the controller and session
- **THEN** bounded cancellation/drain completes and later admission is rejected
- **AND** no provider or tool task continues detached

### Requirement: Child-completion internal source

The session API SHALL represent an automatic child-completion continuation as
an attributed InternalTurnSource with a stable source kind, revision,
sensitivity, and staged protected outcome cursor reference. It MUST remain
distinct from user input and goal work, MUST append no fabricated user-role
message, and MUST use the ordinary ProviderAttempt usage source.

#### Scenario: Child result is admitted

- **GIVEN** protected child outcomes are ready and the parent is idle
- **WHEN** Runtime admits a child-completion continuation
- **THEN** the turn carries child-completion provenance and its staged cursor
- **AND** canonical history contains no synthetic user message
- **AND** the attempt remains ordinary attributed internal work rather than
  synthetic cache usage

#### Scenario: Protected result is unavailable

- **GIVEN** a child-completion request names a cursor that is missing,
  regressed, or incompatible
- **WHEN** Runtime evaluates admission
- **THEN** it returns a structured stale or conflict result
- **AND** starts no provider or tool work

### Requirement: User-priority admission arbitration

Runtime SHALL serialize user, goal, and child-completion admission at one
session boundary. A real user submission winning the boundary MUST prevent
internal acceptance; internal work MUST NOT queue ahead of already-submitted
user work, and a rejected protected child outcome MUST remain available for a
later idle attempt.

#### Scenario: User and child completion race

- **GIVEN** a parent is idle and both a user submission and child-completion
  admission are ready
- **WHEN** the serialized boundary resolves the race
- **THEN** user work is admitted
- **AND** child-completion admission returns busy without consuming the outcome

#### Scenario: Goal and child completion race

- **GIVEN** a goal controller and child-completion controller compete while
  the session is idle
- **WHEN** one source wins serialized admission
- **THEN** the other source receives busy or stale
- **AND** no two internal provider turns run concurrently

### Requirement: Child completion observes ordinary turn controls

An admitted child-completion internal turn SHALL pass through ordinary context,
provider, tool, cancellation, deadline, usage, and limit controls. Child
provenance MUST NOT grant additional tool authority or bypass user-facing
turn limits.

#### Scenario: Child continuation requests a tool

- **GIVEN** a child-completion internal turn causes a provider tool call
- **WHEN** Runtime processes that call
- **THEN** normal authorization, approval, workspace, cancellation, and limit
  checks apply
- **AND** child-completion provenance grants no additional authority
