## ADDED Requirements

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
