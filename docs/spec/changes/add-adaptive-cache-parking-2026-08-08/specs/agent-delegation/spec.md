## ADDED Requirements

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
