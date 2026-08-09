## ADDED Requirements

### Requirement: Conditional child-completion continuation

Runtime SHALL expose `try_admit_child_completion_if_idle` as a bounded
provenance-bearing delegation/session operation. It consumes all ready
protected child outcomes named by a `ChildCompletionAdmissionRequest` and
attempts at most one delegation.child-completion internal turn when the parent
is idle at the serialized decision boundary. It MUST return
`ChildCompletionAdmission::{Accepted, Busy, Stale, Shutdown, Conflict}` and
MUST be safe to retry with the same expected `ChildOutcomeCursor` revision.

#### Scenario: Idle parent admits one batch

- **GIVEN** a parent is idle and several protected child outcomes are ready
- **WHEN** the child-completion operation reaches the serialized boundary
- **THEN** Runtime consumes the canonical ready batch and admits one attributed
  internal turn
- **AND** no second child-completion provider turn starts concurrently

#### Scenario: Parent became busy

- **GIVEN** a user, goal, or local action acquired the parent boundary first
- **WHEN** child-completion admission evaluates the session
- **THEN** it returns busy
- **AND** it does not consume the protected outcomes

### Requirement: Parent parking has no implicit provider work

The shared Runtime integration SHALL permit a consumer to represent a parent
as waiting for child work without starting a provider or tool request merely
because that state exists. Provider work MAY begin only after a real user,
goal, admitted child-completion turn, or explicitly authorized synthetic
operation crosses its normal admission boundary.

#### Scenario: Parent waits for a child

- **GIVEN** the consumer marks a parent as waiting for child completion
- **WHEN** no new turn or authorized synthetic operation is admitted
- **THEN** Runtime emits no provider request
- **AND** the parent remains recoverable through protected state

#### Scenario: Child completion arrives while user input is pending

- **GIVEN** a protected child outcome and a real user submission are both ready
- **WHEN** Runtime serializes the admission boundary
- **THEN** the user turn wins
- **AND** child-completion delivery remains protected for a later idle boundary

### Requirement: Replay-safe child-completion integration

The child-completion operation SHALL stage its admitted turn, outcome cursor,
event sequence, and protected checkpoint as one parent TurnCheckpoint revision.
The outcome MUST remain protected until that revision commits. Recovery MUST
see either the prior cursor with no accepted turn or the committed cursor with
the accepted internal boundary, without duplicating provider or tool work.

#### Scenario: Crash after outcome consumption

- **GIVEN** the cursor and internal-turn acceptance are persisted in one parent
  TurnCheckpoint revision
- **WHEN** the process resumes
- **THEN** the accepted child-completion turn resumes at its recorded boundary
- **AND** consumed outcomes are not injected a second time

#### Scenario: Crash before outcome consumption

- **GIVEN** protected outcomes are staged but the parent TurnCheckpoint
  revision did not commit
- **WHEN** the parent resumes
- **THEN** the cursor remains at its prior revision
- **AND** the same outcomes can be admitted once later
