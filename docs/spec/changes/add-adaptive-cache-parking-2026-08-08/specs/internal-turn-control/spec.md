## ADDED Requirements

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
