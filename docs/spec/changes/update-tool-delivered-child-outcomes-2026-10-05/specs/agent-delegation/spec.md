## ADDED Requirements

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
