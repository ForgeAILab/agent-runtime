## ADDED Requirements

### Requirement: Structured agent-to-user questionnaire

The generic harness SHALL support a bounded questionnaire interaction
containing origin session/turn/call identity, stable request, question, and
choice identities, one to three questions, optional mutually exclusive
choices, optional free-form answers, deadline, cancellation, and sensitivity
metadata.

#### Scenario: Agent needs a product choice
- **GIVEN** the agent cannot safely choose between two materially different
  implementations
- **WHEN** it invokes the questionnaire ability
- **THEN** the interactive host presents the structured choices
- **AND** the selected answer resumes the same turn as the ability result

### Requirement: Interaction and approval are separate authorities

A questionnaire response SHALL provide task information only. It MUST NOT
approve a prepared tool action, widen permissions, resolve a grant, or bypass
the authorization and approval pipeline.

#### Scenario: Answer resembles permission
- **GIVEN** a questionnaire asks whether the user prefers an implementation
  that may later require a write
- **WHEN** the user chooses that implementation
- **THEN** the choice informs the model
- **AND** any subsequent write still undergoes independent preparation,
  authorization, and approval

### Requirement: Pending interactions are cancellable and durable

The turn machine SHALL represent a pending questionnaire explicitly, observe
turn cancellation and deadline, and persist exact pending state through the
protected checkpoint contract. Answer, decline, timeout, cancellation, and
unavailable host support MUST be distinct results.

#### Scenario: Process restarts with a question open
- **GIVEN** a questionnaire was checkpointed before an answer
- **WHEN** an interactive host resumes the session
- **THEN** it re-presents the same request identity and choices
- **AND** one accepted answer resumes the turn exactly once

### Requirement: Non-interactive hosts never hang

Questionnaire activation SHALL depend on host interaction readiness. A
non-interactive host without an explicit response protocol MUST omit the
ability or return a structured interaction-unavailable result.

#### Scenario: Headless run cannot ask a question
- **GIVEN** no TTY or bidirectional interaction broker is configured
- **WHEN** questionnaire support is requested
- **THEN** the run reports interaction unavailable without waiting
  indefinitely
- **AND** no answer is fabricated

### Requirement: Child interaction requires explicit host policy

A child session MUST NOT gain direct user-interaction readiness merely because
its parent has it. Hosts MAY authorize attributed child questionnaires or
require children to return a structured needs-input result through the parent.

#### Scenario: Child needs a user decision
- **GIVEN** a child session lacks direct interaction readiness
- **WHEN** it reaches a material ambiguity
- **THEN** it returns a structured needs-input result to its parent
- **AND** the runtime does not silently open a root-user prompt on the child's
  behalf
