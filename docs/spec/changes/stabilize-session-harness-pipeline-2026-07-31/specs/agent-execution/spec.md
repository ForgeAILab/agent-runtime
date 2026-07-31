## ADDED Requirements

### Requirement: Checkpointable direct turn machine

The canonical direct loop SHALL execute as a versioned serializable state
machine with explicit planning, model, pending-action, tool, completion, and
terminal states. Transitions MUST be idempotent and MUST NOT require a general
graph engine.

#### Scenario: Process stops after one tool result
- **GIVEN** a model requested several prepared tool calls
- **AND** one result was committed and checkpointed before process exit
- **WHEN** the turn resumes
- **THEN** the committed result is not executed again
- **AND** the machine continues from the recorded state

### Requirement: Turn interruption is not session cancellation

Every active turn SHALL have its own cancellation handle. Interrupting the
current turn MUST leave the session able to accept a later turn, while terminal
session cancellation MUST propagate permanently to all active and future work.

#### Scenario: User interrupts then sends another prompt
- **GIVEN** one turn is streaming
- **WHEN** the host interrupts that turn
- **THEN** the turn completes as cancelled
- **AND** a later turn on the same session may complete normally

### Requirement: Provider output is attempt scoped

Visible and reasoning deltas SHALL identify their logical request and provider
attempt and remain speculative until an explicit attempt commit. A discarded
attempt MUST NOT contribute canonical assistant history or committed visible
output.

#### Scenario: Retry succeeds after partial output
- **GIVEN** the first attempt emits partial text and fails retryably
- **WHEN** the runtime discards it and a second attempt succeeds
- **THEN** live and replay reducers commit only the second attempt's text
- **AND** usage and failure diagnostics for both attempts remain observable
