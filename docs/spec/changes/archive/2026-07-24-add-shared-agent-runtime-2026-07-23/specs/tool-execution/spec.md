## ADDED Requirements

### Requirement: Neutral tool contract

Tools SHALL declare a stable name, description, input schema, effects, and
asynchronous invocation contract. Invocation context MUST carry workspace,
deadline, cancellation, output-limit, approval, and request identity without
consumer-domain types.

#### Scenario: Consumer registers a product tool

- **GIVEN** a host implements the neutral tool trait
- **WHEN** it registers the tool before constructing a session
- **THEN** the shared agent loop can advertise and invoke it
- **AND** the shared repository does not depend on the consumer package

### Requirement: Fail-closed approval

Mutating or process-spawning tools MUST obtain an allowed decision from the
injected approval policy before side effects. A missing or failed approval
implementation SHALL deny the action.

#### Scenario: Headless host has no approval policy

- **GIVEN** a tool request can modify a workspace
- **AND** the host has not supplied an allowing approval policy
- **WHEN** the runtime evaluates the invocation
- **THEN** it returns a structured denial without running the tool

### Requirement: Deterministic tool registry

The tool registry SHALL reject name conflicts and preserve deterministic
advertisement and result ordering.

#### Scenario: Duplicate names are registered

- **GIVEN** two tools declare the same stable name
- **WHEN** the runtime seals the registry
- **THEN** construction fails with a name-conflict error

### Requirement: Side-effect-aware scheduling

The runtime MAY execute independent read-only tools concurrently, but MUST
serialize or reject tool calls whose declared write scopes overlap unless the
host supplies an explicit conflict policy.

#### Scenario: Two writes target one path

- **GIVEN** one model turn requests two writes to the same path
- **WHEN** the runtime schedules the calls
- **THEN** it does not execute them concurrently
- **AND** result ordering remains deterministic
