# tool-execution Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
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

### Requirement: Invocation-specific preparation

Every tool call SHALL be schema-validated and prepared into canonical
arguments, concrete security resource, required typed permissions, effects,
display metadata, and a preparation fingerprint before authorization or
approval. Invocation MUST execute exactly the prepared action.

#### Scenario: Edit targets a relative path
- **GIVEN** an edit call names a relative workspace path
- **WHEN** the tool prepares the call
- **THEN** it resolves and authorizes the exact canonical path
- **AND** execution fails if the prepared fingerprint or canonical resource
  changes before invocation

### Requirement: Descriptor permissions bound prepared authority

Tool ability descriptors SHALL use the registry's typed permission vocabulary
and declare a conservative permission upper bound. A prepared invocation whose
permissions are not a subset of that bound MUST fail closed before approval or
side effects.

#### Scenario: Tool prepares undeclared network access
- **GIVEN** a descriptor declares filesystem read only
- **WHEN** preparation requests network egress
- **THEN** the executor rejects the invocation as outside its declared bound
- **AND** user approval cannot override the rejection

### Requirement: Pending approval is bounded and resumable

Approval SHALL evaluate one immutable prepared action and SHALL observe turn
cancellation and deadline. Pending approval state MUST distinguish allow,
deny, timeout, cancellation, and unavailable host support and MAY be resumed
from a protected checkpoint.

#### Scenario: Turn expires while approval is open
- **GIVEN** a prepared action is waiting for user approval
- **WHEN** the turn deadline elapses
- **THEN** the wait ends with a structured timeout result
- **AND** the tool is never invoked

### Requirement: Edited actions are prepared again

If a host supports editing a proposed action, the edited arguments MUST return
through schema validation, preparation, authorization, and approval as a new
prepared fingerprint. A prior grant or approval MUST NOT be reused.

#### Scenario: Approval editor changes the target
- **GIVEN** a user changes an edit action from one file to another
- **WHEN** the edited action is submitted
- **THEN** the runtime prepares and authorizes the new canonical path
- **AND** discards the prior action's approval eligibility
