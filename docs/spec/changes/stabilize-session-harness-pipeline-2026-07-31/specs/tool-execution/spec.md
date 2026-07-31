## ADDED Requirements

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
