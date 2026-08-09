## ADDED Requirements

### Requirement: Immutable landed Runtime revision for consumers

The coordinated change SHALL provide Smith and every supported consumer an
immutable landed Agent Runtime revision containing the approved cache,
delegation, admission, persistence, event, usage, and conformance contracts.
Consumer manifests MUST NOT depend on a moving branch or an uncommitted
cross-repository state.

#### Scenario: Smith updates its dependency

- **GIVEN** Runtime implementation and compatibility gates have passed
- **WHEN** Smith adopts the shared mechanism
- **THEN** it pins the immutable landed Runtime revision
- **AND** its contract suite exercises the landed public shapes

#### Scenario: Runtime revision is not landed

- **GIVEN** a local Runtime checkout contains the draft API but no immutable
  landed revision
- **WHEN** a consumer landed-revision pin is prepared
- **THEN** the compatibility gate blocks the pin
- **AND** a local path override remains development-only

### Requirement: Cross-consumer gate covers adaptive mechanism contracts

The compatibility gate SHALL run Runtime and supported consumer suites
for new provider capability, cache lifecycle events, synthetic usage,
delegation wait, protected outcome cursor, and conditional admission behavior.
An exhaustive consumer failure MUST block the compatible landed revision.

#### Scenario: Consumer misses a lifecycle event

- **GIVEN** Runtime adds a canonical cache lifecycle event
- **AND** one supported consumer cannot deserialize or project it
- **WHEN** landed-revision eligibility is evaluated
- **THEN** the immutable revision is not accepted as compatible
- **AND** the consumer migration is coordinated before pinning
