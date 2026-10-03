## ADDED Requirements

### Requirement: Failure metadata is backward-readable and consumer-gated

Failure classification and provider timing/recovery additions SHALL default
when missing and omit unknown values on serialization. Current readers MUST
read legacy errors, and unknown or malformed diagnostic evidence MUST NOT make
an enclosing error record unreadable or grant retry authority. All supported
consumer suites MUST pass; any required Rust literal or destructuring updates
MUST be coordinated before a compatible release is published.

#### Scenario: Legacy error journal is loaded

- **WHEN** a current reader loads an Error event without the new fields
- **THEN** kind, message, retryability, and metadata remain readable
- **AND** class is Unclassified and timing/recovery evidence is absent

#### Scenario: Future or malformed diagnostic evidence is lenient

- **WHEN** a reader encounters an unknown reason tag, a class payload it cannot decode, an unknown closed recovery value, or a timing value that is not a non-negative integer in otherwise valid error JSON
- **THEN** it uses Unclassified for an undecodable class, `Unknown` for a future stage/component name, and absence for undecodable optional timing/recovery evidence
- **AND** the enclosing RuntimeError, Error or ChildFailed event, and child failure record remain readable
- **AND** separately valid evidence fields remain readable
- **AND** it does not invent a retry schedule

#### Scenario: A consumer uses a Rust struct literal

- **WHEN** adding failure metadata makes a supported consumer's RuntimeError literal fail compilation
- **THEN** the candidate is blocked until that consumer's documented source update passes its gate
- **AND** JSON compatibility alone is insufficient to publish a compatible release
