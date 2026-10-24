## ADDED Requirements

### Requirement: Parent-owned child replies name their child

Artifact provenance SHALL carry an optional delegated child identity, omitted
from serialization when absent so earlier records load unchanged. When Runtime
stores a completed child's reply as a parent-owned copy, that copy's provenance
SHALL name the child that produced it. The reply's idempotency key SHALL differ
from the key used before attribution existed, so a store never sees one key
with two provenances.

#### Scenario: A long reply is stored for its parent

- **GIVEN** a factory with an artifact store and a child reply longer than a
  delivery excerpt
- **WHEN** the child's task completes
- **THEN** the stored reply is owned by the parent session with purpose
  `delegation.child-reply`
- **AND** its provenance names that child and carries no lineage

#### Scenario: An artifact stored before attribution is loaded

- **GIVEN** serialized provenance without a child identity
- **WHEN** it is deserialized
- **THEN** it equals the same provenance built without a child
- **AND** serializing it again omits the child field
