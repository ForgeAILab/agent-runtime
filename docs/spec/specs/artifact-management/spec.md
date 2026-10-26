# artifact-management Specification

## Purpose
TBD - created by archiving change stabilize-session-harness-pipeline. Update Purpose after archive.
## Requirements
### Requirement: Session-private artifact storage

The runtime harness SHALL expose an injected `ArtifactStore` orthogonal to
workspace filesystem access. Artifacts MUST be session-private by default and
carry stable reference, content hash, media type, byte length, sensitivity,
provenance, and retention metadata.

#### Scenario: Tool output exceeds the inline budget
- **GIVEN** a tool returns content too large for model context
- **WHEN** the harness offloads it
- **THEN** the model receives a bounded preview and typed artifact reference
- **AND** the original remains retrievable under session policy

### Requirement: Artifact reads are bounded and authorized

The standard artifact-read ability SHALL use paginated bounded reads and
invocation-specific permission checks. An artifact reference MUST NOT imply
workspace or cross-session authority.

#### Scenario: Another session presents an artifact reference
- **GIVEN** an artifact belongs to a different session
- **WHEN** the current session requests it
- **THEN** the read fails closed
- **AND** no artifact content is returned

### Requirement: Offloading preserves provenance

Tool-output offloading and semantic summarization SHALL retain references to
the original artifact, covered fragment identifiers, hashes, sensitivity, and
model-purpose attribution. Irreversible truncation MUST NOT be the only way to
fit retrievable output into context.

#### Scenario: Summary replaces old tool output
- **GIVEN** old tool output was stored as an artifact
- **WHEN** a validated semantic summary replaces its inline preview
- **THEN** the summary records the artifact and covered fragment identities
- **AND** authorized bounded reads can still recover the original

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

### Requirement: Text artifact pages never split a character

`artifact.read` SHALL return a page of a text artifact (media type `text/*`,
JSON, or XML) as UTF-8 text when an edge of the page falls inside a
multi-byte character, leaving the partial character out, and SHALL report the
`offset` and `next_offset` of the bytes it returned so following `next_offset`
reads every character exactly once. A page of another media type, a page that
is not UTF-8 inside its bounds, or a page too short to hold one whole character
SHALL be returned as bytes unchanged.

#### Scenario: The last byte starts a character

- **GIVEN** a tool-outcome artifact of two-byte characters
- **WHEN** a 2,047-byte page is read from offset 0
- **THEN** it is returned as text of 1,023 characters with `next_offset` 2,046

#### Scenario: The offset falls inside a character

- **GIVEN** a `text/plain` artifact of two-byte characters
- **WHEN** a page is read from offset 1
- **THEN** it is returned as text with `offset` 2

#### Scenario: Binary media stays bytes

- **GIVEN** an `image/png` artifact
- **WHEN** a page is read
- **THEN** it is returned as bytes
