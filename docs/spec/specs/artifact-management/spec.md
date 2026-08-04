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
