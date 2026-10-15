## MODIFIED Requirements

### Requirement: One Canonical Semantic Compaction Path

Agent Runtime SHALL integrate LCM as its canonical persisted semantic-history
compaction mechanism for hosts that configure `RuntimeBuilder::lcm`. A host
MAY instead opt in to the flat semantic-summary coordinator through
`RuntimeBuilder::semantic_summary`. The runtime MUST NOT maintain two active
semantic compaction histories for one session, and MUST reject a composition
that configures both.

#### Scenario: Existing semantic-summary state resumes
- **GIVEN** a persisted session contains valid semantic-summary schema-v1 state and matching canonical source history
- **WHEN** it resumes under a runtime configured with `RuntimeBuilder::lcm`
- **THEN** the runtime has a durable `SessionStore` and a coordinator configured
  with the protected legacy `ArtifactStore`, then automatically validates the
  schema-v1 protected state,
  exact canonical history/source fingerprint, protected artifact bytes and
  provenance, and host-authorized timeline binding
- **AND** it commits the equivalent LCM leaf and replacement protected
  checkpoint before accepting turns
- **AND** subsequent context projection uses only the LCM DAG

#### Scenario: Legacy summary source does not match
- **GIVEN** persisted semantic-summary state has a missing artifact, incompatible revision, or source fingerprint mismatch
- **WHEN** the session resumes with `.lcm` configured
- **THEN** it fails closed with a structured compatibility result
- **AND** no partial timeline or DAG mutation commits

#### Scenario: Legacy artifact store is not configured
- **GIVEN** a resumed session contains schema-v1 state but the configured
  coordinator has no protected legacy `ArtifactStore`
- **WHEN** automatic LCM cutover begins
- **THEN** resume fails closed before accepting turns
- **AND** no manual restore path or public importer is exposed

#### Scenario: Durable session store is not configured
- **GIVEN** a resumed session contains schema-v1 state but no durable
  `SessionStore` is configured for the runtime
- **WHEN** automatic LCM cutover begins
- **THEN** resume fails closed before accepting turns
- **AND** the legacy namespace is not acknowledged as migrated

#### Scenario: New session enables LCM
- **GIVEN** a new session has an authorized LCM binding
- **WHEN** runtime composition is sealed
- **THEN** exactly one LCM semantic compaction component is present
- **AND** no flat semantic-summary component is registered

#### Scenario: Host opts in to the flat summary
- **GIVEN** a runtime configured with `RuntimeBuilder::semantic_summary` and no LCM
- **WHEN** a session with schema-v1 semantic-summary state resumes
- **THEN** the state stays live and is not imported into LCM
- **AND** no LCM component is present
