## ADDED Requirements

### Requirement: Versioned Runtime mechanism extension state

Runtime SHALL persist cache lifecycle state, synthetic operation identity, and
child-outcome consumption state through versioned namespaced extension records
using the existing SessionSnapshot and VersionedSessionState contracts. Each
record MUST declare sensitivity and revision, and incompatible state MUST fail
closed rather than being guessed.

#### Scenario: Cache lifecycle state resumes

- **GIVEN** a session snapshot contains a compatible cache lifecycle extension
- **WHEN** Runtime resumes the session
- **THEN** the identity state and suspension evidence are restored exactly
- **AND** no provider maintenance request is inferred from persistence alone

#### Scenario: Extension revision changes

- **GIVEN** a persisted outcome cursor uses an unsupported revision
- **WHEN** Runtime loads the parent session
- **THEN** it reports a structured compatibility failure or explicit
  non-equivalent recovery choice
- **AND** it does not silently reset the cursor

### Requirement: Protected cursor and checkpoint ordering

Runtime SHALL stage automatic child-outcome consumption and any admitted
child-completion internal turn in one parent TurnCheckpoint revision using the
existing protected checkpoint and journal watermarks. Runtime MUST leave the
outcome protected until that revision commits, make cursor advancement
idempotent, and MUST NOT treat a redacted event stream as sufficient evidence
for exact recovery.

#### Scenario: Committed child completion is replayed

- **GIVEN** a parent checkpoint records the accepted internal turn and cursor
  revision together
- **WHEN** the process resumes
- **THEN** it continues from that exact boundary
- **AND** it does not replay consumed child outcomes or committed provider work

#### Scenario: Recovery sees the pre-commit state

- **GIVEN** the parent TurnCheckpoint revision did not commit after staging
- **WHEN** the process resumes
- **THEN** the prior cursor and protected outcome remain visible
- **AND** no child-completion turn is treated as accepted

#### Scenario: Event observer missed the terminal event

- **GIVEN** a bounded observer misses a child-completed event
- **WHEN** the parent recovers its protected state
- **THEN** the child outcome remains discoverable through the protected cursor
- **AND** no lossy event replay is used to reconstruct exact content

### Requirement: Reproducible cache lifecycle attribution

Persisted manifests and lifecycle events SHALL retain redaction-safe cache
identity, provider/model/adaptor revisions, operation purpose, request/attempt
identity, evidence status, and bounded metrics sufficient for equivalent
replay and audit. Raw prompt content, provider bodies, credentials, and
consumer resume-capsule text MUST remain outside ordinary observability.

#### Scenario: Audit a suspended cache identity

- **GIVEN** a maintenance miss suspends an exact cache identity
- **WHEN** an operator inspects the persisted manifest
- **THEN** the identity digest, provider/model revisions, purpose, and
  suspension reason are identifiable
- **AND** raw prompt or provider response content is absent
