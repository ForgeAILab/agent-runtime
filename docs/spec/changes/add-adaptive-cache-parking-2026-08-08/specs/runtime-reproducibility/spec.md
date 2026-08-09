## ADDED Requirements

### Requirement: Versioned Runtime mechanism extension state

Runtime SHALL persist cache lifecycle state, synthetic operation identity, and
child-outcome consumption state through versioned namespaced extension records
using the existing SessionSnapshot and VersionedSessionState contracts. Each
record MUST declare sensitivity and revision, and incompatible state MUST fail
closed rather than being guessed. Cache operation idempotency MUST bind an
operation ID to a redaction-safe one-way fingerprint of its exact semantic
request and authority capability. An exact duplicate MAY return its committed
result without provider I/O; reuse with a different identity, purpose,
resource kind, handoff suffix, authority, or bounded request shape MUST return
a conflict without provider I/O or protected output.

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

#### Scenario: Operation ID is reused for another request

- **GIVEN** a cache operation ID already identifies a committed handoff
- **WHEN** a caller submits that ID with a different suffix, authority, cache
  identity, purpose, or bounded request shape
- **THEN** Runtime returns a structured conflict and no protected prior output
- **AND** it performs no provider request

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

### Requirement: Cache checkpoint journal scope

Runtime SHALL bind each non-terminal cache checkpoint watermark to the
operation's synthetic cache turn. During recovery, journal reconciliation MUST
truncate and republish only that scoped cache-operation tail; ordinary session,
child, and shutdown events emitted while an asynchronous protected checkpoint
save is in progress MUST remain durable and MUST NOT be dropped or replayed.
Terminal cache checkpoints are state-only on restart because their lifecycle
tail already crossed the protected terminal boundary.

#### Scenario: An unrelated event interleaves with a cache checkpoint save

- **GIVEN** a cache operation has written a Prepared, Started, or ResultReady
  checkpoint and an unrelated session or child event is emitted while the next
  protected save is pending
- **WHEN** the process resumes from that cache checkpoint
- **THEN** recovery truncates only cache events carrying the checkpoint's
  synthetic turn at or after its watermark
- **AND** the unrelated event remains in the journal exactly once
- **AND** cache lifecycle/evidence/usage events are republished in their
  original order without provider I/O

### Requirement: Reproducible cache lifecycle attribution

Runtime SHALL retain reproducible cache lifecycle attribution in the persisted
synthetic-operation manifest (the versioned cache-mechanism extension plus
protected `CacheOperationCheckpoint` and `CacheOperationResultCheckpoint`)
and lifecycle events, including redaction-safe cache identity,
provider/model/adapter revisions, operation
purpose, request/attempt identity, evidence status, and bounded metrics
sufficient for equivalent replay and audit. The ordinary `RunManifest` MAY
retain only its existing identity projection; it is not required to contain
transient operation ids or evidence. Raw prompt content, provider bodies,
credentials, and consumer resume-capsule text MUST remain outside ordinary
observability.

#### Scenario: Audit a suspended cache identity

- **GIVEN** a maintenance miss suspends an exact cache identity
- **WHEN** an operator inspects the persisted synthetic-operation manifest or
  protected cache checkpoint
- **THEN** the identity digest, provider/model revisions, purpose, and
  suspension reason are identifiable
- **AND** raw prompt or provider response content is absent
