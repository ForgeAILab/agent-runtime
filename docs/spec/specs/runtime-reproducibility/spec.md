# runtime-reproducibility Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Versioned run manifest

Every turn SHALL reference a versioned run manifest containing registry
snapshot/view fingerprints, resolved model profile, capability resolver and
activation revisions, tokenizer and adapter revisions, context/compaction/cache
policy revisions, ordered segment identifiers and hashes, token counts, and
context/cache fingerprints.

#### Scenario: Audit a completed turn

- **GIVEN** a completed turn used automatic capability routing and compaction
- **WHEN** an operator inspects its persisted manifest
- **THEN** the exact registry, model, activation, tokenizer, context, and policy
  revisions are identifiable
- **AND** the manifest explains why compaction occurred without requiring raw
  sensitive content

### Requirement: Observable planning lifecycle

The runtime SHALL emit versioned neutral events for registry sealing, model
resolution, capability retrieval and activation, context planning and
compaction, cache-plan changes, downgrades, and budget failures. Events MUST
include bounded metrics and structured reasons without exposing secrets or raw
sensitive content by default.

#### Scenario: Automatic routing activates browser research

- **GIVEN** intent routing selects authorized research capabilities
- **WHEN** the initial context plan is completed
- **THEN** event consumers receive the snapshot, resolution, activation, and
  context-planning milestones in order
- **AND** the events report capability IDs and token totals without embedding
  credentials or full skill instructions

### Requirement: Revision-safe persistence and replay

Session persistence SHALL retain enough versioned manifest data to resolve the
same registry view, model profile, activation set, and context decisions during
equivalent replay. Missing or changed required revisions MUST fail explicitly
unless the host opts into a labeled non-equivalent replay.

#### Scenario: Required skill revision is unavailable

- **GIVEN** a persisted turn references a specific skill revision
- **AND** only a different revision is installed during replay
- **WHEN** equivalent replay is requested
- **THEN** replay fails with a structured revision-mismatch result
- **AND** it does not silently substitute the installed revision

### Requirement: Privacy-safe context telemetry

Default planning events and manifests SHALL store identifiers, classifications,
hashes, revisions, counts, and decisions rather than raw credentials, secrets,
or sensitive fragment content. Hosts MAY persist raw content only through an
explicit storage policy and sensitivity-aware contract.

#### Scenario: Tool result contains a secret

- **GIVEN** a sensitive tool result participates in context planning
- **WHEN** planning metrics and the run manifest are emitted
- **THEN** they contain its bounded identifier, classification, hash, and token
  count
- **AND** they do not contain the raw secret value

### Requirement: Provider usage calibration

Provider-reported input, output, reasoning, and cache usage SHALL remain
attempt-visible and MAY calibrate future estimator diagnostics. Observed usage
MUST NOT retroactively change the frozen context plan or replace preflight
limit enforcement.

#### Scenario: Provider reports a different input count

- **GIVEN** an estimated context plan was sent successfully
- **WHEN** the provider reports a different authoritative input count
- **THEN** both planned and observed counts remain attributable to their source
- **AND** the completed turn retains the original plan fingerprint

### Requirement: Completed turns are durably persisted

When a session store is configured, the runtime SHALL persist canonical
history, usage, identity, and all ordered manifests after every completed turn,
not only during orderly session shutdown.

#### Scenario: Process exits after a completed turn
- **GIVEN** a turn reached its terminal event
- **WHEN** the process exits before explicit session shutdown
- **THEN** a resumed session retains that turn and every earlier manifest
- **AND** the snapshot does not regress to its pre-turn state

### Requirement: Protected checkpoints are distinct from audit journals

Exact resumable turn state SHALL be stored through a protected checkpoint
contract with a journal/checkpoint watermark. Redacted observability journals
MUST NOT be treated as sufficient to reconstruct raw pending arguments,
sensitive content, or completed side effects.

#### Scenario: Approval is pending at restart
- **GIVEN** an exact prepared action was checkpointed while awaiting approval
- **WHEN** the host restarts and resumes the session
- **THEN** it can present that same preparation fingerprint for a decision
- **AND** does not reconstruct arguments from a redacted event record

### Requirement: Boundary recovery is idempotent

Every checkpointed transition SHALL carry enough identity and fingerprints to
resume without repeating committed provider calls, user answers, approvals, or
tool side effects. A revision mismatch MUST fail explicitly or require a
labeled non-equivalent recovery policy.

#### Scenario: Tool result was committed before a crash
- **GIVEN** a tool result and its transition watermark were persisted
- **WHEN** the process resumes the turn
- **THEN** the runtime reuses the committed result
- **AND** does not invoke the tool again

### Requirement: Child catalog and checkpoint recovery are atomic and idempotent

Durable child lifecycle transitions SHALL connect a parent-scoped child record
to the exact child session checkpoint with versioned identities, revisions,
and watermarks. The runtime MUST publish a record watermark only after the
referenced child state is durable, MUST reconcile partial commits
deterministically, and MUST use an exclusive execution lease or equivalent
compare-and-swap guard before continuing a child.

#### Scenario: Crash occurs between child checkpoint and catalog commit

- **GIVEN** a newer child checkpoint is durable but its catalog transition did
  not commit before process loss
- **WHEN** the parent coordinator recovers
- **THEN** it reconciles the compatible checkpoint and record without executing
  provider or tool work
- **AND** emits at most one recovery transition for the resulting state

#### Scenario: Two processes attempt the same child resume

- **GIVEN** two hosts can read the same durable parent and child records
- **WHEN** both attempt to resume the same interrupted child
- **THEN** only one acquires the execution lease and commits progress
- **AND** the other receives a structured conflict without duplicating work

### Requirement: Child recovery preserves canonical accounting

Resuming or following up a durable child SHALL restore its ordered history,
manifests, identity counters, extension state, artifact ownership, usage, task
count, and checkpoint boundary. Recovery MUST NOT derive exact execution state
from a redacted parent journal or reset accounting because the child runtime
was reconstructed.

#### Scenario: Idle child receives a post-restart follow-up

- **GIVEN** an idle child has two completed turns, artifacts, and cumulative
  usage before process exit
- **WHEN** the parent resumes and follows up that child
- **THEN** the third turn is planned from both prior turns and the restored
  child state
- **AND** its manifest order, identities, artifact ownership, usage, and task
  count remain monotonic

### Requirement: Reproducible provider continuation content

Canonical persistence and equivalent replay SHALL retain bounded
provider-required continuation content, including signed reasoning blocks,
without rendering opaque signatures or treating them as host presentation
metadata. Missing required continuation MUST fail explicitly rather than
silently producing a non-equivalent provider request.

#### Scenario: Resume signature-only reasoning

- **GIVEN** a completed provider step contains redacted reasoning with an
  opaque signature and no summary text
- **WHEN** the session is saved, loaded, and equivalently replayed
- **THEN** the signed reasoning block remains in the same canonical position
- **AND** its signature is available only to provider request reconstruction

#### Scenario: Replay record predates signed continuation

- **GIVEN** an older valid session contains no signed provider continuation
- **WHEN** it is loaded for a provider that does not require signed history
- **THEN** the session remains backward compatible
- **AND** no empty or invented signature is added

#### Scenario: Required continuation cannot be restored

- **GIVEN** an equivalent replay targets a provider whose current-turn history
  requires signed continuation that is absent
- **WHEN** request preparation validates the history
- **THEN** replay fails with a structured incompatibility before provider I/O
- **AND** does not substitute provider-side state or an unsigned request

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
