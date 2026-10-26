# runtime-reproducibility Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Versioned run manifest

Every planned provider request SHALL produce a versioned run manifest
containing registry snapshot/view fingerprints, resolved model profile,
capability resolver and activation revisions, tokenizer and adapter revisions,
context/compaction/cache policy revisions, ordered segment identifiers and
hashes, token counts, and context/cache fingerprints. Runtime retention SHALL
be an ordered finite recent window; longer historical retention is host-owned.

#### Scenario: Audit a completed turn

- **GIVEN** a completed turn used automatic capability routing and compaction
- **WHEN** an operator inspects its retained or host-archived manifest
- **THEN** the exact registry, model, activation, tokenizer, context, and policy revisions are identifiable
- **AND** the manifest explains why compaction occurred without requiring raw sensitive content

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

Session persistence SHALL retain versioned manifest data for its configured
recent window to resolve the same registry view, model profile, activation set,
and context decisions during equivalent replay. Replay outside that window
MUST require host-archived manifest evidence. Missing manifests or changed
required revisions MUST fail explicitly unless the host opts into a labeled
non-equivalent replay.

#### Scenario: Required skill revision is unavailable

- **GIVEN** a persisted turn references a specific skill revision
- **AND** only a different revision is installed during replay
- **WHEN** equivalent replay is requested
- **THEN** replay fails with a structured revision-mismatch result
- **AND** it does not silently substitute the installed revision

#### Scenario: Historical manifest was evicted

- **WHEN** equivalent manifest replay targets a record outside the retained window without host-archived evidence
- **THEN** replay fails explicitly before provider or tool I/O
- **AND** no manifest is fabricated from current registry state or conversation history

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
history, usage, identity, and the ordered retained manifest window after every
completed turn, not only during orderly session shutdown. Eviction MUST affect
only diagnostic manifest records and MUST NOT remove execution history,
usage, identities, or protected extension state.

#### Scenario: Process exits after a completed turn

- **GIVEN** a turn reached its terminal event
- **WHEN** the process exits before explicit session shutdown
- **THEN** a resumed session retains that turn and the newest manifests within the configured window
- **AND** the snapshot does not regress to its pre-turn execution state

### Requirement: Protected checkpoints are distinct from audit journals

Exact resumable turn state SHALL be stored through a protected checkpoint contract with a journal/checkpoint watermark. Native version-4 checkpoints MAY reference exact protected SessionJournal objects instead of embedding another snapshot, provided every referenced execution object is durable and validated before the checkpoint is published. Redacted observability journals and ordinary SessionStore projections MUST NOT be treated as sufficient to reconstruct raw pending arguments, sensitive content, or completed side effects. The protected-state split and all existing boundary, authority, sensitivity and transition checks SHALL remain.

#### Scenario: Approval is pending at restart

- **GIVEN** an exact prepared action was checkpointed while awaiting approval
- **WHEN** the host restarts and resumes the session
- **THEN** it can present that same preparation fingerprint for a decision
- **AND** does not reconstruct arguments from a redacted event record

#### Scenario: Prepared action is stored once by reference

- **GIVEN** a referenced checkpoint publishes an exact protected prepared-action object
- **WHEN** the next transition reuses that action
- **THEN** it references the existing object without another body write
- **AND** confidentiality, idempotency, reauthorization and no-repeat guarantees remain

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
retained manifest window, identity counters, extension state, artifact
ownership, usage, task count, and checkpoint boundary. Recovery MUST NOT
derive exact execution state from a redacted parent journal or reset accounting
because the child runtime was reconstructed. Manifest eviction MUST NOT alter
child execution or accounting.

#### Scenario: Idle child receives a post-restart follow-up

- **GIVEN** an idle child has two completed turns, artifacts, and cumulative usage before process exit
- **WHEN** the parent resumes and follows up that child
- **THEN** the third turn is planned from both prior turns and the restored child state
- **AND** its retained manifest order, identities, artifact ownership, usage, and task count remain monotonic

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

### Requirement: Replay-Safe LCM State and Operations

Runtime persistence SHALL retain enough versioned, redaction-safe LCM metadata
to restore the authorized timeline binding, immutable frontier, DAG revision,
active node set, source fingerprints, compaction policy/algorithm/model/sizer
revisions, per-node model purpose, classifications, usage, operation
watermarks, and the distinct binding/authorization, backing-store,
store-view, and source-classifier revisions. Equivalent replay MUST reuse
committed entries and nodes without invoking a summary model or repeating a
mutation; missing or incompatible required revisions MUST fail explicitly
under the existing replay policy.

#### Scenario: Process exits after node commit before session checkpoint
- **GIVEN** a leaf or condensation mutation committed under a stable operation fingerprint
- **AND** the process exits before the corresponding session checkpoint is published
- **WHEN** recovery reconciles the store and protected checkpoint
- **THEN** it recognizes and adopts the compatible committed mutation exactly once
- **AND** it performs no provider summary request and creates no duplicate node

#### Scenario: Process exits after model response before node commit
- **GIVEN** a protected checkpoint contains an attributed, validated summary result whose node mutation did not commit
- **WHEN** the session recovers with matching policy, model, source, and DAG revisions
- **THEN** it may commit that protected result without repeating model work
- **AND** a mismatch fails explicitly without exposing the protected body

#### Scenario: Equivalent replay inspects a compacted turn
- **GIVEN** a persisted turn used active LCM nodes and a recent raw suffix
- **WHEN** equivalent replay resolves its manifest
- **THEN** the same ordered node identities, revisions, source fingerprints, classifications, and context fingerprint are resolved
- **AND** raw summary or source bodies are not required in the ordinary run manifest

#### Scenario: Revision identities are not collapsed
- **GIVEN** a projected node was created under one host binding, store schema,
  authorized view, and source classifier
- **WHEN** its protected LCM state and associated run manifest are persisted
- **THEN** the binding/authorization, store, store-view, node, policy,
  algorithm, model, and sizer revisions remain separately represented, while
  the classifier revision remains in protected LCM state alongside the joined
  classification
- **AND** a mismatch in one revision fails equivalent replay without being
  hidden by a replacement revision

### Requirement: Redaction-Safe LCM Lifecycle Events

Runtime SHALL emit versioned, redaction-safe events for LCM pressure
decisions, operation admission, escalation, node commit, condensation,
fallback, import, expansion metadata, and structured failure. Events MUST carry
only bounded opaque identities, revisions, classifications, counts, hashes,
usage, and stable reasons without raw source entries, summary bodies, protected
artifacts, credentials, or authorization grants. Run manifests MUST preserve
the fuller revision and per-node producer/purpose record needed for equivalent
replay; event metadata is not a substitute for protected state.

#### Scenario: Condensation completes
- **GIVEN** a protected condensation operation commits one parent over active children
- **WHEN** the lifecycle event is emitted
- **THEN** it identifies the timeline/DAG revision, parent identity, bounded child identities/count, covered range, algorithm/policy revisions, and token reduction
- **AND** it contains no child or parent summary text

#### Scenario: Unauthorized expansion is denied
- **GIVEN** an LCM expansion request fails the host-authorized view check
- **WHEN** the denial is observed
- **THEN** telemetry records a stable denial class and bounded request identity
- **AND** it leaks neither node existence nor content

#### Scenario: Lifecycle and manifest replay are redaction-safe
- **GIVEN** a model-produced node is projected into a turn
- **WHEN** lifecycle events and the turn manifest are persisted
- **THEN** lifecycle events contain only bounded IDs, ranges, fingerprints,
  revisions, classifications, counts, usage, and stable reasons, while the
  manifest/protected state retains the node's purpose and producer metadata
- **AND** summary/source bodies, artifacts, credentials, and opaque authority
  grants remain outside telemetry and ordinary manifests

### Requirement: Typed LCM failures across runtime boundaries
RangeOverlap, EntryConflict, TimelineOwned and LcmDivergence MUST retain typed,
redaction-safe evidence through the harness, request driver and RuntimeError.
They MUST NOT become config(string). Protected exact checkpoints and ordinary
redaction policy SHALL retain their existing roles.

#### Scenario: Store conflict at provider admission
- **WHEN** an LCM store reports RangeOverlap or EntryConflict
- **THEN** the host receives the specific typed LCM failure and Conflict kind
- **AND** no provider request occurs

#### Scenario: Durable fork seed
- **WHEN** an ordinary store redacts a Sensitive summary seed
- **THEN** compatible protected state restores its exact value
- **AND** no summary text enters errors or diagnostic manifests

#### Scenario: Fork seed generated on resume
- **WHEN** a resume with the Fork policy generates a summary seed
- **THEN** the seed is protected before ordinary persistence
- **AND** the planner receives the exact seed, never a redacted placeholder

### Requirement: Revision Tolerant LCM Tunables
Persisted LCM identity MUST match (schema, authorized timeline/binding, store,
classifier and guard). Tunable policy, sizer, model and algorithm changes SHALL
rebuild derived state from authorized active store nodes instead of rejecting
valid state. Canonical history, provenance and pending durable operations MUST
remain validated; protected state and authority checks MUST remain fail-closed.

#### Scenario: Policy bump on live session
- **WHEN** a live persisted session is resumed with a new policy revision
- **THEN** derived state MUST rebuild with current tunables without losing committed nodes or failing decode.

#### Scenario: Identity mismatch
- **WHEN** the binding, schema, store, classifier or guard identity differs
- **THEN** restore MUST reject the state before provider or summary work.

### Requirement: Session manifest diagnostics are bounded

Runtime SHALL keep at most a configured positive finite K manifest records per
session, defaulting to 32, ordered by their existing append order. The public
snapshot.manifests vector and recent_manifests accessor MUST expose the same
retained suffix. Adoption of a legacy snapshot MUST apply the bound only after
normal recovery validation; raw serde readers MUST retain stored records.

#### Scenario: More records are planned than the window

- **GIVEN** a window of two records
- **WHEN** five provider requests are planned
- **THEN** live and ordinary snapshot diagnostics retain exactly the newest two records in order
- **AND** retries of the same frozen request do not create additional manifest records

#### Scenario: Older snapshot has an unbounded list

- **WHEN** an older snapshot with more than K manifests is deserialized and resumed
- **THEN** raw deserialization retains the entire stored list
- **AND** validated live adoption and the next ordinary save retain its newest K records

### Requirement: Checkpoints exclude diagnostic manifests

New runtime-generated protected checkpoint snapshots SHALL contain no
diagnostic manifests while retaining exact execution history, usage,
identities, sensitive extension state, pending requests/actions, and outcome
watermarks. Historical manifest lists MUST remain readable. Manifest
retention differences MUST NOT establish or invalidate an execution boundary;
ordinary/protected overlay SHALL require equal validated planned-step boundary
counters in constant-size versioned extension state, with legacy full lists
providing their counter before pruning. All other boundary, revision,
authority, and idempotency checks MUST remain. Missing or malformed required
boundary evidence MUST fail explicitly rather than attest ordinary state.

#### Scenario: Prepared action resumes without ordinary diagnostics

- **GIVEN** a new protected checkpoint contains an exact prepared action and no manifests
- **AND** the ordinary session snapshot is unavailable
- **WHEN** the host resumes that checkpoint
- **THEN** it recovers the same preparation fingerprint and committed outcomes without duplicate work
- **AND** its recent manifest diagnostics may be empty

#### Scenario: Old checkpoint contains manifests

- **WHEN** a current reader loads a valid historical checkpoint with manifests
- **THEN** the original records remain readable as diagnostics
- **AND** repeating the same stored checkpoint revision does not rewrite its payload merely to prune manifests

#### Scenario: Execution boundary differs despite absent manifests

- **WHEN** terminal overlay detects incompatible identity, planned-step boundary, history structure, usage, or extension revision
- **THEN** recovery still fails closed
- **AND** diagnostic omission cannot bypass the existing execution-state checks

#### Scenario: Legacy checkpoint follows a newly bounded ordinary save

- **GIVEN** validated legacy lists supplied the live planned-step counter before pruning
- **WHEN** a later restart compares a bounded ordinary snapshot with the original legacy terminal checkpoint
- **THEN** it compares the persisted counter with the legacy full-list count and all other required boundary evidence
- **AND** it neither rewrites the checkpoint at its existing revision nor treats trimmed list length as the lifetime counter

### Requirement: History sharing preserves independent public values

Internal history sharing SHALL preserve the existing owned public history,
snapshot, request, and checkpoint value contracts and serialized forms.
Subsequent appends, projections, compaction, or recovery MUST NOT mutate a
previously returned snapshot or a frozen request, and equivalent execution
MUST retain exact provider continuation, prepared actions, and fingerprints.

#### Scenario: Caller retains an old snapshot while another turn runs

- **WHEN** a caller holds snapshot/history values while the session appends or compacts later history
- **THEN** the held values retain their original content, ordering, and serde form
- **AND** the session's new values reflect only its validated later execution

#### Scenario: Provider request and checkpoint cross an asynchronous boundary

- **WHEN** a frozen request or prepared-action checkpoint is retained while runtime state advances
- **THEN** its messages, signed continuation, exact action arguments, and operation fingerprint remain unchanged
- **AND** recovery still avoids repeating committed provider or tool work

#### Scenario: Existing consumer reads public history fields

- **WHEN** a consumer uses ProviderRequest.messages or SessionSnapshot.history as Vec<Message>, HistoryView.history as Arc<[Message]>, or the existing history/with_history methods
- **THEN** it compiles and retains its existing value semantics
- **AND** no consumer adopts a new history wrapper or serde migration
