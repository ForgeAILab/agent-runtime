## MODIFIED Requirements

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

## ADDED Requirements

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
