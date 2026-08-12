## ADDED Requirements

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
