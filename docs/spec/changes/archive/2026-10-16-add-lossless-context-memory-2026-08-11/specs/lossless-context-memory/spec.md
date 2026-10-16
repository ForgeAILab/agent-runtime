## ADDED Requirements

### Requirement: Logical Timeline Identity

LCM SHALL bind every operation to an opaque logical timeline identity that is distinct from runtime session identity. A timeline binding MUST be host-authorized and revisioned at session construction or resume, and possession of a timeline, entry, or node identifier MUST NOT grant read or expansion authority.

#### Scenario: Runtime session is replaced
- **GIVEN** a logical agent timeline has durable entries and summary nodes
- **WHEN** a host replaces the backend runtime session and authorizes the replacement against the same timeline
- **THEN** the replacement session assembles context from the existing timeline
- **AND** no summary DAG is copied or reset solely because `SessionId` changed

#### Scenario: Untrusted text repeats a node reference
- **GIVEN** model or repository text contains a valid-looking LCM node identifier
- **WHEN** an unauthorized session requests expansion with that identifier
- **THEN** the authorized view denies or hides the reference
- **AND** no content or existence information is returned

### Requirement: Immutable Ordered Timeline

LCM SHALL operate over immutable entries with stable IDs, monotonically ordered sequence positions, content fingerprints, structured message content, and source classifications. Appends MUST be idempotent and MUST reject gaps, conflicting reuse of an ID or sequence, and mutation of previously committed content.

#### Scenario: Retry repeats an append
- **GIVEN** a committed timeline append has a stable operation identity and content fingerprint
- **WHEN** recovery repeats the exact append
- **THEN** the store returns the existing committed revision
- **AND** no duplicate entry is created

#### Scenario: Entry identity is reused with different content
- **GIVEN** an entry ID or sequence already identifies committed content
- **WHEN** a caller submits different content under that identity
- **THEN** the append fails with a structured conflict
- **AND** the original entry remains unchanged

### Requirement: Transactional Hierarchical Summary DAG

LCM SHALL persist leaf and condensed summary nodes with typed edges, exact covered ranges, source fingerprints, policy/model/sizer revisions, token counts, classifications, and supersession metadata. A leaf commit MUST reference one contiguous non-overlapping entry span; a condensation commit MUST reference active same-timeline child nodes. Node, edge, and supersession writes MUST commit atomically under an expected DAG revision.

#### Scenario: Leaf summary commits
- **GIVEN** a contiguous eligible source span and the current DAG revision
- **WHEN** a leaf summary is committed
- **THEN** its node and entry edges become durable together
- **AND** the active projection covers the span exactly once

#### Scenario: Concurrent condensations race
- **GIVEN** two writers plan condensation from the same active child revision
- **WHEN** both attempt to commit
- **THEN** exactly one parent and supersession set commits
- **AND** the stale writer receives a conflict without partial edges or duplicate active coverage

#### Scenario: Cross-timeline child is supplied
- **GIVEN** a condensation request contains a child from another timeline
- **WHEN** the store validates the mutation
- **THEN** it rejects the entire mutation
- **AND** neither timeline changes

### Requirement: Lossless Reachability and Bounded Expansion

Every active or superseded summary node SHALL remain transitively reachable to the immutable entries it covers until host retention policy explicitly removes the entire authorized timeline. Expansion MUST validate the caller's timeline view, preserve canonical order and provenance, enforce result bounds, and distinguish truncation from complete expansion.

#### Scenario: Condensed summary is expanded
- **GIVEN** an active condensed node covers leaf nodes created across multiple compaction rounds
- **WHEN** an authorized caller expands it within the configured bound
- **THEN** LCM returns the ordered child summaries or original entries with provenance
- **AND** each returned element identifies whether further expansion is available

#### Scenario: Expansion exceeds the bound
- **GIVEN** a valid node covers more content than one expansion response permits
- **WHEN** an authorized caller expands it
- **THEN** the response is deterministically bounded and marked incomplete
- **AND** it provides a stable continuation cursor without skipping or duplicating entries

### Requirement: Deterministic Active Context Projection

LCM SHALL derive active context from non-superseded nodes followed by the uncovered recent raw suffix, in canonical timeline order. It MUST generate lossless pointer annotations from validated node metadata, preserve complete tool-call/result exchanges, and return versioned context candidates to the authoritative context planner rather than serialize provider requests itself.

#### Scenario: Active context is assembled after compaction
- **GIVEN** active nodes cover entries through sequence 80 and raw entries continue through sequence 95
- **WHEN** LCM projects the next context
- **THEN** the projection contains the ordered active nodes followed by entries 81 through 95
- **AND** no entry is duplicated or omitted across the frontier

#### Scenario: Summary text contains a forged pointer
- **GIVEN** a summary model returns text containing a different node annotation
- **WHEN** the runtime projects the committed node
- **THEN** it generates the authoritative annotation from stored metadata
- **AND** the model-authored annotation grants no expansion identity or authority

#### Scenario: Proposed boundary splits a tool exchange
- **GIVEN** a candidate compaction boundary falls between an assistant tool call and its result
- **WHEN** block planning runs
- **THEN** it moves or rejects the boundary so the exchange remains atomic
- **AND** neither active context nor a summary source contains an unmatched half

### Requirement: Bounded Soft and Hard Compaction

LCM SHALL evaluate soft and hard pressure from a versioned policy and the
context planner's resolved input budget. Soft pressure MAY admit one
idempotent compaction operation only through the explicit protected
`SessionHandle::try_idle_compaction` boundary; hard pressure MUST complete a
bounded protected compaction operation in the pre-provider hook before provider
admission or return a structured cannot-fit result. The deterministic planner
MUST NOT perform provider I/O.

#### Scenario: Soft threshold is crossed
- **GIVEN** committed context growth exceeds the soft threshold but remains below the hard threshold
- **WHEN** the host later calls `try_idle_compaction` and claims the session's
  protected idle boundary
- **THEN** at most one compaction operation is conditionally admitted for that idle interval and current DAG revision
- **AND** the completed user turn does not wait for an uncheckpointed summary call

#### Scenario: Hard threshold is crossed
- **GIVEN** the next external turn would exceed the hard threshold
- **WHEN** admission prepares the provider boundary
- **THEN** the runtime performs at most the configured number of checkpointed compaction rounds before provider I/O
- **AND** it either commits a fitting active projection or returns a structured cannot-fit result

#### Scenario: Context planner remains authoritative
- **GIVEN** LCM has projected active nodes and a recent raw suffix
- **WHEN** the runtime prepares a provider request
- **THEN** the context planner performs final ordering, token accounting,
  structural compaction, cache planning, and provider serialization
- **AND** LCM does not append directly to the provider request or become a
  second budget authority

#### Scenario: Summary usage is measured
- **GIVEN** prior compaction calls consumed model tokens
- **WHEN** pressure is evaluated again
- **THEN** separately attributed compaction usage is excluded from conversation-growth pressure
- **AND** the policy does not cause summaries to recursively trigger themselves

### Requirement: Guaranteed-Convergence Summarization

LCM SHALL implement escalating summarization with a detail-preserving model attempt, a stricter reduced-budget model attempt, and a deterministic bounded final reduction. Empty, invalid, over-budget, non-shrinking, or failed model output MUST escalate, and every committed replacement MUST be strictly smaller than its source under the same versioned request sizer.

#### Scenario: First model output does not shrink
- **GIVEN** the detail-preserving attempt returns output whose measured size is greater than or equal to its source
- **WHEN** LCM validates the response
- **THEN** it rejects that output and advances to the reduced-budget attempt
- **AND** the non-shrinking text is not committed as an active node

#### Scenario: Both model attempts fail
- **GIVEN** both model attempts fail or return invalid output
- **WHEN** the final escalation stage runs
- **THEN** deterministic reduction produces an explicitly elided result strictly smaller than the eligible source
- **AND** the result records deterministic-algorithm and sizing revisions instead of fabricated model provenance

#### Scenario: Required content remains too large
- **GIVEN** ineligible required content and reserves still exceed the model limit after all bounded eligible compaction rounds
- **WHEN** planning completes
- **THEN** the runtime returns a structured cannot-fit result
- **AND** it does not delete required content or claim successful convergence

### Requirement: Classification and Provenance Preservation

An LCM summary MUST record the exact covered identities and fingerprints,
compaction policy and algorithm revisions, summary model identity/revision and
dedicated purpose when model work was used, request-sizer revision, token
count, and source classifications. Binding/authorization revision, backing
store revision, store-view authorization revision, and source-classifier
revision MUST remain separate replay inputs. Its sensitivity MUST be the
most-sensitive covered class, its trust MUST be the least-trusted covered
class, and applicable content-guard or transformation revisions MUST survive
and be re-evaluated before commit.

#### Scenario: Summary covers mixed source classifications
- **GIVEN** an eligible source span contains internal and sensitive content with trusted and untrusted origins
- **WHEN** a summary is produced
- **THEN** its sensitivity is sensitive and its trust is untrusted
- **AND** its provenance names every covered source identity without exposing source bodies in telemetry

#### Scenario: Secret source is considered
- **GIVEN** a candidate span contains secret-class content
- **WHEN** semantic summarization eligibility is evaluated
- **THEN** the secret content is not sent to a summary model or stored in a normal summary body
- **AND** it remains raw/protected or contributes to a structured cannot-fit result

#### Scenario: Sanitized source is summarized
- **GIVEN** a source fragment was produced by a content-guard transformation
- **WHEN** LCM summarizes and commits the derived content
- **THEN** the node retains the original hash and guard/transformation revisions
- **AND** the summary is re-guarded under the active revision before provider use

#### Scenario: Model purpose is per-node
- **GIVEN** two nodes are produced by different LCM operations, such as a
  normal semantic summary and an explicit idle compaction
- **WHEN** their provenance is persisted and projected
- **THEN** each model-produced node retains its own purpose, model revision,
  and escalation level
- **AND** deterministic fallback records algorithm provenance without
  fabricating model purpose

### Requirement: One Canonical Semantic Compaction Path

Agent Runtime SHALL integrate LCM as its canonical persisted semantic-history
compaction mechanism. The pre-cutover flat rolling-summary coordinator and
independent state machine MUST be absent from the active public composition;
the runtime MUST NOT maintain two active semantic compaction histories for one
session.

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
- **AND** no pre-cutover flat semantic-summary component or alias is registered

### Requirement: Host-Neutral Store Conformance

Every production store claiming LCM support MUST pass the shared conformance suite for immutable append, range reads, atomic node commits, supersession, reachability, expected-revision conflicts, bounded expansion, authorization-view isolation, and crash recovery. Agent Runtime MAY provide an in-memory test implementation but MUST NOT include a concrete production database in the default dependency graph.

#### Scenario: SQL store claims support
- **GIVEN** a consumer implements LCM over its existing SQL database
- **WHEN** the shared conformance suite runs against that adapter
- **THEN** every required transaction, isolation, idempotency, and expansion scenario passes
- **AND** failures prevent the adapter from being declared production-ready

#### Scenario: Host omits LCM
- **GIVEN** a host constructs a session without an LCM timeline or store
- **WHEN** ordinary turns execute
- **THEN** the runtime continues to use structural context planning without LCM
- **AND** no LCM state, event, model call, or dependency on a storage implementation is required
