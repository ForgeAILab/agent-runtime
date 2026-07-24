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
