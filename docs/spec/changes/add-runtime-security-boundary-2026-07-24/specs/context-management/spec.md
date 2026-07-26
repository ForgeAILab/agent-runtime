## MODIFIED Requirements

### Requirement: Versioned context fragments

Every source of provider context SHALL contribute a versioned fragment with a
stable identity, source, kind, priority, required status, content revision,
dependency/pairing metadata, sensitivity classification, trust classification,
content-guard provenance, and cache class. Contributors MUST NOT append
unplanned content directly to provider requests or silently classify unknown
external/tool content as trusted.

#### Scenario: Ability contributes tool instructions

- **GIVEN** an activated ability supplies instructions and a tool schema
- **WHEN** the turn is prepared
- **THEN** each contribution is represented by a versioned fragment carrying
  activation provenance, sensitivity, and trust
- **AND** changing content, trust, guard, or activation revision changes the
  resulting context fingerprint

### Requirement: Semantic context compaction

Compaction SHALL preserve required system/developer constraints, the current
user request, unresolved decisions, required ability instructions, and valid
tool-call/result pairs. It MAY remove expired optional fragments, bound tool
results, or summarize older history, but every summary MUST retain provenance,
covered identifiers, policy revision, sensitivity, trust classification,
content-guard revision, content hash, and token count. When a summary covers
fragments spanning more than one trust classification, the summary's trust
classification MUST be the least-trusted class among the covered fragments —
a minimum/least-trusted join, the inverse of the sensitivity join's
maximum/most-sensitive rule. A summary produced by sanitization or another
content-guard-derived transformation MUST be re-guarded under the applicable
content-guard revision rather than inheriting the trusted compactor's own
provenance.

#### Scenario: Old history is summarized

- **GIVEN** required content and recent turns approach the configured high
  watermark
- **WHEN** older eligible history is compacted
- **THEN** the plan targets the configured lower watermark
- **AND** the summary records exactly which messages it replaces
- **AND** no unmatched tool call or result remains

#### Scenario: Required content cannot fit

- **GIVEN** required fragments and output/reasoning reserve exceed the model
  limits even after permitted compaction
- **WHEN** planning completes
- **THEN** it fails with a structured cannot-fit report
- **AND** no required fragment is silently discarded

#### Scenario: Summary spans mixed trust classes

- **GIVEN** eligible history fragments include both trusted
  instructions-derived content and untrusted external/tool content at the same
  compaction priority
- **WHEN** they are compacted into one summary
- **THEN** the summary's trust classification is the least-trusted class among
  the covered fragments
- **AND** the summary is re-guarded under the active content-guard revision
  rather than inheriting the trusted compactor's own provenance

## ADDED Requirements

### Requirement: Structural containment of untrusted content

The context engine SHALL preserve explicit structural/source boundaries around
user, external, tool-result, and untrusted extension content when rendering
provider messages. Instruction-like text inside an untrusted fragment MUST
remain data and MUST NOT be promoted to host/system authority by concatenation,
compaction, sanitization, caching, or provider serialization.

#### Scenario: Web page contains a fake system instruction

- **GIVEN** retrieved web content includes text formatted as a system message
- **WHEN** it is guarded, planned, compacted, cached, and serialized
- **THEN** its external-content trust classification and source boundary remain
  attributable
- **AND** it does not become a system/developer instruction fragment

### Requirement: Provenance-preserving content guard decisions

Content-guard results SHALL carry guard and policy revisions, bounded risk
signals, transformation identifiers, original content hash, and the selected
allow/isolate/sanitize/quarantine/reject outcome. Sanitization MUST produce a
derived fragment rather than silently overwrite the original identity, and
quarantined/rejected content MUST NOT reach provider I/O.

#### Scenario: Sanitizer removes an unsafe control sequence

- **GIVEN** an untrusted fragment triggers a configured deterministic
  sanitization rule
- **WHEN** the context plan uses the sanitized derivative
- **THEN** the plan records the original hash and transformation revision
- **AND** telemetry contains no raw sensitive or quarantined content
