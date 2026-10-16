## ADDED Requirements

### Requirement: Flat semantic summary is an opt-in alternative to LCM

A host SHALL be able to compose the flat semantic-summary coordinator instead
of LCM through `RuntimeBuilder::semantic_summary`. A runtime MUST have at most
one semantic compaction strategy. Building MUST fail when LCM and a flat
summary are both configured, whether the summary was added through the
dedicated method or as a component with id `harness.semantic_summary`.
Building MUST also fail when the flat summary is combined with
`soft_on_turn_boundary(true)`.

#### Scenario: Host composes the flat summary
- **GIVEN** a runtime built with `RuntimeBuilder::semantic_summary` and no LCM
- **WHEN** a turn commits under context pressure
- **THEN** the coordinator stores the summarized originals, records the
  summary under `harness.semantic_summary`, and projects it into later
  requests
- **AND** no LCM component is present

#### Scenario: Both strategies are configured
- **GIVEN** a builder with `RuntimeBuilder::lcm` and a flat summary, by method
  or by component id
- **WHEN** the runtime is built
- **THEN** building fails with a configuration error naming both strategies

#### Scenario: Soft turn-boundary compaction with the flat summary
- **GIVEN** a builder with the flat summary and `soft_on_turn_boundary(true)`
- **WHEN** the runtime is built
- **THEN** building fails with a configuration error

### Requirement: Idle semantic compaction returns the protected summary

`SessionHandle::try_idle_semantic_compaction` SHALL run, with the flat summary
configured, the coordinator's idle compaction under the
`cache_idle_compaction` purpose. It SHALL return the protected summary, any
fallback reason, and the usage, and SHALL record that usage with source
`SemanticSummary`. LCM's `try_idle_compaction` result MUST be unchanged.

#### Scenario: Idle compaction under the flat summary
- **GIVEN** an idle session with the flat summary configured and eligible old
  turns
- **WHEN** the host calls `try_idle_semantic_compaction`
- **THEN** the result carries the new protected summary and its usage
- **AND** the usage ledger attributes that usage to the semantic summary

#### Scenario: LCM idle compaction is unchanged
- **GIVEN** a runtime configured with LCM
- **WHEN** the host calls `try_idle_compaction`
- **THEN** the result carries no summary body

### Requirement: Persisted flat summary state resumes under the flat summary

Persisted `harness.semantic_summary` schema-v1 state SHALL resume as live
state when the flat summary is configured, and MUST NOT be imported into LCM
or rejected. A host whose ordinary session store omits the state SHALL be
able to restore it through `restore_semantic_summary_if_absent`. That restore
MUST fail closed unless the session is idle and the component revision,
source session, canonical history prefix and summary revision all match. It
MUST never overwrite state the runtime already restored. State written by the
flat coordinator MUST remain importable by the LCM legacy decoder.

#### Scenario: Resume keeps the summary
- **GIVEN** a session persisted with flat summary state
- **WHEN** it resumes under a runtime configured with the flat summary
- **THEN** the next request projects the persisted summary
- **AND** the namespace is still present and no LCM state is created

#### Scenario: Host restores omitted state
- **GIVEN** a resumed idle session without the namespace and a matching
  protected copy held by the host
- **WHEN** the host calls `restore_semantic_summary_if_absent`
- **THEN** it returns true and the summary is projected on the next request

#### Scenario: Restore does not overwrite
- **GIVEN** a session that already holds the namespace
- **WHEN** the host calls `restore_semantic_summary_if_absent`
- **THEN** it returns false and the state is unchanged

#### Scenario: LCM imports flat summary state
- **GIVEN** a session whose flat summary state was written by the restored
  coordinator
- **WHEN** it resumes under a runtime configured with LCM
- **THEN** the legacy import succeeds as it does for pin-line state

### Requirement: Flat summary fallback is reported as a downgrade

The runtime SHALL emit `HarnessEvent::SemanticSummaryFallback` when the flat
coordinator cannot produce a summary and falls back to unsummarized history.
The event is observable as `RuntimeEvent::Downgrade` with capability
`semantic_summary`, and the turn SHALL continue.

#### Scenario: Summary model fails
- **GIVEN** a session under pressure whose summary model returns an error
- **WHEN** the turn commits
- **THEN** a `Downgrade` event with capability `semantic_summary` is emitted
- **AND** the turn's result is committed without a new summary
