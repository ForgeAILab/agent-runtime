## ADDED Requirements

### Requirement: Manual semantic compaction may repeat within an idle interval

`SessionHandle::try_manual_semantic_compaction` SHALL run the same flat
semantic compaction as `try_idle_semantic_compaction` for an explicit host
request. An earlier attempt in the current idle interval MUST NOT refuse it.
It MUST apply every other admission rule of idle compaction: no running or
pending turn, no compaction in flight, a safe checkpoint, a configured flat
summary component, and the same persistence and rollback. After a manual
attempt the interval SHALL remain attempted for `try_idle_semantic_compaction`.

#### Scenario: Retry after an idle attempt

- **GIVEN** an idle session whose current interval already had a semantic
  compaction attempt
- **WHEN** the host calls `try_idle_semantic_compaction`
- **THEN** it is refused as busy
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** the compaction is admitted
- **AND** a later `try_idle_semantic_compaction` in the same interval is still
  refused as busy

#### Scenario: Retry after a failed summary

- **GIVEN** an idle interval whose semantic compaction failed with a fallback
  reason
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** the summary model is called again and its result is returned

#### Scenario: Manual compaction while another is in flight

- **GIVEN** an idle semantic compaction that is still running
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** it is refused as busy
- **AND** the running compaction completes normally
