## ADDED Requirements

### Requirement: Resume Disowns an Uncheckpointed Tail
LCM resume validation SHALL recover a checkpoint whose store holds entries
past the checkpointed frontier that were appended by a turn which never
reached a terminal checkpoint. The recovery MUST apply only when the
checkpoint carries no pending summary, the checkpointed history prefix matches
canonical history, the store's active DAG equals the DAG the checkpoint
recorded, and every entry below the frontier matches canonical history. It
MUST then truncate the store from the frontier and re-anchor the checkpoint to
the resulting revision. It MUST NOT change any summary node, and any other
disagreement MUST remain a conflict.

#### Scenario: Process killed between provider steps
- **WHEN** a host with only a session store is killed after a turn appended several provider steps and before the session was saved
- **THEN** resuming the saved session MUST succeed
- **AND** the timeline MUST hold no entry of the killed turn
- **AND** the next turn MUST complete.

#### Scenario: Recovery interrupted after its truncation
- **WHEN** a recovery truncated the tail and the process died before the repaired checkpoint was saved
- **THEN** the next resume of the unchanged saved session MUST succeed.

#### Scenario: Store without truncation
- **WHEN** the store does not support truncating its tail
- **THEN** resume MUST fail with the original conflict and the store MUST be unchanged.
