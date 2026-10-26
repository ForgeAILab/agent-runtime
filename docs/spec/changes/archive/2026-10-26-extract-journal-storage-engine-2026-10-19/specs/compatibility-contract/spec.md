## ADDED Requirements

### Requirement: Engine extraction is source-compatible

Extracting the journal engine SHALL NOT change or remove any public item of `SessionJournal`, `SessionStore`, `CheckpointStore`, the journal DTOs, `JournalReader`, `JournalProjectionPolicy`, `IdentityJournalProjection`, `JournalCommitFailurePoint` or `InMemorySessionJournal` (`protected`, `ordinary`, `fail_next_commit_at`, `revoke_writer`, `enable_collection`, `root_epoch`, `authorize_retirement`, `pin_transfer`, `retain_root`, `release_pin`, `stage_leaves`, `compact`, `object_count`, `object_bytes`). Testkit fixtures and `assert_*` functions SHALL keep their signatures and behavior. New items MUST be additive and listed in the CHANGELOG.

#### Scenario: A consumer built against the previous release recompiles

- **WHEN** a host or test that uses `InMemorySessionJournal` and the testkit journal fixtures is rebuilt against this change
- **THEN** it compiles without source edits
- **AND** its journal tests observe the same results
