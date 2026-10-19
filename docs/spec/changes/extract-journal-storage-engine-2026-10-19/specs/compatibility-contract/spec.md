## ADDED Requirements

### Requirement: Engine extraction is source-compatible

Extracting the journal engine SHALL NOT change or remove any public item of `SessionJournal`, `SessionStore`, `CheckpointStore`, the journal DTOs, `JournalReader`, `JournalProjectionPolicy`, `IdentityJournalProjection`, `JournalCommitFailurePoint` or `InMemorySessionJournal` (`protected`, `ordinary`, `fail_next_commit_at`, `revoke_writer`, `enable_collection`, `root_epoch`, `authorize_retirement`, `pin_transfer`, `retain_root`, `release_pin`, `stage_leaves`, `compact`, `object_count`, `object_bytes`). Testkit fixtures and `assert_*` functions SHALL keep their signatures and behavior. New items MUST be additive and listed in the CHANGELOG.

#### Scenario: A consumer built against the previous release recompiles

- **WHEN** a host or test that uses `InMemorySessionJournal` and the testkit journal fixtures is rebuilt against this change
- **THEN** it compiles without source edits
- **AND** its journal tests observe the same results

### Requirement: A journal format change keeps old readers and never rewrites silently

If a journal format lever ships, its objects and heads SHALL carry journal schema 5 while existing schema-4 objects keep schema 4. Readers MUST accept schema 4 and 5 for at least one full release after the first release whose writer produces schema 5. A writer MUST continue an existing schema-4 head in schema 4; converting a session to schema 5 SHALL require an explicit host request that publishes one counted full-replacement commit. Materialized checkpoints SHALL remain schema 3.

#### Scenario: An existing schema-4 session after the upgrade

- **WHEN** a runtime with schema-5 writing enabled resumes a session whose head is schema 4
- **THEN** it reads the session and commits further boundaries in schema 4
- **AND** no schema-4 object is rewritten unless the host requested the upgrade

#### Scenario: Materialized checkpoints are unchanged

- **WHEN** a schema-5 head is materialized
- **THEN** the resulting `TurnCheckpoint` is schema 3 and byte-identical to what a schema-4 head of the same state materializes
