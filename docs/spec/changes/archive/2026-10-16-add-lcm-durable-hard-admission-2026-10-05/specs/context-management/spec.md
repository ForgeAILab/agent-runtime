## ADDED Requirements

### Requirement: Durable ordinary-store hard admission
The runtime SHALL persist each pending hard-compaction response in the Sensitive
LCM extension of a full canonical SessionSnapshot before committing its DAG
mutation when SessionStore is configured and CheckpointStore is absent. It MUST
retain existing authorization, identity, body, source-plan and revision checks.

#### Scenario: Crash before the staged mutation lands
- **WHEN** the ordinary intent snapshot is saved but its DAG CAS has not landed
- **THEN** resume validates and discards the uncommitted intent before new work
- **AND** retains complete canonical history and already charged summary usage.

#### Scenario: Crash after the staged mutation lands
- **WHEN** its DAG CAS lands before terminal ordinary persistence
- **THEN** resume adopts exactly the proven successor without duplicate nodes
- **AND** persists repaired state before exposing a live session handle.

#### Scenario: Fully saved turn
- **WHEN** terminal persistence succeeds
- **THEN** normal resume retains history and the committed DAG without replay.

#### Scenario: Failed staging save
- **WHEN** SessionStore fails to save the hard intent
- **THEN** admission surfaces an error and does not commit that DAG mutation
- **AND** provider I/O does not proceed.

#### Scenario: Other host compositions
- **WHEN** CheckpointStore is configured or neither store is configured
- **THEN** hard admission and recovery retain their existing behavior.
