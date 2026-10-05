## ADDED Requirements

### Requirement: Explicit session intent
Runtime MUST expose StartSession::create(id, seed), resume(id), and ephemeral(history)
with command schema version 2. Create SHALL reject existing persisted state; resume
SHALL require existing state and reject any seed with typed Conflict. Ephemeral
SHALL neither load nor save session/checkpoint stores. The result SHALL report resumed.

#### Scenario: Resume cannot drop a seed
- **WHEN** a resume request includes seed history
- **THEN** construction returns Conflict before any provider request
- **AND** no history is overwritten

#### Scenario: Create cannot silently resume
- **WHEN** create names an existing snapshot or protected checkpoint
- **THEN** construction returns Conflict

#### Scenario: Ephemeral host history
- **WHEN** a host starts ephemeral history with stores configured
- **THEN** that exact history seeds a new session and resumed is false
- **AND** no session or checkpoint store write occurs

### Requirement: Fork and supersession
Runtime SHALL fork an idle session using Summary, FromIndex or Empty seed and
NewTimeline or Continue LCM choice. Summary SHALL enter the planner as a Stable
Summary fragment. The child SHALL be persisted before its parent is superseded;
superseded parents MUST reject future resume and work.

#### Scenario: Topic rotation
- **WHEN** a host forks with a Summary and NewTimeline
- **THEN** the child claims a fresh authorized timeline
- **AND** the planner receives a cacheable summary and no parent raw history
- **AND** the parent is marked superseded

#### Scenario: Continue timeline
- **WHEN** a host explicitly forks with Continue
- **THEN** the new owner claims the existing timeline under a newer generation
- **AND** its canonical history retains the existing timeline projection

#### Scenario: Active source
- **WHEN** a fork source has pending or active work
- **THEN** the fork returns Conflict without provider work

### Requirement: Durable fork repair
Runtime MUST protect pending fork intent before transferring a binding, and
protect the successor seed before superseding its parent. Protected stores
SHALL admit a validated initial idle session boundary without provider work.
Only the same request SHALL repair partial persistence; completed retries SHALL
return the existing successor.

#### Scenario: Child seed save fails after ownership transfer
- **WHEN** a protected child seed save fails
- **THEN** the parent remains fenced and default resume returns Conflict
- **AND** retrying the same fork after restart repairs the child and supersedes its parent
- **AND** no provider request is repeated

#### Scenario: Fork after a frontier error on restart
- **WHEN** an idle source has typed below-frontier divergence on normal resume
- **THEN** an explicit NewTimeline fork may load it only for archival and supersession
- **AND** strict binding, store, classifier, guard and checkpoint identities remain validated
- **AND** no old-source provider work or truncation occurs
