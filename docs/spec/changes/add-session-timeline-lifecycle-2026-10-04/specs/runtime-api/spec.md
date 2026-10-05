## ADDED Requirements

### Requirement: Explicit session intent
Runtime MUST expose StartSession::create(id, seed), resume(id), and ephemeral(history)
with command schema version 2, and no builder that names an unnamed create.
Create SHALL reject existing persisted state; resume SHALL require existing
state and reject any seed with typed Conflict. Ephemeral SHALL neither load nor
save session/checkpoint stores. The result SHALL report resumed.
StartSession::from_json SHALL reject any other schema version with a typed
Config error before field decoding.

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

#### Scenario: Schema 1 payload
- **WHEN** a host decodes a schema 1 StartSession payload with from_json
- **THEN** it receives a Config RuntimeError naming the unsupported version

### Requirement: Fork and supersession
Runtime SHALL fork an idle session using Summary, FromIndex or Empty seed and
NewTimeline or Continue LCM choice. Summary SHALL enter the planner as a Stable
Summary fragment. The child SHALL be persisted before its parent is superseded;
superseded parents MUST reject future resume and work. With LCM configured,
Continue SHALL accept only Empty or FromIndex(0) and reject other seeds with
typed Conflict before any fork intent.

#### Scenario: Topic rotation
- **WHEN** a host forks with a Summary and NewTimeline
- **THEN** the child claims a fresh authorized timeline
- **AND** the planner receives a cacheable summary and no parent raw history
- **AND** the parent is marked superseded

#### Scenario: Continue timeline
- **WHEN** a host explicitly forks with Continue
- **THEN** the new owner claims the existing timeline under a newer generation
- **AND** its canonical history retains the existing timeline projection

#### Scenario: Continue seed that cannot be honoured
- **WHEN** a Continue fork carries a Summary or a FromIndex suffix after index zero
- **THEN** the fork returns Conflict and the parent stays usable

#### Scenario: Active source
- **WHEN** a fork source has pending or active work
- **THEN** the fork returns Conflict without provider work

### Requirement: Durable fork repair
Runtime MUST validate the resolver hook, replacement emptiness and claim support
before it protects pending fork intent. It MUST protect the successor seed
before superseding its parent. Protected stores SHALL admit a validated initial
idle session boundary without provider work. A failure after the intent SHALL
roll the intent back while the successor owns nothing durable; otherwise only
the same request SHALL repair partial persistence. Runtime::abort_fork SHALL
clear an intent a crash left behind under the same condition. Completed retries
SHALL return the existing successor.

#### Scenario: Fork that cannot complete
- **WHEN** the resolver cannot supply the successor binding
- **THEN** the fork returns typed ForkRequired before any fork intent
- **AND** the parent keeps accepting turns and resumes normally

#### Scenario: Failure after the intent
- **WHEN** the successor's first protected save fails before it owns anything durable
- **THEN** the intent is rolled back and the parent keeps accepting turns
- **AND** the identical retry completes the fork

#### Scenario: Abort after a crash
- **WHEN** a crash leaves a pending fork whose successor owns nothing durable
- **THEN** abort_fork clears it and the parent resumes normally

#### Scenario: Child seed save fails after ownership transfer
- **WHEN** a Continue successor has claimed the timeline and its protected save fails
- **THEN** the parent remains fenced and default resume returns Conflict
- **AND** retrying the same fork after restart repairs the child and supersedes its parent
- **AND** no provider request is repeated

#### Scenario: Fork after a frontier error on restart
- **WHEN** an idle source has typed below-frontier divergence on normal resume
- **THEN** an explicit NewTimeline fork may load it only for archival and supersession
- **AND** strict binding, store, classifier, guard and checkpoint identities remain validated
- **AND** no old-source provider work or truncation occurs
