## ADDED Requirements

### Requirement: Child catalog and checkpoint recovery are atomic and idempotent

Durable child lifecycle transitions SHALL connect a parent-scoped child record
to the exact child session checkpoint with versioned identities, revisions,
and watermarks. The runtime MUST publish a record watermark only after the
referenced child state is durable, MUST reconcile partial commits
deterministically, and MUST use an exclusive execution lease or equivalent
compare-and-swap guard before continuing a child.

#### Scenario: Crash occurs between child checkpoint and catalog commit

- **GIVEN** a newer child checkpoint is durable but its catalog transition did
  not commit before process loss
- **WHEN** the parent coordinator recovers
- **THEN** it reconciles the compatible checkpoint and record without executing
  provider or tool work
- **AND** emits at most one recovery transition for the resulting state

#### Scenario: Two processes attempt the same child resume

- **GIVEN** two hosts can read the same durable parent and child records
- **WHEN** both attempt to resume the same interrupted child
- **THEN** only one acquires the execution lease and commits progress
- **AND** the other receives a structured conflict without duplicating work

### Requirement: Child recovery preserves canonical accounting

Resuming or following up a durable child SHALL restore its ordered history,
manifests, identity counters, extension state, artifact ownership, usage, task
count, and checkpoint boundary. Recovery MUST NOT derive exact execution state
from a redacted parent journal or reset accounting because the child runtime
was reconstructed.

#### Scenario: Idle child receives a post-restart follow-up

- **GIVEN** an idle child has two completed turns, artifacts, and cumulative
  usage before process exit
- **WHEN** the parent resumes and follows up that child
- **THEN** the third turn is planned from both prior turns and the restored
  child state
- **AND** its manifest order, identities, artifact ownership, usage, and task
  count remain monotonic
