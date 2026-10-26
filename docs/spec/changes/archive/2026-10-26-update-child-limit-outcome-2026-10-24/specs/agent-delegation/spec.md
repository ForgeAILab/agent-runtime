## ADDED Requirements

### Requirement: Limit-stopped child tasks

When a delegated child's turn ends at a configured limit, Runtime SHALL record
a distinct limit outcome that names the limit. It MUST NOT be a completed
outcome and MUST NOT carry the child's last assistant message as a result.
The child SHALL remain available for follow-ups within its limits. Its status
SHALL carry no last result and a limit error naming the limit until its next
task starts. The parent stream SHALL receive a child-limit-reached event
instead of a completed event. Automatic child-completion delivery SHALL say
which limit stopped the task. Artifacts produced by the stopped turn SHALL
transfer to the parent as they do for a completed task. Recovery from a
limit-stopped terminal checkpoint SHALL produce the same outcome. It MUST keep
an outcome already recorded for that turn by an earlier build.

#### Scenario: Output limit after a preamble

- **GIVEN** a child says "One quick check, then I'll write the document",
  runs a tool, and its next response runs over the output limit
- **WHEN** the turn ends
- **THEN** the child's outcome is a limit outcome naming the output limit
- **AND** neither the outcome nor the parent stream reports a completion or
  the preamble
- **AND** the child is idle with a limit error and accepts a follow-up

#### Scenario: Limit outcome recovers after a failed parent save

- **GIVEN** a child task stopped at a limit and the parent's outcome save
  failed
- **WHEN** the coordinator recovers the terminal checkpoint
- **THEN** it records the same limit outcome once and publishes one limit
  event

#### Scenario: Earlier build recorded the limit as a completion

- **GIVEN** a parent ledger recorded a limit-stopped turn as completed
- **WHEN** an upgraded coordinator recovers that child
- **THEN** recovery succeeds and the recorded outcome stands
