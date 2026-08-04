# goal-accounting Specification

## Purpose
TBD - created by archiving change add-persistent-session-goals. Update Purpose after archive.
## Requirements
### Requirement: Goal token usage is provider-reported and exact once

The goal harness SHALL charge provider-reported uncached input plus output
tokens attributable after goal activation and SHALL exclude cached input.
Each attempt/turn contribution MUST be applied at most once and retain
reported or unknown provenance.

#### Scenario: Provider reports cached input

- **GIVEN** an active goal attempt reports input, cached-input, and output
  counters
- **WHEN** the goal accounts that attempt
- **THEN** charged tokens equal uncached input plus output
- **AND** cached input is not charged again

#### Scenario: Terminal event is observed twice

- **GIVEN** one attributed attempt/turn usage boundary is duplicated or replayed
- **WHEN** the goal component reconciles it again
- **THEN** total goal usage remains unchanged
- **AND** persisted accounting identity prevents double charge

### Requirement: Active elapsed time excludes idle and downtime

The goal harness SHALL derive elapsed time only while an active goal owns a
serving turn in the current process. Idle time, stopped time, time before
activation, and process downtime MUST NOT increase the stored duration.

#### Scenario: Active goal remains dormant between processes

- **GIVEN** an active goal was persisted when its host stopped
- **WHEN** wall-clock time passes before another host resumes it
- **THEN** stored active elapsed time does not advance
- **AND** a new monotonic serving baseline begins only with later work

### Requirement: Token budgets stop at observed boundaries

An optional token budget SHALL be evaluated against actual charged usage at
safe reported boundaries. Usage at or above budget MUST produce
`budget_limited` before another automatic turn; the contract MUST expose actual
overshoot and MUST NOT describe the budget as a pre-spend hard cap.

#### Scenario: One response overshoots budget

- **GIVEN** a goal is below budget before a provider request
- **WHEN** reported usage crosses the budget after the response
- **THEN** actual usage is persisted and status becomes budget-limited
- **AND** no later continuation starts under that budget

#### Scenario: Required usage evidence is missing

- **GIVEN** an active goal has an explicit token budget
- **WHEN** a completed attributable boundary lacks trustworthy required
  counters
- **THEN** status becomes blocked with `accounting_unavailable`
- **AND** the runtime neither guesses remaining budget nor continues

### Requirement: Terminal outcomes finalize goal accounting once

The harness SHALL finalize in-flight attributable token/time deltas once before
committing complete, blocked, paused, usage-limited, budget-limited, or
unrecoverable-error state. A more specific already-committed terminal goal
state MUST win over a later generic error.

#### Scenario: User interrupts active goal work

- **GIVEN** a goal-owned turn is serving
- **WHEN** user pause interrupts the turn
- **THEN** its final observed token/time deltas commit once before paused state
- **AND** no completion race double-counts or restarts work

#### Scenario: Provider account limit occurs

- **GIVEN** an active goal turn reaches an external usage limit
- **WHEN** the terminal limit is committed
- **THEN** status becomes usage-limited with structured evidence
- **AND** no automatic retry or continuation starts
