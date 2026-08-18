## ADDED Requirements

### Requirement: Admission finalizes interrupted turns

When newly admitted work (a user turn, an attributed internal turn, or a
local action) finds the session's latest protected checkpoint non-terminal
and owned by a turn that is no longer running, the runtime SHALL finalize
that interrupted turn as an explicit `Failed` terminal through the ordinary
terminal publication path before accepting the new work. The finalization
MUST NOT replay the interrupted turn's indeterminate provider or tool
outcome, MUST attribute an error event and a `TurnCompleted { Failed }`
event to the interrupted turn before the new work starts, and MUST continue
the checkpoint watermark so the new turn's acceptance checkpoint succeeds.
Live turns (including checkpoint-resume recovery that is still serving) and
cache-operation checkpoints MUST NOT be finalized by this path; admission
over those keeps failing closed.

#### Scenario: Terminal publication fails mid-session
- **GIVEN** one turn's terminal checkpoint write fails non-durably
- **AND** the turn ends with its checkpoint short of a terminal state
- **WHEN** the host submits a new turn on the same session
- **THEN** the interrupted turn is finalized as an explicit `Failed` terminal
- **AND** an attributed error and `TurnCompleted { Failed }` precede the new
  turn's start
- **AND** the new turn is accepted and completes normally

#### Scenario: Dormant checkpoint under the defer policy
- **GIVEN** a restart loads a non-terminal checkpoint under the defer
  recovery policy without resuming it
- **WHEN** the host submits new work on that session
- **THEN** the dormant turn is finalized as failed without provider I/O
- **AND** the new work is admitted over the reconciled checkpoint chain

#### Scenario: Live turns are never finalized
- **GIVEN** checkpoint-resume recovery is actively serving a non-terminal
  checkpoint
- **WHEN** another admission is attempted
- **THEN** reconciliation skips the live turn
- **AND** acceptance fails closed with the existing conflict
