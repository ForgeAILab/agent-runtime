## ADDED Requirements

### Requirement: Completed turns are durably persisted

When a session store is configured, the runtime SHALL persist canonical
history, usage, identity, and all ordered manifests after every completed turn,
not only during orderly session shutdown.

#### Scenario: Process exits after a completed turn
- **GIVEN** a turn reached its terminal event
- **WHEN** the process exits before explicit session shutdown
- **THEN** a resumed session retains that turn and every earlier manifest
- **AND** the snapshot does not regress to its pre-turn state

### Requirement: Protected checkpoints are distinct from audit journals

Exact resumable turn state SHALL be stored through a protected checkpoint
contract with a journal/checkpoint watermark. Redacted observability journals
MUST NOT be treated as sufficient to reconstruct raw pending arguments,
sensitive content, or completed side effects.

#### Scenario: Approval is pending at restart
- **GIVEN** an exact prepared action was checkpointed while awaiting approval
- **WHEN** the host restarts and resumes the session
- **THEN** it can present that same preparation fingerprint for a decision
- **AND** does not reconstruct arguments from a redacted event record

### Requirement: Boundary recovery is idempotent

Every checkpointed transition SHALL carry enough identity and fingerprints to
resume without repeating committed provider calls, user answers, approvals, or
tool side effects. A revision mismatch MUST fail explicitly or require a
labeled non-equivalent recovery policy.

#### Scenario: Tool result was committed before a crash
- **GIVEN** a tool result and its transition watermark were persisted
- **WHEN** the process resumes the turn
- **THEN** the runtime reuses the committed result
- **AND** does not invoke the tool again
