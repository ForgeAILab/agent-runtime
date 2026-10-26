## MODIFIED Requirements

### Requirement: Protected checkpoints are distinct from audit journals

Exact resumable turn state SHALL be stored through a protected checkpoint contract with a journal/checkpoint watermark. Native version-4 checkpoints MAY reference exact protected SessionJournal objects instead of embedding another snapshot, provided every referenced execution object is durable and validated before the checkpoint is published. Redacted observability journals and ordinary SessionStore projections MUST NOT be treated as sufficient to reconstruct raw pending arguments, sensitive content, or completed side effects. The protected-state split and all existing boundary, authority, sensitivity and transition checks SHALL remain.

#### Scenario: Approval is pending at restart

- **GIVEN** an exact prepared action was checkpointed while awaiting approval
- **WHEN** the host restarts and resumes the session
- **THEN** it can present that same preparation fingerprint for a decision
- **AND** does not reconstruct arguments from a redacted event record

#### Scenario: Prepared action is stored once by reference

- **GIVEN** a referenced checkpoint publishes an exact protected prepared-action object
- **WHEN** the next transition reuses that action
- **THEN** it references the existing object without another body write
- **AND** confidentiality, idempotency, reauthorization and no-repeat guarantees remain
