## ADDED Requirements

### Requirement: Child-completion turns use their own content bound

An internal turn whose source kind is `delegation.child-completion` SHALL be
validated against `MAX_CHILD_COMPLETION_TURN_CHARS`; every other internal
turn SHALL keep `MAX_INTERNAL_TURN_CHARS`. The same bound SHALL apply at
admission, checkpoint creation, and checkpoint or journal restore.

#### Scenario: A delivery larger than the general bound

- **GIVEN** child-completion content longer than 4,096 characters
- **WHEN** the internal turn input is validated
- **THEN** it is accepted
- **AND** the same content under another source kind is rejected
