## ADDED Requirements

### Requirement: Session capability catalog
Session handles SHALL expose a read-only typed catalog of sealed abilities with Active, Available, and Denied states.

#### Scenario: A host renders restricted capabilities
- **WHEN** a host renders restricted capabilities
- **THEN** The host can distinguish denied entries without exposing them to agent discovery.
