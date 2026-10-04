## ADDED Requirements

### Requirement: Revision Tolerant LCM Tunables
Persisted LCM identity MUST match (schema, authorized timeline/binding, store,
classifier and guard). Tunable policy, sizer, model and algorithm changes SHALL
rebuild derived state from authorized active store nodes instead of rejecting
valid state. Canonical history, provenance and pending durable operations MUST
remain validated; protected state and authority checks MUST remain fail-closed.

#### Scenario: Policy bump on live session
- **WHEN** a live persisted session is resumed with a new policy revision
- **THEN** derived state MUST rebuild with current tunables without losing committed nodes or failing decode.

#### Scenario: Identity mismatch
- **WHEN** the binding, schema, store, classifier or guard identity differs
- **THEN** restore MUST reject the state before provider or summary work.
