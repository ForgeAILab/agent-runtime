## ADDED Requirements

### Requirement: Typed LCM failures across runtime boundaries
RangeOverlap, EntryConflict, TimelineOwned and LcmDivergence MUST retain typed,
redaction-safe evidence through the harness, request driver and RuntimeError.
They MUST NOT become config(string). Protected exact checkpoints and ordinary
redaction policy SHALL retain their existing roles.

#### Scenario: Store conflict at provider admission
- **WHEN** an LCM store reports RangeOverlap or EntryConflict
- **THEN** the host receives the specific typed LCM failure and Conflict kind
- **AND** no provider request occurs

#### Scenario: Durable fork seed
- **WHEN** an ordinary store redacts a Sensitive summary seed
- **THEN** compatible protected state restores its exact value
- **AND** no summary text enters errors or diagnostic manifests
