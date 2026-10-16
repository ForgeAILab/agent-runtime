## ADDED Requirements

### Requirement: Cache diagnostics and normalization remain opt-in compatible

New cache diagnostic fields SHALL default to absent when reading legacy
plans/events and SHALL be omitted when unavailable. Existing tools MUST retain
identity normalization without migration. All supported consumer gates MUST
verify the existing event variant set, prepared authority, and legacy serde
behavior, coordinating any field-exact Rust construction updates before a
compatible release.

#### Scenario: Old cache event and plan are read

- **WHEN** a current reader loads a cache event or plan without first_changed_fragment
- **THEN** all existing fields retain their values and the diagnostic is absent
- **AND** no changed fragment is inferred from invalidated token counts alone

#### Scenario: Consumer does not adopt normalization

- **WHEN** an unchanged consumer runs its legacy tool fixture against the candidate
- **THEN** its advertised schema, validation outcome, and authority checks remain unchanged
- **AND** a failing consumer suite blocks publication even if the new optional JSON is readable
