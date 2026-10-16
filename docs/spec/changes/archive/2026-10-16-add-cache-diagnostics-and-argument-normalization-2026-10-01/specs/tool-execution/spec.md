## ADDED Requirements

### Requirement: Hosts may normalize arguments before schema validation

Tool SHALL offer a pure deterministic normalization hook whose default returns
the original arguments. For each newly assembled call or approval edit, the
executor MUST normalize once before full validation against the frozen
advertised schema, then perform existing preparation and all authority checks.
Normalization MUST NOT grant permissions, widen advertisements, or repair
streams already rejected as unrepresentable by existing assembly checks.

#### Scenario: Host opts into an envelope transformation

- **GIVEN** a tool advertises a strict canonical object schema and its host supplies an envelope normalizer
- **WHEN** representable wrapped arguments reach the executor
- **THEN** normalized arguments are validated against that unchanged canonical schema before preparation
- **AND** workspace, descriptor permissions, authorization, approval, and scheduling still govern invocation

#### Scenario: Existing tool supplies no hook

- **WHEN** an existing Tool or LegacyTool receives arguments that fail its advertised schema
- **THEN** identity normalization preserves the rejection and existing tool-error result behavior
- **AND** no automatic provider-specific unwrapping is performed

#### Scenario: Hook fails or produces invalid output

- **WHEN** normalization returns an error or output that fails schema validation
- **THEN** preparation and invocation do not run
- **AND** the executor does not retry using raw arguments

#### Scenario: Edited approval arguments change the action

- **WHEN** an approval edit supplies a new argument value
- **THEN** the edit is normalized, validated, prepared, and authorized again
- **AND** prior grants and approval eligibility cannot authorize the new fingerprint

#### Scenario: Exact prepared action resumes

- **WHEN** a protected checkpoint resumes an already prepared action
- **THEN** the runtime uses its exact normalized arguments and fingerprint
- **AND** it does not reapply a potentially changed normalization hook
