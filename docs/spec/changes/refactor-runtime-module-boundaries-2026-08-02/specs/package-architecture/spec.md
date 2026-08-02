## ADDED Requirements

### Requirement: Responsibility-aligned source modules

The runtime workspace SHALL organize oversized production and conformance
modules around cohesive responsibilities while preserving supported module
paths and public contracts through stable roots or re-exports. A source-only
decomposition MUST NOT change runtime semantics, serialized representations,
event ordering, checkpoint transitions, conformance coverage, or dependency
boundaries.

#### Scenario: Oversized runtime module is decomposed

- **GIVEN** a production module contains several independently changing
  lifecycle, provider, execution, persistence, or recovery responsibilities
- **WHEN** the module is decomposed
- **THEN** each extracted module owns a cohesive responsibility with the
  narrowest practical visibility
- **AND** existing callers continue to compile through the same supported path
- **AND** focused and workspace conformance remain behaviorally unchanged

#### Scenario: Exhaustive or security-critical logic remains cohesive

- **GIVEN** a large function centralizes an exhaustive state transition or a
  security-critical prepared-execution pipeline
- **WHEN** surrounding source is reorganized
- **THEN** the exhaustive match or ordered pipeline remains together in one
  responsibility-focused module
- **AND** the refactor does not duplicate, reorder, or weaken its checks

#### Scenario: Test-heavy source is cleaned without fragmenting production

- **GIVEN** a cohesive production module is large mainly because it embeds an
  extensive test suite
- **WHEN** maintainability cleanup is applied
- **THEN** the tests move into private responsibility-focused test modules
- **AND** the production implementation remains centralized
- **AND** the test and public conformance inventories do not shrink
