## MODIFIED Requirements

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

#### Scenario: Cache maintenance responsibilities have stable owners

- **GIVEN** cache maintenance request construction, operation fingerprints,
  persisted state, dispatch, and result validation are implemented together
- **WHEN** the cache mechanism is decomposed
- **THEN** each responsibility belongs to a cohesive module behind the
  existing `agent_runtime::cache` root
- **AND** cache dispatch ordering, checkpoint semantics, and validation remain
  unchanged

#### Scenario: Session cache and recovery code is separated from turn lifecycle

- **GIVEN** a session implementation combines cache checkpoint coordination,
  interrupted-operation recovery, and turn admission/lifecycle
- **WHEN** the session implementation is decomposed
- **THEN** each responsibility has a cohesive module behind the existing
  `agent_runtime::runtime::session` path
- **AND** session, cache, and recovery events and checkpoint transitions retain
  their existing order and behavior

#### Scenario: Provider cache identity remains on its supported root path

- **GIVEN** provider cache identity values, builder validation, and custom
  deserialization rules form one cohesive type boundary
- **WHEN** those declarations move into a private provider child module
- **THEN** supported consumers continue to resolve the same types through
  `agent_runtime_core::provider`
- **AND** serialized identity forms and validation behavior remain unchanged

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
