## ADDED Requirements

### Requirement: Journal hot-path API evolution is declared per phase

P1/P3 optional policy and validated-view transports SHALL retain default
fallbacks for existing implementations. P4's new GC
request/report fields, required storage collection capability and successful
partial-call behavior MUST be released as an explicitly coordinated pre-1.0
breaking change, superseding extraction's source-compatibility promise only for
those listed surfaces in the new release.

#### Scenario: Existing policy or journal does not opt into a fast path

- **WHEN** it is rebuilt with P1/P3's defaulted methods
- **THEN** it retains complete projection/reader semantics without implementing the optional capability
- **AND** only integrations forwarding the capability claim its performance benefit

#### Scenario: A host constructs GC DTOs or implements storage

- **WHEN** it adopts P4
- **THEN** its Rust request/report construction and storage/wrapper implementation are migrated explicitly
- **AND** it loops on reported continuation, refreshing epoch state and charging all calls
- **AND** unknown/unsupported bounded-storage capability retains data rather than running unbounded work

#### Scenario: Older GC JSON is read

- **WHEN** a request/report omits new budget or progress fields
- **THEN** serde defaults preserve the old fields and use documented bounded request defaults or legacy-unknown report progress
- **AND** deserialization additivity is not represented as Rust source compatibility

### Requirement: Journal performance release preserves formats and consumer gates

The hot-path release SHALL retain canonical journal schema 4, materialized
checkpoint schema 3, import/replay/retirement semantics and every existing
conformance case. It MUST document adapter-specific metadata layout changes,
all listed API breaks and immutable consumer release prerequisites, and pass
supported consumer compatibility suites; runtime benchmark acceptance SHALL
NOT substitute for Forge's real D10 storage/adoption gate.

#### Scenario: Existing persisted sessions are opened

- **WHEN** the optimized runtime reads or continues a v0.9.0 session
- **THEN** it validates the existing schema-4 graph and schema-3 checkpoint without implicit object-format conversion
- **AND** protected state and policy-projected ordinary state retain their respective equality contracts

#### Scenario: Consumer adoption is evaluated

- **WHEN** the runtime phase benchmarks pass but Forge D10 still misses turn, import, write or physical disk thresholds
- **THEN** Forge adoption remains blocked by those unchanged checks
- **AND** no benchmark-only storage model, budget increase or deferred collection slice is presented as a production pass

#### Scenario: A supported consumer cannot compile the new collection contract

- **WHEN** Smith, Nyx or Open Forge compatibility suites fail against the candidate
- **THEN** release/pinning waits for documented coordinated consumer updates
- **AND** the release records breaking surfaces and source provenance in its changelog
