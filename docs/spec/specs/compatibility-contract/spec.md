# compatibility-contract Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Versioned consumer dependency

Released consumers SHALL depend on a tagged semantic version or an exact Git
revision. Default-branch manifests MUST NOT require a sibling relative path to
the shared repository.

#### Scenario: Build consumer in isolation

- **GIVEN** only one consumer repository is checked out
- **WHEN** its normal release build resolves dependencies
- **THEN** it obtains the shared runtime from the declared versioned source

### Requirement: Local override workflow

The shared repository SHALL document an uncommitted Cargo path override for
developers changing the runtime and a consumer together. Removing the override
MUST restore the versioned dependency without source changes.

#### Scenario: Developer tests a local runtime change

- **GIVEN** the runtime and one consumer are sibling checkouts
- **WHEN** the developer enables the documented local override
- **THEN** the consumer builds against the local runtime
- **AND** cleanup restores the pinned release dependency

### Requirement: Cross-consumer compatibility gate

A shared-runtime release candidate SHALL run supported contract suites for
Smith, Nyx, and Open Forge. A failing consumer blocks a compatible release
unless the version is explicitly breaking and coordinated consumer changes are
documented.

#### Scenario: One consumer fixture fails

- **GIVEN** the shared workspace tests pass
- **AND** a supported consumer contract suite fails
- **WHEN** release eligibility is evaluated
- **THEN** a compatible release tag is rejected

### Requirement: Declared toolchain and license

Production packages SHALL declare Rust 1.86 or newer as their minimum supported
version and SHALL be distributed under the MIT license with retained upstream
copyright notices.

#### Scenario: Minimum-version verification

- **GIVEN** the declared minimum Rust toolchain
- **WHEN** CI builds all production packages and tests their public API
- **THEN** the build succeeds without relying on a newer compiler

### Requirement: Cache-event evolution is backward-readable and coordinated

The runtime SHALL deserialize legacy numeric cache observations with missing
attribution after introducing attributed cache observations and
`CacheStateChanged`. It MUST NOT derive missed tokens from a legacy observation
that cannot be joined to a request, attempt, and cache plan.
Because the Rust event shape and variant set change, the release MUST be
documented as a pre-1.0 breaking contract and pass every supported consumer
gate with coordinated updates.

#### Scenario: Legacy journal contains a positive observation

- **GIVEN** a journal written before cache attribution contains numeric
  read/write values and no request, attempt, or plan fields
- **WHEN** the new runtime deserializes it
- **THEN** the legacy read/write evidence remains available
- **AND** no canonical miss or fabricated attribution is produced

#### Scenario: Consumer exhaustively matches runtime events

- **GIVEN** a supported consumer compiles an exhaustive `RuntimeEvent` match
- **WHEN** the candidate runtime adds `CacheStateChanged`
- **THEN** the consumer is updated in the coordinated compatibility change
- **AND** a failing consumer gate blocks the candidate release

### Requirement: Immutable landed Runtime revision for consumers

The coordinated change SHALL provide Smith and every supported consumer an
immutable landed Agent Runtime revision containing the approved cache,
delegation, admission, persistence, event, usage, and conformance contracts.
Consumer manifests MUST NOT depend on a moving branch or an uncommitted
cross-repository state.

#### Scenario: Smith updates its dependency

- **GIVEN** Runtime implementation and compatibility gates have passed
- **WHEN** Smith adopts the shared mechanism
- **THEN** it pins the immutable landed Runtime revision
- **AND** its contract suite exercises the landed public shapes

#### Scenario: Runtime revision is not landed

- **GIVEN** a local Runtime checkout contains the draft API but no immutable
  landed revision
- **WHEN** a consumer landed-revision pin is prepared
- **THEN** the compatibility gate blocks the pin
- **AND** a local path override remains development-only

### Requirement: Cross-consumer gate covers adaptive mechanism contracts

The compatibility gate SHALL run Runtime and supported consumer suites
for new provider capability, cache lifecycle events, synthetic usage,
delegation wait, protected outcome cursor, and conditional admission behavior.
An exhaustive consumer failure MUST block the compatible landed revision.

#### Scenario: Consumer misses a lifecycle event

- **GIVEN** Runtime adds a canonical cache lifecycle event
- **AND** one supported consumer cannot deserialize or project it
- **WHEN** landed-revision eligibility is evaluated
- **THEN** the immutable revision is not accepted as compatible
- **AND** the consumer migration is coordinated before pinning

### Requirement: Rate-limit observation additivity

Adding rate-limit observation SHALL NOT break an existing transport, adapter,
or serialized record. The byte-stream transport method remains required and
unchanged; the header-observing method is defaulted. New stream and runtime
event variants and the new provider error kind SHALL serialize as additive
tagged variants, and no existing field SHALL be removed or repurposed.

#### Scenario: An existing transport keeps compiling

- **GIVEN** a transport implementing only the byte-stream method
- **WHEN** the crate is rebuilt against this change
- **THEN** it compiles unchanged
- **AND** attempts it serves produce no snapshot rather than failing

#### Scenario: An older record still deserializes

- **GIVEN** a serialized provider error written before this change
- **WHEN** it is deserialized after it
- **THEN** it deserializes successfully
- **AND** its reset time reads as absent

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

### Requirement: Retry decision metadata remains backward-readable

Provider retry-decision fields SHALL be optional and omitted when unavailable.
Current readers MUST deserialize legacy finished-attempt events that lack the
fields, and coordinated consumers MUST treat absence as unknown rather than
fabricate an attempt total, delay, or admitted retry.

#### Scenario: Legacy journal omits retry decision fields

- **GIVEN** a journal contains a finished-attempt event written before retry
  decision metadata existed
- **WHEN** a current runtime or coordinated consumer deserializes it
- **THEN** attempt position, configured total, and scheduled delay are absent
- **AND** the existing finish, retryability, and provider error remain readable

#### Scenario: Consumer adopts the new Rust event shape

- **GIVEN** a supported consumer constructs or matches
  `ProviderAttemptFinished`
- **WHEN** the candidate runtime adds the optional fields
- **THEN** the consumer is updated in the coordinated compatibility change
- **AND** a failing consumer gate blocks publication of the compatible revision
