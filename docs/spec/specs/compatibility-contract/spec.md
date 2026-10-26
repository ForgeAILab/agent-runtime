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

### Requirement: Bounded manifests preserve readers and require retention gates

Current readers SHALL accept legacy snapshots with absent or unbounded
manifests and legacy checkpoints containing manifest lists. Existing public
manifest vectors, store traits, and checkpoint schema/transition versions MUST
remain supported. All consumer gates MUST prove equivalent execution recovery
and explicitly accept recent-window retention; readable serde alone MUST NOT
be presented as old-binary execution or lifetime archival compatibility.

#### Scenario: Legacy snapshot omitted manifests

- **WHEN** a current reader loads the snapshot
- **THEN** manifests default to empty and existing history, usage, identities, and extensions remain readable
- **AND** no missing manifest is reconstructed or inferred as a missing turn

#### Scenario: Smith reads snapshot manifests directly

- **WHEN** Smith's fixture inspects snapshot.manifests after an over-window run
- **THEN** its existing Vec surface still compiles and yields the same ordered suffix as recent_manifests
- **AND** the compatible release is blocked if its required archival/retention behavior is not satisfied

#### Scenario: Legacy binary cannot overlay a new checkpoint

- **WHEN** downgrade testing shows an older binary rejects a manifest-free checkpoint paired with retained ordinary diagnostics
- **THEN** the release documents that execution downgrade limitation
- **AND** supported consumers pin the gated candidate revision without relaxing checkpoint validation

### Requirement: Failure metadata is backward-readable and consumer-gated

Failure classification and provider timing/recovery additions SHALL default
when missing and omit unknown values on serialization. Current readers MUST
read legacy errors, and unknown or malformed diagnostic evidence MUST NOT make
an enclosing error record unreadable or grant retry authority. All supported
consumer suites MUST pass; any required Rust literal or destructuring updates
MUST be coordinated before a compatible release is published.

#### Scenario: Legacy error journal is loaded

- **WHEN** a current reader loads an Error event without the new fields
- **THEN** kind, message, retryability, and metadata remain readable
- **AND** class is Unclassified and timing/recovery evidence is absent

#### Scenario: Future or malformed diagnostic evidence is lenient

- **WHEN** a reader encounters an unknown reason tag, a class payload it cannot decode, an unknown closed recovery value, or a timing value that is not a non-negative integer in otherwise valid error JSON
- **THEN** it uses Unclassified for an undecodable class, `Unknown` for a future stage/component name, and absence for undecodable optional timing/recovery evidence
- **AND** the enclosing RuntimeError, Error or ChildFailed event, and child failure record remain readable
- **AND** separately valid evidence fields remain readable
- **AND** it does not invent a retry schedule

#### Scenario: A consumer uses a Rust struct literal

- **WHEN** adding failure metadata makes a supported consumer's RuntimeError literal fail compilation
- **THEN** the candidate is blocked until that consumer's documented source update passes its gate
- **AND** JSON compatibility alone is insufficient to publish a compatible release

### Requirement: Journal migration is backward-readable and coordinated

The first released U9 version SHALL read valid v3 checkpoints and v3-era unversioned snapshots for its entire release lifetime, including unfinished turns. Native version-4 import SHALL preserve exact execution/recovery semantics and keep source-compatible existing store methods. A coordinated pre-1.0 persisted-contract release MUST pass Smith, Nyx and Open Forge gates against an immutable landed tag or exact revision; readable JSON alone MUST NOT imply old-binary execution compatibility.

#### Scenario: Host adopts no native journal

- **WHEN** a host upgrades to the release containing only the journal contracts and readers
- **THEN** every checkpoint and snapshot it writes is byte-identical to what the previous release wrote, with checkpoint schema 3
- **AND** the previous release can read them, so the host can roll back without an export

#### Scenario: Stored v3 session is mid-turn

- **WHEN** the new runtime imports a supported v3 CallingModel, pending approval or committed outcome boundary
- **THEN** it publishes an exact reference boundary under an import writer fence without provider/tool I/O
- **AND** crash retry resumes the authoritative legacy or native boundary without repeating committed work

#### Scenario: Consumer has no durable store

- **WHEN** Nyx starts ephemeral history with journals configured
- **THEN** the seeded history and subscribe-before-send behavior remain
- **AND** no journal discovery, load, write, claim or durable collection occurs

#### Scenario: One actual consumer store rejects schema 4

- **WHEN** a release candidate passes neutral runtime tests but a supported product store rejects its schema
- **THEN** compatible release eligibility is blocked pending coordinated store migration
- **AND** consumers do not pin a moving branch or unlanded local override

### Requirement: Engine extraction is source-compatible

Extracting the journal engine SHALL NOT change or remove any public item of `SessionJournal`, `SessionStore`, `CheckpointStore`, the journal DTOs, `JournalReader`, `JournalProjectionPolicy`, `IdentityJournalProjection`, `JournalCommitFailurePoint` or `InMemorySessionJournal` (`protected`, `ordinary`, `fail_next_commit_at`, `revoke_writer`, `enable_collection`, `root_epoch`, `authorize_retirement`, `pin_transfer`, `retain_root`, `release_pin`, `stage_leaves`, `compact`, `object_count`, `object_bytes`). Testkit fixtures and `assert_*` functions SHALL keep their signatures and behavior. New items MUST be additive and listed in the CHANGELOG.

#### Scenario: A consumer built against the previous release recompiles

- **WHEN** a host or test that uses `InMemorySessionJournal` and the testkit journal fixtures is rebuilt against this change
- **THEN** it compiles without source edits
- **AND** its journal tests observe the same results
