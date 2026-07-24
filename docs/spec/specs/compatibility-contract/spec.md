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
