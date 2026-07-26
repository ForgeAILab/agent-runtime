## MODIFIED Requirements

### Requirement: Declared toolchain and license

Production packages SHALL declare Rust 1.86 or newer as their minimum
supported version and SHALL be distributed under the MIT license with
retained upstream copyright notices. A package MAY declare a minimum Rust
version higher than 1.86 only when it is explicitly designated an optional
isolation-backend package that is absent from default dependency graphs (for
example, `agent-runtime-sandbox-wasm`). CI MUST verify both baselines
explicitly: the 1.86 baseline for every production package other than a
designated optional isolation-backend package, and each such package's own
declared higher baseline. The project MUST NOT pin an unsupported or
unmaintained isolation-backend engine solely to preserve the 1.86 baseline.

#### Scenario: Minimum-version verification

- **GIVEN** the declared minimum Rust toolchain
- **WHEN** CI builds all production packages other than designated optional
  isolation-backend packages and tests their public API
- **THEN** the build succeeds without relying on a newer compiler

#### Scenario: Base consumer remains on Rust 1.86

- **GIVEN** a consumer does not enable an optional isolation-backend package
- **WHEN** it builds the default runtime dependency graph on Rust 1.86
- **THEN** the existing production packages continue to build
- **AND** Cargo does not resolve or compile the higher-MSRV isolation-backend
  package

#### Scenario: Optional isolation-backend package declares a higher MSRV

- **GIVEN** a package is explicitly designated an optional isolation-backend
  package excluded from default dependency graphs
- **WHEN** it declares a minimum Rust version above 1.86 for its maintained
  security engine
- **THEN** compatibility CI builds and verifies it separately against its own
  declared toolchain, including sandbox security/advisory and consumer
  conformance gates
- **AND** its higher baseline does not change the 1.86 baseline required of
  every other production package
