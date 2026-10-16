## ADDED Requirements

### Requirement: Opt-in shared HTTP implementation

The provider crate SHALL expose reqwest transport only through the off-by-default
reqwest-transport feature. Its default graph MUST remain networking-free and
build on Rust 1.86. The optional feature compiler floor SHALL be checked and
documented separately if dependencies require newer Rust. CLI SHALL use the
shared PublicHttps mechanism with its existing exact-origin host policy.

#### Scenario: Default embedding

- **WHEN** a host builds provider without optional features on Rust 1.86
- **THEN** reqwest, url, and TLS dependencies are absent from its normal graph.

#### Scenario: Reference CLI migration

- **WHEN** CLI builds and executes provider requests
- **THEN** its transport is the shared implementation bound to PublicHttps
- **AND** the existing restricted-address, DNS, status and body-bound tests pass.
