## ADDED Requirements

### Requirement: Embeddable runtime facade

The shared repository SHALL expose a host-neutral in-process runtime that can
start and control sessions without a daemon. Hosts MUST be able to inject
provider, tool, approval, workspace, session, secret, event, and clock
implementations through documented Rust contracts.

#### Scenario: Minimal host starts a session

- **GIVEN** a host supplies deterministic fake services
- **WHEN** it builds the runtime and starts a session
- **THEN** the session can execute an agent turn in process
- **AND** no Smith, Nyx, Open Forge, daemon, or UI dependency is required

### Requirement: Versioned commands and events

Runtime commands and canonical events SHALL have explicit schema versions and
structured payloads. Consumers MUST receive the same semantic events for the
same runtime behavior regardless of their presentation layer.

#### Scenario: Two hosts run the same fixture

- **GIVEN** two hosts use identical fake-provider input and runtime policy
- **WHEN** they execute the conformance turn
- **THEN** their canonical event sequences are equivalent
- **AND** differences are limited to declared host presentation metadata

### Requirement: Consumer-neutral dependency boundary

Production shared packages MUST NOT depend on Smith, Nyx, Open Forge, or their
domain types. Consumer integrations SHALL convert product-owned types at the
consumer boundary.

#### Scenario: Build without consumer sources

- **GIVEN** no consumer repository is available
- **WHEN** the shared workspace builds and runs its production tests
- **THEN** every production package succeeds independently

### Requirement: Explicit lifecycle control

The runtime SHALL expose cancellation, event subscription, and bounded shutdown
for every active session. Cancellation MUST propagate to provider attempts and
tool invocations without requiring a process-global singleton.

#### Scenario: Host cancels an active turn

- **GIVEN** a provider stream or tool invocation is active
- **WHEN** the host cancels its session
- **THEN** active work observes cancellation
- **AND** the runtime emits a terminal event before bounded shutdown completes
