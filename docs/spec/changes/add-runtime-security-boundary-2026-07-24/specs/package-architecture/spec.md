## MODIFIED Requirements

### Requirement: Dependency-light registry and ability packages

The workspace SHALL provide a registry kernel whose default feature graph is
std-only and an ability package that depends only on that kernel by default.
Executable native-tool integration MAY enable an explicit bridge to core, but
descriptor registration and search MUST NOT require the runtime, provider,
Tokio, HTTP, or storage implementations. The security vocabulary a descriptor
carries — permission names, trust class, artifact kind, and isolation-profile
identifiers — SHALL be defined as a dependency-free set of plain data types
(no async runtime, no I/O, no serde requirement beyond the kernel's existing
optional `serde` feature) placed in the registry kernel itself so that both
`agent-runtime-registry` and `agent-runtime-ability` can reference it under
their default, Tokio-free feature graphs. `agent-runtime-core` MUST reuse
these same kernel-defined vocabulary types for its own security contracts
rather than defining a second, divergent canonical vocabulary that ability
and registry cannot reach without the `tool`/core bridge.

#### Scenario: Manifest-only extension package

- **GIVEN** an extension author only publishes ability descriptors and skill
  metadata
- **WHEN** the extension compiles with default features
- **THEN** it depends on the registry and ability contracts without the agent
  loop or provider adapters

#### Scenario: Descriptor carries security vocabulary without Tokio

- **GIVEN** a descriptor declares a permission upper bound, trust class,
  artifact kind, and required isolation-profile identifier
- **WHEN** `agent-runtime-ability` compiles and searches that descriptor with
  default features
- **THEN** it resolves the permission/trust/artifact/profile vocabulary types
  from the registry kernel
- **AND** it does not pull `agent-runtime-core`, Tokio, or any async-runtime
  dependency to do so

#### Scenario: Dependency-boundary CI check stays green

- **GIVEN** the security vocabulary types have been added to descriptors
- **WHEN** the dependency-boundary CI check runs `cargo tree` for
  `agent-runtime-registry` and `agent-runtime-ability` built with default
  features
- **THEN** neither package's resolved dependency tree gains Tokio, an HTTP
  client, or a storage implementation
- **AND** the resolved crate count for each package stays at its current
  minimal baseline rather than growing to absorb the security vocabulary

## ADDED Requirements

### Requirement: Optional reference WASM isolation package

The workspace SHALL keep engine-neutral security, isolation-profile, and backend
contracts in core and enforcement in the runtime facade while placing the
maintained WebAssembly engine/WASI reference implementation in an optional
`agent-runtime-sandbox-wasm` package. Default runtime, registry, ability,
provider, context, and observability dependency graphs MUST NOT include the
sandbox engine.

#### Scenario: Host does not execute untrusted tools

- **GIVEN** a host depends on the runtime facade with default features
- **WHEN** its dependency graph and MSRV are evaluated
- **THEN** no Wasmtime/WASI implementation is present
- **AND** authorization checks for native/provider behavior remain enabled

#### Scenario: Host enables WASM tools

- **GIVEN** a host opts into the sandbox package or facade feature
- **WHEN** it registers an untrusted WASM component
- **THEN** the runtime uses the sandbox package through the neutral contract
- **AND** no other package gains a direct dependency on the engine

### Requirement: Pluggable isolation backend packages

The runtime SHALL permit clients to provide alternative `IsolationBackend`
implementations without changing core or depending on the reference WASM
package. Every backend MUST declare stable identity, version, supported artifact
kinds, exact isolation-profile revisions, and configuration fingerprint, and
MUST pass the shared conformance suite for each claimed profile before
production approval. Host policy MUST explicitly approve the backend/profile
pair; unsupported, unapproved, nonconformant, or downgraded profiles MUST deny.

#### Scenario: Client registers a container backend

- **GIVEN** a client package implements the neutral backend contract for a
  container artifact and claims `UntrustedToolV1`
- **AND** it passes conformance and host policy approves its exact identity and
  profile revision
- **WHEN** a matching untrusted tool is activated
- **THEN** the runtime can invoke it without a dependency on Wasmtime/WASI
- **AND** the same authorization, brokers, grants, events, and no-fallback rules
  apply

#### Scenario: Backend attempts a profile downgrade

- **GIVEN** an untrusted tool requires `UntrustedToolV1`
- **AND** a registered backend supports only a weaker or unknown profile
- **WHEN** activation is attempted
- **THEN** activation is denied with a structured profile-mismatch result
- **AND** the runtime does not substitute native execution or the weaker profile

### Requirement: Security checks cannot be feature-disabled

Feature flags MUST NOT remove authorization calls, install permissive fallbacks,
remove required checks, or change default-deny composition semantics. They MAY
omit optional isolation, detector, check, or broker implementations; an
unavailable required implementation SHALL deny only the affected privileged
capability with a structured reason.

#### Scenario: Required isolation backend is unavailable

- **GIVEN** the runtime is built without any approved backend for the required
  isolation profile
- **WHEN** an untrusted executable tool is selected
- **THEN** activation is denied as isolation-unavailable
- **AND** the tool is not executed natively
