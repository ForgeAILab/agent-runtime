## ADDED Requirements

### Requirement: MCP-enabled CLI remains an isolated higher-MSRV leaf

The `agent-runtime-cli` package SHALL remain an isolated leaf while depending
directly on `agent-runtime-mcp` with its local stdio transport and declaring
Rust 1.88 to match the protocol SDK. It MUST NOT enable the MCP
HTTP transport, and MUST NOT raise the Rust 1.86 baseline or dependency graph
of the embeddable runtime, registry, core, ability, provider, context, LCM,
observability, or testkit packages.

#### Scenario: Embedder omits the CLI

- **GIVEN** a library host depends on the runtime facade and does not install
  the CLI package
- **WHEN** Cargo resolves and builds its dependency graph at Rust 1.86
- **THEN** neither the CLI nor the protocol SDK is required by that host
- **AND** the existing embeddable package MSRV remains unchanged

#### Scenario: Developer builds the MCP-enabled CLI

- **GIVEN** a developer builds `agent-runtime-cli` with its supported Rust
  toolchain
- **WHEN** Cargo resolves the package's default dependency graph
- **THEN** the stdio MCP transport is present under the CLI's Rust 1.88
  declaration
- **AND** the MCP HTTP client transport is absent

### Requirement: CLI MCP verification remains hermetic by default

The CLI SHALL keep configuration, consent, binding, policy, output, and
lifecycle behavior behind injectable seams so its default tests require no
public provider request, real credential, installed third-party MCP server, or
remote MCP endpoint.

#### Scenario: Default CLI test suite runs offline

- **GIVEN** a machine has no provider credentials, third-party MCP commands, or
  public network access
- **WHEN** the CLI package tests execute
- **THEN** versioning, consent, tool policy, failure, cancellation, output, and
  cleanup behavior are covered with in-memory or local deterministic fixtures
- **AND** no public socket is opened
