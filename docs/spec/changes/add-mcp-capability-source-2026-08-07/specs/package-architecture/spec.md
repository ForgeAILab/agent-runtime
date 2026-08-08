## ADDED Requirements

### Requirement: Isolated Model Context Protocol client package

Model Context Protocol client behavior SHALL live in its own package beside the
runtime facade rather than inside the registry, ability, core, or facade
packages. No existing package may depend on it, and its transport dependencies
MUST be feature-gated so a host that does not enable a transport does not
acquire that transport's dependency graph. Tokio features required only by a
transport MUST be declared by this package and MUST NOT be promoted to the
workspace default feature set.

#### Scenario: Host omits the MCP package

- **GIVEN** a host depends on the runtime facade with default features
- **WHEN** its dependency graph is resolved
- **THEN** no protocol client, child-process, or HTTP client dependency is
  present
- **AND** the workspace Tokio feature set is unchanged

#### Scenario: Host enables only the local transport

- **GIVEN** a host enables the MCP package with its default transport
- **WHEN** its dependency graph is resolved
- **THEN** the child-process transport is present
- **AND** no HTTP client dependency is present

### Requirement: Protocol client reuses the shared capability and tool contracts

The MCP package SHALL express servers and their tools through the existing
registry, ability, and tool contracts rather than a parallel contract. Remote
tools MUST implement the shared tool trait so they traverse the same
preparation, authorization, approval, and invocation pipeline as native tools,
and MUST NOT introduce a separate execution path.

#### Scenario: Remote tool is invoked

- **GIVEN** an activated remote tool and an approval policy requiring approval
  for its permission set
- **WHEN** the model calls it
- **THEN** the call is prepared, authorized, and approved through the same
  pipeline as a native tool
- **AND** its outcome is committed through the same tool-outcome path

### Requirement: Protocol conformance runs without a process or network

The MCP package's conformance suite SHALL exercise connection, listing,
invocation, and failure handling against an in-process server fixture. Tests
requiring a real child process or network endpoint MUST be a separate opt-in
target excluded from the default test run.

#### Scenario: Default test run is hermetic

- **GIVEN** a machine with no MCP server installed and no network access
- **WHEN** the package's default test suite runs
- **THEN** connection, listing, invocation, and failure scenarios all execute
- **AND** no child process is spawned and no socket is opened
