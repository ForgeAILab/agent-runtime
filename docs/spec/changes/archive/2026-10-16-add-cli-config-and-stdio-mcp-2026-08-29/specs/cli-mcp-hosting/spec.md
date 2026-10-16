## ADDED Requirements

### Requirement: Stdio MCP launch requires explicit per-run server consent

Loading a server definition SHALL NOT execute it. The `run` command MUST start
a configured stdio server only when the caller explicitly names it in a
per-run server-consent argument. Before spawn, the host SHALL validate the
complete definition, resolve the executable and working directory, and build a
minimal explicit environment. Remote MCP transports MUST be rejected by the
version-1 CLI schema.

#### Scenario: Configured server has no consent flag

- **GIVEN** an explicit file defines an MCP server
- **WHEN** the caller runs a turn without naming that server in a consent flag
- **THEN** no process is started for the server
- **AND** none of its tools is registered

#### Scenario: Selected local server is resolved

- **GIVEN** the caller consents to a configured stdio server
- **WHEN** launch preparation succeeds
- **THEN** the server identity uses the resolved executable, ordered arguments,
  resolved working directory, and explicit environment names
- **AND** no remote HTTP transport is compiled or selected for it

### Requirement: MCP tools require config and per-run allowlists

An MCP tool SHALL be registered only when its server is consented for the run,
its remote name is present in that server's configured `allow_tools`, and the
caller supplies an exact per-run `<server>/<tool>` approval. Unknown,
duplicate, unselected, or non-allowlisted approvals MUST fail before process
spawn. The registered tool MUST traverse the ordinary prepare, authorize,
approve, invoke, and outcome path with the MCP package's conservative authority
floor.

#### Scenario: File attempts to approve its own tool

- **GIVEN** a file defines a server and includes a tool in `allow_tools`
- **WHEN** the caller supplies server consent but no exact tool approval
- **THEN** that tool is not exposed to the provider
- **AND** the file alone does not authorize an invocation

#### Scenario: Exact tool is approved

- **GIVEN** the server is consented and one tool appears in both the configured
  and per-run allowlists
- **WHEN** the model invokes its namespaced tool name
- **THEN** the runtime applies an authoritative host check and exact approval
  policy before invoking the MCP adapter
- **AND** server-authored annotations cannot remove external write, endpoint
  network, or data-egress authority from an unreviewed tool

### Requirement: Stdio children receive no ambient environment

The shared MCP stdio launch boundary SHALL clear the host process's environment
and add only variables explicitly present in the resolved
`McpServerConfig`. Transport and server debug representations MUST redact all
environment and header values while retaining safe names useful for diagnosis.

#### Scenario: Host contains an unrelated secret

- **GIVEN** the CLI process environment contains a secret variable not mapped
  by the selected server's `env_from`
- **WHEN** the stdio child starts
- **THEN** the child cannot observe that variable
- **AND** explicitly mapped variables remain available to it

#### Scenario: Transport is debug-formatted

- **GIVEN** a resolved transport contains secret environment or header values
- **WHEN** it is formatted for a diagnostic or test failure
- **THEN** safe variable/header names may appear
- **AND** none of their values appears

### Requirement: Static MCP inspection has no launch authority

The CLI SHALL provide a static inspection command that renders redaction-safe
server command metadata, environment mappings, configured tool allowlists, and
bounds. Inspection MUST NOT resolve secret values, spawn the command, connect a
socket, or list live server tools.

#### Scenario: User inspects a server definition

- **GIVEN** a valid config containing a local server
- **WHEN** the user runs `agent-runtime mcp inspect`
- **THEN** the output is sufficient to identify the configured process and
  requested environment names
- **AND** no executable or protocol operation occurs

### Requirement: MCP lifecycle preserves command output and cleanup contracts

Selected server connections SHALL remain alive through the accepted turn and
receive bounded shutdown after normal completion, failure, or interruption. A
required server startup/binding failure SHALL fail the run; an optional server
failure SHALL emit a safe stderr diagnostic and contribute no tools. MCP
diagnostics MUST NOT contaminate text or JSONL stdout.

#### Scenario: Required server cannot initialize

- **GIVEN** a selected required server exceeds its startup deadline
- **WHEN** run preparation fails
- **THEN** already-opened MCP connections are shut down within a bound
- **AND** the command exits as a runtime/setup failure without starting the
  provider turn

#### Scenario: Optional server cannot initialize

- **GIVEN** a selected optional server fails initialization while another
  selected server succeeds
- **WHEN** the turn runs
- **THEN** the failing server contributes no tools and a safe diagnostic is
  written to stderr
- **AND** stdout retains the selected text or JSONL contract

#### Scenario: User interrupts an MCP-enabled turn

- **GIVEN** an MCP-enabled turn is active
- **WHEN** the process receives Ctrl-C
- **THEN** the accepted turn and in-flight tool work are interrupted before
  session shutdown
- **AND** every retained MCP connection then receives bounded cleanup before
  exit `130`
