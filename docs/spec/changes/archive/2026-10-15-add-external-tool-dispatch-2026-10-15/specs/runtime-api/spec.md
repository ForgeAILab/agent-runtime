## ADDED Requirements

### Requirement: External tool dispatch handle on the turn request

`ExternalTurnRequest` SHALL carry an `ExternalToolDispatch` in a `tools`
field behind the `external-agent` feature. The handle SHALL expose the tool
specifications offered for the turn and an asynchronous call that takes a
tool name and JSON arguments and returns the tool's result block or a runtime
error. Dropping the future a call returned SHALL abandon that call. The
handle SHALL be cheap to clone and usable from any task.

The handle is independent of the runtime MCP bridge endpoint. The existing
`bridge: Option<ExternalToolBridge>` field, its `external-agent-bridge`
feature, and the `ExternalCapabilities::runtime_tools` switch SHALL keep
their current meaning; a turn may carry both.

A disabled handle, which lists nothing and refuses every call, SHALL be
constructible without a runtime, so a backend can be exercised in isolation.

The builder SHALL accept the tool names a host does not offer to an external
backend through the handle. Supplying them without a backend MUST be
harmless.

#### Scenario: A backend lists and calls

- **GIVEN** a runtime built with a backend and two authorized tools, one of
  them excluded by the host
- **WHEN** the backend reads the handle on its turn request
- **THEN** it lists the other tool's specification
- **AND** calling it returns that tool's result

#### Scenario: A disabled handle

- **WHEN** a backend is given a disabled handle
- **THEN** it lists no tools
- **AND** every call returns an error without dispatch

#### Scenario: Exclusions without a backend

- **GIVEN** a runtime built with excluded names and no backend
- **WHEN** a turn runs
- **THEN** the provider loop offers its tools exactly as before

#### Scenario: The MCP bridge is unchanged

- **GIVEN** a session whose capabilities enable `runtime_tools` under the
  `external-agent-bridge` feature
- **WHEN** an external turn starts
- **THEN** the request carries both the MCP bridge endpoint and the dispatch
  handle
- **AND** the MCP bridge lists, dispatches, and records calls exactly as it
  did before this change
