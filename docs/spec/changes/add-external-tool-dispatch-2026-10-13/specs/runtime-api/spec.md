## ADDED Requirements

### Requirement: External tool bridge on the turn request

`ExternalTurnRequest` SHALL carry an `ExternalToolBridge` behind the
`external-agent` feature. The bridge SHALL expose the tool specifications
offered for the turn and an asynchronous call that takes a tool name and
JSON arguments and returns the tool's result block or a runtime error.
Dropping the future a call returned SHALL abandon that call. The bridge
SHALL be cheap to clone and usable from any task.

A disabled bridge, which lists nothing and refuses every call, SHALL be
constructible without a runtime, so a backend can be exercised in isolation.

The builder SHALL accept the tool names a host does not offer to an external
backend. Supplying them without a backend MUST be harmless.

#### Scenario: A backend lists and calls

- **GIVEN** a runtime built with a backend and two authorized tools, one of
  them excluded by the host
- **WHEN** the backend reads the bridge on its turn request
- **THEN** it lists the other tool's specification
- **AND** calling it returns that tool's result

#### Scenario: A disabled bridge

- **WHEN** a backend is given a disabled bridge
- **THEN** it lists no tools
- **AND** every call returns an error without dispatch

#### Scenario: Exclusions without a backend

- **GIVEN** a runtime built with excluded names and no backend
- **WHEN** a turn runs
- **THEN** the provider loop offers its tools exactly as before
