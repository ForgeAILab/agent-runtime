## ADDED Requirements

### Requirement: External capabilities configuration

The runtime API SHALL let a host attach `ExternalCapabilities` to a session
that uses an external agent backend, and SHALL deliver them unchanged on each
`ExternalTurnRequest`. The default value MUST be empty, so hosts and backends
that do not use injection observe no behaviour change.

#### Scenario: Default is empty

- **GIVEN** a session configured with an external backend and no capabilities
- **WHEN** a turn runs
- **THEN** the backend receives an empty `ExternalCapabilities`

#### Scenario: Invalid definition is rejected before the turn

- **GIVEN** a skill bundle whose directory lacks `SKILL.md`, or an MCP server
  name that is empty or collides with the reserved bridge name `runtime`
- **WHEN** the host attaches the capabilities
- **THEN** configuration fails with a config error and no turn starts
