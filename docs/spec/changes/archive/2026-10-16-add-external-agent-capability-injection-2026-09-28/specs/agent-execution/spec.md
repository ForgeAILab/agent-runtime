## ADDED Requirements

### Requirement: External capability injection

An external turn SHALL carry the session's `ExternalCapabilities` -- skill
bundles, MCP server definitions, a tool policy, and an optional runtime tool
bridge -- to its backend. A backend that supports injection MUST make exactly
those capabilities available to the CLI for that turn without writing to the
user's global CLI configuration or workspace, and MUST re-apply them on every
turn, including a resumed one.

#### Scenario: Skill and MCP server reach the CLI

- **GIVEN** a session whose capabilities include skill `lab-greeting` and stdio
  MCP server `lab` exposing `lab_nonce`
- **WHEN** a turn asks for the lab greeting and a `lab_nonce` call
- **THEN** the CLI loads the skill and calls `lab_nonce` on the injected server
- **AND** the backend reports the call as `ToolInvoked`/`ToolCompleted`
  observation events

#### Scenario: Injection survives resume

- **GIVEN** a turn that reported external session `S`
- **WHEN** the next turn resumes `S`
- **THEN** the same skills and MCP servers are available again

#### Scenario: User MCP servers are excluded

- **GIVEN** the user's global CLI config defines MCP server `personal`
- **WHEN** an injected turn runs on a backend that supports strict selection
- **THEN** `personal` is not started for that turn

### Requirement: Headless tool policy never waits

A backend SHALL run the CLI in a non-prompting mode. A tool the policy allows
MUST run without a prompt; any other tool call MUST be refused by the CLI and
reported as `ToolCompleted { ok: false }` rather than blocking the turn.

#### Scenario: Unallowed MCP tool is refused

- **GIVEN** an injected tool that is not in the tool policy's allowlist
- **WHEN** the model calls it
- **THEN** the turn continues without waiting for input
- **AND** the backend emits `ToolCompleted` with `ok: false`

### Requirement: Runtime tool bridge

When `ExternalCapabilities` enables the bridge, the runtime SHALL expose the
session's runtime-owned tools to the CLI as an MCP server for the lifetime of
the turn. A bridge call is runtime-dispatched: it MUST traverse the ordinary
authorize, approve, and invoke pipeline and be recorded in the canonical tool
path. The bridge endpoint and its credential MUST be scoped to one turn and torn
down at the turn's terminal event or cancellation.

#### Scenario: Bridge call is approved by the runtime

- **GIVEN** a runtime tool that requires approval, exposed through the bridge
- **WHEN** the CLI calls it
- **THEN** the host receives the ordinary approval request
- **AND** on approval the tool runs and its result returns to the CLI and is
  recorded canonically

#### Scenario: Bridge closes with the turn

- **WHEN** an external turn completes, fails, or is cancelled
- **THEN** later connections with that turn's bridge credential are refused

#### Scenario: CLI-native tools stay observation-only

- **WHEN** the CLI runs one of its own tools or a third-party injected MCP tool
- **THEN** the runtime records only observation events and requests no approval
