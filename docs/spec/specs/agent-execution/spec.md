# agent-execution Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Canonical direct agent loop

The runtime SHALL own one provider/tool loop that assembles host-supplied
prompt content and history, streams provider events, validates tool calls,
appends canonical tool results, and repeats until completion, cancellation, or
a configured limit.

#### Scenario: Provider requests a tool

- **GIVEN** a provider emits a valid tool request
- **WHEN** the host policy allows the invocation
- **THEN** the runtime records the request and canonical tool result
- **AND** continues the same turn with the updated history

### Requirement: Consumer-owned prompt policy

The shared runtime MUST NOT hard-code Smith, Nyx, or Open Forge prompts,
commands, workflow rules, or presentation messages. Hosts SHALL supply product
instructions and policy through neutral request and adapter contracts.

#### Scenario: Consumers use different instructions

- **GIVEN** Smith and Nyx construct sessions with different product instructions
- **WHEN** both run the same shared agent loop
- **THEN** each request contains its host-supplied instructions
- **AND** the shared runtime contains no product-name conditional

### Requirement: Deterministic limits and cancellation

The agent loop SHALL enforce configured provider-attempt, tool-step, time, and
output limits and MUST observe session cancellation at provider and tool
boundaries.

#### Scenario: Tool-step limit is reached

- **GIVEN** a provider continues requesting tools
- **WHEN** the configured tool-step limit is reached
- **THEN** the runtime stops further invocations
- **AND** emits a structured terminal event identifying the exhausted limit

### Requirement: Host-observable progress

The agent loop SHALL expose normalized progress through runtime events without
requiring consumers to parse logs or provider-specific wire formats.

#### Scenario: Streaming text reaches a terminal host

- **GIVEN** a provider emits several text deltas
- **WHEN** a host subscribes to the session event stream
- **THEN** it receives ordered normalized text events before turn completion
