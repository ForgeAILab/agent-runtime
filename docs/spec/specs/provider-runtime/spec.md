# provider-runtime Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Capability-driven provider contract

The provider interface SHALL describe model-dependent streaming, tool,
reasoning, structured-output, usage, cache, authentication, and continuation
capabilities. Unsupported behavior MUST fail before network I/O or follow an
explicit configured downgrade that emits an event.

#### Scenario: Unsupported reasoning request

- **GIVEN** a selected model does not support requested reasoning controls
- **WHEN** the runtime validates the provider request
- **THEN** it fails before network I/O unless an explicit downgrade is enabled
- **AND** an enabled downgrade is observable in the runtime event stream

### Requirement: Normalized streaming events

Providers SHALL emit typed events for text, reasoning, tool-call deltas, finish
state, errors, usage, and cache observations while allowing bounded redacted
vendor metadata. The runtime MUST validate complete tool calls before exposing
them for execution.

#### Scenario: Tool arguments arrive in fragments

- **GIVEN** a provider streams one tool call across multiple fragments
- **WHEN** the stream completes the call
- **THEN** the runtime assembles and validates one typed tool request
- **AND** malformed or incomplete arguments produce a structured provider error

### Requirement: Attempt-visible retries

Every provider attempt SHALL retain request identity, attempt identity,
retryability, timing, finish state, and available usage. Retry wrappers MUST NOT
replace or hide failed-attempt accounting.

#### Scenario: Second attempt succeeds

- **GIVEN** the first request attempt consumes tokens and fails retryably
- **WHEN** a later attempt succeeds
- **THEN** both attempts remain visible to usage and event consumers

### Requirement: Initial conformance adapters

The first release SHALL include a deterministic fake adapter and a configurable
OpenAI-compatible adapter that pass the same streaming, cancellation, tool,
error, and usage conformance suite.

#### Scenario: Compare fake and production adapter contracts

- **GIVEN** equivalent recorded response fixtures
- **WHEN** each adapter runs the provider conformance suite
- **THEN** both produce the required normalized event sequence
