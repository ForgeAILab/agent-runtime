## ADDED Requirements

### Requirement: One-shot calls on a provider or an external agent

The runtime SHALL provide a one-shot call that returns one bounded,
tool-free answer from either a provider model or an external agent backend
through the same request, answer, and error types. The caller SHALL supply
instructions and input and SHALL own the usage accumulator. The call SHALL
offer no tools, SHALL refuse a provider tool call, SHALL require
non-whitespace text, SHALL report whether the answer stopped or was
truncated, and SHALL bound collected text by an optional character limit
while still reading usage and the finish. Cancellation, a deadline, and
dropping the call's future SHALL each end the call. Dropping it SHALL cancel
the work it started, and usage reported before that point SHALL remain in
the caller's accumulator. External input SHALL be a single user message.

#### Scenario: Provider answer

- **GIVEN** a provider executor whose stream returns text, usage, and a
  `Stop` finish
- **WHEN** a host runs a one-shot call with instructions and one user
  message
- **THEN** the provider request carries a system message with the
  instructions, the user message, no tools, and a `None` tool choice
- **AND** the answer is the text with finish `Stopped`
- **AND** the caller's usage holds the reported counts

#### Scenario: Same call on an external agent

- **GIVEN** an external executor whose backend streams text, usage, and
  `Completed`
- **WHEN** the host runs the same request
- **THEN** the backend's turn request carries the instructions, the input,
  no resume, no bridge, and a disabled tool handle
- **AND** the answer and usage come back as for a provider

#### Scenario: Provider tries a tool

- **WHEN** a provider stream emits a tool call during a one-shot call
- **THEN** the call fails with the tool-call error

#### Scenario: Length finish

- **WHEN** a provider stream finishes with `Length`
- **THEN** the answer's finish is `Truncated`

#### Scenario: Dropped call stops the agent

- **GIVEN** an external one-shot call whose backend is still running and has
  reported usage
- **WHEN** the host drops the call's future
- **THEN** the backend's turn cancellation is cancelled
- **AND** the host's usage accumulator keeps the reported usage

#### Scenario: Deadline

- **WHEN** the deadline passes before a terminal event
- **THEN** the call ends with the timed-out error and cancels its work

#### Scenario: Empty answer

- **WHEN** the call ends normally with no non-whitespace text
- **THEN** it fails with the empty-answer error

#### Scenario: Multi-message input to an external agent

- **WHEN** a host passes more than one input message to an external executor
- **THEN** the call fails with the unsupported-input error before the
  backend runs

### Requirement: Per-turn instructions on the external turn request

`ExternalTurnRequest` SHALL carry optional instructions that a backend adds
to its own system instructions for that turn. It SHALL be constructible
through a constructor that defaults every optional part, and it SHALL be
non-exhaustive so later fields do not break consumers. Turns the runtime
drives SHALL pass no instructions. The bundled CLI backends SHALL place
request instructions after any instructions configured on the backend.

#### Scenario: Constructor defaults

- **WHEN** a consumer builds a request with `new(input, turn, cancel)`
- **THEN** it has no resume, empty capabilities, no bridge, a disabled tool
  handle, and no instructions

#### Scenario: Driven turns are unchanged

- **WHEN** the runtime drives an external turn
- **THEN** the request carries no instructions

#### Scenario: Claude Code receives instructions

- **GIVEN** a Claude Code backend configured with instructions `A`
- **WHEN** it runs a request with instructions `B`
- **THEN** its system-prompt append contains `A` followed by `B`
