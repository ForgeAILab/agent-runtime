## MODIFIED Requirements

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
  and may receive bounded model correction without executing the failed call

## ADDED Requirements

### Requirement: Malformed tool arguments receive bounded model correction

The canonical provider loop SHALL distinguish model-generated invalid JSON from
structural and transport failures. On a completed or output-truncated response it SHALL admit a
correction within the existing provider-attempt, input-budget, cancellation, and
deadline limits. The model SHALL receive bounded feedback asking for complete
JSON and smaller calls. Feedback MUST NOT quote raw arguments or untrusted names.

#### Scenario: Corrected tool call succeeds

- **GIVEN** a completed response contains malformed tool JSON
- **WHEN** capacity remains for correction
- **THEN** the failed output is discarded and its usage remains recorded
- **AND** the next attempt includes a bounded model-visible correction
- **AND** only the successful replacement enters history and executes

#### Scenario: Repeated syntax failures exhaust attempts

- **WHEN** each admitted attempt produces invalid tool JSON
- **THEN** correction stops at the configured total-attempt ceiling
- **AND** no malformed or sibling call executes or produces an orphan result
- **AND** feedback is replaced rather than accumulated

#### Scenario: Correction is not admitted

- **WHEN** input size, cancellation, the deadline, or attempt capacity prevents
  another request
- **THEN** no correction request starts
- **AND** the turn reports the corresponding budget failure, terminal limit, or cancellation

#### Scenario: Failed response has structural errors

- **WHEN** a response omits a tool name or reports a malformed provider stream
- **THEN** it remains a terminal provider failure without argument correction

#### Scenario: Provider explicitly truncates a response

- **GIVEN** an output-length finish accompanies incomplete tool JSON
- **WHEN** the runtime classifies the response
- **THEN** it admits bounded correction asking the model for smaller calls
- **AND** no failed call executes or enters history
- **AND** exhausted correction reports the output limit instead of a parsing error

#### Scenario: Correction changes the planned request

- **WHEN** a correction request is admitted
- **THEN** it is sized with the frozen sizer and input cap
- **AND** it carries no stale explicit cache identity or boundary
- **AND** the original immutable cache baseline is retired
