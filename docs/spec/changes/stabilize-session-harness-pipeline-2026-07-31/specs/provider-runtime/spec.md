## ADDED Requirements

### Requirement: Explicit speculative output lifecycle

Normalized provider streaming SHALL expose request- and attempt-attributed text
and reasoning deltas plus exactly one committed or discarded output terminal
for every started attempt. Retry policy MUST resolve the prior attempt's output
before starting the next attempt.

#### Scenario: Attempt is discarded
- **GIVEN** an attempt emitted visible and reasoning deltas
- **WHEN** it ends in a retryable provider error
- **THEN** the runtime emits an output-discarded terminal for that attempt
- **AND** no consumer must infer rollback from an unstructured error

### Requirement: Successful-attempt continuation only

Tool calls, reasoning continuation, finish state, and assistant history SHALL
be assembled only from the committed successful attempt. Failed-attempt
fragments MUST NOT be sent back to the provider on a continuation request.

#### Scenario: Tool call fragments precede a retry
- **GIVEN** a failed attempt streamed partial text and tool-call fragments
- **WHEN** a later attempt produces a valid tool call
- **THEN** only the later attempt's assembled message enters canonical history
- **AND** the next provider request contains no failed-attempt fragments
