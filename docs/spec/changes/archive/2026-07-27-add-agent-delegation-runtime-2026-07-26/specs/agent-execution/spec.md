## ADDED Requirements

### Requirement: Safe-boundary content injection

The runtime SHALL accept bounded, host-enqueued content for an active session
and introduce it to the model only at safe provider/tool boundaries, never by
mutating an in-flight provider stream. Queue overflow MUST return a structured
result to the enqueuer, and content marked as a final child result MUST NOT be
dropped or coalesced away.

#### Scenario: Content arrives during streaming

- **GIVEN** the session's provider stream is actively emitting deltas
- **WHEN** a host enqueues content for the model
- **THEN** the in-flight stream is not interrupted or mutated
- **AND** the content is introduced at the next provider/tool boundary

#### Scenario: Queue bound is reached

- **GIVEN** the per-session injection queue is at its configured bound
- **WHEN** a host enqueues additional coalescable content
- **THEN** the runtime returns a structured overflow result
- **AND** any queued final child result is still delivered
