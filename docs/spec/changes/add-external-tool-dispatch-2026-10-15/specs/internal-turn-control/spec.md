## ADDED Requirements

### Requirement: Internal turns on an external session run on the backend

On a session configured with an external agent backend, the runtime SHALL
execute an admitted internal turn on that backend, offering the stored
continuation identity so the backend continues its own conversation, and
MUST NOT call the session's provider for it. Admission, attribution, outcome
consumption, checkpointing, and replay safety SHALL be the ones the internal
turn already has.

The internal input SHALL be given to the backend as the turn's input text,
prefixed with one line stating that it is a delivery from the host and
naming the internal source. It MUST NOT be appended to canonical history as
a user message. The dispatch handle, and the runtime MCP bridge when the
session serves one, are available on an internal turn as on any other.

#### Scenario: Child completion on an external session

- **GIVEN** an idle external session with a ready child outcome
- **WHEN** the child-completion turn is admitted
- **THEN** the backend runs the turn with the outcome text as its input
- **AND** the provider records no attempt
- **AND** the turn keeps its internal attribution

#### Scenario: History after an internal external turn

- **GIVEN** a committed internal turn on an external session
- **WHEN** canonical history is inspected
- **THEN** it contains no user message for the internal input
- **AND** it contains the assistant text the backend reported

#### Scenario: Replay after a committed internal external turn

- **GIVEN** an internal turn on an external session committed before a crash
- **WHEN** the session is resumed and the journal replayed
- **THEN** the backend is not invoked again for that outcome
