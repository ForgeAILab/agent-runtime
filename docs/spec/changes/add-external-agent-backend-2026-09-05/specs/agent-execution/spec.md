## ADDED Requirements

### Requirement: External agent turn execution

The runtime SHALL support an opt-in `ExternalAgentBackend` that executes one
turn in place of the provider/tool loop. A session configured with a backend
MUST route every turn to it; a session without one MUST behave exactly as
before. The two execution paths MUST NOT interleave within a single turn.

An external turn SHALL keep the ordinary turn identity, admission boundary,
cancellation, event stream, and persistence. The backend owns only what happens
between the turn starting and its terminal event.

#### Scenario: External turn replaces the provider loop

- **GIVEN** a session configured with an external agent backend
- **WHEN** a turn is admitted
- **THEN** the runtime invokes the backend instead of planning a provider
  request
- **AND** no provider attempt, tool dispatch, or context plan is recorded for
  that turn
- **AND** the turn emits the ordinary started and completed events

#### Scenario: Backend reports assistant output

- **GIVEN** a running external turn
- **WHEN** the backend streams assistant text and a terminal completion
- **THEN** the runtime appends one canonical assistant message
- **AND** hosts observe the same text events as a direct turn

#### Scenario: Backend fails

- **GIVEN** a running external turn
- **WHEN** the backend reports a terminal failure
- **THEN** the turn completes with that failure recorded
- **AND** canonical history retains no partial assistant message

#### Scenario: Turn is cancelled

- **GIVEN** a running external turn
- **WHEN** the turn is cancelled
- **THEN** the runtime signals the backend and stops consuming its events
- **AND** shutdown remains bounded

### Requirement: External tool activity is observable, not authorized

An external backend runs its own tools under its own policy. The runtime SHALL
record reported tool activity as observation only, and MUST NOT represent it as
a runtime-dispatched tool call, consult approvals for it, or admit it to the
tool-authority path.

#### Scenario: Backend reports a tool it ran

- **GIVEN** a backend that executed a tool inside its turn
- **WHEN** it reports the invocation and outcome
- **THEN** the runtime emits observation events a host can render
- **AND** no approval is requested and no tool result enters the canonical tool
  path

### Requirement: Advisory external session continuity

The runtime SHALL persist a backend-reported session identity in extension
state and offer it to the backend on the next turn, so a backend that keeps its
own conversation is not forced to replay history the runtime already owns.

Continuation MUST be advisory. A backend that cannot resume the offered
identity MUST be able to report that and continue with a fresh one without
failing the turn.

#### Scenario: Second turn continues the external session

- **GIVEN** a completed external turn that reported a session identity
- **WHEN** the next turn starts
- **THEN** the runtime offers that identity to the backend

#### Scenario: Offered session is gone

- **GIVEN** an offered identity the backend cannot resume
- **WHEN** the backend reports a new identity instead
- **THEN** the turn proceeds
- **AND** the stored identity is replaced

### Requirement: External usage accounting

Usage reported by a backend SHALL be recorded through the ordinary usage path,
preserving any cache breakdown it reports, and attributed to the turn that
produced it.

#### Scenario: Backend reports usage

- **GIVEN** a backend that reports input, output, and cached token counts
- **WHEN** the turn completes
- **THEN** the runtime records one usage entry for that turn
- **AND** hosts read it through the existing usage surface
