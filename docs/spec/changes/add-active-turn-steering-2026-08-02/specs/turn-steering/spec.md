## ADDED Requirements

### Requirement: Steering admission targets one eligible serving turn

The session facade SHALL expose bounded real-user steering for an eligible
provider-backed serving turn with an optional expected `TurnId` and a stable
steer receipt. Admission MUST return structured non-acceptance for no active
turn, turn mismatch, non-steerable work, limits, or shutdown without consuming
the input into another turn or exposing its raw content in diagnostics.

#### Scenario: User steers an active provider request

- **GIVEN** an eligible turn is serving and a provider response is in flight
- **WHEN** the host submits bounded non-empty input for the expected turn
- **THEN** the runtime returns a stable receipt attributed to that turn
- **AND** does not mutate or cancel the in-flight provider request

#### Scenario: Expected turn is stale

- **GIVEN** the host names an expected turn that is no longer serving
- **WHEN** steering admission checks the current lifecycle
- **THEN** it returns a structured mismatch or no-active-turn result
- **AND** no history, provider request, queued whole turn, or steer disposition
  is created

#### Scenario: Pending steer bound is reached

- **GIVEN** the serving turn has reached a configured pending or cumulative
  steer bound
- **WHEN** another steer is attempted
- **THEN** admission returns a structured limit result
- **AND** already accepted entries retain their exact FIFO order

### Requirement: Steers commit only at safe boundaries

Accepted steers SHALL enter canonical history as ordered user-role input only
after the current provider response or tool operation reaches a protected safe
boundary. The runtime MUST build any later provider request through the normal
context planner and MUST NOT append steering input to an already-built request.

#### Scenario: Steer arrives during streaming

- **GIVEN** one provider request is streaming a response
- **WHEN** a steer is accepted before that response finishes
- **THEN** the response commits under its original request context
- **AND** the steer enters history before the next provider request
- **AND** the next request remains attributed to the same turn

#### Scenario: Steer arrives during tool execution

- **GIVEN** the serving turn is executing an authorized tool
- **WHEN** a steer is accepted
- **THEN** the canonical tool result commits before the steer
- **AND** the steer participates in the normal next-step context plan

#### Scenario: Several steers share a boundary

- **GIVEN** several steers are accepted before one safe boundary
- **WHEN** the driver drains the turn mailbox
- **THEN** each input enters history once in admission order
- **AND** every entry receives one matching committed disposition

### Requirement: Pending steering extends the same turn

The direct agent loop SHALL extend the same turn when accepted steering input
is pending at the atomic terminal boundary. When a provider response would
otherwise complete, it commits that response, commits the pending user input,
and performs another provider step under the same `TurnId`. Normal context,
usage, authority, deadline, and limit policy MUST continue to apply.

#### Scenario: Final answer races a steer

- **GIVEN** a provider returns a complete answer while a steer is accepted
- **WHEN** the driver evaluates terminal completion
- **THEN** it continues the same turn with the steer when admission won the
  atomic boundary
- **AND** it completes without that steer when terminal close won and admission
  returned non-acceptance

#### Scenario: Context cannot fit the steer continuation

- **GIVEN** a committed steer causes the next planned request to exceed a hard
  context limit
- **WHEN** normal planning validates the continuation
- **THEN** the turn emits the existing structured budget/terminal evidence
- **AND** does not bypass or weaken context enforcement

### Requirement: Accepted steers have exactly one in-process disposition

The serving turn SHALL serialize steer admission with mailbox close. Every
accepted steer MUST produce exactly one privacy-safe committed or discarded
disposition before a graceful terminal event, while input rejected by admission
MUST produce neither disposition.

#### Scenario: User interrupts before commit

- **GIVEN** one or more steers are accepted but not committed
- **WHEN** the serving turn is interrupted
- **THEN** the runtime closes the mailbox and emits one discarded disposition
  for each uncommitted receipt before turn completion
- **AND** no discarded input enters canonical history

#### Scenario: Admission races cancellation

- **GIVEN** steering admission and cancellation occur concurrently
- **WHEN** the lifecycle fence resolves
- **THEN** the steer is either accepted then discarded exactly once or rejected
  to the caller
- **AND** it is never silently lost, committed twice, or carried into another
  turn

#### Scenario: Process exits before commitment

- **GIVEN** a steer was accepted only in process and no committed disposition
  reached protected state
- **WHEN** a compatible process restores the session
- **THEN** recovery does not claim that steer entered canonical history
- **AND** the runtime does not invent a committed or resent user message

### Requirement: Steering preserves source and authority boundaries

Steering an eligible ordinary or internal provider-backed turn SHALL preserve
that turn's source attribution, goal generation when present, usage accounting,
tool permissions, approvals, workspace policy, and cancellation scope. Generic
injected content and queued whole turns MUST remain semantically distinct from
real-user steering.

#### Scenario: User steers a goal-owned internal turn

- **GIVEN** an eligible internal goal turn is serving
- **WHEN** real-user input is accepted as a steer
- **THEN** the input becomes ordinary user-role history for the next step
- **AND** the turn retains its original goal source and accounting
- **AND** the steer grants no additional tool authority

#### Scenario: Generic child result arrives with a steer

- **GIVEN** generic injected child content and real-user steering are pending at
  one safe boundary
- **WHEN** the driver commits boundary input
- **THEN** it applies the documented deterministic cross-kind ordering
- **AND** only the real-user entry receives a steer disposition
