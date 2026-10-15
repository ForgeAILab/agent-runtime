## ADDED Requirements

### Requirement: External continuation identity survives a redaction-safe store

The stored external continuation identity SHALL be marked redaction-safe so
that a host store retaining only redaction-safe extension state still offers
it to the backend after a restart.

#### Scenario: Continuation through a store that drops sensitive state

- **GIVEN** a backend reported a continuation identity during a session's turn
- **WHEN** the session is saved through a store that drops sensitive extension
  state and restored from the saved snapshot in a new runtime
- **THEN** the next turn offers that identity to the backend

### Requirement: External turns dispatch host tools through the runtime

The runtime SHALL give an external agent backend a turn-scoped handle that
lists the tool specifications the host offers for the turn and executes one
call through the runtime's own tool path. A call made through the handle
SHALL pass the same schema validation, preparation, exact-resource
authorization, approval, workspace enforcement, cancellation, deadline, and
output bound as a model-requested tool call, and SHALL emit the ordinary tool
events attributed to the external turn. No provider request is made for it.

The handle SHALL list the session's authorized tools, never one the session
scope denies, less the names the host excluded, in registration order. A
call SHALL resolve against that same view: it MUST NOT require the tool to
be in an activation epoch, MUST NOT count against the activation budget, and
MUST be refused without dispatch when it names a tool the handle does not
list. Calls made through one handle SHALL run one at a time in arrival
order.

A refusal, denial, or tool failure SHALL be returned to the caller as that
call's result or error and MUST NOT fail the turn.

A call the CLI makes through the runtime MCP bridge is not a dispatched call
in the sense of this change: it keeps the listing, concurrency, and
history behavior it already has. Both paths MAY be used during one turn.

#### Scenario: A dispatched call during an external turn

- **GIVEN** an external turn in flight
- **WHEN** the backend submits a call through the handle
- **THEN** the runtime emits the ordinary tool request and result events
  attributed to that turn
- **AND** the caller receives the tool's result
- **AND** no provider attempt is recorded

#### Scenario: A tool outside the activation epoch

- **GIVEN** an authorized tool that is not in the session's activation epoch
- **WHEN** the backend calls it through the handle
- **THEN** the call is dispatched

#### Scenario: A denied or excluded tool

- **GIVEN** a tool the session scope denies and a tool the host excluded
- **WHEN** the handle lists its specifications
- **THEN** neither is listed
- **AND** a call naming either is refused without dispatch

#### Scenario: A dispatched call needs approval

- **GIVEN** an approval policy that asks before the called tool runs
- **WHEN** the backend submits the call
- **THEN** the runtime awaits the host's ApprovalPolicy decision before invoking
  the tool, exactly as for a model-requested call
- **AND** no InteractionRequested or InteractionResolved event is emitted for
  the approval decision
- **AND** a denial is returned to the caller as a tool error while the turn
  continues

#### Scenario: The MCP bridge alongside the handle

- **GIVEN** an external turn with the runtime MCP bridge enabled
- **WHEN** the CLI calls a tool through the MCP bridge and the backend
  calls another through the handle
- **THEN** the MCP-bridged call behaves as it did before this change and
  adds nothing to canonical history
- **AND** the handle call is committed to canonical history

#### Scenario: Two calls arrive together

- **GIVEN** two calls submitted before either finished
- **WHEN** the runtime runs them
- **THEN** the second starts only after the first has committed its result

### Requirement: Dispatched calls are committed to canonical history in order

The runtime SHALL commit a dispatched call and its result to canonical history
as a tool call and tool result, in the order they occurred relative to the
text the backend reported: text reported before the call is committed as the
assistant message that carries the call, and text reported after it is
committed after the result. Tools the backend ran itself remain observations
and never enter canonical history. A turn that fails or is cancelled SHALL
keep the dispatched rounds that had already committed.

#### Scenario: Text, a call, then more text

- **GIVEN** the backend reported text, then made a dispatched call, then
  reported more text and completed
- **WHEN** canonical history is inspected
- **THEN** it holds the first text with the call, the tool result, and the
  remaining text, in that order

#### Scenario: The turn fails after a dispatched call

- **GIVEN** a dispatched call that committed its result
- **WHEN** the backend then reports a terminal failure
- **THEN** the committed call and result remain in canonical history
- **AND** no assistant message is added for the failed remainder

### Requirement: An abandoned dispatched call is settled by how far it got

The runtime SHALL settle a dispatched call by its phase when the caller stops
waiting for it while the turn continues, by dropping the call or because
the backend's stream ended:

- not yet started (queued, or not yet authorized): it never runs and leaves
  nothing in canonical history;
- waiting for approval: the pending ApprovalPolicy decision future is dropped
  or cancelled, without synthesizing interaction events; the tool never runs
  and a later decision has no effect;
- waiting for a broker interaction: the broker request is closed and its real
  InteractionResolved event records Cancelled; the tool's effect never happens
  and a later answer has no effect;
- executing: it runs to completion and its result commits to canonical
  history, and nothing is returned to the caller.

Cancelling the turn is different from abandoning a call. When the turn is
cancelled, by the user, by its deadline, or by session shutdown, a dispatched
call in any phase SHALL be cancelled exactly as a model-requested call in
that phase is: a queued call never runs, a pending decision or interaction
is withdrawn, and an executing tool receives the turn's cancellation and is
settled the way the provider loop settles a cancelled tool. The turn MUST
NOT wait for an executing tool that ignores cancellation any longer than a
provider turn would.

The handle MUST refuse a call submitted after its turn has reached a
terminal event.

#### Scenario: Abandoned while queued

- **GIVEN** a call queued behind another
- **WHEN** the caller abandons it
- **THEN** the tool is never invoked and history holds no trace of it

#### Scenario: Abandoned while awaiting approval

- **GIVEN** a call waiting for approval
- **WHEN** the caller abandons it and the approval is then granted
- **THEN** the tool is never invoked

#### Scenario: Abandoned while executing

- **GIVEN** a call whose tool is running
- **WHEN** the caller abandons it
- **THEN** the tool finishes and its result is committed to history

#### Scenario: The turn is cancelled while a dispatched tool executes

- **GIVEN** a dispatched call whose tool is running
- **WHEN** the user cancels the turn
- **THEN** the tool observes the cancellation as it would in a provider
  turn
- **AND** the turn finishes as cancelled without waiting for the tool to
  run to completion

#### Scenario: A call after the turn ended

- **GIVEN** an external turn that has published its terminal event
- **WHEN** a call is submitted through its handle
- **THEN** the handle refuses it and nothing is dispatched

### Requirement: Tools can read the backend's own tool observations

The runtime SHALL keep a bounded log of the tool invocations and outcomes an
external backend reported during the turn in flight, in order relative to
the dispatched calls, and SHALL make it readable to a tool through the session
view. The log is cleared when the turn reaches a terminal event. An entry
MUST be distinguishable from a runtime-dispatched tool call. The owned snapshot
SHALL include dispatch-position markers and a dropped-entry count. The log SHALL
retain at most the newest 200 entries and 256 KiB of serialized detail, dropping
oldest entries until both limits hold. Each detail SHALL be cut to at most
8 KiB of JSON with an explicit truncation marker. The log MUST NOT be written
to canonical history, checkpoints, or the journal; existing backend observation
event projection is unchanged.

#### Scenario: A tool reads the log mid-turn

- **GIVEN** the backend reported two tools it ran, then made a dispatched call
- **WHEN** the called tool reads the session view
- **THEN** it sees both observations in order, marked as the backend's own

#### Scenario: The log is bounded

- **GIVEN** a backend that reports more observations than the bound
- **WHEN** a tool reads the log
- **THEN** it holds the most recent entries and says how many were dropped

### Requirement: An interrupted external turn is not replayed

When a process ends during an external turn, recovery SHALL finalize that
turn as interrupted. It MUST NOT invoke the backend again for it and MUST
NOT run a dispatched call again, whatever phase the call had reached; this includes a call the CLI had
made through the runtime MCP bridge. Dispatched rounds that had committed
remain in canonical history.

#### Scenario: Process ends during a dispatched call

- **GIVEN** a dispatched call in flight when the process exits
- **WHEN** the session is resumed
- **THEN** the turn is finalized as interrupted
- **AND** the call is not executed again
- **AND** the backend is not invoked for that turn

### Requirement: Backend diagnostics do not end an external turn

The runtime SHALL accept `Diagnostic { message }` as non-terminal host
information from an external backend and project it onto the existing turn-attributed
`RuntimeEvent::Error` with `ErrorKind::Conflict`, the same surfaced recoverable
condition used by interrupted-checkpoint reconciliation, and SHALL bound the
message to 1,000 characters. A diagnostic MUST NOT change the turn's finish or
enter canonical history, and MUST NOT introduce a new runtime event variant.

#### Scenario: A diagnostic precedes normal completion

- **GIVEN** an external backend reporting a 5,000-character diagnostic
- **WHEN** the backend subsequently completes normally
- **THEN** one Conflict error is attributed to the turn with 1,000 characters
- **AND** the turn completes normally and history contains no diagnostic

### Requirement: A requested tool call always settles before its turn ends

The runtime SHALL commit a result and emit `ToolCallCompleted` for every
tool call whose `ToolCallRequested` it emitted, before the turn's terminal
event, including when the turn is cancelled or reaches its deadline while
the tool runs or while output processors run. A result that is already the
cancellation result MUST bypass the remaining output processors. A real
result that finished before processing was interrupted SHALL be committed
raw. Exactly one result is committed per call. This applies to provider
turns, dispatched calls on external turns, internal turns, and local actions.
It complements the existing closure of a call left unanswered when a tool
step fails, which keeps its behavior.

#### Scenario: Cancelled while a tool runs, with an output processor

- **GIVEN** a runtime with an output processor installed
- **WHEN** the turn is cancelled while a tool executes
- **THEN** history holds "cancelled while tool was running" as that call's
  error result and `ToolCallCompleted` precedes the cancelled terminal event
- **AND** the output processor is not invoked on that result

#### Scenario: Cancelled while a real result is processed

- **GIVEN** a tool that finished and an output processor still running
- **WHEN** the turn is cancelled or its deadline passes
- **THEN** the raw result is committed once and completion is emitted
