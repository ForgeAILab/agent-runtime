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

### Requirement: Turn-scoped reasoning retention

The driver SHALL retain streamed reasoning as first-class assistant history
content for the duration of the turn that produced it. Consecutive reasoning
deltas sharing a redaction flag MUST merge into one part, parts MUST precede
the visible answer and tool calls on the assistant message, and redacted
reasoning MUST keep its flag. When a new user turn starts, the driver SHALL
remove reasoning retained from earlier turns from the canonical history and
MUST drop assistant messages left without content by that removal.

#### Scenario: Reasoning round-trips within a tool-call turn

- **GIVEN** a provider streams reasoning followed by a tool call
- **WHEN** the driver continues the turn after executing the tool
- **THEN** the continuation request's assistant message carries the merged
  reasoning ahead of the tool call

#### Scenario: A new turn sheds prior reasoning

- **GIVEN** a session whose history holds reasoning from a completed turn
- **WHEN** the next user turn starts
- **THEN** the model-facing history contains no reasoning from earlier turns
- **AND** an assistant message that contained only reasoning is removed
  entirely rather than sent empty

### Requirement: Reasoning-only completion signal

The driver SHALL report on turn completion whether any visible text was
streamed during the turn, so hosts can distinguish a reasoning-only
completion from an answered one instead of showing nothing. The signal MUST
be absent from the serialized event for ordinary turns, and journals written
before the field existed MUST read as ordinary turns.

#### Scenario: A silent completion is flagged

- **GIVEN** a turn whose provider stream contained reasoning but no visible
  text
- **WHEN** the turn completes normally
- **THEN** the completion event reports that no visible output was produced

#### Scenario: An answered turn stays wire-stable

- **GIVEN** a turn that streamed visible text
- **WHEN** its completion event is serialized
- **THEN** the event carries no extra field and matches the pre-change wire
  shape

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

### Requirement: Checkpointable direct turn machine

The canonical direct loop SHALL execute as a versioned serializable state
machine with explicit planning, model, pending-action, tool, completion, and
terminal states. Transitions MUST be idempotent and MUST NOT require a general
graph engine.

#### Scenario: Process stops after one tool result
- **GIVEN** a model requested several prepared tool calls
- **AND** one result was committed and checkpointed before process exit
- **WHEN** the turn resumes
- **THEN** the committed result is not executed again
- **AND** the machine continues from the recorded state

### Requirement: Turn interruption is not session cancellation

Every active turn SHALL have its own cancellation handle. Interrupting the
current turn MUST leave the session able to accept a later turn, while terminal
session cancellation MUST propagate permanently to all active and future work.

#### Scenario: User interrupts then sends another prompt
- **GIVEN** one turn is streaming
- **WHEN** the host interrupts that turn
- **THEN** the turn completes as cancelled
- **AND** a later turn on the same session may complete normally

### Requirement: Provider output is attempt scoped

Visible and reasoning deltas SHALL identify their logical request and provider
attempt and remain speculative until an explicit attempt commit. A discarded
attempt MUST NOT contribute canonical assistant history or committed visible
output.

#### Scenario: Retry succeeds after partial output
- **GIVEN** the first attempt emits partial text and fails retryably
- **WHEN** the runtime discards it and a second attempt succeeds
- **THEN** live and replay reducers commit only the second attempt's text
- **AND** usage and failure diagnostics for both attempts remain observable

### Requirement: Attempt-correlated cache evidence

The direct provider loop SHALL attach logical request identity, provider
attempt identity, and exact cache-plan fingerprint to every canonical cache
observation. Retries MUST remain distinct attempts and MUST NOT inherit,
replace, or hide another attempt's observed cache values.

#### Scenario: Retry reports a different cache result

- **GIVEN** a logical request's first attempt reports a zero cache read and
  fails retryably
- **WHEN** its second attempt reports a positive cache read
- **THEN** both observations identify the same request and cache plan
- **AND** each observation identifies its own provider attempt

#### Scenario: Tool continuation uses a new cache plan

- **GIVEN** one turn completes a provider tool call and replans with the tool
  result
- **WHEN** the continuation attempt reports cache usage
- **THEN** its observation identifies the continuation's cache-plan
  fingerprint
- **AND** it is not joined to the earlier provider request by turn identity
  alone

### Requirement: Canonical cache-state projection

The runtime SHALL emit at most one `CacheStateChanged` projection for every
provider attempt that reaches a cache-evidence boundary, after normalized
usage/cache evidence and before the attempt terminal. The projection SHALL use
`unsupported`, `unknown`, `eligible`, `warm_observed`, or
`miss_observed` and SHALL carry the attributed plan, expected read, observed
read/write, derived missed tokens, and expectation confidence. Only an
explicit provider observation MAY establish `warm_observed` or
`miss_observed`.

#### Scenario: Explicit zero proves a miss

- **GIVEN** an attempt has a comparable 105,000-token cache-read expectation
- **AND** the provider explicitly reports zero cache-read tokens
- **WHEN** the runtime resolves cache state
- **THEN** it emits `miss_observed` with 105,000 derived missed tokens
- **AND** the zero remains provider-observed while the difference retains
  derived confidence

#### Scenario: Partial read produces a saturating miss

- **GIVEN** an attempt expects 105,000 reusable tokens
- **AND** the provider reports 80,000 cache-read tokens
- **WHEN** the runtime resolves cache state
- **THEN** it emits `miss_observed` with 25,000 missed tokens
- **AND** it retains the 80,000 provider-reported read unchanged

#### Scenario: Cache field is absent

- **GIVEN** provider caching is supported
- **AND** the completed response supplies no cache observation
- **WHEN** the runtime resolves cache state
- **THEN** the state is `unknown` rather than `miss_observed`
- **AND** no zero read or missed-token value is fabricated

#### Scenario: First request reports zero

- **GIVEN** a first cache-eligible request has no comparable predecessor
- **AND** the provider explicitly reports a zero cache read
- **WHEN** the runtime resolves cache state
- **THEN** the state remains `eligible`
- **AND** no cache miss is emitted for establishing the initial entry

#### Scenario: Attempt fails before a provider response

- **GIVEN** transport fails before any response, usage, or cache evidence
- **WHEN** the attempt terminates
- **THEN** no `CacheStateChanged` event is emitted
- **AND** consumers do not mistake transport absence for a cache miss

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

### Requirement: Admission finalizes interrupted turns

The runtime SHALL finalize an interrupted turn as an explicit `Failed`
terminal, through the ordinary terminal publication path, before accepting
newly admitted work (a user turn, an attributed internal turn, or a local
action) that finds the session's latest protected checkpoint non-terminal and
owned by a turn that is no longer running. The finalization
MUST NOT replay the interrupted turn's indeterminate provider or tool
outcome, MUST attribute an error event and a `TurnCompleted { Failed }`
event to the interrupted turn before the new work starts, and MUST continue
the checkpoint watermark so the new turn's acceptance checkpoint succeeds.
Live turns (including checkpoint-resume recovery that is still serving) and
cache-operation checkpoints MUST NOT be finalized by this path; admission
over those keeps failing closed.

#### Scenario: Terminal publication fails mid-session
- **GIVEN** one turn's terminal checkpoint write fails non-durably
- **AND** the turn ends with its checkpoint short of a terminal state
- **WHEN** the host submits a new turn on the same session
- **THEN** the interrupted turn is finalized as an explicit `Failed` terminal
- **AND** an attributed error and `TurnCompleted { Failed }` precede the new
  turn's start
- **AND** the new turn is accepted and completes normally

#### Scenario: Dormant checkpoint under the defer policy
- **GIVEN** a restart loads a non-terminal checkpoint under the defer
  recovery policy without resuming it
- **WHEN** the host submits new work on that session
- **THEN** the dormant turn is finalized as failed without provider I/O
- **AND** the new work is admitted over the reconciled checkpoint chain

#### Scenario: Live turns are never finalized
- **GIVEN** checkpoint-resume recovery is actively serving a non-terminal
  checkpoint
- **WHEN** another admission is attempted
- **THEN** reconciliation skips the live turn
- **AND** acceptance fails closed with the existing conflict

### Requirement: External capability injection

An external turn SHALL carry the session's `ExternalCapabilities` -- skill
bundles, MCP server definitions, a tool policy, and an optional runtime tool
bridge -- to its backend. A backend that supports injection MUST make exactly
those capabilities available to the CLI for that turn without writing to the
user's global CLI configuration or workspace, and MUST re-apply them on every
turn, including a resumed one.

#### Scenario: Skill and MCP server reach the CLI

- **GIVEN** a session whose capabilities include skill `lab-greeting` and stdio
  MCP server `lab` exposing `lab_nonce`
- **WHEN** a turn asks for the lab greeting and a `lab_nonce` call
- **THEN** the CLI loads the skill and calls `lab_nonce` on the injected server
- **AND** the backend reports the call as `ToolInvoked`/`ToolCompleted`
  observation events

#### Scenario: Injection survives resume

- **GIVEN** a turn that reported external session `S`
- **WHEN** the next turn resumes `S`
- **THEN** the same skills and MCP servers are available again

#### Scenario: User MCP servers are excluded

- **GIVEN** the user's global CLI config defines MCP server `personal`
- **WHEN** an injected turn runs on a backend that supports strict selection
- **THEN** `personal` is not started for that turn

### Requirement: Headless tool policy never waits

A backend SHALL run the CLI in a non-prompting mode. A tool the policy allows
MUST run without a prompt; any other tool call MUST be refused by the CLI and
reported as `ToolCompleted { ok: false }` rather than blocking the turn.

#### Scenario: Unallowed MCP tool is refused

- **GIVEN** an injected tool that is not in the tool policy's allowlist
- **WHEN** the model calls it
- **THEN** the turn continues without waiting for input
- **AND** the backend emits `ToolCompleted` with `ok: false`

### Requirement: Runtime tool bridge

When `ExternalCapabilities` enables the bridge, the runtime SHALL expose the
session's runtime-owned tools to the CLI as an MCP server for the lifetime of
the turn. A bridge call is runtime-dispatched: it MUST traverse the ordinary
authorize, approve, and invoke pipeline and be recorded in the canonical tool
path. The bridge endpoint and its credential MUST be scoped to one turn and torn
down at the turn's terminal event or cancellation.

#### Scenario: Bridge call is approved by the runtime

- **GIVEN** a runtime tool that requires approval, exposed through the bridge
- **WHEN** the CLI calls it
- **THEN** the host receives the ordinary approval request
- **AND** on approval the tool runs and its result returns to the CLI and is
  recorded canonically

#### Scenario: Bridge closes with the turn

- **WHEN** an external turn completes, fails, or is cancelled
- **THEN** later connections with that turn's bridge credential are refused

#### Scenario: CLI-native tools stay observation-only

- **WHEN** the CLI runs one of its own tools or a third-party injected MCP tool
- **THEN** the runtime records only observation events and requests no approval
