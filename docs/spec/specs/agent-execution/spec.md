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
