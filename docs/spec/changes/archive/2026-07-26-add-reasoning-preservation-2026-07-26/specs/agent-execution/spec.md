## ADDED Requirements

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
