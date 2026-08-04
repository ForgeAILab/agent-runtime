## MODIFIED Requirements

### Requirement: Initial conformance adapters

The crate SHALL ship at least an OpenAI-compatible chat-completions adapter
and a deterministic fake provider, both passing a shared conformance suite
covering streaming, tool calls, usage accounting, error mapping,
cancellation, and reasoning normalization (including acceptance of
continuation requests that carry reasoning history back). The OpenAI adapter
SHALL serialize non-redacted reasoning history parts as `reasoning_content`
on assistant wire messages so OpenAI-compatible thinking models receive
their reasoning back during tool-call continuations, and MUST NOT serialize
redacted reasoning to the wire in any form. Canonical reasoning parts SHALL
carry an optional provider-issued signature that adapters for signing
providers round-trip verbatim; a signature MUST be dropped whenever the text
it signed is altered.

#### Scenario: Reasoning echoes on the continuation wire message

- **GIVEN** an assistant history message holding non-redacted reasoning and a
  tool call
- **WHEN** the OpenAI adapter renders the continuation request
- **THEN** the assistant wire message carries the reasoning text as
  `reasoning_content` alongside its unchanged content and tool calls

#### Scenario: Redacted reasoning never reaches the wire

- **GIVEN** an assistant history message whose only reasoning is redacted
- **WHEN** the OpenAI adapter renders the request
- **THEN** the wire message has no `reasoning_content` field and the redacted
  text appears nowhere in the serialized request
