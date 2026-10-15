## ADDED Requirements

### Requirement: Chat Completions carries tool-result images

The OpenAI Chat Completions adapter SHALL accept image parts inside tool
results. For each run of consecutive tool messages, each result's text SHALL
go in its `tool` message. The run's images SHALL go, in order, in one `user`
message placed right after the run, each preceded by a text part naming its
`tool_call_id`. A run with no images SHALL serialize exactly as before.

#### Scenario: Screenshot in a tool result

- **GIVEN** a tool message whose result holds text and one PNG image
- **WHEN** the request is serialized
- **THEN** the `tool` message holds the text
- **AND** the next message is a `user` message holding the call id label and
  the image

#### Scenario: Text-only results are unchanged

- **GIVEN** a request whose tool results are text only
- **WHEN** it is serialized
- **THEN** the wire body is byte-identical to the previous serializer's output
