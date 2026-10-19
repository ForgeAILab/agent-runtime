## ADDED Requirements

### Requirement: Transport-vouched rejection diagnostics

When an adapter replaces a transport error's text with its fixed class
message, it SHALL keep two metadata entries a transport vouches for: an
integer `http.status` from 100 to 599, and a textual
`provider.redacted_reason` that the transport has already cleared of every
credential and header value it sent. The adapter MUST bound the reason again,
replacing control characters with spaces and keeping at most 300 characters,
and SHALL append both to the class message, as in
`provider rejected the request (HTTP 404): Model is not deployed or ready`.
Every other metadata entry, `provider.detail` included, and the transport's
own message MUST still be dropped.

#### Scenario: A rejected model is named

- **GIVEN** a transport rejects a request with `http.status` 404 and
  `provider.redacted_reason` `Model is not deployed or ready`
- **WHEN** an OpenAI-compatible, Anthropic, Responses or Gemini adapter
  reports the error
- **THEN** its message is the class message followed by `(HTTP 404): Model is
  not deployed or ready`
- **AND** both entries remain in its metadata

#### Scenario: Unvouched provider text is still dropped

- **GIVEN** a transport error whose message and `provider.detail` repeat a
  credential
- **WHEN** an adapter reports the error
- **THEN** the credential appears in no rendering of the error or its events

#### Scenario: A malformed diagnostic is ignored

- **GIVEN** `http.status` is text or outside 100 to 599, or the reason is
  blank after control characters are removed
- **WHEN** an adapter reports the error
- **THEN** that entry is neither kept nor appended to the message
