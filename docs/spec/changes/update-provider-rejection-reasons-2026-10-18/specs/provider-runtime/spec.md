## ADDED Requirements

### Requirement: Provider rejections carry a checked reason

Every adapter SHALL keep the reason a provider stated for a failed request
when that reason repeats no credential value the adapter put on the request:
from the documented error object of a transport failure (`status`, or a
string `code`, and `message`) and from an in-band stream error. The adapter
MUST check the reason against the request it sent, not against the host
transport's own scrubbing: every header value outside a fixed public list,
each whitespace-separated token of eight or more characters inside such a
value, and the URL's password and query values. A reason that contains any
of them MUST be dropped whole. A kept reason SHALL be trimmed, have control
characters normalized, be bounded to 300 characters, be appended to the
fixed class message as `"{message}: {reason}"`, and be kept as metadata
`provider.detail`. Metadata `http.status` SHALL survive as an unsigned
integer, and all other transport metadata and text MUST still be dropped.
Authentication failures SHALL carry a checked reason too, on the error their
credential-rejection classification builds. A converted provider
`BadRequest` SHALL be `ErrorKind::Provider`; `Unsupported` stays
`ErrorKind::Config`, and failure classes are unchanged.

#### Scenario: Gemini rejects a request with a reason

- **GIVEN** Gemini answers a request with an `error` frame whose message is
  "Request contains an invalid argument." and whose code is `invalid_request`
- **WHEN** the adapter reports the failure
- **THEN** the error reads `Gemini provider rejected the request:
  invalid_request: Request contains an invalid argument.`
- **AND** its kind is `provider` and its class is request-rejected

#### Scenario: A gateway out of credit answers 403

- **GIVEN** a gateway answers 403 with "Insufficient quota. Please top up and
  try again."
- **WHEN** the adapter classifies the authentication rejection
- **THEN** the error reads `provider authentication rejected: ... Insufficient
  quota. Please top up and try again.`

#### Scenario: A reason echoes the request's credential

- **GIVEN** a provider's error message contains the bare token of the
  `Authorization: Bearer <token>` header the adapter sent
- **WHEN** the adapter reports the failure
- **THEN** no part of the reason appears in the message, metadata, Debug or
  events

## MODIFIED Requirements

### Requirement: Gemini credential non-disclosure

The native adapter SHALL acquire provider credentials only after request and
endpoint validation and SHALL inject the key only as `x-goog-api-key` at the
transport boundary. Keys, signatures, raw bodies, prompts, interaction/event
IDs, and backend diagnostics MUST NOT enter errors, events, metadata, debug
output, manifests, checkpoints, or session diagnostics. The one exception is
a provider's stated reason that passes the checks in "Provider rejections
carry a checked reason".

#### Scenario: Gemini rejects an API key

- **GIVEN** the provider returns an authentication status and sensitive body
- **WHEN** the adapter classifies the failure before semantic output
- **THEN** it uses the shared authentication and credential-recovery contract
- **AND** discards the raw body and every value the request sent as a
  credential, keeping at most a checked reason

### Requirement: Bounded redaction-safe streaming transport

Both policies SHALL disable proxies and redirects, bound error bodies to 8 KiB,
return at most 128 headers of at most 8 KiB each, and stream successful bodies
through ByteStream. Errors and Debug MUST omit secrets. A rejected request
SHALL report the provider's reason from the documented error object of the
body, or of the first SSE `data:` line that holds one, under the checks of
"Provider rejections carry a checked reason", and never a raw excerpt.
Connect timeout SHALL be configurable, defaulting to ten seconds;
cancellation SHALL work by dropping pending requests or response streams,
without detached I/O or hidden retries.

#### Scenario: Redirect or credential echo

- **WHEN** a response redirects or echoes credentials in an error body
- **THEN** no redirect is followed and no raw credential enters diagnostics.

#### Scenario: SSE-framed rejection

- **WHEN** a streaming endpoint rejects a request with `event: error` and a
  `data:` line holding an error object
- **THEN** the transport reports that object's checked reason

#### Scenario: Stream local response and cancel

- **WHEN** a Loopback server emits chunks before closing its response
- **THEN** the caller can consume them incrementally
- **AND** dropping a pending request or ByteStream releases its I/O.
