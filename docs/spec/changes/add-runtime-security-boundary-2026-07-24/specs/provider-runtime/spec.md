## ADDED Requirements

### Requirement: Policy-mediated provider transport

Production provider and remote-catalog adapters SHALL perform network I/O only
through a policy-mediated transport that authorizes the normalized endpoint,
method, path, redirects, address class, sizes, headers, and credential binding.
Provider request payload sensitivity/data classes MUST also be authorized for
the destination and purpose. Credential material MUST be injected only after
the endpoint and data decisions and MUST NOT be accepted as an unrestricted
tool-visible or debug-visible header value. When a host has not configured an
explicit provider allowlist, the runtime MUST apply a defined default: the
default allowlist authorizes exactly the host's configured provider base
URL(s) for that adapter and denies every other endpoint. This default keeps an
existing single-provider host's configured traffic working under default-deny
while denying unconfigured wildcard egress.

#### Scenario: Configured provider endpoint is not allowed

- **GIVEN** an OpenAI-compatible adapter is configured with an endpoint outside
  the host's active provider allowlist
- **WHEN** the adapter prepares a request
- **THEN** it fails before credential resolution, DNS, or network I/O
- **AND** emits a redaction-safe endpoint-denial event

#### Scenario: Provider redirects across origin

- **GIVEN** an allowed provider endpoint responds with a cross-origin redirect
- **WHEN** redirects are not explicitly granted for the target
- **THEN** the transport rejects the redirect
- **AND** never forwards authorization or configured sensitive headers

#### Scenario: Host configures no explicit allowlist

- **GIVEN** a host has not configured an explicit provider allowlist
- **AND** the OpenAI-compatible adapter is configured with one provider base
  URL
- **WHEN** the adapter prepares a request to that configured base URL
- **THEN** the request is authorized under the default allowlist derived from
  the configured base URL
- **AND** a request to any other endpoint is denied

### Requirement: Redaction-safe provider configuration

Provider configuration SHALL remain redaction-safe across transport requests,
retry records, errors, and debug representations, including all header values,
request bodies, credential bindings, and configured secret material. The API
MUST distinguish public headers from broker-injected sensitive headers rather
than relying on callers to remember a redact-key list.

#### Scenario: Host logs provider debug output

- **GIVEN** provider configuration contains authorization and custom credential
  headers
- **WHEN** the provider or configuration is formatted with `Debug` or emitted in
  an error
- **THEN** header names may be shown but all values and body content are absent
  or redacted

### Requirement: Policy-mediated MCP transport

MCP server transports SHALL be authorized through the same policy-mediated
transport contract as provider and remote-catalog traffic, covering both
remote HTTP/SSE transports and local stdio transports that spawn a child
process. A
stdio MCP transport MUST be treated as a `process.spawn` action requiring
authorization, and the spawned process MUST NOT inherit the host's ambient
environment; only an explicitly granted, minimal environment MAY be passed to
it.

#### Scenario: Stdio MCP server spawn is authorized

- **GIVEN** a host configures a local MCP server launched over stdio
- **WHEN** the runtime starts that transport
- **THEN** it requests `process.spawn` authorization through the same composed
  check-set path used for provider transport
- **AND** the spawned process receives only an explicitly granted environment
  rather than the host's full environment

#### Scenario: Remote MCP transport reuses provider transport policy

- **GIVEN** a host configures a remote MCP server over HTTP
- **WHEN** the runtime connects to it
- **THEN** the connection is authorized through the same normalized
  endpoint/method/path policy-mediated transport used for provider and catalog
  traffic
- **AND** an endpoint outside the active allowlist is denied before connection
