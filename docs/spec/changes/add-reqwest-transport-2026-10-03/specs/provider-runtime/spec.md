## ADDED Requirements

### Requirement: Explicit reqwest destination policy

The optional ReqwestTransport SHALL require an explicit PublicHttps or Loopback
policy, enforce it on every request, and optionally pin scheme/host/effective
port. Neither feature unification nor configuration MUST bypass a check.

#### Scenario: Public HTTPS endpoint

- **WHEN** PublicHttps receives HTTP, userinfo, fragments, a restricted host,
  or a private/loopback/link-local/restricted IP
- **THEN** it rejects the destination before HTTP I/O with the CLI classification.

#### Scenario: Local model server

- **WHEN** Loopback receives HTTP or HTTPS to 127/8, ::1, or localhost
- **THEN** it permits only nonempty all-loopback address sets
- **AND** rejects private/public IPs, other DNS names and non-loopback localhost answers.

#### Scenario: DNS rebinding or origin change

- **WHEN** any DNS answer violates policy or a request changes a pinned origin
- **THEN** the entire request is rejected
- **AND** allowed answers are pinned into the client without a second DNS lookup.

### Requirement: Bounded redaction-safe streaming transport

Both policies SHALL disable proxies and redirects, bound error bodies to 8 KiB,
return at most 128 headers of at most 8 KiB each, and stream successful bodies
through ByteStream. Errors and Debug MUST omit secrets. Connect timeout SHALL
be configurable, defaulting to ten seconds; cancellation SHALL work by dropping
pending requests or response streams, without detached I/O or hidden retries.

#### Scenario: Redirect or credential echo

- **WHEN** a response redirects or echoes credentials in an error body
- **THEN** no redirect is followed and no raw credential enters diagnostics.

#### Scenario: Stream local response and cancel

- **WHEN** a Loopback server emits chunks before closing its response
- **THEN** the caller can consume them incrementally
- **AND** dropping a pending request or ByteStream releases its I/O.
