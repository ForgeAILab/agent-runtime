## ADDED Requirements

### Requirement: Effect domains remain structurally distinct

The runtime SHALL distinguish local filesystem, same-user host filesystem, and
external-service read/write effects, and SHALL represent network and data
egress with exact destination scopes when supplied. Permission derivation,
scheduling, prepared-resource validation, and workspace enforcement MUST retain
those domains: host or external effects MUST NOT be interpreted as project
filesystem access, while local effects MUST NOT escape workspace enforcement.

#### Scenario: Host process writes outside a project

- **GIVEN** a prepared host-process action declares same-user host read/write
  effects and a matching host-defined resource
- **WHEN** the executor validates it
- **THEN** the call remains approval-eligible under typed host permissions
- **AND** the executor does not reject it as a mismatched workspace resource

#### Scenario: External tool has an endpoint

- **GIVEN** an external-service tool declares possible external write, network
  to one endpoint, and data egress to that destination
- **WHEN** its prepared authority is derived
- **THEN** the permission set includes external write, network, and data egress
- **AND** the resource/effects retain the service identity and exact endpoint

### Requirement: Remote tools never claim argument-narrowed authority

A tool whose argument schema is defined by a remote party SHALL derive its
prepared authority solely from its declared static effects. It MUST NOT map
argument values to concrete host resources or narrow its permission set from
raw arguments, because the runtime cannot verify a remote schema's meaning.

#### Scenario: Remote tool takes a path-shaped argument

- **GIVEN** a remote tool whose schema declares a `path` string argument
- **WHEN** the model calls it with a workspace-relative path
- **THEN** preparation claims the tool's full static authority rather than that
  path
- **AND** the prepared permission set does not fall below the declared upper
  bound

### Requirement: Remote invocation honors the runtime deadline and cancellation

A remote tool call SHALL derive its timeout from the invocation context's
deadline rather than a package-local clock, and MUST observe cancellation. When
a deadline expires or the turn is cancelled, the in-flight protocol request MUST
be cancelled and the call MUST resolve as a tool error rather than blocking
turn completion.

#### Scenario: Server stops responding mid-call

- **GIVEN** an invoked remote tool whose server accepts the request and never
  replies
- **WHEN** the invocation deadline expires
- **THEN** the protocol request is cancelled
- **AND** the call resolves as a tool error the model can observe
- **AND** the turn completes

#### Scenario: Turn is interrupted during a remote call

- **GIVEN** an in-flight remote tool call
- **WHEN** the user interrupts the turn
- **THEN** the call observes cancellation and stops waiting
- **AND** session shutdown is not blocked by the pending request

### Requirement: Remote results are bounded before entering the transcript

Remote tool output SHALL be translated into the canonical outcome shape and
bounded before it reaches the transcript. A server-reported tool error MUST
surface as a model-visible tool error rather than a transport failure, and
non-text content MUST be represented by bounded metadata rather than inlined
payload bytes.

#### Scenario: Server returns an error result

- **GIVEN** a remote tool that returns a result flagged as an error
- **WHEN** the outcome is committed
- **THEN** the model observes a tool error containing the server's message
- **AND** the session continues rather than failing the turn

#### Scenario: Server returns an oversized result

- **GIVEN** a remote tool returning text far exceeding the configured output
  bound
- **WHEN** the outcome is committed
- **THEN** the transcript receives a bounded representation with an explicit
  truncation marker
- **AND** the model-facing context bound is not exceeded

#### Scenario: Server returns binary content

- **GIVEN** a remote tool returning an image content block
- **WHEN** the outcome is committed
- **THEN** the transcript records bounded metadata identifying the content type
  and size
- **AND** the payload bytes are not inlined into the transcript
