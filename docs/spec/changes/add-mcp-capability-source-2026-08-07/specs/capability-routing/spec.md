## ADDED Requirements

### Requirement: MCP servers publish descriptors without executable content

A configured MCP server SHALL contribute one bounded `AbilityDescriptor` per
advertised tool, derived from the server's tool listing. Descriptors MUST carry
the server's registry id as a dependency so a tool is never selectable without
its server, and listing MUST NOT be required again for retrieval, ranking, or
context planning.

#### Scenario: Server catalog is searched after one listing

- **GIVEN** a connected MCP server advertising eight tools
- **WHEN** the runtime indexes and searches the resulting descriptors
- **THEN** retrieval matches on names, descriptions, and declared affordances
  using only bounded card metadata
- **AND** no further protocol request is issued to the server

#### Scenario: Tool is unreachable without its server

- **GIVEN** a descriptor for a tool on a server that is not active
- **WHEN** dependency resolution evaluates the candidate bundle
- **THEN** the tool reports its server as an unsatisfied dependency
- **AND** it is not activated on its own

### Requirement: Remote tool authority is a host floor annotations may only raise

A remote tool's declared effects SHALL be the union of a host-supplied effect
floor and any additional effects implied by server-provided annotations. A
server-provided annotation MUST NOT remove an effect, reduce a permission, or
lower risk. The permission upper bound MUST be derived from the resulting
effects by the same derivation used for native tools.

#### Scenario: Server claims a destructive tool is read-only

- **GIVEN** a server advertises a tool with `readOnlyHint` set to true
- **WHEN** its descriptor and specification are built
- **THEN** the declared effects still include the full host floor
- **AND** the permission upper bound equals that of an identical tool
  advertising no annotations at all

#### Scenario: Server declares a destructive tool

- **GIVEN** a server advertises a tool with `destructiveHint` set to true
- **WHEN** its descriptor is built
- **THEN** the declared effects add a write effect above the floor
- **AND** activation policy can distinguish it from an unannotated tool on the
  same server

### Requirement: Activation dials only after policy and readiness pass

Producing an MCP connection SHALL occur only after activation policy approves
the server and its declared readiness requirements are confirmed. A denied or
unready server MUST NOT cause a process spawn, a socket connection, or any
protocol request.

#### Scenario: Server requires an unset credential

- **GIVEN** a server declares a required credential name that is not ready
- **WHEN** the runtime attempts activation
- **THEN** activation fails with a structured readiness result naming the
  missing credential
- **AND** no child process is spawned and no connection is opened

#### Scenario: Host policy denies a server

- **GIVEN** activation policy denies a server's registry id
- **WHEN** the runtime attempts activation
- **THEN** activation fails with a structured denial
- **AND** no protocol request is issued

### Requirement: Server failure is isolated from the session

A failing server MUST NOT fail the session or corrupt an activation epoch,
whether it fails to start, exceeds its startup deadline, negotiates an
incompatible protocol version, terminates unexpectedly, or emits malformed or
oversized frames. Its tools SHALL become unavailable at a safe execution
boundary and the failure MUST be recorded as a diagnostic.

#### Scenario: Server dies during an active turn

- **GIVEN** an active turn whose frozen epoch includes a server's tools
- **WHEN** that server's process terminates unexpectedly
- **THEN** any in-flight call to it fails as a tool error the model can observe
- **AND** the in-flight provider request is not mutated
- **AND** the tools are removed at the next safe boundary with a new epoch

#### Scenario: Server never completes initialization

- **GIVEN** a configured server that accepts a connection but never answers
  initialization
- **WHEN** its startup deadline expires
- **THEN** it contributes zero abilities
- **AND** the session starts normally with the remaining capabilities
