## MODIFIED Requirements

### Requirement: Descriptor-first abilities

Tools, skills, MCP capabilities, and agents SHALL publish compact descriptors
separately from executable factories and full context content. A descriptor
MUST declare its provided affordances, dependencies, conflicts, permissions
(typed permission upper bound), trust class, artifact kind, required isolation
profile (for descriptors representing an executable artifact), risk, readiness
requirements, estimated context cost, and content revisions.

#### Scenario: Search a skill without loading its body

- **GIVEN** a skill references a large instruction file and supporting assets
- **WHEN** the registry indexes and searches its descriptor
- **THEN** only bounded card metadata is required
- **AND** the instruction file is loaded only after the skill is selected and
  activated

#### Scenario: Descriptor declares trust class, artifact kind, and isolation profile

- **GIVEN** a descriptor represents an executable artifact
- **WHEN** it is registered
- **THEN** it declares its trust class, artifact kind, and required isolation
  profile alongside its permission upper bound
- **AND** registration is rejected if any of these fields is missing for an
  executable descriptor

### Requirement: Policy-checked lazy activation

The runtime SHALL materialize executable behavior, schemas, instructions, MCP
connections, or agent definitions only after selection and authorization
against the active security subject, composed check-set revision, descriptor
permissions, trust class, required isolation profile/artifact kind, risk,
readiness, provenance, dependencies, conflicts, and intended resource scope.
Discovery MUST NOT imply activation permission, activation MUST NOT bypass
invocation-time authorization or approval, and an activation grant MUST NOT be
reused as an invocation grant.

#### Scenario: Search result requires unavailable credentials

- **GIVEN** a relevant MCP capability requires credentials that are not ready
- **WHEN** the runtime creates the scoped view or attempts activation
- **THEN** the capability is filtered or activation fails with a structured
  readiness result according to host policy
- **AND** no credential resolution, connection, or side effect occurs

#### Scenario: Capability requests an ungranted permission

- **GIVEN** an ability descriptor requires `net.http` or `credential.use`
- **AND** the active security subject has no matching composed grant
- **WHEN** the runtime derives the scoped view or attempts activation
- **THEN** the ability is absent or activation is denied before its factory is
  materialized
- **AND** retrieval and errors do not disclose globally denied entries

## ADDED Requirements

### Requirement: Permission-consistent capability lifecycle

The same security subject and composed check-set revision SHALL govern hard
filtering, retrieval, dependency expansion, selection, activation, context
contribution, and invocation. A later check-set or identity change MUST create a
new scoped view and activation epoch; it MUST NOT mutate or silently widen an
in-flight epoch.

#### Scenario: Security check set changes after activation

- **GIVEN** a capability was activated under one composed check-set revision
- **WHEN** the host changes the subject's permissions before the next invocation
- **THEN** the runtime derives a new scoped view and activation epoch or denies
  the invocation
- **AND** the old activation cannot supply authority under the new revision

### Requirement: Host-authoritative remote descriptor effects

The host SHALL be authoritative for the declared effects, permissions, trust
class, artifact kind, and required isolation profile of any capability whose
descriptor is produced by a remote or otherwise untrusted source, such as an
MCP server's `tools/list` response. The
runtime MUST NOT honor a no-effect, pure, or reduced-permission declaration
supplied by a non-host source over the host's configured classification for
that source. The descriptor's content revision MUST be pinned at the point the
host approves or activates it; a later change to the remote descriptor's
declared affordances, permissions, or effects under the same nominal identity
MUST invalidate the pinned approval and require re-authorization before the
changed descriptor may activate or execute (rug-pull defense).

#### Scenario: Remote MCP tool declares no effects

- **GIVEN** an MCP server's `tools/list` response declares a tool with no
  side effects
- **AND** host policy classifies tools from that source as capable of
  filesystem/network authority by default
- **WHEN** the descriptor is registered
- **THEN** the runtime uses the host-configured effects/permissions rather
  than the source-declared no-effect claim
- **AND** the tool cannot bypass authorization by under-declaring its own
  effects

#### Scenario: Remote descriptor changes after approval

- **GIVEN** a host approved and pinned the revision of a remote MCP tool
  descriptor
- **WHEN** the remote source later returns a descriptor with different
  declared affordances or permissions under the same nominal identity
- **THEN** the runtime treats the change as a new descriptor revision
- **AND** the previously approved activation/grant does not extend to the
  changed descriptor
- **AND** re-authorization is required before the changed descriptor may
  activate or execute
