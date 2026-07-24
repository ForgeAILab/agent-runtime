# registry-foundation Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Typed namespaced registry entries

The runtime SHALL represent every registrable component with a stable
namespaced identifier, a typed domain, a descriptor revision, bounded
searchable metadata, and source provenance. Enumerating or searching entries
MUST NOT require construction of executable instances.

#### Scenario: Two domains reuse a local name

- **GIVEN** a tool and a model both use the local name `browser`
- **WHEN** they are registered under different typed namespaces
- **THEN** both entries are addressable without collision
- **AND** resolving either entry returns a handle of the declared domain type

### Requirement: Deterministic layered sealing

Registry builders SHALL combine explicitly ordered sources, reject ambiguous
duplicates and alias cycles, and seal into an immutable snapshot with a stable
revision and fingerprint. Cross-layer replacement MUST require an explicit
override relationship and deterministic precedence.

#### Scenario: Plugin shadows a built-in entry without permission

- **GIVEN** a plugin and the built-in layer declare the same registry ID
- **AND** the plugin declaration has no explicit override relationship
- **WHEN** the registry is sealed
- **THEN** sealing fails with a structured conflict
- **AND** no partially resolved snapshot is exposed

#### Scenario: Equivalent inputs are sealed twice

- **GIVEN** identical entries, layers, aliases, and revisions
- **WHEN** two registry builders seal them independently
- **THEN** iteration order and resolution results are identical
- **AND** both snapshots have the same fingerprint

### Requirement: Policy-scoped registry views

The runtime SHALL derive immutable registry views from a sealed snapshot using
identity, workspace, policy, sandbox, readiness, health, quota, risk, and model
compatibility inputs. Hard exclusions MUST be applied before retrieval and MUST
NOT disclose excluded entry metadata through results or errors.

#### Scenario: Browser capability is denied for one agent

- **GIVEN** the global snapshot contains a browser MCP capability
- **AND** the active agent policy denies network navigation
- **WHEN** that agent searches the scoped registry view
- **THEN** the browser capability is absent from candidates and dependency
  expansion
- **AND** the response does not reveal whether the entry exists globally

### Requirement: Unified query with typed resolution

The runtime SHALL expose one query surface over authorized registry domains
while retaining typed resolution and activation contracts underneath. The
ordinary agent-facing view SHALL expose actionable abilities only unless the
host grants authority for another domain.

#### Scenario: Agent searches for research capabilities

- **GIVEN** the scoped view contains an authorized search skill, browser MCP
  tool, and research agent
- **WHEN** the agent performs one capability query for web research
- **THEN** the query can return cards from all three ability kinds
- **AND** resolving each selected card yields its correctly typed activation
  handle

#### Scenario: Agent lacks model-routing authority

- **GIVEN** the same global snapshot contains models and tokenizers
- **WHEN** an ordinary agent queries its registry view
- **THEN** model and tokenizer entries are not discoverable
- **AND** host APIs may still resolve them for runtime composition

### Requirement: Snapshot isolation

A turn or declared execution phase SHALL reference one registry snapshot and
one scoped-view fingerprint. Control-plane registration, health refresh, or
remote catalog updates MUST NOT silently change that run-plane view.

#### Scenario: Plugin is installed during a provider request

- **GIVEN** a request is executing against a sealed registry view
- **WHEN** a plugin registers a new capability in the control plane
- **THEN** the active request continues with its original snapshot
- **AND** the new capability is eligible only for a later explicitly resolved
  view
