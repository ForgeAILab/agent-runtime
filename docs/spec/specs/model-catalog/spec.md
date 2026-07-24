# model-catalog Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Resolved model profile

The runtime SHALL resolve a canonical immutable model profile containing model
and provider identity, context/input/output limits, modalities, capabilities,
tokenizer revision, request-adapter revision, cache-policy revision, and
field-level source/confidence metadata when known.

#### Scenario: Provider and generic metadata are combined

- **GIVEN** generic metadata describes a model's modalities
- **AND** provider-local configuration declares a lower served context limit
- **WHEN** the model profile is resolved
- **THEN** the provider-local limit wins according to precedence
- **AND** the profile retains provenance for both resolved fields

### Requirement: Local-first layered resolution

Model resolution SHALL use deterministic precedence from explicit host/session
overrides through provider-local data, embedded data, validated cached remote
data, and optional future remote refreshes. Conflicting entries at the same
precedence MUST fail rather than resolve by insertion order.

#### Scenario: Explicit host override exists

- **GIVEN** cached catalog metadata declares one output limit
- **AND** the host explicitly configures another supported output limit
- **WHEN** the profile is resolved
- **THEN** the explicit host value is used
- **AND** the lower-precedence value remains identifiable in resolution
  diagnostics

### Requirement: Optional offline-first models.dev source

When enabled, the runtime SHALL provide a models.dev catalog source through
injected transport and cache contracts. The source SHALL keep remote retrieval
outside the request path, validate bounded schema data, retain source revision
and age, and make the last validated cache usable offline according to host
policy.

#### Scenario: Network is unavailable at turn start

- **GIVEN** the host has a validated cached models.dev catalog revision
- **AND** no network connection is available
- **WHEN** a turn resolves a known model
- **THEN** it can use the cached revision according to stale-data policy
- **AND** no request-path network call is attempted

#### Scenario: Remote catalog changes during a turn

- **GIVEN** a turn has frozen a resolved model profile
- **WHEN** a background refresh installs a newer catalog revision
- **THEN** the active turn retains its original model profile
- **AND** the new revision is considered only by a later registry snapshot

### Requirement: Provider-owned token and cache semantics

Remote catalog limits and capability metadata MUST NOT override exact
provider/tokenizer request sizing, wire framing, or prompt-cache semantics.
Provider and tokenizer adapters SHALL own those versioned contracts, with
explicit local configuration taking precedence.

#### Scenario: Catalog advertises cache pricing only

- **GIVEN** remote metadata includes cache-read pricing but no exact cache-marker
  semantics
- **WHEN** the runtime prepares a cache plan
- **THEN** it uses the selected provider adapter's declared cache policy
- **AND** it does not infer marker placement or cache lifetime from pricing

### Requirement: Conservative unknown-model handling

The runtime MUST NOT guess a permissive context window for an unknown model.
When safe enforcement limits cannot be resolved, context planning SHALL fail
before provider network I/O unless the host supplies an explicit profile.

#### Scenario: Custom model has no registered limits

- **GIVEN** a host selects an unknown custom model
- **AND** no catalog source or explicit override provides safe limits
- **WHEN** the runtime prepares the turn
- **THEN** it returns a structured missing-model-profile error
- **AND** no provider request is sent
