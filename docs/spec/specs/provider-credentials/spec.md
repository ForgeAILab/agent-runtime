# provider-credentials Specification

## Purpose
TBD - created by archiving change add-renewable-provider-credentials. Update Purpose after archive.
## Requirements
### Requirement: Hosts inject renewable provider credential sources

Agent Runtime SHALL define a host-injected provider credential source that can
acquire a provider-scoped authorization lease containing secret material, an
optional absolute expiry, and an opaque bounded revision. Acquisition MUST be
bounded by the provider attempt's cancellation and deadline and MUST request a
minimum validity interval, while a non-expiring static source remains supported
for existing direct API-key integrations.

#### Scenario: Acquire a static provider credential

- **GIVEN** a direct provider adapter is configured with a static API key
- **WHEN** a provider attempt acquires authorization
- **THEN** the compatibility source returns a non-expiring lease
- **AND** the adapter performs no refresh or invalidation replay for that key

#### Scenario: Refresh a near-expiry credential

- **GIVEN** a cached renewable lease expires before the requested minimum
  validity interval
- **WHEN** the adapter acquires authorization for a provider attempt
- **THEN** the host source refreshes before returning a lease
- **AND** the adapter rejects an expired or insufficiently valid lease before
  provider network I/O

#### Scenario: Credential acquisition is cancelled

- **GIVEN** credential acquisition or host-owned refresh is waiting on
  external I/O
- **WHEN** attempt cancellation or its deadline occurs
- **THEN** acquisition stops with a fixed redaction-safe credential error
- **AND** no stale, expired, or partially refreshed credential is injected
- **AND** no provider network request starts

### Requirement: Credential invalidation is exact-revision and bounded

A provider adapter SHALL invalidate only the exact lease revision used by the
rejected attempt. Invalidation MUST observe the remaining attempt cancellation
and deadline, MUST return a bounded outcome indicating whether replacement is
meaningful, and MUST NOT let an older concurrent attempt evict or downgrade a
newer lease.

#### Scenario: Rejected current revision is invalidated

- **GIVEN** a provider rejects the renewable lease used by one attempt before
  semantic output
- **WHEN** the adapter invalidates that exact revision within the deadline
- **THEN** the source atomically marks that revision unusable
- **AND** reports whether a subsequent acquisition may yield a replacement

#### Scenario: Older attempt rejects after a newer refresh

- **GIVEN** two concurrent attempts acquired different credential revisions
- **AND** the newer revision is current
- **WHEN** the older attempt invalidates its rejected revision
- **THEN** the source leaves the newer revision current
- **AND** returns a stale or no-change outcome without exposing either revision

### Requirement: Provider credentials remain non-disclosing

Agent Runtime SHALL keep provider authorization non-disclosing. Provider lease
secrets, revisions, expiry timestamps, source references, authorization
headers, refresh material, raw authentication responses, and backend
diagnostics MUST NOT enter tool-visible values, model context, debug output,
provider errors, runtime events, usage, manifests, checkpoints, or session
snapshots. Any recovery state exposed by the runtime SHALL be a closed
redaction-safe classification attributed only to existing request and attempt
identity.

#### Scenario: Renewable secret canary crosses runtime boundaries

- **GIVEN** a fake renewable source issues a canary access token and revision
- **WHEN** acquisition, invalidation, replay, and terminal failure are observed
- **THEN** the canary and its configured encoded forms are absent from every
  observable and persisted runtime boundary
- **AND** tests can inspect only source-local call records and fixed recovery
  classifications

#### Scenario: Provider returns a sensitive authentication body

- **GIVEN** a provider authentication response includes credential-shaped or
  account-specific content
- **WHEN** the adapter classifies the rejection
- **THEN** it discards the raw body and headers at the adapter boundary
- **AND** returns only the fixed authentication and recovery classification

### Requirement: OAuth ceremony and storage remain host policy

The renewable credential contract SHALL be mechanism-only. Agent Runtime MUST
NOT define browser presentation, callback listeners, OAuth endpoints, client
identifiers, scopes, authorization-code or device-code ceremony, account
selection, logout policy, or access/refresh-token persistence. A host MAY
implement a source using a publicly supported authorization system without
exposing those details to the runtime.

#### Scenario: Host uses OAuth-backed provider credentials

- **GIVEN** a host has completed a publicly supported OAuth flow and owns its
  protected token storage
- **WHEN** Agent Runtime serves a direct provider attempt
- **THEN** it consumes only the generic credential lease and invalidation
  contract
- **AND** it neither starts the login ceremony nor reads refresh material from
  host storage

#### Scenario: Consumer subscription is not a provider key

- **GIVEN** a product login produces credentials whose supported contract is an
  external managed agent backend rather than provider API authorization
- **WHEN** a host composes Agent Runtime providers
- **THEN** it does not adapt those credentials into this lease contract
- **AND** any external backend integration remains outside the direct provider
  adapter path
