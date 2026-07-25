## MODIFIED Requirements

### Requirement: Embeddable runtime facade

The shared repository SHALL expose a host-neutral in-process runtime that can
start and control sessions without a daemon. Hosts MUST be able to inject
provider, tool, security-check, approval, isolation-backend, filesystem, egress,
credential, content-guard, workspace, session, event, and clock implementations
through documented Rust contracts. Missing required security implementations
MUST deny the affected privileged action rather than install a permissive
fallback. The `credential` injectable is the brokered `CredentialBroker`/opaque
`CredentialRef` path used for all tool-facing and invocation-time secret
access. The pre-existing raw-resolve `SecretStore` injectable remains available
as a deprecated host-only configuration path: it MUST NOT be reachable from
tool invocation, activation, or any other tool-visible contract, and it MUST
NOT be treated as satisfying the `credential` injectable requirement.

#### Scenario: Minimal host starts a session

- **GIVEN** a host supplies deterministic fake services and an explicit
  no-privilege authoritative check
- **WHEN** it builds the runtime and starts a session
- **THEN** the session can execute a provider-only agent turn in process
- **AND** no Smith, Nyx, Open Forge, daemon, UI, network, filesystem, credential,
  or isolation-backend dependency is required

#### Scenario: Deprecated SecretStore stays out of tool-facing paths

- **GIVEN** a host supplies both a `SecretStore` and a `CredentialBroker`
  implementation
- **WHEN** a tool requests brokered credential access
- **THEN** the runtime resolves the request only through the
  `CredentialBroker`/opaque-reference path
- **AND** the `SecretStore` raw-resolve path is not reachable from tool
  invocation, activation, or any tool-visible contract

## ADDED Requirements

### Requirement: Per-session security context

Each session SHALL bind an immutable security subject, tenant/workspace/agent
scope, composed check-set fingerprint/revision, approved isolation
backend/profile set, and applicable broker/guard revisions before its first
turn. A change to identity or authority MUST create an explicit new context and
scoped view; callers MUST NOT mutate authority in place during an active request.

#### Scenario: Session starts without a security subject

- **GIVEN** a host starts a session without an explicit security context
- **WHEN** the session requests a privileged capability or side effect
- **THEN** the runtime uses a stable anonymous/no-privilege subject and denies
  the action
- **AND** it does not infer authority from session identifiers or prompt text

### Requirement: Extensible security component registration

The runtime SHALL let hosts register multiple client-defined `SecurityCheck`,
`ContentGuard`, broker, credential-store, and `IsolationBackend`
implementations through stable host-neutral contracts before the runtime or
session is sealed. Registration MUST NOT permit replacement or bypass of the
runtime-owned enforcer, decision composer, grant validator, broker call sites,
or no-fallback rule. Duplicate identities, ambiguous revisions, unsupported
profiles, and mutable post-seal replacement MUST be rejected.

#### Scenario: Client adds a tenant-specific check

- **GIVEN** a client registers a required-constraint check for tenant quotas and
  an authoritative endpoint policy before runtime construction
- **WHEN** a tool requests a permitted endpoint above its tenant quota
- **THEN** the client check participates in the same composed decision and can
  deny or narrow the request
- **AND** the client does not fork the runtime or replace enforcement call sites

#### Scenario: Client adds a new tool and backend

- **GIVEN** a client registers a tool with explicit permissions, artifact kind,
  and required isolation profile
- **AND** it registers an approved backend supporting that artifact and exact
  profile revision
- **WHEN** the runtime seals its registries and starts a session
- **THEN** the tool participates through the neutral tool and isolation contracts
- **AND** all central authorization, broker, grant, audit, and no-fallback
  invariants remain active

### Requirement: Guard-revalidated session resume

Resuming a persisted session SHALL revalidate the persisted snapshot's
content-guard, composed check-set, and permission-vocabulary revisions against
the runtime's currently active revisions, and MUST fail closed on any mismatch
unless the host explicitly opts into a labeled non-equivalent resume. Resuming
a persisted session under a different security subject than the one it was
created under MUST be denied, or MUST force re-guarding of the session's
context under the new subject before any privileged action proceeds. The
host-supplied `SessionStore` is inside the trusted computing base: the runtime
trusts the snapshot bytes it returns to be unmodified, but MUST still
revalidate their recorded security revisions rather than trusting the stored
security context as automatically current.

#### Scenario: Resume after a guard or check-set revision changes

- **GIVEN** a persisted session references an older content-guard, check-set,
  or permission-vocabulary revision
- **WHEN** a host resumes that session under the current runtime
- **THEN** resume fails with a structured revision-mismatch result unless the
  host explicitly requests non-equivalent resume
- **AND** no privileged action is authorized from the stale revisions

#### Scenario: Resume under a different subject

- **GIVEN** a persisted session was created under one security subject
- **WHEN** a host resumes it while authenticated as a different subject
- **THEN** the runtime denies the resume or creates a new re-guarded security
  context for the session
- **AND** the resumed session cannot inherit the original subject's grants

### Requirement: Bounded sub-agent delegation

A delegated sub-agent session SHALL derive its security subject, composed
check set, and approved isolation backend/profile set as a subset of the
parent session's corresponding values. Delegation occurs through an ability
modeled as a sub-agent kind. Delegation MUST NOT grant the child session
any permission, backend, or profile the parent does not already hold, and
grants issued to the parent session MUST NOT cross the delegation boundary
into the child session. The parent turn's trust classification and
content-guard/taint evidence for any content that seeds the child's request
MUST propagate into the child's authorization requests, so the child cannot
receive elevated trust for content the parent could not trust.

#### Scenario: Sub-agent inherits a bounded subset

- **GIVEN** a parent session has an approved isolation backend/profile set and
  a composed check set
- **WHEN** it delegates to a sub-agent ability
- **THEN** the child session's security subject, check set, and approved
  backend/profile set are each a subset of the parent's
- **AND** the child cannot activate a capability or backend/profile the
  parent was not itself authorized for

#### Scenario: Tainted content propagates into delegation

- **GIVEN** the parent turn's content includes a fragment classified as
  external/untrusted
- **WHEN** the parent delegates a sub-agent session seeded with that content
- **THEN** the child's authorization requests carry the same trust
  classification/taint evidence for that content
- **AND** the child cannot treat delegated untrusted content as trusted host
  authority
