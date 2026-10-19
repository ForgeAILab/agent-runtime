## MODIFIED Requirements

### Requirement: Durable child ownership and policy compatibility

Every recovered child operation SHALL resolve through the original
parent-scoped catalog and composed authorization path. Recovery MUST validate
the parent session, security scope, child session, specification fingerprint,
workspace identity, model/provider policy, tool-view upper bound, and relevant
revisions. It MAY apply a strictly narrower compatible policy, but MUST fail
closed rather than widen authority or silently change identity. A child whose
policy the host cannot compute while the parent's catalog is restored SHALL
be marked incompatible, and an interrupted one non-resumable, without
failing the parent's restore.

#### Scenario: Another parent knows the child ID

- **GIVEN** a child belongs to parent session A
- **WHEN** parent session B requests its result, follow-up, resume, or stop by
  the same child ID
- **THEN** the runtime returns a structured ownership denial
- **AND** exposes no child content or state mutation

#### Scenario: Workspace identity changed

- **GIVEN** a durable child was authorized for one canonical workspace
- **WHEN** recovery resolves a different or less-trusted workspace identity
- **THEN** recovery fails closed as incompatible
- **AND** does not rebuild the child with the new workspace

#### Scenario: The host can no longer build a child's policy

- **GIVEN** a parent's catalog records a child the host factory now fails to
  fingerprint, for example because the child's profile route is gone
- **WHEN** the parent session is restored
- **THEN** the parent restores
- **AND** that child reports the incompatibility and is not resumable
- **AND** the other children are unaffected
