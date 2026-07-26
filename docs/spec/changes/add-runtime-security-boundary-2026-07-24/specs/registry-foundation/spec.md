## MODIFIED Requirements

### Requirement: Policy-scoped registry views

The runtime SHALL derive immutable registry views from a sealed snapshot using
identity, workspace, policy, sandbox, readiness, health, quota, risk, model
compatibility, security subject, and composed security-check-set revision
inputs. Hard exclusions MUST be applied before retrieval and MUST NOT disclose
excluded entry metadata through results or errors. A change to the active
security subject or composed check-set revision MUST derive a new scoped view
rather than mutating an existing one, and an entry excluded by security-subject
or check-set evaluation MUST remain indistinguishable from an entry that is
simply absent from the snapshot.

#### Scenario: Browser capability is denied for one agent

- **GIVEN** the global snapshot contains a browser MCP capability
- **AND** the active agent policy denies network navigation
- **WHEN** that agent searches the scoped registry view
- **THEN** the browser capability is absent from candidates and dependency
  expansion
- **AND** the response does not reveal whether the entry exists globally

#### Scenario: Security check set changes exclusions

- **GIVEN** a scoped registry view was derived under one composed
  security-check-set revision
- **AND** a capability is denied for the active security subject under that
  revision
- **WHEN** the host's composed check-set revision changes
- **THEN** the runtime derives a new scoped view for subsequent queries rather
  than mutating the existing one
- **AND** the denied capability remains indistinguishable from an absent entry
  in both views' results and errors
