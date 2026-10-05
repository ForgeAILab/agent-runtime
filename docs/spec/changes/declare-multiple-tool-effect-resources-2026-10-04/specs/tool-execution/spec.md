## ADDED Requirements

### Requirement: Prepared actions bind authority to every resource claim

If approved, a prepared tool action SHALL bind each required permission set to
its concrete resource claim in one immutable fingerprint. The runtime MUST
validate and authorize every claim before approval or invocation, and MUST NOT
reuse a grant for another resource or infer a permission/resource Cartesian product.

#### Scenario: Shell invocation needs workspace and host writes

- **GIVEN** a shell action declares a filesystem workspace write claim and an Other host-filesystem write claim
- **WHEN** the runtime prepares and authorizes the action
- **THEN** each permission is checked against its own resource
- **AND** denial of either claim prevents the whole invocation

#### Scenario: Approval edits one resource

- **WHEN** approval changes arguments affecting any resource claim
- **THEN** preparation, fingerprinting, validation, and authorization repeat for the whole action
- **AND** prior grants cannot authorize the edited action

#### Scenario: Legacy action is recovered

- **WHEN** a compatible reader loads a valid legacy single-resource action
- **THEN** an explicit migration preserves its exact authority binding
- **AND** unsupported or ambiguous multi-resource recovery fails closed
