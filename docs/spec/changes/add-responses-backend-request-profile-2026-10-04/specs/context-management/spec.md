## ADDED Requirements

### Requirement: Responses profile additions are admitted as counted context

Runtime composition using a Responses profile MUST prepare fallback
instructions and synthetic user input before immutable plan admission and count
their text and wire framing. Profile-aware sizing and cache identity SHALL use
the effective context projection/revision. Adapter projection MUST NOT add
uncounted context or retain an incompatible exact-prefix identity.

#### Scenario: Default instructions exceed the input budget

- **GIVEN** ordinary input fits but profile fallback/synthetic content would exceed the enforced budget
- **WHEN** the profile-aware plan is admitted
- **THEN** planning fails before credential acquisition and transport
- **AND** the adapter cannot bypass the failure by injecting defaults afterward

#### Scenario: Profile changes the cacheable projection

- **WHEN** instruction placement, default content, or synthetic bytes change the effective prefix
- **THEN** sizing and exact cache identity reflect the new projection
- **AND** old identity/conformance claims are not reused for different wire context

#### Scenario: Adapter and admitted profile disagree

- **GIVEN** the immutable plan did not account for required profile additions
- **WHEN** the adapter validates a runtime-planned request
- **THEN** it fails before credential acquisition rather than injecting hidden text
