## ADDED Requirements

### Requirement: Capability scope patterns
Scopes SHALL accept validated domain:name wildcard patterns. Deny MUST win; allow ids and patterns form a union. Protected bootstrap tools MUST remain visible.

#### Scenario: Allow and deny overlap
- **WHEN** allow and deny overlap
- **THEN** The matching entry is absent from all agent discovery and activation surfaces.
