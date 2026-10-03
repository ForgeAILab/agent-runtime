## ADDED Requirements

### Requirement: First changed cache-prefix fragment is observable

Cache plans and CachePlanChanged SHALL expose optional first_changed_fragment
evidence from their committed predecessor comparison. The diagnostic MUST name
the earliest changed ID, content hash, or cache class within an established
stable prefix, using the current ID or the removed predecessor ID when no
current item exists. It MUST NOT change cache identity, fingerprints, provider
wire output, or canonical cache metrics.

#### Scenario: A stable instruction changes

- **GIVEN** two eligible requests share the same non-fragment identity partition
- **WHEN** the second changes one stable instruction's content
- **THEN** first_changed_fragment names that instruction at the first differing position
- **AND** downstream unchanged fragments are not reported as the cause

#### Scenario: Prefix segment is removed or changes class

- **WHEN** a comparable request removes the final stable-prefix segment or makes it ephemeral
- **THEN** the diagnostic names the removed predecessor ID or the changed current ID respectively
- **AND** no sensitive fragment content is emitted

#### Scenario: New tail does not invalidate an established prefix

- **WHEN** the prior stable prefix remains identical and only an appended or newly promoted tail is added
- **THEN** first_changed_fragment is absent
- **AND** existing prefix metrics keep their current semantics

#### Scenario: No fragment cause is attributable

- **WHEN** a request has no usable predecessor, a suppressed/unsupported boundary, or a changed non-fragment identity partition
- **THEN** first_changed_fragment is absent even if aggregate invalidation is nonzero
- **AND** the runtime does not fabricate a fragment cause

#### Scenario: Fragment ID is unsafe for telemetry

- **WHEN** a changed ID violates the new diagnostic field's bounded safe-ID rules
- **THEN** the event omits first_changed_fragment
- **AND** the planner's fragment identity and all cache behavior remain unchanged
