## ADDED Requirements

### Requirement: Reused LCM accounting preserves decisions and authority

Reusing LCM accounting SHALL produce the same pressure totals, configured
budget decisions, and observable outcomes as full computation under the same
versioned policy and source evidence. Reuse MUST NOT weaken view authorization,
revision checks, source validation, checked arithmetic, protected commit
ordering, or explicit conflict behavior. Loss of reusable accounting MUST
affect performance only and MUST NOT reset persisted state or infer authority.

#### Scenario: Warm append and cold recomputation agree

- **GIVEN** the same authorized history and versioned sizer/policy
- **WHEN** pressure is evaluated after a validated append using reused accounting and using full computation
- **THEN** both yield identical token totals and pressure/compaction decisions
- **AND** provider usage and configured limits are unchanged

#### Scenario: Authority is revoked after a prior successful evaluation

- **WHEN** the host revokes an LCM view grant before the next evaluation
- **THEN** previously computed accounting cannot authorize another lookup or result
- **AND** the existing denial behavior remains fail-closed

#### Scenario: Revision or canonical prefix changes

- **WHEN** store, policy, sizer, classifier, guard, DAG, binding evidence, or canonical history no longer satisfies existing validation
- **THEN** reusable accounting cannot override the mismatch
- **AND** the existing full validation, conflict, and recovery policy remain authoritative

#### Scenario: Old protected LCM state resumes cold

- **WHEN** a supported legacy checkpoint resumes without process-local accounting
- **THEN** its existing schema, component revision, history fingerprint, and canonical state are validated as before
- **AND** accounting is rebuilt without a state reset or additional authority
