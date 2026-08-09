## ADDED Requirements

### Requirement: Comparable provider-cache expectation

The cache planner SHALL distinguish a plan with no prior provider-request
baseline from a plan compared with a predecessor. It SHALL expose an absent
expected-read value for the first request, zero when a predecessor exists but
no prefix survives under the same cache identity, and the preserved-prefix
token count when reuse is comparable. The expectation MUST retain the
planner's token-count confidence.

#### Scenario: First eligible request has no read expectation

- **GIVEN** a cache-capable session has not yet sent a provider request
- **WHEN** its first context and cache plan are built
- **THEN** the stable prefix remains eligible for future reuse
- **AND** the current request has no expected cache-read value

#### Scenario: Unchanged prefix has a comparable expectation

- **GIVEN** a prior provider request used cache plan A
- **AND** the next plan retains 42,000 tokens under the same cache identity
- **WHEN** the next request is prepared
- **THEN** its expected cache read is 42,000 tokens
- **AND** the expectation identifies the new plan and its confidence

#### Scenario: Model identity changes

- **GIVEN** a prior provider request used one model identity
- **WHEN** the next plan resolves a different provider or model identity
- **THEN** its comparable expected cache read is zero
- **AND** no token from the prior identity is represented as a missed read
