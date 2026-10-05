## ADDED Requirements

### Requirement: Authorized timeline ownership
LcmWriter SHALL expose defaulted claim(view, owner, generation). Unsupported
implementations MUST return Fork. A fresh session MUST reject populated or owned
timelines with typed TimelineOwned unless the host selects Adopt, Fork or Retire.
Replacement and continuation bindings MUST pass unchanged LcmViewAuthority checks.

#### Scenario: Populated timeline policies
- **WHEN** a new owner resolves a timeline with committed nodes
- **THEN** default construction fails with TimelineOwned
- **AND** Adopt seeds canonical history from the timeline projection
- **AND** Fork claims a fresh timeline with a summary seed
- **AND** Retire claims an empty replacement while preserving old immutable data

#### Scenario: Restart after leaf commit
- **WHEN** a new session identity encounters a committed leaf
- **THEN** it requires explicit policy and never starts appending at sequence zero

#### Scenario: Unsupported claim
- **WHEN** a store uses the default claim method
- **THEN** the result is Fork after authority validation
- **AND** no timeline is silently adopted

#### Scenario: Stale ownership epoch
- **WHEN** another owner acquires a newer claim generation
- **THEN** the old owner receives TimelineOwned before provider admission or a timeline commit
- **AND** no old-owner provider request or LCM mutation starts at that boundary

### Requirement: Frontier-safe reconcile
Reconciliation MUST never truncate below max(active_node.range.end)+1. Divergence
below that frontier SHALL return typed LcmDivergence { frontier, at }, with the
same Adopt, Fork or Retire policy options at a host recovery boundary.

#### Scenario: Divergent summarized source
- **WHEN** canonical history differs below the active summary frontier
- **THEN** no truncation occurs and LcmDivergence identifies frontier and at
- **AND** committed entries and nodes remain unchanged

#### Scenario: Concurrent leaf commit
- **WHEN** a leaf advances the frontier after the runtime check
- **THEN** the store atomically rejects truncation with typed RangeOverlap
