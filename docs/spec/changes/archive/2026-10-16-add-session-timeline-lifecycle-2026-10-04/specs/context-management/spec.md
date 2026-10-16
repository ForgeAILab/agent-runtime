## ADDED Requirements

### Requirement: Authorized timeline ownership
LcmWriter SHALL expose defaulted claim(view, owner, generation). The default
records nothing: after authority validation it SHALL return Claimed for an empty
timeline and Unsupported for a populated one. A fresh session MUST reject
populated or owned timelines of a claim-aware store with typed TimelineOwned
unless the host selects Adopt, Fork or Retire. Replacement and continuation
bindings MUST pass unchanged LcmViewAuthority checks.

#### Scenario: Populated timeline policies
- **WHEN** a new owner resolves a timeline with committed nodes
- **THEN** default construction fails with TimelineOwned
- **AND** Adopt claims the next generation first and then seeds canonical history from the claimed timeline projection
- **AND** Fork claims a fresh timeline with a summary seed
- **AND** Retire claims an empty replacement while preserving old immutable data

#### Scenario: Adopt never discards a seed
- **WHEN** a host requests Adopt with non-empty seed history
- **THEN** construction returns typed Conflict and no timeline is claimed

#### Scenario: Restart after leaf commit
- **WHEN** a new session identity encounters a committed leaf
- **THEN** it requires explicit policy and never starts appending at sequence zero

#### Scenario: Claim-unaware store
- **WHEN** a store uses the default claim method
- **THEN** a session creates and runs normally on an empty timeline
- **AND** the session that holds the binding continues on its populated timeline, live and after resume
- **AND** a fresh binding of a populated timeline forks to a resolver-supplied replacement with a summary seed
- **AND** explicit Adopt returns typed ForkRequired, so no timeline is adopted without fencing

#### Scenario: Pre-existing unclaimed timeline
- **WHEN** a session whose validated state predates ownership resumes with Adopt
- **THEN** it claims generation one, rebuilds its state against the claimed timeline and continues
- **AND** without a policy the resume fails with TimelineOwned and no owner

#### Scenario: Stale ownership epoch
- **WHEN** another owner acquires a newer claim generation
- **THEN** the claim bumps the store revision
- **AND** the old owner receives TimelineOwned before provider admission or a timeline commit
- **AND** a claim-aware store rejects any mutation whose view fence is not the current owner with TimelineOwned

#### Scenario: Ephemeral LCM
- **WHEN** an ephemeral session runs with LCM configured
- **THEN** its LCM uses a volatile in-memory timeline
- **AND** the host's durable timeline is neither claimed nor written

#### Scenario: Bounded fork summary
- **WHEN** a Fork policy renders the projected summary of a timeline
- **THEN** the rendering is capped by a configurable bound and keeps the newest tail
- **AND** sources classified Secret never enter it

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
