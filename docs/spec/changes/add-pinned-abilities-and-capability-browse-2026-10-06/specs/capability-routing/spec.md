## ADDED Requirements

### Requirement: Pinned activation
The runtime SHALL authorize and materialize visible pinned abilities in the first epoch, resolve dependencies, charge token budgets but not candidate cardinality, and reconcile missing pins on restore or rebase.

#### Scenario: A scope denies a pin
- **WHEN** a scope denies a pin
- **THEN** The denied pin is skipped; authorized pins remain mandatory and an oversized pinned closure fails configuration.

### Requirement: Agent capability discovery
The runtime SHALL protect registry.search and registry.activate in every live session. Empty queries SHALL list authorized non-bootstrap entries with paging and domain counts without staging. Explicit ids SHALL stage independently under token budgets and use the search transaction guard.

#### Scenario: A turn fails after staging
- **WHEN** a turn fails after staging
- **THEN** The staged activation is rolled back and no new active capability leaks.

### Requirement: Descriptive explicit retrieval
Explicit search SHALL score whole words in descriptive text below curated keyword weight and rank tools before skills on equal scores. Prompt pre-activation MUST retain metadata-only matching.

#### Scenario: A term appears only in description
- **WHEN** a term appears only in description
- **THEN** Explicit search finds the entry, while prompt pre-activation does not bind it.
