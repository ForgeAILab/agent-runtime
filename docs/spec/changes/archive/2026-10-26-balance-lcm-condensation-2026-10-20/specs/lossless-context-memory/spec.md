## ADDED Requirements

### Requirement: Condensation keeps the summary tree balanced
The runtime SHALL schedule a condensation when a size tier is full: a
contiguous run of `condensation_fanout` adjacent active nodes whose tier
(`floor(log_fanout(covered entries))`) is equal, condensing the newest full
run first. The active-node count alone SHALL NOT trigger a condensation.
When compaction is required by token pressure and leaf summarization cannot
relieve it, the runtime SHALL condense the smallest-tier run of at least two
adjacent nodes (newest first), falling back to the adjacent pair with the
smallest combined size. Selection SHALL be a deterministic function of the
active nodes, policy and pressure mode.

#### Scenario: Old large node is not re-wrapped by every condensation
- **WHEN** the frontier holds one large condensed node followed by new leaf summaries and no token pressure applies
- **THEN** only a full tier of similar-sized nodes is condensed
- **AND** the large node is not a child of the new condensed node

#### Scenario: Depth stays logarithmic without pressure
- **WHEN** a timeline accumulates 2000 leaf summaries under a fanout of 4 without token pressure
- **THEN** no active node's depth exceeds ceil(log4(covered entries)) plus a small constant
- **AND** the frontier holds at most about (fanout - 1) nodes per tier

#### Scenario: Pressure still bounds context
- **WHEN** compaction is required and no tier is full
- **THEN** the smallest-tier run of at least two adjacent nodes is condensed

#### Scenario: Resume re-plans the same group
- **WHEN** a pending condensation is re-planned from the same active nodes after a restart
- **THEN** the same group is selected
