## ADDED Requirements

### Requirement: Shared LCM Request Sizing
LCM SHALL measure immutable messages and projected summary messages with the
planner RequestSizer, including reasoning, tool arguments/results and framing.

#### Scenario: Tool and reasoning parity
- **WHEN** the planner and LCM measure the same message using the shared sizer
- **THEN** the measured counts MUST match, including structured tool content.

### Requirement: Working Set Budget and Pressure
An optional host WorkingSetPolicy SHALL cap planner input at the lesser of the
resolved input window and hard_tokens. LCM SHALL evaluate pressure against
target_tokens minus measured fixed overhead from the last successful plan.

#### Scenario: Independent target
- **WHEN** a large provider window is configured with a smaller working set
- **THEN** LCM MUST compact according to the working-set target and the planner MUST enforce the hard cap.

#### Scenario: Absent policy
- **WHEN** no working set is configured
- **THEN** existing provider-budget selection MUST remain available.

### Requirement: Bounded Reclaiming Summaries
LCM SHALL separate leaf source targets from summary output caps, default the
summary maximum ratio to 0.25, and escalate when measured reclaim is below
min_reclaim_ratio. Every accepted summary including fallback MUST meet both
ratios and strict shrink. Hard pressure SHALL derive sufficient bounded rounds
from overage and expected reclaim; leaf selection MUST permit a full turn pair.

#### Scenario: Weak reclaim escalates
- **WHEN** a model response is smaller than its source but violates a ratio
- **THEN** LCM MUST escalate rather than commit it.

#### Scenario: Boundary compaction
- **WHEN** a host opts into soft_on_turn_boundary and a turn completes
- **THEN** runtime SHALL attempt soft compaction after TurnCompleted through existing idle admission and persistence.

### Requirement: Neutral Provider Summary Mechanism
A feature-gated provider summary adapter SHALL use host-supplied instructions,
compact tool-aware rendering, the authoritative planner for every request,
LCM purpose usage accounting, cancellation and deadlines. Oversized leaves
SHALL use bounded map-reduce without exceeding the summarizer input budget.

#### Scenario: Oversized tool leaf
- **WHEN** a source leaf exceeds the summarizer window
- **THEN** multiple measured map calls and bounded reduction MUST preserve tool evidence and aggregate usage.

#### Scenario: Deterministic tool evidence
- **WHEN** model escalation reaches fallback
- **THEN** bounded rendering MUST include tool names, truncated arguments and results.
