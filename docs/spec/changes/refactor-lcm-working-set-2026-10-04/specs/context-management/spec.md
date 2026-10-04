## ADDED Requirements

### Requirement: Shared LCM Request Sizing
By default LCM SHALL measure immutable messages and projected summary messages
with the planner RequestSizer, including reasoning, tool arguments/results and
framing. A host-supplied LcmSizer MUST be kept as given; builder composition
MUST NOT replace it.

#### Scenario: Tool and reasoning parity
- **WHEN** the planner and LCM measure the same message using the shared sizer
- **THEN** the measured counts MUST match, including structured tool content.

#### Scenario: Host sizer kept
- **WHEN** a host configures its own LcmSizer and composes LCM through the builder
- **THEN** LCM MUST size entries with the host sizer, and only the default sizer is replaced by the planner RequestSizer.

### Requirement: Working Set Budget and Pressure
An optional host WorkingSetPolicy SHALL cap planner input at the lesser of the
resolved input window and hard_tokens. LCM SHALL evaluate pressure against
target_tokens minus measured fixed overhead from the last successful plan, but
never against less than 25% of target_tokens. When the floor applies LCM MUST
emit a typed OverheadExceedsTarget diagnostic; overhead MUST be re-measured at
the next plan.

#### Scenario: Independent target
- **WHEN** a large provider window is configured with a smaller working set
- **THEN** LCM MUST compact according to the working-set target and the planner MUST enforce the hard cap.

#### Scenario: Overhead at or above target
- **WHEN** measured fixed overhead leaves less than the floor, including a stale persisted overhead
- **THEN** LCM MUST use the floor, MUST NOT refuse the turn for pressure alone, and MUST emit the typed diagnostic.

#### Scenario: Absent policy
- **WHEN** no working set is configured
- **THEN** existing provider-budget selection MUST remain available.

### Requirement: Bounded Reclaiming Summaries
LCM SHALL separate leaf source targets from summary output caps, default the
summary maximum ratio to 0.25, and escalate when measured reclaim is below
min_reclaim_ratio. Every accepted model summary MUST meet both ratios and strict
shrink. The deterministic fallback MUST strictly shrink within
min(deterministic_token_cap, source - 1) and is not ratio-capped. Leaf selection
MUST permit a full turn pair and MUST widen a leaf the fallback cannot shrink
through the next turn. A summary that still cannot fit under hard pressure MUST
end the round as a structured cannot_fit. Hard pressure SHALL derive rounds with
one shared formula from overage and expected reclaim, fixed once per admission
epoch and never above MAX_DERIVED_HARD_ROUNDS (64).

#### Scenario: Weak reclaim escalates
- **WHEN** a model response is smaller than its source but violates a ratio
- **THEN** LCM MUST escalate rather than commit it.

#### Scenario: Tiny oldest turn
- **WHEN** the oldest turn is smaller than the deterministic fallback's framing and later turns exceed the leaf target
- **THEN** hard compaction MUST widen that leaf and commit progress rather than fail on it at every admission.

#### Scenario: Derived bound is fixed
- **WHEN** hard pressure derives its round bound and later rounds run in the same admission epoch
- **THEN** the bound MUST NOT grow, and the structured cannot_fit MUST report it as max_rounds.

#### Scenario: Boundary compaction
- **WHEN** a host opts into soft_on_turn_boundary and a turn completes
- **THEN** runtime SHALL attempt soft compaction after TurnCompleted through existing idle admission and persistence.

### Requirement: Neutral Provider Summary Mechanism
A feature-gated provider summary adapter SHALL use host-supplied instructions,
compact tool-aware rendering, the authoritative planner for every request,
LCM purpose usage accounting, cancellation and deadlines. Oversized leaves
SHALL use bounded map-reduce without exceeding the summarizer input budget.
Every provider call MUST have its own deadline (default 60 s) within an
operation budget scaled by the planned calls and capped (default 10 min); both
MUST be configurable. Cancellation MUST be scoped to one summary operation.
The attempt purpose MUST be idle compaction only for idle-boundary summaries.

#### Scenario: Oversized tool leaf
- **WHEN** a source leaf exceeds the summarizer window
- **THEN** multiple measured map calls and bounded reduction MUST preserve tool evidence and aggregate usage.

#### Scenario: Per-operation cancellation
- **WHEN** a host cancels the scope of one summary operation
- **THEN** that operation MUST fail without provider I/O, and the next operation MUST run normally.

#### Scenario: Deterministic tool evidence
- **WHEN** model escalation reaches fallback
- **THEN** bounded rendering MUST include tool names, truncated arguments and results.
