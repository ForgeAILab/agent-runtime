## MODIFIED Requirements

### Requirement: Complete preflight accounting

Every planned request SHALL carry a complete budget report: token counts per
category, the total counted input tokens, the enforced input budget, and the
output/reasoning reserves, produced by a versioned sizer whose confidence is
recorded. The planning event SHALL report the counted consumption
(`input_tokens`) and the enforced budget (`input_budget_tokens`) as distinct
values that match the budget report.

#### Scenario: Telemetry separates consumption from the enforced budget

- **GIVEN** a plan whose counted input is below its enforced budget
- **WHEN** the planning event is emitted
- **THEN** `input_tokens` equals the counted consumption
- **AND** `input_budget_tokens` equals the enforced budget rather than the
  consumption

### Requirement: Semantic context compaction

Compaction SHALL reclaim tokens in cost order, treating retained reasoning
from turns before the last user message as the cheapest reclaim: a first
stage SHALL remove such reasoning parts from message fragments before any
fragment eviction, truncation, elision, or summarization runs, while
reasoning at or after the last user message MUST be preserved for the
provider's same-turn continuation contract. Bounded truncation SHALL treat
reasoning parts like text parts.

#### Scenario: Prior-turn reasoning is reclaimed first

- **GIVEN** an over-budget history whose older assistant messages retain
  reasoning parts
- **WHEN** compaction runs
- **THEN** the prior-turn reasoning parts are removed before other content is
  evicted or summarized
- **AND** the containing messages and their other parts survive

#### Scenario: Current-turn reasoning survives compaction

- **GIVEN** an over-budget history whose assistant reasoning follows the last
  user message
- **WHEN** compaction runs
- **THEN** that reasoning is preserved
