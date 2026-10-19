## MODIFIED Requirements

### Requirement: Complete preflight accounting

Every planned request SHALL carry a complete budget report: token counts per
category, the total counted input tokens, the enforced input budget, and the
output/reasoning reserves, produced by a versioned sizer whose confidence is
recorded. The planning event SHALL report the counted consumption
(`input_tokens`) and the enforced budget (`input_budget_tokens`) as distinct
values that match the budget report. The default offline sizer SHALL count a
reasoning part's opaque signature, at its own configurable
characters-per-token ratio, because providers bill the continuation state
they hand back.

#### Scenario: Telemetry separates consumption from the enforced budget

- **GIVEN** a plan whose counted input is below its enforced budget
- **WHEN** the planning event is emitted
- **THEN** `input_tokens` equals the counted consumption
- **AND** `input_budget_tokens` equals the enforced budget rather than the
  consumption

#### Scenario: A replayed search step is counted

- **GIVEN** history holding a Gemini search step stored as a redacted
  reasoning part with a 46,704-character signature
- **WHEN** the default sizer prices that part
- **THEN** it counts the signature at six characters per token, 7,784 tokens,
  rather than zero
