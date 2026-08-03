## MODIFIED Requirements

### Requirement: Semantic summarization responds to context pressure

The semantic summary coordinator SHALL decide to summarize from observed input
usage measured against a configured input budget, not from a count of completed
turns. A configured minimum completed-turn count SHALL remain as an eligibility
floor, and reaching it MUST NOT by itself cause summarization. Growth MUST be
measured relative to the session's opening input cost so that a larger stable
prefix does not advance the trigger. Only provider-attempt usage MAY inform the
decision; the coordinator's own summary spend MUST be excluded.

#### Scenario: A long session of small turns is not summarized
- **GIVEN** a session past the minimum turn floor
- **AND** input usage well below the configured share of the budget
- **WHEN** a turn commits
- **THEN** no summary is produced
- **AND** no summary model call is made

#### Scenario: A single large tool result triggers summarization
- **GIVEN** a session past the minimum turn floor
- **WHEN** one turn's input usage crosses the configured share of the budget
- **THEN** a summary is produced at that commit

#### Scenario: The floor protects a young session
- **GIVEN** a session below the minimum completed-turn floor
- **AND** input usage above the configured share of the budget
- **WHEN** a turn commits
- **THEN** no summary is produced

#### Scenario: A larger prefix does not advance the trigger
- **GIVEN** two sessions whose conversation bodies cost identically
- **AND** one begins with a substantially larger opening input cost
- **WHEN** both commit the same number of equivalent turns
- **THEN** neither summarizes before the other

#### Scenario: Summary spend does not feed the trigger
- **GIVEN** a session that has already produced a semantic summary
- **WHEN** the decision is evaluated again
- **THEN** the separately attributed summary usage is excluded from it

#### Scenario: A policy without an input budget is rejected
- **GIVEN** a policy whose input budget is zero
- **WHEN** it is validated
- **THEN** validation fails
- **AND** the trigger is not silently disabled
