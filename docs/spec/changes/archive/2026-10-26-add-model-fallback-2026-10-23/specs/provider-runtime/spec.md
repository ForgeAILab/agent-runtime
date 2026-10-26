## ADDED Requirements
### Requirement: Safe request-level model fallback
The shared runtime SHALL apply ordered, preflighted fallback bindings before meaningful response progress on typed quota exhaustion, exhausted transient failures, or a configured first-response budget. It SHALL replan the pending request under the new model's limits without replaying completed tools or resetting turn limits. Unknown replay safety and provider-hosted side effects MUST prevent unsafe automatic replay.

#### Scenario: Startup budget spans retries and empty streams
- **WHEN** a candidate has not produced meaningful progress before its startup budget expires
- **THEN** its attempt is cancelled and retired before a safe next binding is dispatched
- **AND** empty deltas, headers, usage observations, and retries cannot restart the budget

#### Scenario: Output already started
- **WHEN** meaningful text, reasoning, or tool activity starts
- **THEN** the startup timer is disarmed and later stalls cannot trigger cross-model replay

#### Scenario: Request continuation
- **WHEN** a provider request following completed tools advances to a compatible model
- **THEN** canonical tool results are preserved and each completed tool executes only once
