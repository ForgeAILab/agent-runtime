## ADDED Requirements

### Requirement: Responses account headers bind to the acquired lease

An opt-in Responses profile SHALL project the existing lease account into one
validated host-selected header at the transport boundary, paired with the same
lease's token. Values MUST NOT come from cached adapter configuration or be
persisted/diagnosed. Auth/control header names, invalid values, and conflicting
static headers MUST be rejected; a missing required account MUST prevent POST.

#### Scenario: Source changes account on replacement

- **GIVEN** attempt one acquires token/account A and recovery acquires token/account B
- **WHEN** the two visible requests are sent
- **THEN** each request contains only its own token/account pair
- **AND** no A account is retained on the B request

#### Scenario: Static account header conflicts

- **GIVEN** lease account projection and a case-insensitive static header with the same name
- **WHEN** provider configuration is validated
- **THEN** it fails before acquisition, including a header set by `with_chatgpt_account`

#### Scenario: Required account is missing or invalid

- **WHEN** the lease lacks a required account or its value contains invalid HTTP characters
- **THEN** the adapter returns a fixed error without printing the account and sends no POST

#### Scenario: Account projection is optional and absent

- **GIVEN** an optional account projection and a lease without an account
- **WHEN** the request is sent
- **THEN** the account header is omitted with no static fallback
