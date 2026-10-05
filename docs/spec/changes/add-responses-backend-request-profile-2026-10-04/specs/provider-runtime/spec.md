## ADDED Requirements

### Requirement: Responses request profiles are opt-in and bounded

Responses SHALL accept an explicitly host-selected bounded declarative request
profile, preserving the standard encoder when absent. Profiles MUST NOT be
inferred from endpoint names, enable stateful/hosted behavior, alter streaming
normalization, or bypass capability, byte/content, or canonical budget checks.
Profile text and diagnostics MUST use existing redaction mechanisms.

#### Scenario: Standard endpoint remains unchanged

- **GIVEN** no custom profile
- **WHEN** a valid OpenAI/xAI request is encoded
- **THEN** current input/tuning/reasoning behavior remains unchanged
- **AND** vendor overrides and reasoning token budgets remain rejected

#### Scenario: Endpoint alone cannot activate policy

- **GIVEN** the ChatGPT preset but no explicitly selected request profile
- **WHEN** the request is built
- **THEN** no new fallback prompt, synthetic turn, omission mask, or reasoning default is inferred

#### Scenario: Profile attempts stateful behavior

- **WHEN** configuration or request attempts provider storage, continuation IDs, background execution, or hosted tools
- **THEN** the adapter rejects it before credential or transport I/O

### Requirement: Responses profiles project counted instructions and input

A configured profile SHALL support ordered system-text projection into top-level
`instructions`, excluding the projected system items from `input`. It SHALL
support host-supplied fallback instructions without nonempty system text and a
synthetic user turn for system-only input. Added context MUST be counted before
plan admission, and non-system continuation order MUST remain intact.

#### Scenario: Multiple system messages are projected

- **GIVEN** top-level instruction mode and multiple system text messages
- **WHEN** the payload is built
- **THEN** `instructions` contains their text once in canonical order
- **AND** `input` retains the non-system messages without duplicated system items

#### Scenario: Conversation lacks system content

- **GIVEN** a host fallback and no nonempty system text
- **WHEN** a profile-aware request is planned and encoded
- **THEN** the counted fallback becomes top-level `instructions`

#### Scenario: Conversation is system-only

- **GIVEN** only system content and configured host synthetic user text
- **WHEN** the counted profile projection is encoded
- **THEN** `instructions` carries the system text and `input` contains one synthetic user turn
- **AND** canonical durable conversation history is unchanged

### Requirement: Responses profiles omit rejected tuning fields explicitly

The profile SHALL expose a finite tuning omission mask. Selected fields MUST
be absent from the wire object, while model limits, output/reasoning reserves,
and cancellation/deadline constraints MUST remain authoritative. Wire omission
MUST NOT be represented as proof of a backend generation cap.

#### Scenario: Backend rejects temperature and output-token controls

- **GIVEN** canonical temperature and max-output-token settings with both fields masked
- **WHEN** the profile payload is encoded
- **THEN** neither wire field exists, including as null
- **AND** canonical budget/model validation still applies

### Requirement: Responses profiles resolve host reasoning policy deterministically

Profiles SHALL resolve effort in the order explicit named effort, configured
token-budget mapping, then configured default, and SHALL support configured
reasoning summary such as `auto`. Mappings MUST be bounded and validated;
supported effort labels and thresholds remain host/model policy. Unsupported or
unmapped budgets MUST fail before source or network I/O without a global label
allowlist or silently changed accounting.

#### Scenario: No reasoning effort was selected

- **GIVEN** a reasoning-capable model and host profile default effort/summary
- **WHEN** no explicit effort or token budget is supplied
- **THEN** the payload sends that effort and configured `summary: "auto"`

#### Scenario: Token budget maps to named effort

- **GIVEN** a valid host threshold table and an admitted reasoning token budget
- **WHEN** no explicit effort is present
- **THEN** the payload sends the table-selected bounded effort
- **AND** reasoning budget accounting is preserved

#### Scenario: Explicit named effort takes precedence

- **GIVEN** explicit model-supported effort `xhigh`, a budget, and profile defaults
- **WHEN** the payload is built
- **THEN** `xhigh` is preserved without replacement by mapping or default

#### Scenario: Invalid mapping or zero/unmapped budget

- **WHEN** thresholds/labels are invalid or a zero/unmapped budget is supplied
- **THEN** a fixed compatibility error occurs before credential or provider I/O
