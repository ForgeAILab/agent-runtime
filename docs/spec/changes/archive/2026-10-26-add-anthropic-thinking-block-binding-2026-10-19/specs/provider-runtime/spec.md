## ADDED Requirements

### Requirement: Anthropic thinking block-binding control

The Anthropic adapter SHALL let a host set a thinking block-binding behavior,
`error` or `drop_block`, which is unset by default. When it is set and a
request carries a thinking configuration, the adapter SHALL send it as
`thinking.block_binding.prefix_mismatch_behavior` and SHALL include
`thinking-binding-controls-2026-08-01` in the request's `anthropic-beta`
header. The adapter SHALL send at most one `anthropic-beta` header, merging
every beta value it adds or the host supplies, without duplicates. When the
behavior is unset, or the request carries no thinking configuration, the
request MUST be byte-identical to one built without this control.

#### Scenario: Unset behavior changes nothing
- **WHEN** a host builds a request with thinking configured and no block-binding behavior
- **THEN** the request body and headers are identical to those built before this control existed

#### Scenario: Drop-block with adaptive thinking
- **WHEN** a host sets `drop_block` and the request uses adaptive thinking with an effort
- **THEN** the body carries `thinking: {"type": "adaptive", "block_binding": {"prefix_mismatch_behavior": "drop_block"}}`
- **AND** the `anthropic-beta` header contains `thinking-binding-controls-2026-08-01`

#### Scenario: No thinking configuration
- **WHEN** a host sets `drop_block` and the request carries no thinking configuration
- **THEN** the body has no `thinking` field and no binding beta is added

#### Scenario: Beta values are merged
- **WHEN** the host also supplies an `anthropic-beta` extra header and interleaved thinking is enabled
- **THEN** the request carries a single `anthropic-beta` header listing each beta value once
