## ADDED Requirements

### Requirement: Canonical tool names at provider boundaries

Anthropic Messages, OpenAI-compatible Chat Completions and Responses adapters SHALL use one shared deterministic bidirectional tool-name mapping with an ASCII
letter/digit/underscore/hyphen wire alphabet and a 1–64 character bound. They
MUST translate definitions, named choices and historical calls/results to the
same wire names and translate model call names back before emitting events.
Runtime requests, history, events and manifests MUST retain canonical identities.

#### Scenario: Dotted and over-long names round-trip

- **WHEN** a request registers a dotted or over-long canonical tool name
- **THEN** its definition and named choice use a bounded provider-safe name
- **AND** a model call using that name emits the canonical name
- **AND** replay of its call/result uses the same wire identity

#### Scenario: Valid names are unchanged

- **WHEN** a tool name already meets the wire policy
- **THEN** its wire name is identical to the canonical name

#### Scenario: Collision disambiguation is deterministic

- **WHEN** distinct canonical names sanitize or truncate to the same prefix
- **THEN** deterministic suffixes distinguish them, including collisions with
  already valid registered names or generated suffixes
- **AND** reordering the same tool set produces the same mapping
- **AND** growing history does not change registered tool wire names

#### Scenario: Unknown model tool remains unknown

- **WHEN** a model emits a wire name absent from the mapping
- **THEN** the adapter passes it through unchanged for runtime unknown-tool handling

#### Scenario: Fragmented OpenAI tool name

- **WHEN** an OpenAI-compatible model streams a wire name in several fragments
- **THEN** the adapter emits its complete canonical name before the tool finish
- **AND** no wire-name fragments escape as runtime tool names
