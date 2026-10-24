## MODIFIED Requirements

### Requirement: Canonical tool names at provider boundaries

Anthropic Messages, OpenAI-compatible Chat Completions and Responses adapters SHALL use one shared deterministic tool-name mapping with an ASCII
letter/digit/underscore/hyphen wire alphabet and a 1–64 byte bound. The wire
name SHALL be the canonical name with every byte outside that alphabet
replaced by `_`, truncated to 64 bytes, and MUST NOT carry a generated suffix.
Adapters MUST translate definitions, named choices and historical
calls/results to the same wire names and translate model call names back
before emitting events. Runtime requests, history, events and manifests MUST
retain canonical identities. An adapter MUST reject a request whose tools share
a wire name.

#### Scenario: Dotted and over-long names round-trip

- **WHEN** a request registers a dotted or over-long canonical tool name
- **THEN** its definition and named choice use the replaced and truncated name
- **AND** a model call using that name emits the canonical name
- **AND** replay of its call/result uses the same wire identity

#### Scenario: Valid names are unchanged

- **WHEN** a tool name already meets the wire policy
- **THEN** its wire name is identical to the canonical name

#### Scenario: Colliding tools are rejected

- **WHEN** two tools in one request map to the same wire name
- **THEN** the adapter rejects the request before any I/O, naming both tools
- **AND** growing history never changes a registered tool's wire name

#### Scenario: Model output may use the canonical name

- **WHEN** a model emits a registered tool's canonical name instead of its wire
  name
- **THEN** the adapter emits that canonical name unchanged

#### Scenario: Unknown model tool remains unknown

- **WHEN** a model emits a wire name absent from the mapping
- **THEN** the adapter passes it through unchanged for runtime unknown-tool handling

#### Scenario: Fragmented OpenAI tool name

- **WHEN** an OpenAI-compatible model streams a wire name in several fragments
- **THEN** the adapter emits its complete canonical name before the tool finish
- **AND** no wire-name fragments escape as runtime tool names
