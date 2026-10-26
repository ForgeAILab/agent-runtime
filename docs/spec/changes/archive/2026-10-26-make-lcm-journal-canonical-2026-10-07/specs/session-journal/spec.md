## ADDED Requirements

### Requirement: Timeline references are a versioned journal source

Version-5 canonical LCM heads/checkpoint envelopes SHALL identify TimelineHistory and its validated source/index/projection roots instead of retaining a second exact journal history body tree. U9 usage, identities, extensions, diagnostics and request-only objects SHALL remain metadata journal state. Generic journal commits MUST NOT bypass the combined canonical source/checkpoint transaction; corruption below a published source root MUST fail closed.

#### Scenario: Canonical body already exists as an LCM entry

- **WHEN** the next checkpoint or frozen request refers to that exact Message
- **THEN** it writes only references and changed metadata
- **AND** counted unique-body writes report no additional exact history copy

#### Scenario: Canonical source object is corrupted

- **WHEN** a published source-index digest fails validation
- **THEN** recovery requires verified host repair and performs no provider/tool I/O
- **AND** it does not select an older snapshot or reconstruct raw history from summaries

### Requirement: Redacted projection is independent of canonical exact content

SessionStore SHALL retain ordinary redaction policy and its proven terminal boundary; CheckpointStore SHALL retain exact execution authority over references into the protected timeline. Terminal resume and its next provider request/active checkpoint MUST preserve the selected ordinary redacted execution view; exact source integrity and saved execution-view roots SHALL remain distinct. Ordinary reads MUST NOT implicitly fetch exact timeline values, and different exact/projection hashes MUST NOT bypass existing boundary or extension validation. Digests, grants, credentials and source bodies MUST remain outside default diagnostic output.

#### Scenario: Stored terminal history contains a redacted credential literal

- **WHEN** ordinary session history is loaded after canonical migration
- **THEN** it exposes the same host-policy redaction
- **AND** only authorized protected execution recovery can resolve required exact continuation

#### Scenario: Redacted terminal prefix is used for a new turn

- **WHEN** a terminal resume selects ordinary history with a removed credential literal and the session accepts new input
- **THEN** the next provider request and active checkpoint retain that selected execution view while source integrity uses the protected source stream
- **AND** no raw literal is restored solely because the canonical timeline contains it
