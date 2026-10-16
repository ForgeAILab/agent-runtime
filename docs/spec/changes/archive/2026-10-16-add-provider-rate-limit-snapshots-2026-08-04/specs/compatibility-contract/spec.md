## ADDED Requirements

### Requirement: Rate-limit observation additivity

Adding rate-limit observation SHALL NOT break an existing transport, adapter,
or serialized record. The byte-stream transport method remains required and
unchanged; the header-observing method is defaulted. New stream and runtime
event variants and the new provider error kind SHALL serialize as additive
tagged variants, and no existing field SHALL be removed or repurposed.

#### Scenario: An existing transport keeps compiling

- **GIVEN** a transport implementing only the byte-stream method
- **WHEN** the crate is rebuilt against this change
- **THEN** it compiles unchanged
- **AND** attempts it serves produce no snapshot rather than failing

#### Scenario: An older record still deserializes

- **GIVEN** a serialized provider error written before this change
- **WHEN** it is deserialized after it
- **THEN** it deserializes successfully
- **AND** its reset time reads as absent
