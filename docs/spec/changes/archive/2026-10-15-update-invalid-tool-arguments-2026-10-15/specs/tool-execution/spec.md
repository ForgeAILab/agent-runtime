## ADDED Requirements

### Requirement: Schema-invalid tool calls return correctable tool errors

The runtime SHALL treat a model tool call whose arguments parse as JSON but
violate the registered tool's input schema as a tool-level failure, not a
provider failure. It SHALL commit the call with a tool-error result naming
the tool and the violation, SHALL NOT prepare, approve, or invoke it, and
SHALL continue the turn so the model can issue a corrected call. Unparseable
arguments and finish-reason mismatches SHALL remain provider stream errors.

#### Scenario: Missing required argument

- **GIVEN** a registered tool whose schema requires `session`
- **WHEN** the model calls it without `session`
- **THEN** history holds the call and an error result stating that `session`
  is a required property
- **AND** no approval is requested and the tool is not invoked
- **AND** the next model request includes that error result
- **AND** observers receive the call as requested and then completed with that
  error, as for an unknown tool, so live and resumed transcripts agree

#### Scenario: Corrected call succeeds

- **GIVEN** the error result above
- **WHEN** the model calls the tool again with `session`
- **THEN** the call is prepared and invoked normally and the turn completes
