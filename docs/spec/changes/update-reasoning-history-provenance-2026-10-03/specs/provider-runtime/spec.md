## ADDED Requirements

### Requirement: Reasoning history is replayed only to its producer

The runtime SHALL record on each reasoning part it commits to canonical
history the provider and model that produced it, and SHALL omit from a
provider request every reasoning part whose recorded producer differs from
that request's provider or model. Canonical history MUST keep omitted parts.
A reasoning part with no recorded producer SHALL be sent as it was before
this requirement. A request whose reasoning parts were all produced by its
own provider and model MUST be byte-identical to the request built without
this requirement.

#### Scenario: Continuing on another provider

- **GIVEN** a session whose earlier turn holds signed reasoning from
  provider A
- **WHEN** the next turn is sent to provider B
- **THEN** the request to B carries none of A's reasoning parts or signatures
- **AND** the earlier turn's text and tool calls are still in the request

#### Scenario: Same provider and model

- **GIVEN** a session whose reasoning was all produced by the current
  provider and model
- **WHEN** the next request is built
- **THEN** it is byte-identical to the request built before this requirement

#### Scenario: Switching back

- **GIVEN** reasoning from provider A was omitted while the session ran on B
- **WHEN** the session returns to A with the same model
- **THEN** A's reasoning is sent again with its signatures

#### Scenario: Reasoning recorded before provenance existed

- **GIVEN** a reasoning part with no recorded producer
- **WHEN** any request is built
- **THEN** the part is sent exactly as before
