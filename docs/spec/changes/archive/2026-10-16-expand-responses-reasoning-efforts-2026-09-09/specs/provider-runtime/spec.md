## ADDED Requirements

### Requirement: Model-advertised Responses reasoning effort

The Responses adapter SHALL accept a bounded, non-empty named reasoning
effort supplied from a host-resolved model catalog and SHALL serialize that
value unchanged. The adapter MUST reject an empty or oversized effort before
credential or network I/O and MUST NOT impose one provider-wide enumeration
on model-specific effort vocabularies.

#### Scenario: Forward an extended Codex reasoning effort

- **GIVEN** the host resolved `xhigh`, `max`, or `ultra` as a supported effort
  for the selected model
- **WHEN** the runtime builds the Responses request
- **THEN** the adapter serializes the selected effort unchanged
- **AND** does not reject it through a legacy three-value allowlist

#### Scenario: Reject an invalid effort label

- **GIVEN** a Responses request contains an empty or oversized reasoning
  effort label
- **WHEN** the adapter validates the request
- **THEN** it returns a structured bad-request error before credential or
  network I/O
