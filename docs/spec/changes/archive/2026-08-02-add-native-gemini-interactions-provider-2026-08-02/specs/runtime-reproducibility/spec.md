## ADDED Requirements

### Requirement: Reproducible provider continuation content

Canonical persistence and equivalent replay SHALL retain bounded
provider-required continuation content, including signed reasoning blocks,
without rendering opaque signatures or treating them as host presentation
metadata. Missing required continuation MUST fail explicitly rather than
silently producing a non-equivalent provider request.

#### Scenario: Resume signature-only reasoning

- **GIVEN** a completed provider step contains redacted reasoning with an
  opaque signature and no summary text
- **WHEN** the session is saved, loaded, and equivalently replayed
- **THEN** the signed reasoning block remains in the same canonical position
- **AND** its signature is available only to provider request reconstruction

#### Scenario: Replay record predates signed continuation

- **GIVEN** an older valid session contains no signed provider continuation
- **WHEN** it is loaded for a provider that does not require signed history
- **THEN** the session remains backward compatible
- **AND** no empty or invented signature is added

#### Scenario: Required continuation cannot be restored

- **GIVEN** an equivalent replay targets a provider whose current-turn history
  requires signed continuation that is absent
- **WHEN** request preparation validates the history
- **THEN** replay fails with a structured incompatibility before provider I/O
- **AND** does not substitute provider-side state or an unsigned request
