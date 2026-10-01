## ADDED Requirements

### Requirement: Bounded manifests preserve readers and require retention gates

Current readers SHALL accept legacy snapshots with absent or unbounded
manifests and legacy checkpoints containing manifest lists. Existing public
manifest vectors, store traits, and checkpoint schema/transition versions MUST
remain supported. All consumer gates MUST prove equivalent execution recovery
and explicitly accept recent-window retention; readable serde alone MUST NOT
be presented as old-binary execution or lifetime archival compatibility.

#### Scenario: Legacy snapshot omitted manifests

- **WHEN** a current reader loads the snapshot
- **THEN** manifests default to empty and existing history, usage, identities, and extensions remain readable
- **AND** no missing manifest is reconstructed or inferred as a missing turn

#### Scenario: Smith reads snapshot manifests directly

- **WHEN** Smith's fixture inspects snapshot.manifests after an over-window run
- **THEN** its existing Vec surface still compiles and yields the same ordered suffix as recent_manifests
- **AND** the compatible release is blocked if its required archival/retention behavior is not satisfied

#### Scenario: Legacy binary cannot overlay a new checkpoint

- **WHEN** downgrade testing shows an older binary rejects a manifest-free checkpoint paired with retained ordinary diagnostics
- **THEN** the release documents that execution downgrade limitation
- **AND** supported consumers pin the gated candidate revision without relaxing checkpoint validation
