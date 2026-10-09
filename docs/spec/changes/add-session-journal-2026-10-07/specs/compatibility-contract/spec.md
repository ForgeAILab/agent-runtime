## ADDED Requirements

### Requirement: Journal migration is backward-readable and coordinated

The first released U9 version SHALL read valid v3 checkpoints and v3-era unversioned snapshots for its entire release lifetime, including unfinished turns. Native version-4 import SHALL preserve exact execution/recovery semantics and keep source-compatible existing store methods. A coordinated pre-1.0 persisted-contract release MUST pass Smith, Nyx and Open Forge gates against an immutable landed tag or exact revision; readable JSON alone MUST NOT imply old-binary execution compatibility.

#### Scenario: Host adopts no native journal

- **WHEN** a host upgrades to the release containing only the journal contracts and readers
- **THEN** every checkpoint and snapshot it writes is byte-identical to what the previous release wrote, with checkpoint schema 3
- **AND** the previous release can read them, so the host can roll back without an export

#### Scenario: Stored v3 session is mid-turn

- **WHEN** the new runtime imports a supported v3 CallingModel, pending approval or committed outcome boundary
- **THEN** it publishes an exact reference boundary under an import writer fence without provider/tool I/O
- **AND** crash retry resumes the authoritative legacy or native boundary without repeating committed work

#### Scenario: Consumer has no durable store

- **WHEN** Nyx starts ephemeral history with journals configured
- **THEN** the seeded history and subscribe-before-send behavior remain
- **AND** no journal discovery, load, write, claim or durable collection occurs

#### Scenario: One actual consumer store rejects schema 4

- **WHEN** a release candidate passes neutral runtime tests but a supported product store rejects its schema
- **THEN** compatible release eligibility is blocked pending coordinated store migration
- **AND** consumers do not pin a moving branch or unlanded local override
