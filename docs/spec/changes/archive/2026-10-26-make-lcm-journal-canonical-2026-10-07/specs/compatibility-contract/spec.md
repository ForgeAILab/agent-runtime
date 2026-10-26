## ADDED Requirements

### Requirement: Canonical timeline migration is coordinated and backward-readable

The first U10 release SHALL read supported v3 checkpoints/unversioned snapshots and U9 v4 journal heads/checkpoints for its entire release lifetime. CHECKPOINT_SCHEMA_VERSION SHALL advance from 4 to 5 with strict readers for supported schemas 3/4/5. Existing materialized store methods/fields SHALL remain, while restrictive backend version guards MUST be migrated. Version-5 import MUST preserve exact mid-turn execution without provider/tool/model calls and require validated authorized source evidence. Older readers MUST reject unsupported version-5 source formats rather than silently infer empty history. All supported consumer suites and actual store/schema adapters MUST pass against an immutable landed version or exact revision before a compatible release.

#### Scenario: v3 CallingModel has history missing from its existing timeline

- **WHEN** a capable host imports the validated exact checkpoint
- **THEN** missing source bodies and reference metadata publish in one canonical transaction
- **AND** the same frozen request, signed continuation, usage, deadline and watermarks survive without another external call during migration

#### Scenario: Legacy timeline contains only redacted source

- **WHEN** its bytes cannot match the exact execution source under current authority
- **THEN** canonical import fails closed or the host explicitly chooses a new authorized timeline
- **AND** the runtime does not infer exact content from redaction placeholders or summaries

#### Scenario: Mid-turn import has no prior terminal proof

- **WHEN** the last completed source cutoff cannot be validated
- **THEN** migration preserves all exact durable content and uses a conservative zero completed cutoff until exact continuation commits
- **AND** old ordinary display may remain under its own policy without being used as exact execution authority

#### Scenario: A supported product rejects the new record

- **WHEN** an actual Forge, Smith or Nyx supported adapter cannot accept its configured version-5 path
- **THEN** compatible release eligibility is blocked for coordinated migration
- **AND** optional non-LCM and ephemeral fixtures remain required regression gates
