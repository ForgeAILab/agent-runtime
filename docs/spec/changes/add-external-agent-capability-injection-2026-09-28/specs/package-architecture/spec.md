## ADDED Requirements

### Requirement: Opt-in CLI agent adapters package

The workspace SHALL provide an opt-in `agent-runtime-agent-cli` package that
implements `ExternalAgentBackend` for named CLIs, one cargo feature per CLI
(`claude`, `codex`). It MUST NOT be a dependency of `agent-runtime` by default,
and each adapter MUST declare the CLI version range it supports and verify it
with an explicit preflight.

#### Scenario: Default graph unchanged

- **WHEN** a consumer depends on `agent-runtime` with default features
- **THEN** `agent-runtime-agent-cli` and its dependencies are not built

#### Scenario: Unsupported CLI version

- **GIVEN** an installed CLI outside the adapter's supported range
- **WHEN** the adapter's preflight runs
- **THEN** it reports the version mismatch and the backend is not used
