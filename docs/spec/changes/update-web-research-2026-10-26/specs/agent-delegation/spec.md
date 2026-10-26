## ADDED Requirements

### Requirement: Host-reviewed tools stay in read-only child views

A child factory SHALL be able to name, on the builder it returns, the tools it
reviewed for a read-only child view. When a child's tool scope is read-only or
its workspace is the read-only view, the coordinator SHALL keep a tool that
only reads the filesystem, clock or random source, and SHALL also keep a named
tool whose permission upper bound includes none of `fs.write`, `fs.create`,
`fs.delete`, `host.fs.write`, `external.write`, or `process.spawn`. A named tool
whose bound includes any of those MUST NOT be kept. Delegation-management
tools MUST NOT reach a child view even when named.

#### Scenario: A reviewed network read reaches a read-only child

- **GIVEN** a factory that names `fetch` (bound `net.http`, `data.egress`)
  and `artifact.read` (a host-defined permission)
- **WHEN** a child is spawned with a read-only tool scope, and another with
  scope `all` in the read-only workspace view
- **THEN** both children's views hold `fetch`, `artifact.read` and the
  filesystem read tools

#### Scenario: An unnamed network tool stays out

- **GIVEN** the same factory and a tool `unreviewed_fetch` it does not name
- **WHEN** a read-only child is spawned
- **THEN** the child's view does not hold `unreviewed_fetch`

#### Scenario: A named tool that writes stays out

- **GIVEN** a factory that names tools bound by `fs.create`,
  `process.spawn`, and a workspace write, and the host's delegation tool
- **WHEN** a read-only child is spawned
- **THEN** the child's view holds none of them
