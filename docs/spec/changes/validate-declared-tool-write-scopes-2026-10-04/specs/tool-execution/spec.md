## ADDED Requirements

### Requirement: Configured workspace validates static tool write scopes at build

`RuntimeBuilder::build()` SHALL validate every workspace write scope in the
sealed tool registry's frozen specifications when a workspace is configured.
It MUST use the same `Workspace::contains` scope check as invocation and return
host-visible `RuntimeError` with `ErrorKind::Config` and typed
`FailureClass::InvalidToolWriteScope` carrying tool name, rejected scope, and
workspace root before a session can start. It SHALL NOT validate host or external
mutation conflict scopes as workspace paths.

#### Scenario: Build rejects a statically declared scope outside the workspace

- **GIVEN** a tool declares `/` and a configured workspace rooted at `/ws` rejects it
- **WHEN** the runtime builds its sealed tool registry
- **THEN** build fails with the typed error naming the tool and `/`
- **AND** no invocation, authorization, approval, or model-visible tool result occurs

#### Scenario: Workspace root and ordinary in-workspace scopes are accepted

- **WHEN** a configured workspace contains its root and a declared child scope
- **THEN** both scopes pass build validation, including absolute contained paths
- **AND** another workspace that rejects those same paths rejects the build

#### Scenario: Runtime with no workspace is unaffected

- **WHEN** a host builds a runtime without configuring a workspace
- **THEN** static write scopes do not introduce a build failure
- **AND** standalone registration before any workspace exists remains supported
- **AND** the invocation default remains `DenyAllWorkspace`

#### Scenario: Invocation rejects an argument-dependent scope

- **GIVEN** build accepted the static declaration
- **WHEN** preparation derives a write scope the session workspace rejects
- **THEN** invocation retains its existing workspace rejection before authorization or approval
- **AND** approved outside-mount resource validation retains its existing behavior
