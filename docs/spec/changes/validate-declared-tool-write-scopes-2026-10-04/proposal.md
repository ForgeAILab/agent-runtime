---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-05T00:18:20Z
---

# Proposal: Validate declared tool write scopes (U.1)

## Why

A host declared `ToolEffects::read_only().with_write("/")` as unlimited write
access. Registration and runtime construction succeeded, but every invocation
failed with a model-visible workspace violation before authorization or approval.
Hosts need an actionable configuration failure before starting a session.

The repository owner approved U.1 for implementation on 2026-10-04.

## What Changes

- **BREAKING**: `RuntimeBuilder::build()` checks workspace write scopes on the
  frozen `ToolSpec` of every sealed tool against the configured `Workspace`.
- Share the executor's `Workspace::contains` scope check; keep all invocation
  checks, including argument-dependent scopes and approved outside-mount handling.
- Return `RuntimeError` with `ErrorKind::Config` and typed
  `FailureClass::InvalidToolWriteScope(Box<ToolWriteScopeViolation>)` evidence.
  The public violation details carry `tool`, `scope`, and `workspace` strings;
  boxing keeps the existing common error size unchanged.
- Validate only when a workspace is explicitly configured. Standalone registry
  registration and runtimes without a workspace remain supported; invocation
  still uses `DenyAllWorkspace` when no workspace was supplied.
- Document the contained workspace root as the workspace-wide declaration,
  subject to the host's actual `contains` contract. No universal wildcard exists.

## Impact

- Affected specs: `tool-execution`.
- Affected code: core tool rustdoc/error classification; runtime builder and
  shared scope validation used by the executor; focused runtime tests.
- All tools are checked, including tools composed through live ability routing.
  Host/external mutation conflict scopes are not workspace write scopes.
- Absolute paths remain valid when the workspace contains them. A host should
  replace `/` with its contained workspace root (for example `/ws`) or a narrower
  contained scope; use host effects for authority outside that workspace.
- No builder signature, dependency, event vocabulary, or checkpoint schema changes.
  The non-exhaustive failure class gains an additive JSON reason; older readers
  fall back to `Unclassified`. This host-visible configuration evidence contains
  declared tool/scope/root strings and is not emitted as runtime telemetry.
- U.2 remains a separate proposal and is not implemented by this change.
