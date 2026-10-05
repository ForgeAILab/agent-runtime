---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
---

# PROPOSED: Declare multiple tool effect resources (U.2)

## Why

Read-only investigation confirms `PreparedToolCall::new` and its stored
`resource` accept one `SecurityResource`; `AuthorizationRequest` also carries
one resource for its entire requested permission set. In the executor,
`verify_prepared` requires every host read/write effect to match an `Other`
resource kind; `enforce_workspace` requires a `Filesystem` resource for each
workspace write effect. One resource cannot satisfy both variants. A shell
invocation needing both workspace-write and host-filesystem authority therefore
cannot express and pass validation for both in one prepared call.

Static `ToolSpec` permission bounds can already include both permissions. The
limit is exact per-call preparation/authorization, not the descriptor. The
legacy `ToolEffects::authorization_request` mapping chooses one resource using
`get_or_insert_with`, so it also cannot solve mixed-resource effects. Multiple
workspace scopes can share a filesystem ancestor today; multiple host kinds or
external services have similar limitations, and read-only resource validation
must be reviewed alongside writes.

## What Changes

- **PROPOSED ONLY — NOT APPROVED, NOT IMPLEMENTED.** U.1 approval does not
  authorize U.2. Obtain separate owner/consumer approval before any API change.
- Recommend permission-bound resource claims, each carrying its own required
  `PermissionSet` and concrete `SecurityResource`, on one immutable prepared action.
- Bind all claims to the preparation fingerprint, authorization/grants, approval
  display, protected recovery, and invocation validation; require every claim
  to pass before side effects.
- Preserve the distinction between workspace and same-user host authority.

## Impact

- Affected specs: `tool-execution`; follow-up approval must also cover security
  check/grant, protected checkpoint, event redaction, and compatibility contracts.
- Affected code if approved: core tool/security/grant/check-set contracts,
  runtime executor/approval/recovery, testkit host checks and consumer adapters.
- Rust signatures, public struct literals, accessors, check implementations,
  grant binding, and prepared fingerprints may change. See design.md for wire
  migration, downgrade, and alternatives. No U.2 code or wire changes land here.
- Consumer gates: Smith, Nyx, and Open Forge must compile their tool and security
  adapters and prove all-claims authorization, edited approval, and exact recovery.
