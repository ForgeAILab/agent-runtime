## Context and Status

PROPOSED only. No implementation is approved. Source evidence:
`agent-runtime-core/src/tool.rs`: `PreparedToolCall` fields, `new`, `resource`,
`prepared_fingerprint`, and `ToolEffects::authorization_request`;
`agent-runtime-core/src/security.rs`: `AuthorizationRequest`;
`agent-runtime/src/tool/executor.rs`: `verify_prepared`, `enforce_workspace`,
`escapes_workspace`, authorization and edited-approval paths.

## Options

1. **Permission-bound resource claims (recommended).** Introduce a nonempty,
   bounded list of `(PermissionSet, SecurityResource)` claims. Authorize each
   with the existing check-set machinery, then bind the complete grant set to
   one prepared action. Aggregate permissions must remain within the descriptor
   upper bound. Validation must tie each effect to an appropriate claim and
   reject missing, ambiguous, or unused authority. This expresses mixed domains
   without letting host write permission authorize a workspace path.
2. **Flat resource list plus one permission set.** Simpler constructors, but
   loses which permission belongs to which resource. Checking the Cartesian
   product creates spurious denials or excess authority; deriving mappings
   from effect order is fragile. Not recommended.
3. **Composite SecurityResource variant.** Retains a singular field, but every
   contains/check/grant implementation must understand composite semantics;
   still needs per-permission binding. Expands the wire variant vocabulary and
   is easier to accidentally authorize as one opaque resource. Not recommended.
4. **Split shell work into separate tools/calls.** No shared API change, but
   cannot represent one indivisible command with both authority domains and
   invites partial execution. Viable only for host operations that truly split.
5. **Use host-only effects for the entire shell.** Supported when the actual
   operation is entirely a host action and the host authorizes it explicitly.
   It does not represent simultaneous workspace capability enforcement and
   must not serve as an automatic fallback for mixed authority.

## Compatibility and Wire/Schema Impact

- Prefer a new claims constructor/accessor while retaining the singular
  constructor as a one-claim adapter where semantics are exact. A singular
  `resource()` accessor has no correct answer for multiple claims; either
  deprecate it with a fallible replacement or coordinate a source break.
- Changing public `AuthorizationRequest` fields breaks struct literals and
  resource-based host checks. Prefer evaluating separate singular requests
  internally first, if the grant/check-set design can bind all results safely.
- `PreparedToolCall` is serialized in protected execution checkpoints. Adding
  claims changes canonical fingerprint bytes; do not simply serde-default a
  list and recompute legacy fingerprints. Version prepared authority and
  migrate only verified old bindings, or reject unsupported checkpoints.
- Review `InvocationGrant`/grant signatures and containment for complete claim
  coverage. New readers may accept legacy one-claim wire actions after exact
  verification; old readers must reject multi-claim actions rather than select
  one claim. A coordinated protected checkpoint schema/version gate is likely.
- Provider tool input schemas need not change: authority is host preparation
  data. No model/provider wire change is needed. Event vocabulary need not
  change unless metadata-only claim evidence is required; raw resources must
  stay out of telemetry. Freeze exact version choices in the approved design.
- Approval display must describe the whole action, and outside-workspace
  approval requirements must consider every filesystem claim. Cancellation,
  deadlines, edits, and crash recovery apply to the entire action; no partial
  grants or approvals permit partial invocation.

## Recommendation and Open Questions

Choose option 1 after a focused review of grants/check-set and checkpoint
versioning. Keep U.1 independently releasable. Before approval decide list
bounds/deduplication, effect-to-claim mapping, outside-workspace approval rules,
legacy fingerprint migration, and the source/wire compatibility window with all
three consumers. Static mixed permission upper bounds alone do not require
multiple-resource authorization until preparation actually exercises them.
