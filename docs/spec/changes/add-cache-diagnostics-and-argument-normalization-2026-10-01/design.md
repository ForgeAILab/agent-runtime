---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-03T05:26:44Z
---

# Design: Explain prefix changes and normalize before validation

## Evidence on Main

Verified locally at `main`/HEAD
`b4b421fe8e049709063bac02e602bdeadf296c26`.

| Verified fact | Evidence |
| --- | --- |
| Segment fingerprints carry IDs, class, content hash, and tokens. | `crates/agent-runtime-context/src/cache.rs:50` |
| The comparison uses current/prior stable boundaries and matching IDs/hashes; changed_segments is the entire suffix, not a list of actual causes. | `crates/agent-runtime-context/src/cache.rs:392`, `:402`, `:439` |
| Suppression clears comparable expectations and marks every segment changed. | `crates/agent-runtime-context/src/cache.rs:330` |
| The runtime emits only aggregate prefix metrics. | `crates/agent-runtime/src/agent/driver/provider.rs:633`; `crates/agent-runtime-core/src/event.rs:1042` |
| FragmentId is defined in context and accepts arbitrary strings; it is not inherently redaction-safe. | `crates/agent-runtime-context/src/fragment.rs:20` |
| Both executor entry paths validate before prepare; Tool offers prepare but no earlier hook. | `crates/agent-runtime/src/tool/executor.rs:339`, `:369`, `:689`; `crates/agent-runtime-core/src/tool.rs:1245` |
| Registration compiles/discards a validator; per-call validation recompiles it from the frozen spec. | `crates/agent-runtime/src/tool/registry.rs:42`, `:100`, `:155` |
| Stream-level validate_call checks object representability, not the complete schema. | `crates/agent-runtime/src/tool/registry.rs:182` |
| Edited actions must return through validation and new authorization. | `docs/spec/specs/tool-execution/spec.md:97` |

G2.3/G10 are confirmed, with two corrections: changed_segments cannot simply
be sampled for a culprit, and arbitrary fragment IDs need an explicit safe
diagnostic contract. Full schema validation is in the executor; the earlier
stream check rejects malformed object representations only. Envelope behavior
in Forge's own source is audit input, not an upstream behavior to copy.

## Decisions

### Diagnostic Semantics

Add `CachePlan.first_changed_fragment: Option<FragmentId>` and an accessor.
Compute it while the previous plan is available, before comparison state is
discarded. Add the same optional string-valued ID to `CachePlanChanged`; core
must not depend on context merely to name this value. The string is the
transparent FragmentId wire representation, not a new ID namespace.

Use the same committed predecessor and provider representability gates as
cache expectations. Within the predecessor's declared stable prefix, compare
ordered ID, content hash, and cache class. "First" means plan-segment order
(Instructions before Capabilities), not provider wire byte order. At the
earliest differing position, if the IDs differ and the predecessor ID is
absent from the current plan's leading Stable segments, report that removed
predecessor ID, even when an unchanged neighbour or Ephemeral tail occupies
its old position. If no current item exists, also report the predecessor ID.
Otherwise report the current ID, preserving insertion and reorder attribution.
A Stable-to-Ephemeral/NoCache change at an existing prefix
position is a change even if bytes are equal. A pure appended/promoted tail
outside the predecessor's stable prefix is not an invalidation.

Return None for the first request, an unavailable/unmarked predecessor,
unsupported or suppressed wire boundaries, or unchanged established prefix.
Also return None for a non-fragment identity-partition change (model,
endpoint, tokenizer, adapter, policy, registry/view revision) that cannot be
attributed to an ordered segment. A fragment change that changes the identity
digest still reports its ID if the non-fragment partition is unchanged. Do
not use exact digest equality as the diagnostic comparability test. If both
the non-fragment partition and fragments change, return None rather than
claim a single fragment caused the invalidation. The existing metrics and
identity evidence continue to describe the invalidation.

New telemetry accepts only bounded, redaction-safe IDs under the existing
manifest identity discipline. Because FragmentId itself does not enforce that
discipline, the event projection omits an unsafe/unbounded ID instead of
truncating it or publishing raw host text. Planner-internal FragmentId remains
unchanged. Choose a 256-byte UTF-8 bound for this new field, reject control
characters, and document that hosts must not put sensitive content in IDs;
syntax validation alone cannot prove absence of secrets. The optional
diagnostic never changes plan/cache fingerprints, wire bytes, hit/miss
attribution, breakpoints, baseline commitment, or retry policy.

### Host Hook and Execution Order

Add an object-safe synchronous method:
`Tool::normalize_arguments(Value) -> Result<Value, RuntimeError>`, defaulting
to identity. It is a pure, deterministic, side-effect-free transformation.
It receives no credentials, workspace, authority, or network context. Hosts
choose wrapping/coercion rules per tool; adapters contain no model-family
heuristic. The existing stream representability check remains and this hook
does not repair non-object malformed streams for object-schema tools.

Order for each newly assembled call or approval edit: tool lookup and active
deadline/cancellation check, normalize once, validate the normalized value
against the frozen advertised input_schema, prepare, verify the prepared
action, enforce workspace, authorize, approve, schedule, invoke. Recheck the
deadline/cancellation after the synchronous hook; hosts must keep it bounded.
There is no retry with raw arguments when normalization fails. Unknown tools
retain their canonical error-result behavior.

The pre-hook bounds checks change tool-result wording from
`cancelled before tool preparation` / `deadline elapsed before tool preparation`
to `cancelled before tool normalization` / `deadline elapsed before tool normalization`.
This text change also applies to tools using the default identity hook.

Checkpoint the exact prepared normalized arguments and fingerprint. Resume a
prepared action without re-normalizing it. Approval edits create a new
normalization/validation/preparation cycle and invalidate old grants. Both
executor paths share this order; raw model arguments remain in canonical
model history for replay, while the prepared action is the execution authority.
In turn-machine preparation, opt-in tools' `ReadyToolCall.call` carries the
normalized arguments; these feed `ToolOutputView.call` and the
`LocalActionExecuting` checkpoint's `call`. Those execution-facing values are
distinct from the raw model arguments retained in canonical history. On
recovery they carry the exact checkpointed prepared arguments instead.
Normalization output/errors obey the existing tool redaction contract and do
not enter cache telemetry. The hook does not promise that Tool::prepare cannot
perform its existing deeper canonicalization.

### Validator Ownership

Cache compiled validators privately in the runtime registry alongside the
frozen schema. Do not put jsonschema dependencies into the generic ability
ToolEntry or default-feature registry kernel. Sealing/cloning shares validators;
registration/replacement compiles the exact new schema and rejects an invalid
one. Keep `validate_arguments` as validation-only for callers that already
have canonical arguments; the executor owns invoking normalization.

## Compatibility and Alternatives

Adding a default trait method preserves existing Tool implementations and
LegacyTool's blanket identity behavior. Optional diagnostic fields deserialize
legacy plans/events as None and are omitted when unavailable. Rust literals
and field-exact patterns still need the three-consumer compile gate. No new
RuntimeEvent variant or public type relocation is introduced.

Reject unconditional adapter envelope stripping: `parameters` may be a valid
canonical field. Reject validating both raw and normalized forms against
different schemas: the advertised schema remains the sole validation contract.
Reject changed_segments.first(): it can name a newly appended tail or an
identity-wide invalidation, rather than a changed prefix fragment.

## Open Questions

- Owner approval of the synchronous pure hook and the proposed event ID bound.
- Consumer owners must choose which tools opt in and when their redundant
  schemas can be removed. Those choices do not belong to the library.
