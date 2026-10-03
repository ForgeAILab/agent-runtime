---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Proposal: Bound session manifests and exclude them from checkpoints (U3)

## Why

Each planned provider step appends a history-sized manifest to session state.
Snapshots and protected checkpoints then clone the accumulated list. A finite
diagnostic window removes the calls-times-history manifest term without
changing the store traits or moving canonical history into a journal. It
benefits persisted Smith/Forge sessions and bounds in-memory state for Nyx.

## What Changes

- Add a positive finite runtime manifest window, default 32 records, trimming
  oldest records after append and after validated resume.
- Retain `SessionSnapshot.manifests: Vec<TurnManifest>` and its serde form as an
  ordered recent window; add `SessionHandle::recent_manifests()` for the same
  owned diagnostic view.
- Keep manifests empty/absent in newly produced protected checkpoint snapshots.
  Treat historical checkpoint manifest lists as readable diagnostics rather
  than execution authority. Keep exact history, requests, prepared actions,
  usage, identities, extension state, and checkpoint fences protected.
- Replace manifest-list equality as a recovery-boundary check with a bounded
  planned-step counter in existing versioned extension state; older full lists
  supply the legacy counter before pruning.
- Explicitly limit historical manifest replay to retained or host-archived
  records; unavailable evidence fails before equivalent replay.

## Impact

- Affected specs: `runtime-reproducibility`, `compatibility-contract`.
- Existing snapshot/checkpoint types, schema/transition versions, store method
  signatures, and serde shapes remain. Missing manifests already default to
  empty. Old unbounded snapshots and checkpoints remain readable.
- APIs/configuration are additive, but **retention behavior changes**. Current
  truth requires every earlier manifest to survive a completed turn, so this
  proposal includes full MODIFIED requirements rather than pretending the
  bounded behavior is already allowed. Consumers relying on lifetime retention
  must adopt explicit host archival or a suitable finite window before release.
- Ordinary snapshots still contain O(history + K × history) data; checkpoints
  still contain exact history and, in CallingModel, the full request. With
  fixed K they cease growing as O(calls × history) from manifests, but neither
  O(delta) persistence nor a measured byte reduction is claimed.

## Compatibility and Consumer Gates

| Consumer | Required work before release | Testkit proof to add |
| --- | --- | --- |
| Smith | Keep direct snapshot.manifests reads compiling; confirm commands expect recent diagnostics. If full audit/replay is required, the host must archive manifests or choose an adequate finite window. Stores must preserve the new RedactionSafe boundary record through their existing extension-state surface. | `consumer_smith`: neutral file-backed/redacting session store plus exact checkpoint store resumes old and new forms; snapshot.manifests and recent_manifests expose the same ordered suffix. |
| Forge | Keep protected-store implementations; confirm code does not require checkpoint manifest equality or lifetime archive semantics. Preserve the new RedactionSafe boundary record; no journal/blob trait change is required. | `consumer_open_forge`: paired redacting/exact stores and authorized LCM, crash at pending approval and committed tool/model-result boundaries, no repeated external work, missing ordinary snapshot allowed. |
| Nyx | Keep fresh seeded-history and subscribe-before-send behavior; no store adoption required. | `consumer_nyx`: storeless seeded session makes more than K planned calls and retains only the newest K diagnostic records without losing history or usage. |

These are new neutral testkit fixtures to implement, not claims about existing
coverage. Add legacy unbounded/absent-manifest JSON and crash-boundary tests.
Run all three named consumer targets and actual consumer retention/projection
checks. A failure blocks a compatible release; document retention migration
and use only tagged or exact landed consumer dependencies.

All audit §5 Rust surfaces remain: Nyx's start/history/subscription flow;
Smith's file/session/checkpoint stores, explicit contributor lanes/classes,
public snapshot.manifests vector, idle compaction, and exhaustive event matches;
Forge's protected stores, LCM reader/writer/resolver, ProviderAttemptFinished,
and TurnFinish. There are no new RuntimeEvent variants. Snapshot manifests
remain hash/identity diagnostics with their existing redaction rules, not raw
source content. `Secret`, redaction-safe LCM Debug, Sensitive extension defaults,
credential exclusion, opaque LCM authority and authorization before lookups
remain. Checkpoint exactness covers all execution-relevant state; removing
diagnostic manifests does not remove provider requests or pending tool actions.
The ordinary/protected store split, revision/idempotency gates, planner-only
provider construction, and fail-closed security/model/cache defaults remain.
No consumer concept or product prompt enters the library.

## Dependencies

Independent of U1/U2; implement before U11's checkpoint/history optimization
to establish which diagnostic data is excluded. Use the completed cache/session
responsibility refactor's stable module roots. This is independent of U9's
journal/schema-v4 work and does not authorize it. No changes-root index is added
because this repo has no established convention for one; each W1 proposal
records its ordering.

## Approval Boundary

This draft permits no code changes. Later approval must explicitly accept
bounded diagnostic retention and the tested recovery equivalence; it does not
authorize relaxing protected execution state, dropping historical readers,
changing store traits, or making the LCM log canonical history.
