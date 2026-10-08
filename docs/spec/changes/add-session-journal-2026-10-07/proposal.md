---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-07T00:00:00Z
---

# Proposal: SessionJournal with referenced checkpoints (U9)

## Why

At main `c2e464699a455af251c29bd228fd16f47a4ecba9`, protected transitions still
serialize the entire history, usage ledger and extension state. CallingModel
also persists the frozen request. U3 removed unbounded diagnostic accumulation;
U11 reduced private in-memory copying and LCM accounting work. Neither changes
these durable serialization boundaries. Smith and Forge need delta persistence;
Nyx needs the same neutral capability if it later chooses durable sessions.

## What Changes

- Add a host-injected SessionJournal contract for immutable content-addressed
  objects, append-only transition batches, atomic head/checkpoint publication,
  fenced writers, pinned readers, retirement and reachability collection.
- Add defaulted `journal()` discovery methods to SessionStore and CheckpointStore.
  Their existing methods and materialized public types remain. Old implementations
  compile and use the full-snapshot compatibility path; native journals deliver
  the write reduction. No blanket adapter invents durable CAS from `save`.
- Store exact checkpoint execution state by references to protected journal
  objects. Freeze request contents once, with ordered persistent references to
  every final planned message and tool schema. Recovery never replans a request.
- **BREAKING persisted contract:** introduce version-4 journal heads and referenced
  checkpoint envelopes, and move CHECKPOINT_SCHEMA_VERSION from 3 to 4. Read
  valid v3 checkpoints and v3-era unversioned SessionSnapshot JSON for at least
  the entire first released version containing U9. Preserve transition revision
  4 unless equivalent-execution tests demonstrate that a new revision is required.
- Keep ordinary redaction and exact protected persistence separate. Native
  ordinary roots describe host-policy projections, never execution authority.
- Collect unreachable old checkpoint/request/extension objects and retired
  superseded-session content, under the same transactional root/pin fence used
  by fork and resume. Live canonical history is retained.

## Impact

Affected capabilities: `session-journal`, `runtime-reproducibility`,
`context-management`, `compatibility-contract`. Future implementation touches core store/checkpoint
contracts, runtime persistence/recovery/fork, driver transitions, and testkit.
No Rust, Cargo, existing truth spec, Git state or consumer repository is changed
by this proposal.

Keep U9 and [U10](../make-lcm-journal-canonical-2026-10-07/proposal.md) separate.
U9 lands first and is independently useful without LCM. Its four merge groups
are contracts/readers, native persistence, safe collection, and consumer gates.
An intermediate merge is not a release approval. U10 removes the LCM content
copy only after U9 recovery and pin semantics pass their own gates.

## Changes from the Audit

The audit at Forge `ef060ba2`, G3/U9, predates U3, U7/U8, U11 and durable hard
admission. Do not redo manifest-window APIs, Arc captures, claim/reconcile or
session constructors/fork. Main saves an embedded protected snapshot per changed
checkpoint revision, not an ordinary SessionStore snapshot on every transition.
The full derivation and current evidence are in design.md; there is no claim
that the old 100 GB incident or an estimated percentage was remeasured.

Reject the audit's blanket `SnapshotJournal<S: SessionStore + CheckpointStore>`
as a native journal: two `save` methods supply neither atomic multi-object CAS
nor collection pins. Provide a named SnapshotJournal compatibility adapter over
separate ordinary/protected stores, with full-snapshot writes and collection
unsupported, outside the native SessionJournal trait. Keep existing store
methods for this release; removing them needs another approved coordinated change.

## Consumer Compatibility and Forge Row 3.7

| Consumer | Adoption and retained surfaces | Required new testkit fixture |
| --- | --- | --- |
| Forge | Implement SessionJournal over its chunked blob backend twice, under distinct ordinary and exact protected policies; expose each via the corresponding store's journal(). Content hashes identify plaintext canonical objects within a protected tenant namespace, not ciphertext or global cross-tenant objects. Implement atomic chunk/index/head/checkpoint commit, fenced leases, pins and fenced collection. Keep its existing LCM stores/resolver; U9 does not make them canonical. | Extend `consumer_open_forge` with `assert_chunked_journal_recovery`: redacted ordinary history/Sensitive extensions versus exact prepared calls, v3 mid-turn import, hard-admission crash cuts, concurrent fork/resume/GC, and counted unique chunk writes. |
| Smith | FileSessionStore and all three CheckpointStore implementations keep compiling. Implement native journal capabilities in paired file/capsule stores to obtain the benefit; restrictive schema guards must accept supported version 4 and idle seed boundaries. Keep snapshot.manifests, contributors, idle compaction and exhaustive events unchanged. | Extend `consumer_smith` with `assert_file_journal_migration`: old source impls compile, v3 file/capsule loads, pending approval and signed reasoning recover exactly, files/head publication survive cuts, and repeated saves deduplicate. |
| Nyx | No required store implementation; ephemeral(history) and subscribe-before-send remain. Durable adoption is optional through the same pair of host-neutral stores; no channel/thread model enters Runtime. | Extend `consumer_nyx` with `assert_ephemeral_journal_isolation` (zero journal/store calls even when configured), plus `assert_optional_durable_journal` with neutral stores. |

These fixture names are proposed additions, not existing coverage. Existing named
consumer targets remain the release gate. Run their suites plus actual product
store/schema checks against one immutable landed tag or exact revision. A failing
supported consumer blocks a compatible release; document this as a coordinated
pre-1.0 persisted-contract break. Nyx must not use a moving main pin. Local path
overrides remain uncommitted development aids, not release dependencies.

## Approval Decisions

Recommend approving native opt-in plus legacy source compatibility for this
release, one full release of v3 readers, a SHA-256 versioned encoding, and
host-triggered retention/collection with no automatic expiry default. Approve
these choices before implementation; the exact decisions and risks are listed
in design.md. This document authorizes no implementation or release.
