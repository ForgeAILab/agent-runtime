---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-07T00:00:00Z
---

# Proposal: LCM timeline as the canonical content journal (U10)

## Why

Main `c2e464699a455af251c29bd228fd16f47a4ecba9` already appends only the missing
history suffix to its authorized LCM timeline, with idempotency and claim
fencing. It still serializes canonical history into snapshots/checkpoints as
well. U9 removes repeated snapshot writes but leaves a durable SessionJournal
content copy beside LCM entries. LCM sessions need one exact durable content
source, usable by each host, without changing ordinary redaction policy.

## What Changes

- Build on [U9](../add-session-journal-2026-10-07/proposal.md), U7/U8 and current
  durable hard admission. For explicitly configured native LCM sessions, use
  immutable LCM entries as the protected canonical Message bodies; journal
  heads/checkpoints reference the timeline and validated source cutoffs.
- Add a companion LcmCanonicalJournal trait with authorized journal(view) and
  atomic commit_history(view, lease, expected_dag_revision, request) methods.
  Existing LcmReader/LcmWriter and SessionStore/CheckpointStore methods are
  unchanged from U9. Old store implementations keep compiling and running in
  their existing modes; explicit canonical configuration without this capability
  fails before work. No fabricated transaction across unrelated stores.
- Atomically publish newly durable entries and the exact referenced checkpoint;
  retain a distinct last-terminal cutoff. Provider preparation reads the active
  protected boundary; ordinary terminal projections use their own cutoff and
  redaction policy. An append count alone is not permission to expose raw content.
- **BREAKING persisted contract:** version-5 timeline-backed heads/checkpoint
  envelopes distinguish this source from U9's version-4 journal history roots;
  CHECKPOINT_SCHEMA_VERSION advances from 4 to 5.
  First U10 release reads supported v3 and v4 sessions, including unfinished
  turns, and migrates only with complete validated source evidence.
- Retain redacting SessionStore projections as a separate host-policy view.
  Keep exact CheckpointStore ownership of recovery, lease/pin fencing, credentials
  exclusion, LcmViewAuthority checks, signed continuation, planner-only requests
  and Sensitive pending summary/seed handling. 'One record' means one exact
  canonical Message body; protected raw operation payloads and ordinary redacted
  projections may legitimately differ and are not deleted as duplicates.

## Impact and Scope

Affected capabilities: `context-management`, `session-journal`,
`compatibility-contract`. Future implementation changes LCM contract/reference
backend, Runtime coordinator/persistence/recovery, and testkit. No production
consumer crate or policy is introduced. No new package, event variant, session
command, working-set policy, summary model or general storage redesign is needed.

Keep this separate from U9 and implement it second. U9 proves native persistence
without changing the source of truth. U10 adds only timeline-backed content,
its atomic checkpoint commit and migration/GC reachability integration; usage,
identity, extensions, frozen request recipes and diagnostic trees remain U9
metadata. Non-LCM and ephemeral modes retain their U9/current path. Old stores
need no forced LCM adoption; every consumer gets the same optional mechanism.

## Changes from G4/U10

Do not reimplement suffix append, claim/fork/resume, frontier-safe reconcile,
O(delta) accounting or durable hard-admission intent; these exist on main.
Do not make an arbitrary LcmStore a SessionJournal by blanket implementation:
existing append does not atomically publish an execution checkpoint, and its
current Fingerprint is not a collision-resistant journal content address.
Require an explicit companion capability over the same protected transaction
and collection domain. Keep U9's metadata journal; the LCM timeline is canonical
for conversation content, not a new store for every runtime event or usage type.

G4 proposed moving redaction to projection. Preserve that idea only for the
ordinary store *view*: an authorized exact timeline must not replace main's
redacted terminal history at an ordinary-reader boundary. Exact recovery and
provider continuation require the host's protected authorization; UI/audit
reads still get SessionStore's policy. Version 5 makes the source change
explicit instead of silently teaching an old v4 reader a new reference meaning.

## Consumer Compatibility and Forge Row 3.7

| Consumer | Required/optional adoption | Required new testkit fixture |
| --- | --- | --- |
| Forge | To enable canonical mode, implement LcmCanonicalJournal on its authorized LCM adapter using the same protected chunk/transaction domain as CheckpointStore.journal(). Persist each Message body once, with SHA-256 integrity and LCM entry identity/sequence side metadata. commit_history atomically commits LCM entries, exact journal roots/checkpoint and idempotency proof; enforce authority, claim generation, expected revisions and GC pins. Keep ordinary redacted chunks/projections separate. U9's chunk interface stays compatible; no consumer database schema is prescribed. | Extend `consumer_open_forge` with `assert_canonical_timeline_journal`: one body across timeline/history/request references, crash after each physical write, revoked authority, hard-admission leaf CAS, Continue/NewTimeline fork and concurrent collection. Run actual Forge chunk/LCM adapter checks before its coordinated pin. |
| Smith | No required LCM adoption. Existing file/capsule journal stores continue U9 mode; if Smith enables LCM canonical sessions, add the companion capability over its protected domain and preserve ordinary projections. Version/filter readers must support v5; manifests, contributor lanes, idle compaction and exhaustive RuntimeEvent matches remain. | Extend `consumer_smith` with `assert_optional_canonical_file_journal`: ordinary policy remains redacted, protected signed continuation and pending approval are exact, v3/v4 migration resumes once, and U9 non-LCM compatibility still passes. |
| Nyx | Ephemeral/history/subscription path stays storeless; no channel store or LCM requirement is added. Durable canonical adoption is optional using the same authorized host-neutral interface. A moving main dependency remains outside the compatibility contract. | Extend `consumer_nyx` with `assert_canonical_ephemeral_isolation` and an optional durable authorized timeline case, proving no durable discovery/claim/commit/GC for ephemeral history. |

Names above are future fixtures. Existing CI consumer targets and actual product
adapters must pass; a failing supported consumer blocks a compatible release.
Coordinate the persisted pre-1.0 break, all active adapter readers/guards, and
immutable landed tag/exact revision adoption across Forge, Smith and Nyx. An
uncommitted path override is only for development.

## Approval Decisions

Recommend explicit canonical opt-in, an atomic same-domain companion trait,
version 5 rather than reinterpretation of v4, continued v3/v4 readers through
the first U10 release, and no automatic source deletion/retention default.
Approve the decisions in design.md before implementation. Neither proposal
approves code, a commit, or a release.
