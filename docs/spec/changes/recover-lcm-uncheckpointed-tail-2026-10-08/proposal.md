---
created_at: 2026-10-08T00:00:00Z
updated_at: 2026-10-08T00:00:00Z
---

# Proposal: Resume an LCM session whose turn was killed between provider steps

## Why

Every provider step of a turn appends its history to the LCM timeline. A host
with a SessionStore and no CheckpointStore saves the session when the turn
ends. A process that dies in the middle of a multi-step turn therefore leaves
the store several appends ahead of the saved session, holding entries no saved
history contains.

Resume validation accepted three shapes only: the exact checkpoint, a pending
summary successor, and one canonical append. This residue is none of them, so
resume failed with `LCM DAG revision no longer matches its protected
checkpoint` on every later turn and the session never recovered. A Nyx
production session was stuck this way after a service restart during a
scheduled turn. `reconcile_diverged_store` already drops the same residue at
an append, but resume refused the session before any append could run.

## What Changes

- When strict validation and the append-successor check both fail, resume
  tries one more recovery. If the checkpoint has no pending summary, its
  history prefix is canonical, the store's active DAG is exactly the one the
  checkpoint recorded, every entry below the checkpointed frontier matches
  canonical history and the store holds an entry at the frontier, the store
  is truncated from the frontier and the checkpoint is re-anchored to the
  resulting revision.
- Any other disagreement stays the original conflict.

## Impact

Affected spec: context-management. Affected code: LCM coordinator resume
validation. No public API, event, state schema or store contract change; the
recovery uses the existing `truncate_from`, and a store that does not support
truncation keeps today's conflict. The interrupted turn's entries are removed
from the timeline; canonical history past the frontier is appended again by
the next synchronization. Summary nodes are never changed.
