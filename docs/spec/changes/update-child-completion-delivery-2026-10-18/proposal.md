---
created_at: 2026-10-19T01:40:00Z
updated_at: 2026-10-19T01:40:00Z
---

## Why

Automatic child-completion delivery builds one internal turn bounded by the
general 4,096-character internal-turn limit. A batch over that limit is
refused as a conflict and stays ready, so one long child reply (or several
short ones together) blocks every later automatic delivery for the parent.
The delivery also names only each child's id and reply text; artifacts
transferred to the parent are never mentioned, so the model cannot know they
exist without inspecting the result itself.

Approved through Smith's `update-child-result-delivery-2026-10-18`, which
specifies the host-facing behavior; this change records the Runtime contract.

## What Changes

- `DelegationConfig::delivery_inline_bytes` (default 4,096, maximum 1 MiB)
  sets how many bytes of replies one delivery carries inline. Replies are
  inlined in batch order while they fit; replies of at most 1,024 bytes are
  always inlined.
- A completed child reply longer than 1,024 bytes is stored, when the factory
  exposes an artifact store, as a parent-owned artifact with purpose
  `delegation.child-reply`, keyed by parent, child session, child, turn, and
  text. `ChildTaskResult::reply` carries the reference.
- A reply that does not fit is delivered as its opening 1,024 bytes plus the
  stored reference, or a pointer to the child's result when nothing was
  stored.
- Each completed outcome lists its transferred artifacts (parent id, child
  source id, media type, size), up to 32 with a count for the rest.
- A `delegation.child-completion` internal turn is validated against
  `MAX_CHILD_COMPLETION_TURN_CHARS` (2 Mi characters) instead of the general
  bound. A batch that would exceed it is rebuilt as one compact line per
  outcome. Size never refuses an admission.
- No checkpoint, cursor, or session-state revision constant changes;
  `ChildTaskResult::reply` is an optional serde field.

## Impact

- Affected specs: `agent-delegation` (two ADDED requirements),
  `internal-turn-control` (one ADDED requirement).
- Affected code: `agent-runtime-core/src/content.rs`,
  `agent-runtime/src/delegation/{mod,types,coordinator,persistence}.rs`,
  testkit `ChildTaskResult` literals.
- **BREAKING (pre-1.0)**: `ChildTaskResult` and `DelegationConfig` gain a
  public field; struct literals must add it (`..Default::default()` already
  covers `DelegationConfig`).
