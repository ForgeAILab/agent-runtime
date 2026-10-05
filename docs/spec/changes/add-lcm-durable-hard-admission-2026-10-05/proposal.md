---
created_at: 2026-10-05T00:00:00Z
updated_at: 2026-10-05T07:20:00Z
---

# Proposal: Durable ordinary-store hard admission

## Why

LCM hard admission already stages a Sensitive pending response before its DAG
CAS, but only CheckpointStore persists that boundary. A SessionStore-only host
can lose exact successor proof on a crash after the CAS and before terminal save.
U7/U8 ownership and append reconciliation do not persist pending summary intent.

## What Changes

- Persist the existing pending hard-response snapshot through SessionStore before
  retrying admission, only when no CheckpointStore is configured.
- Validate and discard an uncommitted ordinary-resume intent before new history
  appends; adopt an exact committed successor using existing validation.
- Exercise crash boundaries and staging errors with network-free testkit stores.

## Impact

Affected spec: context-management. Affected code: runtime LCM resume, turn driver,
LCM coordinator and testkit. No public production API, event or command schema,
LCM state schema or store adapter migration changes. Sensitive classification,
authority, fingerprints, revisions and guard checks remain mandatory. Checkpointed
and storeless admission retain their existing behavior. No hard-disable fallback,
bounded condensation, escalation hooks or guard interoperability is included.

## Authorization

The owner explicitly approved proposal then implementation in the 2026-10-05
request. Leave this change uncommitted and unarchived; no further approval pause.
