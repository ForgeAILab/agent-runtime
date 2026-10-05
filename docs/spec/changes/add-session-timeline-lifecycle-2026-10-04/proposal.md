---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-05T01:53:30Z
---

# Proposal: Explicit session and timeline lifecycle (U7 + U8)

## Why
Fresh sessions bind populated timelines without ownership checks and reconcile
may truncate committed summary sources. Implicit resume silently discards host
history. Hosts need bounded topic rotation and explicit session intent.

## What Changes
- Add defaulted, authorized LcmWriter::claim(view, owner, generation). The
  default claims an empty timeline and reports Unsupported for a populated one,
  which a fresh binding turns into a Fork. Claim-aware stores fence writes by
  owner generation and bump their revision on ownership change. Populated or
  differently owned timelines require explicit Adopt, Fork or Retire policy;
  default fails closed with TimelineOwned. Adopt claims before reading.
- Check the active-node frontier before truncation; return typed LcmDivergence.
  Preserve EntryConflict and RangeOverlap through harness, driver and RuntimeError.
- **BREAKING:** command schema 2 and explicit create/resume/ephemeral constructors.
  Create rejects existing state; resume requires existing state and rejects seed;
  ephemeral ignores persistence, never writes session/checkpoint stores and keeps
  LCM on a volatile timeline. SessionHandle reports resumed. Keep checkpoint
  recovery and identity floors. Remove with_id; add typed StartSession::from_json.
- Add Runtime::fork_session with Summary, FromIndex or Empty seed and NewTimeline
  or Continue LCM choice; validate before durable intent, roll the intent back on
  failure and add Runtime::abort_fork. Persist a Sensitive, Stable Summary fragment; supersede
  the idle parent. NewTimeline requires a fresh host-authorized binding; Continue
  explicitly transfers the existing timeline claim and adopts its projection.
- Extend the host resolver with defaulted replacement/continuation hooks; hosts
  persist binding decisions and supply every new authority. No product prompts.

## Impact
Affected specs: runtime-api, context-management, runtime-reproducibility.
Affected code: core error contracts, LCM store/test support, runtime commands,
engine/harness/driver, testkit and consumer fixtures. U6 WorkingSetPolicy and
revision tolerance remain intact. Protected-state separation, redaction,
LcmViewAuthority, planner admission and dependency neutrality remain unchanged.
No production consumer dependency is introduced. U9 journals and U10 canonical
LCM history are deferred.

## Authorization
The 2026-10-04 user request authorizes this spec-first implementation and final
checks. Leave all work uncommitted and unarchived.
