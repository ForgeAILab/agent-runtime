---
created_at: 2026-08-01T00:56:16Z
updated_at: 2026-08-01T01:08:11Z
---

## Why

Agent Runtime already gives a child a stable `ChildId` and preserves its
conversation for multiple follow-ups while the owning process remains alive.
That continuity ends at process or parent-session teardown: the child factory
clears session persistence, child startup refuses snapshot/checkpoint recovery,
and a resumed parent cannot reconstruct the delegation coordinator. A
follow-up after restart therefore cannot reach the same child or its history.

Long-running coding work needs a durable teammate abstraction, not repeated
one-shot dispatch. The child session, authority, limits, history, usage, and
result lineage should survive a Smith restart. At the same time, background
execution must not silently restart after a crash or repeat a committed model
call or tool side effect.

## What Changes

- **BREAKING** Separate a durable child-session record from its process-owned
  execution lease. A child keeps one stable child ID and one stable runtime
  session ID across parent resume.
- Add a host-neutral protected delegation store/catalog tied to the existing
  session and checkpoint stores. Persist parent ownership, immutable child
  specification fingerprint, policy revisions, cumulative limits, lifecycle
  revision, and checkpoint watermark without duplicating raw task content in
  an unprotected index.
- Permit child sessions to load and save their canonical snapshots and exact
  checkpoints. Reconstruct a parent's coordinator from durable records and
  lazily rebind child handles only when an operation needs execution.
- Extend the lifecycle with an interrupted/resumable state and an explicit
  `resume` operation. A crash or process exit never automatically executes a
  child; `resume` continues the exact checkpoint, while `follow_up` starts a
  new turn on an idle child with its prior history.
- Require recovery to be idempotent. Committed provider responses, tool
  results, approvals, interaction answers, usage, and turn counts are not
  repeated or reset across child resume.
- Bind every durable child to its original parent session, tenant/project
  scope, workspace posture, model/provider policy, tool view, and authority
  revisions. Recovery may narrow authority but must fail closed on an
  incompatible or widening change and must never substitute a newly spawned
  child.
- Retain depth-one delegation, running-child concurrency caps, cumulative
  per-child limits, cancellation propagation, and explicit retention/terminal
  cleanup. Persistence increases continuity, not delegation depth or ambient
  authority.
- Preserve an explicitly labelled ephemeral mode when the host provides no
  durable delegation/session/checkpoint stores; cross-restart continuity must
  never be claimed in that mode.

## Impact

- Affected specs: `agent-delegation`, `runtime-reproducibility`
- Affected code: `agent-runtime-core` store/identifier/event contracts,
  `agent-runtime` delegation and session startup, and
  `agent-runtime-testkit` recovery/conformance fixtures
- Public compatibility: child lifecycle/state schemas, delegation operations,
  child factory/store composition, and parent replay events
- Security: durable records are parent-owned, policy-fingerprinted, protected,
  and lease-guarded; recovery never authorizes a child by possession of an ID
- Persistence: child canonical state uses the same protected exact-state
  boundaries as root sessions, with a delegation catalog watermark connecting
  parent ownership to the child checkpoint
- Consumer: coordinated Smith behavior is specified by
  `../tui/docs/spec/changes/integrate-resumable-child-sessions-2026-07-31/`

## Active Change Coordination

- `add-agent-delegation-runtime-2026-07-26` currently defines children as
  process-ephemeral. Its completed implementation must be archived or
  explicitly rebased into truth before this change replaces that lifecycle
  clause; stable IDs, depth-one enforcement, authorization, and safe parent
  reporting remain intact.
- `stabilize-session-harness-pipeline-2026-07-31` owns exact checkpoints,
  idempotent turn recovery, per-session execution contexts, and protected
  interaction state. Child recovery reuses those contracts and does not add a
  second turn machine or checkpoint format.
- No nested agents, daemon scheduler, remote worker protocol, or automatic
  post-crash execution is authorized by this proposal.

## Delivery Slices

1. Add failing conformance fixtures for durable identity, follow-up after
   restart, explicit interrupted-task resume, idempotency, and ownership.
2. Add versioned child records, store/catalog contracts, lifecycle states,
   leases, and compatibility fixtures.
3. Persist child sessions and restore/rebind coordinators without auto-running
   work.
4. Enforce cumulative limits and policy compatibility across recovery, then
   expose exact lifecycle events and operations.
5. Validate the coordinated Smith consumer against a pinned runtime revision.

## Approval Boundary

Approval authorizes Stage 2 implementation in Agent Runtime only. It does not
authorize the coordinated Smith changes, release publication, nested agents,
or a persistent background daemon. `../tui` requires separate approval of its
coordinated proposal.
