---
created_at: 2026-08-01T00:56:16Z
updated_at: 2026-08-01T00:56:16Z
---

## Context

Today a `DelegationCoordinator` owns in-memory `ChildEntry` values containing a
runtime, `SessionHandle`, status channel, and limits. `follow_up` correctly
reuses that handle and its canonical history. However,
`ChildRuntimeFactory::child_builder` is followed by `clear_session_store()`,
and `Runtime::start_child_session` explicitly excludes parented sessions from
snapshot and checkpoint loading. This makes child identity stable only for one
process lifetime.

## Goals

- Reuse the exact child and its canonical history after parent/process restart.
- Resume an interrupted child task only through an explicit operation.
- Reuse root session checkpoint/idempotency behavior instead of inventing a
  child-specific execution loop.
- Preserve depth-one delegation, cumulative limits, concrete ownership, and
  fail-closed authorization.
- Support lazy recovery so listing durable children does not start providers or
  consume model tokens.

## Non-Goals

- Automatically restart work after a crash.
- Keep an OS process alive independently of the parent host.
- Add grandchildren, distributed workers, or cross-project child adoption.
- Promise durable continuity when the host configured ephemeral stores.

## Decision 1: Session and Execution Are Separate Lifetimes

The durable unit is a child session; the running unit is an execution lease.

```text
Parent Session
  └── ChildSessionRecord (durable, stable ChildId + SessionId)
        ├── canonical SessionSnapshot / TurnCheckpoint
        └── ChildExecutionLease (process-owned, optional)
```

Process shutdown cancels the execution lease. It does not delete an idle or
recoverable child session. A record observed as running without a live lease is
reconciled to `Interrupted`; it is never run as a startup side effect.

## Decision 2: Versioned Parent-Owned Delegation Catalog

The implementation reuses `SessionSnapshot::extension_state` rather than
adding a second persistence trait. The redaction-safe, versioned catalog is
owned by the parent session and committed through the same `SessionStore`
boundary as that parent. Each record contains at least:

```rust
pub struct ChildSessionRecord {
    pub child: ChildId,
    pub child_session: SessionId,
    pub parent_session: SessionId,
    pub specification_fingerprint: Fingerprint,
    pub policy_revisions: ChildPolicyRevisions,
    pub workspace: WorkspacePolicy,
    pub limits: ChildLimits,
    pub turns_used: u32,
    pub state: DurableChildState,
    pub checkpoint_watermark: Option<CheckpointWatermark>,
    pub revision: u64,
}
```

The catalog stores authority and reconciliation metadata, not a second copy of
raw prompts, tool arguments, or sensitive results. Exact content remains in
the child's protected snapshot/checkpoint. A store commit publishes a child
checkpoint watermark only after that checkpoint is durable.

Agent Runtime rejects a second coordinator for one live parent handle and
serializes bind/catalog commits inside that coordinator. The host's existing
exclusive parent-session lifecycle lease is the cross-process ownership
boundary; the generic `SessionStore` is intentionally not expanded into a
second product-specific lease API.

## Decision 3: Explicit Recovery Operations

`follow_up(child, input)` and `resume(child)` have different meanings:

- `follow_up` starts a new child turn, increments the cumulative task count,
  and is valid only for an idle/needs-input child.
- `resume` reopens the exact interrupted turn checkpoint, does not create a new
  user turn, and does not increment the task count a second time.
- `spawn` is the only operation that creates a child identity. Neither
  operation falls back to spawn if lookup or recovery fails.

An interrupted record with no compatible exact checkpoint becomes
`InterruptedUnrecoverable` and requires an explicit new spawn. A compatible
checkpoint resumes through the canonical `TurnMachine`, preserving committed
boundaries exactly once.

## Decision 4: Lazy Coordinator Rebinding

When a parent resumes, the coordinator loads and validates its child records,
verifies its in-process parent ownership, and exposes them through `list`
without constructing runtimes. `follow_up` or `resume` asks the host factory to
rebuild the original narrowed composition, starts the child with its persisted
session ID and parent link, then installs the ordinary monitor and cancellation
paths.

Only one coordinator may own a live parent handle. A host must enforce the same
exclusive ownership across processes for the parent session before creating
that handle. Per-child revisions and the coordinator bind gate prevent
competing follow-ups or resumes from both committing inside the owner.

## Decision 5: Authority Is Revalidated, Never Recreated

A durable child remains owned by the exact parent session and host security
scope. Recovery checks tenant/project binding, canonical workspace identity,
model/provider availability, tool-view upper bound, activation/profile
revisions, and grants. The host may apply a narrower compatible view. Any
required widening, changed workspace identity, missing provider, or untrusted
revision returns a structured incompatibility and leaves the record untouched.

Knowing a `ChildId` is insufficient authority. List, result, follow-up, resume,
and stop all resolve through the parent-scoped catalog and composed
authorization path.

## Decision 6: Bounded Retention and Events

Running-child capacity continues to count live execution leases, while
per-parent retained-child limits bound durable records. Turn/token/deadline
usage remains cumulative across restarts. Explicit stop is terminal; expiry or
parent deletion makes the child non-resumable according to host retention
policy.

Add versioned interrupted/recovered/resumed lifecycle events with child,
parent, and child-session attribution. Event replay may reconstruct
presentation, but protected records/checkpoints remain authoritative for
execution.

## Migration

- Existing snapshots without a delegation catalog load normally and expose no
  fabricated durable children.
- Journal-only legacy child IDs are labelled legacy ephemeral and cannot be
  resumed.
- Hosts without the new store remain source compatible through explicit
  ephemeral composition, but status must report that durability honestly.
- Archive or rebase the completed delegation delta before applying the
  modified lifecycle requirement.
