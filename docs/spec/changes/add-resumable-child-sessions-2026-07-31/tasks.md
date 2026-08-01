---
created_at: 2026-08-01T00:56:16Z
updated_at: 2026-08-01T03:24:02Z
completed_at: 2026-08-01T03:24:02Z
---

## 0. Coordination and Baseline

- [x] 0.1 Approve this proposal and the coordinated Smith proposal before
  implementation.
- [x] 0.2 Archive or explicitly rebase
  `add-agent-delegation-runtime-2026-07-26` and reconcile its ephemeral-child
  clause before applying this delta.
- [x] 0.3 Record current child state/event/factory schemas and the existing
  in-process follow-up conformance behavior as compatibility baselines.

## 1. Recovery-First Conformance

- [x] 1.1 Add
  `follow_up_after_parent_restart_reuses_child_session_and_history` and assert
  the provider receives the prior child conversation under the same child and
  session IDs.
- [x] 1.2 Add `interrupted_child_requires_explicit_resume` and prove startup,
  list, and result perform no provider or tool work.
- [x] 1.3 Add `resumed_child_does_not_repeat_committed_provider_or_tool_work`
  at every child checkpoint boundary.
- [x] 1.4 Add ownership, incompatible-policy, competing-lease, cumulative-limit,
  stopped/expired-child, and explicitly-ephemeral-host adversarial cases.

## 2. Durable Delegation Contracts

- [x] 2.1 Define versioned `ChildSessionRecord`, durability and recovery
  states, child-session identity, policy revisions, and checkpoint watermark.
- [x] 2.2 Add the protected parent-scoped catalog over session extension state,
  per-record revisions, lifecycle leases, authoritative checkpoint
  reconciliation, and deterministic in-memory store coverage.
- [x] 2.3 Extend normalized events and schema fixtures for interrupted,
  recovered, resumed, incompatible, expired, and durability-labelled states.
- [x] 2.4 Extend lifecycle operations with explicit `resume` while preserving
  `spawn`, `list`, `follow_up`, `wait`, `result`, and `stop` compatibility.

## 3. Child Session Persistence and Rebinding

- [x] 3.1 Remove unconditional child store clearing; compose child snapshot,
  checkpoint, artifact, and delegation stores without widening its tool view.
- [x] 3.2 Permit parented session startup with an explicit persisted child
  session ID only through the coordinator's parent-bound recovery path.
- [x] 3.3 Persist child state at accepted input, model response, pending
  interaction/approval, each committed tool result, terminal turn, and
  lifecycle-record boundaries.
- [x] 3.4 Restore the parent coordinator from catalog records and lazily bind
  child runtimes without provider spend during parent startup or listing.

## 4. Explicit and Idempotent Resume

- [x] 4.1 Reconcile orphaned running records to interrupted state without
  automatic execution and emit one attributed recovery event.
- [x] 4.2 Resume an exact compatible checkpoint through the canonical turn
  machine without adding a task or repeating committed work.
- [x] 4.3 Follow up an idle recovered child as a new turn with full canonical
  history, cumulative usage, manifests, limits, and artifact ownership.
- [x] 4.4 Fail closed on absent/corrupt checkpoints, policy or workspace
  mismatch, unavailable model/provider, stale revision, or another live lease;
  never create a replacement child implicitly.

## 5. Security, Limits, and Retention

- [x] 5.1 Bind every operation to the original parent session, tenant/project,
  child-session ID, and immutable specification fingerprint through composed
  authorization.
- [x] 5.2 Preserve depth-one views and cumulative turn/token/deadline limits
  across every restart and coordinator rebind.
- [x] 5.3 Add bounded retained-child policy plus explicit stopped, expired, and
  parent-deleted non-executable behavior; leave physical orphan-data garbage
  collection to the host's session-store retention policy.

## 6. Compatibility and Release

- [x] 6.1 Update API docs, migration guidance, changelog, event/store fixtures,
  and runtime/testkit examples; label legacy and no-store children ephemeral.
- [x] 6.2 Run fmt, warning-denied Clippy, workspace/all-feature tests, MSRV,
  store corruption/privacy tests, and delegation conformance.
- [x] 6.3 Pin and validate the coordinated Smith revision
  `71ada9c5d3fc6bda37fb20f0b9f327fe39771573`, including a
  process-restart follow-up and an explicit interrupted-task resume, before
  release.
