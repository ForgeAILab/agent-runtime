---
created_at: 2026-07-27T02:57:00Z
updated_at: 2026-07-27T04:06:20Z
completed_at: 2026-07-27T03:25:13Z
---

## 1. Contracts and Events

- [x] 1.1 Add child ID and parent-session linkage to core identifiers.
- [x] 1.2 Define the child specification types: task content, provider/model
  selection, turn/token/deadline limits, tool-view scope, and the workspace
  policy enum (shared project, explicit directory, isolated worktree,
  read-only view) with structural validation.
- [x] 1.3 Extend `RuntimeEvent` with attributed child lifecycle variants
  (spawned, progress, completed, stopped, failed) carrying workspace policy
  and limit metadata through the existing envelope.

## 2. Engine Lifecycle

- [x] 2.1 Implement spawn/list/follow-up/wait/result/stop on the runtime
  engine, creating children as full sessions with a parent link and reusing
  the existing driver unchanged.
- [x] 2.2 Enforce process, session, and per-parent concurrency caps with
  reject-by-default capacity results and an explicit host queue policy hook.
- [x] 2.3 Stop children with their parent session and process teardown;
  guarantee exactly one terminal event per child and no restart on resume.
- [x] 2.4 Enforce per-child turn/token/deadline limits through the existing
  deterministic limit machinery.

## 3. Scoping and Authorization

- [x] 3.1 Build scoped registry/tool views for children that exclude
  delegation operations at maximum depth.
- [x] 3.2 Route spawn/follow-up/stop through the composed authorization entry
  point, fail-closed, rejecting operations from sessions that have a parent
  link as depth violations.
- [x] 3.3 Verified the delegation descriptor kind already exists in the
  ability layer (`AbilityKind::Agent`, `RegistryDomain::Agent`); hub
  discovery stays unwired (gated on the security change's Phase B).

## 4. Safe-Boundary Injection

- [x] 4.1 Add the bounded per-session injection queue drained only at
  provider/tool boundaries, with structured overflow results.
- [x] 4.2 Implement coalescing rules that preserve final child results and
  deliver them with the completed event.

## 5. Conformance and Docs

- [x] 5.1 Add testkit conformance for lifecycle ordering, depth rejection,
  capacity behavior, and cancellation propagation into child tools and
  provider streams.
- [x] 5.2 Add event-order and injection-boundary tests (including
  content-arrives-during-streaming).
- [x] 5.3 Update runtime docs and CHANGELOG; record the compatibility
  authority note if the security-boundary change is still unarchived.
