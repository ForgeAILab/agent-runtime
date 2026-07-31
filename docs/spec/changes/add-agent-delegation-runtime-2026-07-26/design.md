## Context

The shared runtime owns one canonical agent loop (`agent-execution`) and a
neutral tool contract (`tool-execution`), with composed authorization live for
tool invocation only (security boundary Phase A). Smith's approved
`child-agents` delta specifies product behavior (one-level hierarchy, parent
inbox, workspace policies) but its GOAL forbids Smith-local delegation
mechanisms. The capability hub already anticipates a sub-agent capability
kind but is unwired. This change adds the runtime-side delegation lifecycle
that Smith (and later Nyx/Open Forge) consume.

## Goals / Non-Goals

- Goals:
  - Neutral child-session lifecycle owned by the runtime engine.
  - Depth-one-by-default hierarchy enforced fail-closed in the runtime.
  - Child lifecycle observable through normalized runtime events.
  - Bounded budgets/concurrency reusing existing limit machinery.
  - A generic safe-boundary injection mechanism in the agent loop.
- Non-Goals:
  - Monitors, extension/MCP hosting, prompt-cache keepalive (Smith GOAL
    deferrals stay deferred).
  - Depth greater than one, cross-process children, child persistence/resume.
  - Workspace creation (worktrees, read-only mounts) — host adapter work.
  - Subject-derivation/taint policy semantics — owned by
    `add-runtime-security-boundary` ("Bounded sub-agent delegation").
  - Wiring the capability hub/discovery path (still gated on that change's
    Phase B).

## Decisions

- Decision: Children are full runtime sessions with a parent link.
  The engine reuses `Runtime`/`SessionHandle`/driver unchanged; a child is a
  session created with a parent session ID, scoped views, and its own limits.
  - Alternatives considered: a second lightweight child loop (rejected —
    duplicates the canonical loop and diverges on cancellation/limits).
- Decision: The delegation surface is host-facing runtime API, not a built-in
  tool. Hosts register their own delegation tool (name, prompt text, JSON
  schema) that calls the API. Keeps `agent-execution`'s consumer-owned prompt
  policy intact.
  - Alternatives considered: runtime-registered `spawn_agent` tool (rejected —
    bakes product prompt/presentation into the neutral runtime).
- Decision: Depth is enforced twice. Structurally, child views exclude
  delegation operations (the ability is absent). Defensively, the
  authorization path rejects spawn/follow-up/stop whose requesting session
  has a parent link, even if a malformed or injected call reaches the host.
- Decision: Workspace policy is declared, validated, and carried — not
  implemented. The runtime validates the policy shape (shared project,
  explicit directory, isolated worktree, read-only view), records it in
  lifecycle events, and hands it to the host adapter that creates/validates
  the actual workspace. Filesystem enforcement composes with security
  boundary Phase C when it lands.
- Decision: Delegation operations are authority-bearing and pass the same
  composed authorization entry point used for tool invocation (Phase A),
  fail-closed when no authorizer is composed.
- Decision: Safe-boundary injection is a generic `agent-execution` mechanism:
  a bounded per-session queue of host-supplied content drained only at
  provider/tool boundaries. Child final results use it; Smith's inbox (its
  task 6.5) builds on the same mechanism. Overflow returns a structured
  result to the enqueuer; a final child result is never dropped by
  coalescing.
- Decision: Capacity default is reject-with-structured-result; queueing is an
  explicit host opt-in policy. Keeps runtime behavior deterministic.
- Decision: Event surface extends `RuntimeEvent` with child lifecycle
  variants (spawned, progress, completed, stopped, failed) carrying child ID,
  parent session ID, workspace policy, and limit metadata, delivered in order
  through the existing envelope.

## Risks / Trade-offs

- The security-boundary change is unarchived; its delegation requirement
  could still be revised. Mitigation: no shared delta files, lifecycle-only
  scope here, and implementation task-gated on the composed authorization
  entry point that is already merged.
- Event schema growth. Mitigation: additive variants behind the existing
  versioned envelope; testkit conformance pins ordering.
- Scope creep toward hub/discovery. Mitigation: explicit non-goal; the
  delegation descriptor kind is registered but discovery stays unwired.

## Migration Plan

Purely additive. Existing consumers compile unchanged; delegation is opt-in
via new API. Smith adopts it in its existing harness change (tasks 7.1–7.3);
no data or config migration.

## Open Questions

- None blocking. Capacity queue policy shape can be finalized during
  implementation review (default reject is specified).
