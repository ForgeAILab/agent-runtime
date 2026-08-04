---
created_at: 2026-07-31T08:34:33Z
updated_at: 2026-07-31T08:34:33Z
---

## Why

Agent Runtime has strong host-neutral contracts for context accounting,
authorization, events, registries, and delegation, but the canonical turn path
does not yet uphold several of those contracts. Conversation classification
currently changes provider message order, mutable planning state is shared by
all sessions in a runtime, resume drops manifests, user interruption cancels
the whole session, failed retry output is committed to consumers, and tool
authority is derived before invocation arguments are prepared.

The runtime also lacks the integrated, reusable harness path needed to make its
ability registry, lifecycle events, checkpoints, artifacts, skills, memory,
todo planning, and human interaction usable together. Adding more extension
sources before correcting the live path would multiply incompatible behavior.

## What Changes

- **BREAKING** Separate context classification from placement. Preserve
  conversation chronology exactly and compact a complete tool exchange or none
  of it, including assistant messages containing multiple parallel calls.
- **BREAKING** Move mutable planner, cache, compaction, activation, and
  current-turn state into a per-session execution context.
- Remove the compactor outcome side channel. Compaction returns content and
  outcome atomically, and the deterministic context crate is renamed and
  scoped as structural compaction rather than semantic summarization.
- **BREAKING** Distinguish turn interruption from permanent session
  cancellation. Turn submission returns a structured handle/result and cannot
  silently accept work during shutdown.
- **BREAKING** Attribute streamed text and reasoning to one provider attempt.
  Consumers buffer speculative output and commit or discard it according to an
  explicit attempt terminal event.
- **BREAKING** Add invocation-specific tool preparation. Canonical arguments,
  concrete resources, permissions, display metadata, and a preparation
  fingerprint are frozen before authorization and approval; execution must use
  that exact prepared action.
- Use the registry's typed permission vocabulary in ability descriptors and
  require every prepared invocation to remain within its descriptor's declared
  permission upper bound.
- Refactor the direct loop into a typed, serializable `TurnMachine` and add a
  protected checkpoint contract. Persist at least every completed turn first,
  then checkpoint accepted input, model responses, pending interactions,
  committed tool results, and completion.
- Add a host-neutral interaction broker and a standard questionnaire ability
  for agent-originated clarification or choice. This is separate from security
  approval and can never grant authority.
- Wire scoped ability views, retrieval, activation epochs, materialized tool
  schemas/instructions, and declared lifecycle events into session creation and
  provider boundaries.
- Add an ordered, phase-specific harness component pipeline plus standard
  mechanisms for todo state, skill/memory contribution, session-private
  artifacts, recoverable tool-output offloading, and model-assisted semantic
  summaries.
- Keep the direct loop; do not introduce a general graph engine or unrestricted
  mutable middleware.

## Impact

- Affected specs: `context-management`, `agent-execution`, `provider-runtime`,
  `runtime-api`, `tool-execution`, `runtime-reproducibility`,
  `capability-routing`, `package-architecture`, new `host-interaction`, and new
  `artifact-management`
- Affected code: `agent-runtime-context`, `agent-runtime-core`,
  `agent-runtime-ability`, `agent-runtime`, `agent-runtime-obs`, and
  `agent-runtime-testkit`
- Public compatibility: provider/context event schemas, `Tool`, approval and
  session-control APIs, snapshots/checkpoints, and descriptor permissions
- Security: authorization moves from static tool-wide effects to the exact
  prepared invocation; approval remains unable to override a hard denial
- Persistence: redacted observability journals remain audit records, while a
  separate host-protected checkpoint contains exact resumable state
- Consumers: coordinated Smith work is specified by
  `../tui/docs/spec/changes/integrate-stable-session-harness-2026-07-31/`

## Active Change Coordination

- `add-runtime-security-boundary-2026-07-24` remains the owner of grant,
  isolation, broker, and security-check semantics. Release-gate Sections 1
  through 3 can proceed independently; prepared-invocation Section 4 must
  coordinate with its landed Phase A and becomes a prerequisite for later
  security/extension work. The two changes MUST use one typed
  `agent_runtime_registry::Permission` vocabulary and one authorization
  request, without waiting for unrelated isolation-backend tasks.
- Completed delegation and reasoning-preservation behavior remains intact.
  Child sessions receive their own execution context; attempt-scoped streaming
  must preserve reasoning continuation within a successful attempt.
- No MCP, subprocess extension source, nested-agent expansion, or public
  release proceeds until the release-gate tests in this change pass.

## Delivery Slices

1. Release gate: exact conversation order, active-turn atomicity, per-session
   planning state, compaction result ownership, manifest resume, turn
   interruption, retry-output isolation, and provider-request capture tests.
2. Prepared authority: invocation preparation, typed permission bounds,
   cancellable/deadline-aware approval, and exact-resource tests.
3. Durable turn machine: serializable states, checkpoint watermarks,
   completed-turn persistence, pending approval/question resume, and recovery
   tests.
4. Integrated harness: live ability activation, ordered component phases,
   lifecycle events, versioned prompt/context contributors, and scenario
   evaluations.
5. Standard components: questionnaire, todos, skills, memory, artifacts,
   recoverable offloading, and semantic-summary coordination.

Each slice must pass its compatibility and conformance gates before the next
slice changes the public surface.

## Follow-on Roadmap

After this change is complete, separate proposals may wire one concrete
isolation backend end to end, add MCP and subprocess sources through the
ability registry, enrich child results with structured artifacts, add
model-specific harness profiles only where evaluations justify them, and
consider a separate agent process only for demonstrated background,
crash-isolation, or remote-client needs. None of those follow-ons may bypass
the prepared-invocation, activation, checkpoint, or host-interaction contracts
established here.

## Approval Boundary

Approval authorizes Stage 2 implementation in this repository only. It does not
authorize consumer changes, package publication, MCP/extension rollout, a
general graph engine, nested delegation, or a concrete isolation backend.
`../tui` requires separate approval of its coordinated proposal.
