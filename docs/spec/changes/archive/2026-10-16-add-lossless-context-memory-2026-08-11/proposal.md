---
created_at: 2026-08-11T21:39:04Z
updated_at: 2026-08-12T01:49:11Z
---

## Why

Before this cutover, Agent Runtime's semantic-summary harness replaced one
growing history prefix with one rolling summary. Nyx has proven a stronger
long-session mechanism: an immutable timeline plus a hierarchical summary DAG,
lossless source pointers, threshold-driven compaction, and a
convergence-guaranteed summarizer. Nyx, Smith, and Open Forge all need this
mechanism, satisfying the shared-code admission rule and justifying an
independently reusable package.

## What Changes

- Add a production package, `agent-runtime-lcm`, containing host-neutral Lossless Context Memory (LCM) identities, store contracts, DAG invariants, deterministic block planning, active-context projection, lossless expansion, and summarization escalation.
- Make `LcmTimelineId` independent from `SessionId`, allowing one logical agent or conversation timeline to survive process restart, runtime-session rotation, provider replacement, and host-specific session models.
- Add a runtime `LcmCoordinator` that admits soft compaction only at an explicit idle boundary and performs checkpointed hard-pressure compaction before provider admission, projects committed active nodes as context fragments, and emits redaction-safe lifecycle events.
- **BREAKING:** replace the single rolling `SemanticSummaryCoordinator` public contract and state schema with the LCM coordinator and remove the superseded implementation rather than retaining two semantic-compaction paths.
- Cut over valid persisted schema-v1 state automatically during resume when
  `RuntimeBuilder::lcm` is configured and the coordinator has the legacy
  protected `ArtifactStore` and the runtime has a durable `SessionStore`. The
  runtime validates canonical history, artifact integrity/provenance, and the
  host binding, persists the replacement LCM leaf/checkpoint before accepting
  turns, then removes the old namespace; invalid state fails closed and no
  public/manual restore alias exists.
- Add deterministic conformance suites covering immutable history, atomic DAG writes, reachability, concurrency, tool-exchange boundaries, convergence, restart recovery, replay, sensitivity, and bounded expansion.
- Re-export the supported LCM composition surface from `agent-runtime`, while allowing hosts to depend on `agent-runtime-lcm` directly.
- Document both consumption modes: direct hosts implement `LcmReader`/
  `LcmWriter`, share a `LcmViewAuthority`, and run testkit conformance;
  facade hosts compose `LcmTimelineBinding`/resolver, store, model, and policy
  through `RuntimeBuilder::lcm`.
- Record the Nyx donor revision and path map in `PROVENANCE.md`; Agent Runtime
  is the canonical shared implementation while each consumer adoption remains
  a separate approved change.

## Impact

- Affected specs: `lossless-context-memory` (new), `package-architecture`, `runtime-reproducibility`
- Affected packages: new `crates/agent-runtime-lcm`; `crates/agent-runtime`; `crates/agent-runtime-context`; `crates/agent-runtime-core`; `crates/agent-runtime-testkit`
- Public contracts: LCM package API, facade re-exports, session-start timeline
  binding, protected state schema, runtime events, and redaction-safe
  run-manifest projections
- Consumer coordination: Nyx, Smith, and Open Forge require separate approved adoption changes and must pin the exact resulting Agent Runtime revision. The runtime release remains blocked until their applicable conformance rows pass.
- Active-change coordination: the runtime adapter is rebased onto the approved `add-runtime-security-boundary-2026-07-24` sensitivity, trust, content-guard, and transformation-revision vocabulary; LCM joins and preserves those neutral classifications rather than defining a parallel taxonomy.

## Non-Goals

- Moving Nyx's vector/embedding memory store, skill memory, cron scheduler, chat/channel policy, or product configuration into Agent Runtime.
- Defining Forge project, Mission, Room, identity, ACL, authority, publication, or retention semantics.
- Providing a concrete SQLite, SQLx, Qdrant, filesystem, or hosted database implementation in the production dependency graph.
- Making summary generation or memory expansion a source of agent authority.
- Modifying Nyx, Smith, or Open Forge in this change.
