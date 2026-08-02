---
created_at: 2026-08-02T06:09:22Z
updated_at: 2026-08-02T07:57:20Z
---

## Why

Consumers need an explicit multi-turn objective that can survive ordinary turn
boundaries without fabricating user messages or reimplementing session state,
usage accounting, and continuation races in each host. The current runtime can
queue `UserInput` and contribute per-turn todo state, but it has no neutral
goal state machine or atomic idle-only internal-turn admission.

## What Changes

- Add a versioned reusable goal envelope with one current goal, bounded public
  objective, lifecycle status, optional observed token budget, usage
  provenance, active elapsed time, stable identity/generation, timestamps, and
  bounded stopped reason.
- Add standard `get_goal`, `create_goal`, and `update_goal` tools plus a goal
  harness component that owns validation, context contribution, exact tool
  output mutation, turn-commit accounting, typed events, and persistence
  patches.
- **BREAKING** Add a provenance-bearing internal-turn input and
  `try_send_internal_if_idle` session admission. Internal turns use the normal
  execution pipeline but do not append a user-role canonical history message
  and are never queued behind competing user work.
- Add a reusable process-scoped goal controller that observes durable goal and
  terminal state, deduplicates by goal identity/generation, and attempts at
  most one continuation whenever an active goal is idle.
- Add typed host goal controls for create/edit/budget/pause/resume/clear,
  including busy-state policy hooks and goal-aware interruption support.
- Charge provider-reported uncached input plus output tokens and derived active
  serving time exactly once. Budgeted goals fail closed when required usage
  evidence is unavailable and stop at the first observed boundary at or above
  budget.
- Extend checkpoints, completed-turn snapshots, schema fixtures, and testkit
  conformance to cover goal mutation, resume, idle races, interruption,
  accounting, replay, and process shutdown.

## Explicit Non-Goals

- Smith commands, TUI rendering, headless output formats, or product policy.
- Image/paste attachment materialization, analytics, fork inheritance or
  deferrals, app-server JSON-RPC/SDKs, or product telemetry.
- Daemons, remote workers, restart-triggered scheduling, nested goals, multiple
  concurrent goals, or implicit child-session inheritance.
- Guessing missing provider usage or claiming a token budget is a pre-spend
  hard cap.

## Impact

- Affected specs: `goal-harness`, `internal-turn-control`, `goal-accounting`,
  `goal-conformance`
- Affected code: `agent-runtime-core` events/checkpoints/usage, `agent-runtime`
  harness/session/driver/controller, `agent-runtime-testkit`, schema fixtures,
  documentation, changelog, and coordinated consumer conformance
- Public compatibility: session turn-input/admission APIs, runtime events,
  goal state/control types, checkpoint schemas, and facade exports
- Persistence: one namespaced versioned goal component state in canonical
  session extension state; no parallel store or project files
- Consumer: coordinated Smith work is specified by
  `../tui/docs/spec/changes/add-persistent-session-goals-2026-08-02/`

## Active Change Coordination

- `stabilize-session-harness-pipeline-2026-07-31` remains authoritative for
  per-session execution state, ordered harness phases, completed-turn
  persistence, checkpoint watermarks, typed events, and interruption. This
  change extends those contracts and requires its Sections 1 through 8.
- `add-runtime-security-boundary-2026-07-24` remains authoritative for grants,
  preparation, authorization, and isolation. Goal context grants no authority;
  internal turns traverse the same prepared invocation pipeline.
- `add-resumable-child-sessions-2026-07-31` remains authoritative for child
  identity and recovery. Root goals are neither copied nor advertised to child
  sessions in this change.

## Delivery Slices

1. Land goal schemas, pure lifecycle validation, typed control/result types,
   events, and compatibility fixtures.
2. Land the three standard tools and goal component with context, mutation,
   persistence, and provider-usage accounting tests.
3. Land provenance-bearing internal turns and serialized idle-only admission,
   preserving real-user priority and canonical history.
4. Land the reusable controller, resume/interruption/shutdown behavior, and
   deterministic race/crash conformance.
5. Run runtime and Smith consumer gates, update docs/changelog, and record the
   exact runtime revision for the coordinated Smith implementation.

## Approval Boundary

Approval authorizes Stage 2 implementation in Agent Runtime and the separately
approved coordinated Smith consumer change. It does not authorize other
consumer changes, publication, attachments, analytics, forks, app-server APIs,
daemons, child goals, nested goals, or multiple concurrent goals.
