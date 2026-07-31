---
created_at: 2026-07-27T02:57:00Z
updated_at: 2026-07-27T04:06:20Z
---

## Why

Smith's approved `child-agents` capability (harness change tasks 7.1–7.3) and
its release goal both defer direct-child work until the shared runtime exposes
delegation contracts, so consumers do not invent product-local child
mechanisms. The runtime currently has none: no child-session lifecycle, no
scoped child views, no delegation events. Live testing confirmed Smith cannot
delegate at all today.

## What Changes

- Add a new `agent-delegation` capability: neutral, host-invoked child-session
  lifecycle operations (spawn, list, follow up, wait, fetch result, stop)
  addressed by stable child ID, with a host-owned child specification
  (task, provider/model, turn/token/deadline limits, tool-view scope,
  workspace policy).
- Enforce a configurable delegation depth (default one): child tool/registry
  views exclude delegation operations, and the runtime rejects spawn attempts
  originating from a child even if a malformed call reaches the host.
- Route spawn/follow-up/stop through the same composed authorization path
  already live for tool invocation. Subject-derivation and taint semantics
  remain owned by the active `add-runtime-security-boundary` change's
  "Bounded sub-agent delegation" requirement; this change wires lifecycle,
  not policy semantics.
- Emit normalized child lifecycle events attributed with child ID, parent
  session ID, declared workspace policy, and limit metadata; a final child
  result must survive progress coalescing.
- Enforce bounded child concurrency (process/session/per-parent) plus
  per-child limits via the existing deterministic limit machinery. Children
  stop with their parent or the process and never restart on session resume.
- Add safe-boundary content injection to `agent-execution`: hosts can enqueue
  bounded content for an active session that is introduced to the model only
  at provider/tool boundaries, never by mutating an in-flight stream. This is
  the mechanism child results (and host notifications) use to reach a parent
  model safely.
- Extend the testkit with delegation conformance coverage (lifecycle order,
  depth rejection, capacity, cancellation propagation, injection boundaries).

## Impact

- Affected specs: `agent-delegation` (new), `agent-execution` (added
  requirement). No delta touches `runtime-api` or `provider-runtime`, so there
  is no file-level conflict with `add-runtime-security-boundary-2026-07-24`.
- Affected code: `crates/agent-runtime` (runtime engine/session, driver,
  scoped views), `crates/agent-runtime-core` (ids, events),
  `crates/agent-runtime-ability` (delegation descriptor kind),
  `crates/agent-runtime-testkit` (conformance).
- Consumers: Smith harness tasks 7.1–7.3 build directly on this contract;
  Nyx and Open Forge are unaffected until they opt in. The runtime remains
  product-neutral — no consumer names or prompts.
- Coordination: depends on the composed authorization entry point merged in
  `add-runtime-security-boundary` Phase A (live for tool invocation). If that
  change is unarchived at implementation time, delegation authorization ships
  under the same explicit compatibility authority note Smith already relies
  on.
