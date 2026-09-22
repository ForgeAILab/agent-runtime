---
created_at: 2026-09-22T20:49:36Z
updated_at: 2026-09-22T21:10:24Z
---

# Proposal: Refactor cache and session source responsibilities

## Why

The runtime cache mechanism and session implementation have grown to combine
independently changing request, identity, persistence, dispatch, recovery, and
turn lifecycle code in single source files. Their existing contracts are
already specified, so the work is a source-only decomposition to make those
responsibilities easier to review and maintain.

## What Changes

- Split `cache.rs` into cohesive request, fingerprint, state, dispatch, and
  validation modules while retaining `agent_runtime::cache` as the public
  compatibility root.
- Split `runtime/session.rs` into cache coordination, recovery, and turn
  lifecycle/admission modules while retaining the supported
  `agent_runtime::runtime::session` path and current re-exports.
- Move the self-contained cache-identity value, builder, and validation types
  from `agent-runtime-core::provider` into a private child module while
  preserving their existing `agent_runtime_core::provider` paths.
- Keep exhaustive cache reductions, security-sensitive validation, ordered
  checkpoint transitions, and existing conformance tests intact.
- Do not change behavior, public signatures, serialization, dependencies, or
  supported module paths.

## Impact

- Affected specs: `package-architecture`.
- Affected code: `crates/agent-runtime/src/cache.rs` and
  `crates/agent-runtime/src/runtime/session.rs`, plus the identity section of
  `crates/agent-runtime-core/src/provider.rs`.
- Compatibility: source organization only; public APIs and persisted shapes
  remain unchanged.
- Dependencies: no new dependencies.

## Approval Boundary

Approval covers internal module moves, private visibility adjustments, and
compatibility re-exports required to preserve the existing public API. It does
not cover behavior, schema, dependency, or public API changes.
