## Context

`cache.rs` contains public request/result types, protected operation
fingerprints, persisted idempotency state, the provider dispatch mechanism,
validation/reduction helpers, and a large in-file test suite. `runtime/session.rs`
contains the session handle and state access, cache/checkpoint coordination,
turn admission and execution, cache recovery, persistence, and lifecycle
helpers. These responsibilities can be moved without changing contracts.

## Goals / Non-Goals

- Goals:
  - Give request/fingerprint/state/dispatch/validation code cohesive owners.
  - Give session cache coordination, checkpoint recovery, and turn lifecycle
    cohesive owners.
  - Preserve supported module paths, visibility, serialized forms, and runtime
    ordering.
  - Keep cache identity construction, its validated wire form, and bounded
    component validation together behind the provider module's current root.
- Non-Goals:
  - Change cache behavior, checkpoint state transitions, provider semantics,
    or turn admission policy.
  - Add dependencies, alter public APIs, or edit the compatibility consumer.

## Decisions

- Decision: Convert the cache file into a directory module with a small stable
  root that declares private submodules and re-exports the current public
  surface. Keep the dispatch pipeline and its exhaustive reduction in the
  dispatch owner; keep validation rules grouped in validation.
- Decision: Convert `runtime/session.rs` into `runtime/session/mod.rs` and
  private children. Keep `SessionInner` and state-owning type declarations at
  the root, and place `SessionHandle` implementation groups in child modules
  where their lifecycle responsibility is clear. Re-export current supported
  types from the root.
- Decision: Keep tests with the ownership boundary they exercise; move large
  private test groups only where module privacy can be preserved without
  weakening assertions or shrinking the conformance inventory.
- Decision: Move the provider cache-identity data types, builder, custom
  deserializer, and identity-only validation helpers as one cohesive unit. The
  provider root re-exports its current public types; all other provider
  contracts stay in the existing provider module.
- Alternatives considered: broad mechanical line-range splitting; rejected
  because it would create unclear boundaries and brittle private visibility.
- Alternatives considered: introduce facade helper modules that only forward
  calls; rejected because extracted modules must own implementation.

## Risks / Trade-offs

- Moving private code can require `pub(super)` access between sibling modules.
  Keep visibility no broader than necessary and keep public declarations
  re-exported from stable roots.
- Rust privacy and test access can constrain file boundaries; when a coherent
  operation must remain together, favor preserving the ordered implementation
  over splitting a single critical pipeline.

## Migration Plan

No migration is required. This change only moves source items and updates
module declarations. Compile and focused conformance gates verify the move.

## Open Questions

None.
