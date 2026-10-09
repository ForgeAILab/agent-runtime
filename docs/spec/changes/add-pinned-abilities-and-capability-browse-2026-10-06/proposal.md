---
created_at: 2026-10-06T23:49:35Z
updated_at: 2026-10-06T23:53:59Z
---

## Why
Core tools must be usable regardless of prompt vocabulary, and agents must be able to discover the full authorized capability scope.

## What Changes
Implement the user-approved runtime brief: pinned abilities, browse and explicit activation, descriptive search, scope patterns, and a host catalog. Preserve existing public signatures.

## Impact
Affected specs: capability-routing, registry-foundation, runtime-api. Activation state keeps its wire layout and supports completed-boundary rebase of older snapshots. A second protected bootstrap necessarily changes live-routing epoch contents even with an empty pinned set.
