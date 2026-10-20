---
created_at: 2026-10-20T00:00:00Z
updated_at: 2026-10-20T00:00:00Z
status: implemented
---

## Why

Condensation builds a linear chain instead of a tree. When the active
frontier exceeds `condensation_fanout`, the harness plans groups over all
active nodes in range order and commits only the first group
(`crates/agent-runtime/src/harness/lcm.rs`, `plan_condensations(...)` then
`plan.group_plans.first()`). The oldest active node is the largest condensed
node, so every condensation wraps it again and depth grows by one per
condensation. A production Nyx timeline reached a 646-level chain: the
oldest content had been re-summarized hundreds of times, losing detail at
each pass, and any consumer that walks the tree recursively has to cope
with depth proportional to history length.

## What Changes

- The structural trigger changes. Today the harness condenses whenever the
  active frontier holds more than `condensation_fanout` nodes. A frontier
  capped at `fanout` cannot hold a balanced tree (that needs up to
  `fanout - 1` nodes per level), so every condensation must re-wrap the
  largest node. Instead, a condensation is due when a size tier is full: a
  contiguous run of `fanout` adjacent active nodes of the same tier (tier =
  `floor(log_fanout(covered entries))`). The newest full run is condensed
  first.
- Token pressure (the existing soft/hard thresholds) still bounds context:
  when compaction is needed and leaf summarization alone cannot relieve it,
  the smallest-tier run of at least two adjacent nodes is condensed even if
  its tier is not full (newest first; fallback: the adjacent pair with the
  smallest combined size). Depth then grows only when the budget forces it.
- Without pressure, summary depth grows logarithmically with history length
  and the frontier holds at most about `(fanout - 1)` nodes per tier.
- Selection stays a deterministic function of the active nodes, policy and
  pressure mode, so a pending condensation re-plans identically on resume.
- No persisted field is added. Existing deep chains are left as they are;
  new condensations form a balanced structure beside them.


- Measured trade-off (simulation of the selection rules over 2000 leaf
  summaries, condensing only under pressure): with room for 2× `fanout`
  active summaries, max depth is ~18–21 at fanout 4 (log bound ~8); with
  room for only `fanout` summaries depth stays ~110, because a frontier of
  k nodes holding n leaves cannot be shallow. The old count trigger fixed
  the frontier at `fanout` regardless of budget; now the frontier is bounded
  by the token budget, so hosts with budget headroom get shallow trees.

## Non-Goals

- Rewriting or rebalancing existing nodes.
- Changing leaf summarization, pressure thresholds or fanout defaults.

## Impact

- Affected specs: `lossless-context-memory`
- Affected code: `crates/agent-runtime-lcm/src/planning.rs`
  (`plan_condensations` or a new selection helper),
  `crates/agent-runtime/src/harness/lcm.rs` (both call sites),
  `crates/agent-runtime-testkit/src/conformance/lcm.rs`
- Compatibility: behavior change in which nodes are grouped; plans remain
  deterministic and fingerprints keep their meaning.
