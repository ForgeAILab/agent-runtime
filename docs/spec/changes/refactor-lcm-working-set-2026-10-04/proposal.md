---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
---

# Proposal: One LCM sizer, working sets and provider summaries (U6)

## Why

LCM undercounts tool content, uses a provider-window-sized pressure budget, and
rejects resumed sessions when tuning changes. Source spans and summary output
caps are conflated. Hosts need bounded per-request working sets and a neutral,
provider-backed summary mechanism.

## What Changes

- Split strict timeline/binding/store-schema/classifier/guard identity from
  policy/sizer/model tunables before changing algorithm revisions. Rebuild
  derived state from authorized active nodes when tunables change.
- Adapt RequestSizer::size_message for LCM entries and summary messages. Carry
  measured fixed overhead from the last successful plan into LCM state.
- Add optional WorkingSetPolicy { target_tokens, hard_tokens } to RuntimeBuilder.
  Cap planner input at min(window minus reserves, hard); evaluate LCM pressure
  against target minus measured overhead. Absence preserves budget selection.
- Ensure leaf selection can include one full turn pair, derive hard rounds from
  overage and expected reclaim, and offer opt-in soft compaction after
  TurnCompleted through the existing idle admission/persistence path.
- **BREAKING:** replace escalation target_tokens with
  leaf_source_target_tokens, summary_max_ratio (default 0.25), and
  min_reclaim_ratio. Enforce measured output/source and reclaim ratios at every
  escalation level and deterministic fallback.
- Add feature-gated ProviderLcmSummaryModel<P: Provider>. Hosts supply all
  instruction text. Runtime rendering preserves compact tool calls/results,
  every request crosses ContextPlanner, and bounded map-reduce handles source
  leaves exceeding the summary model window with cancellation/deadline support.
- Bump LCM_ALGORITHM_REVISION for new sizing/summary behavior. Improve fallback
  rendering to include tool names, bounded arguments and results.

## Impact

Affected specs: context-management, runtime-reproducibility. Affected crates:
agent-runtime-lcm, agent-runtime, agent-runtime-testkit (context budget cap only
if needed). No consumer dependencies or product prompts are introduced.
Protected exact state, Sensitive defaults, redaction, authority checks, guard
identity and provider planner admission remain unchanged. Smith/Nyx session,
event and store surfaces remain supported. U7 timeline ownership/reconciliation
and U8 session modes/forking are explicitly deferred.

## Compatibility and Consumer Gates

Forge 3.6 may replace its private sizer and summarizer, configure a working set,
and opt into boundary compaction. Hosts constructing LcmEscalationPolicy must
replace target_tokens with the new source target and ratio fields (or defaults).
No store adapter migration or checkpoint backfill is required. Tunable bumps
rebuild active-node metadata rather than deleting persisted LCM state; strict
identity changes still fail closed. Existing node provenance remains exact.
Actual three-consumer builds remain a coordinated release gate; run all named
testkit consumer fixtures and LCM conformance here.

## Authorization

The 2026-10-04 U6 request explicitly authorizes spec-first implementation and
checks. This proposal records that approved scope; no separate approval pause
is needed. Leave the implementation and spec uncommitted and unarchived.
