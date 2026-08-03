---
created_at: 2026-08-03T21:05:00Z
updated_at: 2026-08-03T21:05:00Z
---

## Why

`SemanticSummaryCoordinator` triggers on a count of completed turns
(`SemanticSummaryPolicy::trigger_turns`, default 6). Turn count is a poor proxy
for context pressure in both directions: one turn carrying a large tool result
can exhaust the window before turn two, while six trivial turns fire a paid
summary model call that reclaims almost nothing.

It is also cache-hostile. Summarization rewrites history, which invalidates a
provider's prompt-cache prefix from the rewrite point onward — and because
summarization is most effective on the *oldest* history, an effective summary is
also a maximally destructive one for cache. Firing it on a clock rather than on
pressure means paying that invalidation on sessions that did not need it.

The data required to do better is already at the seam. `TurnCommitView` carries
the canonical append-only usage ledger, so the coordinator can observe what each
turn actually cost. What it lacks is the budget to compare against.

## What Changes

- `SemanticSummaryPolicy::trigger_turns` becomes `min_turns`: an eligibility
  floor rather than a trigger. Reaching it no longer causes summarization on its
  own, so a long session of small turns is left alone.
- New `trigger_percent` and `input_budget_tokens`. Summarization fires when
  observed input growth reaches `trigger_percent` of the budget that remains
  after the session's opening turn.
- Growth is measured **after the opening turn's input cost**, which stands in
  for the stable prefix. A host that activates more skills or larger project
  instructions therefore does not summarize earlier merely for having a larger
  prefix — the case a total-usage comparison gets backwards, and the one whose
  cached prefix is most worth preserving.
- Only `UsageSource::ProviderAttempt` records are read. The coordinator's own
  `SemanticSummary` spend is separately attributed and must not feed back into
  the decision to summarize again.

## Impact

- Affected specs: `context-management`
- Affected code: `crates/agent-runtime/src/harness/semantic_summary.rs`
- Breaking: `SemanticSummaryPolicy` loses `trigger_turns` and gains three
  fields. Hosts constructing the policy must supply an input budget; a zero
  budget is rejected by `validate` rather than silently disabling the trigger.
- No pipeline API change: `TurnCommitView` already carries the usage ledger, and
  no new session state is stored — the baseline is derived from the ledger
  itself.
- Not in scope: the near-boundary notice to the model, which belongs in the
  host's context contribution rather than here, and structural compaction, which
  keeps its existing watermarks.
